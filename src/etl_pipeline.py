"""
ETL Pipeline - Mining Grade Control
Extrae datos crudos de CSV, los transforma y carga en PostgreSQL.

Flujo:
    data/raw/sensor_readings.csv  ──►  fact_sensor_readings
    data/raw/assay_results.csv    ──►  fact_assay_results
    (dimensiones se pueblan automáticamente desde los datos)
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
from datetime import datetime

# ── Cargar credenciales desde .env ──────────────────────────────────────────
load_dotenv()

DB_HOST     = os.getenv("DB_HOST")
DB_PORT     = os.getenv("DB_PORT")
DB_NAME     = os.getenv("DB_NAME")
DB_USER     = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


# ── Conexión ─────────────────────────────────────────────────────────────────
def get_engine():
    engine = create_engine(DATABASE_URL)
    return engine


# ════════════════════════════════════════════════════════════════════════════
# EXTRACCIÓN
# ════════════════════════════════════════════════════════════════════════════
def extract():
    """Lee los CSV crudos y retorna DataFrames."""
    print("\n[EXTRACT] Leyendo archivos CSV...")

    sensor_df = pd.read_csv(
        "data/raw/sensor_readings.csv",
        parse_dates=["timestamp"]
    )
    assay_df = pd.read_csv(
        "data/raw/assay_results.csv",
        parse_dates=["sample_date", "result_date"]
    )

    print(f"  sensor_readings : {len(sensor_df):>6} filas")
    print(f"  assay_results   : {len(assay_df):>6} filas")
    return sensor_df, assay_df


# ════════════════════════════════════════════════════════════════════════════
# TRANSFORMACIÓN + VALIDACIÓN (QC)
# ════════════════════════════════════════════════════════════════════════════
def transform_sensor(df):
    """
    Limpia y enriquece las lecturas de sensor.
    Marca outliers, duplicados y registros que no pasan QC.
    """
    print("\n[TRANSFORM] Procesando sensor_readings...")
    original_count = len(df)

    # ── 1. Duplicados ────────────────────────────────────────────────────────
    df["is_duplicate"] = df.duplicated(subset=["reading_id"], keep="first")
    n_dupes = df["is_duplicate"].sum()
    print(f"  Duplicados detectados     : {n_dupes}")

    # ── 2. Nulos en tonnage ──────────────────────────────────────────────────
    n_nulls = df["tonnage"].isna().sum()
    print(f"  Nulos en tonnage          : {n_nulls}")
    # Imputamos con la mediana por equipo (estrategia conservadora)
    df["tonnage"] = df.groupby("equipment_id")["tonnage"].transform(
        lambda x: x.fillna(x.median())
    )

    # ── 3. Outliers en sensor_cu_grade ──────────────────────────────────────
    # Definición: más de 3 desviaciones estándar de la media (regla estadística estándar)
    mean_grade = df["sensor_cu_grade"].mean()
    std_grade  = df["sensor_cu_grade"].std()
    upper_limit = mean_grade + 3 * std_grade
    lower_limit = max(0, mean_grade - 3 * std_grade)

    df["is_outlier"] = (
        (df["sensor_cu_grade"] > upper_limit) |
        (df["sensor_cu_grade"] < lower_limit)
    )
    n_outliers = df["is_outlier"].sum()
    print(f"  Outliers en sensor grade  : {n_outliers}")
    print(f"    Límite inferior         : {lower_limit:.4f}%")
    print(f"    Límite superior         : {upper_limit:.4f}%")

    # ── 4. Flag QC general ───────────────────────────────────────────────────
    # Pasa QC si: no es duplicado, no es outlier, tonnage válido
    df["qc_passed"] = (
        ~df["is_duplicate"] &
        ~df["is_outlier"] &
        df["tonnage"].notna()
    )
    n_qc_passed = df["qc_passed"].sum()
    print(f"  Registros que pasan QC    : {n_qc_passed} / {original_count}")

    # ── 5. Guardar versión procesada ─────────────────────────────────────────
    df.to_csv("data/processed/sensor_readings_clean.csv", index=False)

    return df


def transform_assay(df):
    """Calcula campos derivados en assay_results."""
    print("\n[TRANSFORM] Procesando assay_results...")

    # Calcular turnaround en días (tiempo de respuesta del laboratorio)
    df["turnaround_days"] = (
        df["result_date"] - df["sample_date"]
    ).dt.total_seconds() / 86400

    df.to_csv("data/processed/assay_results_clean.csv", index=False)
    print(f"  Turnaround promedio (días): {df['turnaround_days'].mean():.1f}")
    return df


def build_dim_equipment(sensor_df):
    """Construye la tabla de dimensión de equipos desde los datos."""
    equipment_ids = sensor_df["equipment_id"].unique()
    dim = pd.DataFrame({
        "equipment_id"  : equipment_ids,
        "equipment_type": "SHOVEL",
        "model"         : "CAT 7495",
        "capacity_tons" : 250.0,
        "active"        : True
    })
    return dim


def build_dim_location(sensor_df):
    """Construye la tabla de dimensión de ubicaciones/bancos."""
    benches = sensor_df["bench_id"].unique()
    elevation_map = {
        "BENCH-100": 3100,
        "BENCH-105": 3095,
        "BENCH-110": 3090,
    }
    zone_map = {
        "BENCH-100": "SULFIDE",
        "BENCH-105": "SULFIDE",
        "BENCH-110": "OXIDE",
    }
    dim = pd.DataFrame({
        "bench_id"        : benches,
        "bench_elevation" : [elevation_map.get(b, 3000) for b in benches],
        "zone"            : [zone_map.get(b, "UNKNOWN") for b in benches],
    })
    return dim


def build_dim_time(sensor_df, assay_df):
    """Construye la dimensión tiempo con todos los días en el rango de datos."""
    all_dates = pd.concat([
        sensor_df["timestamp"].dt.date.rename("date"),
        assay_df["sample_date"].dt.date.rename("date"),
        assay_df["result_date"].dt.date.rename("date"),
    ]).drop_duplicates()

    dates = pd.to_datetime(sorted(all_dates))
    dim = pd.DataFrame({
        "full_date"   : dates.date,
        "year"        : dates.year,
        "quarter"     : dates.quarter,
        "month"       : dates.month,
        "month_name"  : dates.strftime("%B"),
        "week"        : dates.isocalendar().week.values,
        "day_of_week" : dates.dayofweek,
        "shift"       : "DIA",  # simplificado
    })
    return dim


# ════════════════════════════════════════════════════════════════════════════
# CARGA
# ════════════════════════════════════════════════════════════════════════════
def load_dimensions(engine, dim_equipment, dim_location, dim_time):
    """Carga las tablas de dimensión. Usa INSERT ... ON CONFLICT DO NOTHING (idempotente)."""
    print("\n[LOAD] Cargando dimensiones...")

    with engine.begin() as conn:
        # dim_equipment
        for _, row in dim_equipment.iterrows():
            conn.execute(text("""
                INSERT INTO dim_equipment (equipment_id, equipment_type, model, capacity_tons, active)
                VALUES (:equipment_id, :equipment_type, :model, :capacity_tons, :active)
                ON CONFLICT (equipment_id) DO NOTHING
            """), row.to_dict())
        print(f"  dim_equipment : {len(dim_equipment)} registros")

        # dim_location
        for _, row in dim_location.iterrows():
            conn.execute(text("""
                INSERT INTO dim_location (bench_id, bench_elevation, zone)
                VALUES (:bench_id, :bench_elevation, :zone)
                ON CONFLICT (bench_id) DO NOTHING
            """), row.to_dict())
        print(f"  dim_location  : {len(dim_location)} registros")

        # dim_time
        for _, row in dim_time.iterrows():
            conn.execute(text("""
                INSERT INTO dim_time (full_date, year, quarter, month, month_name, week, day_of_week, shift)
                VALUES (:full_date, :year, :quarter, :month, :month_name, :week, :day_of_week, :shift)
                ON CONFLICT (full_date) DO NOTHING
            """), row.to_dict())
        print(f"  dim_time      : {len(dim_time)} registros")


def load_facts(engine, sensor_df, assay_df):
    """Carga las tablas de hechos. Solo carga registros que pasan QC básico."""
    print("\n[LOAD] Cargando tablas de hechos...")

    # Solo cargamos el primer registro de cada reading_id (eliminamos duplicados reales)
    sensor_clean = sensor_df.drop_duplicates(subset=["reading_id"], keep="first")

    # Columnas que van a la tabla
    sensor_cols = [
        "reading_id", "timestamp", "equipment_id", "bench_id",
        "x_coord", "y_coord", "sensor_cu_grade", "tonnage",
        "is_outlier", "is_duplicate", "qc_passed"
    ]

    sensor_clean[sensor_cols].to_sql(
        "fact_sensor_readings",
        engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=500
    )
    print(f"  fact_sensor_readings: {len(sensor_clean)} filas cargadas")

    assay_cols = [
        "assay_id", "reading_id", "sample_date",
        "result_date", "assay_cu_grade", "turnaround_days"
    ]
    assay_df[assay_cols].to_sql(
        "fact_assay_results",
        engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=500
    )
    print(f"  fact_assay_results  : {len(assay_df)} filas cargadas")


# ════════════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    start = datetime.now()
    print("=" * 60)
    print("  MINING ETL PIPELINE - Iniciando")
    print(f"  {start.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    engine = get_engine()

    # EXTRACT
    sensor_raw, assay_raw = extract()

    # TRANSFORM
    sensor_clean = transform_sensor(sensor_raw)
    assay_clean  = transform_assay(assay_raw)

    # BUILD DIMENSIONS
    dim_eq   = build_dim_equipment(sensor_clean)
    dim_loc  = build_dim_location(sensor_clean)
    dim_time = build_dim_time(sensor_clean, assay_clean)

    # LOAD
    load_dimensions(engine, dim_eq, dim_loc, dim_time)
    load_facts(engine, sensor_clean, assay_clean)

    elapsed = (datetime.now() - start).total_seconds()
    print(f"\n{'='*60}")
    print(f"  ✅ Pipeline completado en {elapsed:.2f} segundos")
    print(f"{'='*60}")
