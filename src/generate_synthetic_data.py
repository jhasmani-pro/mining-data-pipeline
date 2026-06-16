"""
Genera datos sintéticos de sensores de ley mineral (grade) y resultados de assay.
Simula el flujo de datos típico en operaciones de control de ley en minería.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# Configuración reproducible
np.random.seed(42)
random.seed(42)

# ---- PARÁMETROS DE SIMULACIÓN ----
N_READINGS = 5000  # número de lecturas de sensor
START_DATE = datetime(2025, 1, 1)
DAYS_RANGE = 90  # 3 meses de operación

EQUIPMENT_IDS = [f"SHOVEL-0{i}" for i in range(1, 4)]  # 3 palas
BENCH_IDS = [f"BENCH-{i}" for i in range(100, 115, 5)]  # niveles de mina: 100,105,110

# Ley promedio real de cobre en el yacimiento (simplificado, en %)
TRUE_GRADE_MEAN = 0.85
TRUE_GRADE_STD = 0.25


def generate_sensor_readings(n=N_READINGS):
    """Genera lecturas de sensor con ruido respecto a la 'ley real' subyacente."""
    rows = []
    for i in range(n):
        # Timestamp aleatorio dentro del rango de días
        ts = START_DATE + timedelta(
            days=random.randint(0, DAYS_RANGE),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59)
        )

        # Ley "real" subyacente del bloque de mina (no observable directamente)
        true_grade = max(0.05, np.random.normal(TRUE_GRADE_MEAN, TRUE_GRADE_STD))

        # El sensor mide con un error/ruido característico (+/- 15% aprox)
        sensor_noise = np.random.normal(0, 0.08)
        sensor_grade = max(0.0, true_grade + sensor_noise)

        row = {
            "reading_id": i + 1,
            "timestamp": ts,
            "equipment_id": random.choice(EQUIPMENT_IDS),
            "bench_id": random.choice(BENCH_IDS),
            "x_coord": round(random.uniform(1000, 2000), 2),
            "y_coord": round(random.uniform(5000, 6000), 2),
            "sensor_cu_grade": round(sensor_grade, 4),
            "tonnage": round(random.uniform(80, 250), 2),
            "_true_grade": round(true_grade, 4)  # oculto: para generar assays consistentes
        }
        rows.append(row)

    return pd.DataFrame(rows)


def generate_assay_results(sensor_df, sample_rate=0.08):
    """
    Genera resultados de assay para una muestra de las lecturas de sensor.
    El assay es más preciso (menos ruido) que el sensor, y llega con delay.
    """
    sampled = sensor_df.sample(frac=sample_rate, random_state=42)

    rows = []
    for idx, (_, r) in enumerate(sampled.iterrows()):
        # El laboratorio mide con menos ruido (+/- 3% aprox)
        lab_noise = np.random.normal(0, 0.02)
        assay_grade = max(0.0, r["_true_grade"] + lab_noise)

        sample_date = r["timestamp"]
        result_delay_days = random.randint(2, 5)
        result_date = sample_date + timedelta(days=result_delay_days)

        rows.append({
            "assay_id": idx + 1,
            "reading_id": r["reading_id"],
            "sample_date": sample_date,
            "result_date": result_date,
            "assay_cu_grade": round(assay_grade, 4)
        })

    return pd.DataFrame(rows)


def introduce_data_quality_issues(sensor_df):
    """
    Introduce problemas realistas de calidad de datos:
    - Algunos valores nulos
    - Algunos outliers (sensor descalibrado)
    - Algunos duplicados
    """
    df = sensor_df.copy()

    # 1. Valores nulos en tonnage (~1%)
    null_idx = df.sample(frac=0.01, random_state=1).index
    df.loc[null_idx, "tonnage"] = np.nan

    # 2. Outliers: sensor descalibrado da lecturas absurdas (~0.5%)
    outlier_idx = df.sample(frac=0.005, random_state=2).index
    df.loc[outlier_idx, "sensor_cu_grade"] = df.loc[outlier_idx, "sensor_cu_grade"] * 10

    # 3. Duplicados: mismo reading_id aparece dos veces (~0.3%)
    dup_rows = df.sample(frac=0.003, random_state=3)
    df = pd.concat([df, dup_rows], ignore_index=True)

    return df


if __name__ == "__main__":
    print("Generando lecturas de sensor...")
    sensor_df = generate_sensor_readings()

    print("Generando resultados de assay (muestra del 8%)...")
    assay_df = generate_assay_results(sensor_df)

    print("Introduciendo problemas de calidad de datos...")
    sensor_df_dirty = introduce_data_quality_issues(sensor_df)

    # Quitamos la columna oculta antes de guardar (no existe en la realidad)
    sensor_df_dirty = sensor_df_dirty.drop(columns=["_true_grade"])

    # Guardar en data/raw/
    sensor_df_dirty.to_csv("data/raw/sensor_readings.csv", index=False)
    assay_df.to_csv("data/raw/assay_results.csv", index=False)

    print(f"\n✅ sensor_readings.csv: {len(sensor_df_dirty)} filas")
    print(f"✅ assay_results.csv: {len(assay_df)} filas")
    print("\nMuestra de sensor_readings:")
    print(sensor_df_dirty.head())
    print("\nMuestra de assay_results:")
    print(assay_df.head())
