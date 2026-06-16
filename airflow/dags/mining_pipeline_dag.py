"""
DAG: Mining Grade Control Pipeline
Orquesta el pipeline completo de datos mineros.
Schedule: diario a las 6:00 AM (antes del turno de día)
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Agregar el proyecto al path de Python
PROJECT_DIR = os.path.expanduser("~/mining-data-pipeline")
sys.path.insert(0, PROJECT_DIR)

# ── Argumentos por defecto para todas las tareas ─────────────────────────────
default_args = {
    "owner"           : "mining_engineer",
    "depends_on_past" : False,
    "start_date"      : datetime(2026, 6, 16),
    "email_on_failure": False,
    "email_on_retry"  : False,
    "retries"         : 1,
    "retry_delay"     : timedelta(minutes=5),
}

# ── Definición del DAG ────────────────────────────────────────────────────────
with DAG(
    dag_id="mining_grade_control_pipeline",
    default_args=default_args,
    description="Pipeline ETL para control de ley mineral - sensor vs assay",
    schedule_interval="0 6 * * *",  # diario a las 6:00 AM
    catchup=False,
    tags=["mining", "etl", "grade-control"],
) as dag:

    # ── TAREA 1: Verificar que PostgreSQL está disponible ─────────────────────
    def check_db_connection():
        from sqlalchemy import create_engine, text
        from dotenv import load_dotenv
        import os
        load_dotenv(f"{PROJECT_DIR}/.env")
        url = (
            f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
            f"@localhost:5432/mining_db"
        )
        engine = create_engine(url)
        with engine.connect() as conn:
            count = conn.execute(
                text("SELECT COUNT(*) FROM fact_sensor_readings")
            ).scalar()
            print(f"DB OK - fact_sensor_readings tiene {count} filas")
        return "check_db OK"

    check_db = PythonOperator(
        task_id="check_database_connection",
        python_callable=check_db_connection,
    )
    # ── TAREA 2: Generar datos sintéticos ─────────────────────────────────────
    def generate_data():
        os.chdir(PROJECT_DIR)
        from src.generate_synthetic_data import (
            generate_sensor_readings,
            generate_assay_results,
            introduce_data_quality_issues,
        )
        import pandas as pd

        print("Generando datos sintéticos...")
        sensor_df = generate_sensor_readings()
        assay_df  = generate_assay_results(sensor_df)
        sensor_dirty = introduce_data_quality_issues(sensor_df)
        sensor_dirty = sensor_dirty.drop(columns=["_true_grade"])

        sensor_dirty.to_csv(f"{PROJECT_DIR}/data/raw/sensor_readings.csv", index=False)
        assay_df.to_csv(f"{PROJECT_DIR}/data/raw/assay_results.csv", index=False)

        print(f"sensor_readings.csv: {len(sensor_dirty)} filas")
        print(f"assay_results.csv  : {len(assay_df)} filas")
        return "generate_data OK"

    task_generate = PythonOperator(
        task_id="generate_synthetic_data",
        python_callable=generate_data,
    )

    # ── TAREA 3: Ejecutar ETL ─────────────────────────────────────────────────
    def run_etl():
        os.chdir(PROJECT_DIR)
        from dotenv import load_dotenv
        load_dotenv(f"{PROJECT_DIR}/.env")

        from src.etl_pipeline import (
            extract,
            transform_sensor,
            transform_assay,
            build_dim_equipment,
            build_dim_location,
            build_dim_time,
            load_dimensions,
            load_facts,
            get_engine,
        )
        from sqlalchemy import text

        engine = get_engine()

        # Limpiar tablas antes de recargar (pipeline idempotente)
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE fact_assay_results, fact_sensor_readings CASCADE"))
            conn.execute(text("TRUNCATE dim_equipment, dim_location, dim_time CASCADE"))
        print("Tablas limpiadas para recarga...")

        sensor_raw, assay_raw = extract()
        sensor_clean = transform_sensor(sensor_raw)
        assay_clean  = transform_assay(assay_raw)

        dim_eq   = build_dim_equipment(sensor_clean)
        dim_loc  = build_dim_location(sensor_clean)
        dim_time = build_dim_time(sensor_clean, assay_clean)

        load_dimensions(engine, dim_eq, dim_loc, dim_time)
        load_facts(engine, sensor_clean, assay_clean)
        return "etl OK"

    task_etl = PythonOperator(
        task_id="run_etl_pipeline",
        python_callable=run_etl,
    )

    # ── TAREA 4: Reporte de calidad de datos ─────────────────────────────────
    def run_qc_report():
        os.chdir(PROJECT_DIR)
        from dotenv import load_dotenv
        load_dotenv(f"{PROJECT_DIR}/.env")

        from src.data_quality_checks import get_engine, generate_report
        engine = get_engine()
        generate_report(engine)
        return "qc_report OK"

    task_qc = PythonOperator(
        task_id="data_quality_report",
        python_callable=run_qc_report,
    )

    # ── DEPENDENCIAS: define el orden de ejecución ───────────────────────────
    # check_db → generate_data → etl → qc_report
    check_db >> task_generate >> task_etl >> task_qc
