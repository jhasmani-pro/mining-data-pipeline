"""
Data Quality Report
Genera métricas de calidad de datos y validación de sensores.
"""
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import sys
from datetime import datetime

# ── Cargar credenciales ──────────────────────────────────────────────────────
load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def get_engine():
    return create_engine(DATABASE_URL)

def generate_report(engine=None): 
    if engine is None:
        engine = get_engine()
        
    print("\n" + "="*60)
    print("  REPORTE DE CALIDAD DE DATOS - MINING GRADE CONTROL")
    print(f"  Generado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60 + "\n")
    sys.stdout.flush()

    # Usamos SQLAlchemy puro para leer, cero dependencias de Pandas SQL
    with engine.connect() as conn:
        
        # ── CHECK 1: Cobertura por Equipo ────────────────────────────────────────
        print("📊 CHECK 1: Cobertura por Equipo")
        print("-" * 40)
        res1 = conn.execute(text("SELECT * FROM v_equipment_kpis"))
        df_kpis = pd.DataFrame(res1.fetchall(), columns=res1.keys())
        print(df_kpis.to_string(index=False))
        sys.stdout.flush()

        # ── CHECK 2: Calibración de Sensores ─────────────────────────────────────
        print("\n📊 CHECK 2: Calibración de Sensores")
        print("-" * 40)
        for index, row in df_kpis.iterrows():
            status = "✅ OK" if abs(row['bias']) <= 0.05 else "❌ REVISAR CALIBRACIÓN"
            print(f"Equipo {row['equipment_id']}: Bias = {row['bias']:.4f} | MAE = {row['mae']:.4f} -> {status}")
        sys.stdout.flush()

        # ── CHECK 3: Turnaround de Laboratorio ───────────────────────────────────
        print("\n📊 CHECK 3: Turnaround de Laboratorio")
        print("-" * 40)
        res3 = conn.execute(text("SELECT AVG(turnaround_days) as avg_t, MAX(turnaround_days) as max_t FROM fact_assay_results"))
        row3 = res3.fetchone()
        avg_t = row3[0] if row3[0] is not None else 0
        max_t = row3[1] if row3[1] is not None else 0
        print(f"Turnaround promedio : {avg_t:.2f} días")
        print(f"Turnaround máximo   : {max_t:.2f} días")
        sys.stdout.flush()

        # ── CHECK 4: Anomalías por Banco ─────────────────────────────────────────
        print("\n📊 CHECK 4: Anomalías por Banco")
        print("-" * 40)
        res4 = conn.execute(text("""
            SELECT bench_id, COUNT(*) as lecturas_outliers 
            FROM fact_sensor_readings 
            WHERE is_outlier = true 
            GROUP BY bench_id
            ORDER BY lecturas_outliers DESC
        """))
        df_bench = pd.DataFrame(res4.fetchall(), columns=res4.keys())
        if not df_bench.empty:
            print(df_bench.to_string(index=False))
        else:
            print("No se detectaron outliers significativos por banco.")
        sys.stdout.flush()

    print("\n" + "="*60)
    print("  ✅ REPORTE FINALIZADO")
    print("="*60 + "\n")
    sys.stdout.flush()

if __name__ == "__main__":
    generate_report()
