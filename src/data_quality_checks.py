"""
Data Quality Checks - Mining Grade Control Pipeline
Genera reporte automático de calidad de datos.
En producción esto correría diariamente vía Airflow o cron job.
"""

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from datetime import datetime
import os

load_dotenv()

DATABASE_URL = (
    f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)


def get_engine():
    return create_engine(DATABASE_URL)


def check_sensor_coverage(engine):
    """
    Verifica cobertura de datos por equipo en las últimas 24h.
    En producción: alerta si una pala deja de reportar.
    """
    query = text("""
        SELECT
            equipment_id,
            COUNT(*)                                    AS readings_last_90d,
            MAX(timestamp)                              AS last_reading,
            ROUND(AVG(sensor_cu_grade)::numeric, 4)    AS avg_grade,
            SUM(CASE WHEN NOT qc_passed THEN 1 ELSE 0 END) AS failed_qc
        FROM fact_sensor_readings
        GROUP BY equipment_id
        ORDER BY equipment_id
    """)
    return pd.read_sql(query, engine)


def check_sensor_bias(engine):
    """
    Detecta bias sistemático por equipo.
    Bias > 0.05 en valor absoluto = sensor requiere recalibración.
    """
    query = text("""
        SELECT
            equipment_id,
            bias,
            mae,
            rmse,
            mean_relative_error_pct,
            CASE
                WHEN ABS(bias) > 0.05 THEN '⚠️  REQUIERE CALIBRACIÓN'
                WHEN ABS(bias) > 0.02 THEN '⚡ MONITOREAR'
                ELSE '✅ OK'
            END AS calibration_status
        FROM v_equipment_kpis
        ORDER BY ABS(bias) DESC
    """)
    return pd.read_sql(query, engine)


def check_assay_turnaround(engine):
    """
    Verifica si el laboratorio está tardando más de lo normal.
    SLA típico en minería: máximo 5 días hábiles.
    """
    query = text("""
        SELECT
            ROUND(AVG(turnaround_days)::numeric, 1)    AS avg_turnaround,
            ROUND(MIN(turnaround_days)::numeric, 1)    AS min_turnaround,
            ROUND(MAX(turnaround_days)::numeric, 1)    AS max_turnaround,
            COUNT(*) FILTER (WHERE turnaround_days > 5) AS breached_sla,
            COUNT(*)                                    AS total_assays
        FROM fact_assay_results
    """)
    return pd.read_sql(query, engine)


def check_grade_anomalies(engine):
    """
    Detecta bancos con diferencia sensor-assay mayor al 10%.
    Puede indicar mineralogía compleja o sensor mal posicionado.
    """
    query = text("""
        SELECT
            bench_id,
            COUNT(*)                                        AS paired_samples,
            ROUND(AVG(relative_error_pct)::numeric, 2)     AS avg_relative_error_pct,
            ROUND(AVG(ABS(grade_difference))::numeric, 4)  AS mae,
            COUNT(*) FILTER (
                WHERE accuracy_category IN ('REVISAR','CRITICO')
            )                                              AS problematic_readings
        FROM v_sensor_assay_reconciliation
        GROUP BY bench_id
        ORDER BY mae DESC
    """)
    return pd.read_sql(query, engine)


def generate_report(engine):
    """Genera reporte completo de calidad de datos."""

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    separator = "=" * 60

    print(f"\n{separator}")
    print(f"  REPORTE DE CALIDAD DE DATOS - MINING GRADE CONTROL")
    print(f"  Generado: {now}")
    print(f"{separator}")

    # ── CHECK 1: Cobertura ───────────────────────────────────────────────────
    print("\n📊 CHECK 1: Cobertura por Equipo")
    print("-" * 40)
    df = check_sensor_coverage(engine)
    print(df.to_string(index=False))

    # ── CHECK 2: Calibración de sensores ────────────────────────────────────
    print("\n🎯 CHECK 2: Estado de Calibración de Sensores")
    print("-" * 40)
    df_bias = check_sensor_bias(engine)
    print(df_bias.to_string(index=False))

    # Alerta crítica
    critical = df_bias[df_bias["calibration_status"].str.contains("CALIBRACIÓN")]
    if not critical.empty:
        print(f"\n🚨 ALERTA: {len(critical)} sensor(es) requieren calibración:")
        for _, row in critical.iterrows():
            print(f"   → {row['equipment_id']} | bias={row['bias']} | MAE={row['mae']}")

    # ── CHECK 3: SLA laboratorio ─────────────────────────────────────────────
    print("\n🧪 CHECK 3: Turnaround de Laboratorio (SLA: ≤5 días)")
    print("-" * 40)
    df_sla = check_assay_turnaround(engine)
    print(df_sla.to_string(index=False))

    sla_breaches = df_sla["breached_sla"].iloc[0]
    if sla_breaches > 0:
        print(f"\n⚠️  {sla_breaches} assay(s) superaron el SLA de 5 días")
    else:
        print("\n✅ Todos los assays dentro del SLA")

    # ── CHECK 4: Anomalías por banco ─────────────────────────────────────────
    print("\n⛏️  CHECK 4: Anomalías por Banco (Sensor vs Assay)")
    print("-" * 40)
    df_anomaly = check_grade_anomalies(engine)
    print(df_anomaly.to_string(index=False))

    # ── RESUMEN EJECUTIVO ────────────────────────────────────────────────────
    print(f"\n{separator}")
    print("  RESUMEN EJECUTIVO")
    print(separator)

    with engine.connect() as conn:
        total_s = conn.execute(
            text("SELECT COUNT(*) FROM fact_sensor_readings")
        ).scalar()
        total_a = conn.execute(
            text("SELECT COUNT(*) FROM fact_assay_results")
        ).scalar()
        qc_rate = conn.execute(text("""
            SELECT ROUND(100.0 * SUM(CASE WHEN qc_passed THEN 1 ELSE 0 END)
                   / COUNT(*), 2)
            FROM fact_sensor_readings
        """)).scalar()

    print(f"  Total lecturas sensor  : {total_s:>6}")
    print(f"  Total assays           : {total_a:>6}")
    print(f"  QC Pass Rate global    : {qc_rate}%")
    print(f"  Cobertura assay        : {round(total_a/total_s*100, 1)}%")
    print(separator)


if __name__ == "__main__":
    engine = get_engine()
    generate_report(engine)
