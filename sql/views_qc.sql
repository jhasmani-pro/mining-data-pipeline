-- ============================================================
-- VISTAS ANALÍTICAS - Mining Grade Control
-- Propósito: Reconciliación sensor vs assay, KPIs operacionales,
--            detección de problemas de calidad de datos
-- Estas vistas son el producto que consume el Data Analyst
-- ============================================================


-- ============================================================
-- VISTA 1: Reconciliación directa sensor vs assay
-- Pregunta que responde: ¿Qué tan preciso es el sensor
-- comparado con el laboratorio (fuente de verdad)?
-- ============================================================
CREATE OR REPLACE VIEW v_sensor_assay_reconciliation AS
SELECT
    s.reading_id,
    s.timestamp,
    s.equipment_id,
    s.bench_id,
    s.sensor_cu_grade,
    s.tonnage,
    a.assay_cu_grade,
    a.turnaround_days,

    -- Diferencia absoluta entre sensor y assay
    ROUND((s.sensor_cu_grade - a.assay_cu_grade)::numeric, 4)
        AS grade_difference,

    -- Error relativo porcentual (clave para reportes de precisión)
    ROUND(
        ((s.sensor_cu_grade - a.assay_cu_grade) / 
         NULLIF(a.assay_cu_grade, 0) * 100)::numeric
    , 2) AS relative_error_pct,

    -- Clasificación del error para dashboards
    CASE
        WHEN ABS(s.sensor_cu_grade - a.assay_cu_grade) <= 0.05 THEN 'EXCELENTE'
        WHEN ABS(s.sensor_cu_grade - a.assay_cu_grade) <= 0.10 THEN 'ACEPTABLE'
        WHEN ABS(s.sensor_cu_grade - a.assay_cu_grade) <= 0.20 THEN 'REVISAR'
        ELSE 'CRITICO'
    END AS accuracy_category,

    s.qc_passed

FROM fact_sensor_readings s
INNER JOIN fact_assay_results a ON s.reading_id = a.reading_id
WHERE s.qc_passed = TRUE;


-- ============================================================
-- VISTA 2: KPIs por equipo (resumen ejecutivo)
-- Pregunta que responde: ¿Qué pala tiene mejor precisión?
-- ¿Hay algún sensor descalibrado?
-- ============================================================
CREATE OR REPLACE VIEW v_equipment_kpis AS
SELECT
    r.equipment_id,
    COUNT(*)                                            AS total_paired_readings,

    -- Precisión del sensor
    ROUND(AVG(ABS(r.grade_difference))::numeric, 4)    AS mae,
    ROUND(
        SQRT(AVG(POWER(r.grade_difference, 2)))::numeric
    , 4)                                               AS rmse,
    ROUND(AVG(r.relative_error_pct)::numeric, 2)       AS mean_relative_error_pct,

    -- Sesgo (bias): ¿el sensor sobreestima o subestima?
    ROUND(AVG(r.grade_difference)::numeric, 4)         AS bias,

    -- Distribución de categorías de precisión
    COUNT(*) FILTER (WHERE r.accuracy_category = 'EXCELENTE') AS cat_excelente,
    COUNT(*) FILTER (WHERE r.accuracy_category = 'ACEPTABLE') AS cat_aceptable,
    COUNT(*) FILTER (WHERE r.accuracy_category = 'REVISAR')   AS cat_revisar,
    COUNT(*) FILTER (WHERE r.accuracy_category = 'CRITICO')   AS cat_critico,

    -- Tonelaje total procesado
    ROUND(SUM(s.tonnage)::numeric, 0)                  AS total_tonnage

FROM v_sensor_assay_reconciliation r
JOIN fact_sensor_readings s ON r.reading_id = s.reading_id
GROUP BY r.equipment_id
ORDER BY mae ASC;


-- ============================================================
-- VISTA 3: Tendencia mensual de ley
-- Pregunta que responde: ¿Cómo evoluciona la ley del mineral
-- mes a mes? ¿Hay zonas de mejor ley?
-- ============================================================
CREATE OR REPLACE VIEW v_monthly_grade_trend AS
SELECT
    DATE_TRUNC('month', s.timestamp)                   AS month,
    s.bench_id,
    COUNT(*)                                            AS total_loads,
    ROUND(AVG(s.sensor_cu_grade)::numeric, 4)          AS avg_sensor_grade,
    ROUND(AVG(a.assay_cu_grade)::numeric, 4)           AS avg_assay_grade,
    ROUND(SUM(s.tonnage)::numeric, 0)                  AS total_tonnage,

    -- Metal contenido estimado (toneladas de cobre)
    ROUND(
        (SUM(s.tonnage) * AVG(s.sensor_cu_grade) / 100)::numeric
    , 2) AS estimated_cu_tons

FROM fact_sensor_readings s
LEFT JOIN fact_assay_results a ON s.reading_id = a.reading_id
WHERE s.qc_passed = TRUE
GROUP BY DATE_TRUNC('month', s.timestamp), s.bench_id
ORDER BY month, s.bench_id;


-- ============================================================
-- VISTA 4: Reporte de calidad de datos
-- Pregunta que responde: ¿Cuántos datos problemáticos hay?
-- ¿De qué equipos vienen los problemas?
-- ============================================================
CREATE OR REPLACE VIEW v_data_quality_report AS
SELECT
    equipment_id,
    COUNT(*)                                            AS total_readings,
    SUM(CASE WHEN is_duplicate THEN 1 ELSE 0 END)      AS duplicates,
    SUM(CASE WHEN is_outlier   THEN 1 ELSE 0 END)      AS outliers,
    SUM(CASE WHEN tonnage IS NULL THEN 1 ELSE 0 END)   AS null_tonnage,
    SUM(CASE WHEN NOT qc_passed THEN 1 ELSE 0 END)     AS failed_qc,
    ROUND(
        100.0 * SUM(CASE WHEN qc_passed THEN 1 ELSE 0 END) / COUNT(*)
    , 2)                                               AS qc_pass_rate_pct
FROM fact_sensor_readings
GROUP BY equipment_id
ORDER BY qc_pass_rate_pct ASC;
