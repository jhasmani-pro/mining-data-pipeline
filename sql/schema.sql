-- ============================================================
-- MINING DATA PIPELINE - Schema Principal
-- Base de datos: mining_db
-- Autor: tu nombre
-- Descripción: Modelo dimensional para control de ley (grade control)
--              Simula flujo de datos sensor-based sorting vs assay lab
-- ============================================================

-- Extensión para UUID (buena práctica en pipelines reales)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================
-- DIMENSIONES
-- ============================================================

-- DIM: Equipos de mina (palas, camiones)
CREATE TABLE IF NOT EXISTS dim_equipment (
    equipment_id    VARCHAR(20) PRIMARY KEY,
    equipment_type  VARCHAR(50) NOT NULL,   -- SHOVEL, TRUCK, DRILL
    model           VARCHAR(50),
    capacity_tons   NUMERIC(8,2),
    active          BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMP DEFAULT NOW()
);

-- DIM: Ubicaciones / Bancos de la mina
CREATE TABLE IF NOT EXISTS dim_location (
    location_id     SERIAL PRIMARY KEY,
    bench_id        VARCHAR(20) NOT NULL UNIQUE,
    bench_elevation INTEGER,                -- cota en metros sobre nivel del mar
    zone            VARCHAR(20),            -- zona geológica: OXIDE, SULFIDE, etc.
    created_at      TIMESTAMP DEFAULT NOW()
);

-- DIM: Tiempo (permite análisis por turno, mes, trimestre)
CREATE TABLE IF NOT EXISTS dim_time (
    time_id         SERIAL PRIMARY KEY,
    full_date       DATE NOT NULL UNIQUE,
    year            INTEGER NOT NULL,
    quarter         INTEGER NOT NULL,
    month           INTEGER NOT NULL,
    month_name      VARCHAR(20) NOT NULL,
    week            INTEGER NOT NULL,
    day_of_week     INTEGER NOT NULL,
    shift           VARCHAR(10)             -- DIA / NOCHE (calculado al insertar)
);

-- ============================================================
-- HECHOS (FACT TABLES)
-- ============================================================

-- FACT: Lecturas de sensor en tiempo real
CREATE TABLE IF NOT EXISTS fact_sensor_readings (
    reading_id          BIGINT PRIMARY KEY,
    timestamp           TIMESTAMP NOT NULL,
    equipment_id        VARCHAR(20) REFERENCES dim_equipment(equipment_id),
    bench_id            VARCHAR(20) REFERENCES dim_location(bench_id),
    x_coord             NUMERIC(10,2),
    y_coord             NUMERIC(10,2),
    sensor_cu_grade     NUMERIC(6,4),       -- % cobre medido por sensor
    tonnage             NUMERIC(8,2),
    -- Flags de calidad de dato (los llenará el pipeline de QC)
    is_outlier          BOOLEAN DEFAULT FALSE,
    is_duplicate        BOOLEAN DEFAULT FALSE,
    qc_passed           BOOLEAN DEFAULT NULL,
    loaded_at           TIMESTAMP DEFAULT NOW()
);

-- FACT: Resultados de assay de laboratorio
CREATE TABLE IF NOT EXISTS fact_assay_results (
    assay_id            BIGINT PRIMARY KEY,
    reading_id          BIGINT REFERENCES fact_sensor_readings(reading_id),
    sample_date         TIMESTAMP NOT NULL,
    result_date         TIMESTAMP NOT NULL,
    assay_cu_grade      NUMERIC(6,4),       -- % cobre medido en laboratorio
    turnaround_days     NUMERIC(4,1),       -- días entre muestra y resultado
    loaded_at           TIMESTAMP DEFAULT NOW()
);

-- ============================================================
-- ÍNDICES (performance en queries analíticos)
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_sensor_timestamp
    ON fact_sensor_readings(timestamp);

CREATE INDEX IF NOT EXISTS idx_sensor_equipment
    ON fact_sensor_readings(equipment_id);

CREATE INDEX IF NOT EXISTS idx_sensor_bench
    ON fact_sensor_readings(bench_id);

CREATE INDEX IF NOT EXISTS idx_assay_reading
    ON fact_assay_results(reading_id);

CREATE INDEX IF NOT EXISTS idx_assay_sample_date
    ON fact_assay_results(sample_date);

-- ============================================================
-- COMENTARIOS EN TABLAS (documentación dentro de la BD)
-- ============================================================

COMMENT ON TABLE fact_sensor_readings IS
    'Lecturas en tiempo real del sistema sensor-based (ej. MineSense ShovelSense).
     Cada fila = una carga de pala. Alta frecuencia, menor precisión que assay.';

COMMENT ON TABLE fact_assay_results IS
    'Resultados de laboratorio para muestras físicas extraídas de cargas.
     ~8% de cobertura. Alta precisión. Delay de 2-5 días hábiles.
     Referencia a fact_sensor_readings via reading_id (FK).';
