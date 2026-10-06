-- Gold layer: analytics/BI-ready aggregate tables.
-- Written by spark/jobs/silver_to_gold.py (see src/transformations/gold.py
-- for the aggregation logic, unit tested in tests/test_gold_transformations.py).
--
-- These are intentionally NOT copies of Silver -- each is aggregated to the
-- grain a dashboard would actually query (per patient, per day+code, per
-- class/status, per condition/medication code).

CREATE TABLE IF NOT EXISTS iceberg.medical_gold.patient_summary (
    patient_id         VARCHAR,
    gender             VARCHAR,
    age_band           VARCHAR,
    birth_year         INTEGER,
    encounter_count    BIGINT,
    condition_count    BIGINT,
    medication_count   BIGINT
)
WITH (
    format = 'PARQUET',
    location = '${WAREHOUSE_PATH}medical_gold/patient_summary/'
);

CREATE TABLE IF NOT EXISTS iceberg.medical_gold.observation_daily_summary (
    observation_date    DATE,
    code_system         VARCHAR,
    code                VARCHAR,
    code_display        VARCHAR,
    observation_count   BIGINT,
    avg_value           DOUBLE,
    min_value           DOUBLE,
    max_value           DOUBLE
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(observation_date)'],
    location = '${WAREHOUSE_PATH}medical_gold/observation_daily_summary/'
);

CREATE TABLE IF NOT EXISTS iceberg.medical_gold.encounter_summary (
    class_code         VARCHAR,
    status             VARCHAR,
    encounter_count    BIGINT
)
WITH (
    format = 'PARQUET',
    location = '${WAREHOUSE_PATH}medical_gold/encounter_summary/'
);

CREATE TABLE IF NOT EXISTS iceberg.medical_gold.condition_summary (
    code               VARCHAR,
    code_display       VARCHAR,
    condition_count    BIGINT,
    patient_count      BIGINT
)
WITH (
    format = 'PARQUET',
    location = '${WAREHOUSE_PATH}medical_gold/condition_summary/'
);

CREATE TABLE IF NOT EXISTS iceberg.medical_gold.medication_summary (
    medication_code       VARCHAR,
    medication_display    VARCHAR,
    prescription_count    BIGINT,
    patient_count         BIGINT
)
WITH (
    format = 'PARQUET',
    location = '${WAREHOUSE_PATH}medical_gold/medication_summary/'
);
