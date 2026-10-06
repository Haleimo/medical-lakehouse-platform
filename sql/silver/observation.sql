-- Silver layer: Observation table (vitals, labs, etc.)
-- Written by spark/jobs/bronze_to_silver.py (table: iceberg.medical.observation)
CREATE TABLE IF NOT EXISTS iceberg.medical.observation (
    id                    VARCHAR,
    status                VARCHAR COMMENT 'registered | preliminary | final | amended | corrected | cancelled | entered-in-error | unknown',
    category_code         VARCHAR COMMENT 'vital-signs | laboratory | imaging | procedure',
    category_display      VARCHAR,
    code_system           VARCHAR COMMENT 'Coding system URI, e.g. http://loinc.org (kept so non-LOINC codes are not lost)',
    code                  VARCHAR COMMENT 'Code within code_system (LOINC code when code_system = http://loinc.org)',
    code_display          VARCHAR,
    patient_id            VARCHAR COMMENT 'Foreign key -> patient.id',
    encounter_id          VARCHAR COMMENT 'Foreign key -> encounter.id',
    effective_datetime    TIMESTAMP(6) COMMENT 'Cast from the ISO-8601 string produced by the Spark job',
    value_quantity_value  DOUBLE,
    value_quantity_unit   VARCHAR,
    value_string          VARCHAR,
    interpretation_code   VARCHAR COMMENT 'N (Normal) | H (High) | L (Low)',
    batch_id              VARCHAR,
    source_file           VARCHAR,
    resource_type         VARCHAR
)
WITH (
    format = 'PARQUET',
    -- Observation is event/time-series data at high volume in a real system;
    -- almost every analytical query filters on a date range, so partitioning
    -- by day gives real partition pruning. category_code is deliberately NOT
    -- also used for partitioning: it only has ~4 distinct values, which (like
    -- gender on Patient) would not meaningfully improve pruning.
    partitioning = ARRAY['day(effective_datetime)'],
    location = '${WAREHOUSE_PATH}medical/observation/'
);
