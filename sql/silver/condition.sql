-- Silver layer: Condition table
-- Written by spark/jobs/bronze_to_silver.py (table: iceberg.medical.condition)
CREATE TABLE IF NOT EXISTS iceberg.medical.condition (
    id                    VARCHAR,
    clinical_status       VARCHAR COMMENT 'active | recurrence | relapse | inactive | remission | resolved',
    verification_status   VARCHAR COMMENT 'unconfirmed | provisional | differential | confirmed | refuted | entered-in-error',
    code                  VARCHAR COMMENT 'SNOMED CT code',
    code_display          VARCHAR,
    patient_id            VARCHAR COMMENT 'Foreign key -> patient.id',
    encounter_id          VARCHAR COMMENT 'Foreign key -> encounter.id',
    onset_datetime        VARCHAR,
    recorded_date         VARCHAR,
    batch_id              VARCHAR,
    source_file           VARCHAR,
    resource_type         VARCHAR
)
WITH (
    format = 'PARQUET',
    location = '${WAREHOUSE_PATH}medical/condition/'
);
