-- Silver layer: MedicationRequest table
-- Written by spark/jobs/bronze_to_silver.py (table: iceberg.medical.medication_request)
CREATE TABLE IF NOT EXISTS iceberg.medical.medication_request (
    id                    VARCHAR,
    status                VARCHAR COMMENT 'active | on-hold | cancelled | completed | entered-in-error | stopped | draft | unknown',
    intent                VARCHAR COMMENT 'proposal | plan | order | ...',
    medication_code       VARCHAR COMMENT 'RxNorm code',
    medication_display    VARCHAR,
    patient_id            VARCHAR COMMENT 'Foreign key -> patient.id',
    encounter_id          VARCHAR COMMENT 'Foreign key -> encounter.id',
    authored_on           VARCHAR,
    dosage_text           VARCHAR,
    batch_id              VARCHAR,
    source_file           VARCHAR,
    resource_type         VARCHAR
)
WITH (
    format = 'PARQUET',
    location = '${WAREHOUSE_PATH}medical/medication_request/'
);
