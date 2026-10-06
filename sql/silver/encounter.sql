-- Silver layer: Encounter table
-- Written by spark/jobs/bronze_to_silver.py (table: iceberg.medical.encounter)
CREATE TABLE IF NOT EXISTS iceberg.medical.encounter (
    id                VARCHAR,
    status            VARCHAR COMMENT 'planned | arrived | triaged | in-progress | onleave | finished | cancelled | entered-in-error | unknown',
    class_code        VARCHAR COMMENT 'AMB | EMER | IMP | ... (FHIR Encounter.class)',
    type_code         VARCHAR,
    type_display      VARCHAR,
    patient_id        VARCHAR COMMENT 'Foreign key -> patient.id',
    period_start      TIMESTAMP(6),
    period_end        TIMESTAMP(6),
    service_provider  VARCHAR,
    batch_id          VARCHAR,
    source_file       VARCHAR,
    resource_type     VARCHAR
)
WITH (
    format = 'PARQUET',
    -- Not partitioned at demo scale. If encounter volume grows, partition by
    -- day(period_start) for the same reason as observation.effective_datetime.
    location = '${WAREHOUSE_PATH}medical/encounter/'
);
