-- Silver layer: de-identified Patient table
-- Written by spark/jobs/bronze_to_silver.py (table: iceberg.medical.patient)
--
-- PHI handling: mrn is stored only as a salted SHA-256 hash (mrn_hash);
-- ssn, first_name, last_name, phone, email, address_line, city and the exact
-- birth_date are NEVER written here -- see src/deidentification/deidentify.py
-- and the README "De-identification" section for the full field-by-field policy.
CREATE TABLE IF NOT EXISTS iceberg.medical.patient (
    id                  VARCHAR COMMENT 'FHIR Patient resource id (not PHI on its own; used as the join key for all other tables)',
    active              BOOLEAN,
    gender              VARCHAR COMMENT 'male | female | other | unknown',
    deceased_boolean    BOOLEAN,
    deceased_datetime   VARCHAR,
    state               VARCHAR,
    country             VARCHAR,
    version_id          VARCHAR,
    last_updated        VARCHAR,
    mrn_hash            VARCHAR COMMENT 'Deterministic salted SHA-256 hash of the original MRN',
    birth_year          INTEGER COMMENT 'Generalized from birthDate; the exact date is discarded',
    age_band            VARCHAR COMMENT '10-year age band, e.g. 40-49; 90+ is a single top band (HIPAA Safe Harbor)',
    postal_code_prefix  VARCHAR COMMENT '3-digit ZIP prefix (HIPAA Safe Harbor generalization)',
    batch_id            VARCHAR COMMENT 'Ingestion batch id, for lineage/debugging',
    source_file         VARCHAR COMMENT 'Original source file, for lineage/debugging',
    resource_type       VARCHAR
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['age_band'],
    location = '${WAREHOUSE_PATH}medical/patient/'
);
