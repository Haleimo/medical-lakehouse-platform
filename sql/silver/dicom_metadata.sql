-- Silver layer: de-identified DICOM metadata
-- Scope note: this project processes DICOM METADATA only, not pixel data.
-- Written by spark/jobs/bronze_to_silver.py from Bronze records produced by
-- src/ingestion/dicom_ingest.py. patient_name is removed, patient_birth_date is
-- generalized to patient_birth_year, and patient_id (DICOM PatientID, which
-- conventionally carries the MRN) is replaced by the salted SHA-256 hash --
-- the same function that produces patient.mrn_hash, so:
--     dicom_metadata.patient_id = patient.mrn_hash
-- The Spark job enforces this link (non-matching rows are quarantined).
CREATE TABLE IF NOT EXISTS iceberg.medical.dicom_metadata (
    sop_instance_uid     VARCHAR,
    series_instance_uid  VARCHAR,
    study_instance_uid   VARCHAR,
    patient_id           VARCHAR COMMENT 'Salted SHA-256 of the MRN; FK -> patient.mrn_hash (pipeline-enforced)',
    modality             VARCHAR COMMENT 'CT | MRI | X-RAY | US | PET',
    body_part_examined   VARCHAR,
    study_date           DATE,
    manufacturer         VARCHAR,
    institution_name     VARCHAR,
    rows                 INTEGER,
    columns              INTEGER,
    pixel_spacing        VARCHAR,
    slice_thickness      DOUBLE,
    patient_birth_year   INTEGER COMMENT 'Generalized from DICOM (0010,0030) PatientBirthDate',
    batch_id             VARCHAR
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['modality'],
    location = '${WAREHOUSE_PATH}medical/dicom_metadata/'
);
