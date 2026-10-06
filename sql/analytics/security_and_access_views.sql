-- Trino views supporting defense-in-depth access control on top of the
-- Silver layer (which is already de-identified -- see
-- src/deidentification/deidentify.py). These views are an EXTRA layer for
-- environments where even the hashed/generalized Silver data should be
-- restricted to a smaller audience than Gold aggregates.
--
-- Honesty note: earlier drafts of this file re-masked fields (raw MRN, raw
-- SSN, raw name) that, in this implementation, are never written to Silver
-- in the first place -- there is nothing left to mask. The view below only
-- operates on fields that genuinely still exist in iceberg.medical.patient.

CREATE OR REPLACE VIEW iceberg.medical.v_patient_research AS
SELECT
    id,
    gender,
    age_band,
    postal_code_prefix,
    country
FROM iceberg.medical.patient;
-- Intentionally excludes mrn_hash, birth_year, and state, on the assumption
-- that a broad "research" audience does not need even the pseudonymous MRN
-- hash or a birth year finer-grained than the existing age_band.

-- Access-audit logging: in production this would be backed by Trino's
-- built-in event listener / query logging (see
-- https://trino.io/docs/current/admin/event-listeners-http.html) writing to
-- a durable sink, NOT a SQL view. This project does not implement a real
-- event listener; the view below is left as a documented placeholder/example
-- of the query shape such a listener's log table would support, not a
-- working audit trail.
-- CREATE OR REPLACE VIEW iceberg.medical.v_patient_access_audit AS
--     SELECT accessing_user, access_time, operation_type, target_table
--     FROM <your configured Trino event-listener sink table>;
