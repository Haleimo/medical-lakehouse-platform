-- Total de-identified patients on file
SELECT COUNT(*) AS total_patients
FROM iceberg.medical.patient;

-- Patients by gender (low-cardinality breakdown -- this is exactly the kind
-- of query that does NOT benefit from partitioning by gender; see
-- sql/silver/patient.sql for why we partition by age_band instead)
SELECT gender, COUNT(*) AS patient_count
FROM iceberg.medical.patient
GROUP BY gender
ORDER BY patient_count DESC;

-- Age distribution (already de-identified into 10-year bands)
SELECT age_band, COUNT(*) AS patient_count
FROM iceberg.medical.patient
GROUP BY age_band
ORDER BY age_band;
