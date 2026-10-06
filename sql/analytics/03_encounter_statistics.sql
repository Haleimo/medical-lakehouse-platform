-- Encounters by class and status
SELECT class_code, status, encounter_count
FROM iceberg.medical_gold.encounter_summary
ORDER BY encounter_count DESC;

-- Encounter volume per patient (top 20 highest utilizers -- a common
-- population-health KPI)
SELECT patient_id, COUNT(*) AS encounter_count
FROM iceberg.medical.encounter
GROUP BY patient_id
ORDER BY encounter_count DESC
LIMIT 20;
