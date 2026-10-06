-- Most common conditions across the de-identified population
SELECT code_display, condition_count, patient_count
FROM iceberg.medical_gold.condition_summary
ORDER BY patient_count DESC
LIMIT 20;

-- Conditions per patient (co-morbidity signal)
SELECT patient_id, COUNT(DISTINCT code) AS distinct_condition_count
FROM iceberg.medical.condition
WHERE clinical_status = 'active'
GROUP BY patient_id
ORDER BY distinct_condition_count DESC;
