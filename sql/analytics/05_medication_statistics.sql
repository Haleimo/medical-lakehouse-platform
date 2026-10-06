-- Most frequently prescribed medications
SELECT medication_display, prescription_count, patient_count
FROM iceberg.medical_gold.medication_summary
ORDER BY prescription_count DESC
LIMIT 20;

-- Active medication load per patient (polypharmacy signal)
SELECT patient_id, COUNT(*) AS active_medication_count
FROM iceberg.medical.medication_request
WHERE status = 'active'
GROUP BY patient_id
HAVING COUNT(*) >= 5
ORDER BY active_medication_count DESC;
