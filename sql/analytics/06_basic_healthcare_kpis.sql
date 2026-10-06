-- A handful of simple, commonly-tracked KPIs built entirely from Gold/Silver.

-- KPI 1: Emergency encounter rate
SELECT
    SUM(CASE WHEN class_code = 'EMER' THEN encounter_count ELSE 0 END) * 1.0
        / NULLIF(SUM(encounter_count), 0) AS emergency_encounter_rate
FROM iceberg.medical_gold.encounter_summary;

-- KPI 2: Average distinct active conditions per patient
SELECT AVG(condition_count) AS avg_conditions_per_patient
FROM iceberg.medical_gold.patient_summary;

-- KPI 3: Abnormal observation rate (High/Low interpretation vs Normal)
SELECT
    interpretation_code,
    COUNT(*) AS observation_count,
    COUNT(*) * 1.0 / SUM(COUNT(*)) OVER () AS pct_of_total
FROM iceberg.medical.observation
WHERE interpretation_code IS NOT NULL
GROUP BY interpretation_code;

-- KPI 4: Patients with polypharmacy (5+ active medications)
SELECT COUNT(*) AS polypharmacy_patient_count
FROM iceberg.medical_gold.patient_summary
WHERE medication_count >= 5;
