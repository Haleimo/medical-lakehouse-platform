-- Daily observation volume for a given LOINC code over the last 30 days
SELECT observation_date, code_display, observation_count, avg_value
FROM iceberg.medical_gold.observation_daily_summary
WHERE code_system = 'http://loinc.org'
  AND code = '8480-6'  -- Systolic blood pressure
  AND observation_date >= current_date - INTERVAL '30' DAY
ORDER BY observation_date;

-- Which observation codes are most frequently recorded
SELECT code_system, code, code_display, SUM(observation_count) AS total_observations
FROM iceberg.medical_gold.observation_daily_summary
GROUP BY code_system, code, code_display
ORDER BY total_observations DESC
LIMIT 20;

-- Ad-hoc query directly against Silver (bypassing the Gold aggregate)
SELECT status, category_code, COUNT(*) AS observation_count
FROM iceberg.medical.observation
GROUP BY status, category_code;
