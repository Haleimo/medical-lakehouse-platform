#!/usr/bin/env bash
# Run the full pipeline (ingest -> bronze -> silver -> gold) against the
# bundled sample FHIR data, entirely on the local filesystem (RUN_MODE=local,
# the default). No Docker, Kubernetes, or Spark cluster required.
set -euo pipefail
cd "$(dirname "$0")/.."

if [ -f .env ]; then
  set -a; source .env; set +a
fi
export RUN_MODE="${RUN_MODE:-local}"

echo "== 1/4: FHIR ingestion (Bronze + quarantine) =="
BATCH_OUTPUT=$(python3 -m src.ingestion.fhir_ingest data/sample/fhir)
echo "${BATCH_OUTPUT}"
# The first line is a structured JSON log record (see src/utils/logging_config.py)
# and already contains batch_id -- read it from there rather than the
# pretty-printed summary that follows, to avoid concatenating two JSON blobs.
BATCH_ID=$(echo "${BATCH_OUTPUT}" | head -n1 | python3 -c "import json,sys; print(json.load(sys.stdin)['batch_id'])")

echo
echo "== 2/4: DICOM metadata ingestion, same batch (Bronze + quarantine) =="
python3 -c "
from src.ingestion.dicom_ingest import run_dicom_ingestion
r = run_dicom_ingestion('data/sample/dicom', batch_id='${BATCH_ID}')
print(r.metrics.as_dict())
"

echo
echo "== 3/4: Spark transform, Bronze -> Silver (de-identify, dedupe, quality) =="
python3 spark/jobs/bronze_to_silver.py --batch-id "${BATCH_ID}"

echo
echo "== 4/4: Spark aggregation, Silver -> Gold =="
python3 spark/jobs/silver_to_gold.py --batch-id "${BATCH_ID}"

echo
echo "Pipeline complete for batch ${BATCH_ID}."
echo "  Bronze:      ${LOCAL_DATA_ROOT:-data}/bronze/${BATCH_ID}/"
echo "  Quarantine:  ${LOCAL_DATA_ROOT:-data}/quarantine/${BATCH_ID}/"
echo "  Silver:      ${LOCAL_DATA_ROOT:-data}/silver/${BATCH_ID}/"
echo "  Gold:        ${LOCAL_DATA_ROOT:-data}/gold/${BATCH_ID}/"
