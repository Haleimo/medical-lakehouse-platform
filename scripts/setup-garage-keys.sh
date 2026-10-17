#!/usr/bin/env bash
# setup-garage-keys.sh - Initialize Garage S3 Layout, Buckets, & API Keys
set -euo pipefail

NAMESPACE="${1:-data-lake}"
POD_NAME=$(kubectl get pods -n "${NAMESPACE}" -l app.kubernetes.io/name=garage -o jsonpath='{.items[0].metadata.name}')

echo "==================================================================="
echo " Initializing Garage S3 Cluster Layout & Buckets on Pod: ${POD_NAME}"
echo "==================================================================="

# 1. Assign node to layout
NODE_ID=$(kubectl exec -n "${NAMESPACE}" "${POD_NAME}" -- /garage node status | grep -E "^[a-f0-9]" | head -n1 | awk '{print $1}')
kubectl exec -n "${NAMESPACE}" "${POD_NAME}" -- /garage layout assign "${NODE_ID}" -z zone1 -c 50G
kubectl exec -n "${NAMESPACE}" "${POD_NAME}" -- /garage layout apply --version 1

# 2. Create S3 buckets for the Medical Lakehouse (bronze/raw landing +
# quarantine + the Nessie/Iceberg warehouse bucket -- keep this list in sync
# with .env.example's BUCKET_RAW / BUCKET_BRONZE / BUCKET_QUARANTINE /
# WAREHOUSE_PATH).
kubectl exec -n "${NAMESPACE}" "${POD_NAME}" -- /garage bucket create nessie-warehouse || true
kubectl exec -n "${NAMESPACE}" "${POD_NAME}" -- /garage bucket create medical-raw || true
kubectl exec -n "${NAMESPACE}" "${POD_NAME}" -- /garage bucket create medical-bronze || true
kubectl exec -n "${NAMESPACE}" "${POD_NAME}" -- /garage bucket create medical-quarantine || true

# 3. Create a key for Nessie/Trino/the pipeline's S3 access
kubectl exec -n "${NAMESPACE}" "${POD_NAME}" -- /garage key new lakehouse-key || true
for bucket in nessie-warehouse medical-raw medical-bronze medical-quarantine; do
  kubectl exec -n "${NAMESPACE}" "${POD_NAME}" -- /garage bucket allow "${bucket}" --read --write --key lakehouse-key || true
done

echo "==================================================================="
echo " Garage S3 buckets and key 'lakehouse-key' created."
echo " IMPORTANT: this script deliberately does NOT print the key's secret"
echo " access key/secret key -- 'garage key new' already printed them once"
echo " above. Copy them into the medical-pipeline-secrets Secret yourself"
echo " (see k8s/secret.example.yaml) rather than piping them through a"
echo " script or committing them anywhere."
echo " Garage WebUI URL: http://garage-ui.dlake.local"
echo "==================================================================="
