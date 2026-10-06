#!/usr/bin/env bash
# Practical (not automated/scheduled) backup script for the lakehouse.
#
# What actually needs backing up:
#   1. Object data:        the Garage S3 buckets (medical-raw, medical-bronze,
#      medical-quarantine, nessie-warehouse) -- this is where every Parquet
#      file and every raw Bronze JSON record lives.
#   2. Iceberg/Nessie metadata: Nessie's own backing store (JDBC/Postgres in
#      a real deployment, in-memory in the docker-compose demo -- see
#      docker-compose.yml's NESSIE_VERSION_STORE_TYPE). This is what makes
#      table snapshots/branches/tags resolvable; losing it without a backup
#      of the underlying DB is effectively losing the catalog even if the
#      Parquet files on S3 are intact.
#   3. Airflow metadata DB (Postgres): DAG run history, task state.
#
# This script demonstrates (1): a full copy of the Garage buckets to a local
# (or second remote) target using the AWS CLI's S3-compatible mode. It does
# NOT back up Nessie's or Airflow's Postgres databases -- that would use
# standard `pg_dump`/`pg_basebackup` against whichever Postgres instance you
# point Nessie/Airflow at in a real deployment, and is intentionally left as
# a documented next step (see README "Known limitations" -- this project
# does not ship a production Postgres deployment to back up in the first
# place; the Nessie in docker-compose.yml runs IN_MEMORY for demo purposes).
set -euo pipefail

: "${S3_ENDPOINT:?set S3_ENDPOINT}"
: "${S3_ACCESS_KEY:?set S3_ACCESS_KEY}"
: "${S3_SECRET_KEY:?set S3_SECRET_KEY}"
BACKUP_DEST="${1:?usage: backup_iceberg_warehouse.sh <destination-dir-or-s3-uri>}"

export AWS_ACCESS_KEY_ID="${S3_ACCESS_KEY}"
export AWS_SECRET_ACCESS_KEY="${S3_SECRET_KEY}"

for bucket in medical-raw medical-bronze medical-quarantine nessie-warehouse; do
  echo "Backing up bucket: ${bucket}"
  aws --endpoint-url "${S3_ENDPOINT}" s3 sync "s3://${bucket}" "${BACKUP_DEST}/${bucket}"
done

echo "Backup complete: ${BACKUP_DEST}"
echo "RPO/RTO NOTE: this is a manual, on-demand script, not a scheduled job."
echo "Current RPO = time since you last ran this script. Current RTO = however"
echo "long it takes you to 'aws s3 sync' the backup back and re-point Nessie/"
echo "Trino at it -- untested/unmeasured in this repository."
