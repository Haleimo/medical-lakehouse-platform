# System Design: Medical Data Lakehouse Platform

## 1. Purpose and scope

A batch data platform that ingests healthcare data (FHIR resources and DICOM
imaging **metadata**), removes or pseudonymizes PHI, and publishes analytics-ready
tables through a lakehouse (Iceberg + Nessie + Trino).

In scope: batch ingestion, validation, de-identification, medallion modelling,
data-quality gates, orchestration, GitOps deployment.
Out of scope: streaming ingestion, DICOM pixel data, a bundled BI tool, real-time serving.

## 2. Architecture at a glance

| Diagram | File |
|---|---|
| System architecture (components) | `docs/diagrams/architecture.svg` |
| ETL / data flow (medallion + quality gates) | `docs/diagrams/etl_architecture.svg` |
| ERD, Silver layer | `docs/diagrams/erd.svg` |
| Gold data marts and lineage | `docs/diagrams/gold_model.svg` |

## 3. Components and why they were chosen

| Layer | Choice | Reason |
|---|---|---|
| Orchestration | Apache Airflow | Explicit task dependencies, retries, scheduling, UI for reruns |
| Processing | PySpark | Scales the same code from `local[*]` to a cluster |
| Table format | Apache Iceberg | ACID writes, schema evolution, hidden partitioning |
| Catalog | Project Nessie | Git-like versioning of table state (branches/commits) |
| Object storage | Garage (S3-compatible) | Lightweight self-hosted S3 for Kubernetes |
| Query engine | Trino | Interactive SQL directly on Iceberg, no data copy |
| Deployment | Docker, Kubernetes, Helm, ArgoCD | Declarative, repeatable, Git as source of truth |

Business logic lives in `src/` (pure Python, unit tested). Airflow and Spark
jobs are thin wrappers, so the pipeline can run without Airflow
(`scripts/run_pipeline_local.sh`) and be tested in isolation.

## 4. Data flow

1. **Ingest and validate** (`src/ingestion/fhir_ingest.py`,
   `src/ingestion/dicom_ingest.py`, `src/fhir/validator.py`): each FHIR
   resource and each DICOM metadata record is validated in the same Airflow
   task, against a combined invalid-ratio threshold; invalid ones go to
   Quarantine with a reason, valid ones land in Bronze with `batch_id` and
   ingestion timestamp.
2. **Transform** (`spark/jobs/bronze_to_silver.py`): parse to flat rows,
   de-identify, deduplicate by natural key, then check referential integrity
   in two passes -- `patient_id` (and, for DICOM, `patient_id` against
   `patient.mrn_hash`) first, then `encounter_id` against the *surviving*
   encounters. Violating rows are **quarantined as full records** (not just
   logged) with the specific FK and message, via the same
   `write_quarantine_record` helper ingestion uses, so a referential failure
   looks the same as a validation failure to anyone reading Quarantine. The
   quality gate below runs on the quarantined count *before* Silver is
   written, so a batch that fails the gate leaves no partial Silver output.
3. **Quality gate** (`src/quality/checks.py`): required fields, forbidden (PHI)
   fields must be absent, and the invalid ratio must stay under
   `MAX_INVALID_RATIO`, otherwise the run fails with `DataQualityError`.
4. **Silver**: de-identified, one row per resource (see ERD).
5. **Aggregate** (`spark/jobs/silver_to_gold.py`, `src/transformations/gold.py`):
   Gold marts at dashboard grain.
6. **Serve**: Trino queries over `sql/analytics/*.sql`.

## 5. Data model

* **Bronze**: raw resource JSON plus ingestion metadata; immutable, replayable.
* **Silver** (`iceberg.medical`): `patient`, `encounter`, `observation`,
  `condition`, `medication_request`, `dicom_metadata`. `patient.id` is the join
  key for FHIR tables; `patient.mrn_hash` is the join key for `dicom_metadata`.
  Iceberg does not enforce foreign keys -- the pipeline does, in
  `spark/jobs/bronze_to_silver.py:quarantine_referential_failures` (see
  Data flow, step 2), and `src/quality/silver_checks.py` re-checks the same
  three relationships after the fact as the `data_quality` DAG task.
* **Gold** (`iceberg.medical_gold`): `patient_summary`,
  `observation_daily_summary`, `encounter_summary`, `condition_summary`,
  `medication_summary`.
* **Partitioning**: `observation` by `day(effective_datetime)` (time-range
  queries), `patient` by `age_band`, `dicom_metadata` by `modality`.

## 6. Privacy and security design

* PHI removed before Silver: names, SSN, phone, email, address, exact birth date.
* MRN stored only as a salted SHA-256 hash (`mrn_hash`); birth date generalized
  to `birth_year` and `age_band`; ZIP reduced to a 3-digit prefix.
* A PHI-leak assertion runs before Silver is written.
* DICOM `patient_id` (DICOM tag 0010,0020, which conventionally carries the
  MRN) is hashed with the same salted function as `patient.mrn_hash`, and the
  link is **pipeline-enforced**: a DICOM record whose hashed `patient_id`
  doesn't match any `patient.mrn_hash` is quarantined, the same as a dangling
  `patient_id`/`encounter_id` on any FHIR table (see `erd.svg`'s legend --
  this used to be documented as a "logical, not enforced" link; it wasn't
  actually enforced until this was reviewed, at which point the check was
  added rather than the documentation being loosened to match the code).
* Secrets come from environment/Kubernetes Secrets; `k8s/secret.example.yaml` is a
  template only and real values are never committed.

## 7. Reliability and quality

* Airflow tasks: `retries=2`, 5 minute delay, 1 hour timeout.
* Idempotency differs by mode: in **cluster** mode, `bronze_to_silver.py`
  creates each Iceberg table on first write and `MERGE INTO ... ON <key>`
  afterwards (`spark/jobs/common.py:upsert_iceberg_table`), so Silver
  accumulates across batches and re-running a batch updates rows in place
  rather than duplicating or wiping them -- confirmed by
  `tests/test_spark_jobs.py::test_rerunning_a_batch_is_idempotent` and the
  mocked sequencing in `tests/test_iceberg_io.py` (not against a live
  Nessie catalog, see section 10). Gold is a full recompute from Silver each
  run (`replace_iceberg_table`), which is correct because it isn't additive.
  In **local** dev mode, Silver/Gold are plain per-`batch_id` Parquet
  directories (`data/silver/<batch_id>/...`) with no merge step -- each batch
  is independent and nothing accumulates; this is a simplification for a
  fast, dependency-light dev loop, not the production behaviour.
* Bad data never blocks good data: invalid records (validation failures,
  DICOM records, and now referential-integrity failures) are all quarantined
  as full, inspectable records, and the run only fails when the invalid ratio
  crosses `MAX_INVALID_RATIO`.
* Tests: 80 tests with Airflow installed (74 + 1 skipped without it) across
  unit tests (parsing, validation, de-identification, quality checks, Gold
  aggregation, quarantine persistence), a DICOM ingestion suite, a
  Silver-layer quality suite that asserts checks actually *find* problems
  (not just that they run), mocked Iceberg write-sequencing tests, real
  PySpark integration tests exercising orphan patient/encounter references,
  non-LOINC observations, the DICOM/mrn_hash join, and the quality gate, and
  6 DAG-import/structure tests. See `CHANGELOG.md` for the specific defects
  each regression test was written against.

## 8. Deployment (GitOps)

Git is the source of truth; ArgoCD reconciles the cluster to it.

```
Git repo
  argocd/root-app.yaml        "app of apps": watches argocd/apps/
  argocd/apps/
    garage-storage.yaml       -> charts/garage (this repo)
    nessie-catalog.yaml       -> upstream Nessie Helm chart
    trino-query.yaml          -> upstream Trino Helm chart
    airflow-orchestration.yaml-> upstream Airflow Helm chart
```

All apps use automated sync with `prune` and `selfHeal`, so manual drift in the
cluster is reverted to what Git says.

## 9. Scalability notes

* Spark scales horizontally by moving from `local[*]` to a cluster master
  (`k8s/spark-job-template.yaml`).
* Iceberg hidden partitioning and Trino give pruning for date-range queries.
* Object storage decouples storage from compute; each can scale on its own.

## 10. Known limitations and next steps

* Batch only; streaming (Kafka + Spark Structured Streaming) is a future step.
* No BI tool bundled (Superset/Metabase would sit on Trino).
* Compose and Helm assets were reviewed but not executed end to end in the
  build sandbox; run `helm lint` and a smoke test before production use.
* The Iceberg `MERGE INTO` upsert path (section 7) is unit tested with mocks,
  not against a live Nessie/Iceberg catalog -- the SQL it generates has not
  been run for real. Verify it against a real cluster before relying on it.
* Local dev mode (`RUN_MODE=local`) does not accumulate Silver/Gold across
  batches the way cluster mode does (section 7) -- don't use it to reason
  about multi-batch behaviour.
* Add data-contract tests and lineage (OpenLineage) as next improvements.
