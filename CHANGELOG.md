# Changelog

Notable fixes to this repository, kept here (rather than only in commit
messages) because several of them were found by external review after the
test suite was already green -- the point of writing them down is that each
one now has a regression test, so the same defect failing silently again is
the thing to watch for in review, not just "does `pytest` pass."

## Round 2 -- fixes from a second, more adversarial review

Found by re-running the pipeline end to end (including a real `airflow dags
test` run, which the first round only import-tested) and by checking every
factual claim in `docs/SYSTEM_DESIGN.md` and the diagrams against the code
that was supposed to back it up.

1. **DICOM was documented as flowing through the pipeline; it did not.**
   `dags/medical_data_pipeline.py` and `spark/jobs/bronze_to_silver.py` never
   called `src/ingestion/dicom_ingest.py`, and no Spark code ever wrote a
   `dicom_metadata` Silver table -- `docs/diagrams/erd.svg` documented one
   anyway. Fixed: `extract_validate_fhir` now also runs
   `run_dicom_ingestion` (combined invalid-ratio gate with FHIR), and
   `bronze_to_silver.py` parses, de-identifies, and writes `dicom_metadata`
   alongside the FHIR tables.
   Test: `tests/test_spark_jobs.py::test_end_to_end_with_edge_cases` asserts
   `dicom_metadata` row count in the Spark job's own summary.

2. **The ERD's DICOM join key was false.** It claimed
   `dicom_metadata.patient_id = patient.mrn_hash`, but `patient_id` was
   populated from the FHIR *resource id* (`pat-1001`), hashed with a
   different input than `mrn_hash` (which hashes the MRN) -- the two never
   matched. Fixed at the data level: the sample DICOM fixture now carries the
   MRN in `patient_id` (matching real DICOM tag 0010,0020 semantics, where
   PatientID conventionally *is* the MRN), so hashing it with the same
   function as `mrn_hash` produces the same value.
   Test: `tests/test_deidentification.py::test_dicom_patient_id_joins_to_patient_mrn_hash`
   and `::test_sample_dicom_records_link_to_sample_patients`.

3. **That join was undeclared as a foreign key, and unenforced.** Once (2)
   made the link real, `bronze_to_silver.py` was extended to check it:
   `dicom_metadata.patient_id` with no matching `patient.mrn_hash` is
   quarantined, same as any other dangling reference. The ERD badge changed
   from `LK` ("logical, not enforced") to `FK`, and the legend's now-unused
   `LK` entry was removed.
   Test: `tests/test_silver_checks.py::test_dicom_must_link_to_a_patient_mrn_hash`,
   `tests/test_spark_jobs.py` (dicom join assertion).

4. **A DICOM record missing its own key was still accepted.** The
   round-1 fix to DICOM validation (below, round 1 item 2) added an
   `id_field` parameter but the rejection path still matched rejected
   records by parsing `id=...` back out of an error *string*. A record
   missing `sop_instance_uid` itself produced `id=None` in its error
   message, matched nothing, was accepted into Bronze, and would then throw
   a `KeyError` in the write step. Fixed by replacing string-parsed
   rejection matching everywhere with structured findings
   (`src/quality/checks.RowIssue`, returned by the new
   `find_missing_required` / `find_referential_violations`) that carry a row
   index and record id as real fields, not text to re-parse.
   Test: `tests/test_dicom_ingest.py::test_record_missing_its_own_key_is_rejected`.

5. **Rows failing referential-integrity checks were dropped, not
   quarantined**, despite `docs/SYSTEM_DESIGN.md` saying they were. They were
   logged at WARNING and discarded. Fixed:
   `quarantine_referential_failures` now returns the removed rows (with which
   FK failed and why) and `persist_quarantine` writes each one through the
   same `write_quarantine_record` helper ingestion uses, so every rejection
   in this pipeline -- validation or referential -- has the same
   inspectable shape on disk.
   Test: `tests/test_spark_jobs.py::test_end_to_end_with_edge_cases` reads
   the quarantine files back and checks their contents; the pipeline crash
   this masked is covered by
   `test_quality_gate_blocks_silver_when_too_many_rows_are_quarantined`.

6. **Only `patient_id` was ever checked; `encounter_id` was not,** though the
   ERD marked it `FK (pipeline-enforced)` on three tables. Fixed: a second
   referential pass in `bronze_to_silver.py` checks `encounter_id` against
   the encounters that survived the first pass.
   Test: `tests/test_spark_jobs.py::test_end_to_end_with_edge_cases`
   (`obs-orphan-encounter` case).

7. **`code_loinc` (round 1's fix, below) nulled the code for any non-LOINC
   observation, and `pandas.groupby` drops rows with a null group key by
   default** -- so a non-LOINC or system-less observation present in Silver
   silently disappeared from every Gold aggregate keyed on that column.
   Fixed at the schema level, not just the null-handling level: replaced the
   single `code_loinc` column with `code_system` + `code` (the code is never
   discarded, regardless of system), and added `dropna=False` to every
   `groupby` in `src/transformations/gold.py` so a null grouping key gets its
   own group instead of vanishing.
   Test: `tests/test_gold_transformations.py::test_no_rows_lost_for_observations_without_a_coding_system`
   and `::test_no_rows_lost_for_null_group_keys_in_other_summaries`.

8. **`spark/jobs/bronze_to_silver.py` used `.createOrReplace()` for Silver**,
   which in cluster mode overwrites the whole table on every run -- so Silver
   would only ever contain the most recent batch, contradicting the
   "idempotent, incremental" description in `docs/SYSTEM_DESIGN.md`. Fixed:
   `spark/jobs/common.py:upsert_iceberg_table` creates the table on first
   write and issues `MERGE INTO ... ON <key> WHEN MATCHED UPDATE ... WHEN NOT
   MATCHED INSERT` afterwards. Gold is left as a full replace
   (`replace_iceberg_table`), which is correct there because Gold is always
   a full recompute from Silver, not an accumulation.
   Test: `tests/test_iceberg_io.py` (mocked -- proves the create-then-merge
   *sequencing*, not that the generated SQL runs against a live catalog: see
   `docs/SYSTEM_DESIGN.md` section 10); `tests/test_spark_jobs.py::test_rerunning_a_batch_is_idempotent`
   for the local-mode, file-level version of the same guarantee.

9. **Documentation/diagram hygiene**: `docs/diagrams/architecture.svg` listed
   "Docker images (Airflow, Spark)" as two images; there is one image running
   both. `.gitignore` and `.github/workflows/ci.yml` were missing from the
   delivered archive. `.pytest_cache/`/`.ruff_cache/` were committed. This
   changelog was pulled out of `README.md` into its own file.

## Round 1 -- fixes made before external review

1. **`src/fhir/parser.py`** -- `code_loinc` was set to `code_code`
   regardless of whether the coding system was actually LOINC (both branches
   of the `if`/`else` returned the same value), so non-LOINC codes were
   mislabeled as LOINC. Superseded by round 2 item 7 (the column no longer
   exists; see `code_system`/`code`).
2. **`src/ingestion/dicom_ingest.py`** -- invalid DICOM metadata was never
   quarantined: rejection matching parsed an `id=...` token out of an error
   string, but DICOM records have no `id` field, so the filter never matched
   and bad records were accepted into Bronze. Fixed by adding an `id_field`
   parameter -- itself superseded by round 2 item 4's structured-findings
   rewrite, which removed string-parsed matching entirely.
3. **`dags/medical_data_pipeline.py`** -- the `data_quality` task globbed
   Silver for `*.json`, but Silver is Parquet; the task always saw zero
   records and the quality gate silently no-op'd. Fixed to read Parquet;
   the check itself was later moved to `src/quality/silver_checks.py` (round
   2) so it could be unit tested directly instead of only through Airflow.
4. **`src/ingestion/dicom_ingest.py`** -- a no-op
   `bucket = x if cond else x` copy-paste artifact; simplified.

All 80 tests pass (74 + 1 skipped without `apache-airflow` installed) and
`ruff check .` is clean as of both rounds; see `README.md` section 17 for how
to run them.
