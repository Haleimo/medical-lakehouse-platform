# Silver layer -- Iceberg DDL

These tables are written by `spark/jobs/bronze_to_silver.py`. Column names and
types here match the explicit PySpark schemas in that job
(`_silver_schemas()`) exactly -- if you change one, change the other.

All PHI has already been removed/hashed/generalized before rows reach these
tables (see `src/deidentification/deidentify.py` and the
"De-identification" section of the root README). Silver tables are safe to
expose to analysts/BI tools without additional masking.

Partitioning notes (see project README "Iceberg" section for the full
rationale):
  * `patient`: partitioned by `age_band` (a handful of coarse buckets) rather
    than `gender`, which the original skeleton used -- gender is extremely
    low-cardinality (effectively 2-4 values) and partitioning on it does not
    meaningfully prune scans, it just creates a handful of huge partitions.
  * `observation`: partitioned by `day(effective_datetime)` since Observation
    is an event-style, time-series table and almost every analytical query
    filters by a date range.
  * `encounter` / `condition` / `medication_request`: not partitioned in this
    demo-scale dataset; documented here as a candidate for
    `day(period_start)` / `day(recorded_date)` / `day(authored_on)` once data
    volume justifies it.

## Known type gap between local dev mode and this DDL

`RUN_MODE=local` (the default, used for tests and the sample pipeline run)
writes date/time fields as plain strings to keep the local Parquet path
simple and dependency-free. The DDL in this directory documents the intended
**production** Iceberg schema, where date/time columns are proper
`TIMESTAMP`/`DATE` types (required for `day(...)` partition transforms to
work, and for correct range-filtering in Trino). A cluster-mode Spark job
would add an explicit `.withColumn("effective_datetime", to_timestamp(...))`
style cast before the Iceberg write. This gap is called out again in the
root README's "Known limitations" section -- it is not hidden.
