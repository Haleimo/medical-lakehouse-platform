# Bronze layer

Bronze is raw, per-resource JSON (original FHIR resource + ingestion
metadata: `ingestion_timestamp`, `source_file`, `batch_id`) written by
`src/ingestion/fhir_ingest.py` directly to object storage
(`s3://medical-bronze/<batch_id>/<ResourceType>/<id>.json` in cluster mode,
or `data/bronze/<batch_id>/...` in local dev mode).

Bronze is intentionally **not** modeled as Iceberg tables and is not queried
directly through Trino:
  * it still contains raw PHI (that's the point -- it's the untouched
    original record, kept so ingestion is reproducible/auditable), so it must
    stay behind stricter access controls than Silver/Gold (see the root
    README's "Security" section)
  * it is one-JSON-object-per-file, which is a poor fit for a Trino/Iceberg
    table -- Spark reads it directly (`spark/jobs/bronze_to_silver.py`) and
    is responsible for turning it into the flat, typed Silver tables that
    *are* registered in Nessie and queryable from Trino.

If ad-hoc querying of Bronze is ever needed, a `hive` or generic JSON
connector could be pointed at the same S3 path in Trino as a Bronze
"external table" -- this is documented here as a future option, not
implemented, since it isn't needed by the current pipeline.
