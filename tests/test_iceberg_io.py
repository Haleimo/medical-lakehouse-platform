"""Sequencing tests for the Iceberg write strategies, using mocks (no JVM/catalog).

These prove the create-then-MERGE / replace logic; they cannot prove the MERGE
works against a live Nessie catalog -- that still needs a real cluster.
"""
from unittest.mock import MagicMock, patch

import pytest

from spark.jobs import common


@pytest.fixture(autouse=True)
def _no_spark_transforms():
    # prepare_for_iceberg / partition exprs need pyspark; stub them so the
    # sequencing logic is what's under test.
    with patch.object(common, "prepare_for_iceberg", side_effect=lambda df, table: df), \
         patch.object(common, "_partition_exprs", return_value=["PARTITION_EXPR"]):
        yield


def test_first_write_creates_a_partitioned_table():
    spark, df = MagicMock(), MagicMock()
    spark.catalog.tableExists.return_value = False
    result = common.upsert_iceberg_table(spark, df, "iceberg.medical.patient", "patient")
    assert result == "created"
    writer = df.writeTo.return_value.using.return_value
    writer.partitionedBy.assert_called_once_with("PARTITION_EXPR")
    writer.partitionedBy.return_value.create.assert_called_once()
    spark.sql.assert_not_called()


def test_subsequent_write_merges_by_key_instead_of_replacing():
    spark, df = MagicMock(), MagicMock()
    spark.catalog.tableExists.return_value = True
    result = common.upsert_iceberg_table(spark, df, "iceberg.medical.dicom_metadata", "dicom_metadata", key="sop_instance_uid")
    assert result == "merged"
    sql = spark.sql.call_args[0][0]
    assert sql.startswith("MERGE INTO iceberg.medical.dicom_metadata")
    assert "ON t.sop_instance_uid = s.sop_instance_uid" in sql
    assert "WHEN MATCHED THEN UPDATE SET *" in sql and "WHEN NOT MATCHED THEN INSERT *" in sql
    df.writeTo.assert_not_called()  # must NOT createOrReplace: that wiped earlier batches


def test_gold_is_a_full_replace():
    spark, df = MagicMock(), MagicMock()
    assert common.replace_iceberg_table(spark, df, "iceberg.medical_gold.x", "x") == "replaced"
    df.writeTo.return_value.using.return_value.partitionedBy.return_value.createOrReplace.assert_called_once()


def test_coerce_value_handles_nan_ints_and_pandas_missing():
    import pandas as pd

    assert common.coerce_value(float("nan"), "double") is None
    assert common.coerce_value(pd.NaT, "string") is None
    assert common.coerce_value(pd.NA, "string") is None
    assert common.coerce_value(118, "double") == 118.0 and isinstance(common.coerce_value(118, "double"), float)
    assert common.coerce_value(3.0, "long") == 3 and isinstance(common.coerce_value(3.0, "long"), int)
    assert common.coerce_value("x", "string") == "x"
