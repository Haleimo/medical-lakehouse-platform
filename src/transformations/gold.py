"""
Gold layer transformations: analytics-friendly, aggregated datasets built on
top of the de-identified Silver tables.

Implemented with pandas so the logic is easy to unit test without spinning up
a Spark cluster. `spark/jobs/silver_to_gold.py` reads Silver through Spark,
calls these functions, and writes the result back out through Spark (local
Parquet or Iceberg, depending on RUN_MODE).

Gold tables produced:
  * patient_summary            -- one row per (de-identified) patient
  * observation_daily_summary  -- one row per (day, code system, code)
  * encounter_summary          -- one row per encounter class + status
  * condition_summary          -- one row per condition code
  * medication_summary         -- one row per medication code

Row-conservation rule: every Silver row must land in exactly one Gold group.
All groupbys therefore use ``dropna=False`` -- pandas drops rows whose group
key is null by default, which silently lost observations that had no coding
system (see tests/test_gold_transformations.py::test_no_rows_lost_*).
"""

from __future__ import annotations

import pandas as pd

_COUNT_COLS = ["encounter_count", "condition_count", "medication_count"]


def patient_summary(
    patients: pd.DataFrame,
    encounters: pd.DataFrame,
    conditions: pd.DataFrame,
    medications: pd.DataFrame,
) -> pd.DataFrame:
    cols = ["patient_id", "gender", "age_band", "birth_year", *_COUNT_COLS]
    if patients.empty:
        return pd.DataFrame(columns=cols)

    def _count(df: pd.DataFrame, name: str) -> pd.Series:
        if df.empty:
            return pd.Series(dtype="int64", name=name)
        return df.groupby("patient_id").size().rename(name)

    summary = (
        patients.set_index("id")
        .join(_count(encounters, "encounter_count"))
        .join(_count(conditions, "condition_count"))
        .join(_count(medications, "medication_count"))
    )
    for c in _COUNT_COLS:  # joins introduce NaN (-> float); counts are integers
        summary[c] = summary[c].fillna(0).astype("int64")
    summary = summary.reset_index().rename(columns={"id": "patient_id"})
    return summary[[c for c in cols if c in summary.columns]]


def observation_daily_summary(observations: pd.DataFrame) -> pd.DataFrame:
    keys = ["observation_date", "code_system", "code", "code_display"]
    cols = [*keys, "observation_count", "avg_value", "min_value", "max_value"]
    if observations.empty:
        return pd.DataFrame(columns=cols)
    df = observations.copy()
    df["effective_datetime"] = pd.to_datetime(df["effective_datetime"], errors="coerce", utc=True)
    df["observation_date"] = df["effective_datetime"].dt.date
    grouped = df.groupby(keys, dropna=False).agg(
        observation_count=("id", "count"),
        avg_value=("value_quantity_value", "mean"),
        min_value=("value_quantity_value", "min"),
        max_value=("value_quantity_value", "max"),
    )
    return grouped.reset_index()


def encounter_summary(encounters: pd.DataFrame) -> pd.DataFrame:
    if encounters.empty:
        return pd.DataFrame(columns=["class_code", "status", "encounter_count"])
    return encounters.groupby(["class_code", "status"], dropna=False).size().rename("encounter_count").reset_index()


def condition_summary(conditions: pd.DataFrame) -> pd.DataFrame:
    if conditions.empty:
        return pd.DataFrame(columns=["code", "code_display", "condition_count", "patient_count"])
    return (
        conditions.groupby(["code", "code_display"], dropna=False)
        .agg(condition_count=("id", "count"), patient_count=("patient_id", "nunique"))
        .reset_index()
    )


def medication_summary(medications: pd.DataFrame) -> pd.DataFrame:
    if medications.empty:
        return pd.DataFrame(columns=["medication_code", "medication_display", "prescription_count", "patient_count"])
    return (
        medications.groupby(["medication_code", "medication_display"], dropna=False)
        .agg(prescription_count=("id", "count"), patient_count=("patient_id", "nunique"))
        .reset_index()
    )
