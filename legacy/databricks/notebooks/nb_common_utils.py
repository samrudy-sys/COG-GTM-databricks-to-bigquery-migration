# Databricks notebook source
# MAGIC %md
# MAGIC ## nb_common_utils
# MAGIC Shared helpers `%run` by every Retail Sales Intelligence (RSI) notebook.
# MAGIC Owner: pds-rsi data engineering. Do not add business logic here.

# COMMAND ----------

import datetime as dt

from pyspark.sql import Window
from pyspark.sql import functions as F

PDS_SCHEMA = "pds_rsi"

COUNTRY_CURRENCY = {"GB": "GBP", "DE": "EUR", "FR": "EUR", "NL": "EUR", "IN": "INR", "US": "USD", "PL": "PLN"}


def normalize_sku(col):
    """SKUs arrive from some retailer feeds lower-cased and padded."""
    return F.upper(F.trim(F.col(col) if isinstance(col, str) else col))


def window_bounds(run_date: str, lookback_days: int):
    end = dt.date.fromisoformat(run_date)
    return end - dt.timedelta(days=lookback_days - 1), end


def write_delta(df, table, replace_where=None):
    """All PDS tables carry an audit column `_load_ts`."""
    writer = df.withColumn("_load_ts", F.current_timestamp()).write.format("delta").mode("overwrite")
    if replace_where:
        writer = writer.option("replaceWhere", replace_where)
    else:
        writer = writer.option("overwriteSchema", "true")
    writer.saveAsTable(f"{PDS_SCHEMA}.{table}")
