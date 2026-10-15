# Databricks notebook source
# MAGIC %md
# MAGIC # nb_store_dim_init
# MAGIC (Re)builds `dim_store` (SCD type 2) from the current store master.

# COMMAND ----------

# MAGIC %run ./nb_common_utils

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {PDS_SCHEMA}.dim_store")

init = (
    spark.table("udl.stores")
    .withColumn("valid_from", F.col("updated_at"))
    .withColumn("valid_to", F.to_timestamp(F.lit("9999-12-31 00:00:00")))
    .withColumn("is_current", F.lit(True))
    .withColumn("store_sk", F.concat_ws("-", "store_id", F.date_format("valid_from", "yyyyMMddHHmmss")))
    .select("store_sk", "store_id", "retailer", "country", "region", "store_format", "open_date",
            "valid_from", "valid_to", "is_current")
)
write_delta(init, "dim_store")
dbutils.notebook.exit("initialised")
