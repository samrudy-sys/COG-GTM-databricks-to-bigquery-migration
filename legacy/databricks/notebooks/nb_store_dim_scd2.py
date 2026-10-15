# Databricks notebook source
# MAGIC %md
# MAGIC # nb_store_dim_scd2
# MAGIC Applies store master changes to `dim_store` as SCD type 2 using a Delta MERGE.
# MAGIC Tracked attributes: retailer, region, store_format. Late duplicates: last `updated_at` wins.

# COMMAND ----------

# MAGIC %run ./nb_common_utils

# COMMAND ----------

if not spark.catalog.tableExists(f"{PDS_SCHEMA}.dim_store"):
    dbutils.notebook.run("./nb_store_dim_init", 600)

# COMMAND ----------

latest = Window.partitionBy("store_id").orderBy(F.col("updated_at").desc())
(
    spark.table("udl.store_updates")
    .withColumn("_rn", F.row_number().over(latest)).filter("_rn = 1").drop("_rn")
    .createOrReplaceTempView("v_store_updates")
)

spark.sql(f"""
MERGE INTO {PDS_SCHEMA}.dim_store t
USING (
  SELECT u.store_id AS merge_key, u.* FROM v_store_updates u
  UNION ALL
  SELECT NULL AS merge_key, u.*
  FROM v_store_updates u
  JOIN {PDS_SCHEMA}.dim_store d
    ON u.store_id = d.store_id AND d.is_current
   AND (u.retailer <> d.retailer OR u.region <> d.region OR u.store_format <> d.store_format)
) s
ON t.store_id = s.merge_key AND t.is_current
WHEN MATCHED AND (t.retailer <> s.retailer OR t.region <> s.region OR t.store_format <> s.store_format) THEN
  UPDATE SET t.is_current = false, t.valid_to = s.updated_at
WHEN NOT MATCHED THEN
  INSERT (store_sk, store_id, retailer, country, region, store_format, open_date, valid_from, valid_to, is_current, _load_ts)
  VALUES (concat_ws('-', s.store_id, date_format(s.updated_at, 'yyyyMMddHHmmss')), s.store_id, s.retailer, s.country,
          s.region, s.store_format, s.open_date, s.updated_at, timestamp'9999-12-31 00:00:00', true, current_timestamp())
""")
