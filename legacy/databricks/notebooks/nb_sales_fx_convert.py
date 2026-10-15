# Databricks notebook source
# MAGIC %md
# MAGIC # nb_sales_fx_convert
# MAGIC Converts silver sales to EUR using the **latest published rate on or before** the
# MAGIC transaction date (treasury does not publish at weekends, so this is an as-of join).

# COMMAND ----------

# MAGIC %run ./nb_common_utils

# COMMAND ----------

dbutils.widgets.text("run_date", "")
run_date = dbutils.widgets.get("run_date")

# COMMAND ----------

sales = spark.table(f"{PDS_SCHEMA}.silver_sales").drop("_load_ts")
fx = spark.table("udl.fx_rates")

asof = Window.partitionBy("s.txn_id").orderBy(F.col("fx.rate_date").desc())
converted = (
    sales.alias("s")
    .join(fx.alias("fx"), (F.col("s.currency") == F.col("fx.currency")) & (F.col("fx.rate_date") <= F.col("s.txn_date")), "left")
    .withColumn("_rn", F.row_number().over(asof))
    .filter("_rn = 1")
    .select("s.*", F.col("fx.rate_to_eur").alias("fx_rate_to_eur"), F.col("fx.rate_date").alias("fx_rate_date"))
    .withColumn("gross_amount_eur", F.round(F.col("gross_amount") * F.col("fx_rate_to_eur"), 4))
    .withColumn("net_amount_eur", F.round(F.col("net_amount") * F.col("fx_rate_to_eur"), 4))
)

write_delta(converted, "silver_sales_eur")
