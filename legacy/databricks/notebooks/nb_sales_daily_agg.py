# Databricks notebook source
# MAGIC %md
# MAGIC # nb_sales_daily_agg
# MAGIC Gold daily sell-out by store x SKU. Feeds the Power BI "RSI Sell-Out" dataset.

# COMMAND ----------

# MAGIC %run ./nb_common_utils

# COMMAND ----------

dbutils.widgets.text("run_date", "")
run_date = dbutils.widgets.get("run_date")

# COMMAND ----------

s = spark.table(f"{PDS_SCHEMA}.silver_sales_eur")

daily = (
    s.groupBy("txn_date", "store_id", "country", "sku")
    .agg(
        F.sum("qty").alias("net_units"),
        F.sum(F.when(F.col("is_return"), -F.col("qty")).otherwise(0)).alias("return_units"),
        F.countDistinct("txn_id").alias("txn_count"),
        F.round(F.sum("net_amount_eur"), 2).alias("net_revenue_eur"),
        F.round(F.sum("gross_amount_eur"), 2).alias("gross_revenue_eur"),
        F.round(F.sum("net_amount_eur") / F.sum("qty"), 4).alias("avg_selling_price_eur"),
    )
)

write_delta(daily, "gold_sales_daily")
