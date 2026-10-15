# Databricks notebook source
# MAGIC %md
# MAGIC # nb_exec_kpi_pack
# MAGIC Monthly executive KPI pack by region: revenue, OTIF and media efficiency.
# MAGIC Published to the Power BI "Exec Pack" workspace (refresh triggered by ADF).

# COMMAND ----------

# MAGIC %run ./nb_common_utils

# COMMAND ----------

dbutils.widgets.text("run_date", "")
run_date = dbutils.widgets.get("run_date")

# COMMAND ----------

country_region = spark.table("udl.stores").select("country", "region").distinct()

revenue = (
    spark.table(f"{PDS_SCHEMA}.gold_sales_daily")
    .join(spark.table("udl.stores").select("store_id", "region"), "store_id")
    .withColumn("month", F.date_trunc("month", "txn_date").cast("date"))
    .groupBy("region", "month")
    .agg(F.round(F.sum("net_revenue_eur"), 2).alias("net_revenue_eur"), F.sum("net_units").alias("net_units"))
)

otif = (
    spark.table(f"{PDS_SCHEMA}.gold_otif")
    .groupBy("region", "month")
    .agg(F.round(F.sum(F.col("otif_pct") * F.col("shipments")) / F.sum("shipments"), 2).alias("otif_pct"),
         F.sum("shipments").alias("shipments"))
)

media = (
    spark.table(f"{PDS_SCHEMA}.gold_media_roi")
    .join(country_region, "country")
    .withColumn("month", F.date_trunc("month", "week_start").cast("date"))
    .groupBy("region", "month")
    .agg(F.round(F.sum("total_spend_eur"), 2).alias("media_spend_eur"))
)

pack = (
    revenue.join(otif, ["region", "month"], "full_outer")
    .join(media, ["region", "month"], "left")
    .withColumn("media_spend_eur", F.coalesce("media_spend_eur", F.lit(0.0)))
    .withColumn("revenue_to_media_ratio", F.round(F.col("net_revenue_eur") / F.col("media_spend_eur"), 3))
    .filter(F.col("month") <= F.lit(run_date))
)

write_delta(pack, "gold_exec_kpi_monthly")
