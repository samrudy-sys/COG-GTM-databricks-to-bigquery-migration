# Databricks notebook source
# MAGIC %md
# MAGIC # nb_otif_kpis
# MAGIC On-Time-In-Full by region / DC / retailer / month. Called by ADF once **per region**
# MAGIC (ForEach) and overwrites only that region's partition via `replaceWhere`.

# COMMAND ----------

# MAGIC %run ./nb_common_utils

# COMMAND ----------

dbutils.widgets.text("region", "")
dbutils.widgets.text("run_date", "")
region = dbutils.widgets.get("region")
run_date = dbutils.widgets.get("run_date")

# COMMAND ----------

ship = (
    spark.table("udl.shipments")
    .join(spark.table("udl.stores").select("store_id", "retailer", "region"), "store_id")
    .filter(F.col("region") == region)
    .filter(F.col("promised_date") <= F.lit(run_date))
    .withColumn("on_time", F.coalesce(F.col("delivered_date") <= F.col("promised_date"), F.lit(False)))
    .withColumn("in_full", F.col("shipped_units") >= F.col("ordered_units"))
    .withColumn("month", F.date_trunc("month", "promised_date").cast("date"))
)

otif = (
    ship.groupBy("region", "dc_id", "retailer", "month")
    .agg(
        F.count("*").alias("shipments"),
        F.round(100 * F.avg((F.col("on_time") & F.col("in_full")).cast("int")), 2).alias("otif_pct"),
        F.round(100 * F.avg(F.col("on_time").cast("int")), 2).alias("on_time_pct"),
        F.round(F.sum("shipped_units") / F.sum("ordered_units"), 4).alias("fill_rate"),
        F.sum(F.when(F.col("delivered_date").isNull(), 1).otherwise(0)).alias("undelivered"),
    )
)

write_delta(otif, "gold_otif", replace_where=f"region = '{region}'")
