# Databricks notebook source
# MAGIC %md
# MAGIC # nb_inventory_cover
# MAGIC Weekly inventory health: days of cover against trailing 28-day rate of sale,
# MAGIC stock-out flags and week-over-week stock movement.

# COMMAND ----------

# MAGIC %run ./nb_common_utils

# COMMAND ----------

dbutils.widgets.text("run_date", "")
dbutils.widgets.text("ros_days", "28")
run_date = dbutils.widgets.get("run_date")
ros_days = int(dbutils.widgets.get("ros_days"))

# COMMAND ----------

snap = spark.table("udl.inventory_snapshots").withColumn("sku", normalize_sku("sku")).filter(F.col("snapshot_date") <= F.lit(run_date))
sales = spark.table(f"{PDS_SCHEMA}.silver_sales").filter(~F.col("is_return"))

ros = (
    snap.alias("i")
    .join(sales.alias("s"),
          (F.col("i.store_id") == F.col("s.store_id")) & (F.col("i.sku") == F.col("s.sku"))
          & (F.col("s.txn_date") < F.col("i.snapshot_date"))
          & (F.col("s.txn_date") >= F.date_sub(F.col("i.snapshot_date"), ros_days)),
          "left")
    .groupBy("i.snapshot_date", "i.store_id", "i.sku", "i.on_hand_units")
    .agg(F.coalesce(F.sum("s.qty"), F.lit(0)).alias("units_sold_trailing"))
    .withColumn("avg_daily_units", F.round(F.col("units_sold_trailing") / F.lit(ros_days), 4))
)

w = Window.partitionBy("store_id", "sku").orderBy("snapshot_date")
health = (
    ros.withColumn("days_of_cover", F.when(F.col("avg_daily_units") > 0, F.round(F.col("on_hand_units") / F.col("avg_daily_units"), 1)))
    .withColumn("is_stockout", F.col("on_hand_units") == 0)
    .withColumn("wow_change_units", F.col("on_hand_units") - F.lag("on_hand_units").over(w))
    .withColumn("risk_band",
                F.when(F.col("is_stockout"), "OOS")
                .when(F.col("days_of_cover").isNull(), "NO_SALES")
                .when(F.col("days_of_cover") < 7, "LOW")
                .when(F.col("days_of_cover") > 60, "EXCESS")
                .otherwise("HEALTHY"))
)

write_delta(health, "gold_inventory_health")
