# Databricks notebook source
# MAGIC %md
# MAGIC # nb_forecast_accuracy
# MAGIC Weekly forecast vs. actual sell-out (latest forecast version only), rolled up to
# MAGIC SKU x country MAPE and bias. Weeks are Monday-start to match the demand-planning calendar.

# COMMAND ----------

# MAGIC %run ./nb_common_utils

# COMMAND ----------

dbutils.widgets.text("run_date", "")
run_date = dbutils.widgets.get("run_date")

# COMMAND ----------

latest_v = Window.partitionBy("week_start", "sku", "country").orderBy(F.col("forecast_version").desc())
fc = (
    spark.table("bdl.demand_forecast")
    .withColumn("sku", normalize_sku("sku"))
    .withColumn("_rn", F.row_number().over(latest_v)).filter("_rn = 1").drop("_rn")
    .filter(F.col("week_start") <= F.lit(run_date))
)

actuals = (
    spark.table(f"{PDS_SCHEMA}.silver_sales")
    .withColumn("week_start", F.date_trunc("week", "txn_date").cast("date"))
    .groupBy("week_start", "sku", "country")
    .agg(F.sum("qty").alias("actual_units"))
)

weekly = (
    fc.join(actuals, ["week_start", "sku", "country"], "left")
    .withColumn("actual_units", F.coalesce("actual_units", F.lit(0)))
    .withColumn("abs_error", F.abs(F.col("forecast_units") - F.col("actual_units")))
    .withColumn("ape", F.col("abs_error") / F.col("actual_units"))  # NULL when actual = 0
)

acc = (
    weekly.groupBy("sku", "country")
    .agg(
        F.count("*").alias("weeks"),
        F.round(F.sum("forecast_units"), 2).alias("forecast_units"),
        F.sum("actual_units").alias("actual_units"),
        F.round(100 * F.avg("ape"), 2).alias("mape_pct"),
        F.round(100 * (F.sum("forecast_units") - F.sum("actual_units")) / F.sum("actual_units"), 2).alias("bias_pct"),
        F.round(100 * (1 - F.sum("abs_error") / F.sum("actual_units")), 2).alias("accuracy_pct"),
    )
)

write_delta(acc, "gold_forecast_accuracy")
