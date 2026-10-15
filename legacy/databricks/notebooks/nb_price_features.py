# Databricks notebook source
# MAGIC %md
# MAGIC # nb_price_features
# MAGIC Weekly SKU x country feature table for the price-elasticity model (Revenue Growth Management).

# COMMAND ----------

# MAGIC %run ./nb_common_utils

# COMMAND ----------

dbutils.widgets.text("run_date", "")
run_date = dbutils.widgets.get("run_date")

# COMMAND ----------

weekly = (
    spark.table(f"{PDS_SCHEMA}.silver_sales_eur")
    .filter(~F.col("is_return"))
    .withColumn("week_start", F.date_trunc("week", "txn_date").cast("date"))
    .groupBy("week_start", "sku", "country")
    .agg(F.sum("qty").alias("units"), F.sum("net_amount_eur").alias("net_revenue_eur"))
    .withColumn("avg_price_eur", F.round(F.col("net_revenue_eur") / F.col("units"), 4))
    .withColumn("log_units", F.round(F.log1p("units"), 6))
    .withColumn("log_price", F.round(F.log("avg_price_eur"), 6))
)

w = Window.partitionBy("sku", "country").orderBy("week_start")
promos = spark.table("udl.promotions").withColumn("sku", normalize_sku("sku"))
promo_weeks = (
    weekly.alias("w").join(
        promos.alias("p"),
        (F.col("w.sku") == F.col("p.sku"))
        & (F.col("p.start_date") <= F.date_add(F.col("w.week_start"), 6))
        & (F.col("p.end_date") >= F.col("w.week_start")),
        "left_semi",
    )
    .select("week_start", "sku", "country")
    .withColumn("on_promo", F.lit(True))
)

features = (
    weekly.join(promo_weeks, ["week_start", "sku", "country"], "left")
    .withColumn("on_promo", F.coalesce("on_promo", F.lit(False)))
    .withColumn("price_lag_1w", F.lag("avg_price_eur").over(w))
    .withColumn("price_change_pct", F.round((F.col("avg_price_eur") / F.col("price_lag_1w") - 1) * 100, 3))
    .withColumn("units_ma_4w", F.round(F.avg("units").over(w.rowsBetween(-3, 0)), 3))
    .withColumn("net_revenue_eur", F.round("net_revenue_eur", 2))
)

write_delta(features, "feat_price_elasticity")
