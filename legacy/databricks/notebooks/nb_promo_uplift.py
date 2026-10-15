# Databricks notebook source
# MAGIC %md
# MAGIC # nb_promo_uplift
# MAGIC Promotion effectiveness: average daily units during the promo window vs. a
# MAGIC 28-day pre-promo baseline, for the promoted SKU at the promoting retailer.

# COMMAND ----------

# MAGIC %run ./nb_common_utils

# COMMAND ----------

dbutils.widgets.text("run_date", "")
dbutils.widgets.text("baseline_days", "28")
run_date = dbutils.widgets.get("run_date")
baseline_days = int(dbutils.widgets.get("baseline_days"))

# COMMAND ----------

sales = (
    spark.table(f"{PDS_SCHEMA}.silver_sales_eur")
    .filter(~F.col("is_return"))
    .join(spark.table("udl.stores").select("store_id", "retailer"), "store_id")
    .select("txn_date", "retailer", "sku", "qty", "net_amount_eur")
)
promos = (
    spark.table("udl.promotions")
    .withColumn("sku", normalize_sku("sku"))
    .filter(F.col("start_date") <= F.lit(run_date))
    .withColumn("promo_days", F.datediff("end_date", "start_date") + 1)
    .withColumn("baseline_start", F.date_sub("start_date", baseline_days))
)

joined = promos.alias("p").join(
    sales.alias("s"),
    (F.col("p.sku") == F.col("s.sku"))
    & (F.col("p.retailer") == F.col("s.retailer"))
    & (F.col("s.txn_date") >= F.col("p.baseline_start"))
    & (F.col("s.txn_date") <= F.col("p.end_date")),
    "left",
)
in_promo = F.col("s.txn_date") >= F.col("p.start_date")

uplift = (
    joined.groupBy("p.promo_id", "p.sku", "p.retailer", "p.promo_type", "p.discount_pct", "p.start_date", "p.end_date", "p.promo_days")
    .agg(
        F.sum(F.when(in_promo, F.col("s.qty")).otherwise(0)).alias("promo_units"),
        F.sum(F.when(~in_promo, F.col("s.qty")).otherwise(0)).alias("baseline_units"),
        F.round(F.sum(F.when(in_promo, F.col("s.net_amount_eur")).otherwise(0)), 2).alias("promo_revenue_eur"),
    )
    .withColumn("promo_daily_units", F.round(F.col("promo_units") / F.col("promo_days"), 4))
    .withColumn("baseline_daily_units", F.round(F.col("baseline_units") / F.lit(baseline_days), 4))
    # Spark returns NULL on divide-by-zero (ANSI mode off on our clusters)
    .withColumn("uplift_pct", F.round((F.col("promo_daily_units") - F.col("baseline_daily_units")) / F.col("baseline_daily_units") * 100, 2))
    .withColumn("verdict", F.when(F.col("uplift_pct").isNull(), "no_baseline")
                .when(F.col("uplift_pct") >= 50, "strong")
                .when(F.col("uplift_pct") >= 10, "moderate")
                .otherwise("weak"))
)

write_delta(uplift, "gold_promo_uplift")
