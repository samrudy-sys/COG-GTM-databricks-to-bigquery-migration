# Databricks notebook source
# MAGIC %md
# MAGIC # nb_ratings_pulse
# MAGIC Rolling 30-day consumer ratings pulse per SKU (avg rating + NPS-style score).
# MAGIC Promoters = 5 stars, detractors = 1-3 stars.

# COMMAND ----------

# MAGIC %run ./nb_common_utils

# COMMAND ----------

dbutils.widgets.text("run_date", "")
dbutils.widgets.text("window_days", "30")
run_date = dbutils.widgets.get("run_date")
window_days = int(dbutils.widgets.get("window_days"))

# COMMAND ----------

daily = (
    spark.table("bdl.customer_reviews")
    .withColumn("sku", normalize_sku("sku"))
    .filter(F.col("review_date") <= F.lit(run_date))
    .groupBy("sku", "review_date")
    .agg(
        F.count("*").alias("reviews"),
        F.sum("rating").alias("rating_sum"),
        F.sum(F.when(F.col("rating") == 5, 1).otherwise(0)).alias("promoters"),
        F.sum(F.when(F.col("rating") <= 3, 1).otherwise(0)).alias("detractors"),
    )
    .withColumn("day_num", F.datediff("review_date", F.lit("1970-01-01")))
)

w = Window.partitionBy("sku").orderBy("day_num").rangeBetween(-(window_days - 1), 0)
pulse = (
    daily.select(
        "sku", "review_date",
        F.sum("reviews").over(w).alias("reviews_30d"),
        F.sum("rating_sum").over(w).alias("_rating_sum_30d"),
        F.sum("promoters").over(w).alias("_promoters_30d"),
        F.sum("detractors").over(w).alias("_detractors_30d"),
    )
    .withColumn("avg_rating_30d", F.round(F.col("_rating_sum_30d") / F.col("reviews_30d"), 3))
    .withColumn("nps_30d", F.round(100 * (F.col("_promoters_30d") - F.col("_detractors_30d")) / F.col("reviews_30d"), 1))
    .withColumn("low_confidence", F.col("reviews_30d") < 5)
    .drop("_rating_sum_30d", "_promoters_30d", "_detractors_30d")
)

write_delta(pulse, "gold_ratings_pulse")
