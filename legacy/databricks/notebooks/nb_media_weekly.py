# Databricks notebook source
# MAGIC %md
# MAGIC # nb_media_weekly
# MAGIC Weekly (ISO, Monday-start) media spend by brand x country, pivoted by channel.

# COMMAND ----------

# MAGIC %run ./nb_common_utils

# COMMAND ----------

dbutils.widgets.text("run_date", "")
run_date = dbutils.widgets.get("run_date")
CHANNELS = ["tv", "search", "social", "retail_media"]

# COMMAND ----------

media = (
    spark.table("bdl.media_spend")
    .filter(F.col("spend_date") <= F.lit(run_date))
    .withColumn("week_start", F.date_trunc("week", "spend_date").cast("date"))
)

weekly = (
    media.groupBy("week_start", "brand", "country")
    .pivot("media_channel", CHANNELS)
    .agg(F.round(F.sum("spend_eur"), 2))
    .na.fill(0.0, CHANNELS)
)
for c in CHANNELS:
    weekly = weekly.withColumnRenamed(c, f"spend_{c}_eur")

totals = media.groupBy("week_start", "brand", "country").agg(
    F.round(F.sum("spend_eur"), 2).alias("total_spend_eur"),
    F.sum("impressions").alias("impressions"),
)

write_delta(weekly.join(totals, ["week_start", "brand", "country"]), "silver_media_weekly")
