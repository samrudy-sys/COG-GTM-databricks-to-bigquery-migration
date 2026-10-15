# Databricks notebook source
# MAGIC %md
# MAGIC # nb_market_share
# MAGIC Monthly brand share of category revenue within each retailer x country, with rank.

# COMMAND ----------

# MAGIC %run ./nb_common_utils

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW v_sales_enriched AS
# MAGIC SELECT CAST(date_trunc('month', d.txn_date) AS DATE) AS month,
# MAGIC        d.country, s.retailer, p.category, p.brand,
# MAGIC        d.net_revenue_eur, d.net_units
# MAGIC FROM pds_rsi.gold_sales_daily d
# MAGIC JOIN pds_rsi.dim_store s ON s.store_id = d.store_id AND s.is_current
# MAGIC JOIN pds_rsi.dim_product p ON p.sku = d.sku

# COMMAND ----------

brand = (
    spark.table("v_sales_enriched")
    .groupBy("month", "country", "retailer", "category", "brand")
    .agg(F.round(F.sum("net_revenue_eur"), 2).alias("brand_revenue_eur"), F.sum("net_units").alias("brand_units"))
)
cat = Window.partitionBy("month", "country", "retailer", "category")
share = (
    brand.withColumn("category_revenue_eur", F.round(F.sum("brand_revenue_eur").over(cat), 2))
    .withColumn("value_share_pct", F.round(100 * F.col("brand_revenue_eur") / F.col("category_revenue_eur"), 3))
    .withColumn("share_rank", F.dense_rank().over(cat.orderBy(F.col("brand_revenue_eur").desc())))
)

write_delta(share, "gold_market_share")
