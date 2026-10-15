# Databricks notebook source
# MAGIC %md
# MAGIC # nb_media_roi
# MAGIC Joins weekly media spend to weekly sell-out revenue for the same brand/country.

# COMMAND ----------

# MAGIC %run ./nb_common_utils

# COMMAND ----------

dbutils.widgets.text("run_date", "")
run_date = dbutils.widgets.get("run_date")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW v_weekly_brand_revenue AS
SELECT CAST(date_trunc('week', d.txn_date) AS DATE) AS week_start,
       p.brand,
       d.country,
       SUM(d.net_revenue_eur)                 AS net_revenue_eur,
       SUM(d.net_units)                       AS net_units
FROM {PDS_SCHEMA}.gold_sales_daily d
JOIN udl.products p ON p.sku = d.sku
WHERE d.txn_date <= '{run_date}'
GROUP BY 1, 2, 3
""")

roi = spark.sql(f"""
SELECT m.week_start, m.brand, m.country,
       m.total_spend_eur,
       m.spend_tv_eur, m.spend_search_eur, m.spend_social_eur, m.spend_retail_media_eur,
       m.impressions,
       ROUND(COALESCE(r.net_revenue_eur, 0), 2)                         AS net_revenue_eur,
       COALESCE(r.net_units, 0)                                          AS net_units,
       ROUND(COALESCE(r.net_revenue_eur, 0) / m.total_spend_eur, 4)      AS revenue_per_eur_spent,
       ROUND(m.total_spend_eur * 1000 / NULLIF(m.impressions, 0), 4)     AS cpm_eur
FROM {PDS_SCHEMA}.silver_media_weekly m
LEFT JOIN v_weekly_brand_revenue r
  ON r.week_start = m.week_start AND r.brand = m.brand AND r.country = m.country
""")

write_delta(roi, "gold_media_roi")
