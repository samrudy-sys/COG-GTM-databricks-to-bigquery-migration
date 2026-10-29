-- Migrated from nb_media_roi (PL_RSI_MediaROI / NB_Media_ROI)
-- Temp view v_weekly_brand_revenue -> CTE; f-string run_date -> Jinja RUN_DATE
CREATE OR REPLACE TABLE pds_rsi.gold_media_roi AS
WITH v_weekly_brand_revenue AS (
  SELECT DATE_TRUNC(d.txn_date, WEEK(MONDAY)) AS week_start,
         p.brand,
         d.country,
         SUM(d.net_revenue_eur) AS net_revenue_eur,
         SUM(d.net_units) AS net_units
  FROM pds_rsi.gold_sales_daily d
  JOIN udl.products p ON p.sku = d.sku
  WHERE d.txn_date <= DATE('{{ params.run_date or ds }}')
  GROUP BY 1, 2, 3
)
SELECT
  m.week_start, m.brand, m.country,
  m.total_spend_eur,
  m.spend_tv_eur, m.spend_search_eur, m.spend_social_eur, m.spend_retail_media_eur,
  m.impressions,
  CAST(ROUND(CAST(COALESCE(r.net_revenue_eur, 0) AS NUMERIC), 2) AS FLOAT64) AS net_revenue_eur,
  COALESCE(r.net_units, 0) AS net_units,
  CAST(ROUND(CAST(SAFE_DIVIDE(COALESCE(r.net_revenue_eur, 0), m.total_spend_eur) AS NUMERIC), 4) AS FLOAT64)
    AS revenue_per_eur_spent,  -- Spark returns NULL on x/0
  CAST(ROUND(CAST(SAFE_DIVIDE(m.total_spend_eur * 1000, NULLIF(m.impressions, 0)) AS NUMERIC), 4) AS FLOAT64) AS cpm_eur,
  CURRENT_TIMESTAMP() AS _load_ts
FROM pds_rsi.silver_media_weekly m
LEFT JOIN v_weekly_brand_revenue r
  ON r.week_start = m.week_start AND r.brand = m.brand AND r.country = m.country
