-- Migrated from nb_market_share (PL_RSI_MarketShare): monthly brand share of category revenue per retailer x country
CREATE OR REPLACE TABLE pds_rsi.gold_market_share AS
WITH sales_enriched AS (
  SELECT
    DATE_TRUNC(d.txn_date, MONTH) AS month,
    d.country, s.retailer, p.category, p.brand,
    d.net_revenue_eur, d.net_units
  FROM pds_rsi.gold_sales_daily d
  JOIN pds_rsi.dim_store s ON s.store_id = d.store_id AND s.is_current
  JOIN pds_rsi.dim_product p ON p.sku = d.sku
),
brand AS (
  SELECT
    month, country, retailer, category, brand,
    CAST(ROUND(CAST(SUM(net_revenue_eur) AS NUMERIC), 2) AS FLOAT64) AS brand_revenue_eur,
    SUM(net_units) AS brand_units
  FROM sales_enriched
  GROUP BY month, country, retailer, category, brand
),
brand_category AS (
  SELECT
    *,
    CAST(ROUND(CAST(SUM(brand_revenue_eur) OVER (PARTITION BY month, country, retailer, category) AS NUMERIC), 2)
      AS FLOAT64) AS category_revenue_eur
  FROM brand
)
SELECT
  month, country, retailer, category, brand, brand_revenue_eur, brand_units, category_revenue_eur,
  CAST(ROUND(CAST(SAFE_DIVIDE(100 * brand_revenue_eur, category_revenue_eur) AS NUMERIC), 3) AS FLOAT64)
    AS value_share_pct,  -- Spark returns NULL on x/0
  DENSE_RANK() OVER (PARTITION BY month, country, retailer, category ORDER BY brand_revenue_eur DESC) AS share_rank,
  CURRENT_TIMESTAMP() AS _load_ts
FROM brand_category
