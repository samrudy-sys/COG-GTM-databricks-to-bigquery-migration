-- Migrated from nb_sales_daily_agg (PL_RSI_SalesDaily)
CREATE OR REPLACE TABLE pds_rsi.gold_sales_daily AS
SELECT
  txn_date, store_id, country, sku,
  SUM(qty) AS net_units,
  SUM(IF(is_return, 0 - qty, 0)) AS return_units,  -- emulator has no unary minus
  COUNT(DISTINCT txn_id) AS txn_count,
  CAST(ROUND(CAST(SUM(net_amount_eur) AS NUMERIC), 2) AS FLOAT64) AS net_revenue_eur,
  CAST(ROUND(CAST(SUM(gross_amount_eur) AS NUMERIC), 2) AS FLOAT64) AS gross_revenue_eur,
  CAST(ROUND(CAST(SAFE_DIVIDE(SUM(net_amount_eur), SUM(qty)) AS NUMERIC), 4) AS FLOAT64) AS avg_selling_price_eur,  -- Spark returns NULL on x/0
  CURRENT_TIMESTAMP() AS _load_ts
FROM pds_rsi.silver_sales_eur
GROUP BY txn_date, store_id, country, sku
