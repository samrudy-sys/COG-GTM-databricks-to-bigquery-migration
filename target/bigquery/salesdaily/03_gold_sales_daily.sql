{% from '_macros.sql' import spark_round %}
-- Migrated from nb_sales_daily_agg (PL_RSI_SalesDaily)
CREATE OR REPLACE TABLE pds_rsi.gold_sales_daily AS
SELECT
  txn_date, store_id, country, sku,
  SUM(qty) AS net_units,
  SUM(IF(is_return, 0 - qty, 0)) AS return_units,  -- emulator has no unary minus
  COUNT(DISTINCT txn_id) AS txn_count,
  {{ spark_round('SUM(net_amount_eur)', 2) }} AS net_revenue_eur,
  {{ spark_round('SUM(gross_amount_eur)', 2) }} AS gross_revenue_eur,
  {{ spark_round('SAFE_DIVIDE(SUM(net_amount_eur), SUM(qty))', 4) }} AS avg_selling_price_eur,  -- Spark returns NULL on x/0
  CURRENT_TIMESTAMP() AS _load_ts
FROM pds_rsi.silver_sales_eur
GROUP BY txn_date, store_id, country, sku
