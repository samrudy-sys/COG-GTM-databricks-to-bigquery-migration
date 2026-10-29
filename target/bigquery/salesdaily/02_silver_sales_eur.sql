{% from '_macros.sql' import spark_round %}
-- Migrated from nb_sales_fx_convert (PL_RSI_Sales_FxConvert): as-of join to the latest FX rate on/before txn_date
CREATE OR REPLACE TABLE pds_rsi.silver_sales_eur AS
SELECT
  s.* EXCEPT (_load_ts),
  fx.rate_to_eur AS fx_rate_to_eur,
  fx.rate_date AS fx_rate_date,
  {{ spark_round('s.gross_amount * fx.rate_to_eur', 4) }} AS gross_amount_eur,
  {{ spark_round('s.net_amount * fx.rate_to_eur', 4) }} AS net_amount_eur,
  CURRENT_TIMESTAMP() AS _load_ts
FROM pds_rsi.silver_sales s
LEFT JOIN udl.fx_rates fx
  ON fx.currency = s.currency AND fx.rate_date <= s.txn_date
QUALIFY ROW_NUMBER() OVER (PARTITION BY s.txn_id ORDER BY fx.rate_date DESC) = 1
