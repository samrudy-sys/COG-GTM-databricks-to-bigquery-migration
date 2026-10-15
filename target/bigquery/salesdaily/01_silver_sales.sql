-- Migrated from nb_sales_cleanse (PL_RSI_Sales_Cleanse)
-- ADF LookupWatermark (Azure SQL ctl.watermarks) -> inline read of BigQuery ctl.watermarks
{% set lookback_days = params.lookback_days | default(120) | int %}
CREATE OR REPLACE TABLE pds_rsi.silver_sales AS
WITH latest AS (
  SELECT *
  FROM udl.sales_transactions
  WHERE ingest_ts > (SELECT last_ingest_ts FROM ctl.watermarks WHERE pipeline_name = 'PL_RSI_SalesDaily')
  QUALIFY ROW_NUMBER() OVER (PARTITION BY txn_id ORDER BY ingest_ts DESC) = 1
),
cleansed AS (
  SELECT
    l.txn_id,
    DATE(l.txn_ts) AS txn_date,
    l.txn_ts,
    l.store_id,
    s.country,
    UPPER(TRIM(l.sku)) AS sku,
    l.qty,
    l.qty < 0 AS is_return,
    l.unit_price,
    CAST(ROUND(CAST(l.qty * l.unit_price AS NUMERIC), 2) AS FLOAT64) AS gross_amount,
    IF(l.qty < 0, 0.0, l.discount_amt) AS discount_amt,
    COALESCE(l.currency, CASE s.country
      WHEN 'GB' THEN 'GBP' WHEN 'DE' THEN 'EUR' WHEN 'FR' THEN 'EUR' WHEN 'NL' THEN 'EUR'
      WHEN 'IN' THEN 'INR' WHEN 'US' THEN 'USD' WHEN 'PL' THEN 'PLN' END) AS currency,
    l.channel
  FROM latest l
  LEFT JOIN udl.stores s USING (store_id)
)
SELECT
  txn_id, txn_date, txn_ts, store_id, country, sku, qty, is_return, unit_price, gross_amount, discount_amt,
  CAST(ROUND(CAST(gross_amount - discount_amt AS NUMERIC), 2) AS FLOAT64) AS net_amount,
  currency, channel,
  CURRENT_TIMESTAMP() AS _load_ts
FROM cleansed
WHERE txn_date BETWEEN DATE_SUB(DATE('{{ params.run_date or ds }}'), INTERVAL {{ lookback_days - 1 }} DAY)
                   AND DATE('{{ params.run_date or ds }}')
