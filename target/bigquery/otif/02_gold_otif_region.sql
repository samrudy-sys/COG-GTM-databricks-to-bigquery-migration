-- Migrated from nb_otif_kpis (PL_RSI_OTIF_Region), run once per region by the ForEach_Region fan-out.
-- Delta replaceWhere "region = '<region>'" -> DELETE + INSERT of that region in one transaction.
BEGIN TRANSACTION;

DELETE FROM pds_rsi.gold_otif WHERE region = '{{ params.region }}';

INSERT INTO pds_rsi.gold_otif
  (region, dc_id, retailer, month, shipments, otif_pct, on_time_pct, fill_rate, undelivered, _load_ts)
WITH ship AS (
  SELECT
    st.region,
    sh.dc_id,
    st.retailer,
    sh.shipped_units,
    sh.ordered_units,
    sh.delivered_date,
    COALESCE(sh.delivered_date <= sh.promised_date, FALSE) AS on_time,
    sh.shipped_units >= sh.ordered_units AS in_full,
    DATE_TRUNC(sh.promised_date, MONTH) AS month
  FROM udl.shipments sh
  JOIN udl.stores st USING (store_id)
  WHERE st.region = '{{ params.region }}'
    AND sh.promised_date <= DATE('{{ params.run_date or ds }}')
)
SELECT
  region, dc_id, retailer, month,
  COUNT(*) AS shipments,
  CAST(ROUND(CAST(100 * AVG(CAST(on_time AND in_full AS INT64)) AS NUMERIC), 2) AS FLOAT64) AS otif_pct,
  CAST(ROUND(CAST(100 * AVG(CAST(on_time AS INT64)) AS NUMERIC), 2) AS FLOAT64) AS on_time_pct,
  CAST(ROUND(CAST(SAFE_DIVIDE(SUM(shipped_units), SUM(ordered_units)) AS NUMERIC), 4) AS FLOAT64) AS fill_rate,  -- Spark returns NULL on x/0
  SUM(IF(delivered_date IS NULL, 1, 0)) AS undelivered,
  CURRENT_TIMESTAMP() AS _load_ts
FROM ship
GROUP BY region, dc_id, retailer, month;

COMMIT TRANSACTION;
