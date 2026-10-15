-- Migrated from nb_inventory_cover (PL_RSI_InventoryHealth)
{% set ros_days = params.ros_days | default(28) | int %}
CREATE OR REPLACE TABLE pds_rsi.gold_inventory_health AS
WITH snap AS (
  -- the notebook groups by these columns; duplicate snapshot rows each joined the sales, so keep the multiplicity
  SELECT snapshot_date, store_id, UPPER(TRIM(sku)) AS sku, on_hand_units, COUNT(*) AS dup_rows
  FROM udl.inventory_snapshots
  WHERE snapshot_date <= DATE('{{ params.run_date or ds }}')
  GROUP BY snapshot_date, store_id, UPPER(TRIM(sku)), on_hand_units
),
-- Spark's lag orders by snapshot_date only, so ties (several snapshot rows per store/sku/date) are in arbitrary order;
-- on_hand_units is added as a deterministic tiebreak.
snap_wow AS (
  SELECT *, on_hand_units - LAG(on_hand_units) OVER (PARTITION BY store_id, sku ORDER BY snapshot_date, on_hand_units) AS wow_change_units
  FROM snap
),
sales AS (
  SELECT store_id, sku, txn_date, SUM(qty) AS qty
  FROM pds_rsi.silver_sales
  WHERE NOT is_return AND store_id IS NOT NULL AND sku IS NOT NULL  -- null keys never match the notebook's join
  GROUP BY store_id, sku, txn_date
),
-- The notebook's range join (txn_date in [snapshot_date - ros_days, snapshot_date)) as a running sum:
-- each snapshot emits a 'start' marker at snapshot_date - ros_days and an 'end' marker at snapshot_date,
-- ordered before same-day sales; units = running_sum(end) - running_sum(start).
events AS (
  SELECT store_id, sku, txn_date AS d, 1 AS kind, qty, CAST(NULL AS STRING) AS marker,
         CAST(NULL AS DATE) AS snapshot_date, CAST(NULL AS INT64) AS on_hand_units, 0 AS dup_rows, CAST(NULL AS INT64) AS wow_change_units
  FROM sales
  UNION ALL
  SELECT store_id, sku, DATE_SUB(snapshot_date, INTERVAL {{ ros_days }} DAY), 0, 0, 'start', snapshot_date, on_hand_units, dup_rows, wow_change_units
  FROM snap_wow
  UNION ALL
  SELECT store_id, sku, snapshot_date, 0, 0, 'end', snapshot_date, on_hand_units, dup_rows, wow_change_units
  FROM snap_wow
),
running AS (
  SELECT *, SUM(qty) OVER (PARTITION BY store_id, sku ORDER BY d, kind ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_qty
  FROM events
),
ros AS (
  SELECT
    snapshot_date, store_id, sku, on_hand_units, wow_change_units,
    dup_rows * COALESCE(MAX(IF(marker = 'end', cum_qty, NULL)) - MAX(IF(marker = 'start', cum_qty, NULL)), 0) AS units_sold_trailing
  FROM running
  WHERE marker IS NOT NULL
  GROUP BY snapshot_date, store_id, sku, on_hand_units, dup_rows, wow_change_units
),
rate AS (
  SELECT *, CAST(ROUND(CAST(units_sold_trailing / {{ ros_days }} AS NUMERIC), 4) AS FLOAT64) AS avg_daily_units
  FROM ros
),
cover AS (
  SELECT *,
    IF(avg_daily_units > 0, CAST(ROUND(CAST(SAFE_DIVIDE(on_hand_units, avg_daily_units) AS NUMERIC), 1) AS FLOAT64), NULL) AS days_of_cover
  FROM rate
)
SELECT
  snapshot_date, store_id, sku, on_hand_units, units_sold_trailing, avg_daily_units, days_of_cover,
  on_hand_units = 0 AS is_stockout,
  wow_change_units,
  CASE
    WHEN on_hand_units = 0 THEN 'OOS'
    WHEN days_of_cover IS NULL THEN 'NO_SALES'
    WHEN days_of_cover < 7 THEN 'LOW'
    WHEN days_of_cover > 60 THEN 'EXCESS'
    ELSE 'HEALTHY'
  END AS risk_band,
  CURRENT_TIMESTAMP() AS _load_ts
FROM cover
