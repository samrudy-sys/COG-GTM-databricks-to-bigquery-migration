-- Migrated from nb_promo_uplift (PL_RSI_PromoEffectiveness)
{% set baseline_days = params.baseline_days | default(28) | int %}
CREATE OR REPLACE TABLE pds_rsi.gold_promo_uplift AS
WITH sales AS (
  SELECT s.txn_date, st.retailer, s.sku, s.qty, s.net_amount_eur
  FROM pds_rsi.silver_sales_eur s
  JOIN udl.stores st ON st.store_id = s.store_id
  WHERE NOT s.is_return
),
promos AS (
  SELECT
    promo_id, UPPER(TRIM(sku)) AS sku, retailer, promo_type, discount_pct, start_date, end_date,
    DATE_DIFF(end_date, start_date, DAY) + 1 AS promo_days,
    DATE_SUB(start_date, INTERVAL {{ baseline_days }} DAY) AS baseline_start
  FROM udl.promotions
  WHERE start_date <= DATE('{{ params.run_date or ds }}')
),
agg AS (
  SELECT
    p.promo_id, p.sku, p.retailer, p.promo_type, p.discount_pct, p.start_date, p.end_date, p.promo_days,
    -- in_promo is NULL for promos with no matching sales (left join): Spark's when(NULL) falls to otherwise(0), as does IF
    SUM(IF(s.txn_date >= p.start_date, s.qty, 0)) AS promo_units,
    SUM(IF(NOT (s.txn_date >= p.start_date), s.qty, 0)) AS baseline_units,
    CAST(ROUND(CAST(SUM(IF(s.txn_date >= p.start_date, s.net_amount_eur, 0)) AS NUMERIC), 2) AS FLOAT64) AS promo_revenue_eur
  FROM promos p
  LEFT JOIN sales s
    ON s.sku = p.sku AND s.retailer = p.retailer
   AND s.txn_date >= p.baseline_start AND s.txn_date <= p.end_date
  GROUP BY p.promo_id, p.sku, p.retailer, p.promo_type, p.discount_pct, p.start_date, p.end_date, p.promo_days
),
daily AS (
  SELECT
    *,
    CAST(ROUND(CAST(SAFE_DIVIDE(promo_units, promo_days) AS NUMERIC), 4) AS FLOAT64) AS promo_daily_units,
    CAST(ROUND(CAST(baseline_units / {{ baseline_days }} AS NUMERIC), 4) AS FLOAT64) AS baseline_daily_units
  FROM agg
),
uplift AS (
  SELECT
    *,
    -- Spark returns NULL on x/0 (ANSI off)
    CAST(ROUND(CAST(SAFE_DIVIDE(promo_daily_units - baseline_daily_units, baseline_daily_units) * 100 AS NUMERIC), 2) AS FLOAT64)
      AS uplift_pct
  FROM daily
)
SELECT
  promo_id, sku, retailer, promo_type, discount_pct, start_date, end_date, promo_days,
  promo_units, baseline_units, promo_revenue_eur, promo_daily_units, baseline_daily_units, uplift_pct,
  CASE
    WHEN uplift_pct IS NULL THEN 'no_baseline'
    WHEN uplift_pct >= 50 THEN 'strong'
    WHEN uplift_pct >= 10 THEN 'moderate'
    ELSE 'weak'
  END AS verdict,
  CURRENT_TIMESTAMP() AS _load_ts
FROM uplift
