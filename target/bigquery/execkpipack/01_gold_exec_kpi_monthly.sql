-- Migrated from nb_exec_kpi_pack (PL_RSI_ExecKPIPack)
CREATE OR REPLACE TABLE pds_rsi.gold_exec_kpi_monthly AS
WITH country_region AS (
  SELECT DISTINCT country, region
  FROM udl.stores
),
revenue AS (
  SELECT
    s.region,
    DATE_TRUNC(g.txn_date, MONTH) AS month,
    CAST(ROUND(CAST(SUM(g.net_revenue_eur) AS NUMERIC), 2) AS FLOAT64) AS net_revenue_eur,
    SUM(g.net_units) AS net_units
  FROM pds_rsi.gold_sales_daily g
  JOIN udl.stores s ON s.store_id = g.store_id
  GROUP BY region, month
),
otif AS (
  SELECT
    region,
    month,
    CAST(ROUND(CAST(SAFE_DIVIDE(SUM(otif_pct * shipments), SUM(shipments)) AS NUMERIC), 2) AS FLOAT64) AS otif_pct,  -- Spark returns NULL on x/0
    SUM(shipments) AS shipments
  FROM pds_rsi.gold_otif
  GROUP BY region, month
),
media AS (
  SELECT
    cr.region,
    DATE_TRUNC(m.week_start, MONTH) AS month,
    CAST(ROUND(CAST(SUM(m.total_spend_eur) AS NUMERIC), 2) AS FLOAT64) AS media_spend_eur
  FROM pds_rsi.gold_media_roi m
  JOIN country_region cr ON cr.country = m.country
  GROUP BY region, month
),
rev_otif AS (
  -- Spark full_outer join on ["region", "month"] coalesces the join keys
  SELECT
    COALESCE(r.region, o.region) AS region,
    COALESCE(r.month, o.month) AS month,
    r.net_revenue_eur, r.net_units, o.otif_pct, o.shipments
  FROM revenue r
  FULL OUTER JOIN otif o ON o.region = r.region AND o.month = r.month
),
pack AS (
  SELECT
    ro.region, ro.month, ro.net_revenue_eur, ro.net_units, ro.otif_pct, ro.shipments,
    COALESCE(md.media_spend_eur, 0.0) AS media_spend_eur
  FROM rev_otif ro
  LEFT JOIN media md ON md.region = ro.region AND md.month = ro.month
)
SELECT
  region, month, net_revenue_eur, net_units, otif_pct, shipments, media_spend_eur,
  CAST(ROUND(CAST(SAFE_DIVIDE(net_revenue_eur, media_spend_eur) AS NUMERIC), 3) AS FLOAT64) AS revenue_to_media_ratio,  -- Spark returns NULL on x/0
  CURRENT_TIMESTAMP() AS _load_ts
FROM pack
WHERE month <= DATE('{{ params.run_date or ds }}')
