-- Migrated from nb_forecast_accuracy (PL_RSI_ForecastAccuracy)
CREATE OR REPLACE TABLE pds_rsi.gold_forecast_accuracy AS
WITH fc AS (
  SELECT week_start, UPPER(TRIM(sku)) AS sku, country, forecast_units, forecast_version
  FROM bdl.demand_forecast
  QUALIFY ROW_NUMBER() OVER (PARTITION BY week_start, UPPER(TRIM(sku)), country ORDER BY forecast_version DESC) = 1
),
actuals AS (
  SELECT
    DATE_TRUNC(txn_date, WEEK(MONDAY)) AS week_start,  -- Spark date_trunc('week') is Monday-based
    sku, country,
    SUM(qty) AS actual_units
  FROM pds_rsi.silver_sales
  GROUP BY week_start, sku, country
),
weekly AS (
  SELECT
    fc.forecast_units,
    COALESCE(a.actual_units, 0) AS actual_units,
    fc.sku, fc.country,
    ABS(fc.forecast_units - COALESCE(a.actual_units, 0)) AS abs_error,
    SAFE_DIVIDE(ABS(fc.forecast_units - COALESCE(a.actual_units, 0)), COALESCE(a.actual_units, 0)) AS ape  -- NULL when actual = 0
  FROM fc
  LEFT JOIN actuals a USING (week_start, sku, country)
  WHERE fc.week_start <= DATE('{{ params.run_date or ds }}')
)
SELECT
  sku, country,
  COUNT(*) AS weeks,
  CAST(ROUND(CAST(SUM(forecast_units) AS NUMERIC), 2) AS FLOAT64) AS forecast_units,
  SUM(actual_units) AS actual_units,
  CAST(ROUND(CAST(100 * AVG(ape) AS NUMERIC), 2) AS FLOAT64) AS mape_pct,
  CAST(ROUND(CAST(SAFE_DIVIDE(100 * (SUM(forecast_units) - SUM(actual_units)), SUM(actual_units)) AS NUMERIC), 2) AS FLOAT64) AS bias_pct,
  CAST(ROUND(CAST(100 * (1 - SAFE_DIVIDE(SUM(abs_error), SUM(actual_units))) AS NUMERIC), 2) AS FLOAT64) AS accuracy_pct,
  CURRENT_TIMESTAMP() AS _load_ts
FROM weekly
GROUP BY sku, country
