-- Migrated from nb_price_features (PL_RSI_PriceFeatures): weekly SKU x country price-elasticity features
{#- Spark round(double, n) is HALF_UP on the double's shortest decimal string, so a value whose double is the nearest
    double to a tie (2.13405) rounds up, and 6.3241499999999995 (float drift) rounds down. Compare x against the double of
    the tie k + 0.5 (correctly-rounded IEEE division of exact integers) instead of rounding the binary value or a NUMERIC
    cast: exact on BigQuery and on the emulator, and it matches Spark where CAST(x AS NUMERIC) would round the drift up. #}
{% macro spark_round(x, n) -%}
{%- set mag = "IF(ABS(" ~ x ~ ") >= (10 * FLOOR(ABS(" ~ x ~ ") * " ~ 10 ** n ~ ") + 5) / " ~ 10 ** (n + 1)
    ~ ", FLOOR(ABS(" ~ x ~ ") * " ~ 10 ** n ~ ") + 1, FLOOR(ABS(" ~ x ~ ") * " ~ 10 ** n ~ ")) / " ~ 10 ** n -%}
IF({{ x }} < 0, 0 - {{ mag }}, {{ mag }})  {#- emulator: no unary minus, and SIGN(x) * y truncates to integer #}
{%- endmacro %}
CREATE OR REPLACE TABLE pds_rsi.feat_price_elasticity AS
WITH weekly_raw AS (
  SELECT
    DATE_TRUNC(txn_date, WEEK(MONDAY)) AS week_start,  -- Spark date_trunc('week') is Monday-based
    sku,
    country,
    SUM(qty) AS units,
    SUM(net_amount_eur) AS net_revenue_eur
  FROM pds_rsi.silver_sales_eur
  WHERE NOT is_return
  GROUP BY week_start, sku, country
),
weekly AS (
  SELECT *, {{ spark_round('SAFE_DIVIDE(net_revenue_eur, units)', 4) }} AS avg_price_eur  -- Spark returns NULL on x/0
  FROM weekly_raw
),
promos AS (
  SELECT UPPER(TRIM(sku)) AS sku, start_date, end_date  -- normalize_sku
  FROM udl.promotions
),
promo_weeks AS (  -- left_semi join: weeks overlapping any promo for the SKU (any retailer)
  SELECT DISTINCT w.week_start, w.sku, w.country
  FROM weekly w
  JOIN promos p
    ON p.sku = w.sku
   AND p.start_date <= DATE_ADD(w.week_start, INTERVAL 6 DAY)
   AND p.end_date >= w.week_start
),
features AS (
  SELECT
    w.week_start, w.sku, w.country, w.units, w.net_revenue_eur, w.avg_price_eur,
    LN(1 + w.units) AS log_units_raw,
    IF(w.avg_price_eur > 0, LN(w.avg_price_eur), NULL) AS log_price_raw,  -- Spark log(x <= 0) is NULL, BigQuery errors
    pw.sku IS NOT NULL AS on_promo,
    LAG(w.avg_price_eur) OVER (PARTITION BY w.sku, w.country ORDER BY w.week_start) AS price_lag_1w,
    AVG(w.units) OVER (PARTITION BY w.sku, w.country ORDER BY w.week_start ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
      AS units_ma_4w_raw
  FROM weekly w
  LEFT JOIN promo_weeks pw
    ON pw.week_start = w.week_start AND pw.sku = w.sku AND pw.country = w.country
),
pct AS (
  SELECT *, (SAFE_DIVIDE(avg_price_eur, price_lag_1w) - 1) * 100 AS price_change_pct_raw
  FROM features
)
SELECT
  week_start, sku, country, units,
  {{ spark_round('net_revenue_eur', 2) }} AS net_revenue_eur,
  avg_price_eur,
  {{ spark_round('log_units_raw', 6) }} AS log_units,
  {{ spark_round('log_price_raw', 6) }} AS log_price,
  on_promo, price_lag_1w,
  {{ spark_round('price_change_pct_raw', 3) }} AS price_change_pct,
  {{ spark_round('units_ma_4w_raw', 3) }} AS units_ma_4w,
  CURRENT_TIMESTAMP() AS _load_ts
FROM pct
