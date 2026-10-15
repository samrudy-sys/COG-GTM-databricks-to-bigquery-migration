-- Migrated from nb_ratings_pulse (PL_RSI_RatingsPulse): rolling window_days (default 30) ratings + NPS per SKU
-- Spark Window.partitionBy("sku").orderBy("day_num").rangeBetween(-(window_days - 1), 0) is rewritten as an
-- equivalent self-join on the day range (the emulator cannot run RANGE ... PRECEDING with PARTITION BY).
{% set window_days = params.window_days | default(30) | int %}
CREATE OR REPLACE TABLE pds_rsi.gold_ratings_pulse AS
WITH daily AS (
  SELECT
    UPPER(TRIM(sku)) AS sku,
    review_date,
    COUNT(*) AS reviews,
    SUM(rating) AS rating_sum,
    SUM(IF(rating = 5, 1, 0)) AS promoters,
    SUM(IF(rating <= 3, 1, 0)) AS detractors,
    UNIX_DATE(review_date) AS day_num
  FROM bdl.customer_reviews
  WHERE review_date <= DATE('{{ params.run_date or ds }}')
  GROUP BY 1, 2
),
pulse AS (
  SELECT
    d.sku,
    d.review_date,
    SUM(h.reviews) AS reviews_30d,
    SUM(h.rating_sum) AS _rating_sum_30d,
    SUM(h.promoters) AS _promoters_30d,
    SUM(h.detractors) AS _detractors_30d
  FROM daily d
  JOIN daily h
    ON h.sku = d.sku AND h.day_num BETWEEN d.day_num - {{ window_days - 1 }} AND d.day_num
  GROUP BY d.sku, d.review_date
)
SELECT
  sku,
  review_date,
  reviews_30d,
  CAST(ROUND(CAST(SAFE_DIVIDE(_rating_sum_30d, reviews_30d) AS NUMERIC), 3) AS FLOAT64) AS avg_rating_30d,
  CAST(ROUND(CAST(SAFE_DIVIDE(100 * (_promoters_30d - _detractors_30d), reviews_30d) AS NUMERIC), 1) AS FLOAT64) AS nps_30d,
  reviews_30d < 5 AS low_confidence,
  CURRENT_TIMESTAMP() AS _load_ts
FROM pulse
