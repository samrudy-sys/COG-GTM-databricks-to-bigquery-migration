-- Migrated from nb_product_dim (PL_RSI_ProductDim): conformed product dimension + "Clean Future" ESG scorecard
-- Python UDFs pack_size_band / sustainability_score are ported as SQL CASE expressions (same branch order and thresholds)
CREATE OR REPLACE TABLE pds_rsi.dim_product AS
WITH products AS (
  SELECT UPPER(TRIM(sku)) AS sku, product_name, brand, category, subcategory, pack_size_ml, launch_date, is_active
  FROM udl.products
),
metrics AS (
  SELECT UPPER(TRIM(sku)) AS sku, plastic_g, recycled_content_pct, carbon_g_co2e
  FROM bdl.sustainability_metrics
),
joined AS (
  SELECT
    p.*,
    m.plastic_g,
    m.recycled_content_pct,
    m.carbon_g_co2e,
    -- UDF sustainability_score: raw = 100 - plastic_g * 0.6 + recycled_pct * 30 - carbon_g / 20, clamped to [0, 100]
    IF(m.plastic_g IS NULL OR m.recycled_content_pct IS NULL OR m.carbon_g_co2e IS NULL, NULL,
       LEAST(100.0, GREATEST(0.0, 100 - m.plastic_g * 0.6 + m.recycled_content_pct * 30 - m.carbon_g_co2e / 20))) AS clamped_score
  FROM products p
  LEFT JOIN metrics m USING (sku)
),
-- Python round(x, 1) rounds the exact binary double, ties-to-even. ROUND(FLOAT64) computes x * 10 in floating
-- point first, which rounds e.g. 84.8499999999999943 (the double for 84.85) up to 84.9. So 10 * x is computed
-- exactly as hi + lo (x * 8 and x * 2 are exact, then a TwoSum), and the integer is chosen by exact comparisons.
scaled AS (
  SELECT *, clamped_score * 8 + clamped_score * 2 AS hi
  FROM joined
),
split AS (
  SELECT
    * EXCEPT (hi),
    hi,
    FLOOR(hi) AS hi_floor,
    (clamped_score * 8 - (hi - (hi - clamped_score * 8))) + (clamped_score * 2 - (hi - clamped_score * 8)) AS lo
  FROM scaled
),
scored AS (
  SELECT
    * EXCEPT (clamped_score, hi, hi_floor, lo),
    CASE
      WHEN hi - hi_floor - 0.5 > 0 - lo THEN hi_floor + 1
      WHEN hi - hi_floor - 0.5 < 0 - lo THEN hi_floor
      ELSE hi_floor + MOD(CAST(hi_floor AS INT64), 2)
    END / 10 AS sustainability_score
  FROM split
)
SELECT
  sku,
  product_name,
  brand,
  category,
  subcategory,
  -- sha2(concat_ws('|', brand, category), 256): concat_ws skips NULLs, CONCAT would return NULL
  TO_HEX(SHA256(CONCAT(
    COALESCE(brand, ''),
    IF(brand IS NOT NULL AND category IS NOT NULL, '|', ''),
    COALESCE(category, '')
  ))) AS brand_category_key,
  pack_size_ml,
  -- UDF pack_size_band
  CASE
    WHEN pack_size_ml IS NULL THEN NULL
    WHEN pack_size_ml < 250 THEN 'travel'
    WHEN pack_size_ml < 500 THEN 'standard'
    WHEN pack_size_ml < 1000 THEN 'family'
    ELSE 'bulk'
  END AS pack_size_band,
  launch_date,
  -- floor(months_between('2024-04-30', launch_date) / 12): Spark months_between is a whole number when the
  -- days of month match or both dates are month-ends, otherwise it adds (day1 - day2) / 31
  CAST(FLOOR((
    DATE_DIFF(DATE '2024-04-30', launch_date, MONTH)
    + IF(EXTRACT(DAY FROM launch_date) = 30 OR EXTRACT(DAY FROM DATE_ADD(launch_date, INTERVAL 1 DAY)) = 1,
         0, (30 - EXTRACT(DAY FROM launch_date)) / 31)
  ) / 12) AS INT64) AS years_on_market,
  is_active,
  plastic_g,
  recycled_content_pct,
  carbon_g_co2e,
  sustainability_score,
  CASE
    WHEN sustainability_score >= 80 THEN 'A'
    WHEN sustainability_score >= 60 THEN 'B'
    WHEN sustainability_score >= 40 THEN 'C'
    WHEN sustainability_score >= 20 THEN 'D'
    ELSE 'E'
  END AS esg_grade,
  CURRENT_TIMESTAMP() AS _load_ts
FROM scored
