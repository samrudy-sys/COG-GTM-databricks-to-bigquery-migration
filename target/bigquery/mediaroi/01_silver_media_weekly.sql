-- Migrated from nb_media_weekly (PL_RSI_MediaROI / NB_Media_Weekly)
-- Spark date_trunc('week') is Monday-based -> DATE_TRUNC(.., WEEK(MONDAY)) (BigQuery WEEK alone is Sunday-based)
-- Spark .pivot(media_channel, CHANNELS).agg(round(sum(spend_eur), 2)).na.fill(0.0) -> conditional aggregation + COALESCE 0.0
CREATE OR REPLACE TABLE pds_rsi.silver_media_weekly AS
WITH media AS (
  SELECT DATE_TRUNC(spend_date, WEEK(MONDAY)) AS week_start, brand, country, media_channel, spend_eur, impressions
  FROM bdl.media_spend
  WHERE spend_date <= DATE('{{ params.run_date or ds }}')
)
SELECT
  week_start, brand, country,
  COALESCE(CAST(ROUND(CAST(SUM(IF(media_channel = 'tv', spend_eur, NULL)) AS NUMERIC), 2) AS FLOAT64), 0.0) AS spend_tv_eur,
  COALESCE(CAST(ROUND(CAST(SUM(IF(media_channel = 'search', spend_eur, NULL)) AS NUMERIC), 2) AS FLOAT64), 0.0) AS spend_search_eur,
  COALESCE(CAST(ROUND(CAST(SUM(IF(media_channel = 'social', spend_eur, NULL)) AS NUMERIC), 2) AS FLOAT64), 0.0) AS spend_social_eur,
  COALESCE(CAST(ROUND(CAST(SUM(IF(media_channel = 'retail_media', spend_eur, NULL)) AS NUMERIC), 2) AS FLOAT64), 0.0)
    AS spend_retail_media_eur,
  CAST(ROUND(CAST(SUM(spend_eur) AS NUMERIC), 2) AS FLOAT64) AS total_spend_eur,
  SUM(impressions) AS impressions,
  CURRENT_TIMESTAMP() AS _load_ts
FROM media
GROUP BY week_start, brand, country
