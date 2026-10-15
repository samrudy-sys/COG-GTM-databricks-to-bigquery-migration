-- Migrated from nb_otif_kpis (PL_RSI_OTIF_Region): write_delta(..., replace_where=...) creates the table on first run.
-- The table is never replaced as a whole; each region task replaces only its own partition (02_gold_otif_region.sql).
-- BigQuery cannot partition on a STRING column, so the table is clustered by region instead.
CREATE TABLE IF NOT EXISTS pds_rsi.gold_otif (
  region STRING,
  dc_id STRING,
  retailer STRING,
  month DATE,
  shipments INT64,
  otif_pct FLOAT64,
  on_time_pct FLOAT64,
  fill_rate FLOAT64,
  undelivered INT64,
  _load_ts TIMESTAMP
)
CLUSTER BY region
