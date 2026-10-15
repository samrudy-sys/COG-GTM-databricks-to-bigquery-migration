-- Migrated from nb_store_dim_init (PL_RSI_StoreDimSCD2.If_Full_Reload / dbutils.notebook.run from nb_store_dim_scd2)
-- (Re)builds dim_store (SCD type 2) from the current store master: DROP + overwrite -> CREATE OR REPLACE
CREATE OR REPLACE TABLE pds_rsi.dim_store AS
SELECT
  ARRAY_TO_STRING([store_id, FORMAT_TIMESTAMP('%Y%m%d%H%M%S', updated_at, 'UTC')], '-') AS store_sk,  -- concat_ws skips NULLs
  store_id,
  retailer,
  country,
  region,
  store_format,
  open_date,
  updated_at AS valid_from,
  TIMESTAMP '9999-12-31 00:00:00' AS valid_to,
  TRUE AS is_current,
  CURRENT_TIMESTAMP() AS _load_ts
FROM udl.stores
