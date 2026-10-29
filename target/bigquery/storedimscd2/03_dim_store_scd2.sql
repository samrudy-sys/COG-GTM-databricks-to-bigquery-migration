-- Migrated from nb_store_dim_scd2 (PL_RSI_StoreDimSCD2.NB_Store_Dim_SCD2), step 2 of 2: Delta MERGE (SCD type 2) -> BigQuery MERGE
-- Tracked attributes: retailer, region, store_format.
MERGE pds_rsi.dim_store t
USING pds_rsi._stg_dim_store s
ON t.store_id = s.merge_key AND t.is_current
WHEN MATCHED AND (t.retailer <> s.retailer OR t.region <> s.region OR t.store_format <> s.store_format) THEN
  UPDATE SET is_current = FALSE, valid_to = s.updated_at
WHEN NOT MATCHED THEN
  INSERT (store_sk, store_id, retailer, country, region, store_format, open_date, valid_from, valid_to, is_current, _load_ts)
  VALUES (ARRAY_TO_STRING([s.store_id, FORMAT_TIMESTAMP('%Y%m%d%H%M%S', s.updated_at, 'UTC')], '-'), s.store_id, s.retailer,
          s.country, s.region, s.store_format, s.open_date, s.updated_at, TIMESTAMP '9999-12-31 00:00:00', TRUE,
          CURRENT_TIMESTAMP());

DROP TABLE IF EXISTS pds_rsi._stg_dim_store;
DROP TABLE IF EXISTS pds_rsi._stg_store_updates;
