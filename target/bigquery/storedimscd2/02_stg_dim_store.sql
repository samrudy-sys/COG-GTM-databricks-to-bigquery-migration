-- Migrated from nb_store_dim_scd2 (PL_RSI_StoreDimSCD2.NB_Store_Dim_SCD2), step 1 of 2: MERGE source
-- Window row_number() = 1 dedup (late duplicates: last updated_at wins) -> QUALIFY.
-- Staged as physical tables (the emulator MERGE source must be a single table reference), dropped by 03_dim_store_scd2.sql.
CREATE OR REPLACE TABLE pds_rsi._stg_store_updates AS
SELECT *
FROM udl.store_updates
QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY updated_at DESC) = 1;

-- Explicit columns and the NULL-key branch first: the emulator's UNION ALL mis-maps columns when the first branch
-- selects u.store_id twice (merge_key and store_id), which NULLed store_id. Row order is irrelevant to the MERGE.
-- merge_key = store_id closes the current version, merge_key = NULL inserts the new version of a changed store
CREATE OR REPLACE TABLE pds_rsi._stg_dim_store AS
SELECT CAST(NULL AS STRING) AS merge_key, u.store_id, u.retailer, u.country, u.region, u.store_format, u.open_date, u.updated_at
FROM pds_rsi._stg_store_updates u
JOIN pds_rsi.dim_store d
  ON u.store_id = d.store_id AND d.is_current
 AND (u.retailer <> d.retailer OR u.region <> d.region OR u.store_format <> d.store_format)
UNION ALL
SELECT u.store_id AS merge_key, u.store_id, u.retailer, u.country, u.region, u.store_format, u.open_date, u.updated_at
FROM pds_rsi._stg_store_updates u;
