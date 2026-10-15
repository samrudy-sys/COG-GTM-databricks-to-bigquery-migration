# storedimscd2: migration notes

ADF `PL_RSI_StoreDimSCD2` (notebooks `nb_store_dim_init`, `nb_store_dim_scd2`) → DAG `pds_rsi_storedimscd2`.

| ADF activity | Composer / BigQuery |
|---|---|
| parameter `full_reload` (bool, default false) | DAG param `full_reload` (boolean, default false) |
| `If_Full_Reload` (IfCondition) | `check_full_reload` `BranchPythonOperator` → `dim_store_init` or `skip_init` (not replayed) |
| `nb_store_dim_scd2`: `if not tableExists(dim_store): dbutils.notebook.run(nb_store_dim_init)` | folded into `check_full_reload`: it also branches to init when `pds_rsi.dim_store` does not exist (`BigQueryHook.table_exists`) |
| ifTrue `NB_Store_Dim_Init` → `nb_store_dim_init` | `dim_store_init` (`01_dim_store_init.sql`) |
| ifFalse (no activities) | `skip_init` (`EmptyOperator`, not replayed) |
| `NB_Store_Dim_SCD2` → `nb_store_dim_scd2` | `stg_dim_store` (`02_stg_dim_store.sql`, `trigger_rule=none_failed_min_one_success`) >> `dim_store_scd2` (`03_dim_store_scd2.sql`) |
| activity policy retry 2 / 300s, timeout 2h | `default_args` `retries=2`, `retry_delay=300s`, `execution_timeout=2h` |
| trigger `TR_RSI_Daily_0200` (via `PL_RSI_Master_Daily`) | triggered by the master DAG (`schedule=None`) |

The harness renders and runs every `BigQueryInsertJobOperator` in topological order, so `dim_store_init` always runs there.
That matches the legacy replay: `full_reload=false`, but `dim_store` did not exist yet, so the notebook chained into `nb_store_dim_init`.

## Semantic differences handled
* `DROP TABLE` + Delta overwrite in `nb_store_dim_init` → `CREATE OR REPLACE TABLE pds_rsi.dim_store AS`.
* `concat_ws('-', store_id, date_format(ts, 'yyyyMMddHHmmss'))` → `ARRAY_TO_STRING([store_id, FORMAT_TIMESTAMP('%Y%m%d%H%M%S', ts, 'UTC')], '-')`.
  `ARRAY_TO_STRING` skips NULL elements like `concat_ws` (plain `CONCAT` would return NULL). The legacy session timezone is UTC, so the format is pinned to UTC.
* `Window.partitionBy(store_id).orderBy(updated_at desc)` + `row_number() == 1` → `QUALIFY ROW_NUMBER() OVER (...) = 1` (last `updated_at` wins for late duplicates).
* Delta `MERGE` (SCD2 with the `merge_key` / `NULL merge_key` union trick) → BigQuery `MERGE`. `UPDATE SET t.col` → `UPDATE SET col` (BigQuery does not allow the target alias in SET).
  The source is staged as `pds_rsi._stg_store_updates` and `pds_rsi._stg_dim_store` and dropped after the MERGE.
* `timestamp'9999-12-31 00:00:00'` → `TIMESTAMP '9999-12-31 00:00:00'`. `true`/`false` → `TRUE`/`FALSE`.
* `<>` on the tracked attributes keeps Spark's NULL semantics (a NULL attribute never counts as a change) in both engines.

## Emulator workarounds (also valid on real BigQuery)
* Staging + MERGE + `DROP` of the staging tables in a single script crashes the emulator (`nil pointer dereference`), so staging is its own task (`stg_dim_store`), and the MERGE task drops the staging tables.
* `UNION ALL` mis-maps columns when the first branch selects `u.store_id` twice (as `merge_key` and as `store_id`): `store_id` came back NULL for the second branch.
  The fix: explicit column lists (no `u.*`) and the `NULL merge_key` branch first. Row order does not matter to the MERGE.

## Not replayed by the harness
* `check_full_reload` (`BranchPythonOperator`) and `skip_init` (`EmptyOperator`). Only the init path is exercised. The `full_reload=false` + existing-table path (the MERGE on an existing dimension without init) is not.

## Parity (BigQuery emulator, run_date 2024-04-30)
`dim_store` 77 legacy rows, 77 BigQuery rows. PASS (`reports/parity/storedimscd2.json`).
