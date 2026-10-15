# forecastaccuracy: migration notes

ADF `PL_RSI_ForecastAccuracy` (trigger `TR_RSI_Weekly_Mon_0600`) → DAG `pds_rsi_forecastaccuracy`. Upstream unit: `salesdaily` (`pds_rsi.silver_sales`).

| ADF activity | Composer / BigQuery |
|---|---|
| `NB_Forecast_Accuracy` → `nb_forecast_accuracy` | `gold_forecast_accuracy` (`01_gold_forecast_accuracy.sql`) |
| `Copy_To_DemandPlanningDB` (Copy, Azure SQL upsert on `sku, country`) | `copy_to_demand_planning_db` `BigQueryToPostgresOperator` (Cloud SQL, `replace=True`, `replace_index=[sku, country]`). Not replayed |
| `Notify_Failure` webhook | `DEFAULT_ARGS.on_failure_callback`. Not replayed |
| `run_date` pipeline parameter | `RUN_DATE` (`{{ params.run_date or ds }}`) |

## Semantic differences handled
* Weeks: Spark `date_trunc('week', txn_date)` is Monday-based, so the SQL uses `DATE_TRUNC(txn_date, WEEK(MONDAY))`. The BigQuery default `WEEK` starts on Sunday.
* Latest forecast version: the Spark `Window` + `row_number() == 1` became `QUALIFY ROW_NUMBER() OVER (...) = 1`. It partitions on the normalized SKU (`UPPER(TRIM(sku))`, from `normalize_sku`), the same as the notebook. The `week_start <= run_date` filter is applied after the dedup, as in the notebook.
* `ape = abs_error / actual_units` and the `bias_pct` / `accuracy_pct` ratios: Spark (ANSI off) returns NULL on `/ 0`, so the SQL uses `SAFE_DIVIDE`. 140 to 142 rows per column are NULL in both systems.
* Rounding: `CAST(ROUND(CAST(x AS NUMERIC), 2) AS FLOAT64)` is used wherever the notebook calls `F.round`. There were no `rounding_boundary` rows on the emulator.

## Not replayed by the harness
* `copy_to_demand_planning_db` writes to an external DB. It needs the Airflow connection `demand_planning_db` and the `apache-airflow-providers-postgres` package (not yet in `requirements.txt`).
* `Notify_Failure` is handled by the shared failure callback.

## Parity (BigQuery emulator, run_date 2024-04-30)
`gold_forecast_accuracy`: 790 legacy rows and 790 BigQuery rows. All 8 columns match (checksums identical, no rounding ties).
This run used a diagnostic wrapper. `make parity UNIT=forecastaccuracy` itself currently crashes in `tools/parity.py stage_inputs` before running any SQL: the baseline schema for `pds_rsi.silver_sales` already contains `_load_ts`, and the harness appends a second one. That needs a shared fix in `tools/`.
