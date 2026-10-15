# promoeffectiveness: migration notes

ADF `PL_RSI_PromoEffectiveness` → DAG `pds_rsi_promoeffectiveness`.

| ADF activity | Composer / BigQuery |
|---|---|
| `NB_Promo_Uplift` → `nb_promo_uplift` (`baseline_days=28`) | `gold_promo_uplift` (`01_gold_promo_uplift.sql`); `baseline_days` is a DAG param (default 28) |
| `Notify_Failure` webhook | `DEFAULT_ARGS.on_failure_callback` (not replayed) |
| pipeline parameter `run_date` | `{{ params.run_date or ds }}` |

## Semantic differences handled
* `normalize_sku` (`upper(trim(sku))`) from `nb_common_utils` inlined as `UPPER(TRIM(sku))`.
* `datediff(end, start) + 1` → `DATE_DIFF(end_date, start_date, DAY) + 1`; `date_sub(start, n)` → `DATE_SUB(start_date, INTERVAL n DAY)`.
* Left join with no matching sales: Spark `when(NULL).otherwise(0)` yields 0; `IF(NULL, x, 0)` does the same, so promo/baseline units are 0 (not NULL) for promos without sales.
* `~in_promo` with a NULL `txn_date` is NULL in both engines, so `IF(NOT (...), qty, 0)` keeps parity.
* Divide-by-zero (`baseline_daily_units = 0`) returns NULL in Spark (ANSI off): `SAFE_DIVIDE`, which drives the `no_baseline` verdict.
* `uplift_pct` is computed from the already rounded daily averages, as in the notebook.
* Rounding: every `F.round` became `CAST(ROUND(CAST(x AS NUMERIC), n) AS FLOAT64)`.

## Parity (BigQuery emulator, run_date 2024-04-30)
`gold_promo_uplift`: 80 legacy rows, 80 BigQuery rows, all 15 columns and values match, 0 rounding-boundary rows.
This was measured with the harness's upstream-staging bug patched in memory (see below). Unpatched, `make parity` cannot stage `pds_rsi.silver_sales_eur`.

## Harness blocker (shared, not changed here)
`tools/parity.py` `stage_inputs` appends `_load_ts` to upstream schemas that already contain `_load_ts` (from the baseline snapshot).
The emulator then rejects `CREATE TABLE pds_rsi.silver_sales_eur` with "Duplicate column name _load_ts". Every unit that reads an upstream `pds_rsi.*` table hits this.
