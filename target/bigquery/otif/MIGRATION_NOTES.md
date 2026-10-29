# otif: migration notes

ADF `PL_RSI_OTIF` (+ child `PL_RSI_OTIF_Region`) → DAG `pds_rsi_otif`.

| ADF activity | Composer / BigQuery |
|---|---|
| (first `write_delta` creates `gold_otif`) | `create_gold_otif` (`01_create_gold_otif.sql`, `CREATE TABLE IF NOT EXISTS`, clustered by `region`) |
| `ForEach_Region` over `regions` (parallel, batchCount 3) | one task per region in TaskGroup `otif_region`, loop over `REGIONS`; `max_active_tasks=3` |
| `Exec_OTIF_Region` → `PL_RSI_OTIF_Region` (ExecutePipeline) | inlined into the `otif_region` TaskGroup |
| `NB_OTIF_KPIs` → `nb_otif_kpis` | `otif_region.gold_otif_<region>` (`02_gold_otif_region.sql`, `params.region`) |
| `Notify_Failure` webhook | `DEFAULT_ARGS.on_failure_callback` (not replayed) |
| parameter `run_date` | `RUN_DATE` (`params.run_date or ds`) |
| parameter `regions` | `REGIONS` constant (the ADF default list). It is fixed when the DAG is parsed, so changing regions means a code change. |

## Semantic differences handled
* **Delta `replaceWhere "region = '<r>'"`** becomes `DELETE ... WHERE region = '<r>'` + `INSERT` inside `BEGIN/COMMIT TRANSACTION`. Each region
  task overwrites only its own region, atomically, as Delta did. Re-running one region leaves the others untouched (checked on the emulator).
  The table is created once with `CREATE TABLE IF NOT EXISTS`, not `CREATE OR REPLACE`, so that a single-region run cannot drop other regions.
  BigQuery cannot partition on STRING, so the table uses `CLUSTER BY region`.
* **Rounding:** `F.round(100 * avg(...), 2)` and `F.round(sum/sum, 4)` become `CAST(ROUND(CAST(x AS NUMERIC), n) AS FLOAT64)`.
  The emulator found 0 rounding-boundary rows.
* `sum(shipped) / sum(ordered)` becomes `SAFE_DIVIDE` (Spark with ANSI off returns NULL on /0).
* `coalesce(delivered_date <= promised_date, false)` and `on_time & in_full` keep the same three-valued logic in BigQuery. `AVG(CAST(bool AS INT64))` ignores NULLs, as Spark's `avg` does.
* `date_trunc('month', promised_date).cast('date')` becomes `DATE_TRUNC(promised_date, MONTH)`.
* `count(*)`/`sum(int)` (Spark bigint) become INT64.

## Emulator notes (local only)
* On the emulator, `CREATE TABLE IF NOT EXISTS` errors if the table already exists. That is an emulator bug; real BigQuery is a no-op. The harness resets
  datasets before each run, so it is not hit there.

## Not replayed by the harness
* `Notify_Failure` (failure callback). The ForEach concurrency limit (`max_active_tasks=3`) is a scheduler setting, so the harness, which runs tasks sequentially, does not exercise it.

## Parity (BigQuery emulator, run_date 2024-04-30)
`gold_otif` 880 legacy rows, 880 BigQuery rows. PASS, no rounding-boundary rows (`reports/parity/otif.json`).
