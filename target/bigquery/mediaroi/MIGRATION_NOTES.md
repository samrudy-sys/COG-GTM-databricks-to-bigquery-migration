# mediaroi: migration notes

ADF `PL_RSI_MediaROI` → DAG `pds_rsi_mediaroi`. Upstream unit: `salesdaily` (`pds_rsi.gold_sales_daily`).

| ADF activity | Composer / BigQuery |
|---|---|
| `NB_Media_Weekly` → `nb_media_weekly` | `silver_media_weekly` (`01_silver_media_weekly.sql`) |
| `NB_Media_ROI` → `nb_media_roi` | `gold_media_roi` (`02_gold_media_roi.sql`) |
| `Notify_Failure` webhook (Logic App) | `DEFAULT_ARGS.on_failure_callback` (not replayed) |
| pipeline parameter `run_date` | `RUN_DATE` (`{{ params.run_date or ds }}`) |

## Semantic differences handled
* Week truncation: Spark `date_trunc('week', d)` is Monday-based, so it becomes `DATE_TRUNC(d, WEEK(MONDAY))` (plain `WEEK` is Sunday-based). This is applied in both notebooks so the media and revenue weeks still join.
* Pivot: `.pivot("media_channel", CHANNELS).agg(round(sum(spend_eur), 2)).na.fill(0.0)` becomes conditional aggregation. `SUM(IF(media_channel = 'tv', spend_eur, NULL))` is rounded and then wrapped in `COALESCE(.., 0.0)`. Missing pivot cells are NULL in Spark, but the notebook's `na.fill` turns them into 0.0.
* Dynamic SQL: the f-string `'{run_date}'` and `{PDS_SCHEMA}` interpolation became the Jinja `RUN_DATE` and literal `pds_rsi.` references. The temp view `v_weekly_brand_revenue` became a CTE.
* Rounding: every `F.round` / SQL `ROUND` becomes `CAST(ROUND(CAST(x AS NUMERIC), n) AS FLOAT64)`, which gives Spark's HALF_UP on the decimal value.
* Division: `COALESCE(rev, 0) / total_spend_eur` uses `SAFE_DIVIDE`, because Spark with ANSI off returns NULL on x/0. `cpm_eur` keeps `NULLIF(impressions, 0)` inside `SAFE_DIVIDE`.
* Types: `SUM(impressions)` and `SUM(net_units)` are INT64 (Spark bigint). Spend and revenue are FLOAT64.

## Not replayed by the harness
* `Notify_Failure` (external webhook). It is covered by the shared failure callback.

## Parity (BigQuery emulator, run_date 2024-04-30)
Unmodified `make parity UNIT=mediaroi` on this branch (with `origin/main` merged in, which includes the shared `_load_ts` staging fix) exits 0.
`silver_media_weekly` has 624/624 rows and `gold_media_roi` has 624/624 rows. Every value matches, with 0 rounding-boundary rows. The report is `reports/parity/mediaroi.json`.
