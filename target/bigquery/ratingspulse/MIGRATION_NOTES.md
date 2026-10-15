# ratingspulse: migration notes

ADF `PL_RSI_RatingsPulse` (trigger `TR_RSI_Daily_0200`) → DAG `pds_rsi_ratingspulse`.

| ADF activity | Composer / BigQuery |
|---|---|
| `NB_Ratings_Pulse` → `nb_ratings_pulse` (`DatabricksNotebook`) | `gold_ratings_pulse` (`01_gold_ratings_pulse.sql`) |
| baseParameter `window_days` = `30` | DAG param `window_days` (default 30), Jinja `params.window_days` |
| pipeline parameter `run_date` | `{{ params.run_date or ds }}` |
| Trigger `TR_RSI_Daily_0200` | `schedule=None` (orchestrated by the master DAG, as for every unit) |
| Activity policy retry 2 | `DEFAULT_ARGS` retries |
| Failure notification | `DEFAULT_ARGS.on_failure_callback` (not replayed) |

## Semantic differences handled
* `normalize_sku` (`%run ./nb_common_utils`) became `UPPER(TRIM(sku))`.
* `F.datediff(review_date, '1970-01-01')` became `UNIX_DATE(review_date)`.
* Rolling window `Window.partitionBy("sku").orderBy("day_num").rangeBetween(-(window_days-1), 0)` is a frame of
  `window_days` calendar days, not rows. On real BigQuery this is `RANGE BETWEEN 29 PRECEDING AND CURRENT ROW` over
  `UNIX_DATE(review_date)`. The emulator (sqlite) rejects a `RANGE` offset frame with `PARTITION BY` ("RANGE with offset
  PRECEDING/FOLLOWING requires one ORDER BY expression"), so the SQL uses an equivalent self-join on
  `h.sku = d.sku AND h.day_num BETWEEN d.day_num - 29 AND d.day_num` + `GROUP BY`. It's standard SQL and gives the same result on real BigQuery.
* Rounding: `F.round(x, 3)` / `F.round(x, 1)` became `CAST(ROUND(CAST(x AS NUMERIC), n) AS FLOAT64)` (Spark HALF_UP on the decimal value).
* Division: `SAFE_DIVIDE` matches Spark's NULL on divide-by-zero (`reviews_30d` is always ≥ 1 here, so no rows are affected).
* `rating <= 3` / `rating == 5` `F.when(...).otherwise(0)` became `SUM(IF(...))`.

## Not replayed by the harness
* Failure callback / webhook.

## Parity (BigQuery emulator, run_date 2024-04-30)
`gold_ratings_pulse`: legacy 2,713 rows, BigQuery 2,713 rows. PASS. No rounding-boundary rows (`reports/parity/ratingspulse.json`).
