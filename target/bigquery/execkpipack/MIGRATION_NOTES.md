# execkpipack: migration notes

ADF `PL_RSI_ExecKPIPack` → DAG `pds_rsi_execkpipack`.

| ADF activity | Composer / BigQuery |
|---|---|
| `NB_Exec_KPI_Pack` → `nb_exec_kpi_pack` | `gold_exec_kpi_monthly` (`01_gold_exec_kpi_monthly.sql`) |
| `Refresh_PowerBI_ExecPack` WebActivity | `refresh_powerbi_execpack` `HttpOperator` on connection `powerbi_api` (not replayed) |
| `Notify_Failure` webhook | `DEFAULT_ARGS.on_failure_callback` (not replayed) |
| `run_date` parameter | `{{ params.run_date or ds }}` |

## Semantic differences handled
* Rounding: every `F.round` became `CAST(ROUND(CAST(x AS NUMERIC), n) AS FLOAT64)` (`net_revenue_eur`, `otif_pct`, `media_spend_eur`, `revenue_to_media_ratio`).
* Division: the weighted `otif_pct` and `revenue_to_media_ratio` use `SAFE_DIVIDE`, because Spark (ANSI off) returns NULL on x/0. `media_spend_eur` is coalesced to 0.0 before the ratio, so months without media give a NULL ratio, as in Spark.
* `F.date_trunc("month", ...).cast("date")` became `DATE_TRUNC(d, MONTH)`.
* The Spark `full_outer` join on `["region", "month"]` coalesces the join keys. This is written as an explicit `COALESCE(r.key, o.key)` over `FULL OUTER JOIN ... ON`.
* `month <= lit(run_date)`, a date compared with a string, became `month <= DATE('{{ params.run_date or ds }}')`.

## Not replayed by the harness
* The Power BI refresh (`HttpOperator`) and the failure webhook.

## Parity (BigQuery emulator, run_date 2024-04-30)
`gold_exec_kpi_monthly`: 24 legacy rows, 24 BigQuery rows. All values and checksums match, with no rounding-boundary rows.
**Caveat:** `make parity UNIT=execkpipack` currently crashes before it runs the DAG. `tools/parity.py stage_inputs` appends `_load_ts`
to baseline schemas that already contain it, so the emulator raises "Duplicate column name _load_ts". This affects every unit that reads an
upstream `pds_rsi.*` table. The PASS result in `reports/parity/execkpipack.json` comes from the unchanged harness plus an out-of-tree patch that
only drops the duplicate `_load_ts` when staging upstream tables. The comparison was not changed. Re-run `make parity` once `tools/` is fixed.
