# marketshare: migration notes

ADF `PL_RSI_MarketShare` (trigger `TR_RSI_Daily_0200`) → DAG `pds_rsi_marketshare`.

| ADF activity | Composer / BigQuery |
|---|---|
| `NB_Market_Share` (`DatabricksNotebook`) → `nb_market_share` | `gold_market_share` (`01_gold_market_share.sql`) |
| `%run ./nb_common_utils` (`write_delta` adds `_load_ts`) | `CURRENT_TIMESTAMP() AS _load_ts` as the last column |
| trigger `TR_RSI_Daily_0200` | `schedule=None`; ordered by the master DAG after `productdim`, `salesdaily`, `storedimscd2` |
| failure handling (none in pipeline JSON; product convention) | `DEFAULT_ARGS.on_failure_callback` (not replayed) |

## Semantic differences handled
* `%sql` temp view `v_sales_enriched` became a CTE. `CAST(date_trunc('month', txn_date) AS DATE)` → `DATE_TRUNC(txn_date, MONTH)`.
* SCD2 store dimension: kept the `s.is_current` join filter.
* Rounding: every `F.round` (brand revenue 2dp, category revenue 2dp, value share 3dp) uses `CAST(ROUND(CAST(x AS NUMERIC), n) AS FLOAT64)`.
  Category revenue is the rounded sum of the already-rounded brand revenues, as in the notebook.
* `100 * brand / category` uses `SAFE_DIVIDE` (Spark returns NULL on x/0 with ANSI off).
* `F.dense_rank().over(cat.orderBy(desc))` → `DENSE_RANK() OVER (PARTITION BY ... ORDER BY brand_revenue_eur DESC)` (`int` → INT64).
* Emulator/ZetaSQL: a CTE named `category` makes `category` in `PARTITION BY` resolve to the CTE row struct
  ("Partitioning by expressions of type STRUCT containing DOUBLE"), so the CTE is named `brand_category`. Also valid on real BigQuery.

## Parity (BigQuery emulator, run_date 2024-04-30)
`gold_market_share` 800 legacy rows, 800 BigQuery rows, all 10 columns and checksums match, 0 rounding-boundary rows.
**Caveat:** `make parity UNIT=marketshare` itself exits non-zero because of a shared harness bug: `tools/parity.py stage_inputs`
adds `_load_ts` to upstream baseline schemas that already contain it, so creating `pds_rsi.dim_product` fails with a duplicate column.
The result above comes from the same harness with only that duplicate column dropped (an out-of-repo monkeypatch). The fix belongs in `tools/`.
In this data every retailer x country x category x month has a single brand (share 100%, rank 1), so ties in `share_rank` and the
divide-by-zero path are not exercised by the baseline.

Not replayed: none of the unit's activities have external side effects.
