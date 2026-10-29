# inventoryhealth: migration notes

ADF `PL_RSI_InventoryHealth` → DAG `pds_rsi_inventoryhealth`.

| ADF activity | Composer / BigQuery |
|---|---|
| `NB_Inventory_Cover` → `nb_inventory_cover` (`ros_days=28`) | `gold_inventory_health` (`01_gold_inventory_health.sql`), `params.ros_days` |
| `Notify_Failure` webhook | `DEFAULT_ARGS.on_failure_callback` (not replayed) |
| `run_date` pipeline parameter | `RUN_DATE` (`{{ params.run_date or ds }}`) |
| trigger `TR_RSI_Daily_0200` | `schedule=None`; run from the master DAG |

## Dependency (finding: race_condition, high)
The notebook reads `pds_rsi.silver_sales` from `pds_rsi_salesdaily`, but the ADF master pipeline ran both in parallel.
The master DAG (owned by the integrator) must order `salesdaily >> inventoryhealth`.

## Semantic differences handled
* `normalize_sku` → `UPPER(TRIM(sku))`. `~is_return` → `NOT is_return` (NULL rows are filtered out in both engines).
* Trailing 28-day range join (`txn_date` in `[snapshot_date - 28, snapshot_date)`, left join, `coalesce(sum(qty), 0)`): on the emulator,
  the inequality join and even the expanded equi-join time out after 300s (no index use). An equivalent window form is used instead:
  daily sales are unioned with a `start` marker at `snapshot_date - ros_days` and an `end` marker at `snapshot_date`
  (markers sort before same-day sales). `units = running_sum(end) - running_sum(start)`. This is also valid on real BigQuery.
  A `RANGE BETWEEN n PRECEDING` frame with `PARTITION BY` fails on the emulator's SQLite ("RANGE with offset requires one ORDER BY expression").
* Duplicate snapshot rows: the notebook groups by `(snapshot_date, store_id, sku, on_hand_units)` after the join, so each duplicate row's
  matched sales are counted again. This is reproduced with `dup_rows * units`.
* Rounding: `CAST(ROUND(CAST(x AS NUMERIC), n) AS FLOAT64)` for `avg_daily_units` (4) and `days_of_cover` (1). `SAFE_DIVIDE` is used for cover.
* `lag(on_hand_units)` is computed on the grouped snapshot rows, before the window over sales. The emulator rejects a second analytic
  function placed after the running-sum window.
* **Legacy nondeterminism in `wow_change_units`:** the seed has 8 store/sku pairs with two snapshot rows per date (same normalized key,
  different `on_hand_units`). Spark's `lag` over `orderBy("snapshot_date")` orders these ties arbitrarily. The migrated SQL adds `on_hand_units`
  as a deterministic tiebreak. 154 of 4,248 rows differ from the baseline, and all of them are in those 8 tied partitions. Every other column and row matches.
  This needs a business decision (dedupe the snapshots, or agree on a tiebreak) before exact parity is possible.

## Parity (BigQuery emulator, run_date 2024-04-30)
`make parity UNIT=inventoryhealth` now runs (the `_load_ts` staging fix is on `main`). Result: 4,248/4,248 rows, every column matches
except `wow_change_units` (154 rows, all in the 8 tied store/sku pairs). **Not passed.**

### Why the legacy `wow_change_units` cannot be reproduced in SQL
The legacy notebook was replayed out-of-repo on PySpark 3.5.3 + Delta 3.2.1 with the harness session (`local[4]`,
`spark.sql.shuffle.partitions=4`, AQE on). That run matches the committed baseline exactly (0 diffs). Changing only the
Spark config changes `wow_change_units` on the tied rows:

| Spark config | rows differing from baseline |
|---|---|
| harness default (`shuffle.partitions=4`, AQE on) | 0 |
| `spark.sql.shuffle.partitions=8` | 93 |
| `spark.sql.shuffle.partitions=1` | 102 |
| `spark.sql.adaptive.enabled=false` | 12 |

The plan is `BroadcastHashJoin -> HashAggregate -> Exchange hashpartitioning(snapshot_date, store_id, sku, on_hand_units, 4)
-> HashAggregate -> Exchange hashpartitioning(store_id, sku, 4) -> Sort(snapshot_date) -> Window(lag)`. The order of tied rows
follows the murmur3 shuffle partition of the 4 group keys (92 of 134 tied dates) and hash-map iteration order inside the
aggregate (the rest). This is a physical-plan artefact, not business logic. It would also differ on the production Databricks cluster.
Copying it into BigQuery SQL would mean reimplementing Spark's murmur3 and its aggregate hash map, so it was not done.

**Decision needed (business owner):** either dedupe `udl.inventory_snapshots` per (snapshot_date, store_id, sku), or agree on a
tiebreak (the SQL uses `ORDER BY snapshot_date, on_hand_units`). Then add the same tiebreak to `nb_inventory_cover` and
regenerate the baseline (a `legacy/` change, outside this unit).
