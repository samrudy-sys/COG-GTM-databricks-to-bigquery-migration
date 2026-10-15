---
name: migrate-pipeline
description: Migrate one pds-rsi migration unit (an ADF parent pipeline + its child pipelines and Databricks notebooks) to BigQuery SQL + a Cloud Composer DAG, and prove data parity against the legacy run. Use for any unit listed in migration/scope/inventory.json.
---

# Migrate one ADF + Databricks unit to BigQuery + Composer

This is the codified version of the reference migration of `salesdaily`
(`target/bigquery/salesdaily/`, `target/composer/dags/pds_rsi_salesdaily.py`).
Copy its conventions exactly. Do not invent new ones.

## Inputs

- `UNIT`: a unit id from `migration/scope/inventory.json` (e.g. `mediaroi`).
- That unit's inventory entry gives the member pipelines, notebooks, tables it reads/writes, flagged constructs and findings.

## Setup (once per machine)

```bash
make setup        # python venv with airflow + google provider (skipped if present)
make emulator     # BigQuery emulator on :9050 (docker)
make inventory    # regenerate migration/scope/inventory.json
```

The legacy baseline (`migration/baseline/*.json.gz`) is committed, so you do not need Spark to run parity.
If you change anything under `legacy/`, run `make baseline` (needs Java 17 + PySpark).

## Procedure

1. **Read the source.** Open every pipeline JSON in `legacy/adf/pipeline/` for the unit, every notebook it runs (including `%run` and `dbutils.notebook.run` children), and the unit's `findings` in `migration/scope/INVENTORY.md`.
2. **Map every ADF activity** to a Composer construct. Put the mapping table in the DAG docstring:
   | ADF | Composer |
   |---|---|
   | `DatabricksNotebook` | one or more `bq_sql(...)` tasks (one SQL file per output table) |
   | `ExecutePipeline` (child) | inline the child's tasks in the same DAG (TaskGroup if more than 2 tasks) |
   | `ForEach` over a parameter list | dynamic task mapping or a loop that builds one task per item. Write each item's partition with `DELETE ... WHERE region = ...` then `INSERT` (this replaces Delta `replaceWhere`) |
   | `IfCondition` | `BranchPythonOperator` / `ShortCircuitOperator` |
   | `Lookup` against Azure SQL `ctl.*` | read `ctl.*` in BigQuery inline in the SQL |
   | `SqlServerStoredProcedure` | a `bq_sql` task (UPDATE/MERGE on `ctl.*`) |
   | `GetMetadata` / file check | `GCSObjectExistenceSensor` |
   | `Copy` to Azure SQL | `BigQueryToPostgresOperator` (Cloud SQL); not replayed by the harness |
   | Power BI refresh `WebActivity` | `HttpOperator` on connection `powerbi_api`; not replayed by the harness |
   | `Notify_Failure` WebActivity | already handled by `DEFAULT_ARGS.on_failure_callback` |
   | trigger parameters / `run_date` | `RUN_DATE` (`{{ params.run_date or ds }}`) |
3. **Write SQL** in `target/bigquery/<UNIT>/NN_<table>.sql`, with one `CREATE OR REPLACE TABLE pds_rsi.<table> AS ...` per legacy output table.
   - Keep the legacy column names, column order and types. Add `CURRENT_TIMESTAMP() AS _load_ts` last.
   - Reference tables as `udl.x`, `bdl.x`, `pds_rsi.x`, `ctl.x`. Do not hard-code a project.
   - Start each file with a comment naming the notebook it came from.
4. **Write the DAG** at `target/composer/dags/pds_rsi_<UNIT>.py` using `pds_rsi_common.dag_kwargs("<UNIT>", ...)` and `bq_sql(...)`. Do not edit `pds_rsi_common.py`; it is shared by all units. If you truly need a change there, report it in your output and do not make it.
5. **Run parity until it passes:** `make parity UNIT=<UNIT>` (or `.venv/bin/python tools/parity.py run <UNIT>`).
   It seeds the emulator with the unit's source tables, plus upstream tables from the legacy baseline. It runs every `BigQueryInsertJobOperator` in DAG order and diffs every output table against the legacy Delta output: row count, column set, types, and every value (1e-9 tolerance, plus one-unit rounding ties; see the traps below).
   The report lands in `reports/parity/<UNIT>.json`. Commit it.
6. **Record decisions** in `target/bigquery/<UNIT>/MIGRATION_NOTES.md`: the activity mapping, each semantic difference you handled, and anything not replayed by the harness.

## Spark → BigQuery semantic traps (check every one)

- `date_trunc('week', d)` is Monday-based. BigQuery `DATE_TRUNC(d, WEEK)` is Sunday-based, so use `WEEK(MONDAY)`.
- With ANSI off, Spark returns `NULL` for `x / 0`. BigQuery errors, so use `SAFE_DIVIDE`.
- **Rounding (the #1 parity breaker):** Spark `round(double, n)` rounds the *decimal string* of the double HALF_UP (2.13405 → 2.1341), while BigQuery `ROUND(FLOAT64)` rounds the binary value (2.13404999… → 2.134). Always write `CAST(ROUND(CAST(x AS NUMERIC), n) AS FLOAT64)` wherever the notebook calls `F.round` / SQL `ROUND`. That is exact on production BigQuery. The emulator stores NUMERIC as a float, so a few exact-tie rows still differ by one unit there. `compare.py` reports them as `rounding_boundary` and tolerates them up to 2% of rows on the emulator only. Anything above that is a real bug.
- `date_format(d, 'yyyy-MM')` maps to `FORMAT_DATE('%Y-%m', d)`. Translate Java patterns carefully (`MM`=month, `mm`=minute, `EEE`=`%a`).
- `sha2(concat_ws('|', a, b), 256)` maps to `TO_HEX(SHA256(CONCAT(a, '|', b)))`. Note that `concat_ws` skips NULLs and `CONCAT` returns NULL.
- `rangeBetween(-(n-1)*86400, 0)` on a timestamp maps to a `RANGE BETWEEN` on `UNIX_DATE(date)` with day offsets.
- `countDistinct` → `COUNT(DISTINCT)`. `approx_count_distinct` must NOT become `APPROX_COUNT_DISTINCT` when parity is exact. Use `COUNT(DISTINCT)` and note it.
- Python UDFs must be rewritten as SQL `CASE` / expressions. Port the exact branch order and thresholds.
- `pivot` maps to conditional aggregation `SUM(IF(channel = 'tv', spend, 0))`, or BigQuery `PIVOT`. Missing pivot cells are `NULL` in Spark, not 0.
- Delta `MERGE` (SCD2) maps to BigQuery `MERGE`. In the emulator the MERGE source must be a physical table, so stage it with `CREATE OR REPLACE TABLE pds_rsi._stg_<x>` first and drop it afterwards.
- Spark `int`/`long` column → `INT64`. `double` → `FLOAT64`. Do not use `NUMERIC`.

## BigQuery emulator quirks (local only, not real BigQuery)

- There is no unary minus: write `0 - x`, not `-x`.
- `CREATE OR REPLACE TABLE` on an existing table fails. The harness pre-issues `DROP TABLE IF EXISTS`, so keep the statement form exactly `CREATE OR REPLACE TABLE pds_rsi.<name> AS`.
- The `MERGE` source must be a single table reference (see above).
- Inequality joins are slow (around 60s for the FX as-of join). That is expected.
- If a construct is unsupported, find an equivalent standard-SQL rewrite that also runs on real BigQuery, and note it in MIGRATION_NOTES.md.

## Done when

- `make parity UNIT=<UNIT>` exits 0 and `reports/parity/<UNIT>.json` has `"passed": true`.
- `make lint` is clean.
- The DAG docstring maps every ADF activity, including ones the harness does not replay.
- The sign-off report was checked in a browser (step 7), and the recording and screenshot are on the PR.
