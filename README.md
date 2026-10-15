# Databricks → BigQuery migration, done by Devin

This demo shows Devin migrating a whole data product from **Azure Data Factory + Azure Databricks (PySpark, Delta)** to
**BigQuery + Cloud Composer (Airflow)**. It covers the full lifecycle, and the per-pipeline work runs in parallel:

| Phase | What happens | Where |
|---|---|---|
| Discover | Parse every ADF pipeline, trigger, dataset and notebook. Build the lineage graph, migration units, waves and findings (race conditions, UDFs, SCD2, semantic traps) | `tools/inventory.py` → `migration/scope/` |
| Walk | One reference migration done interactively (SalesDaily: watermark, sensor, as-of FX join, aggregation, stored proc) | `target/bigquery/salesdaily/`, `target/composer/dags/pds_rsi_salesdaily.py` |
| Jog | The pattern is codified as a playbook | `.devin/skills/migrate-pipeline/SKILL.md` |
| Run | A dynamic workflow runs one Devin per ADF parent pipeline in parallel. Each writes SQL + a DAG, proves parity and opens a PR. An integrator then merges them, writes the master DAG and builds the sign-off report | `.devin/skills/migrate-product/workflow.py` |
| Sign-off | Row/column/value parity between the legacy run and BigQuery, per table, plus an end-to-end run | `tools/parity.py`, `reports/index.html` |

## The legacy estate (`legacy/`)
The product is fictional: `pds-rsi`, a "Retail Sales Intelligence" product modeled on a CPG analytics estate. It has 16 ADF pipelines (a master pipeline,
child pipelines, ForEach fan-out, IfCondition, Lookup watermarks, stored procedures, Power BI refresh, failure webhooks), 17 Databricks
notebooks (`%run`, `dbutils.notebook.run`, Python UDF, Delta MERGE/SCD2, `replaceWhere`, window/range frames, pivot, as-of joins) and
12 messy source tables (deterministic seed data).

## Parity: how "same result as Azure" is proven
* `make baseline` replays the *legacy* estate locally: ADF JSON is interpreted activity by activity, and the notebooks run on Spark 3.5 + Delta 3.2.
  Every product table is snapshotted to `migration/baseline/`.
* `make parity UNIT=<unit>` loads sources and upstream tables into BigQuery, runs that unit's Composer DAG task by task, and diffs every
  output table against the baseline: row count, columns, type family, every value, and per-column checksums.
* `make integration` runs all unit DAGs in wave order from raw sources only.

> **Target used here: the open-source [BigQuery emulator](https://github.com/goccy/bigquery-emulator) (`localhost:9050`), not
> production BigQuery.** It exercises the same SQL dialect and REST API, so no GCP project is needed. Set `BQ_ENDPOINT`/`BQ_PROJECT` to point
> the same harness at real BigQuery. Activities with external side effects (Power BI refresh, Cloud SQL copy, webhooks, GCS sensors) are
> mapped in the DAGs but not replayed.

## Quick start
```bash
make setup-legacy   # venv: Airflow 2.10 + Google provider + PySpark/Delta (Java 17 required for the legacy replay)
make emulator       # BigQuery emulator in Docker
make inventory      # discover the estate -> migration/scope/INVENTORY.md
make baseline       # replay legacy ADF + Databricks (~1 min)
make parity UNIT=salesdaily
make report         # reports/index.html
```

See [`docs/DEMO_RUNBOOK.md`](docs/DEMO_RUNBOOK.md) for the live demo script.
