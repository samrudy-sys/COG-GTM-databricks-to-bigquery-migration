# Demo runbook (about 25 min live, plus a pre-recorded workflow run)

**Story:** "You have 200+ data products on ADF + Databricks. About 80% of the pipelines follow a handful of patterns. Devin learns the pattern
on one pipeline (walk), you codify it (jog), then Devin runs it across the whole product in parallel with parity evidence (run)."

## 0. Before the call
* Start the BigQuery emulator: `make emulator`. Then `make baseline` and `make report`.
* Kick off the workflow about an hour before the call (it takes about 30 to 40 min). Ask Devin: *"Run the migrate-product workflow in
  `.devin/skills/migrate-product/workflow.py`."* Have the workflow run page and the PR list open in tabs.

## 1. The legacy estate (3 min)
* `legacy/adf/pipeline/PL_RSI_Master_Daily.json`: master pipeline, ExecutePipeline children, ForEach over regions, Power BI refresh, failure webhook.
* `legacy/databricks/notebooks/nb_store_dim_scd2.py` (Delta MERGE SCD2) and `nb_product_dim.py` (Python UDF): "this is the messy 20%."

## 2. Discover (3 min)
* `make inventory`, then open `migration/scope/INVENTORY.md`. Show the Mermaid lineage, 12 units, 3 waves and the findings.
* Call out the **race condition**: ADF runs InventoryHealth in parallel with SalesDaily, but it reads SalesDaily's output.
  Devin found this from lineage, not from the JSON `dependsOn`.

## 3. Walk: the reference migration (5 min)
* `target/composer/dags/pds_rsi_salesdaily.py`: the docstring maps each ADF activity to its Airflow equivalent.
* `target/bigquery/salesdaily/*.sql`: watermark lookup to `ctl.watermarks`, as-of FX join, `QUALIFY` dedup.
* `make parity UNIT=salesdaily`: live, about 2 min. Show the rounding story: Spark rounds the decimal string HALF_UP and BigQuery rounds the binary float.
  The harness caught this and the playbook now encodes it.

## 4. Jog: the playbook (2 min)
* `.devin/skills/migrate-pipeline/SKILL.md`: the steps, the Spark-to-BigQuery semantic traps and the done criteria. This is where customer
  engineers put their standards.

## 5. Run: the dynamic workflow (7 min)
* `.devin/skills/migrate-product/workflow.py`: the fan-out list comes from `inventory.json`, there is one Devin per pipeline, and an integrator runs after them.
* Workflow run page: 11 parallel sessions, each with a PR and a parity table in the body. Open one hard PR (storedimscd2 or productdim).
* Integration PR: master DAG ordered by lineage (race fixed), plus end-to-end parity.

## 6. Sign-off (3 min)
* `reports/index.html`: KPIs, wave diagram, per-table parity, column checksums, findings, sign-off block.
* Be explicit: "This evidence is from the BigQuery emulator. In your environment we point the same harness at your GCP project."

## Talking points
* The output is reviewable code plus evidence in the customer's repo. The parity harness is the contract.
* Scale: per product, discovery takes minutes and the migrate phase runs every unit at once. Wall clock goes from weeks to hours.
* Honest limits: side-effect activities (Power BI, webhooks, Cloud SQL copy) are mapped, not replayed, and emulator NUMERIC is not exact (see playbook).
