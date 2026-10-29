#!/usr/bin/env python3
"""Parity harness: proves a migrated unit produces the same data as the legacy estate.

  parity.py baseline            replay the legacy ADF estate on PySpark+Delta, snapshot every PDS table
  parity.py run <unit> [...]    run the unit's Composer DAG SQL on BigQuery, diff against the baseline
  parity.py run --all           whole product, in lineage-wave order (integration test)

Target defaults to the local BigQuery emulator (BQ_ENDPOINT / BQ_PROJECT to override).
"""
import argparse
import gzip
import json
import os
import sys
import time
from datetime import date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from mig.bq import BQ, BQ_TYPES  # noqa: E402
from mig.compare import compare_table  # noqa: E402
from mig.paths import ADF, AUDIT_COLUMNS, DAGS, DEFAULT_RUN_DATE, REPORTS, ROOT, WORK  # noqa: E402
from mig.seeds import seed_rows, table_schemas  # noqa: E402

BASELINE_DIR = ROOT / "migration" / "baseline"
INVENTORY = ROOT / "migration" / "scope" / "inventory.json"
SPARK_TO_BQ = {"bigint": "INTEGER", "int": "INTEGER", "double": "FLOAT", "float": "FLOAT", "string": "STRING",
               "boolean": "BOOLEAN", "date": "DATE", "timestamp": "TIMESTAMP"}


def inventory():
    return json.loads(INVENTORY.read_text())


def _json_value(v):
    if isinstance(v, datetime):
        return v.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(v, date):
        return v.isoformat()
    if hasattr(v, "is_finite"):  # Decimal
        return float(v)
    return v


# ------------------------------------------------------------------ baseline (legacy side)
def cmd_baseline(args):
    from mig import legacy

    t0 = time.time()
    legacy.reset_workspace()
    spark = legacy.spark_session()
    legacy.load_seeds(spark)
    runner = legacy.ADFRunner(spark)
    inv = inventory()
    roots = [p["pipelineReference"]["referenceName"] for t in sorted((ADF / "trigger").glob("*.json"))
             for p in json.loads(t.read_text())["properties"]["pipelines"]]
    for root in roots:
        runner.run_pipeline(root, {"run_date": args.run_date})
    BASELINE_DIR.mkdir(parents=True, exist_ok=True)
    written = sorted({w for u in inv["units"] for w in u["writes"]})
    for table in written:
        df = spark.table(table)
        schema = {f.name: f.dataType.simpleString() for f in df.schema.fields}
        rows = [{k: _json_value(v) for k, v in r.asDict().items() if k != "_load_ts"} for r in df.collect()]
        with gzip.open(BASELINE_DIR / f"{table}.json.gz", "wt") as f:
            json.dump({"table": table, "run_date": args.run_date, "schema": schema, "rows": rows}, f, default=str)
        print(f"  baseline {table:<36} {len(rows):>6} rows")
    (BASELINE_DIR / "_legacy_run.json").write_text(json.dumps({
        "run_date": args.run_date, "triggers": roots, "seconds": round(time.time() - t0, 1),
        "notebook_runs": runner.nb.executed, "activities": runner.trace}, indent=1) + "\n")
    print(f"legacy estate replayed in {time.time() - t0:.0f}s: {len(runner.nb.executed)} notebook runs, {len(written)} tables")


def load_baseline(table):
    with gzip.open(BASELINE_DIR / f"{table}.json.gz", "rt") as f:
        return json.load(f)


# ------------------------------------------------------------------ target side
def _airflow_env():
    home = WORK / "airflow"
    home.mkdir(parents=True, exist_ok=True)
    os.environ.setdefault("AIRFLOW_HOME", str(home))
    os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
    os.environ.setdefault("AIRFLOW__CORE__DAGS_FOLDER", str(DAGS))
    os.environ.setdefault("AIRFLOW__LOGGING__LOGGING_LEVEL", "ERROR")


def load_dag(dag_id, dag_file=None):
    _airflow_env()
    from airflow.models import DagBag

    bag = DagBag(dag_folder=str(dag_file or DAGS), include_examples=False, safe_mode=False)
    if bag.import_errors:
        raise SystemExit("DAG import errors:\n" + "\n".join(f"{k}: {v}" for k, v in bag.import_errors.items()))
    if dag_id not in bag.dags:
        raise SystemExit(f"DAG {dag_id!r} not found in {dag_file or DAGS} (found {sorted(bag.dags)})")
    return bag.dags[dag_id]


def render_bq_tasks(dag, run_date):
    """Yield (task, sql) for BigQueryInsertJobOperator tasks in topological order,
    and (task, None) for every other operator (external side effects not replayed)."""
    from airflow import macros

    for task in dag.topological_sort():
        if task.task_type != "BigQueryInsertJobOperator":
            yield task, None
            continue
        params = {k: (v.value if hasattr(v, "value") else v) for k, v in dict(dag.params).items()}
        params.update({k: v for k, v in task.params.items()} if task.params else {})
        params = {k: (v.value if hasattr(v, "value") else v) for k, v in params.items()}
        params["run_date"] = run_date
        ctx = {"ds": run_date, "ds_nodash": run_date.replace("-", ""), "params": params, "dag": dag, "task": task,
               "macros": macros, "dag_run": None, "var": {"value": {}, "json": {}}}
        task.render_template_fields(ctx, jinja_env=dag.get_template_env())
        yield task, task.configuration["query"]["query"]


def bq_schema_from_spark(schema):
    """Baseline schemas include the audit columns; callers append `_load_ts` themselves."""
    return [(c, SPARK_TO_BQ.get(t, "FLOAT" if t.startswith("decimal") else "STRING"))
            for c, t in schema.items() if c not in AUDIT_COLUMNS]


def stage_inputs(bq, unit, inv, log, upstream_from_baseline=True):
    for ds in ("udl", "bdl", "pds_rsi", "ctl"):
        bq.reset_dataset(ds)
    schemas = table_schemas()
    fixtures = json.loads((ADF / "_harness_fixtures.json").read_text()).get("_bigquery_control_tables", {})
    for name, spec in fixtures.items():
        ds, t = name.split(".")
        bq.create_table(ds, t, spec["schema"])
        bq.insert_rows(ds, t, spec["rows"])
    for table in unit["reads"]:
        ds, t = table.split(".")
        if ds in ("udl", "bdl"):
            cols = schemas[table]
            bq.create_table(ds, t, [(c, BQ_TYPES[ty]) for c, ty in cols.items()])
            rows = list(seed_rows(table))
            bq.insert_rows(ds, t, rows)
            log(f"  seed     {table:<36} {len(rows):>6} rows")
        elif upstream_from_baseline:
            b = load_baseline(table)
            bq.create_table(ds, t, bq_schema_from_spark(b["schema"]) + [("_load_ts", "TIMESTAMP")])
            bq.insert_rows(ds, t, b["rows"])
            log(f"  upstream {table:<36} {len(b['rows']):>6} rows (legacy baseline)")


def run_unit(unit_id, run_date, bq, log=print, stage=True, dag_file=None):
    inv = inventory()
    unit = next((u for u in inv["units"] if u["id"] == unit_id), None)
    if unit is None:
        raise SystemExit(f"unknown unit {unit_id!r}; known: {[u['id'] for u in inv['units']]}")
    t0 = time.time()
    dag_id = f"pds_rsi_{unit_id}"
    dag = load_dag(dag_id, dag_file or (DAGS / f"{dag_id}.py"))
    log(f"[{unit_id}] DAG {dag_id}: {len(dag.tasks)} tasks")
    if stage:
        stage_inputs(bq, unit, inv, log)
    steps = []
    for task, sql in render_bq_tasks(dag, run_date):
        if sql is None:
            steps.append({"task": task.task_id, "operator": task.task_type, "status": "not replayed (external side effect)"})
            log(f"  task     {task.task_id:<36} {task.task_type} (not replayed)")
            continue
        ts = time.time()
        try:
            bq.query(sql)
            steps.append({"task": task.task_id, "operator": task.task_type, "status": "ok", "seconds": round(time.time() - ts, 2)})
            log(f"  task     {task.task_id:<36} ok ({time.time() - ts:.1f}s)")
        except Exception as e:  # report and stop: downstream tasks would fail too
            steps.append({"task": task.task_id, "operator": task.task_type, "status": "error", "error": str(e), "sql": sql})
            log(f"  task     {task.task_id:<36} ERROR {e}")
            break
    results = []
    if all(s["status"] != "error" for s in steps):
        for table in unit["writes"]:
            base = load_baseline(table)
            ds, t = table.split(".")
            try:
                fields, rows = bq.table_rows(ds, t)
            except Exception as e:
                results.append({"table": table, "passed": False, "issues": [f"cannot read target table: {e}"]})
                continue
            r = compare_table(table, base["schema"], base["rows"], fields, rows)
            results.append(r)
            log(f"  parity   {table:<36} {'PASS' if r['passed'] else 'FAIL'} legacy={r['legacy_rows']} bq={r['bigquery_rows']}"
                + ("" if r["passed"] else "\n           " + "\n           ".join(r["issues"])))
    passed = bool(results) and all(r["passed"] for r in results) and all(s["status"] != "error" for s in steps)
    report = {"unit": unit_id, "pipeline": unit["pipeline"], "dag_id": dag_id, "run_date": run_date, "passed": passed,
              "target": f"{bq.base.split('/bigquery')[0]} (project {bq.project})", "seconds": round(time.time() - t0, 1),
              "tasks": steps, "tables": results}
    out = REPORTS / "parity" / f"{unit_id}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=1, default=str) + "\n")
    log(f"[{unit_id}] {'PASSED' if passed else 'FAILED'} in {report['seconds']}s -> {out.relative_to(ROOT)}")
    return report


def cmd_integration(args):
    """Whole product on one BigQuery instance: seeds only, every unit DAG in lineage-wave order,
    each unit reading the tables its upstream DAGs just produced (no baseline shortcuts)."""
    bq = BQ()
    if not bq.ping():
        raise SystemExit(f"BigQuery endpoint {bq.base} unreachable. Start the emulator: make emulator")
    inv = inventory()
    all_reads = sorted({r for u in inv["units"] for r in u["reads"]})
    t0 = time.time()
    stage_inputs(bq, {"reads": all_reads}, inv, print, upstream_from_baseline=False)
    missing = [u["id"] for u in inv["units"] if not (DAGS / f"pds_rsi_{u['id']}.py").exists()]
    reports = []
    for i, wave in enumerate(inv["waves"]):
        print(f"--- wave {i}: {wave}")
        for unit_id in wave:
            if unit_id not in missing:
                reports.append(run_unit(unit_id, args.run_date, bq, stage=False))
    tables = [t for r in reports for t in r["tables"]]
    passed = not missing and all(r["passed"] for r in reports)
    out = {"run_date": args.run_date, "passed": passed, "seconds": round(time.time() - t0, 1), "waves": inv["waves"],
           "missing_units": missing, "units": [{k: r[k] for k in ("unit", "passed", "seconds")} for r in reports],
           "tables_at_parity": sum(t["passed"] for t in tables), "tables_total": len({w for u in inv["units"] for w in u["writes"]}),
           "tables": tables}
    (REPORTS / "parity" / "_integration.json").write_text(json.dumps(out, indent=1, default=str) + "\n")
    print(f"\nintegration: {out['tables_at_parity']}/{out['tables_total']} tables at parity in {out['seconds']}s"
          + (f"; units without a DAG: {missing}" if missing else ""))
    sys.exit(0 if passed else 1)


def cmd_check_dags(args):
    """Every DAG imports; the master DAG (if present) triggers units in an order that respects data lineage."""
    _airflow_env()
    from airflow.models import DagBag

    bag = DagBag(dag_folder=str(DAGS), include_examples=False, safe_mode=False)
    if bag.import_errors:
        raise SystemExit("DAG import errors:\n" + "\n".join(f"{k}: {v}" for k, v in bag.import_errors.items()))
    print(f"{len(bag.dags)} DAGs import cleanly: {sorted(bag.dags)}")
    master = bag.dags.get("pds_rsi_master_daily")
    if not master:
        return
    inv = inventory()
    trig = {t.task_id: t.trigger_dag_id.removeprefix("pds_rsi_") for t in master.tasks if t.task_type == "TriggerDagRunOperator"}
    by_unit = {v: k for k, v in trig.items()}
    problems = []
    for u in inv["units"]:
        if u["id"] not in by_unit:
            continue
        ups = master.get_task(by_unit[u["id"]]).get_flat_relative_ids(upstream=True)
        for up in u["upstream_units"]:
            if up in by_unit and by_unit[up] not in ups:
                problems.append(f"{u['id']} reads from {up} but does not run after it")
    if problems:
        raise SystemExit("master DAG lineage violations:\n  " + "\n  ".join(problems))
    print(f"pds_rsi_master_daily: {len(trig)} unit DAGs, ordering respects data lineage")


def cmd_run(args):
    bq = BQ()
    if not bq.ping():
        raise SystemExit(f"BigQuery endpoint {bq.base} unreachable. Start the emulator: make emulator")
    inv = inventory()
    units = [u for w in inv["waves"] for u in w] if args.all else args.units
    if not units:
        raise SystemExit("give unit ids or --all")
    reports = [run_unit(u, args.run_date, bq) for u in units]
    failed = [r["unit"] for r in reports if not r["passed"]]
    print(f"\n{len(reports) - len(failed)}/{len(reports)} units at parity" + (f"; failed: {failed}" if failed else ""))
    sys.exit(1 if failed else 0)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("baseline")
    b.add_argument("--run-date", default=DEFAULT_RUN_DATE)
    r = sub.add_parser("run")
    r.add_argument("units", nargs="*")
    r.add_argument("--all", action="store_true")
    r.add_argument("--run-date", default=DEFAULT_RUN_DATE)
    i = sub.add_parser("integration")
    i.add_argument("--run-date", default=DEFAULT_RUN_DATE)
    sub.add_parser("check-dags")
    args = ap.parse_args()
    {"baseline": cmd_baseline, "run": cmd_run, "integration": cmd_integration, "check-dags": cmd_check_dags}[args.cmd](args)


if __name__ == "__main__":
    main()
