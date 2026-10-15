"""Dynamic workflow: migrate a whole data product (all ADF parent pipelines) to BigQuery + Composer.

discover (deterministic) -> migrate (1 Devin per unit, in parallel) -> integrate (1 Devin, barrier)

Re-runnable: units whose `migrate/<unit>` branch already has a passing parity report are carried into integration
as-is; units whose branch exists but failed parity get a "resume" prompt that continues the existing branch/PR
(e.g. after a shared harness fix lands on the base branch).
"""
import asyncio
import json
import os
import subprocess
from pathlib import Path

REPO = os.environ.get("MIGRATION_REPO", "samrudy-sys/COG-GTM-databricks-to-bigquery-migration")
LOCAL = Path(os.environ.get("MIGRATION_REPO_DIR", str(Path.home() / "repos" / "databricks-to-bigquery-migration")))
BASE = os.environ.get("MIGRATION_BASE_BRANCH", "main")


def load_inventory():
    """Read the committed inventory from the base branch (what every agent will clone)."""
    subprocess.run(["git", "-C", str(LOCAL), "fetch", "-q", "origin", BASE], check=True)
    raw = subprocess.run(["git", "-C", str(LOCAL), "show", f"origin/{BASE}:migration/scope/inventory.json"],
                         check=True, capture_output=True, text=True).stdout
    tree = subprocess.run(["git", "-C", str(LOCAL), "ls-tree", "-r", "--name-only", f"origin/{BASE}", "target/composer/dags"],
                          check=True, capture_output=True, text=True).stdout.split()
    return json.loads(raw), {Path(p).stem.removeprefix("pds_rsi_") for p in tree}


def existing_unit_branches(units):
    """{unit_id: parity_passed} for units that already have a pushed `migrate/<unit>` branch."""
    subprocess.run(["git", "-C", str(LOCAL), "fetch", "-q", "--prune", "origin"], check=True)
    out = {}
    for u in units:
        ref = f"origin/migrate/{u['id']}"
        if subprocess.run(["git", "-C", str(LOCAL), "rev-parse", "-q", "--verify", ref], capture_output=True).returncode:
            continue
        rep = subprocess.run(["git", "-C", str(LOCAL), "show", f"{ref}:reports/parity/{u['id']}.json"],
                             capture_output=True, text=True)
        try:
            out[u["id"]] = bool(json.loads(rep.stdout)["passed"]) if rep.returncode == 0 else False
        except (ValueError, KeyError):
            out[u["id"]] = False
    return out


def open_pr_url(unit_id):
    r = subprocess.run(["gh", "pr", "list", "-R", REPO, "--head", f"migrate/{unit_id}", "--json", "url", "-q", ".[0].url"],
                       capture_output=True, text=True)
    return r.stdout.strip() if r.returncode == 0 else ""


MIGRATE_SCHEMA = {
    "type": "object",
    "properties": {
        "unit": {"type": "string"},
        "parity_passed": {"type": "boolean"},
        "branch": {"type": "string"},
        "pr_url": {"type": "string"},
        "tables": {"type": "array", "items": {"type": "object", "properties": {
            "table": {"type": "string"}, "rows": {"type": "integer"}, "passed": {"type": "boolean"}}}},
        "dag_tasks": {"type": "integer"},
        "semantic_fixes": {"type": "array", "items": {"type": "string"},
                           "description": "each Spark->BigQuery difference you had to handle, one line each"},
        "not_replayed": {"type": "array", "items": {"type": "string"},
                         "description": "ADF activities mapped but not exercised by the parity harness"},
        "shared_change_requests": {"type": "array", "items": {"type": "string"}},
        "summary": {"type": "string"},
    },
    "required": ["unit", "parity_passed", "branch", "pr_url", "tables", "semantic_fixes", "summary"],
}

INTEGRATE_SCHEMA = {
    "type": "object",
    "properties": {
        "branch": {"type": "string"},
        "pr_url": {"type": "string"},
        "integration_passed": {"type": "boolean"},
        "tables_at_parity": {"type": "integer"},
        "tables_total": {"type": "integer"},
        "master_dag": {"type": "string"},
        "race_conditions_fixed": {"type": "array", "items": {"type": "string"}},
        "merged_branches": {"type": "array", "items": {"type": "string"}},
        "skipped_units": {"type": "array", "items": {"type": "string"}},
        "summary": {"type": "string"},
    },
    "required": ["branch", "pr_url", "integration_passed", "tables_at_parity", "tables_total", "summary"],
}


RESUME = set()


def resume_step(unit):
    if unit["id"] in RESUME:
        return (f"Branch `migrate/{unit['id']}` and its PR already exist from a previous run that did NOT reach parity. Check it out,"
                f" merge the latest `origin/{BASE}` into it (shared harness fixes may have landed there), read the PR"
                f" description and `target/bigquery/{unit['id']}/MIGRATION_NOTES.md` for what was tried, and continue from there."
                " Push to the same branch and update the existing PR instead of opening a new one.")
    return f"Create branch `migrate/{unit['id']}` from `{BASE}`."


def migrate_prompt(unit, findings):
    return f"""You are migrating one unit of the pds-rsi data product from Azure Data Factory + Databricks to BigQuery + Cloud Composer.

Repository: https://github.com/{REPO} (base branch `{BASE}`).
Unit: `{unit['id']}` (ADF parent pipeline `{unit['pipeline']}`)
Inventory entry:
{json.dumps(unit, indent=1, sort_keys=True)}
Findings for this unit:
{json.dumps(findings, indent=1, sort_keys=True)}

Instructions:
1. Clone the repo. {resume_step(unit)}
2. Follow `.devin/skills/migrate-pipeline/SKILL.md` exactly. It is the codified playbook from the reference migration
   (`target/bigquery/salesdaily/` + `target/composer/dags/pds_rsi_salesdaily.py`). Mirror those conventions.
3. Only create or modify files under `target/bigquery/{unit['id']}/`, `target/composer/dags/pds_rsi_{unit['id']}.py` and
   `reports/parity/{unit['id']}.json`. Other agents are migrating the other units in parallel, so any other change will conflict.
   If you believe a shared file needs changing, do not change it. List it in `shared_change_requests`.
4. Setup: `make setup && make emulator`. Loop on `make parity UNIT={unit['id']}` until it exits 0. Do not loosen the comparison
   or edit `tools/` or `legacy/` to make it pass. Fix the SQL instead. Run `make lint`.
5. Commit, push the branch, and open a PR into `{BASE}` titled `Migrate {unit['pipeline']} to BigQuery + Composer`.
   In the body, include the ADF->Composer activity mapping and the parity table (table, legacy rows, BigQuery rows, result).
6. Report the structured output. Set `parity_passed` to true only if the final `make parity` run passed. Never mark it passed otherwise.
"""


def integrate_prompt(results, inv):
    ok = [r for r in results if r.get("parity_passed")]
    bad = [r for r in results if not r.get("parity_passed")]
    return f"""You are the integration engineer for the pds-rsi migration (Azure Data Factory + Databricks -> BigQuery + Cloud Composer).

Repository: https://github.com/{REPO} (base branch `{BASE}`).
Parallel migration agents produced these unit branches (all passed isolated parity against the legacy baseline):
{json.dumps([{k: r.get(k) for k in ("unit", "branch", "pr_url")} for r in ok], indent=1, sort_keys=True)}
Units that did NOT reach parity (leave them out and list them in skipped_units):
{json.dumps([{k: r.get(k) for k in ("unit", "branch", "summary")} for r in bad], indent=1, sort_keys=True)}
Lineage waves from the inventory: {json.dumps(inv["waves"])}
Orchestration findings: {json.dumps([f for f in inv["findings"] if f["kind"] == "race_condition"], sort_keys=True)}

Instructions:
1. Clone the repo. Create branch `migrate/integration` from `{BASE}`, then merge every unit branch above into it.
   The branches touch disjoint files, so there should be no conflicts. If there are, resolve them and explain how in the summary.
2. Write `target/composer/dags/pds_rsi_master_daily.py` (dag_id `pds_rsi_master_daily`). It replaces ADF `PL_RSI_Master_Daily`
   and trigger `TR_RSI_Daily_0200` (daily at 02:00 UTC, run_date = ds). Use one `TriggerDagRunOperator` (wait_for_completion=True,
   conf={{"run_date": "{{{{ ds }}}}"}}) per unit DAG in the master pipeline. Order them by **data lineage** (the waves above), not by
   the legacy dependsOn. That fixes the race conditions listed. Add `pds_rsi_weekly` for `TR_RSI_Weekly_Mon_0600` the same way.
3. Run `make setup && make emulator && make integration`. It runs every unit DAG in wave order on one BigQuery dataset (no
   baseline shortcuts) and diffs all product tables against the legacy run. Fix anything that fails. If a unit's SQL needs a fix,
   make it on this branch and say so in the summary.
4. Run `make report` to build `reports/index.html` (the migration sign-off report) and `make lint`. Commit the report with `git add -f reports/index.html` (it is gitignored on unit branches).
5. Push and open a PR into `{BASE}` titled `Integrate pds-rsi migration: master DAG + sign-off report`. The body should link every
   unit PR and include the integration parity table.
6. Report the structured output. Set `integration_passed` to true only if `make integration` exited 0.
"""


async def main():
    inv, done = load_inventory()  # deterministic discovery: the fan-out list comes from the committed inventory
    branches = existing_unit_branches([u for u in inv["units"] if u["id"] not in done])
    carried = [{"unit": uid, "parity_passed": True, "branch": f"migrate/{uid}", "pr_url": open_pr_url(uid),
                "summary": "carried over from a previous run (passing parity report on its branch)"}
               for uid, ok in sorted(branches.items()) if ok]
    RESUME.update(uid for uid, ok in branches.items() if not ok)
    todo = [u for u in inv["units"] if u["id"] not in done and not branches.get(u["id"])]
    await register_workflow({
        "name": "migrate-product",
        "description": f"Migrate {inv['product']}: {len(todo)} ADF parent pipelines in parallel, then integrate + sign-off",
        "product": REPO,
        "soft_time_limit_minutes": 35,
        "phases": [
            {"title": "migrate", "detail": "one Devin per ADF parent pipeline: SQL + DAG + parity + PR", "labels": [u["id"] for u in todo]},
            {"title": "integrate", "detail": "merge unit branches, master DAG, end-to-end parity, sign-off report", "count": 1,
             "soft_time_limit_minutes": 45},
        ],
    })
    log(f"inventory: {len(inv['units'])} units, {len(inv['waves'])} waves; already migrated: {sorted(done & {u['id'] for u in inv['units']})}")
    log(f"carried over (passing branch): {[c['unit'] for c in carried]}; resuming failed branches: {sorted(RESUME)}")

    async def migrate(unit):
        findings = [f for f in inv["findings"] if f["unit"] == unit["id"]]
        try:
            r = await agent(migrate_prompt(unit, findings), phase="migrate", schema=MIGRATE_SCHEMA, label=unit["id"])
        except WorkflowAgentError as e:  # noqa: F821 (provided by the workflow runtime)
            log(f"{unit['id']}: agent failed ({e}); excluded from integration")
            return {"unit": unit["id"], "parity_passed": False, "branch": "", "pr_url": "", "summary": f"agent failed: {e}"}
        log(f"{unit['id']}: parity={'PASS' if r['parity_passed'] else 'FAIL'} {r.get('pr_url', '')}")
        return r

    results = carried + list(await parallel([lambda u=u: migrate(u) for u in todo]))
    passed = [r for r in results if r.get("parity_passed")]
    log(f"migrate: {len(passed)}/{len(results)} units at parity")
    if not passed:
        raise RuntimeError("no unit reached parity; not integrating")

    final = await agent(integrate_prompt(results, inv), phase="integrate", schema=INTEGRATE_SCHEMA, label="integrate")
    log(f"integrate: passed={final['integration_passed']} tables={final['tables_at_parity']}/{final['tables_total']} {final['pr_url']}")
    log("RESULTS " + json.dumps({"units": results, "integration": final}, sort_keys=True))


asyncio.run(main())
