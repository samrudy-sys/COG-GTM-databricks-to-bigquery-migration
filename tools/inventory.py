#!/usr/bin/env python3
"""Deterministic discovery of the legacy ADF + Databricks estate.

Parses ADF pipeline/trigger JSON and the exported notebooks, then emits
  migration/scope/inventory.json   machine-readable work units (drives the dynamic workflow)
  migration/scope/INVENTORY.md     human-readable scope doc with Mermaid graphs
"""
import json
import re
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from mig.paths import ADF, NOTEBOOKS, ROOT  # noqa: E402

SCOPE = ROOT / "migration" / "scope"
READ_PATTERNS = [
    re.compile(r"spark\.table\(f?[\"']((?:\{PDS_SCHEMA\}|pds_rsi|udl|bdl)\.\w+)[\"']\)"),
    re.compile(r"(?:FROM|JOIN)\s+((?:\{PDS_SCHEMA\}|pds_rsi|udl|bdl)\.\w+)", re.I),
]
WRITE_PATTERNS = [re.compile(r"write_delta\(.*,\s*[\"'](\w+)[\"']\s*[,)]"), re.compile(r"MERGE\s+INTO\s+(?:\{PDS_SCHEMA\}|pds_rsi)\.(\w+)", re.I)]
CHILD_NB = [re.compile(r"dbutils\.notebook\.run\([\"']\./(\w+)"), re.compile(r"%run\s+\./(\w+)")]
CONSTRUCTS = {
    "python_udf": re.compile(r"@F\.udf|\budf\("),
    "merge_scd2": re.compile(r"MERGE\s+INTO", re.I),
    "replace_where": re.compile(r"replace_where="),
    "window_fn": re.compile(r"Window\.|OVER\s*\(", re.I),
    "range_window": re.compile(r"rangeBetween"),
    "pivot": re.compile(r"\.pivot\("),
    "as_of_join": re.compile(r"rate_date\"?\)?\s*<=|<= F.col\(\"txn_date"),
    "week_trunc": re.compile(r"date_trunc\(\s*[\"']week", re.I),
    "date_format": re.compile(r"date_format\("),
    "hashing": re.compile(r"sha2\(|md5\(|xxhash64\(", re.I),
    "notebook_chaining": re.compile(r"dbutils\.notebook\.run"),
    "sql_magic": re.compile(r"# MAGIC %sql"),
    "dynamic_sql": re.compile(r"spark\.sql\(f"),
}


def load(kind):
    return {p.stem: json.loads(p.read_text()) for p in sorted((ADF / kind).glob("*.json"))}


def walk_activities(acts):
    for a in acts:
        yield a
        tp = a.get("typeProperties", {})
        for key in ("activities", "ifTrueActivities", "ifFalseActivities"):
            yield from walk_activities(tp.get(key, []))


def notebook_facts(name, seen=None):
    seen = seen if seen is not None else set()
    src = (NOTEBOOKS / f"{name}.py").read_text()
    reads = {m.replace("{PDS_SCHEMA}", "pds_rsi") for p in READ_PATTERNS for m in p.findall(src)}
    writes = {f"pds_rsi.{m}" for p in WRITE_PATTERNS for m in p.findall(src)}
    children = sorted({m for p in CHILD_NB for m in p.findall(src)} - {"nb_common_utils"})
    constructs = sorted(k for k, rx in CONSTRUCTS.items() if rx.search(src))
    loc = sum(1 for ln in src.splitlines() if ln.strip() and not ln.strip().startswith("#"))
    return {"notebook": name, "reads": sorted(reads - writes | (reads & writes)), "writes": sorted(writes),
            "child_notebooks": children, "constructs": constructs, "loc": loc}


def analyse():
    pipelines, triggers = load("pipeline"), load("trigger")
    linked, datasets = load("linkedService"), load("dataset")
    facts = {}
    pl = {}
    for name, doc in pipelines.items():
        props = doc["properties"]
        acts = list(walk_activities(props["activities"]))
        nbs, children, types = [], [], defaultdict(int)
        for a in acts:
            types[a["type"]] += 1
            tp = a.get("typeProperties", {})
            if a["type"] == "DatabricksNotebook":
                nbs.append(tp["notebookPath"].rsplit("/", 1)[-1])
            if a["type"] == "ExecutePipeline":
                children.append(tp["pipeline"]["referenceName"])
        for nb in list(nbs):
            if nb not in facts:
                facts[nb] = notebook_facts(nb)
            for c in facts[nb]["child_notebooks"]:
                facts.setdefault(c, notebook_facts(c))
                nbs.append(c)
        pl[name] = {"name": name, "folder": props.get("folder", {}).get("name"), "parameters": props.get("parameters", {}),
                    "activity_types": dict(types), "notebooks": list(dict.fromkeys(nbs)), "child_pipelines": list(dict.fromkeys(children)),
                    "annotations": props.get("annotations", [])}
    return pipelines, triggers, linked, datasets, pl, facts


def closure(name, pl):
    out, stack = [], [name]
    while stack:
        n = stack.pop()
        if n not in out:
            out.append(n)
            stack.extend(pl[n]["child_pipelines"])
    return out


def build():
    pipelines, triggers, linked, datasets, pl, facts = analyse()
    triggered = {p["pipelineReference"]["referenceName"]: t for t, d in triggers.items() for p in d["properties"]["pipelines"]}
    master = [n for n in pl if n in triggered and pl[n]["child_pipelines"] and not pl[n]["notebooks"]]
    invoked_by_master = {c for m in master for c in pl[m]["child_pipelines"]}
    unit_roots = sorted(invoked_by_master | {n for n in triggered if n not in master})

    units = []
    for root in unit_roots:
        members = closure(root, pl)
        nbs = list(dict.fromkeys(nb for m in members for nb in pl[m]["notebooks"]))
        writes = sorted({w for nb in nbs for w in facts[nb]["writes"]})
        reads = sorted({r for nb in nbs for r in facts[nb]["reads"]} - set(writes))
        acts = defaultdict(int)
        for m in members:
            for k, v in pl[m]["activity_types"].items():
                acts[k] += v
        constructs = sorted({c for nb in nbs for c in facts[nb]["constructs"]})
        loc = sum(facts[nb]["loc"] for nb in nbs)
        score = loc // 10 + 3 * len(constructs) + 2 * len(members) + sum(acts.values())
        units.append({
            "id": root.replace("PL_RSI_", "").lower(), "pipeline": root, "member_pipelines": members, "notebooks": nbs,
            "reads": reads, "writes": writes, "activity_types": dict(acts), "constructs": constructs, "loc": loc,
            "complexity": "high" if score >= 20 else "medium" if score >= 12 else "low", "complexity_score": score,
            "trigger": triggered.get(root) or next((triggered[m] for m in master if root in pl[m]["child_pipelines"]), None),
            "parameters": pl[root]["parameters"],
        })

    producer = {w: u["id"] for u in units for w in u["writes"]}
    for u in units:
        u["upstream_units"] = sorted({producer[r] for r in u["reads"] if r in producer})

    # Orchestration edges declared in the master pipeline (dependsOn between ExecutePipeline activities)
    orch_edges, findings = set(), []
    for m in master:
        acts = {a["name"]: a for a in pipelines[m]["properties"]["activities"]}
        ref = {n: a["typeProperties"]["pipeline"]["referenceName"] for n, a in acts.items() if a["type"] == "ExecutePipeline"}
        for n, a in acts.items():
            for d in a.get("dependsOn", []):
                if n in ref and d["activity"] in ref and "Succeeded" in d["dependencyConditions"]:
                    orch_edges.add((ref[d["activity"]], ref[n]))
    by_pipe = {u["pipeline"]: u for u in units}
    orch_reach = defaultdict(set)
    for a, b in orch_edges:
        orch_reach[b].add(a)

    def ancestors(p, seen=None):
        seen = seen or set()
        for a in orch_reach[p]:
            if a not in seen:
                seen.add(a)
                ancestors(a, seen)
        return seen

    for u in units:
        in_master = any(u["pipeline"] in pl[m]["child_pipelines"] for m in master)
        for up in u["upstream_units"]:
            upu = next(x for x in units if x["id"] == up)
            if in_master and upu["pipeline"] in by_pipe and any(upu["pipeline"] in pl[m]["child_pipelines"] for m in master) \
                    and upu["pipeline"] not in ancestors(u["pipeline"]):
                tables = sorted(set(u["reads"]) & set(upu["writes"]))
                findings.append({"severity": "high", "unit": u["id"], "kind": "race_condition",
                                 "detail": f"{u['pipeline']} reads {', '.join(tables)} produced by {upu['pipeline']} but the master "
                                           f"pipeline runs them in parallel (no dependsOn). Target DAG must add {upu['id']} >> {u['id']}."})
        if "python_udf" in u["constructs"]:
            findings.append({"severity": "medium", "unit": u["id"], "kind": "python_udf",
                             "detail": "Python UDFs must be rewritten as BigQuery SQL expressions (or SQL UDFs)."})
        if "week_trunc" in u["constructs"]:
            findings.append({"severity": "medium", "unit": u["id"], "kind": "semantic_trap",
                             "detail": "Spark date_trunc('week') is Monday-based; BigQuery DATE_TRUNC(.., WEEK) is Sunday-based -> use WEEK(MONDAY)."})
    for name, doc in linked.items():
        t = doc["properties"]["type"]
        if t in ("AzureSqlDatabase", "AzureKeyVault", "AzureBlobFS", "AzureDatabricks", "AzureDatabricksDeltaLake"):
            pass

    waves, placed = [], set()
    while len(placed) < len(units):
        wave = [u["id"] for u in units if u["id"] not in placed and set(u["upstream_units"]) <= placed]
        if not wave:
            raise RuntimeError("cycle in data lineage")
        waves.append(wave)
        placed |= set(wave)

    return {
        "product": "pds-rsi (Retail Sales Intelligence)",
        "source": {"orchestrator": "Azure Data Factory", "compute": "Azure Databricks (PySpark, Delta Lake)",
                   "pipelines": len(pipelines), "notebooks": len(facts) + 1, "triggers": len(triggers),
                   "linked_services": {n: d["properties"]["type"] for n, d in linked.items()}, "datasets": len(datasets)},
        "target": {"orchestrator": "Cloud Composer (Airflow 2)", "compute": "BigQuery", "dataset": "pds_rsi"},
        "master_pipelines": master, "orchestration_edges": sorted(orch_edges),
        "units": units, "waves": waves, "findings": findings,
        "source_tables": sorted({r for u in units for r in u["reads"] if not r.startswith("pds_rsi.")}),
    }


def mermaid(inv):
    lines = ["flowchart LR"]
    for u in inv["units"]:
        lines.append(f"  {u['id']}[\"{u['pipeline']}<br/><small>{len(u['notebooks'])} notebooks · {u['complexity']}</small>\"]")
    for u in inv["units"]:
        for up in u["upstream_units"]:
            lines.append(f"  {up} --> {u['id']}")
    for f in inv["findings"]:
        if f["kind"] == "race_condition":
            lines.append(f"  style {f['unit']} stroke:#d33,stroke-width:3px")
    return "\n".join(lines)


def markdown(inv):
    s = inv["source"]
    out = [f"# Migration scope: {inv['product']}", "",
           f"Source: **{s['orchestrator']}** + **{s['compute']}**. Target: **{inv['target']['orchestrator']}** + **{inv['target']['compute']}**.", "",
           "| Pipelines | Notebooks | Triggers | Linked services | Migration units | Waves |", "|---|---|---|---|---|---|",
           f"| {s['pipelines']} | {s['notebooks']} | {s['triggers']} | {len(s['linked_services'])} | {len(inv['units'])} | {len(inv['waves'])} |", "",
           "## Data lineage between migration units", "", "```mermaid", mermaid(inv), "```", "",
           "## Migration units", "", "| Unit | Pipeline(s) | Notebooks | Writes | Upstream | Complexity | Notable constructs |", "|---|---|---|---|---|---|---|"]
    for u in inv["units"]:
        out.append(f"| `{u['id']}` | {', '.join(u['member_pipelines'])} | {', '.join(u['notebooks'])} | {', '.join(w.split('.')[1] for w in u['writes'])} "
                   f"| {', '.join(u['upstream_units']) or '-'} | {u['complexity']} ({u['complexity_score']}) | {', '.join(u['constructs'])} |")
    out += ["", "## Findings", ""]
    out += [f"- **{f['severity']}** `{f['unit']}` ({f['kind']}): {f['detail']}" for f in inv["findings"]]
    out += ["", "## Execution waves (data lineage order)", ""]
    out += [f"{i}. {', '.join(w)}" for i, w in enumerate(inv["waves"])]
    return "\n".join(out) + "\n"


def main():
    inv = build()
    SCOPE.mkdir(parents=True, exist_ok=True)
    (SCOPE / "inventory.json").write_text(json.dumps(inv, indent=2) + "\n")
    (SCOPE / "INVENTORY.md").write_text(markdown(inv))
    if "--quiet" not in sys.argv:
        print(f"{len(inv['units'])} migration units, {len(inv['waves'])} waves, {len(inv['findings'])} findings")
        for u in inv["units"]:
            print(f"  {u['id']:<20} {u['complexity']:<7} upstream={u['upstream_units']}")
        for f in inv["findings"]:
            print(f"  [{f['severity']}] {f['unit']}: {f['kind']}")


if __name__ == "__main__":
    main()
