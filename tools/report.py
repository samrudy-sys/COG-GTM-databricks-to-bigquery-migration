#!/usr/bin/env python3
"""Builds reports/index.html: the self-contained migration sign-off report
(scope, lineage, per-unit parity evidence, integration result, workflow run)."""
import html
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from mig.paths import REPORTS, ROOT  # noqa: E402

INV = ROOT / "migration" / "scope" / "inventory.json"
RUN = REPORTS / "workflow_run.json"  # optional: structured results of the migrate-product workflow
E = html.escape

CSS = """
body{font:14px/1.45 -apple-system,Segoe UI,Roboto,sans-serif;margin:0;background:#f6f7f9;color:#1d2330}
header{background:#0f1b2d;color:#fff;padding:28px 40px}header h1{margin:0 0 6px;font-size:24px}header p{margin:0;opacity:.8}
main{padding:24px 40px;max-width:1280px}section{background:#fff;border:1px solid #e3e6ea;border-radius:10px;padding:20px 24px;margin:0 0 20px}
h2{margin:0 0 14px;font-size:17px}.kpis{display:grid;grid-template-columns:repeat(6,1fr);gap:12px}
.kpi{background:#fff;border:1px solid #e3e6ea;border-radius:10px;padding:14px 16px}.kpi b{display:block;font-size:26px}.kpi span{color:#5b6475;font-size:12px}
table{border-collapse:collapse;width:100%;font-size:13px}th,td{text-align:left;padding:7px 8px;border-bottom:1px solid #eef0f3;vertical-align:top}
th{color:#5b6475;font-weight:600;font-size:12px;text-transform:uppercase;letter-spacing:.03em}
.pass{color:#0a7a3e;font-weight:600}.fail{color:#c62828;font-weight:600}.pend{color:#8a6d00;font-weight:600}
.tag{display:inline-block;background:#eef2f7;border-radius:4px;padding:1px 6px;margin:1px;font-size:11px;color:#344}
.sev-high{color:#c62828;font-weight:600}.sev-medium{color:#b26a00;font-weight:600}
details{margin:4px 0}summary{cursor:pointer;color:#245}code{background:#f1f3f6;padding:1px 4px;border-radius:3px;font-size:12px}
.lane{display:flex;gap:28px;align-items:flex-start;overflow-x:auto}.wave{min-width:190px}.wave h3{font-size:12px;color:#5b6475;margin:0 0 8px;text-transform:uppercase}
.node{border:1px solid #cfd6df;border-left:4px solid #9aa5b4;border-radius:6px;padding:6px 9px;margin:0 0 8px;background:#fbfcfd;font-size:12px}
.node.ok{border-left-color:#0a7a3e}.node.bad{border-left-color:#c62828}.node.race{outline:2px dashed #c62828}.node small{color:#5b6475;display:block}
"""


def load(p):
    return json.loads(p.read_text()) if p.exists() else None


def status(passed):
    if passed is None:
        return '<span class="pend">not migrated</span>'
    return '<span class="pass">PASS</span>' if passed else '<span class="fail">FAIL</span>'


def main():
    inv = load(INV)
    parity = {p.stem: load(p) for p in sorted((REPORTS / "parity").glob("*.json")) if not p.stem.startswith("_")}
    integ = load(REPORTS / "parity" / "_integration.json")
    run = load(RUN) or {}
    run_units = {u["unit"]: u for u in run.get("units", [])}
    units = inv["units"]
    at_parity = sum(1 for u in units if parity.get(u["id"], {}).get("passed"))
    tables = sorted({w for u in units for w in u["writes"]})
    rows_checked = sum(t.get("legacy_rows", 0) for r in parity.values() for t in r.get("tables", []))
    race = {f["unit"] for f in inv["findings"] if f["kind"] == "race_condition"}

    out = [f"<!doctype html><html><head><meta charset=utf-8><title>Migration sign-off: {E(inv['product'])}</title><style>{CSS}</style></head><body>",
           f"<header><h1>Migration sign-off: {E(inv['product'])}</h1><p>{E(inv['source']['orchestrator'])} + {E(inv['source']['compute'])}"
           f" &rarr; {E(inv['target']['orchestrator'])} + {E(inv['target']['compute'])}. Parity evidence was produced by "
           "<code>tools/parity.py</code> (legacy notebooks replayed on Spark + Delta, compared with migrated SQL on BigQuery).</p></header><main>"]
    integ_kpi = f"{integ['tables_at_parity']}/{integ['tables_total']}" if integ else "&ndash;"
    kpis = [(f"{at_parity}/{len(units)}", "units at parity"), (str(len(tables)), "product tables"), (f"{rows_checked:,}", "rows compared"),
            (integ_kpi, "end-to-end tables at parity"), (str(len(inv["findings"])), "findings handled"),
            (str(len(run_units)) if run_units else "&ndash;", "parallel Devin sessions")]
    out.append('<div class="kpis">' + "".join(f'<div class="kpi"><b>{v}</b><span>{k}</span></div>' for v, k in kpis) + "</div><br>")

    out.append("<section><h2>Data lineage &amp; execution waves</h2><div class=lane>")
    for i, wave in enumerate(inv["waves"]):
        out.append(f'<div class=wave><h3>Wave {i}</h3>')
        for uid in wave:
            u = next(x for x in units if x["id"] == uid)
            p = parity.get(uid)
            cls = "ok" if p and p["passed"] else "bad" if p else ""
            ups = ", ".join(u["upstream_units"]) or "sources only"
            out.append(f'<div class="node {cls} {"race" if uid in race else ""}"><b>{E(uid)}</b><small>{E(u["pipeline"])}</small>'
                       f'<small>after: {E(ups)}</small></div>')
        out.append("</div>")
    out.append("</div><p style='font-size:12px;color:#5b6475'>Dashed outline: race condition in the legacy master pipeline, "
               "fixed by lineage ordering in the target master DAG"
               + (f" (still open until {E(', '.join(sorted(race - set(parity))))} is migrated)" if race - set(parity) else "") + ".</p></section>")

    out.append("<section><h2>Migration units</h2><table><tr><th>Unit</th><th>Legacy assets</th><th>Complexity</th><th>Constructs</th>"
               "<th>Parity</th><th>Tables</th><th>PR</th></tr>")
    for u in units:
        p = parity.get(u["id"])
        ru = run_units.get(u["id"], {})
        tbls = "".join(f"<div>{status(t['passed'])} <code>{E(t['table'])}</code> {t.get('legacy_rows', '?')} &harr; {t.get('bigquery_rows', '?')}</div>"
                       for t in (p or {}).get("tables", []))
        pr = f'<a href="{E(ru["pr_url"])}">{E(ru["pr_url"].rsplit("/", 1)[-1])}</a>' if ru.get("pr_url") else ""
        if not p and ru and not ru.get("parity_passed"):
            tbls = f"<div class=fail>not integrated: parity failed on its branch</div><small>{E(ru.get('summary', ''))}</small>"
        out.append(f"<tr><td><b>{E(u['id'])}</b></td><td>{E(', '.join(u['member_pipelines']))}<br><small>{E(', '.join(u['notebooks']))}</small></td>"
                   f"<td>{E(u['complexity'])}</td><td>{''.join(f'<span class=tag>{E(c)}</span>' for c in u['constructs'])}</td>"
                   f"<td>{status(p['passed'] if p else None)}</td><td>{tbls}</td><td>{pr}</td></tr>")
    out.append("</table></section>")

    out.append("<section><h2>Findings</h2><table><tr><th>Severity</th><th>Unit</th><th>Kind</th><th>Detail</th></tr>")
    out += [f"<tr><td class=sev-{f['severity']}>{f['severity']}</td><td>{E(f['unit'])}</td><td>{E(f['kind'])}</td><td>{E(f['detail'])}</td></tr>"
            for f in inv["findings"]]
    out.append("</table></section>")

    if run_units:
        out.append("<section><h2>Semantic differences handled by the migration agents</h2><table><tr><th>Unit</th><th>Spark &rarr; BigQuery fix</th></tr>")
        for uid, ru in sorted(run_units.items()):
            out += [f"<tr><td>{E(uid)}</td><td>{E(fx)}</td></tr>" for fx in ru.get("semantic_fixes", [])]
        out.append("</table></section>")

    out.append("<section><h2>Column-level evidence</h2>")
    for uid, p in parity.items():
        for t in p.get("tables", []):
            rows = "".join(f"<tr><td><code>{E(c)}</code></td><td>{E(v['legacy'])}</td><td>{E(v['bigquery'])}</td></tr>"
                           for c, v in t.get("checksums", {}).items())
            note = ""
            if t.get("rounding_boundary"):
                note = f"<p>Accepted half-unit rounding-boundary differences: {E(json.dumps(t['rounding_boundary']))}</p>"
            if t.get("issues"):
                note += "<p class=fail>" + "<br>".join(E(i) for i in t["issues"]) + "</p>"
            out.append(f"<details><summary>{status(t['passed'])} <code>{E(t['table'])}</code> ({E(uid)}): {t.get('legacy_rows', '?')} rows, "
                       f"{t.get('columns', '?')} columns</summary>{note}<table><tr><th>Column</th><th>Legacy (Delta)</th><th>BigQuery</th></tr>{rows}</table></details>")
    out.append("</section>")
    if integ:
        missing = integ.get("missing_units") or []
        ran = "every unit DAG" if not missing else f"{len(integ.get('units', []))} unit DAGs"
        gap = f" Not executed (no DAG yet): {E(', '.join(missing))}." if missing else ""
        out.append(f"<section><h2>End-to-end integration run</h2><p>{status(integ['passed'])}: {ran} executed in wave order on one BigQuery "
                   f"instance from raw sources only. {integ['tables_at_parity']}/{integ['tables_total']} tables at parity in {integ['seconds']}s.{gap}</p></section>")
    target = next(iter(parity.values()), {}).get("target", "n/a")
    emulator = "localhost" in target or "127.0.0.1" in target
    out.append("<section><h2>Sign-off</h2><table><tr><th>Role</th><th>Name</th><th>Date</th></tr>"
               "<tr><td>Product owner</td><td></td><td></td></tr><tr><td>Data engineering lead</td><td></td><td></td></tr></table>"
               + (f"<p class=fail>Validation ran on the local BigQuery emulator ({E(target)}), not production BigQuery. "
                  "Re-run <code>make parity</code> and <code>make integration</code> against a real GCP project before sign-off.</p>" if emulator else
                  f"<p style='font-size:12px;color:#5b6475'>Target used for this evidence: {E(target)}.</p>")
               + "<p style='font-size:12px;color:#5b6475'>Activities that are not replayed (Power BI refresh, Azure SQL &rarr; Cloud SQL copy, "
               "failure webhook, GCS sensors) are mapped in each DAG docstring.</p></section>")
    out.append("</main></body></html>")
    REPORTS.mkdir(exist_ok=True)
    (REPORTS / "index.html").write_text("\n".join(out))
    print(f"wrote {REPORTS / 'index.html'}: {at_parity}/{len(units)} units at parity")


if __name__ == "__main__":
    main()
