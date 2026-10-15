"""Minimal REST client for BigQuery. Defaults to the local BigQuery emulator
(ghcr.io/goccy/bigquery-emulator); set BQ_ENDPOINT/BQ_PROJECT to point elsewhere."""
import os
import re
from datetime import datetime, timezone

import requests

ENDPOINT = os.environ.get("BQ_ENDPOINT", "http://localhost:9050")
PROJECT = os.environ.get("BQ_PROJECT", "demo-proj")
BQ_TYPES = {"string": "STRING", "int": "INTEGER", "double": "FLOAT", "date": "DATE", "timestamp": "TIMESTAMP", "boolean": "BOOLEAN"}
_CREATE_OR_REPLACE = re.compile(r"CREATE\s+OR\s+REPLACE\s+TABLE\s+`?([\w.\-]+)`?", re.I)


class BigQueryError(RuntimeError):
    pass


class BQ:
    def __init__(self, endpoint=ENDPOINT, project=PROJECT):
        self.base = f"{endpoint}/bigquery/v2/projects/{project}"
        self.project = project

    def _post(self, path, body):
        r = requests.post(self.base + path, json=body, timeout=300)
        data = r.json() if r.content else {}
        if r.status_code >= 400 or "error" in data:
            msg = data.get("error", {}).get("message", r.text)
            raise BigQueryError(msg if len(msg) < 1500 else msg[:400] + " ... " + msg[-1000:])
        return data

    def ping(self):
        try:
            requests.get(self.base + "/datasets", timeout=5).raise_for_status()
            return True
        except Exception:
            return False

    def reset_dataset(self, ds):
        requests.delete(f"{self.base}/datasets/{ds}?deleteContents=true", timeout=60)
        self._post("/datasets", {"datasetReference": {"datasetId": ds, "projectId": self.project}})

    def create_table(self, ds, table, fields):
        self._post(f"/datasets/{ds}/tables", {"tableReference": {"projectId": self.project, "datasetId": ds, "tableId": table},
                                               "schema": {"fields": [{"name": n, "type": t} for n, t in fields]}})

    def insert_rows(self, ds, table, rows, batch=5000):
        for i in range(0, len(rows), batch):
            out = self._post(f"/datasets/{ds}/tables/{table}/insertAll", {"rows": [{"json": r} for r in rows[i:i + batch]]})
            if out.get("insertErrors"):
                raise BigQueryError(f"insertAll {ds}.{table}: {out['insertErrors'][:3]}")

    def query(self, sql):
        """Runs a (multi-statement) script. Works around an emulator gap: CREATE OR REPLACE
        TABLE fails if the table exists, so it is preceded by DROP TABLE IF EXISTS."""
        drops = "".join(f"DROP TABLE IF EXISTS {t};\n" for t in _CREATE_OR_REPLACE.findall(sql))
        data = self._post("/queries", {"query": drops + sql, "useLegacySql": False, "maxResults": 1_000_000, "timeoutMs": 300000})
        return _rows(data)

    def table_rows(self, ds, table):
        data = self._post("/queries", {"query": f"SELECT * FROM `{ds}.{table}`", "useLegacySql": False, "maxResults": 1_000_000})
        return data.get("schema", {}).get("fields", []), _rows(data)

    def list_tables(self, ds):
        r = requests.get(f"{self.base}/datasets/{ds}/tables", timeout=30).json()
        return sorted(t["tableReference"]["tableId"] for t in r.get("tables", []))


def _conv(v, f):
    if v is None:
        return None
    t = f["type"]
    if f.get("mode") == "REPEATED" or t in ("RECORD", "STRUCT"):
        return str(v)
    if t in ("INTEGER", "INT64"):
        return int(v)
    if t in ("FLOAT", "FLOAT64", "NUMERIC", "BIGNUMERIC"):
        return float(v)
    if t in ("BOOLEAN", "BOOL"):
        return v in (True, "true", "TRUE")
    if t == "TIMESTAMP":
        try:
            return datetime.fromtimestamp(float(v), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            return str(v)[:19].replace("T", " ")
    return v


def _rows(data):
    fields = data.get("schema", {}).get("fields", [])
    return [{f["name"]: _conv(c["v"], f) for f, c in zip(fields, row["f"])} for row in data.get("rows", [])]
