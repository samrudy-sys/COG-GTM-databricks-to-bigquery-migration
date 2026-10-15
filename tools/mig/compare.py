"""Row-level parity comparison between the legacy (Delta) and migrated (BigQuery) tables."""
import hashlib
import os
import math

from .paths import AUDIT_COLUMNS

FAMILY = {"INTEGER": "int", "INT64": "int", "FLOAT": "float", "FLOAT64": "float", "NUMERIC": "float", "BIGNUMERIC": "float",
          "STRING": "string", "BOOLEAN": "bool", "BOOL": "bool", "DATE": "date", "TIMESTAMP": "timestamp", "DATETIME": "timestamp"}
SPARK_FAMILY = {"bigint": "int", "int": "int", "smallint": "int", "tinyint": "int", "double": "float", "float": "float",
                "string": "string", "boolean": "bool", "date": "date", "timestamp": "timestamp"}
TOL = 1e-9
# A value rounded to N decimals may land one unit in the last place apart when the unrounded
# double sits exactly on a .5 boundary (Spark rounds Double.toString via BigDecimal HALF_UP and
# sums in partition order; BigQuery sums/rounds the binary value). Accepted, counted and reported,
# unless it is systematic (> BOUNDARY_MAX_SHARE of rows), which indicates a real rounding bug.
BOUNDARY_MAX_SHARE = 0.002
# The emulator stores NUMERIC as SQLite REAL, so ROUND(CAST(x AS NUMERIC), n) cannot reproduce Spark's decimal HALF_UP on
# exact ties (2.13405 -> 2.1341). Production BigQuery NUMERIC can. On the emulator, tie-only differences are tolerated up to 2%.
EMULATOR_BOUNDARY_MAX_SHARE = 0.02
IS_EMULATOR = any(h in os.environ.get("BQ_ENDPOINT", "http://localhost:9050") for h in ("localhost", "127.0.0.1"))


def spark_family(t):
    return "float" if t.startswith("decimal") else SPARK_FAMILY.get(t, t)


def _norm(v):
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return None
    if hasattr(v, "isoformat"):
        return v.isoformat(sep=" ") if hasattr(v, "hour") else v.isoformat()
    return v


def _sort_key(row, cols):
    return tuple((0, "") if row[c] is None else (1, f"{row[c]:.5f}" if isinstance(row[c], float) else str(row[c])) for c in cols)


def _is_num(v):
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def _decimals(v):
    s = repr(float(v))
    return len(s.split(".")[1]) if "." in s and "e" not in s else 99


def _eq(a, b):
    """True (exact within TOL), 'boundary' (one unit apart at the rounded scale) or False."""
    if a is None or b is None:
        return a is None and b is None
    if _is_num(a) and _is_num(b):
        if math.isclose(float(a), float(b), rel_tol=TOL, abs_tol=TOL):
            return True
        d = max(_decimals(a), _decimals(b))
        if d <= 6 and abs(float(a) - float(b)) <= 10 ** -d + 1e-9:
            return "boundary"
        return False
    return str(a) == str(b)


def checksum(rows, col):
    vals = [r[col] for r in rows]
    nums = [v for v in vals if isinstance(v, (int, float)) and not isinstance(v, bool)]
    if nums and len(nums) == len([v for v in vals if v is not None]):
        return f"sum={sum(nums):.4f} nulls={vals.count(None)}"
    h = hashlib.md5("|".join(sorted(str(v) for v in vals)).encode()).hexdigest()[:10]
    return f"md5={h} nulls={vals.count(None)}"


def compare_table(name, legacy_schema, legacy_rows, bq_fields, bq_rows):
    """legacy_schema: {col: spark_type}; bq_fields: BigQuery schema fields."""
    lcols = {c: spark_family(t) for c, t in legacy_schema.items() if c not in AUDIT_COLUMNS}
    tcols = {f["name"]: FAMILY.get(f["type"], f["type"].lower()) for f in bq_fields if f["name"] not in AUDIT_COLUMNS}
    issues = []
    missing = [c for c in lcols if c not in tcols]
    extra = [c for c in tcols if c not in lcols]
    if missing:
        issues.append(f"columns missing in BigQuery: {missing}")
    if extra:
        issues.append(f"unexpected extra columns in BigQuery: {extra}")
    type_diffs = {c: (lcols[c], tcols[c]) for c in lcols if c in tcols and lcols[c] != tcols[c]}
    for c, (a, b) in type_diffs.items():
        if {a, b} != {"int", "float"}:
            issues.append(f"type mismatch {c}: legacy={a} bigquery={b}")
    cols = [c for c in lcols if c in tcols]
    L = [{c: _norm(r.get(c)) for c in cols} for r in legacy_rows]
    T = [{c: _norm(r.get(c)) for c in cols} for r in bq_rows]
    if len(L) != len(T):
        issues.append(f"row count legacy={len(L)} bigquery={len(T)}")
    L.sort(key=lambda r: _sort_key(r, cols))
    T.sort(key=lambda r: _sort_key(r, cols))
    col_mismatch, boundary, samples, boundary_samples = {}, {}, [], []
    for a, b in zip(L, T):
        res = {c: _eq(a[c], b[c]) for c in cols}
        bad = [c for c, r in res.items() if r is False]
        edge = [c for c, r in res.items() if r == "boundary"]
        for c in bad:
            col_mismatch[c] = col_mismatch.get(c, 0) + 1
        for c in edge:
            boundary[c] = boundary.get(c, 0) + 1
        if bad and len(samples) < 5:
            samples.append({"legacy": {c: a[c] for c in bad}, "bigquery": {c: b[c] for c in bad}})
        if edge and len(boundary_samples) < 3:
            boundary_samples.append({"legacy": {c: a[c] for c in edge}, "bigquery": {c: b[c] for c in edge}})
    if col_mismatch:
        issues.append(f"value mismatches by column: {col_mismatch}")
    share = EMULATOR_BOUNDARY_MAX_SHARE if IS_EMULATOR else BOUNDARY_MAX_SHARE
    systematic = {c: n for c, n in boundary.items() if n > max(1, share * len(L))}
    if systematic:
        issues.append(f"systematic rounding differences (>{share:.1%} of rows): {systematic}")
    return {
        "table": name, "passed": not issues, "legacy_rows": len(L), "bigquery_rows": len(T), "columns": len(cols),
        "issues": issues, "samples": samples,
        "rounding_boundary": boundary, "rounding_boundary_samples": boundary_samples,
        "checksums": {c: {"legacy": checksum(L, c), "bigquery": checksum(T, c)} for c in cols} if L and T else {},
        "type_notes": {c: f"{a}->{b}" for c, (a, b) in type_diffs.items()},
    }
