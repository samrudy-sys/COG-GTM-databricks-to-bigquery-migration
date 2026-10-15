"""Local stand-in for the Azure side: runs the exported Databricks notebooks on
PySpark + Delta Lake, orchestrated by a small interpreter for the ADF pipeline JSON."""
import json
import os
import re
import shutil

from .paths import ADF, NOTEBOOKS, WORK
from .seeds import seed_rows, table_schemas

SPARK_TYPES = {"string": "string", "int": "bigint", "double": "double", "date": "date", "timestamp": "timestamp", "boolean": "boolean"}
FIXTURES = json.loads((ADF / "_harness_fixtures.json").read_text())


def spark_session(warehouse=None):
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    warehouse = warehouse or str(WORK / "spark-warehouse")
    builder = (
        SparkSession.builder.master("local[4]").appName("legacy-databricks")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", warehouse)
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={WORK / 'derby'}")
        .config("spark.jars.repositories", "https://maven-central.storage-download.googleapis.com/maven2/")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def reset_workspace():
    for d in ("spark-warehouse", "derby", "metastore_db"):
        shutil.rmtree(WORK / d, ignore_errors=True)
    WORK.mkdir(parents=True, exist_ok=True)
    os.chdir(WORK)  # derby metastore lands here


def load_seeds(spark):
    from pyspark.sql import functions as F

    for table, cols in table_schemas().items():
        db = table.split(".")[0]
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
        rows = list(seed_rows(table))
        df = spark.createDataFrame(rows, schema=", ".join(f"`{c}` string" for c in cols))
        df = df.select(*[F.col(c).cast(SPARK_TYPES[t]).alias(c) for c, t in cols.items()])
        df.write.format("delta").mode("overwrite").saveAsTable(table)
    spark.sql("CREATE DATABASE IF NOT EXISTS pds_rsi")


# ---------------------------------------------------------------- notebooks
class NotebookExit(Exception):
    def __init__(self, value):
        self.value = value


class _Widgets:
    def __init__(self, args):
        self.args = {k: str(v) for k, v in (args or {}).items()}
        self.defaults = {}

    def text(self, name, default="", label=None):
        self.defaults[name] = default

    dropdown = combobox = lambda self, name, default="", choices=None, label=None: self.text(name, default)

    def get(self, name):
        if name in self.args:
            return self.args[name]
        if name in self.defaults:
            return self.defaults[name]
        raise KeyError(f"widget {name!r} not defined")

    def removeAll(self):
        pass


class DBUtils:
    def __init__(self, runner, args):
        self.widgets = _Widgets(args)
        self._runner = runner
        self.notebook = self
        self.secrets = type("S", (), {"get": staticmethod(lambda scope, key: "local-secret")})()

    def exit(self, value):
        raise NotebookExit(value)

    def run(self, path, timeout_seconds=0, arguments=None):
        return self._runner.run(path, arguments)


def _cells(source):
    for cell in source.split("# COMMAND ----------"):
        lines = [ln for ln in cell.strip("\n").splitlines() if ln.strip() != "# Databricks notebook source"]
        if lines and all(ln.startswith("# MAGIC") or not ln.strip() for ln in lines):
            body = "\n".join(re.sub(r"^# MAGIC ?", "", ln) for ln in lines).strip()
            yield ("magic", body)
        else:
            yield ("python", "\n".join(lines))


class NotebookRunner:
    def __init__(self, spark, log=print):
        self.spark = spark
        self.log = log
        self.executed = []

    def _resolve(self, path):
        name = os.path.basename(path.rstrip("/"))
        return NOTEBOOKS / f"{name}.py"

    def run(self, path, args=None, namespace=None):
        nb = self._resolve(path)
        ns = namespace if namespace is not None else {}
        if namespace is None:
            ns.update({"spark": self.spark, "dbutils": DBUtils(self, args), "getArgument": lambda n, d=None: ns["dbutils"].widgets.args.get(n, d)})
            self.executed.append((nb.stem, dict(args or {})))
            self.log(f"    notebook {nb.stem} {json.dumps(args or {}, sort_keys=True)}")
        try:
            for kind, body in _cells(nb.read_text()):
                if kind == "python":
                    exec(compile(body, str(nb), "exec"), ns)
                elif body.startswith("%run"):
                    self.run(body.split()[1], namespace=ns)
                elif body.startswith("%sql"):
                    self.spark.sql(body[len("%sql"):])
        except NotebookExit as e:
            if namespace is not None:
                raise
            return e.value
        return None


# ---------------------------------------------------------------- ADF interpreter
_FUNC = re.compile(r"^(\w+)\((.*)\)$", re.S)


class Expr:
    """Tiny evaluator for the subset of ADF expression language used by this estate."""

    def __init__(self, params, item=None, activities=None):
        self.params, self.item, self.activities = params, item, activities or {}

    def value(self, v):
        if isinstance(v, dict) and v.get("type") == "Expression":
            v = v["value"]
        if isinstance(v, str) and v.startswith("@") and not v.startswith("@@"):
            return self.eval(v[1:])
        return v

    def _split_args(self, s):
        out, depth, cur, quote = [], 0, "", False
        for ch in s:
            if ch == "'":
                quote = not quote
            if not quote and ch in "([":
                depth += 1
            if not quote and ch in ")]":
                depth -= 1
            if ch == "," and depth == 0 and not quote:
                out.append(cur.strip()); cur = ""
            else:
                cur += ch
        if cur.strip():
            out.append(cur.strip())
        return out

    def eval(self, e):
        e = e.strip()
        if e.startswith("'") and e.endswith("'"):
            return e[1:-1]
        if e in ("true", "false"):
            return e == "true"
        if re.fullmatch(r"-?\d+", e):
            return int(e)
        m = re.match(r"^pipeline\(\)\.parameters\.(\w+)$", e)
        if m:
            return self.params.get(m.group(1))
        if e == "item()":
            return self.item
        m = re.match(r"^item\(\)\.(\w+)$", e)
        if m:
            return self.item[m.group(1)]
        m = re.match(r"^activity\('([^']+)'\)\.output\.(.+)$", e)
        if m:
            out = self.activities.get(m.group(1), {})
            for part in m.group(2).split("."):
                out = out[part]
            return out
        if e in ("pipeline().Pipeline", "pipeline().RunId"):
            return "local"
        m = _FUNC.match(e)
        if m:
            fn, args = m.group(1), [self.eval(a) for a in self._split_args(m.group(2))]
            if fn == "equals":
                return args[0] == args[1]
            if fn == "concat":
                return "".join(str(a) for a in args)
            if fn == "string":
                return str(args[0])
            if fn == "not":
                return not args[0]
        raise ValueError(f"unsupported ADF expression: @{e}")


def load_pipeline(name):
    return json.loads((ADF / "pipeline" / f"{name}.json").read_text())


def _default_params(props, overrides):
    params = {k: v.get("defaultValue") for k, v in props.get("parameters", {}).items()}
    params.update({k: v for k, v in (overrides or {}).items() if v is not None})
    return params


class ADFRunner:
    """Executes a pipeline's activities in dependency order (Succeeded paths only).
    Side-effecting activities (Lookup, Copy, Web, stored procs) are stubbed from fixtures."""

    def __init__(self, spark, log=print):
        self.nb = NotebookRunner(spark, log)
        self.log = log
        self.trace = []

    def run_pipeline(self, name, params=None, depth=0):
        props = load_pipeline(name)["properties"]
        p = _default_params(props, params)
        self.log(f"{'  ' * depth}pipeline {name} {json.dumps(p, sort_keys=True, default=str)}")
        self._run_activities(props["activities"], p, None, depth, name)

    def _run_activities(self, activities, params, item, depth, pipeline_name):
        outputs, done = {}, set()
        pending = list(activities)
        while pending:
            progressed = False
            for act in list(pending):
                deps = act.get("dependsOn", [])
                if any(set(d["dependencyConditions"]) <= {"Failed", "Skipped"} for d in deps):
                    pending.remove(act); progressed = True  # failure-handler branch: not taken
                    self.trace.append((pipeline_name, act["name"], act["type"], "skipped (failure path)"))
                    continue
                if all(d["activity"] in done for d in deps):
                    outputs[act["name"]] = self._run_activity(act, params, item, outputs, depth, pipeline_name)
                    done.add(act["name"]); pending.remove(act); progressed = True
            if not progressed:
                raise RuntimeError(f"cycle or unsatisfiable dependsOn in {pipeline_name}: {[a['name'] for a in pending]}")

    def _run_activity(self, act, params, item, outputs, depth, pipeline_name):
        ex = Expr(params, item, outputs)
        t, tp = act["type"], act.get("typeProperties", {})
        self.trace.append((pipeline_name, act["name"], t, "executed"))
        if t == "DatabricksNotebook":
            args = {k: ex.value(v) for k, v in tp.get("baseParameters", {}).items()}
            return {"runOutput": self.nb.run(tp["notebookPath"], args)}
        if t == "ExecutePipeline":
            child = {k: ex.value(v) for k, v in tp.get("parameters", {}).items()}
            self.run_pipeline(tp["pipeline"]["referenceName"], child, depth + 1)
            return {}
        if t == "ForEach":
            for it in ex.value(tp["items"]):
                self._run_activities(tp["activities"], params, it, depth + 1, pipeline_name)
            return {}
        if t == "IfCondition":
            branch = "ifTrueActivities" if ex.value(tp["expression"]) else "ifFalseActivities"
            self._run_activities(tp.get(branch, []), params, item, depth + 1, pipeline_name)
            return {}
        fixture = FIXTURES.get(f"{pipeline_name}.{act['name']}", FIXTURES.get(act["name"], {}))
        self.log(f"{'  ' * (depth + 1)}stub {t} {act['name']}")
        return fixture
