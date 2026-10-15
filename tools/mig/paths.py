from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
LEGACY = ROOT / "legacy"
ADF = LEGACY / "adf"
NOTEBOOKS = LEGACY / "databricks" / "notebooks"
SEED = LEGACY / "data" / "seed"
SCHEMAS = LEGACY / "data" / "schemas.yaml"
TARGET = ROOT / "target"
BQ_SQL = TARGET / "bigquery"
DAGS = TARGET / "composer" / "dags"
WORK = ROOT / ".parity"
BASELINE = WORK / "baseline"
REPORTS = ROOT / "reports"
DEFAULT_RUN_DATE = "2024-04-30"
PDS_DATASET = "pds_rsi"
SOURCE_DATASETS = ("udl", "bdl")
AUDIT_COLUMNS = ("_load_ts",)
