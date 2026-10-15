"""Seed tables shared by the legacy (Spark) and target (BigQuery) runs."""
import csv

import yaml

from .paths import SCHEMAS, SEED


def table_schemas():
    with open(SCHEMAS) as f:
        return yaml.safe_load(f)


def seed_rows(table):
    """Yield typed-as-string rows from the CSV for `udl.x` (empty string -> None)."""
    with open(SEED / f"{table.replace('.', '__')}.csv", newline="") as f:
        for row in csv.DictReader(f):
            yield {k: (v if v != "" else None) for k, v in row.items()}
