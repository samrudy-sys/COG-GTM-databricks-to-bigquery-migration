"""Shared conventions for every pds-rsi Composer DAG (replaces ADF linked services,
retry policies and the Logic App failure webhook)."""
import json
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.models.param import Param
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

PRODUCT = "pds-rsi"
GCP_CONN_ID = "google_cloud_default"
BQ_LOCATION = os.environ.get("PDS_RSI_BQ_LOCATION", "EU")
SQL_ROOT = os.environ.get("PDS_RSI_SQL_ROOT", str(Path(__file__).resolve().parents[2] / "bigquery"))
START_DATE = datetime(2024, 1, 1)


def notify_failure(context):
    """ADF Notify_Failure WebActivity -> Airflow failure callback (webhook URL from Airflow Variable)."""
    from airflow.models import Variable
    import requests

    url = Variable.get("pds_rsi_failure_webhook", default_var=None)
    if url:
        ti = context["task_instance"]
        requests.post(url, data=json.dumps({"pipeline": ti.dag_id, "task": ti.task_id, "runId": context["run_id"],
                                            "product": PRODUCT}), headers={"Content-Type": "application/json"}, timeout=30)


DEFAULT_ARGS = {
    "owner": PRODUCT,
    "retries": 2,                       # ADF activity policy: retry 2, interval 120s
    "retry_delay": timedelta(seconds=120),
    "execution_timeout": timedelta(hours=2),
    "on_failure_callback": notify_failure,
}

RUN_DATE_PARAM = {"run_date": Param("", type="string", description="YYYY-MM-DD; defaults to the logical date (ds)")}
RUN_DATE = "{{ params.run_date or ds }}"


def dag_kwargs(unit, **overrides):
    kw = dict(
        dag_id=f"pds_rsi_{unit}", default_args=DEFAULT_ARGS, start_date=START_DATE, schedule=None, catchup=False,
        max_active_runs=1, template_searchpath=[SQL_ROOT], params=dict(RUN_DATE_PARAM),
        tags=[PRODUCT, "migrated-from-adf"],
        render_template_as_native_obj=False,
    )
    kw.update(overrides)
    return kw


def bq_sql(task_id, sql_file, **kwargs):
    """One BigQuery job per SQL file under target/bigquery/<unit>/."""
    return BigQueryInsertJobOperator(
        task_id=task_id,
        configuration={"query": {"query": "{% include '" + sql_file + "' %}", "useLegacySql": False}},
        location=BQ_LOCATION,
        gcp_conn_id=GCP_CONN_ID,
        **kwargs,
    )


DAGS_DIR = Path(__file__).resolve().parent


def unit_dag_exists(unit):
    return (DAGS_DIR / f"pds_rsi_{unit}.py").exists()


def trigger_unit(unit, run_date=RUN_DATE):
    """ADF ExecutePipeline (waitOnCompletion=true) -> TriggerDagRunOperator that blocks until the unit DAG finishes."""
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator

    return TriggerDagRunOperator(
        task_id=f"run_{unit}", trigger_dag_id=f"pds_rsi_{unit}", conf={"run_date": run_date},
        logical_date="{{ logical_date }}", reset_dag_run=True, wait_for_completion=True, poke_interval=60,
        allowed_states=["success"], failed_states=["failed"],
        retries=0,                              # the unit DAG's own tasks retry; ADF ExecutePipeline has no retry
        execution_timeout=timedelta(hours=12),  # covers salesdaily's 6h FX-file sensor
    )
