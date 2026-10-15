"""PL_RSI_ExecKPIPack -> Composer.

ADF                                   Composer / BigQuery
NB_Exec_KPI_Pack -> nb_exec_kpi_pack  gold_exec_kpi_monthly
Refresh_PowerBI_ExecPack (WebActivity) refresh_powerbi_execpack (HttpOperator on powerbi_api, not replayed)
Notify_Failure (WebActivity)          DEFAULT_ARGS.on_failure_callback
"""
from datetime import timedelta

from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator

from pds_rsi_common import bq_sql, dag_kwargs

POWERBI_GROUP_ID = "3b9a7c2e-0d41-4f7e-9a51-6c1f0e2b8d77"
POWERBI_DATASET_ID = "91d0c6aa-55e3-4c0b-a3c2-2f8e7f1d4b19"

with DAG(**dag_kwargs("execkpipack", doc_md=__doc__)) as dag:
    gold_exec_kpi_monthly = bq_sql("gold_exec_kpi_monthly", "execkpipack/01_gold_exec_kpi_monthly.sql")
    refresh_powerbi = HttpOperator(
        task_id="refresh_powerbi_execpack", http_conn_id="powerbi_api", method="POST",
        endpoint=f"v1.0/myorg/groups/{POWERBI_GROUP_ID}/datasets/{POWERBI_DATASET_ID}/refreshes",
        data='{"notifyOption": "MailOnFailure"}', headers={"Content-Type": "application/json"},
        retries=2, retry_delay=timedelta(seconds=60), execution_timeout=timedelta(minutes=30),
    )

    gold_exec_kpi_monthly >> refresh_powerbi
