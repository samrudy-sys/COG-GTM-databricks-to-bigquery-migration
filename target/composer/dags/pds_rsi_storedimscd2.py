"""PL_RSI_StoreDimSCD2 -> Composer.

ADF                                   Composer / BigQuery
parameter full_reload (bool, false)   DAG param full_reload
If_Full_Reload (IfCondition)          BranchPythonOperator check_full_reload: full_reload, or dim_store missing
                                      (folds in nb_store_dim_scd2's tableExists -> dbutils.notebook.run(init) chaining)
  ifTrue: NB_Store_Dim_Init           dim_store_init (01_dim_store_init.sql)
  ifFalse: (none)                     skip_init (EmptyOperator)
NB_Store_Dim_SCD2 -> nb_store_dim_scd2  stg_dim_store (02_stg_dim_store.sql: dedup + MERGE source, trigger_rule
                                      none_failed_min_one_success) >> dim_store_scd2 (03_dim_store_scd2.sql: MERGE)
Trigger TR_RSI_Daily_0200             run by the master DAG (schedule=None here)
Activity policy retry 2 / 300s        default_args retries=2, retry_delay=300s
"""
from datetime import timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator

from pds_rsi_common import DEFAULT_ARGS, GCP_CONN_ID, RUN_DATE_PARAM, bq_sql, dag_kwargs


def _choose_init(params, **_):
    """ADF If_Full_Reload, plus the notebook's own guard: init when the dimension does not exist yet."""
    if str(params.get("full_reload")).lower() == "true":
        return "dim_store_init"
    from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

    hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, use_legacy_sql=False)
    exists = hook.table_exists(project_id=hook.project_id, dataset_id="pds_rsi", table_id="dim_store")
    return "skip_init" if exists else "dim_store_init"


with DAG(**dag_kwargs(
    "storedimscd2", doc_md=__doc__,
    default_args={**DEFAULT_ARGS, "retry_delay": timedelta(seconds=300)},
    params={**RUN_DATE_PARAM, "full_reload": Param(False, type="boolean", description="ADF full_reload: rebuild dim_store")},
)) as dag:
    check_full_reload = BranchPythonOperator(task_id="check_full_reload", python_callable=_choose_init)
    dim_store_init = bq_sql("dim_store_init", "storedimscd2/01_dim_store_init.sql")
    skip_init = EmptyOperator(task_id="skip_init")
    stg_dim_store = bq_sql("stg_dim_store", "storedimscd2/02_stg_dim_store.sql", trigger_rule="none_failed_min_one_success")
    dim_store_scd2 = bq_sql("dim_store_scd2", "storedimscd2/03_dim_store_scd2.sql")

    check_full_reload >> [dim_store_init, skip_init] >> stg_dim_store >> dim_store_scd2
