"""PL_RSI_OTIF (+ child pipeline PL_RSI_OTIF_Region) -> Composer.

ADF                                   Composer / BigQuery
(write_delta creates gold_otif)       create_gold_otif (CREATE TABLE IF NOT EXISTS, clustered by region)
ForEach_Region (batchCount 3)         one task per region in TaskGroup otif_region; max_active_tasks=3
  Exec_OTIF_Region -> PL_RSI_OTIF_Region (ExecutePipeline)   inlined into the TaskGroup
    NB_OTIF_KPIs -> nb_otif_kpis      otif_region.gold_otif_<region>: DELETE + INSERT of that region
                                      (replaces Delta replaceWhere "region = '<region>'")
Notify_Failure (WebActivity)          DEFAULT_ARGS.on_failure_callback
pipeline parameter run_date           RUN_DATE (params.run_date or ds)
pipeline parameter regions            REGIONS (the ADF default list; fixed at DAG parse time)
"""
from airflow import DAG
from airflow.utils.task_group import TaskGroup

from pds_rsi_common import bq_sql, dag_kwargs

REGIONS = ["UKI", "DACH", "WEU", "SA", "NA", "CEE"]

with DAG(**dag_kwargs("otif", doc_md=__doc__, max_active_tasks=3)) as dag:
    create_gold_otif = bq_sql("create_gold_otif", "otif/01_create_gold_otif.sql")
    with TaskGroup("otif_region") as otif_region:
        for region in REGIONS:
            bq_sql(f"gold_otif_{region.lower()}", "otif/02_gold_otif_region.sql", params={"region": region})

    create_gold_otif >> otif_region
