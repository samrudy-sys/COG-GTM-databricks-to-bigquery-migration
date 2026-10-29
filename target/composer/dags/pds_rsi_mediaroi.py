"""PL_RSI_MediaROI -> Composer.

ADF                                   Composer / BigQuery
NB_Media_Weekly -> nb_media_weekly    silver_media_weekly (01_silver_media_weekly.sql)
NB_Media_ROI -> nb_media_roi          gold_media_roi (02_gold_media_roi.sql)
Notify_Failure (WebActivity)          DEFAULT_ARGS.on_failure_callback
pipeline parameter run_date           RUN_DATE ({{ params.run_date or ds }})
"""
from airflow import DAG

from pds_rsi_common import bq_sql, dag_kwargs

with DAG(**dag_kwargs("mediaroi", doc_md=__doc__)) as dag:
    silver_media_weekly = bq_sql("silver_media_weekly", "mediaroi/01_silver_media_weekly.sql")
    gold_media_roi = bq_sql("gold_media_roi", "mediaroi/02_gold_media_roi.sql")

    silver_media_weekly >> gold_media_roi
