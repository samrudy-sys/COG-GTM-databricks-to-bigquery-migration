"""PL_RSI_MarketShare -> Composer.

ADF                                   Composer / BigQuery
NB_Market_Share -> nb_market_share    gold_market_share
(trigger TR_RSI_Daily_0200)           schedule=None; run from the master DAG after productdim, salesdaily, storedimscd2
Notify_Failure (WebActivity)          DEFAULT_ARGS.on_failure_callback
"""
from airflow import DAG

from pds_rsi_common import bq_sql, dag_kwargs

with DAG(**dag_kwargs("marketshare", doc_md=__doc__)) as dag:
    gold_market_share = bq_sql("gold_market_share", "marketshare/01_gold_market_share.sql")
