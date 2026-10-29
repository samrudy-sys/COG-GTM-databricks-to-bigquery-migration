"""PL_RSI_PriceFeatures -> Composer.

ADF                                   Composer / BigQuery
NB_Price_Features -> nb_price_features      feat_price_elasticity (01_feat_price_elasticity.sql)
run_date pipeline parameter           RUN_DATE ({{ params.run_date or ds }})
TR_RSI_Weekly_Mon_0600 (trigger)      orchestrated by the master DAG (schedule=None, as in the reference DAG)
"""
from airflow import DAG

from pds_rsi_common import bq_sql, dag_kwargs

with DAG(**dag_kwargs("pricefeatures", doc_md=__doc__)) as dag:
    feat_price_elasticity = bq_sql("feat_price_elasticity", "pricefeatures/01_feat_price_elasticity.sql")
