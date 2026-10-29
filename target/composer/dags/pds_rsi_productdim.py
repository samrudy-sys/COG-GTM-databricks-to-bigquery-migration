"""PL_RSI_ProductDim -> Composer.

ADF                                   Composer / BigQuery
NB_Product_Dim -> nb_product_dim      dim_product
activity policy (retry 2, 2h timeout) DEFAULT_ARGS (retries, execution_timeout)
TR_RSI_Daily_0200 (via master)        schedule=None; triggered by the master DAG
"""
from airflow import DAG

from pds_rsi_common import bq_sql, dag_kwargs

with DAG(**dag_kwargs("productdim", doc_md=__doc__)) as dag:
    dim_product = bq_sql("dim_product", "productdim/01_dim_product.sql")
