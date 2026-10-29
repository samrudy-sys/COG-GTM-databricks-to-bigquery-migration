"""PL_RSI_PromoEffectiveness -> Composer.

ADF                                   Composer / BigQuery
NB_Promo_Uplift -> nb_promo_uplift    gold_promo_uplift (baseline_days=28 from baseParameters)
Notify_Failure (WebActivity)          DEFAULT_ARGS.on_failure_callback
"""
from airflow import DAG

from pds_rsi_common import bq_sql, dag_kwargs

with DAG(**dag_kwargs("promoeffectiveness", doc_md=__doc__, params={"run_date": "", "baseline_days": 28})) as dag:
    gold_promo_uplift = bq_sql("gold_promo_uplift", "promoeffectiveness/01_gold_promo_uplift.sql")
