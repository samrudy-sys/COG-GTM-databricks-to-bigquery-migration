"""PL_RSI_RatingsPulse -> Composer.

ADF                                   Composer / BigQuery
NB_Ratings_Pulse (DatabricksNotebook) -> nb_ratings_pulse
                                      gold_ratings_pulse (window_days baseParameter -> params.window_days)
run_date pipeline parameter           RUN_DATE ({{ params.run_date or ds }})
TR_RSI_Daily_0200 trigger             schedule=None; orchestrated by the master DAG
Activity policy (retry 2)             DEFAULT_ARGS retries
Failure notification                  DEFAULT_ARGS.on_failure_callback (not replayed)
"""
from airflow import DAG

from pds_rsi_common import bq_sql, dag_kwargs

with DAG(**dag_kwargs("ratingspulse", doc_md=__doc__, params={"run_date": "", "window_days": 30})) as dag:
    gold_ratings_pulse = bq_sql("gold_ratings_pulse", "ratingspulse/01_gold_ratings_pulse.sql")
