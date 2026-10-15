"""PL_RSI_InventoryHealth -> Composer.

ADF                                        Composer / BigQuery
NB_Inventory_Cover -> nb_inventory_cover   gold_inventory_health (ros_days=28 from baseParameters -> params.ros_days)
Notify_Failure (WebActivity)               DEFAULT_ARGS.on_failure_callback (not replayed)
run_date pipeline parameter                RUN_DATE ({{ params.run_date or ds }})
TR_RSI_Daily_0200 trigger                  schedule=None; triggered by the master DAG

Reads pds_rsi.silver_sales from pds_rsi_salesdaily. The ADF master pipeline ran both in parallel
(race condition); the master DAG must order salesdaily >> inventoryhealth.
"""
from airflow import DAG

from pds_rsi_common import bq_sql, dag_kwargs

with DAG(**dag_kwargs("inventoryhealth", doc_md=__doc__, params={"run_date": "", "ros_days": 28})) as dag:
    gold_inventory_health = bq_sql("gold_inventory_health", "inventoryhealth/01_gold_inventory_health.sql")
