"""PL_RSI_Master_Daily + trigger TR_RSI_Daily_0200 -> Composer.

ADF                                   Composer
TR_RSI_Daily_0200 (02:00 UTC, run_date = scheduledTime - 1 day)   schedule "0 2 * * *", run_date = ds
Run_<Unit> (ExecutePipeline, waitOnCompletion)                    run_<unit> (TriggerDagRunOperator, wait_for_completion)
dependsOn (legacy)                                                replaced by data-lineage edges (LINEAGE below)
Notify_Failure (WebActivity on any Failed)                        DEFAULT_ARGS.on_failure_callback

Ordering follows data lineage from migration/scope/inventory.json, not the legacy dependsOn. This fixes the
legacy race where Run_InventoryHealth ran in parallel with Run_SalesDaily although it reads pds_rsi.silver_sales.
Units whose DAG has not landed yet (inventoryhealth: open parity decision) are left out until their DAG file exists,
and are then wired in after their upstream units automatically.
"""
from airflow import DAG

from pds_rsi_common import RUN_DATE, dag_kwargs, trigger_unit, unit_dag_exists

# unit -> upstream units it reads from (lineage waves: [otif, productdim, ratingspulse, salesdaily, storedimscd2]
# -> [inventoryhealth, marketshare, mediaroi, promoeffectiveness] -> [execkpipack])
LINEAGE = {
    "salesdaily": [],
    "storedimscd2": [],
    "productdim": [],
    "ratingspulse": [],
    "otif": [],
    "inventoryhealth": ["salesdaily"],
    "promoeffectiveness": ["salesdaily"],
    "mediaroi": ["salesdaily"],
    "marketshare": ["salesdaily", "storedimscd2", "productdim"],
    "execkpipack": ["salesdaily", "mediaroi", "otif"],
}

with DAG(**dag_kwargs("master_daily", doc_md=__doc__, schedule="0 2 * * *",
                      description="Daily master orchestration for the Retail Sales Intelligence product")) as dag:
    runs = {u: trigger_unit(u, RUN_DATE) for u in LINEAGE if unit_dag_exists(u)}
    for unit, ups in LINEAGE.items():
        for up in ups:
            if unit in runs and up in runs:
                runs[up] >> runs[unit]
