"""PL_RSI_SalesDaily (+ child pipelines PL_RSI_Sales_Cleanse, PL_RSI_Sales_FxConvert) -> Composer.

ADF                                   Composer / BigQuery
LookupWatermark (Azure SQL)           inlined into 01_silver_sales.sql (ctl.watermarks in BigQuery)
Exec_Sales_Cleanse -> nb_sales_cleanse      silver_sales
Check_FX_Published (GetMetadata)      GCSObjectExistenceSensor on the FX landing file
Exec_Sales_FxConvert -> nb_sales_fx_convert silver_sales_eur
NB_Sales_Daily_Agg                    gold_sales_daily
Update_Watermark (stored proc)        update_watermark
Notify_Failure (WebActivity)          DEFAULT_ARGS.on_failure_callback
"""
from airflow import DAG
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor

from pds_rsi_common import RUN_DATE, bq_sql, dag_kwargs

with DAG(**dag_kwargs("salesdaily", doc_md=__doc__, params={"run_date": "", "lookback_days": 120})) as dag:
    silver_sales = bq_sql("silver_sales", "salesdaily/01_silver_sales.sql")
    fx_published = GCSObjectExistenceSensor(
        task_id="check_fx_published", bucket="{{ var.value.get('pds_rsi_landing_bucket', 'pds-rsi-landing') }}",
        object="udl/fx_rates/" + RUN_DATE + "/_SUCCESS", mode="reschedule", poke_interval=300, timeout=6 * 3600,
    )
    silver_sales_eur = bq_sql("silver_sales_eur", "salesdaily/02_silver_sales_eur.sql")
    gold_sales_daily = bq_sql("gold_sales_daily", "salesdaily/03_gold_sales_daily.sql")
    update_watermark = bq_sql("update_watermark", "salesdaily/04_update_watermark.sql")

    silver_sales >> fx_published >> silver_sales_eur >> gold_sales_daily >> update_watermark
