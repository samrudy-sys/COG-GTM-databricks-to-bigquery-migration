"""TR_RSI_Weekly_Mon_0600 -> Composer.

ADF trigger TR_RSI_Weekly_Mon_0600 starts PL_RSI_ForecastAccuracy and PL_RSI_PriceFeatures every Monday 06:00 UTC
with run_date = scheduledTime - 1 day (the Sunday). Airflow's logical date for a weekly cron is the start of the
interval (the previous Monday), so run_date is derived from data_interval_end instead of ds.

Both units read pds_rsi.silver_sales (salesdaily). The legacy trigger only relied on the 02:00 daily run finishing
before 06:00; here wait_for_salesdaily makes that dependency explicit by waiting on the master DAG's run_salesdaily
for the same run_date (daily logical date = Sunday 02:00 = weekly logical date + 6 days - 4 hours).
"""
from datetime import timedelta

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor

from pds_rsi_common import dag_kwargs, trigger_unit

RUN_DATE = "{{ params.run_date or macros.ds_add(data_interval_end | ds, -1) }}"
WEEKLY_UNITS = ["forecastaccuracy", "pricefeatures"]

with DAG(**dag_kwargs("weekly", doc_md=__doc__, schedule="0 6 * * 1",
                      description="Weekly Monday 06:00 UTC runs of forecastaccuracy and pricefeatures")) as dag:
    wait_for_salesdaily = ExternalTaskSensor(
        task_id="wait_for_salesdaily", external_dag_id="pds_rsi_master_daily", external_task_id="run_salesdaily",
        execution_delta=timedelta(days=-6, hours=4), mode="reschedule", poke_interval=300, timeout=12 * 3600,
        allowed_states=["success"], failed_states=["failed", "skipped"],
    )
    for unit in WEEKLY_UNITS:
        wait_for_salesdaily >> trigger_unit(unit, RUN_DATE)
