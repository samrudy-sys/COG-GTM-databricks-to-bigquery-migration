"""PL_RSI_ForecastAccuracy -> Composer.

ADF                                   Composer / BigQuery
NB_Forecast_Accuracy -> nb_forecast_accuracy   gold_forecast_accuracy
Copy_To_DemandPlanningDB (Copy, upsert to Azure SQL on sku, country)
                                      BigQueryToPostgresOperator to Cloud SQL (not replayed by the harness)
Notify_Failure (WebActivity)          DEFAULT_ARGS.on_failure_callback
run_date pipeline parameter           RUN_DATE ({{ params.run_date or ds }})
"""
from airflow import DAG
from airflow.providers.google.cloud.transfers.bigquery_to_postgres import BigQueryToPostgresOperator

from pds_rsi_common import bq_sql, dag_kwargs

with DAG(**dag_kwargs("forecastaccuracy", doc_md=__doc__)) as dag:
    gold_forecast_accuracy = bq_sql("gold_forecast_accuracy", "forecastaccuracy/01_gold_forecast_accuracy.sql")
    copy_to_demand_planning_db = BigQueryToPostgresOperator(
        task_id="copy_to_demand_planning_db",
        dataset_table="pds_rsi.gold_forecast_accuracy",
        selected_fields=["sku", "country", "weeks", "mape_pct", "bias_pct", "accuracy_pct"],
        target_table_name="{{ var.value.get('pds_rsi_demand_planning_table', 'forecast_accuracy') }}",
        postgres_conn_id="demand_planning_db",
        replace=True,
        replace_index=["sku", "country"],
        gcp_conn_id="google_cloud_default",
        location="{{ var.value.get('pds_rsi_bq_location', 'EU') }}",
    )

    gold_forecast_accuracy >> copy_to_demand_planning_db
