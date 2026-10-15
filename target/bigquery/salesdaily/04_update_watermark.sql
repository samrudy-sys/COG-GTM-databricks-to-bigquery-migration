-- ADF Update_Watermark ([ctl].[usp_update_watermark] on Azure SQL) -> BigQuery control table
UPDATE ctl.watermarks
SET last_ingest_ts = (SELECT MAX(ingest_ts) FROM udl.sales_transactions),
    last_run_date = DATE('{{ params.run_date or ds }}')
WHERE pipeline_name = 'PL_RSI_SalesDaily'
