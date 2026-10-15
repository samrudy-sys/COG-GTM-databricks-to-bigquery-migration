# salesdaily: migration notes (reference migration, "walk" phase)

ADF `PL_RSI_SalesDaily` (+ children `PL_RSI_Sales_Cleanse`, `PL_RSI_Sales_FxConvert`) → DAG `pds_rsi_salesdaily`.

| ADF activity | Composer / BigQuery |
|---|---|
| `LookupWatermark` (Azure SQL) | subquery on `ctl.watermarks` inside `01_silver_sales.sql` |
| `Exec_Cleanse` → `nb_sales_cleanse` | `silver_sales` (`01_silver_sales.sql`) |
| `Check_FX_Published` GetMetadata | `GCSObjectExistenceSensor` (not replayed) |
| `Exec_FxConvert` → `nb_sales_fx_convert` | `silver_sales_eur` (`02_silver_sales_eur.sql`) |
| `nb_sales_daily_agg` | `gold_sales_daily` (`03_gold_sales_daily.sql`) |
| `SP_UpdateWatermark` stored proc | `update_watermark` (`04_update_watermark.sql`) |
| `Notify_Failure` webhook | `DEFAULT_ARGS.on_failure_callback` (not replayed) |

## Semantic differences handled
* Dedup of corrected transactions: the Spark `Window` + `row_number() == 1` filter became `QUALIFY ROW_NUMBER() OVER (...) = 1`.
* As-of FX join (the latest rate on or before the transaction date, which covers weekend gaps): an inequality join + `QUALIFY`.
* Rounding: Spark `round(double)` is HALF_UP on the decimal representation. This is handled with `CAST(ROUND(CAST(x AS NUMERIC), n) AS FLOAT64)`.
  The emulator still shows about 0.6% one-unit tie differences on `avg_selling_price_eur` because its NUMERIC is a float. These are reported as `rounding_boundary`.
* Unary minus is not supported by the emulator, so `0 - qty` is used.

## Parity (BigQuery emulator, run_date 2024-04-30)
`silver_sales` 13,889 rows, `silver_sales_eur` 13,889, `gold_sales_daily` 13,785. All PASS (`reports/parity/salesdaily.json`).
