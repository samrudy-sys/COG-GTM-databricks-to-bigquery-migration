# productdim: migration notes

ADF `PL_RSI_ProductDim` → DAG `pds_rsi_productdim`.

| ADF activity | Composer / BigQuery |
|---|---|
| `NB_Product_Dim` → `nb_product_dim` (`%run nb_common_utils`) | `dim_product` (`01_dim_product.sql`) |
| activity policy (retry 2, timeout 2h) | `DEFAULT_ARGS` (`retries`, `execution_timeout`) |
| `TR_RSI_Daily_0200` (schedules `PL_RSI_Master_Daily`) | `schedule=None`; run by the master DAG |

The pipeline has no other activities (no `Notify_Failure`), so nothing is left un-replayed by the harness.

## Semantic differences handled
* `normalize_sku` (`upper(trim(sku))`) is applied to both sides before the left join on `sku`.
* Python UDF `pack_size_band` → `CASE` with the same branch order (`< 250` travel, `< 500` standard, `< 1000` family, else bulk; NULL stays NULL).
* Python UDF `sustainability_score` → SQL expression with the same operand order
  (`100 - plastic_g * 0.6 + recycled_content_pct * 30 - carbon_g_co2e / 20`), clamped with `LEAST(100.0, GREATEST(0.0, ...))`, NULL if any input is NULL.
* **Rounding:** the UDF uses Python `round(x, 1)`, not Spark `F.round`. Python rounds the *exact binary* double with ties-to-even, so the usual
  `CAST(ROUND(CAST(x AS NUMERIC), n) AS FLOAT64)` pattern is wrong here (it rounds the decimal string HALF_UP: 84.85 → 84.9, Python gives 84.8).
  Plain `ROUND(FLOAT64, 1)` is also wrong: it computes `x * 10` in floating point first, which turns 84.8499999999999943 into exactly 848.5 and rounds up.
  The SQL therefore computes `10 * x` exactly as `hi + lo` (`x * 8` and `x * 2` are exact, followed by a TwoSum), picks the integer with exact comparisons (ties to even), then divides by 10.
  This is plain IEEE double arithmetic, so it gives the same result on the emulator and on production BigQuery. Parity has 0 `rounding_boundary` rows.
* `esg_grade`: a `CASE` on the rounded score. A NULL score falls through to `'E'`, as Spark's `otherwise` does.
* `sha2(concat_ws('|', brand, category), 256)` → `TO_HEX(SHA256(CONCAT(...)))`. `COALESCE` plus a conditional separator keeps the `concat_ws` behavior of skipping NULLs.
* `floor(months_between('2024-04-30', launch_date) / 12)`: Spark `months_between` returns a whole number when the days of month match or both dates are month-ends. Otherwise it adds `(day1 - day2) / 31`.
  This is ported as `DATE_DIFF(.., MONTH)` plus that fraction. Month-end is detected with `DATE_ADD(d, INTERVAL 1 DAY)` being day 1, which is portable to the emulator.
* Types: `pack_size_ml`, `years_on_market` → INT64. Score and metrics → FLOAT64.

## Parity (BigQuery emulator, run_date 2024-04-30)
`dim_product` 120 legacy rows, 120 BigQuery rows, 16 columns. PASS, with checksums identical on every column (`reports/parity/productdim.json`).
