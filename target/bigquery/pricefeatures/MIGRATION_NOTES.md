# pricefeatures: migration notes

ADF `PL_RSI_PriceFeatures` (trigger `TR_RSI_Weekly_Mon_0600`) → DAG `pds_rsi_pricefeatures`.

| ADF activity | Composer / BigQuery |
|---|---|
| `NB_Price_Features` (`DatabricksNotebook`) → `nb_price_features` | `feat_price_elasticity` (`01_feat_price_elasticity.sql`) |
| `run_date` pipeline parameter | `RUN_DATE` / `params.run_date` (the notebook reads the widget but never filters on it) |
| `TR_RSI_Weekly_Mon_0600` trigger | `schedule=None` like the reference DAG; scheduling belongs to the master DAG (after `salesdaily`) |
| `Notify_Failure` / activity retry policy | `DEFAULT_ARGS` (`on_failure_callback`, retries) |

Every activity is replayed by the harness (there are no external side effects).

## Semantic differences handled
* `date_trunc('week', txn_date)` is Monday-based in Spark, so it becomes `DATE_TRUNC(txn_date, WEEK(MONDAY))`.
* `~is_return` becomes `WHERE NOT is_return`. Both drop NULLs.
* `normalize_sku` (from `nb_common_utils`) becomes `UPPER(TRIM(sku))` on `udl.promotions`.
* `left_semi` promo join (date-range overlap, any retailer) becomes `SELECT DISTINCT` over an inner join, then a `LEFT JOIN`. `on_promo` = matched.
* `lag` / `avg ... rowsBetween(-3, 0)` become `LAG` and `AVG ... ROWS BETWEEN 3 PRECEDING AND CURRENT ROW`.
* `x / 0`: `SAFE_DIVIDE`. `log(x <= 0)` is NULL in Spark but an error in BigQuery, so it is guarded with `IF(x > 0, LN(x), NULL)`. `log1p(u)` becomes `LN(1 + u)`.
* **Rounding.** Spark `round(double, n)` applies HALF_UP to the double's decimal string. The playbook's `CAST(ROUND(CAST(x AS NUMERIC), n) AS FLOAT64)`
  failed here: summing 4-dp EUR amounts in floating point gives quotients like `37.9449 / 6 = 6.3241499999999995`. Spark rounds that
  *down* to 6.3241, but a NUMERIC cast (9 dp) turns it into 6.32415 and rounds *up*. That affected 44 `avg_price_eur` rows,
  and the error carried into `log_price` / `price_lag_1w` / `price_change_pct` (beyond the one-unit tolerance).
  The SQL uses a Jinja macro `spark_round(x, n)` instead: it compares `|x|` with the double of the tie `(10k+5)/10^(n+1)`, where `k = FLOOR(|x|·10^n)`.
  `>=` rounds up, so an exact tie rounds up and a value below it rounds down. Because this uses only IEEE comparisons/divisions of exact integers, it is exact on
  both BigQuery and the emulator, so parity shows 0 `rounding_boundary` rows.
* Emulator quirks: there is no unary minus, so the macro uses `0 - x`. `SIGN(x) * y / n` truncates to an integer on the emulator, so the macro uses `IF(x < 0, ...)`.

## Parity (BigQuery emulator, run_date 2024-04-30)
`make parity UNIT=pricefeatures` exits 0 (after merging the shared `_load_ts` staging fix from `main`; this unit's SQL/DAG were unchanged):

| table | legacy rows | BigQuery rows | result |
|---|---|---|---|
| `pds_rsi.feat_price_elasticity` | 8,580 | 8,580 | PASS (12 columns, 0 issues, 0 rounding-boundary rows) |

Emulator-only validation; not run against a real GCP project.
