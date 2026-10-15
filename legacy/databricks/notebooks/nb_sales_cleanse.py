# Databricks notebook source
# MAGIC %md
# MAGIC # nb_sales_cleanse
# MAGIC Bronze -> silver for POS transactions from the Universal Data Lake.
# MAGIC * keeps the latest version of each `txn_id` (retailer feeds resend corrections)
# MAGIC * normalises SKU codes, back-fills missing currency from store country
# MAGIC * flags returns (negative qty) - returns do not carry discounts

# COMMAND ----------

# MAGIC %run ./nb_common_utils

# COMMAND ----------

dbutils.widgets.text("run_date", "")
dbutils.widgets.text("lookback_days", "120")
dbutils.widgets.text("watermark", "1900-01-01 00:00:00")
run_date = dbutils.widgets.get("run_date")
lookback_days = int(dbutils.widgets.get("lookback_days"))
watermark = dbutils.widgets.get("watermark")
start, end = window_bounds(run_date, lookback_days)

# COMMAND ----------

latest = Window.partitionBy("txn_id").orderBy(F.col("ingest_ts").desc())
currency_map = F.create_map(*[F.lit(x) for kv in COUNTRY_CURRENCY.items() for x in kv])
stores = spark.table("udl.stores").select("store_id", "country")

silver = (
    spark.table("udl.sales_transactions")
    .filter(F.col("ingest_ts") > F.to_timestamp(F.lit(watermark)))
    .withColumn("_rn", F.row_number().over(latest))
    .filter("_rn = 1")
    .drop("_rn")
    .withColumn("sku", normalize_sku("sku"))
    .withColumn("txn_date", F.to_date("txn_ts"))
    .filter(F.col("txn_date").between(F.lit(start), F.lit(end)))
    .join(stores, "store_id", "left")
    .withColumn("currency", F.coalesce(F.col("currency"), currency_map[F.col("country")]))
    .withColumn("is_return", F.col("qty") < 0)
    .withColumn("gross_amount", F.round(F.col("qty") * F.col("unit_price"), 2))
    .withColumn("discount_amt", F.when(F.col("is_return"), F.lit(0.0)).otherwise(F.col("discount_amt")))
    .withColumn("net_amount", F.round(F.col("gross_amount") - F.col("discount_amt"), 2))
    .select("txn_id", "txn_date", "txn_ts", "store_id", "country", "sku", "qty", "is_return",
            "unit_price", "gross_amount", "discount_amt", "net_amount", "currency", "channel")
)

write_delta(silver, "silver_sales")
dbutils.notebook.exit(str(silver.count()))
