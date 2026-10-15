# Databricks notebook source
# MAGIC %md
# MAGIC # nb_product_dim
# MAGIC Conformed product dimension with the sustainability scorecard used in the
# MAGIC "Clean Future" ESG report.

# COMMAND ----------

# MAGIC %run ./nb_common_utils

# COMMAND ----------

from pyspark.sql.types import DoubleType, StringType


@F.udf(returnType=StringType())
def pack_size_band(ml):
    if ml is None:
        return None
    if ml < 250:
        return "travel"
    if ml < 500:
        return "standard"
    if ml < 1000:
        return "family"
    return "bulk"


@F.udf(returnType=DoubleType())
def sustainability_score(plastic_g, recycled_pct, carbon_g):
    if plastic_g is None or recycled_pct is None or carbon_g is None:
        return None
    raw = 100 - plastic_g * 0.6 + recycled_pct * 30 - carbon_g / 20
    return float(round(min(100.0, max(0.0, raw)), 1))

# COMMAND ----------

prod = (
    spark.table("udl.products")
    .withColumn("sku", normalize_sku("sku"))
    .join(spark.table("bdl.sustainability_metrics").withColumn("sku", normalize_sku("sku")), "sku", "left")
    .withColumn("pack_size_band", pack_size_band("pack_size_ml"))
    .withColumn("sustainability_score", sustainability_score("plastic_g", "recycled_content_pct", "carbon_g_co2e"))
    .withColumn("esg_grade",
                F.when(F.col("sustainability_score") >= 80, "A")
                .when(F.col("sustainability_score") >= 60, "B")
                .when(F.col("sustainability_score") >= 40, "C")
                .when(F.col("sustainability_score") >= 20, "D")
                .otherwise("E"))
    .withColumn("brand_category_key", F.sha2(F.concat_ws("|", "brand", "category"), 256))
    .withColumn("years_on_market", F.floor(F.months_between(F.lit("2024-04-30"), "launch_date") / 12).cast("int"))
    .select("sku", "product_name", "brand", "category", "subcategory", "brand_category_key", "pack_size_ml",
            "pack_size_band", "launch_date", "years_on_market", "is_active", "plastic_g", "recycled_content_pct",
            "carbon_g_co2e", "sustainability_score", "esg_grade")
)

write_delta(prod, "dim_product")
