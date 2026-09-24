# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Conformed order facts and quarantine

# COMMAND ----------
from pyspark.sql import functions as F, Window

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "fmcg_lakehouse")
db = f"{dbutils.widgets.get('catalog')}.{dbutils.widgets.get('schema')}"

# COMMAND ----------
parent = spark.table(f"{db}.parent_orders_raw").select(
    F.lit("PARENT").alias("source_system"), F.trim("order_id").alias("source_order_id"),
    F.col("line_id").alias("source_line_id"), F.to_date("order_date", "yyyy-MM-dd").alias("order_date"),
    F.trim("customer_id").alias("source_customer_id"), F.trim("product_id").alias("source_product_id"),
    "quantity", F.col("unit_price").cast("decimal(18,2)"), F.col("discount_pct").cast("decimal(5,4)"),
    "_source_file", "_ingested_at",
)
acquired = spark.table(f"{db}.acquired_orders_raw").select(
    F.lit("ACQUIRED").alias("source_system"), F.trim("invoice_no").alias("source_order_id"),
    F.col("row_no").alias("source_line_id"), F.to_date("invoice_date", "dd/MM/yyyy").alias("order_date"),
    F.trim("retailer_code").alias("source_customer_id"), F.trim("sku").alias("source_product_id"),
    F.col("units").alias("quantity"), F.col("selling_price").cast("decimal(18,2)").alias("unit_price"),
    (F.col("discount_percent") / 100).cast("decimal(5,4)").alias("discount_pct"), "_source_file", "_ingested_at",
)

standardized = parent.unionByName(acquired).withColumn(
    "quality_error",
    F.when(F.col("source_order_id").isNull(), "missing order id")
     .when(F.col("order_date").isNull(), "invalid order date")
     .when(F.col("source_customer_id").isNull(), "missing customer")
     .when(F.col("source_product_id").isNull(), "missing product")
     .when(F.col("quantity") <= 0, "quantity must be positive")
     .when(F.col("unit_price") < 0, "unit price must be non-negative")
     .when(~F.col("discount_pct").between(0, 1), "discount must be between 0 and 1")
)

standardized.filter(F.col("quality_error").isNotNull()).write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{db}.quarantine_orders")
valid = standardized.filter(F.col("quality_error").isNull()).drop("quality_error")

# COMMAND ----------
customer_keys = spark.table(f"{db}.dim_customer").select("source_system", "source_customer_id", "customer_key")
product_keys = spark.table(f"{db}.dim_product").select("source_system", "source_product_id", "product_key")
dedupe_window = Window.partitionBy("source_system", "source_order_id", "source_line_id").orderBy(F.col("_ingested_at").desc())

facts = (
    valid.withColumn("rn", F.row_number().over(dedupe_window)).filter("rn = 1").drop("rn")
    .join(customer_keys, ["source_system", "source_customer_id"], "left")
    .join(product_keys, ["source_system", "source_product_id"], "left")
    .withColumn("order_key", F.sha2(F.concat_ws("|", "source_system", "source_order_id", F.col("source_line_id")), 256))
    .withColumn("gross_sales", (F.col("quantity") * F.col("unit_price")).cast("decimal(18,2)"))
    .withColumn("net_sales", (F.col("quantity") * F.col("unit_price") * (1 - F.col("discount_pct"))).cast("decimal(18,2)"))
)

missing_keys = facts.filter(F.col("customer_key").isNull() | F.col("product_key").isNull())
if missing_keys.limit(1).count():
    raise ValueError("Referential-integrity failure: fact row has an unknown customer or product")

facts.write.format("delta").mode("overwrite").partitionBy("order_date").option("overwriteSchema", True).saveAsTable(f"{db}.fact_order")
print(f"Valid facts: {facts.count()}, quarantined: {standardized.filter(F.col('quality_error').isNotNull()).count()}")
