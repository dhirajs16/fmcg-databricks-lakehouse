# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Conformed dimensions
# MAGIC Maps different source shapes to one customer and product contract.

# COMMAND ----------
from pyspark.sql import functions as F, Window

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "fmcg_lakehouse")
db = f"{dbutils.widgets.get('catalog')}.{dbutils.widgets.get('schema')}"

# COMMAND ----------
parent_customers = spark.table(f"{db}.parent_customers_raw").select(
    F.lit("PARENT").alias("source_system"),
    F.trim("customer_id").alias("source_customer_id"),
    F.initcap(F.trim("customer_name")).alias("customer_name"),
    F.upper(F.trim("market")).alias("market"),
    F.upper(F.trim("channel")).alias("channel"),
    "_ingested_at",
)
acquired_customers = spark.table(f"{db}.acquired_customers_raw").select(
    F.lit("ACQUIRED").alias("source_system"),
    F.trim("retailer_code").alias("source_customer_id"),
    F.initcap(F.trim("retailer_name")).alias("customer_name"),
    F.upper(F.trim("region")).alias("market"),
    F.upper(F.regexp_replace(F.trim("route_to_market"), "_", " ")).alias("channel"),
    "_ingested_at",
)

customer_window = Window.partitionBy("source_system", "source_customer_id").orderBy(F.col("_ingested_at").desc())
customers = (
    parent_customers.unionByName(acquired_customers)
    .withColumn("rn", F.row_number().over(customer_window)).filter("rn = 1").drop("rn")
    .withColumn("customer_key", F.sha2(F.concat_ws("|", "source_system", "source_customer_id"), 256))
    .withColumnRenamed("_ingested_at", "valid_from")
    .withColumn("is_current", F.lit(True))
)
customers.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{db}.dim_customer")

# COMMAND ----------
parent_products = spark.table(f"{db}.parent_products_raw").select(
    F.lit("PARENT").alias("source_system"), F.trim("product_id").alias("source_product_id"),
    F.initcap(F.trim("product_name")).alias("product_name"), F.upper(F.trim("category")).alias("category"),
    F.upper(F.trim("brand")).alias("brand"), "_ingested_at",
)
acquired_products = spark.table(f"{db}.acquired_products_raw").select(
    F.lit("ACQUIRED").alias("source_system"), F.trim("sku").alias("source_product_id"),
    F.initcap(F.trim("sku_name")).alias("product_name"), F.upper(F.trim("product_group")).alias("category"),
    F.upper(F.trim("manufacturer")).alias("brand"), "_ingested_at",
)
product_window = Window.partitionBy("source_system", "source_product_id").orderBy(F.col("_ingested_at").desc())
products = (
    parent_products.unionByName(acquired_products)
    .withColumn("rn", F.row_number().over(product_window)).filter("rn = 1").drop("rn")
    .withColumn("product_key", F.sha2(F.concat_ws("|", "source_system", "source_product_id"), 256))
)
products.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{db}.dim_product")

print(f"Customers: {customers.count()}, products: {products.count()}")
