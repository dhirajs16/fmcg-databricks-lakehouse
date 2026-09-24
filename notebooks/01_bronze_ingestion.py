# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze ingestion
# MAGIC Preserves source values and adds traceability metadata. Explicit schemas prevent inference drift.

# COMMAND ----------
from datetime import datetime, timezone
import uuid
from pyspark.sql import functions as F, types as T

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "fmcg_lakehouse")
dbutils.widgets.text("landing_volume", "/Volumes/workspace/fmcg_lakehouse/landing")

catalog = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get("schema")
landing = dbutils.widgets.get("landing_volume")
db = f"{catalog}.{schema_name}"
run_id = str(uuid.uuid4())
started_at = datetime.now(timezone.utc)

# COMMAND ----------
parent_customer_schema = T.StructType([
    T.StructField("customer_id", T.StringType(), False),
    T.StructField("customer_name", T.StringType(), True),
    T.StructField("market", T.StringType(), True),
    T.StructField("channel", T.StringType(), True),
])
parent_product_schema = T.StructType([
    T.StructField("product_id", T.StringType(), False),
    T.StructField("product_name", T.StringType(), True),
    T.StructField("category", T.StringType(), True),
    T.StructField("brand", T.StringType(), True),
])
parent_order_schema = T.StructType([
    T.StructField("order_id", T.StringType(), False),
    T.StructField("line_id", T.IntegerType(), False),
    T.StructField("order_date", T.StringType(), True),
    T.StructField("customer_id", T.StringType(), True),
    T.StructField("product_id", T.StringType(), True),
    T.StructField("quantity", T.IntegerType(), True),
    T.StructField("unit_price", T.DecimalType(18, 2), True),
    T.StructField("discount_pct", T.DecimalType(5, 4), True),
])

acquired_customer_schema = T.StructType([
    T.StructField("retailer_code", T.StringType(), False),
    T.StructField("retailer_name", T.StringType(), True),
    T.StructField("region", T.StringType(), True),
    T.StructField("route_to_market", T.StringType(), True),
])
acquired_product_schema = T.StructType([
    T.StructField("sku", T.StringType(), False),
    T.StructField("sku_name", T.StringType(), True),
    T.StructField("product_group", T.StringType(), True),
    T.StructField("manufacturer", T.StringType(), True),
])
acquired_order_schema = T.StructType([
    T.StructField("invoice_no", T.StringType(), False),
    T.StructField("row_no", T.IntegerType(), False),
    T.StructField("invoice_date", T.StringType(), True),
    T.StructField("retailer_code", T.StringType(), True),
    T.StructField("sku", T.StringType(), True),
    T.StructField("units", T.IntegerType(), True),
    T.StructField("selling_price", T.DecimalType(18, 2), True),
    T.StructField("discount_percent", T.DecimalType(7, 2), True),
])

# COMMAND ----------
def read_csv(path: str, schema: T.StructType, source: str):
    return (
        spark.read.option("header", True).schema(schema).csv(path)
        .withColumn("_source_system", F.lit(source))
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_run_id", F.lit(run_id))
    )


inputs = {
    "parent_customers_raw": (f"{landing}/parent_company/customers/*.csv", parent_customer_schema, "PARENT"),
    "parent_products_raw": (f"{landing}/parent_company/products/*.csv", parent_product_schema, "PARENT"),
    "parent_orders_raw": (f"{landing}/parent_company/orders/*.csv", parent_order_schema, "PARENT"),
    "acquired_customers_raw": (f"{landing}/acquired_company/customers/*.csv", acquired_customer_schema, "ACQUIRED"),
    "acquired_products_raw": (f"{landing}/acquired_company/products/*.csv", acquired_product_schema, "ACQUIRED"),
    "acquired_orders_raw": (f"{landing}/acquired_company/orders/*.csv", acquired_order_schema, "ACQUIRED"),
}

rows_written = 0
for table, (path, input_schema, source) in inputs.items():
    frame = read_csv(path, input_schema, source)
    count = frame.count()
    frame.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{db}.{table}")
    rows_written += count
    print(f"{table}: {count} rows")

# COMMAND ----------
audit = [(run_id, "bronze_ingestion", "SUCCESS", rows_written, rows_written, started_at, datetime.now(timezone.utc), None)]
spark.createDataFrame(audit, "run_id string, pipeline_step string, status string, rows_read long, rows_written long, started_at timestamp, completed_at timestamp, message string").write.mode("append").saveAsTable(f"{db}.pipeline_audit")
