# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Incremental order load
# MAGIC Reads incremental folders, transforms them through the same contract, and merges by deterministic key.

# COMMAND ----------
from pyspark.sql import functions as F, types as T
from delta.tables import DeltaTable

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "fmcg_lakehouse")
dbutils.widgets.text("landing_volume", "/Volumes/workspace/fmcg_lakehouse/landing")
db = f"{dbutils.widgets.get('catalog')}.{dbutils.widgets.get('schema')}"
landing = dbutils.widgets.get("landing_volume")

# COMMAND ----------
# Incremental files use the already-standardized contract to keep this notebook focused on replay safety.
schema = T.StructType([
    T.StructField("source_system", T.StringType(), False),
    T.StructField("source_order_id", T.StringType(), False),
    T.StructField("source_line_id", T.IntegerType(), False),
    T.StructField("order_date", T.DateType(), False),
    T.StructField("source_customer_id", T.StringType(), False),
    T.StructField("source_product_id", T.StringType(), False),
    T.StructField("quantity", T.IntegerType(), False),
    T.StructField("unit_price", T.DecimalType(18, 2), False),
    T.StructField("discount_pct", T.DecimalType(5, 4), False),
])

incoming = (
    spark.read.option("header", True).schema(schema).csv(f"{landing}/incremental/orders/*.csv")
    .withColumn("order_key", F.sha2(F.concat_ws("|", "source_system", "source_order_id", F.col("source_line_id")), 256))
    .withColumn("customer_key", F.sha2(F.concat_ws("|", "source_system", "source_customer_id"), 256))
    .withColumn("product_key", F.sha2(F.concat_ws("|", "source_system", "source_product_id"), 256))
    .withColumn("gross_sales", (F.col("quantity") * F.col("unit_price")).cast("decimal(18,2)"))
    .withColumn("net_sales", (F.col("quantity") * F.col("unit_price") * (1 - F.col("discount_pct"))).cast("decimal(18,2)"))
    .withColumn("_source_file", F.input_file_name())
    .withColumn("_ingested_at", F.current_timestamp())
)

if incoming.filter((F.col("quantity") <= 0) | (F.col("unit_price") < 0) | (~F.col("discount_pct").between(0, 1))).limit(1).count():
    raise ValueError("Incremental input failed quality checks")

target = DeltaTable.forName(spark, f"{db}.fact_order")
(target.alias("t").merge(incoming.alias("s"), "t.order_key = s.order_key")
 .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())

# Rebuild Gold tables after the merge.
dbutils.notebook.run("./04_gold_sales_mart", 0, {"catalog": dbutils.widgets.get("catalog"), "schema": dbutils.widgets.get("schema")})
