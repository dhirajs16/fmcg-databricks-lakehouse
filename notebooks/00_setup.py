# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Environment setup
# MAGIC Creates schemas, the landing volume (when permitted), and operational audit tables.

# COMMAND ----------
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "fmcg_lakehouse")
dbutils.widgets.text("landing_volume", "/Volumes/workspace/fmcg_lakehouse/landing")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
landing_volume = dbutils.widgets.get("landing_volume")
database = f"{catalog}.{schema}"

# COMMAND ----------
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {database}")
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {database}.pipeline_audit (
  run_id STRING,
  pipeline_step STRING,
  status STRING,
  rows_read BIGINT,
  rows_written BIGINT,
  started_at TIMESTAMP,
  completed_at TIMESTAMP,
  message STRING
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {database}.processed_files (
  source_system STRING,
  source_file STRING,
  file_size BIGINT,
  processed_at TIMESTAMP,
  run_id STRING
) USING DELTA
""")

print(f"Database ready: {database}")
print(f"Expected landing root: {landing_volume}")
