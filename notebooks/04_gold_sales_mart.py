# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Gold sales mart

# COMMAND ----------
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "fmcg_lakehouse")
db = f"{dbutils.widgets.get('catalog')}.{dbutils.widgets.get('schema')}"

# COMMAND ----------
spark.sql(f"""
CREATE OR REPLACE TABLE {db}.sales_mart USING DELTA AS
SELECT
  f.order_key, f.order_date, f.source_system, f.source_order_id,
  c.customer_name, c.market, c.channel,
  p.product_name, p.category, p.brand,
  f.quantity, f.unit_price, f.discount_pct, f.gross_sales, f.net_sales
FROM {db}.fact_order f
JOIN {db}.dim_customer c ON f.customer_key = c.customer_key
JOIN {db}.dim_product p ON f.product_key = p.product_key
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {db}.daily_sales USING DELTA AS
SELECT order_date, source_system, market,
       SUM(quantity) AS units_sold,
       ROUND(SUM(gross_sales), 2) AS gross_sales,
       ROUND(SUM(net_sales), 2) AS net_sales,
       COUNT(DISTINCT source_order_id) AS order_count
FROM {db}.sales_mart
GROUP BY order_date, source_system, market
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {db}.product_performance USING DELTA AS
SELECT category, brand, product_name,
       SUM(quantity) AS units_sold,
       ROUND(SUM(net_sales), 2) AS net_sales,
       ROUND(AVG(discount_pct) * 100, 2) AS avg_discount_percent
FROM {db}.sales_mart
GROUP BY category, brand, product_name
""")

display(spark.table(f"{db}.daily_sales").orderBy("order_date", ascending=False))
