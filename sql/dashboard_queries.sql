-- Replace workspace.fmcg_lakehouse if you selected different widget values.

-- KPI cards
SELECT
  ROUND(SUM(net_sales), 2) AS total_net_sales,
  SUM(quantity) AS total_units,
  COUNT(DISTINCT source_order_id) AS total_orders,
  ROUND(SUM(net_sales) / COUNT(DISTINCT source_order_id), 2) AS average_order_value
FROM workspace.fmcg_lakehouse.sales_mart;

-- Daily trend
SELECT order_date, ROUND(SUM(net_sales), 2) AS net_sales
FROM workspace.fmcg_lakehouse.sales_mart
GROUP BY order_date
ORDER BY order_date;

-- Parent versus acquired company
SELECT source_system, ROUND(SUM(net_sales), 2) AS net_sales,
       SUM(quantity) AS units_sold
FROM workspace.fmcg_lakehouse.sales_mart
GROUP BY source_system
ORDER BY net_sales DESC;

-- Market performance
SELECT market, ROUND(SUM(net_sales), 2) AS net_sales,
       COUNT(DISTINCT source_order_id) AS orders
FROM workspace.fmcg_lakehouse.sales_mart
GROUP BY market
ORDER BY net_sales DESC;

-- Top products
SELECT product_name, category, brand,
       SUM(quantity) AS units_sold,
       ROUND(SUM(net_sales), 2) AS net_sales
FROM workspace.fmcg_lakehouse.sales_mart
GROUP BY product_name, category, brand
ORDER BY net_sales DESC
LIMIT 10;

-- Data-quality monitoring
SELECT quality_error, COUNT(*) AS rejected_rows
FROM workspace.fmcg_lakehouse.quarantine_orders
GROUP BY quality_error
ORDER BY rejected_rows DESC;
