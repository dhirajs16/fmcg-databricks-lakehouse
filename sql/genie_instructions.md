# Genie space configuration

Add `workspace.fmcg_lakehouse.sales_mart`, `daily_sales`, and `product_performance` to a Genie space.

Recommended instructions:

- “Sales” means `net_sales` unless the user explicitly asks for gross sales.
- “Orders” means distinct `source_order_id`, not line count.
- “Company” maps to `source_system`: `PARENT` or `ACQUIRED`.
- Never sum `unit_price` or `discount_pct`.
- Format money to two decimal places.
- Ask for clarification if no date range is provided for a trend comparison.

Suggested questions:

1. What were total net sales and order count by company?
2. Which five products generated the most net sales?
3. Show weekly sales trends by market.
4. Which category had the highest average discount?
5. Compare average order value between the two source companies.
