# Data dictionary

## Silver dimensions

### `dim_customer`

| Column | Type | Description |
|---|---|---|
| customer_key | string | SHA-256 enterprise surrogate key |
| source_system | string | `PARENT` or `ACQUIRED` |
| source_customer_id | string | Identifier supplied by source |
| customer_name | string | Clean customer name |
| market | string | Standardized market name |
| channel | string | Retail, Wholesale, E-Commerce, etc. |
| valid_from | timestamp | First observed time |
| is_current | boolean | Current record flag |

### `dim_product`

| Column | Type | Description |
|---|---|---|
| product_key | string | SHA-256 enterprise surrogate key |
| source_system | string | Source identifier |
| source_product_id | string | Source product/SKU identifier |
| product_name | string | Clean product name |
| category | string | Standardized category |
| brand | string | Product brand |

## `fact_order`

| Column | Type | Description |
|---|---|---|
| order_key | string | Deterministic source + order + line key |
| order_date | date | Transaction date |
| customer_key | string | FK to customer dimension |
| product_key | string | FK to product dimension |
| quantity | integer | Units sold |
| unit_price | decimal(18,2) | Per-unit selling price |
| discount_pct | decimal(5,4) | Discount fraction from 0 through 1 |
| gross_sales | decimal(18,2) | quantity × unit price |
| net_sales | decimal(18,2) | gross sales after discount |
