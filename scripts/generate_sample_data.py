"""Create deterministic, synthetic CSV inputs for the portfolio project."""

from __future__ import annotations

import csv
from datetime import date, timedelta
from pathlib import Path
import random

ROOT = Path(__file__).resolve().parents[1] / "data" / "sample"
RNG = random.Random(42)


def write_csv(path: Path, columns: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def build_orders(prefix: str, customer_ids: list[str], product_ids: list[str], acquired: bool) -> list[dict]:
    rows = []
    start = date(2026, 1, 1)
    for day_offset in range(14):
        day = start + timedelta(days=day_offset)
        for order_number in range(1, 5):
            order_id = f"{prefix}-{day:%Y%m%d}-{order_number:03d}"
            for line in range(1, RNG.randint(2, 4)):
                quantity = RNG.randint(1, 20)
                price = RNG.choice([1.25, 2.50, 3.75, 5.00, 7.50, 12.00])
                discount = RNG.choice([0, 0, 5, 10, 15])
                if acquired:
                    rows.append({"invoice_no": order_id, "row_no": line,
                                 "invoice_date": day.strftime("%d/%m/%Y"),
                                 "retailer_code": RNG.choice(customer_ids), "sku": RNG.choice(product_ids),
                                 "units": quantity, "selling_price": f"{price:.2f}",
                                 "discount_percent": discount})
                else:
                    rows.append({"order_id": order_id, "line_id": line,
                                 "order_date": day.isoformat(), "customer_id": RNG.choice(customer_ids),
                                 "product_id": RNG.choice(product_ids), "quantity": quantity,
                                 "unit_price": f"{price:.2f}", "discount_pct": f"{discount / 100:.2f}"})
    return rows


def main() -> None:
    parent_customers = [
        {"customer_id": "C001", "customer_name": "Himal Retail", "market": "Nepal", "channel": "Retail"},
        {"customer_id": "C002", "customer_name": "Everest Wholesale", "market": "Nepal", "channel": "Wholesale"},
        {"customer_id": "C003", "customer_name": "Terai Mart", "market": "Nepal", "channel": "Retail"},
        {"customer_id": "C004", "customer_name": "Urban Basket", "market": "India", "channel": "E-Commerce"},
    ]
    parent_products = [
        {"product_id": "P001", "product_name": "Classic Cola 500ml", "category": "Beverages", "brand": "NorthStar"},
        {"product_id": "P002", "product_name": "Salted Crisps 100g", "category": "Snacks", "brand": "NorthStar"},
        {"product_id": "P003", "product_name": "Oat Cookies 200g", "category": "Biscuits", "brand": "DailyJoy"},
        {"product_id": "P004", "product_name": "Spring Water 1L", "category": "Beverages", "brand": "ClearPeak"},
    ]
    acquired_customers = [
        {"retailer_code": "R-11", "retailer_name": "Valley Stores", "region": "Nepal", "route_to_market": "modern_trade"},
        {"retailer_code": "R-12", "retailer_name": "Sunrise Traders", "region": "Nepal", "route_to_market": "general_trade"},
        {"retailer_code": "R-13", "retailer_name": "Metro Foods", "region": "India", "route_to_market": "modern_trade"},
    ]
    acquired_products = [
        {"sku": "SKU-A1", "sku_name": "Orange Fizz 500ml", "product_group": "Beverages", "manufacturer": "Acme Foods"},
        {"sku": "SKU-A2", "sku_name": "Chili Chips 90g", "product_group": "Snacks", "manufacturer": "Acme Foods"},
        {"sku": "SKU-A3", "sku_name": "Butter Cookies 180g", "product_group": "Biscuits", "manufacturer": "GoodGrain"},
    ]

    write_csv(ROOT / "parent_company/customers/customers.csv", list(parent_customers[0]), parent_customers)
    write_csv(ROOT / "parent_company/products/products.csv", list(parent_products[0]), parent_products)
    parent_orders = build_orders("PO", [r["customer_id"] for r in parent_customers], [r["product_id"] for r in parent_products], False)
    write_csv(ROOT / "parent_company/orders/orders.csv", list(parent_orders[0]), parent_orders)

    write_csv(ROOT / "acquired_company/customers/customers.csv", list(acquired_customers[0]), acquired_customers)
    write_csv(ROOT / "acquired_company/products/products.csv", list(acquired_products[0]), acquired_products)
    acquired_orders = build_orders("AO", [r["retailer_code"] for r in acquired_customers], [r["sku"] for r in acquired_products], True)
    write_csv(ROOT / "acquired_company/orders/orders.csv", list(acquired_orders[0]), acquired_orders)
    print(f"Generated {len(parent_orders) + len(acquired_orders)} synthetic order lines under {ROOT}")


if __name__ == "__main__":
    main()
