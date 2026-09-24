from __future__ import annotations

import csv
from decimal import Decimal
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from fmcg_lakehouse.quality import calculate_net_sales, enterprise_key


class BusinessRuleTests(unittest.TestCase):
    def test_enterprise_keys_are_stable_and_source_specific(self):
        self.assertEqual(enterprise_key("parent", "C001"), enterprise_key(" PARENT ", " C001 "))
        self.assertNotEqual(enterprise_key("parent", "C001"), enterprise_key("acquired", "C001"))

    def test_net_sales(self):
        self.assertEqual(calculate_net_sales(10, Decimal("2.50"), Decimal("0.10")), Decimal("22.50"))

    def test_invalid_discount_rejected(self):
        with self.assertRaises(ValueError):
            calculate_net_sales(1, Decimal("2.50"), Decimal("1.10"))


class InputContractTests(unittest.TestCase):
    def read(self, relative: str):
        with (ROOT / relative).open(newline="", encoding="utf-8") as handle:
            return list(csv.DictReader(handle))

    def test_parent_foreign_keys(self):
        customers = {r["customer_id"] for r in self.read("data/sample/parent_company/customers/customers.csv")}
        products = {r["product_id"] for r in self.read("data/sample/parent_company/products/products.csv")}
        orders = self.read("data/sample/parent_company/orders/orders.csv")
        self.assertTrue(orders)
        self.assertTrue(all(r["customer_id"] in customers for r in orders))
        self.assertTrue(all(r["product_id"] in products for r in orders))
        keys = {(r["order_id"], r["line_id"]) for r in orders}
        self.assertEqual(len(keys), len(orders))

    def test_acquired_foreign_keys(self):
        customers = {r["retailer_code"] for r in self.read("data/sample/acquired_company/customers/customers.csv")}
        products = {r["sku"] for r in self.read("data/sample/acquired_company/products/products.csv")}
        orders = self.read("data/sample/acquired_company/orders/orders.csv")
        self.assertTrue(all(r["retailer_code"] in customers for r in orders))
        self.assertTrue(all(r["sku"] in products for r in orders))


if __name__ == "__main__":
    unittest.main()
