from __future__ import annotations

from decimal import Decimal
import hashlib


def enterprise_key(source_system: str, source_key: str) -> str:
    """Stable surrogate key shared by notebooks and local tests."""
    value = f"{source_system.strip().upper()}|{source_key.strip()}"
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def calculate_net_sales(quantity: int, unit_price: Decimal, discount_pct: Decimal) -> Decimal:
    if quantity < 0 or unit_price < 0:
        raise ValueError("quantity and unit_price must be non-negative")
    if not Decimal("0") <= discount_pct <= Decimal("1"):
        raise ValueError("discount_pct must be between 0 and 1")
    return (Decimal(quantity) * unit_price * (Decimal("1") - discount_pct)).quantize(
        Decimal("0.01")
    )
