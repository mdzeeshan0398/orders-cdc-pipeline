"""Pure-Python transformation and data-quality rules.

Shared by the Spark streaming job and the Airflow batch DAG, and deliberately
free of Spark/Airflow imports so the rules can be unit-tested anywhere.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Iterable, Optional

VALID_STATUSES = frozenset({"CREATED", "PAID", "SHIPPED", "DELIVERED", "CANCELLED"})
REQUIRED_FIELDS = ("order_id", "customer_id", "status", "amount", "updated_at")
DEFAULT_CURRENCY = "INR"


class InvalidRecord(ValueError):
    """Raised when a record fails a data-quality rule."""


@dataclass(frozen=True)
class Order:
    order_id: str
    customer_id: str
    status: str
    amount: Decimal
    currency: str
    updated_at: datetime


def parse_ts(value) -> datetime:
    """Parse an ISO-8601 string or datetime; naive values are treated as UTC."""
    if isinstance(value, datetime):
        ts = value
    elif isinstance(value, str):
        try:
            ts = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError as exc:
            raise InvalidRecord(f"bad timestamp: {value!r}") from exc
    else:
        raise InvalidRecord(f"bad timestamp type: {type(value).__name__}")
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def normalize(raw: dict) -> Order:
    """Validate and clean one raw order record."""
    missing = [f for f in REQUIRED_FIELDS if raw.get(f) in (None, "")]
    if missing:
        raise InvalidRecord(f"missing fields: {', '.join(missing)}")

    status = str(raw["status"]).strip().upper()
    if status not in VALID_STATUSES:
        raise InvalidRecord(f"unknown status: {status}")

    try:
        amount = Decimal(str(raw["amount"])).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    except (InvalidOperation, ValueError) as exc:
        raise InvalidRecord(f"bad amount: {raw['amount']!r}") from exc
    if amount < 0:
        raise InvalidRecord("negative amount")

    currency = str(raw.get("currency") or DEFAULT_CURRENCY).strip().upper()
    if len(currency) != 3 or not currency.isalpha():
        raise InvalidRecord(f"bad currency: {currency}")

    return Order(
        order_id=str(raw["order_id"]).strip(),
        customer_id=str(raw["customer_id"]).strip(),
        status=status,
        amount=amount,
        currency=currency,
        updated_at=parse_ts(raw["updated_at"]),
    )


def split_valid(records: Iterable[dict]) -> tuple[list[Order], list[tuple[dict, str]]]:
    """Return (clean orders, [(raw record, rejection reason)])."""
    good: list[Order] = []
    bad: list[tuple[dict, str]] = []
    for raw in records:
        try:
            good.append(normalize(raw))
        except InvalidRecord as exc:
            bad.append((raw, str(exc)))
    return good, bad


def latest_per_key(orders: Iterable[Order]) -> dict[str, Order]:
    """Keep only the newest version of each order (later input wins a tie)."""
    latest: dict[str, Order] = {}
    for o in orders:
        cur = latest.get(o.order_id)
        if cur is None or o.updated_at >= cur.updated_at:
            latest[o.order_id] = o
    return latest


def next_watermark(current: Optional[datetime], orders: Iterable[Order]) -> Optional[datetime]:
    """Advance the high-water mark; it never moves backwards."""
    newest = max((o.updated_at for o in orders), default=None)
    if newest is None:
        return current
    if current is None:
        return newest
    return max(current, newest)


def reconcile(source_count: int, target_count: int,
              source_amount: Decimal, target_amount: Decimal,
              tolerance: Decimal = Decimal("0.00")) -> dict:
    """Source-to-target reconciliation on row count and amount total."""
    count_diff = target_count - source_count
    amount_diff = Decimal(target_amount) - Decimal(source_amount)
    return {
        "source_count": source_count,
        "target_count": target_count,
        "count_diff": count_diff,
        "amount_diff": amount_diff,
        "matched": count_diff == 0 and abs(amount_diff) <= tolerance,
    }
