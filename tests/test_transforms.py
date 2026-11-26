from datetime import datetime, timezone
from decimal import Decimal

import pytest

from pipeline.transforms import (
    InvalidRecord, latest_per_key, next_watermark, normalize, reconcile, split_valid,
)


def raw(**over):
    base = {"order_id": "o-1", "customer_id": "c-1", "status": "paid",
            "amount": "199.995", "currency": "inr", "updated_at": "2026-11-01T10:00:00Z"}
    base.update(over)
    return base


def test_normalize_cleans_fields():
    o = normalize(raw())
    assert o.status == "PAID"
    assert o.amount == Decimal("200.00")
    assert o.currency == "INR"
    assert o.updated_at == datetime(2026, 11, 1, 10, tzinfo=timezone.utc)


def test_naive_timestamp_is_utc():
    assert normalize(raw(updated_at="2026-11-01T10:00:00")).updated_at.tzinfo == timezone.utc


@pytest.mark.parametrize("bad, reason", [
    ({"order_id": None}, "missing"),
    ({"status": "LOST"}, "unknown status"),
    ({"amount": "abc"}, "bad amount"),
    ({"amount": "-5"}, "negative"),
    ({"currency": "RUPEE"}, "bad currency"),
    ({"updated_at": "yesterday"}, "bad timestamp"),
])
def test_rejects_bad_records(bad, reason):
    with pytest.raises(InvalidRecord, match=reason):
        normalize(raw(**bad))


def test_split_valid_routes_rejects():
    good, bad = split_valid([raw(), raw(status="LOST")])
    assert len(good) == 1 and len(bad) == 1
    assert "unknown status" in bad[0][1]


def test_latest_per_key_keeps_newest():
    older = normalize(raw(status="CREATED", updated_at="2026-11-01T09:00:00Z"))
    newer = normalize(raw(status="SHIPPED", updated_at="2026-11-01T11:00:00Z"))
    assert latest_per_key([newer, older])["o-1"].status == "SHIPPED"


def test_watermark_never_moves_back():
    wm = datetime(2026, 11, 2, tzinfo=timezone.utc)
    assert next_watermark(wm, [normalize(raw())]) == wm
    assert next_watermark(None, []) is None
    assert next_watermark(None, [normalize(raw())]) == datetime(2026, 11, 1, 10, tzinfo=timezone.utc)


def test_reconcile():
    assert reconcile(10, 10, Decimal("5.00"), Decimal("5.00"))["matched"]
    r = reconcile(10, 9, Decimal("5.00"), Decimal("4.00"))
    assert not r["matched"] and r["count_diff"] == -1
