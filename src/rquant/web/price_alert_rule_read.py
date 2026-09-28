"""Web adapter for the shared verified Serving price-rule read."""

from __future__ import annotations

from datetime import datetime

from rquant.serving_price_alert_rule_read import PriceRuleRead
from rquant.serving_price_alert_rule_read import (
    read_price_alert_rules as read_serving_price_alert_rules,
)
from rquant.web.serving import BorrowedGeneration


def read_price_alert_rules(borrowed: BorrowedGeneration, *, now: datetime) -> PriceRuleRead:
    return read_serving_price_alert_rules(borrowed.manifest, borrowed.cursor, now=now)
