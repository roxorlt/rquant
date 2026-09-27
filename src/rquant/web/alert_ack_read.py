"""Web adapter for the shared, bounded alert acknowledgment reader."""

from __future__ import annotations

from datetime import datetime, timedelta

from rquant.alert_ack_read import AlertReadModel as AlertReadModel
from rquant.alert_ack_read import _Event as _Event
from rquant.alert_ack_read import read_alert_ack as read_domain_alert_ack
from rquant.web.envelope import ServingMeta, ServingState
from rquant.web.serving import BorrowedGeneration


def read_alert_ack(
    borrowed: BorrowedGeneration | None,
    *,
    meta: ServingMeta,
    now: datetime,
    stale_after: timedelta,
) -> AlertReadModel:
    return read_domain_alert_ack(
        borrowed,
        serving_ready=meta.state is ServingState.READY,
        now=now,
        stale_after=stale_after,
    )
