from __future__ import annotations

import base64
import hashlib
import json
import os
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from threading import Event

import pandas as pd
import pytest

import rquant.daily_close_gateway as daily_close_gateway_module
import rquant.runtime_builder_daily as runtime_builder_daily
from rquant.daily_close_gateway import (
    DAILY_CLOSE_SOURCE_INTERFACES,
    DailyCloseDataset,
    DailyCloseFetchResult,
    DailyCloseGateway,
    DailyCloseGatewayConfig,
    DailyCloseRawPayload,
    DailyCloseSourceRequest,
    DailyCloseValidationError,
)
from rquant.live_contracts import BatchQualityStatus, LiveChannel
from rquant.live_spool import LiveBatchSpool
from rquant.source_quota_store import SourceQuotaAttemptOutcome, SourceQuotaStore
from rquant.source_quota_transport import QuotaBoundTransportObserver

TRADE_DATE = date(2026, 7, 31)
OBSERVED_AT = datetime(2026, 7, 31, 9, 5, tzinfo=UTC)
AVAILABLE_AT = OBSERVED_AT + timedelta(seconds=2)


def _snapshot(
    *,
    trade_date: date = TRADE_DATE,
    close: float = 10.2,
    partial: tuple[str, ...] = (),
) -> dict[str, object]:
    return {
        "daily_bar": (
            {
                "ts_code": "600000.SH",
                "trade_date": trade_date,
                "open": 10.0,
                "high": 10.4,
                "low": 9.9,
                "close": close,
                "pre_close": 9.95,
                "change": close - 9.95,
                "pct_chg": (close - 9.95) / 9.95 * 100,
                "vol": 1_000_000.0,
                "amount": 10_200_000.0,
            },
        ),
        "daily_basic": (
            {
                "ts_code": "600000.SH",
                "trade_date": trade_date,
                "turnover_rate": 0.5,
                "volume_ratio": 1.2,
                "total_mv": 200_000_000.0,
                "circ_mv": 180_000_000.0,
            },
        ),
        "adj_factor": (
            {
                "ts_code": "600000.SH",
                "trade_date": trade_date,
                "adj_factor": 1.01,
            },
        ),
        "index_daily": (
            {
                "ts_code": "000001.SH",
                "trade_date": trade_date,
                "open": 3200.0,
                "high": 3230.0,
                "low": 3190.0,
                "close": 3220.0,
                "pre_close": 3198.0,
                "change": 22.0,
                "pct_chg": 0.688,
                "vol": 2_000_000.0,
                "amount": 30_000_000.0,
            },
        ),
        "security_status": (
            {
                "ts_code": "600000.SH",
                "trade_date": trade_date,
                "name": "浦发银行",
                "is_st": False,
                "listing_status": "L",
            },
        ),
        "suspension_status": (),
        "partial_datasets": partial,
    }


def _gateway(
    tmp_path: Path,
    fetcher: Callable[[DailyCloseSourceRequest], object],
    *,
    completion_clock: Callable[[], datetime] | None = None,
    quota_store: SourceQuotaStore | None = None,
    transport_observer: QuotaBoundTransportObserver | None = None,
    **config_overrides: object,
) -> DailyCloseGateway:
    return DailyCloseGateway(
        spool=LiveBatchSpool(tmp_path / "live"),
        fetcher=fetcher,
        config=DailyCloseGatewayConfig(
            producer_version="daily-close-v1",
            producer_commit="a" * 40,
            **config_overrides,
        ),
        completion_clock=completion_clock,
        quota_store=quota_store,
        transport_observer=transport_observer,
    )


def test_transport_restart_aggregates_a_killed_second_call_as_unknown(
    tmp_path: Path,
) -> None:
    quota_path = tmp_path / "quota.sqlite3"
    old_store = SourceQuotaStore(quota_path, boot_id="boot-old", monotonic_ns=lambda: 0)
    old_observer = QuotaBoundTransportObserver(
        store=old_store,
        source="tushare.daily_close",
        quota_units_per_window=20,
        window_kind="day",
        clock=lambda: OBSERVED_AT,
    )
    initial = _gateway(
        tmp_path,
        lambda _request: pytest.fail("seed does not call the gateway fetcher"),
        quota_store=old_store,
        transport_observer=old_observer,
        quota_units_per_window=20,
        quota_accounting_mode="transport",
        quota_cost_per_request=None,
        require_source_usage_receipt=True,
    )
    request = DailyCloseSourceRequest(source="tushare.daily_close", trade_date=TRADE_DATE)
    logical_request_id = initial._quota_attempt_id(request=request, retry_ordinal=0)
    with (
        pytest.raises(KeyboardInterrupt),
        old_observer.scope(logical_request_id=logical_request_id, observed_at=OBSERVED_AT),
    ):
        old_observer.observe("daily", lambda: None)
        old_observer.observe(
            "daily_basic",
            lambda: (_ for _ in ()).throw(KeyboardInterrupt()),
        )

    external_calls = 0

    def forbidden(_request: DailyCloseSourceRequest) -> object:
        nonlocal external_calls
        external_calls += 1
        return _snapshot()

    restarted_store = SourceQuotaStore(
        quota_path,
        boot_id="boot-new",
        monotonic_ns=lambda: 1,
    )
    restarted_observer = QuotaBoundTransportObserver(
        store=restarted_store,
        source="tushare.daily_close",
        quota_units_per_window=20,
        window_kind="day",
        clock=lambda: OBSERVED_AT + timedelta(seconds=10),
    )
    restarted = _gateway(
        tmp_path,
        forbidden,
        completion_clock=lambda: AVAILABLE_AT,
        quota_store=restarted_store,
        transport_observer=restarted_observer,
        quota_units_per_window=20,
        quota_accounting_mode="transport",
        quota_cost_per_request=None,
        require_source_usage_receipt=True,
    )

    capture = restarted.capture_once(trade_date=TRADE_DATE, observed_at=OBSERVED_AT)

    assert external_calls == 0
    assert capture.degraded_reasons == ("source_attempt_unknown",)


def test_gateway_records_quota_before_provider_dispatch_and_does_not_charge_noop_retry(
    tmp_path: Path,
) -> None:
    calls = 0

    def fetch(_request: DailyCloseSourceRequest) -> object:
        nonlocal calls
        calls += 1
        return _snapshot()

    quota_store = SourceQuotaStore(tmp_path / "quota.sqlite3")
    gateway = _gateway(
        tmp_path,
        fetch,
        completion_clock=lambda: OBSERVED_AT + timedelta(minutes=2),
        quota_store=quota_store,
        quota_units_per_window=2,
    )

    first = gateway.capture_once(trade_date=TRADE_DATE, observed_at=OBSERVED_AT)
    replay = gateway.capture_once(
        trade_date=TRADE_DATE,
        observed_at=OBSERVED_AT + timedelta(seconds=5),
    )
    attempts = quota_store.list_attempts(source="tushare.daily_close")

    assert first.published is True
    assert replay.published is False
    assert calls == 1
    assert len(attempts) == 1
    assert attempts[0].outcome is SourceQuotaAttemptOutcome.SUCCESS
    assert quota_store.remaining("tushare.daily_close", now=OBSERVED_AT) == 1


def test_gateway_fails_stale_on_quota_exhaustion_without_provider_dispatch(tmp_path: Path) -> None:
    calls = 0

    def fetch(_request: DailyCloseSourceRequest) -> object:
        nonlocal calls
        calls += 1
        return _snapshot()

    quota_store = SourceQuotaStore(tmp_path / "quota.sqlite3")
    gateway = _gateway(
        tmp_path,
        fetch,
        completion_clock=lambda: OBSERVED_AT + timedelta(minutes=2),
        quota_store=quota_store,
        quota_units_per_window=1,
    )
    gateway.capture_once(trade_date=TRADE_DATE, observed_at=OBSERVED_AT)

    exhausted = gateway.capture_once(
        trade_date=TRADE_DATE,
        observed_at=OBSERVED_AT + timedelta(minutes=1),
        refresh=True,
        retry_ordinal=1,
    )

    assert exhausted.quality_status is BatchQualityStatus.STALE
    assert exhausted.degraded_reasons == ("quota_exhausted",)
    assert calls == 1


def test_gateway_restart_recovers_crashed_inflight_as_unknown_without_second_dispatch(
    tmp_path: Path,
) -> None:
    calls = 0

    def crash(_request: DailyCloseSourceRequest) -> object:
        nonlocal calls
        calls += 1
        raise KeyboardInterrupt()

    quota_store = SourceQuotaStore(tmp_path / "quota.sqlite3", boot_id="boot-old")
    gateway = _gateway(
        tmp_path,
        crash,
        completion_clock=lambda: AVAILABLE_AT,
        quota_store=quota_store,
        quota_units_per_window=2,
    )
    with pytest.raises(KeyboardInterrupt):
        gateway.capture_once(trade_date=TRADE_DATE, observed_at=OBSERVED_AT)

    restarted = _gateway(
        tmp_path,
        lambda _request: pytest.fail("unknown inflight request must not be dispatched again"),
        completion_clock=lambda: AVAILABLE_AT,
        quota_store=SourceQuotaStore(tmp_path / "quota.sqlite3", boot_id="boot-new"),
        quota_units_per_window=2,
    )
    recovered = restarted.capture_once(trade_date=TRADE_DATE, observed_at=OBSERVED_AT)

    assert calls == 1
    assert recovered.quality_status is BatchQualityStatus.STALE
    assert recovered.degraded_reasons == ("source_attempt_unknown",)
    (attempt,) = quota_store.list_attempts(source="tushare.daily_close")
    assert attempt.outcome is SourceQuotaAttemptOutcome.UNKNOWN


def test_gateway_does_not_recover_an_active_same_boot_attempt(tmp_path: Path) -> None:
    quota_path = tmp_path / "quota.sqlite3"
    first_store = SourceQuotaStore(
        quota_path,
        boot_id="boot-a",
        monotonic_ns=lambda: 0,
    )
    gateway = _gateway(
        tmp_path,
        lambda _request: (_ for _ in ()).throw(KeyboardInterrupt()),
        completion_clock=lambda: AVAILABLE_AT,
        quota_store=first_store,
        quota_units_per_window=2,
        pending_recovery_min_age_seconds=60,
    )
    with pytest.raises(KeyboardInterrupt):
        gateway.capture_once(trade_date=TRADE_DATE, observed_at=OBSERVED_AT)

    restarted = _gateway(
        tmp_path,
        lambda _request: pytest.fail("active attempt must not be dispatched twice"),
        completion_clock=lambda: AVAILABLE_AT,
        quota_store=SourceQuotaStore(
            quota_path,
            boot_id="boot-a",
            monotonic_ns=lambda: 30_000_000_000,
        ),
        quota_units_per_window=2,
        pending_recovery_min_age_seconds=60,
    )
    capture = restarted.capture_once(trade_date=TRADE_DATE, observed_at=OBSERVED_AT)

    assert capture.quality_status is BatchQualityStatus.STALE
    assert capture.degraded_reasons == ("source_attempt_pending",)
    (attempt,) = first_store.list_attempts(source="tushare.daily_close")
    assert attempt.outcome is SourceQuotaAttemptOutcome.PENDING


def test_periodic_recovery_waits_for_the_active_capture_lock(tmp_path: Path) -> None:
    quota_path = tmp_path / "quota.sqlite3"
    provider_started = Event()
    provider_release = Event()

    def slow_provider(_request: DailyCloseSourceRequest) -> object:
        provider_started.set()
        assert provider_release.wait(timeout=2)
        return _snapshot()

    active = _gateway(
        tmp_path,
        slow_provider,
        completion_clock=lambda: AVAILABLE_AT,
        quota_store=SourceQuotaStore(
            quota_path,
            boot_id="boot-a",
            monotonic_ns=lambda: 0,
        ),
        quota_units_per_window=2,
        pending_recovery_min_age_seconds=60,
    )
    recovery = _gateway(
        tmp_path,
        lambda _request: pytest.fail("recovery must not call the provider"),
        completion_clock=lambda: AVAILABLE_AT,
        quota_store=SourceQuotaStore(
            quota_path,
            boot_id="boot-a",
            monotonic_ns=lambda: 120_000_000_000,
        ),
        quota_units_per_window=2,
        pending_recovery_min_age_seconds=60,
    )

    with ThreadPoolExecutor(max_workers=2) as executor:
        capture_future = executor.submit(
            active.capture_once,
            trade_date=TRADE_DATE,
            observed_at=OBSERVED_AT,
        )
        assert provider_started.wait(timeout=2)
        recovery_future = executor.submit(
            recovery.recover_stale_source_attempts,
            observed_at=OBSERVED_AT + timedelta(minutes=5),
        )
        assert not recovery_future.done()
        provider_release.set()
        capture = capture_future.result(timeout=2)
        recovered = recovery_future.result(timeout=2)

    assert capture.quality_status is BatchQualityStatus.PUBLISHED
    assert recovered == ()
    (attempt,) = SourceQuotaStore(quota_path).list_attempts(source="tushare.daily_close")
    assert attempt.outcome is SourceQuotaAttemptOutcome.SUCCESS


def test_gateway_recovers_a_crashed_attempt_across_daily_windows(tmp_path: Path) -> None:
    quota_path = tmp_path / "quota.sqlite3"
    first = _gateway(
        tmp_path,
        lambda _request: (_ for _ in ()).throw(KeyboardInterrupt()),
        completion_clock=lambda: AVAILABLE_AT,
        quota_store=SourceQuotaStore(quota_path, boot_id="boot-old"),
        quota_units_per_window=2,
    )
    with pytest.raises(KeyboardInterrupt):
        first.capture_once(trade_date=TRADE_DATE, observed_at=OBSERVED_AT)

    next_day = OBSERVED_AT + timedelta(days=1)
    restarted = _gateway(
        tmp_path,
        lambda _request: pytest.fail("cross-day recovery must not refetch"),
        completion_clock=lambda: next_day + timedelta(seconds=2),
        quota_store=SourceQuotaStore(quota_path, boot_id="boot-new"),
        quota_units_per_window=2,
    )
    capture = restarted.capture_once(trade_date=TRADE_DATE, observed_at=next_day)

    assert capture.quality_status is BatchQualityStatus.STALE
    assert capture.degraded_reasons == ("source_attempt_unknown",)
    (attempt,) = SourceQuotaStore(quota_path).list_attempts(source="tushare.daily_close")
    assert attempt.outcome is SourceQuotaAttemptOutcome.UNKNOWN


@pytest.mark.parametrize("actual_call_count", (6, 8))
def test_gateway_fails_closed_when_source_call_receipt_mismatches_reserved_cost(
    tmp_path: Path,
    actual_call_count: int,
) -> None:
    quota = SourceQuotaStore(tmp_path / "quota.sqlite3")
    gateway = _gateway(
        tmp_path,
        lambda _request: DailyCloseFetchResult(
            source="tushare.daily_close",
            actual_call_count=actual_call_count,
            interface_calls=tuple(f"call-{index}" for index in range(actual_call_count)),
            payload=_snapshot(),
        ),
        completion_clock=lambda: AVAILABLE_AT,
        quota_store=quota,
        quota_units_per_window=7,
        quota_cost_per_request=7,
        require_source_usage_receipt=True,
    )

    capture = gateway.capture_once(trade_date=TRADE_DATE, observed_at=OBSERVED_AT)

    assert capture.quality_status is BatchQualityStatus.STALE
    assert capture.degraded_reasons == ("source_call_count_mismatch",)
    (attempt,) = quota.list_attempts(source="tushare.daily_close")
    assert attempt.outcome is SourceQuotaAttemptOutcome.FAILURE
    assert quota.remaining("tushare.daily_close", now=OBSERVED_AT) == 0


def test_gateway_rejects_a_seven_call_receipt_with_repeated_interface(tmp_path: Path) -> None:
    quota = SourceQuotaStore(tmp_path / "quota.sqlite3")
    gateway = _gateway(
        tmp_path,
        lambda _request: DailyCloseFetchResult(
            source="tushare.daily_close",
            actual_call_count=7,
            interface_calls=("daily_by_date",) * 7,
            payload=_snapshot(),
        ),
        completion_clock=lambda: AVAILABLE_AT,
        quota_store=quota,
        quota_units_per_window=7,
        quota_cost_per_request=7,
        require_source_usage_receipt=True,
    )

    capture = gateway.capture_once(trade_date=TRADE_DATE, observed_at=OBSERVED_AT)

    assert capture.degraded_reasons == ("source_call_count_mismatch",)
    assert quota.list_attempts()[0].outcome is SourceQuotaAttemptOutcome.FAILURE


def test_default_daily_fetcher_returns_a_seven_interface_usage_receipt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshot = _snapshot()

    class FakeAdapter:
        def __init__(self, *, token: str) -> None:
            assert token == "not-a-real-token"

        def daily_by_date(self, _trade_date: date) -> pd.DataFrame:
            return pd.DataFrame(snapshot["daily_bar"])

        def daily_basic_by_date(self, _trade_date: date) -> pd.DataFrame:
            return pd.DataFrame(snapshot["daily_basic"])

        def adj_factor_by_date(self, _trade_date: date) -> pd.DataFrame:
            return pd.DataFrame(snapshot["adj_factor"])

        def index_daily_major_by_date(self, _trade_date: date) -> pd.DataFrame:
            return pd.DataFrame(snapshot["index_daily"])

        def stock_basic(self, list_status: str = "L") -> pd.DataFrame:
            assert list_status == "L"
            return pd.DataFrame(({"ts_code": "600000.SH", "name": "浦发银行", "list_status": "L"},))

        def stock_st_raw(self, _trade_date: date) -> pd.DataFrame:
            return pd.DataFrame(columns=("ts_code",))

        def suspend_d_raw(self, _trade_date: date) -> pd.DataFrame:
            return pd.DataFrame(columns=("ts_code", "trade_date", "suspend_type", "suspend_timing"))

    monkeypatch.setattr("rquant.adapter.tushare.TushareAdapter", FakeAdapter)
    fetch = runtime_builder_daily._tushare_daily_close_fetcher(
        {"TUSHARE_TOKEN_MAIN": "not-a-real-token"}
    )

    result = fetch(DailyCloseSourceRequest(source="tushare.daily_close", trade_date=TRADE_DATE))

    assert isinstance(result, DailyCloseFetchResult)
    assert result.actual_call_count == 7
    assert result.interface_calls == DAILY_CLOSE_SOURCE_INTERFACES


def _records(gateway: DailyCloseGateway):
    return gateway.spool.list_after(LiveChannel.DAILY_CLOSE, sequence=-1)


def test_capture_persists_typed_raw_payload_and_reuses_success_after_restart(
    tmp_path: Path,
) -> None:
    requests: list[DailyCloseSourceRequest] = []

    def fetch(request: DailyCloseSourceRequest) -> object:
        requests.append(request)
        return _snapshot()

    first_gateway = _gateway(tmp_path, fetch, completion_clock=lambda: AVAILABLE_AT)
    first = first_gateway.capture_once(trade_date=TRADE_DATE, observed_at=OBSERVED_AT)
    restarted = _gateway(tmp_path, fetch, completion_clock=lambda: AVAILABLE_AT)
    replay = restarted.capture_once(trade_date=TRADE_DATE, observed_at=OBSERVED_AT)

    assert len(requests) == 1
    assert first.published is True
    assert replay.published is False
    assert replay.batch_id == first.batch_id
    assert replay.sequence == first.sequence == 0

    records = _records(restarted)
    assert len(records) == 1
    record = records[0]
    payload_bytes = restarted.spool.read_payload(record)
    payload = restarted.decode_payload(payload_bytes)
    assert isinstance(payload, DailyCloseRawPayload)
    assert payload.schema_version == 1
    assert payload.source_request == requests[0]
    assert payload.source_request_id == requests[0].identity_sha256
    assert payload.observed_at == OBSERVED_AT
    assert payload.available_at == AVAILABLE_AT
    assert payload.revision == 1
    assert payload.revises_batch_id is None
    assert payload.quality_status is BatchQualityStatus.PUBLISHED
    assert payload.content_sha256 == payload.facts.identity_sha256
    assert record.envelope.content_sha256 == hashlib.sha256(payload_bytes).hexdigest()
    assert record.envelope.row_count == 5


def test_explicit_refresh_persists_revised_facts_without_overwriting_prior_generation(
    tmp_path: Path,
) -> None:
    responses = iter((_snapshot(close=10.2), _snapshot(close=10.3)))
    gateway = _gateway(tmp_path, lambda _request: next(responses))

    first = gateway.capture_once(trade_date=TRADE_DATE, observed_at=OBSERVED_AT)
    revised = gateway.capture_once(
        trade_date=TRADE_DATE,
        observed_at=OBSERVED_AT + timedelta(minutes=10),
        refresh=True,
    )

    assert revised.published is True
    assert revised.sequence == 1
    assert revised.revision == 2
    assert revised.batch_id != first.batch_id
    records = _records(gateway)
    assert [record.envelope.revision for record in records] == [1, 2]
    assert records[1].envelope.revises_batch_id == records[0].envelope.batch_id
    original = gateway.decode_payload(gateway.spool.read_payload(records[0]))
    replacement = gateway.decode_payload(gateway.spool.read_payload(records[1]))
    assert original.facts.daily_bar[0].close == 10.2
    assert replacement.facts.daily_bar[0].close == 10.3


def test_remote_row_order_does_not_create_a_false_revision(tmp_path: Path) -> None:
    first = _snapshot()
    second_bar = {
        **first["daily_bar"][0],
        "ts_code": "000001.SZ",
        "open": 8.0,
        "high": 8.3,
        "low": 7.9,
        "close": 8.2,
        "pre_close": 8.0,
        "change": 0.2,
        "pct_chg": 2.5,
        "vol": 900_000.0,
        "amount": 7_380_000.0,
    }
    first["daily_bar"] = (first["daily_bar"][0], second_bar)
    reordered = {**first, "daily_bar": tuple(reversed(first["daily_bar"]))}
    responses = iter((first, reordered))
    gateway = _gateway(tmp_path, lambda _request: next(responses))

    initial = gateway.capture_once(trade_date=TRADE_DATE, observed_at=OBSERVED_AT)
    replay = gateway.capture_once(
        trade_date=TRADE_DATE,
        observed_at=OBSERVED_AT + timedelta(minutes=5),
        refresh=True,
    )

    assert initial.published is True
    assert replay.published is False
    assert replay.batch_id == initial.batch_id
    assert len(_records(gateway)) == 1


def test_late_revision_uses_its_trade_date_chain_after_a_newer_day(tmp_path: Path) -> None:
    later_date = TRADE_DATE + timedelta(days=3)
    completion_times = iter(
        (
            OBSERVED_AT,
            OBSERVED_AT + timedelta(days=3),
            OBSERVED_AT + timedelta(days=4),
        )
    )
    responses = iter(
        (
            _snapshot(close=10.2),
            _snapshot(trade_date=later_date, close=10.4),
            _snapshot(close=10.3),
        )
    )
    gateway = _gateway(
        tmp_path,
        lambda _request: next(responses),
        completion_clock=lambda: next(completion_times),
    )

    first = gateway.capture_once(trade_date=TRADE_DATE, observed_at=OBSERVED_AT)
    gateway.capture_once(
        trade_date=later_date,
        observed_at=OBSERVED_AT + timedelta(days=3),
    )
    late = gateway.capture_once(
        trade_date=TRADE_DATE,
        observed_at=OBSERVED_AT + timedelta(days=4),
        refresh=True,
    )

    records = _records(gateway)
    assert late.revision == 2
    assert records[2].envelope.revises_batch_id == first.batch_id


def test_source_timeout_persists_stale_empty_generation_and_can_retry(tmp_path: Path) -> None:
    calls = 0

    def fetch(_request: DailyCloseSourceRequest) -> object:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise TimeoutError("source down")
        return _snapshot()

    quota = SourceQuotaStore(tmp_path / "quota.sqlite3")
    gateway = _gateway(
        tmp_path,
        fetch,
        quota_store=quota,
        quota_units_per_window=2,
    )
    stale = gateway.capture_once(trade_date=TRADE_DATE, observed_at=OBSERVED_AT)
    refused = gateway.capture_once(
        trade_date=TRADE_DATE,
        observed_at=OBSERVED_AT + timedelta(seconds=30),
    )
    recovered = gateway.capture_once(
        trade_date=TRADE_DATE,
        observed_at=OBSERVED_AT + timedelta(minutes=1),
        retry_ordinal=1,
    )

    assert stale.quality_status is BatchQualityStatus.STALE
    assert stale.degraded_reasons == ("source_error:TimeoutError",)
    assert refused.degraded_reasons == ("source_error:TimeoutError",)
    assert recovered.quality_status is BatchQualityStatus.PUBLISHED
    assert recovered.revision == 2
    stale_payload = gateway.decode_payload(gateway.spool.read_payload(_records(gateway)[0]))
    assert stale_payload.facts.total_rows == 0
    assert [attempt.outcome for attempt in quota.list_attempts()] == [
        SourceQuotaAttemptOutcome.FAILURE,
        SourceQuotaAttemptOutcome.SUCCESS,
    ]
    assert quota.remaining("tushare.daily_close", now=OBSERVED_AT) == 0


def test_declared_partial_snapshot_is_degraded_and_immutable(tmp_path: Path) -> None:
    gateway = _gateway(
        tmp_path,
        lambda _request: _snapshot(partial=(DailyCloseDataset.DAILY_BASIC.value,)),
    )

    capture = gateway.capture_once(trade_date=TRADE_DATE, observed_at=OBSERVED_AT)

    assert capture.quality_status is BatchQualityStatus.DEGRADED
    assert capture.degraded_reasons == ("partial:daily_basic",)
    assert len(_records(gateway)) == 1


def test_non_authoritative_daily_close_batches_do_not_advance_current(tmp_path: Path) -> None:
    responses = iter(
        (
            _snapshot(),
            _snapshot(partial=(DailyCloseDataset.DAILY_BASIC.value,)),
            TimeoutError("source down"),
            _snapshot(close=10.3),
        )
    )

    def fetch(_request: DailyCloseSourceRequest) -> object:
        response = next(responses)
        if isinstance(response, Exception):
            raise response
        return response

    gateway = _gateway(tmp_path, fetch)
    authoritative = gateway.capture_once(trade_date=TRADE_DATE, observed_at=OBSERVED_AT)
    degraded = gateway.capture_once(
        trade_date=TRADE_DATE,
        observed_at=OBSERVED_AT + timedelta(minutes=1),
        refresh=True,
    )
    stale = gateway.capture_once(
        trade_date=TRADE_DATE,
        observed_at=OBSERVED_AT + timedelta(minutes=2),
        refresh=True,
    )

    current = gateway.spool.current(LiveChannel.DAILY_CLOSE)
    assert current is not None
    assert current.batch_id == authoritative.batch_id
    assert gateway.spool.source_descriptor(LiveChannel.DAILY_CLOSE).high_watermark == 0
    assert [record.envelope.quality_status for record in _records(gateway)] == [
        BatchQualityStatus.PUBLISHED,
        BatchQualityStatus.DEGRADED,
        BatchQualityStatus.STALE,
    ]
    assert degraded.sequence == 1
    assert stale.sequence == 2

    recovered = gateway.capture_once(
        trade_date=TRADE_DATE,
        observed_at=OBSERVED_AT + timedelta(minutes=3),
        refresh=True,
    )

    assert recovered.sequence == 3
    assert gateway.spool.current(LiveChannel.DAILY_CLOSE).batch_id == recovered.batch_id


def test_unknown_huge_raw_value_is_quarantined_with_bounded_truncated_evidence(
    tmp_path: Path,
) -> None:
    consumed = 0

    def huge_unknown():
        nonlocal consumed
        for value in range(50_000):
            consumed += 1
            yield value

    raw = _snapshot()
    raw["unexpected"] = huge_unknown()
    gateway = _gateway(
        tmp_path,
        lambda _request: raw,
        max_evidence_container_items=3,
        max_evidence_nodes=32,
        max_evidence_bytes=4_096,
    )

    capture = gateway.capture_once(trade_date=TRADE_DATE, observed_at=OBSERVED_AT)
    (record,) = gateway.list_quarantined()

    assert capture.quality_status is BatchQualityStatus.QUARANTINED
    assert capture.degraded_reasons == ("unknown_field:unexpected",)
    assert consumed == 4
    assert record.raw_encoding == "truncated"
    assert record.evidence_truncated is True
    assert record.raw_payload_base64 is not None
    evidence = base64.b64decode(record.raw_payload_base64, validate=True)
    summary = json.loads(evidence)
    assert summary["truncated"] is True
    assert summary["truncation_reason"] == "container_items"
    assert record.content_sha256 == hashlib.sha256(evidence).hexdigest()
    assert record.raw_size_bytes == len(evidence)


@pytest.mark.parametrize(
    ("mutate", "reason"),
    [
        (lambda value: value.pop("adj_factor"), "invalid_payload"),
        (
            lambda value: value["daily_bar"][0].update({"close": float("nan")}),
            "invalid_payload",
        ),
        (
            lambda value: value["daily_bar"].append(value["daily_bar"][0].copy()),
            "duplicate_key:daily_bar",
        ),
        (
            lambda value: value["daily_bar"][0].update(
                {"trade_date": TRADE_DATE + timedelta(days=1)}
            ),
            "trade_date_mismatch:daily_bar",
        ),
    ],
)
def test_invalid_or_future_facts_are_quarantined_without_advancing_current(
    tmp_path: Path,
    mutate: Callable[[dict[str, object]], object],
    reason: str,
) -> None:
    raw = _snapshot()
    for key, value in tuple(raw.items()):
        if isinstance(value, tuple):
            raw[key] = list(value)
    mutate(raw)
    gateway = _gateway(tmp_path, lambda _request: raw)

    capture = gateway.capture_once(trade_date=TRADE_DATE, observed_at=OBSERVED_AT)

    assert capture.quality_status is BatchQualityStatus.QUARANTINED
    assert capture.published is False
    assert capture.quarantined is True
    assert reason in capture.degraded_reasons
    assert gateway.spool.current(LiveChannel.DAILY_CLOSE) is None
    quarantine = gateway.list_quarantined()
    assert len(quarantine) == 1
    assert quarantine[0].content_sha256 == capture.content_sha256


@pytest.mark.parametrize(
    ("config", "mutation", "reason"),
    [
        (
            {"max_rows_per_dataset": 1},
            lambda value: value["daily_bar"].append(
                {**value["daily_bar"][0], "ts_code": "000001.SZ"}
            ),
            "row_bound:daily_bar",
        ),
        (
            {"max_fields_per_row": 8},
            lambda _value: None,
            "field_bound:daily_bar",
        ),
        (
            {"max_payload_bytes": 128},
            lambda _value: None,
            "byte_bound",
        ),
    ],
)
def test_resource_bounds_quarantine_before_publication(
    tmp_path: Path,
    config: dict[str, object],
    mutation: Callable[[dict[str, object]], object],
    reason: str,
) -> None:
    raw = _snapshot()
    for key, value in tuple(raw.items()):
        if isinstance(value, tuple):
            raw[key] = list(value)
    mutation(raw)
    gateway = _gateway(tmp_path, lambda _request: raw, **config)

    capture = gateway.capture_once(trade_date=TRADE_DATE, observed_at=OBSERVED_AT)

    assert capture.quality_status is BatchQualityStatus.QUARANTINED
    assert reason in capture.degraded_reasons
    assert gateway.spool.current(LiveChannel.DAILY_CLOSE) is None


def test_generator_row_bound_stops_after_limit_plus_one(tmp_path: Path) -> None:
    consumed = 0
    daily_bar = _snapshot()["daily_bar"][0]

    def oversized_rows():
        nonlocal consumed
        while consumed < 20:
            consumed += 1
            yield {**daily_bar, "ts_code": f"{consumed:06d}.SZ"}

    raw = _snapshot()
    raw["daily_bar"] = oversized_rows()
    gateway = _gateway(
        tmp_path,
        lambda _request: raw,
        max_rows_per_dataset=2,
    )

    capture = gateway.capture_once(trade_date=TRADE_DATE, observed_at=OBSERVED_AT)

    assert capture.quality_status is BatchQualityStatus.QUARANTINED
    assert capture.degraded_reasons == ("row_bound:daily_bar",)
    assert consumed == 3


def test_valid_generator_rows_are_materialized_once(tmp_path: Path) -> None:
    consumed = 0
    daily_bar = _snapshot()["daily_bar"][0]

    def rows():
        nonlocal consumed
        consumed += 1
        yield daily_bar

    raw = _snapshot()
    raw["daily_bar"] = rows()
    gateway = _gateway(tmp_path, lambda _request: raw)

    capture = gateway.capture_once(trade_date=TRADE_DATE, observed_at=OBSERVED_AT)

    assert capture.quality_status is BatchQualityStatus.PUBLISHED
    assert consumed == 1


@pytest.mark.parametrize(
    "tamper",
    ("raw_payload_base64", "content_sha256", "raw_size_bytes", "quarantine_id"),
)
def test_list_quarantined_rejects_tampered_evidence_and_identity(
    tmp_path: Path,
    tamper: str,
) -> None:
    raw = _snapshot()
    raw.pop("adj_factor")
    gateway = _gateway(tmp_path, lambda _request: raw)
    gateway.capture_once(trade_date=TRADE_DATE, observed_at=OBSERVED_AT)
    (path,) = tuple((tmp_path / "live" / "quarantine" / "daily_close").glob("*.json"))
    stored = json.loads(path.read_bytes())

    if tamper == "raw_payload_base64":
        evidence = bytearray(base64.b64decode(stored[tamper], validate=True))
        evidence[-1] ^= 1
        stored[tamper] = base64.b64encode(evidence).decode("ascii")
    elif tamper == "raw_size_bytes":
        stored[tamper] += 1
    else:
        stored[tamper] = "0" * 64
    path.write_bytes(
        json.dumps(stored, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode("utf-8")
    )

    with pytest.raises(DailyCloseValidationError, match="quarantine record is invalid"):
        gateway.list_quarantined()


@pytest.mark.parametrize("replacement", ("symlink", "hardlink"))
def test_existing_quarantine_record_rejects_link_replacement(
    tmp_path: Path,
    replacement: str,
) -> None:
    raw = _snapshot()
    raw.pop("adj_factor")
    gateway = _gateway(tmp_path, lambda _request: raw)
    gateway.capture_once(trade_date=TRADE_DATE, observed_at=OBSERVED_AT)
    (path,) = tuple((tmp_path / "live" / "quarantine" / "daily_close").glob("*.json"))
    external = path.with_name(f"{replacement}-target.json")

    if replacement == "symlink":
        external.write_bytes(path.read_bytes())
        path.unlink()
        path.symlink_to(external.name)
    else:
        os.link(path, external)

    with pytest.raises(DailyCloseValidationError, match="quarantine record is unsafe"):
        gateway.capture_once(trade_date=TRADE_DATE, observed_at=OBSERVED_AT)


def test_existing_quarantine_record_fails_closed_on_open_time_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw = _snapshot()
    raw.pop("adj_factor")
    gateway = _gateway(tmp_path, lambda _request: raw)
    gateway.capture_once(trade_date=TRADE_DATE, observed_at=OBSERVED_AT)
    (path,) = tuple((tmp_path / "live" / "quarantine" / "daily_close").glob("*.json"))
    external = path.with_name("replacement-target.json")
    external.write_bytes(path.read_bytes())
    original_open = daily_close_gateway_module.os.open
    replaced = False

    def replace_before_open(*args: object, **kwargs: object) -> int:
        nonlocal replaced
        if not replaced and args[0] == path.name and kwargs.get("dir_fd") is not None:
            replaced = True
            path.unlink()
            path.symlink_to(external.name)
        return original_open(*args, **kwargs)

    monkeypatch.setattr(daily_close_gateway_module.os, "open", replace_before_open)

    with pytest.raises(DailyCloseValidationError, match="quarantine record is unsafe"):
        gateway.capture_once(trade_date=TRADE_DATE, observed_at=OBSERVED_AT)


def test_capture_rejects_observation_before_market_close_without_fetch(tmp_path: Path) -> None:
    called = False

    def fetch(_request: DailyCloseSourceRequest) -> object:
        nonlocal called
        called = True
        return _snapshot()

    gateway = _gateway(tmp_path, fetch)

    with pytest.raises(DailyCloseValidationError, match="market close"):
        gateway.capture_once(
            trade_date=TRADE_DATE,
            observed_at=datetime(2026, 7, 31, 6, 59, tzinfo=UTC),
        )

    assert called is False


def test_default_available_at_is_sampled_after_remote_fetch_completion(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fetch_completed_at = OBSERVED_AT + timedelta(seconds=30)
    clock_now = OBSERVED_AT
    fetch_completed = False

    def fetch(_request: DailyCloseSourceRequest) -> object:
        nonlocal clock_now, fetch_completed
        clock_now = fetch_completed_at
        fetch_completed = True
        return _snapshot()

    def completion_time() -> datetime:
        assert fetch_completed is True
        return clock_now

    monkeypatch.setattr(
        daily_close_gateway_module,
        "_completion_utc_now",
        completion_time,
        raising=False,
    )
    gateway = _gateway(tmp_path, fetch)

    capture = gateway.capture_once(trade_date=TRADE_DATE, observed_at=OBSERVED_AT)

    assert capture.observed_at == OBSERVED_AT
    assert capture.available_at >= fetch_completed_at


def test_available_at_clock_is_sampled_once_per_remote_response(tmp_path: Path) -> None:
    clock_values = iter((AVAILABLE_AT,))
    gateway = _gateway(
        tmp_path,
        lambda _request: _snapshot(),
        completion_clock=lambda: next(clock_values),
    )

    capture = gateway.capture_once(trade_date=TRADE_DATE, observed_at=OBSERVED_AT)

    assert capture.available_at == AVAILABLE_AT
    with pytest.raises(StopIteration):
        next(clock_values)
