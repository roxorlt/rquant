from __future__ import annotations

import json
import multiprocessing
import os
import signal
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from rquant.live_contracts import BatchQualityStatus, LiveChannel
from rquant.live_spool import LiveBatchSpool
from rquant.source_quota_store import SourceQuotaStore
from rquant.watchlist_quote_gateway import WatchlistQuoteGateway, WatchlistQuoteGatewayConfig
from rquant.watchlist_quote_provider import AkshareSinaWatchlistQuoteProvider

NOW = datetime(2026, 7, 31, 1, 26, 5, tzinfo=UTC)
COMMIT = "a" * 40
SHANGHAI = ZoneInfo("Asia/Shanghai")


def _quotes(*, source_observed_at: datetime | None = NOW - timedelta(seconds=1)) -> pd.DataFrame:
    row: dict[str, object] = {
        "ts_code": "600000.SH",
        "price": 10.1,
        "open": 10.0,
        "high": 10.2,
        "low": 9.9,
        "volume": 1_000.0,
        "amount": 10_100.0,
    }
    if source_observed_at is not None:
        row["source_observed_at"] = source_observed_at
    return pd.DataFrame([row])


def _record_spawn_loader_start() -> pd.DataFrame:
    started_at = datetime.now(UTC)
    line = json.dumps(
        {
            "monotonic_ns": time.monotonic_ns(),
            "started_at": started_at.isoformat(),
        },
        separators=(",", ":"),
    )
    path = Path(os.environ["RQUANT_WATCHLIST_QUOTE_LOADER_STARTS"])
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    try:
        os.write(descriptor, f"{line}\n".encode())
    finally:
        os.close(descriptor)
    return pd.DataFrame(
        [
            {
                "代码": "600000",
                "最新价": 10.1,
                "今开": 10.0,
                "最高": 10.2,
                "最低": 9.9,
                "成交量": 1_000.0,
                "成交额": 10_100.0,
            }
        ]
    )


def _gateway(
    root: Path,
    provider: Callable[..., pd.DataFrame],
    *,
    completion_at: datetime = NOW + timedelta(minutes=1),
    clock: Callable[[], datetime] | None = None,
    **config: object,
) -> WatchlistQuoteGateway:
    effective_clock = clock or (lambda: completion_at)

    def reporting_provider(
        codes: tuple[str, ...],
        *,
        timeout_seconds: float,
        on_started: Callable[[datetime], None],
    ) -> pd.DataFrame:
        on_started(effective_clock())
        return provider(codes, timeout_seconds=timeout_seconds)

    return WatchlistQuoteGateway(
        spool=LiveBatchSpool(root / "spool"),
        provider=reporting_provider,
        config=WatchlistQuoteGatewayConfig(
            producer_version="watchlist-quote-source-v1",
            producer_commit=COMMIT,
            **config,
        ),
        quota_store=(
            SourceQuotaStore(root / "quota.sqlite3")
            if config.get("quota_units_per_window") is not None
            else None
        ),
        clock=effective_clock,
    )


def test_gateway_separates_request_response_and_true_source_observation_times(
    tmp_path: Path,
) -> None:
    from rquant.watchlist_quote_gateway import decode_watchlist_quote_payload

    calls: list[tuple[tuple[str, ...], float]] = []

    def provider(codes: tuple[str, ...], *, timeout_seconds: float) -> pd.DataFrame:
        calls.append((codes, timeout_seconds))
        return _quotes()

    request_at = NOW
    response_at = NOW + timedelta(minutes=1)
    clock_values = iter(
        (
            NOW - timedelta(milliseconds=2),
            NOW - timedelta(milliseconds=1),
            request_at,
            response_at,
        )
    )
    gateway = _gateway(
        tmp_path,
        provider,
        request_timeout_seconds=2.5,
        clock=lambda: next(clock_values),
    )

    capture = gateway.capture_once(
        codes=("600000.SH",),
        scheduled_at=NOW - timedelta(seconds=2),
        universe_as_of=NOW - timedelta(seconds=3),
        trade_date=NOW.date(),
    )

    assert calls == [(("600000.SH",), 2.5)]
    assert capture.published is True
    record = gateway.spool.list_after(LiveChannel.WATCHLIST_QUOTE, sequence=-1)[0]
    envelope = record.envelope
    assert envelope.schema_version == 2
    assert envelope.source == "akshare.stock_zh_a_spot"
    assert envelope.received_at == NOW + timedelta(minutes=1)
    assert envelope.event_time_start == NOW - timedelta(seconds=1)
    assert envelope.event_time_end == NOW - timedelta(seconds=1)
    assert envelope.available_at >= envelope.received_at
    assert envelope.producer_commit == COMMIT
    assert envelope.sequence == 0
    assert envelope.content_sha256 == capture.pointer.content_sha256
    restored = decode_watchlist_quote_payload(gateway.spool.read_payload(record))
    assert restored.loc[0, "ts_code"] == "600000.SH"
    assert restored.loc[0, "observed_at"] == NOW - timedelta(seconds=1)
    assert restored.loc[0, "scheduled_at"] == NOW - timedelta(seconds=2)
    assert restored.loc[0, "universe_as_of"] == NOW - timedelta(seconds=3)
    assert restored.loc[0, "requested_at"] == request_at
    assert restored.loc[0, "response_received_at"] == response_at
    assert restored.loc[0, "fetched_at"] == response_at
    assert restored.loc[0, "source_timestamp_provenance"] == "provider_source_timestamp"
    assert restored.loc[0, "trade_date"] == NOW.date()
    assert restored.loc[0, "schema_version"] == 2

    retry = gateway.capture_once(
        codes=("600000.SH",),
        scheduled_at=NOW - timedelta(seconds=2),
        universe_as_of=NOW - timedelta(seconds=3),
        trade_date=NOW.date(),
    )
    assert retry.published is False
    assert retry.pointer.sequence == capture.pointer.sequence
    assert calls == [(("600000.SH",), 2.5)]


@pytest.mark.parametrize(
    ("scheduled_at", "requested_at", "response_received_at"),
    (
        (
            datetime(2026, 7, 31, 1, 25, 0, tzinfo=UTC),
            datetime(2026, 7, 31, 1, 25, 0, 500_000, tzinfo=UTC),
            datetime(2026, 7, 31, 1, 25, 2, tzinfo=UTC),
        ),
        (
            datetime(2026, 7, 31, 2, 0, tzinfo=UTC),
            datetime(2026, 7, 31, 2, 0, 0, 500_000, tzinfo=UTC),
            datetime(2026, 7, 31, 2, 0, 1, tzinfo=UTC),
        ),
    ),
)
def test_gateway_uses_response_completion_when_provider_has_no_source_timestamp(
    tmp_path: Path,
    scheduled_at: datetime,
    requested_at: datetime,
    response_received_at: datetime,
) -> None:
    from rquant.watchlist_quote_gateway import decode_watchlist_quote_payload

    admitted_at = scheduled_at + (requested_at - scheduled_at) / 2
    clock_values = iter((scheduled_at, admitted_at, requested_at, response_received_at))
    gateway = _gateway(
        tmp_path,
        lambda _codes, *, timeout_seconds: _quotes(source_observed_at=None),
        clock=lambda: next(clock_values),
    )

    capture = gateway.capture_once(
        codes=("600000.SH",),
        scheduled_at=scheduled_at,
        universe_as_of=scheduled_at,
        trade_date=scheduled_at.astimezone().date(),
    )

    record = gateway.spool.list_after(LiveChannel.WATCHLIST_QUOTE, sequence=-1)[0]
    restored = decode_watchlist_quote_payload(gateway.spool.read_payload(record))
    assert capture.published is True
    assert restored.loc[0, "observed_at"] == response_received_at
    assert restored.loc[0, "scheduled_at"] == scheduled_at
    assert restored.loc[0, "universe_as_of"] == scheduled_at
    assert restored.loc[0, "requested_at"] == requested_at
    assert restored.loc[0, "source_timestamp_provenance"] == "response_received_at_fallback"
    assert record.envelope.event_time_end == response_received_at
    assert record.envelope.received_at == response_received_at
    assert record.envelope.available_at >= response_received_at


def test_gateway_rejects_future_observations_without_publishing(tmp_path: Path) -> None:
    from rquant.watchlist_quote_gateway import WatchlistQuoteValidationError

    gateway = _gateway(
        tmp_path,
        lambda _codes, *, timeout_seconds: _quotes(source_observed_at=NOW + timedelta(minutes=2)),
    )

    try:
        gateway.capture_once(
            codes=("600000.SH",),
            scheduled_at=NOW,
            universe_as_of=NOW,
            trade_date=NOW.date(),
        )
    except WatchlistQuoteValidationError as exc:
        assert "future observation" in str(exc)
    else:
        raise AssertionError("future quote observations must be rejected")
    assert gateway.spool.current(LiveChannel.WATCHLIST_QUOTE) is None


def test_gateway_rejects_regressing_dispatch_clock_without_publishing(tmp_path: Path) -> None:
    from rquant.watchlist_quote_gateway import WatchlistQuoteValidationError

    clock_values = iter((NOW, NOW, NOW - timedelta(milliseconds=1), NOW))
    gateway = _gateway(
        tmp_path,
        lambda _codes, *, timeout_seconds: pytest.fail("provider must not run"),
        clock=lambda: next(clock_values),
    )

    with pytest.raises(WatchlistQuoteValidationError, match="provider start time"):
        gateway.capture_once(
            codes=("600000.SH",),
            scheduled_at=NOW,
            universe_as_of=NOW,
            trade_date=NOW.date(),
        )

    assert gateway.spool.current(LiveChannel.WATCHLIST_QUOTE) is None
    state = json.loads(
        (tmp_path / "spool" / "watchlist-quote-state.json").read_text(encoding="utf-8")
    )
    assert state["admitted_at"] == NOW.isoformat().replace("+00:00", "Z")
    assert state["last_dispatch_at"] is None
    assert state["inflight_request_id"]


def test_gateway_source_timestamp_provenance_does_not_depend_on_provider_index(
    tmp_path: Path,
) -> None:
    from rquant.watchlist_quote_gateway import decode_watchlist_quote_payload

    quotes = _quotes()
    quotes.index = [7]
    gateway = _gateway(tmp_path, lambda _codes, *, timeout_seconds: quotes)

    gateway.capture_once(
        codes=("600000.SH",),
        scheduled_at=NOW,
        universe_as_of=NOW,
        trade_date=NOW.date(),
    )

    record = gateway.spool.list_after(LiveChannel.WATCHLIST_QUOTE, sequence=-1)[0]
    restored = decode_watchlist_quote_payload(gateway.spool.read_payload(record))
    assert restored.loc[0, "observed_at"] == NOW - timedelta(seconds=1)
    assert restored.loc[0, "source_timestamp_provenance"] == "provider_source_timestamp"


def test_gateway_rebuilds_sequence_after_restart_and_keeps_late_quote_evidence(
    tmp_path: Path,
) -> None:
    frames = iter(
        (
            _quotes(source_observed_at=NOW - timedelta(seconds=1)),
            _quotes(source_observed_at=NOW - timedelta(seconds=4)),
        )
    )
    first_clock = iter((NOW, NOW, NOW, NOW + timedelta(seconds=1)))
    first = _gateway(
        tmp_path,
        lambda _codes, *, timeout_seconds: next(frames),
        clock=lambda: next(first_clock),
        rollout_mode="published",
    )

    first.capture_once(
        codes=("600000.SH",),
        scheduled_at=NOW,
        universe_as_of=NOW,
        trade_date=NOW.date(),
    )
    restarted_clock = iter(
        (
            NOW + timedelta(seconds=5),
            NOW + timedelta(seconds=5),
            NOW + timedelta(seconds=5),
            NOW + timedelta(seconds=6),
        )
    )
    restarted = _gateway(
        tmp_path,
        lambda _codes, *, timeout_seconds: next(frames),
        clock=lambda: next(restarted_clock),
        rollout_mode="published",
    )
    capture = restarted.capture_once(
        codes=("600000.SH",),
        scheduled_at=NOW + timedelta(seconds=5),
        universe_as_of=NOW,
        trade_date=NOW.date(),
    )

    records = restarted.spool.list_after(LiveChannel.WATCHLIST_QUOTE, sequence=-1)
    assert [record.envelope.sequence for record in records] == [0, 1]
    assert capture.pointer.sequence == 1
    assert records[1].envelope.event_time_end < records[0].envelope.event_time_end
    assert records[1].envelope.quality_status is BatchQualityStatus.DEGRADED
    assert records[1].envelope.degraded_reasons == ("late_observation",)
    assert restarted.spool.current(LiveChannel.WATCHLIST_QUOTE).sequence == 0


def test_provider_timeout_opens_only_watchlist_circuit_and_records_stale_batches(
    tmp_path: Path,
) -> None:
    calls = 0

    def failing_provider(_codes: tuple[str, ...], *, timeout_seconds: float) -> pd.DataFrame:
        nonlocal calls
        calls += 1
        raise TimeoutError(f"timed out after {timeout_seconds}")

    gateway = _gateway(
        tmp_path,
        failing_provider,
        failure_threshold=1,
        circuit_cooldown_seconds=30,
        quota_units_per_window=12,
    )

    first = gateway.capture_once(
        codes=("600000.SH",),
        scheduled_at=NOW,
        universe_as_of=NOW,
        trade_date=NOW.date(),
    )
    second = gateway.capture_once(
        codes=("600000.SH",),
        scheduled_at=NOW + timedelta(seconds=5),
        universe_as_of=NOW,
        trade_date=NOW.date(),
    )

    assert calls == 1
    assert first.published is second.published is True
    records = gateway.spool.list_after(LiveChannel.WATCHLIST_QUOTE, sequence=-1)
    assert [record.envelope.quality_status for record in records] == [
        BatchQualityStatus.STALE,
        BatchQualityStatus.STALE,
    ]
    assert records[0].envelope.degraded_reasons == ("provider_timeout",)
    assert records[1].envelope.degraded_reasons == ("circuit_open",)


def test_failure_without_started_message_uses_response_as_conservative_dispatch_fence(
    tmp_path: Path,
) -> None:
    provider_calls = 0

    def provider_without_started(
        _codes: tuple[str, ...],
        *,
        timeout_seconds: float,
        on_started: Callable[[datetime], None],
    ) -> pd.DataFrame:
        nonlocal provider_calls
        del timeout_seconds, on_started
        provider_calls += 1
        raise RuntimeError("started message was lost")

    clock_values = iter(
        (
            NOW,
            NOW,
            NOW + timedelta(seconds=2),
            NOW + timedelta(seconds=6),
        )
    )
    gateway = WatchlistQuoteGateway(
        spool=LiveBatchSpool(tmp_path / "missing-started-spool"),
        provider=provider_without_started,
        config=WatchlistQuoteGatewayConfig(
            producer_version="watchlist-quote-source-v1",
            producer_commit=COMMIT,
            quota_units_per_window=12,
        ),
        quota_store=SourceQuotaStore(tmp_path / "missing-started-quota.sqlite3"),
        clock=lambda: next(clock_values),
    )

    gateway.capture_once(
        codes=("600000.SH",),
        scheduled_at=NOW,
        universe_as_of=NOW,
        trade_date=NOW.date(),
    )
    state_path = tmp_path / "missing-started-spool" / "watchlist-quote-state.json"
    failed_state = json.loads(state_path.read_text(encoding="utf-8"))
    assert failed_state["last_dispatch_at"] == (NOW + timedelta(seconds=2)).isoformat().replace(
        "+00:00", "Z"
    )

    gateway.capture_once(
        codes=("600000.SH",),
        scheduled_at=NOW + timedelta(seconds=6),
        universe_as_of=NOW,
        trade_date=NOW.date(),
    )
    records = gateway.spool.list_after(LiveChannel.WATCHLIST_QUOTE, sequence=-1)
    assert provider_calls == 1
    assert records[0].envelope.degraded_reasons == ("provider_error:RuntimeError",)
    assert records[1].envelope.degraded_reasons == ("backoff_active",)


def test_quota_exhaustion_stays_in_the_watchlist_quote_failure_domain(tmp_path: Path) -> None:
    calls = 0

    def provider(_codes: tuple[str, ...], *, timeout_seconds: float) -> pd.DataFrame:
        nonlocal calls
        calls += 1
        return _quotes()

    clock_values = iter(
        (
            NOW,
            NOW,
            NOW,
            NOW + timedelta(seconds=1),
            NOW + timedelta(seconds=5),
            NOW + timedelta(seconds=5),
        )
    )
    gateway = _gateway(
        tmp_path,
        provider,
        clock=lambda: next(clock_values),
        quota_units_per_window=1,
        quota_cost_per_request=1,
    )

    gateway.capture_once(
        codes=("600000.SH",),
        scheduled_at=NOW,
        universe_as_of=NOW,
        trade_date=NOW.date(),
    )
    gateway.capture_once(
        codes=("600000.SH",),
        scheduled_at=NOW + timedelta(seconds=5),
        universe_as_of=NOW,
        trade_date=NOW.date(),
    )

    records = gateway.spool.list_after(LiveChannel.WATCHLIST_QUOTE, sequence=-1)
    assert calls == 1
    assert records[1].envelope.quality_status is BatchQualityStatus.STALE
    assert records[1].envelope.degraded_reasons == ("quota_exhausted",)


def test_partial_retry_state_fails_closed_without_calling_provider(tmp_path: Path) -> None:
    from rquant.watchlist_quote_gateway import WatchlistQuoteStateError

    def provider(_codes: tuple[str, ...], *, timeout_seconds: float) -> pd.DataFrame:
        raise AssertionError(f"provider must not run after partial state: {timeout_seconds}")

    gateway = _gateway(tmp_path, provider)
    (tmp_path / "spool" / "watchlist-quote-state.json").write_text("{", encoding="utf-8")

    with pytest.raises(WatchlistQuoteStateError, match="state is incomplete"):
        gateway.capture_once(
            codes=("600000.SH",),
            scheduled_at=NOW,
            universe_as_of=NOW,
            trade_date=NOW.date(),
        )


def test_naive_persistent_attempt_time_fails_closed_without_dispatch(tmp_path: Path) -> None:
    from rquant.watchlist_quote_gateway import WatchlistQuoteStateError

    gateway = _gateway(
        tmp_path,
        lambda _codes, *, timeout_seconds: pytest.fail("provider must not run"),
    )
    state_path = tmp_path / "spool" / "watchlist-quote-state.json"
    state_path.write_text(
        json.dumps(
            {
                "schema_version": 2,
                "consecutive_failures": 0,
                "retry_not_before": None,
                "circuit_open_until": None,
                "last_attempt_at": "2026-07-31T01:26:05",
                "inflight_request_id": None,
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(WatchlistQuoteStateError, match="state is incomplete"):
        gateway.capture_once(
            codes=("600000.SH",),
            scheduled_at=NOW,
            universe_as_of=NOW,
            trade_date=NOW.date(),
        )


def test_failed_state_is_durable_before_spool_publish_crash(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    failing = _gateway(
        tmp_path,
        lambda _codes, *, timeout_seconds: (_ for _ in ()).throw(TimeoutError("stuck")),
        completion_at=NOW + timedelta(seconds=1),
        failure_threshold=1,
        circuit_cooldown_seconds=30,
    )
    monkeypatch.setattr(
        failing.spool,
        "publish",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("crash after state")),
    )

    with pytest.raises(RuntimeError, match="crash after state"):
        failing.capture_once(
            codes=("600000.SH",),
            scheduled_at=NOW,
            universe_as_of=NOW,
            trade_date=NOW.date(),
        )

    provider_calls = 0

    def must_remain_open(_codes: tuple[str, ...], *, timeout_seconds: float) -> pd.DataFrame:
        nonlocal provider_calls
        provider_calls += 1
        return _quotes()

    restarted = _gateway(
        tmp_path,
        must_remain_open,
        completion_at=NOW + timedelta(seconds=6),
        failure_threshold=1,
        circuit_cooldown_seconds=30,
    )
    restarted.capture_once(
        codes=("600000.SH",),
        scheduled_at=NOW + timedelta(seconds=5),
        universe_as_of=NOW,
        trade_date=NOW.date(),
    )

    record = restarted.spool.list_after(LiveChannel.WATCHLIST_QUOTE, sequence=-1)[0]
    assert provider_calls == 0
    assert record.envelope.degraded_reasons == ("circuit_open",)


def test_gateway_persists_inflight_and_irrevocably_consumes_quota_before_provider(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "spool" / "watchlist-quote-state.json"
    quota_path = tmp_path / "quota.sqlite3"

    def provider(_codes: tuple[str, ...], *, timeout_seconds: float) -> pd.DataFrame:
        assert timeout_seconds == 2.5
        state = json.loads(state_path.read_text(encoding="utf-8"))
        assert state["admitted_at"] == NOW.isoformat().replace("+00:00", "Z")
        assert state["last_dispatch_at"] == NOW.isoformat().replace("+00:00", "Z")
        assert state["inflight_request_id"]
        assert (
            SourceQuotaStore(quota_path).remaining(
                "akshare.stock_zh_a_spot",
                now=NOW,
            )
            == 0
        )
        return _quotes()

    clock_values = iter((NOW, NOW, NOW, NOW + timedelta(seconds=1)))
    gateway = _gateway(
        tmp_path,
        provider,
        clock=lambda: next(clock_values),
        quota_units_per_window=1,
    )

    gateway.capture_once(
        codes=("600000.SH",),
        scheduled_at=NOW,
        universe_as_of=NOW,
        trade_date=NOW.date(),
    )

    final_state = json.loads(state_path.read_text(encoding="utf-8"))
    assert final_state["admitted_at"] == NOW.isoformat().replace("+00:00", "Z")
    assert final_state["last_dispatch_at"] == NOW.isoformat().replace("+00:00", "Z")
    assert final_state["inflight_request_id"] is None


def test_slow_durable_admission_and_quota_preserve_true_provider_boundary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.watchlist_quote_gateway import decode_watchlist_quote_payload

    provider_started_at: list[datetime] = []

    def provider(_codes: tuple[str, ...], *, timeout_seconds: float) -> pd.DataFrame:
        del timeout_seconds
        started_at = datetime.now(UTC)
        provider_started_at.append(started_at)
        return _quotes(source_observed_at=started_at)

    scheduled_at = datetime.now(UTC)
    gateway = _gateway(
        tmp_path,
        provider,
        clock=lambda: datetime.now(UTC),
        quota_units_per_window=1,
    )
    original_write_state = gateway._write_state
    delayed_admission = False

    def slow_write_state(state: object) -> None:
        nonlocal delayed_admission
        if (
            not delayed_admission
            and getattr(state, "inflight_request_id", None) is not None
            and getattr(state, "last_dispatch_at", None) is None
        ):
            delayed_admission = True
            time.sleep(1.2)
        original_write_state(state)  # type: ignore[arg-type]

    original_consume_quota = gateway._consume_quota_before_dispatch

    def slow_consume_quota(**kwargs: object) -> None:
        time.sleep(1.2)
        original_consume_quota(**kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(gateway, "_write_state", slow_write_state)
    monkeypatch.setattr(gateway, "_consume_quota_before_dispatch", slow_consume_quota)

    gateway.capture_once(
        codes=("600000.SH",),
        scheduled_at=scheduled_at,
        universe_as_of=scheduled_at,
        trade_date=scheduled_at.astimezone(SHANGHAI).date(),
    )

    record = gateway.spool.list_after(LiveChannel.WATCHLIST_QUOTE, sequence=-1)[0]
    restored = decode_watchlist_quote_payload(gateway.spool.read_payload(record))
    requested_at = restored.loc[0, "requested_at"].to_pydatetime()
    state = json.loads(
        (tmp_path / "spool" / "watchlist-quote-state.json").read_text(encoding="utf-8")
    )
    admitted_at = datetime.fromisoformat(state["admitted_at"].replace("Z", "+00:00"))

    assert requested_at >= admitted_at
    assert (requested_at - admitted_at).total_seconds() >= 2.3
    assert 0 <= (provider_started_at[0] - requested_at).total_seconds() < 0.1
    assert datetime.fromisoformat(state["last_dispatch_at"].replace("Z", "+00:00")) == requested_at


def test_spawn_provider_cadence_uses_actual_loader_start_after_delayed_ready(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.watchlist_quote_gateway import decode_watchlist_quote_payload

    loader_starts_path = tmp_path / "spawn-loader-starts"
    monkeypatch.setenv(
        "RQUANT_WATCHLIST_QUOTE_LOADER_STARTS",
        str(loader_starts_path),
    )
    provider = AkshareSinaWatchlistQuoteProvider(
        snapshot_loader=_record_spawn_loader_start,
        process_start_method="spawn",
        termination_grace_seconds=0.1,
    )
    receive = provider._receive
    receive_count = 0

    def delay_first_ready(*args: object, **kwargs: object) -> tuple[object, ...]:
        nonlocal receive_count
        receive_count += 1
        if receive_count == 1:
            receiver = args[0]
            assert receiver.poll(5)  # type: ignore[union-attr]
            time.sleep(1.2)
        return receive(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(provider, "_receive", delay_first_ready)
    gateway = WatchlistQuoteGateway(
        spool=LiveBatchSpool(tmp_path / "spawn-spool"),
        provider=provider,
        config=WatchlistQuoteGatewayConfig(
            producer_version="watchlist-quote-source-v1",
            producer_commit=COMMIT,
            request_timeout_seconds=10,
            quota_units_per_window=10,
        ),
        quota_store=SourceQuotaStore(tmp_path / "spawn-quota.sqlite3"),
        clock=lambda: datetime.now(UTC),
    )

    first_call_started = time.monotonic()
    first_scheduled = datetime.now(UTC)
    gateway.capture_once(
        codes=("600000.SH",),
        scheduled_at=first_scheduled,
        universe_as_of=first_scheduled,
        trade_date=first_scheduled.astimezone(SHANGHAI).date(),
    )
    first_record = gateway.spool.list_after(
        LiveChannel.WATCHLIST_QUOTE,
        sequence=-1,
    )[0]
    first_payload = decode_watchlist_quote_payload(gateway.spool.read_payload(first_record))
    first_loader = json.loads(loader_starts_path.read_text(encoding="utf-8").splitlines()[0])
    first_loader_wall = datetime.fromisoformat(first_loader["started_at"])
    first_requested_at = first_payload.loc[0, "requested_at"].to_pydatetime()
    first_state = json.loads(
        (tmp_path / "spawn-spool" / "watchlist-quote-state.json").read_text(encoding="utf-8")
    )

    assert abs((first_requested_at - first_loader_wall).total_seconds()) < 0.1
    assert (
        datetime.fromisoformat(first_state["last_dispatch_at"].replace("Z", "+00:00"))
        == first_requested_at
    )

    time.sleep(max(0, first_call_started + 5.05 - time.monotonic()))
    early_scheduled = datetime.now(UTC)
    gateway.capture_once(
        codes=("600000.SH",),
        scheduled_at=early_scheduled,
        universe_as_of=early_scheduled,
        trade_date=early_scheduled.astimezone(SHANGHAI).date(),
    )
    early_record = gateway.spool.list_after(
        LiveChannel.WATCHLIST_QUOTE,
        sequence=0,
    )[0]
    assert early_record.envelope.degraded_reasons == ("cadence_active",)
    assert len(loader_starts_path.read_text(encoding="utf-8").splitlines()) == 1

    first_loader_monotonic = int(first_loader["monotonic_ns"]) / 1_000_000_000
    time.sleep(max(0, first_loader_monotonic + 5.1 - time.monotonic()))
    second_scheduled = datetime.now(UTC)
    gateway.capture_once(
        codes=("600000.SH",),
        scheduled_at=second_scheduled,
        universe_as_of=second_scheduled,
        trade_date=second_scheduled.astimezone(SHANGHAI).date(),
    )

    loader_starts = [
        json.loads(line) for line in loader_starts_path.read_text(encoding="utf-8").splitlines()
    ]
    assert len(loader_starts) == 2
    actual_gap = (
        int(loader_starts[1]["monotonic_ns"]) - int(loader_starts[0]["monotonic_ns"])
    ) / 1_000_000_000
    assert actual_gap >= 5.0


def _hard_crash_after_gateway_admission(root: str) -> None:
    gateway = _gateway(
        Path(root),
        lambda _codes, *, timeout_seconds: _quotes(),
        clock=lambda: NOW,
        quota_units_per_window=1,
    )
    write_state = gateway._write_state

    def crash_before_started_state_is_durable(state: object) -> None:
        if getattr(state, "last_dispatch_at", None) is not None:
            os.kill(os.getpid(), signal.SIGKILL)
            raise AssertionError("SIGKILL must terminate the gateway process")
        write_state(state)  # type: ignore[arg-type]

    gateway._write_state = crash_before_started_state_is_durable  # type: ignore[method-assign]
    gateway.capture_once(
        codes=("600000.SH",),
        scheduled_at=NOW,
        universe_as_of=NOW,
        trade_date=NOW.date(),
    )


def test_sigkill_restart_keeps_quota_spent_and_recovers_unknown_inflight_as_failure(
    tmp_path: Path,
) -> None:
    context = multiprocessing.get_context("spawn")
    process = context.Process(
        target=_hard_crash_after_gateway_admission,
        args=(str(tmp_path),),
    )
    process.start()
    process.join(timeout=10)
    if process.is_alive():
        process.kill()
        process.join(timeout=2)
    assert process.exitcode == -signal.SIGKILL
    process.close()

    state_path = tmp_path / "spool" / "watchlist-quote-state.json"
    crashed_state = json.loads(state_path.read_text(encoding="utf-8"))
    assert crashed_state["admitted_at"] == NOW.isoformat().replace("+00:00", "Z")
    assert crashed_state["last_dispatch_at"] is None
    assert crashed_state["inflight_request_id"]
    assert (
        SourceQuotaStore(tmp_path / "quota.sqlite3").remaining(
            "akshare.stock_zh_a_spot",
            now=NOW + timedelta(seconds=1),
        )
        == 0
    )

    provider_calls = 0

    def provider(_codes: tuple[str, ...], *, timeout_seconds: float) -> pd.DataFrame:
        nonlocal provider_calls
        provider_calls += 1
        return _quotes()

    restarted = _gateway(
        tmp_path,
        provider,
        clock=lambda: NOW + timedelta(seconds=1),
        quota_units_per_window=1,
    )
    restarted.capture_once(
        codes=("600000.SH",),
        scheduled_at=NOW + timedelta(seconds=1),
        universe_as_of=NOW,
        trade_date=NOW.date(),
    )

    record = restarted.spool.list_after(LiveChannel.WATCHLIST_QUOTE, sequence=-1)[0]
    assert provider_calls == 0
    assert record.envelope.degraded_reasons == ("backoff_active",)
    recovered_state = json.loads(state_path.read_text(encoding="utf-8"))
    assert recovered_state["consecutive_failures"] == 1
    assert recovered_state["last_dispatch_at"] == (NOW + timedelta(seconds=1)).isoformat().replace(
        "+00:00", "Z"
    )
    assert recovered_state["inflight_request_id"] is None


def _different_request_capture(
    root: str,
    start: object,
    ready: object,
    sender: object,
    offset_milliseconds: int,
) -> None:
    call_path = Path(root) / "cadence-provider-calls"
    requested_at = NOW + timedelta(milliseconds=offset_milliseconds)

    def provider(_codes: tuple[str, ...], *, timeout_seconds: float) -> pd.DataFrame:
        del timeout_seconds
        descriptor = os.open(call_path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        try:
            os.write(descriptor, f"{requested_at.isoformat()}\n".encode())
        finally:
            os.close(descriptor)
        return _quotes(source_observed_at=requested_at)

    gateway = _gateway(Path(root), provider, clock=lambda: requested_at)
    ready.set()  # type: ignore[union-attr]
    start.wait()  # type: ignore[union-attr]
    try:
        capture = gateway.capture_once(
            codes=("600000.SH",),
            scheduled_at=requested_at,
            universe_as_of=NOW,
            trade_date=NOW.date(),
        )
        record = gateway.spool.list_after(
            LiveChannel.WATCHLIST_QUOTE,
            sequence=capture.pointer.sequence - 1,
        )[0]
        sender.send(  # type: ignore[union-attr]
            ("ok", capture.pointer.sequence, record.envelope.degraded_reasons)
        )
    except BaseException as exc:
        sender.send(("error", type(exc).__name__, str(exc)))  # type: ignore[union-attr]
    finally:
        sender.close()  # type: ignore[union-attr]


def test_persistent_cadence_blocks_different_process_request_after_35ms(
    tmp_path: Path,
) -> None:
    context = multiprocessing.get_context("spawn")
    starts = (context.Event(), context.Event())
    ready = (context.Event(), context.Event())
    receivers = []
    processes = []
    for index, offset in enumerate((0, 35)):
        receiver, sender = context.Pipe(duplex=False)
        process = context.Process(
            target=_different_request_capture,
            args=(str(tmp_path), starts[index], ready[index], sender, offset),
        )
        process.start()
        sender.close()
        receivers.append(receiver)
        processes.append(process)
    assert all(event.wait(5) for event in ready)

    starts[0].set()
    assert receivers[0].recv() == ("ok", 0, ())
    time.sleep(0.035)
    started = time.monotonic()
    starts[1].set()
    assert receivers[1].recv() == ("ok", 1, ("cadence_active",))
    assert time.monotonic() - started < 1

    for receiver in receivers:
        receiver.close()
    for process in processes:
        process.join(timeout=5)
        assert process.exitcode == 0
        process.close()
    calls = (tmp_path / "cadence-provider-calls").read_text(encoding="utf-8").splitlines()
    assert calls == [NOW.isoformat()]


def _real_boundary_capture(
    root: str,
    start: object,
    ready: object,
    sender: object,
    slow_admission: bool,
) -> None:
    call_path = Path(root) / "real-provider-starts"

    def provider(_codes: tuple[str, ...], *, timeout_seconds: float) -> pd.DataFrame:
        del timeout_seconds
        started_at = datetime.now(UTC)
        line = json.dumps(
            {
                "pid": os.getpid(),
                "monotonic_ns": time.monotonic_ns(),
                "started_at": started_at.isoformat(),
            },
            separators=(",", ":"),
        )
        descriptor = os.open(call_path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        try:
            os.write(descriptor, f"{line}\n".encode())
        finally:
            os.close(descriptor)
        return _quotes(source_observed_at=started_at)

    gateway = _gateway(
        Path(root),
        provider,
        clock=lambda: datetime.now(UTC),
        quota_units_per_window=10,
    )
    if slow_admission:
        original_write_state = gateway._write_state
        delayed_admission = False

        def slow_write_state(state: object) -> None:
            nonlocal delayed_admission
            if (
                not delayed_admission
                and getattr(state, "inflight_request_id", None) is not None
                and getattr(state, "last_dispatch_at", None) is None
            ):
                delayed_admission = True
                time.sleep(1.2)
            original_write_state(state)  # type: ignore[arg-type]

        original_consume_quota = gateway._consume_quota_before_dispatch

        def slow_consume_quota(**kwargs: object) -> None:
            time.sleep(1.2)
            original_consume_quota(**kwargs)  # type: ignore[arg-type]

        gateway._write_state = slow_write_state  # type: ignore[method-assign]
        gateway._consume_quota_before_dispatch = (  # type: ignore[method-assign]
            slow_consume_quota
        )

    ready.set()  # type: ignore[union-attr]
    start.wait()  # type: ignore[union-attr]
    scheduled_at = datetime.now(UTC)
    try:
        capture = gateway.capture_once(
            codes=("600000.SH",),
            scheduled_at=scheduled_at,
            universe_as_of=scheduled_at,
            trade_date=scheduled_at.astimezone(SHANGHAI).date(),
        )
        record = gateway.spool.list_after(
            LiveChannel.WATCHLIST_QUOTE,
            sequence=capture.pointer.sequence - 1,
        )[0]
        sender.send(  # type: ignore[union-attr]
            ("ok", capture.pointer.sequence, record.envelope.degraded_reasons)
        )
    except BaseException as exc:
        sender.send(("error", type(exc).__name__, str(exc)))  # type: ignore[union-attr]
    finally:
        sender.close()  # type: ignore[union-attr]


def test_multiprocess_cadence_measures_actual_provider_start_after_slow_admission(
    tmp_path: Path,
) -> None:
    SourceQuotaStore(tmp_path / "quota.sqlite3")
    context = multiprocessing.get_context("spawn")
    starts = tuple(context.Event() for _ in range(3))
    ready = tuple(context.Event() for _ in range(3))
    receivers = []
    processes = []
    for index in range(3):
        receiver, sender = context.Pipe(duplex=False)
        process = context.Process(
            target=_real_boundary_capture,
            args=(str(tmp_path), starts[index], ready[index], sender, index == 0),
        )
        process.start()
        sender.close()
        receivers.append(receiver)
        processes.append(process)

    call_path = tmp_path / "real-provider-starts"
    try:
        assert all(event.wait(10) for event in ready)
        starts[0].set()
        assert receivers[0].poll(10)
        assert receivers[0].recv() == ("ok", 0, ())
        first = json.loads(call_path.read_text(encoding="utf-8").splitlines()[0])
        first_started = int(first["monotonic_ns"]) / 1_000_000_000

        time.sleep(max(0, first_started + 3.0 - time.monotonic()))
        starts[1].set()
        assert receivers[1].poll(5)
        assert receivers[1].recv() == ("ok", 1, ("cadence_active",))
        assert len(call_path.read_text(encoding="utf-8").splitlines()) == 1

        time.sleep(max(0, first_started + 5.1 - time.monotonic()))
        starts[2].set()
        assert receivers[2].poll(5)
        assert receivers[2].recv() == ("ok", 2, ())
    finally:
        for event in starts:
            event.set()
        for receiver in receivers:
            receiver.close()
        for process in processes:
            process.join(timeout=10)
            if process.is_alive():
                process.kill()
                process.join(timeout=2)

    assert all(process.exitcode == 0 for process in processes)
    entries = [json.loads(line) for line in call_path.read_text(encoding="utf-8").splitlines()]
    assert len(entries) == 2
    assert entries[0]["pid"] != entries[1]["pid"]
    actual_gap = (int(entries[1]["monotonic_ns"]) - int(entries[0]["monotonic_ns"])) / 1_000_000_000
    assert actual_gap >= 5.0
    for process in processes:
        process.close()


def _multi_instance_capture(root: str, start: object, sender: object) -> None:
    call_path = Path(root) / "provider-calls"

    def provider(_codes: tuple[str, ...], *, timeout_seconds: float) -> pd.DataFrame:
        descriptor = os.open(call_path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        try:
            os.write(descriptor, f"{os.getpid()}\n".encode())
        finally:
            os.close(descriptor)
        time.sleep(0.15)
        return _quotes()

    start.wait()  # type: ignore[union-attr]
    gateway = _gateway(
        Path(root),
        provider,
        completion_at=NOW + timedelta(seconds=1),
    )
    try:
        result = gateway.capture_once(
            codes=("600000.SH",),
            scheduled_at=NOW,
            universe_as_of=NOW,
            trade_date=NOW.date(),
        )
        sender.send(("ok", result.published, result.pointer.sequence))  # type: ignore[union-attr]
    except BaseException as exc:
        sender.send(("error", type(exc).__name__, str(exc)))  # type: ignore[union-attr]
    finally:
        sender.close()  # type: ignore[union-attr]


def test_capture_lock_serializes_multiple_gateway_processes(tmp_path: Path) -> None:
    context = multiprocessing.get_context("spawn")
    start = context.Event()
    receivers = []
    processes = []
    for _ in range(2):
        receiver, sender = context.Pipe(duplex=False)
        process = context.Process(
            target=_multi_instance_capture,
            args=(str(tmp_path), start, sender),
        )
        process.start()
        sender.close()
        receivers.append(receiver)
        processes.append(process)
    start.set()

    results = [receiver.recv() for receiver in receivers]
    for receiver in receivers:
        receiver.close()
    for process in processes:
        process.join(timeout=5)
        assert process.exitcode == 0
        process.close()

    assert sorted(results) == [("ok", False, 0), ("ok", True, 0)]
    calls = (tmp_path / "provider-calls").read_text(encoding="utf-8").splitlines()
    assert len(calls) == 1
    spool = LiveBatchSpool(tmp_path / "spool")
    assert len(spool.list_after(LiveChannel.WATCHLIST_QUOTE, sequence=-1)) == 1
