"""#295: one failed reference-slow capture must not lose the trading day.

On the host, 2026-09-14/18/21/22/23 each lost the whole day the same way: the 09:20 capture
failed once, and every later round in the 09:20-09:25 window was refused with
`SourceQuotaConflictError: reference source attempt already exists: <outcome>` before it
called anything. The attempt identity was (source, target session, the manifest's constant
`retry_ordinal`), so the ledger held exactly one attempt per session: no reference batch, no
reference generation, auction_gap refused, no candidate and no signal all day.

Every case here drives the real builder (`reference_slow_source_builder`), the real capture
(`capture_reference_slow_source_snapshot` over a real DuckDB replica), the real quota ledger
in the production accounting mode (`transport`: one ledger attempt per Tushare call), real
`RuntimeServiceControl` heartbeats and the real publisher. Only Tushare is scripted.
"""

from __future__ import annotations

import os
import sqlite3
from collections.abc import Callable
from contextlib import closing
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
import pytest

import rquant.runtime_service_builtin as runtime_service_builtin
from rquant.auction_gap_candidate_input import assemble_auction_gap_candidate_batch
from rquant.auction_match_gateway import AuctionMatchGateway, AuctionMatchGatewayConfig
from rquant.live_contracts import LiveChannel
from rquant.live_spool import LiveBatchSpool
from rquant.reference_data_registry import ReadonlyReferenceRegistry
from rquant.reference_slow_publisher import ReferenceSlowSourceSnapshot
from rquant.runtime_contracts import canonical_sha256
from rquant.runtime_service_builtin import build_builtin_registry
from rquant.runtime_service_control import (
    RuntimeServiceControl,
    RuntimeServiceHeartbeat,
    RuntimeServicePlane,
)
from rquant.runtime_service_entrypoint import RuntimeServiceKind, RuntimeServiceManifest
from rquant.runtime_serving_authority import (
    ServingSourceAuthorityReader,
    ServingSourceAuthorityUnavailableError,
)
from rquant.runtime_serving_snapshot import REFERENCE_SLOW_AUTHORITY_DATASET_ID
from rquant.source_quota_store import SourceQuotaConflictError, SourceQuotaStore
from rquant.strict_json import canonical_json_bytes
from tests.unit.test_reference_slow_publish_window import (
    NEXT_DATE,
    OPEN_DATES,
    SHANGHAI,
    _calendar,
    _Clock,
    _control,
    _snapshot,
    at,
)
from tests.unit.test_reference_slow_runtime import (
    COMMIT,
    TARGET_DATE,
    _reference_publication_credential,  # noqa: F401 - autouse credential fixture
    _runtime_capabilities,
)

CODE = "300001.SZ"
SOURCE = "tushare.reference_slow"
SOURCE_ID = "reference-slow.source.v1"
PUBLISHER_ID = "reference-slow.publisher.v1"

#: How attempt k of a session goes wrong. Each is a way the host's 09:20 capture has failed
#: or is expected to fail on a bad morning:
#:
#: * `adj_factor_not_published` -- Tushare documents the day's factors at 09:15-09:20; asked
#:   too early `adj_factor(trade_date)` answers an empty frame, which the source reads as
#:   "adj_factor source is missing columns". Every call returned, so in `transport` mode the
#:   ledger says `success` for the whole attempt.
#: * `stock_basic_without_delist_date` -- 2026-09-23 (#293): the calls returned, validation
#:   refused. Also `success` in the ledger.
#: * `tushare_error` -- one call raised (quota, network, token): that call is `failure`.
#: * `timeout` -- the last call timed out: `failure`.
FAULTS = (
    "adj_factor_not_published",
    "stock_basic_without_delist_date",
    "tushare_error",
    "timeout",
)


class _ScriptedTushare:
    """The capture's six Tushare calls, each through the quota observer bound to it.

    Like `TushareAdapter`, every call goes through `observer.observe`, so each one is one
    ledger attempt charged and dispatched before the call and committed after it. An attempt
    is counted at `stock_st`, the first call of every capture; `fault(target, k)` says how
    attempt `k` for that target session goes wrong, `None` for a clean one.
    """

    def __init__(self, fault: Callable[[date, int], str | None]) -> None:
        self._fault = fault
        self._observer: Any = None
        self._current: str | None = None
        self.attempts: dict[date, int] = {}
        self.calls: list[str] = []

    def bind_transport_observer(self, observer: object) -> None:
        self._observer = observer

    def _call(self, api_name: str, produce: Callable[[], pd.DataFrame]) -> pd.DataFrame:
        self.calls.append(api_name)
        if self._observer is None:
            return produce()
        return self._observer.observe(api_name, produce)

    def stock_st_raw(self, trade_date: date) -> pd.DataFrame:
        attempt = self.attempts.get(trade_date, 0)
        self.attempts[trade_date] = attempt + 1
        self._current = self._fault(trade_date, attempt)
        return self._call(
            "stock_st",
            lambda: pd.DataFrame(columns=["ts_code", "trade_date"]),
        )

    def stock_basic(self, list_status: str = "L") -> pd.DataFrame:
        columns = ["ts_code", "name", "list_date", "delist_date", "market"]
        if self._current == "stock_basic_without_delist_date":
            columns.remove("delist_date")
        rows = (
            [
                {
                    "ts_code": CODE,
                    "name": "成长样本",
                    "list_date": "20200102",
                    "delist_date": None,
                    "market": "创业板",
                }
            ]
            if list_status == "L"
            else []
        )
        return self._call(
            "stock_basic",
            lambda: pd.DataFrame(rows, columns=columns)[columns],
        )

    def adj_factor_by_date(self, trade_date: date) -> pd.DataFrame:
        def produce() -> pd.DataFrame:
            if self._current == "tushare_error":
                raise RuntimeError("Tushare adj_factor 调用失败：抱歉，您每分钟最多访问该接口")
            if self._current == "adj_factor_not_published":
                return pd.DataFrame()
            return pd.DataFrame([{"ts_code": CODE, "trade_date": trade_date, "adj_factor": 1.0}])

        return self._call("adj_factor", produce)

    def suspend_d_raw(self, trade_date: date) -> pd.DataFrame:
        def produce() -> pd.DataFrame:
            if self._current == "timeout":
                raise TimeoutError("Tushare suspend_d read timed out")
            return pd.DataFrame(columns=["ts_code", "trade_date", "suspend_timing", "suspend_type"])

        return self._call("suspend_d", produce)


def _replica(path: Path, *, days: tuple[date, ...]) -> Path:
    """The read-only replica: prior sessions' `daily_bar` (close and volume) and factors."""

    with duckdb.connect(str(path)) as connection:
        connection.execute(
            "CREATE TABLE daily_bar(ts_code VARCHAR, trade_date DATE, close DOUBLE, vol DOUBLE)"
        )
        connection.execute(
            "CREATE TABLE adj_factor(ts_code VARCHAR, trade_date DATE, adj_factor DOUBLE)"
        )
        connection.executemany(
            "INSERT INTO daily_bar VALUES (?, ?, 10.0, 1000.0)", [(CODE, day) for day in days]
        )
        connection.executemany(
            "INSERT INTO adj_factor VALUES (?, ?, 1.0)", [(CODE, day) for day in days]
        )
    path.chmod(0o600)
    replica_time = at(8, 0).timestamp()
    os.utime(path, (replica_time, replica_time))
    return path


class _Morning:
    """The source and the publisher, each under its own real `RuntimeServiceControl`."""

    def __init__(
        self,
        tmp_path: Path,
        adapter: _ScriptedTushare,
        *,
        accounting: str = "transport",
        revision_lookback_sessions: int = 5,
        replica_days: tuple[date, ...] = OPEN_DATES[:5],
    ) -> None:
        self.tmp_path = tmp_path
        self.adapter = adapter
        self.clock = _Clock(at(9, 19))
        calendar = _calendar()
        calendar_path = (tmp_path / "calendar.json").resolve()
        calendar_path.write_bytes(canonical_json_bytes(calendar.model_dump(mode="json")))
        calendar_path.chmod(0o600)
        self.replica = _replica((tmp_path / "rquant_ro.duckdb").resolve(), days=replica_days)
        self.spool_root = (tmp_path / "live" / "reference-slow").resolve()
        self.quota_path = self.spool_root / "quota.sqlite3"
        self.registry_path = (
            tmp_path / "authorities" / "reference-slow" / "reference.sqlite3"
        ).resolve()
        quota: dict[str, object] = (
            {"quota_accounting_mode": "transport", "quota_cost_per_capture": None}
            if accounting == "transport"
            else {"quota_accounting_mode": "request", "quota_cost_per_capture": 6}
        )
        #: the production profile's settings (`runtime_production_profile.py`), paths aside
        self.source_manifest = RuntimeServiceManifest(
            service_id=SOURCE_ID,
            service_kind=RuntimeServiceKind.REFERENCE_SLOW_SOURCE,
            plane=RuntimeServicePlane.LIVE,
            interval_seconds=30,
            stale_after_seconds=180,
            producer_commit=COMMIT,
            settings={
                "database_path": str(self.replica),
                "calendar_path": str(calendar_path),
                "calendar_expected_commit": calendar.producer_commit,
                "calendar_content_sha256": calendar.content_sha256,
                "spool_root": str(self.spool_root),
                "quota_path": str(self.quota_path),
                "quota_units_per_window": 500,
                **quota,
                "retry_ordinal": 0,
                "pending_recovery_min_age_seconds": 60,
                "revision_lookback_sessions": revision_lookback_sessions,
                "producer_version": "reference-slow-source-v1",
            },
        )
        publisher_manifest = RuntimeServiceManifest(
            service_id=PUBLISHER_ID,
            service_kind=RuntimeServiceKind.REFERENCE_SLOW_PUBLISHER,
            plane=RuntimeServicePlane.LIVE,
            interval_seconds=5,
            stale_after_seconds=180,
            producer_commit=COMMIT,
            settings={
                "calendar_path": str(calendar_path),
                "calendar_expected_commit": calendar.producer_commit,
                "calendar_content_sha256": calendar.content_sha256,
                "spool_root": str(self.spool_root),
                "registry_path": str(self.registry_path),
                "cursor_root": str(
                    (tmp_path / "control" / "reference-slow-publishers" / "cursors").resolve()
                ),
                "consumer_id": "reference-slow-publisher",
            },
        )
        self.runtime = build_builtin_registry(
            reference_adapter_factory=lambda: adapter,  # type: ignore[arg-type,return-value]
            adapter_factory=lambda: object(),  # type: ignore[arg-type]
            universe_loader=lambda: (CODE,),
            clock=self.clock,
            runtime_capabilities=_runtime_capabilities(),
        )
        self.source_step = self.runtime.build(self.source_manifest)
        self.publisher_step = self.runtime.build(publisher_manifest)
        self.source_control = _control(tmp_path / "control", SOURCE_ID, self.clock)
        self.publisher_control = _control(tmp_path / "control", PUBLISHER_ID, self.clock)
        self.source_control.start()
        self.publisher_control.start()
        #: what the last failed source round wrote to its heartbeat
        self.source_failure: RuntimeServiceHeartbeat | None = None

    def restart_source(self) -> None:
        """A new process: a fresh step over the same ledger and spool, as systemd would."""

        self.source_step = self.runtime.build(self.source_manifest)

    def _round(
        self,
        step: Callable[[], Any],
        control: RuntimeServiceControl,
        moment: datetime,
    ) -> RuntimeServiceHeartbeat | Exception:
        """One loop iteration, recorded the way `run_service_loop` records it.

        `record_success` raising `... sequence cannot regress` is not caught: that is the
        #298 exit, and it fails the test.
        """

        self.clock.now = moment
        try:
            result = step()
        except Exception as error:  # noqa: BLE001 - the loop records every failure
            heartbeat = control.record_failure(error)
            if control is self.source_control:
                self.source_failure = heartbeat
            return error
        return control.record_success(result)

    def source(self, moment: datetime) -> RuntimeServiceHeartbeat | Exception:
        return self._round(self.source_step, self.source_control, moment)

    def publisher(self, moment: datetime) -> RuntimeServiceHeartbeat | Exception:
        return self._round(self.publisher_step, self.publisher_control, moment)

    def ledger_request_ids(self) -> list[str]:
        with closing(sqlite3.connect(self.quota_path)) as connection:
            rows = connection.execute(
                "SELECT DISTINCT logical_request_id FROM quota_transport_attempt"
            ).fetchall()
        return sorted(str(row[0]) for row in rows)


def _first_attempt_request_id(target: date, *, logical_revision: int = 0) -> str:
    """What v0.33.23 sent, and what the first attempt of a session must still send."""

    return canonical_sha256(
        {
            "protocol": "reference-source-attempt-v2",
            "source": SOURCE,
            "target_trade_date": target,
            "logical_revision": logical_revision,
        }
    )


def _heartbeat(value: RuntimeServiceHeartbeat | Exception) -> RuntimeServiceHeartbeat:
    assert isinstance(value, RuntimeServiceHeartbeat), f"round failed: {value!r}"
    return value


def _reference_authority(spool_root: Path, as_of: datetime) -> object:
    return ServingSourceAuthorityReader(
        root=spool_root / "serving-authority",
        expected_producer_commit=COMMIT,
        expected_dataset_id=REFERENCE_SLOW_AUTHORITY_DATASET_ID,
        expected_payload_kind="reference_slow",
    )(as_of)


def _auction_gap_at_0929(morning: _Morning) -> object:
    """The first consumer that refused all day without a generation (09-14 .. 09-23)."""

    auction_spool = LiveBatchSpool(morning.tmp_path / "auction-spool")
    frame = pd.DataFrame(
        [
            {
                "ts_code": CODE,
                "trade_date": TARGET_DATE,
                "price": 10.5,
                "vol": 20_000.0,
                "amount": 210_000.0,
                "pre_close": 10.0,
                "turnover_rate": 0.2,
                "volume_ratio": 9.9,
            }
        ]
    )
    capture = AuctionMatchGateway(
        spool=auction_spool,
        fetcher=lambda _trade_date: frame,
        config=AuctionMatchGatewayConfig(
            producer_version="auction-match-v1",
            producer_commit=COMMIT,
            min_coverage_ratio=1.0,
        ),
    ).capture_once(trade_date=TARGET_DATE, received_at=at(9, 29), expected_codes=(CODE,))
    assert capture.published is True
    return assemble_auction_gap_candidate_batch(
        auction_spool=auction_spool,
        daily_database_path=morning.replica,
        reference_registry=ReadonlyReferenceRegistry(morning.registry_path),
        calendar=_calendar(),
        trade_date=TARGET_DATE,
        observed_at=at(9, 29, 30),
        producer_commit=COMMIT,
    )


@pytest.mark.parametrize("accounting", ["transport", "request"])
@pytest.mark.parametrize("fault", FAULTS)
def test_one_failed_capture_is_retried_and_the_session_still_gets_its_reference_generation(
    tmp_path: Path,
    fault: str,
    accounting: str,
) -> None:
    """The 09-14 .. 09-23 morning, with a second chance inside the window.

    Before the fix the 09:20:40 round was refused with `reference source attempt already
    exists: success` (validation failed after every call returned) or `...: failure` (a call
    raised), and so was every round after it.
    """

    adapter = _ScriptedTushare(lambda _target, attempt: fault if attempt == 0 else None)
    morning = _Morning(tmp_path, adapter, accounting=accounting)

    assert _heartbeat(morning.source(at(9, 19))).output_sequence == -1
    first = morning.source(at(9, 20))
    assert isinstance(first, Exception) and not isinstance(first, SourceQuotaConflictError)
    assert morning.source_failure is not None
    assert morning.source_failure.last_error is not None

    retried = _heartbeat(morning.source(at(9, 20, 40)))

    assert adapter.attempts == {TARGET_DATE: 2}
    assert retried.processed_count == 1
    assert (retried.input_sequence, retried.output_sequence) == (0, 0)
    #: the day did get its batch, so the first failure no longer marks the heartbeat
    assert not any(reason.startswith("capture_failed:") for reason in retried.degraded_reasons)
    (record,) = LiveBatchSpool(morning.spool_root).list_after(
        LiveChannel.REFERENCE_SLOW, sequence=-1
    )
    assert record.envelope.available_at == at(9, 21, 10)

    #: a successful attempt is never sent again: the rounds before the 09:24 revision scan
    #: recognise today's batch and call nothing
    calls = len(adapter.calls)
    for moment in (at(9, 21, 10), at(9, 22), at(9, 23, 30)):
        assert _heartbeat(morning.source(moment)).output_sequence == 0
    assert len(adapter.calls) == calls

    published = _heartbeat(morning.publisher(at(9, 21, 20)))
    assert published.processed_count == 1
    with pytest.raises(ServingSourceAuthorityUnavailableError):
        _reference_authority(morning.spool_root, at(9, 24, 59))
    assert _reference_authority(morning.spool_root, at(9, 25)) is not None

    #: after the window: no regress (#298), and still one batch
    assert _heartbeat(morning.source(at(9, 26))).output_sequence == 0
    assert isinstance(morning.publisher(at(9, 26)), Exception)

    batch = _auction_gap_at_0929(morning)
    (fact,) = batch.facts  # type: ignore[attr-defined]
    assert fact.ts_code == CODE
    assert fact.is_listed is True
    assert fact.limit_up_price_session_raw == 12.0


def test_the_first_attempt_of_a_session_still_sends_the_v0_33_23_request(tmp_path: Path) -> None:
    """A good morning is unchanged, and a ledger written by the old code is read the same way."""

    adapter = _ScriptedTushare(lambda _target, _attempt: None)
    morning = _Morning(tmp_path, adapter)

    assert _heartbeat(morning.source(at(9, 20))).processed_count == 1

    assert adapter.attempts == {TARGET_DATE: 1}
    assert morning.ledger_request_ids() == [_first_attempt_request_id(TARGET_DATE)]


def test_a_capture_that_keeps_failing_stops_at_the_daily_cap_and_the_next_session_starts_afresh(
    tmp_path: Path,
) -> None:
    """A defect that fails every attempt (09-23's) costs a bounded number of requests."""

    cap = runtime_service_builtin.REFERENCE_SLOW_MAX_CAPTURE_ATTEMPTS
    adapter = _ScriptedTushare(
        lambda target, _attempt: (
            "stock_basic_without_delist_date" if target == TARGET_DATE else None
        )
    )
    morning = _Morning(tmp_path, adapter, replica_days=OPEN_DATES[:6])

    outcomes = [morning.source(at(9, 20) + timedelta(seconds=30 * k)) for k in range(10)]

    assert adapter.attempts == {TARGET_DATE: cap}
    assert all(isinstance(outcome, Exception) for outcome in outcomes)
    refusals = [str(outcome) for outcome in outcomes[cap:]]
    assert refusals and all(
        f"all {cap} capture attempts for {TARGET_DATE.isoformat()} are used" in refusal
        for refusal in refusals
    ), refusals
    #: the heartbeat names the first failure, not the refusals after it (#293)
    assert _heartbeat(morning.source(at(9, 26))).degraded_reasons == (
        "capture_failed:ReferenceSlowSourceError",
    )

    #: the next session has its own attempts
    next_morning = _heartbeat(morning.source(at(9, 20, day=NEXT_DATE)))
    assert next_morning.processed_count == 1
    assert adapter.attempts == {TARGET_DATE: cap, NEXT_DATE: 1}
    assert next_morning.degraded_reasons == ()


def test_a_batch_write_that_missed_its_guard_is_captured_again_and_sealed_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The capture stood, the spool write did not: the spool rolled it back (#297's guard).

    Retrying is what a successful attempt that never became a batch needs, and it cannot
    publish twice: the spool is where "today's batch exists" is decided, and it has none.
    """

    adapter = _ScriptedTushare(lambda _target, _attempt: None)
    morning = _Morning(tmp_path, adapter)
    real_publish = LiveBatchSpool.publish
    slow = {"left": 1}

    def publish(self: LiveBatchSpool, *args: Any, **kwargs: Any) -> Any:
        if slow["left"]:
            slow["left"] -= 1
            #: the host's throttled slice: the write ran past `prepared_at + 30 s`
            morning.clock.now += timedelta(seconds=31)
        return real_publish(self, *args, **kwargs)

    monkeypatch.setattr(LiveBatchSpool, "publish", publish)

    missed = morning.source(at(9, 20))
    assert isinstance(missed, Exception)
    assert "promised visibility instant" in str(missed)
    assert LiveBatchSpool(morning.spool_root).current(LiveChannel.REFERENCE_SLOW) is None

    sealed = _heartbeat(morning.source(at(9, 21)))

    assert sealed.processed_count == 1
    assert adapter.attempts == {TARGET_DATE: 2}
    (record,) = LiveBatchSpool(morning.spool_root).list_after(
        LiveChannel.REFERENCE_SLOW, sequence=-1
    )
    assert record.envelope.sequence == 0
    assert _heartbeat(morning.source(at(9, 22))).output_sequence == 0
    assert adapter.attempts == {TARGET_DATE: 2}


def test_a_killed_attempt_is_never_sent_again(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Retrying never re-sends a request whose outcome is unknown.

    The process dies inside `adj_factor` (OOM, `SIGKILL` on a stop that ran out of time): the
    call's ledger attempt stays `pending`. Neither the same process nor the restarted one --
    whose ledger turns the stale attempt `unknown` -- may dispatch another capture that day.
    """

    killed = {"done": False}

    class _KilledOnce(_ScriptedTushare):
        def adj_factor_by_date(self, trade_date: date) -> pd.DataFrame:
            if not killed["done"]:
                killed["done"] = True
                return self._call("adj_factor", lambda: (_ for _ in ()).throw(SystemExit(137)))
            return super().adj_factor_by_date(trade_date)

    adapter = _KilledOnce(lambda _target, _attempt: None)
    morning = _Morning(tmp_path, adapter)
    morning.clock.now = at(9, 20)
    with pytest.raises(SystemExit):
        morning.source_step()

    pending = morning.source(at(9, 20, 40))
    assert isinstance(pending, SourceQuotaConflictError)
    assert "already exists: pending" in str(pending)

    #: the restart: another boot, so the stale attempt is recovered as `unknown`
    real_store = SourceQuotaStore
    monkeypatch.setattr(
        runtime_service_builtin,
        "SourceQuotaStore",
        lambda path: real_store(path, boot_id="boot-after-restart"),
    )
    morning.restart_source()
    for moment in (at(9, 21, 20), at(9, 22), at(9, 23)):
        unknown = morning.source(moment)
        assert isinstance(unknown, SourceQuotaConflictError)
        assert "already exists: unknown" in str(unknown)

    assert adapter.attempts == {TARGET_DATE: 1}
    assert LiveBatchSpool(morning.spool_root).current(LiveChannel.REFERENCE_SLOW) is None


def test_an_attempt_missing_from_the_ledger_fails_closed(tmp_path: Path) -> None:
    """The attempts of a session are contiguous; a hole means the ledger was edited."""

    adapter = _ScriptedTushare(lambda _target, _attempt: "tushare_error")
    morning = _Morning(tmp_path, adapter)
    for moment in (at(9, 20), at(9, 20, 40)):
        failed = morning.source(moment)
        assert isinstance(failed, RuntimeError)
        assert not isinstance(failed, SourceQuotaConflictError)
    with closing(sqlite3.connect(morning.quota_path)) as connection:
        deleted = connection.execute(
            "DELETE FROM quota_transport_attempt WHERE logical_request_id = ?",
            (_first_attempt_request_id(TARGET_DATE),),
        ).rowcount
        connection.commit()
    assert deleted > 0

    refused = morning.source(at(9, 21, 20))

    assert isinstance(refused, SourceQuotaConflictError)
    assert "ledger skips an attempt" in str(refused)
    assert adapter.attempts == {TARGET_DATE: 2}


def test_the_next_sessions_revision_scan_of_the_previous_session_is_not_refused(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Same identity defect, second path: revisions of a past session across two days.

    Session 1 scans itself at 09:24 under (session 1, logical revision 1). Session 2's scan of
    session 1 asked for that same identity and was refused `already exists: success` before it
    sent anything -- every session from the second one on (on the host 2026-09-28 would have
    been the first, because 09-24 scanned itself).

    The capture is replaced by one that seals any target session. The real one cannot seal a
    past session yet: `assemble_reference_slow_source_snapshot` takes the session from the
    completion instant, so a past target is refused `daily source must use the exact prior open
    date` -- a separate defect, reported with this fix, that only ever fails the rounds after
    today's batch is sealed. What this case pins is the ledger identity.
    """

    captured: list[tuple[date, date]] = []

    def capture(**kwargs: Any) -> ReferenceSlowSourceSnapshot:
        target: date = kwargs["target_trade_date"]
        captured_at: datetime = kwargs["captured_at"]
        #: one real Tushare call through the bound quota observer, as every capture makes
        kwargs["adapter"].stock_st_raw(target)
        captured.append((captured_at.astimezone(SHANGHAI).date(), target))
        prior = max(day for day in OPEN_DATES if day < target)
        return _snapshot(captured_at=captured_at, target_trade_date=target, prior_trade_date=prior)

    monkeypatch.setattr(
        "rquant.reference_slow_source.capture_reference_slow_source_snapshot",
        capture,
    )
    morning = _Morning(
        tmp_path,
        _ScriptedTushare(lambda _target, _attempt: None),
        revision_lookback_sessions=2,
    )

    for moment in (
        at(9, 20),
        at(9, 24),
        at(9, 20, day=NEXT_DATE),
        at(9, 24, day=NEXT_DATE),
        at(9, 24, 30, day=NEXT_DATE),
    ):
        _heartbeat(morning.source(moment))

    assert captured == [
        (TARGET_DATE, TARGET_DATE),
        (TARGET_DATE, TARGET_DATE),
        (NEXT_DATE, NEXT_DATE),
        (NEXT_DATE, NEXT_DATE),
        (NEXT_DATE, TARGET_DATE),
    ]
    state = LiveBatchSpool(morning.spool_root).load_source_state("reference-revision-scan")
    assert state is not None
    scanned = state.decode("utf-8")
    assert NEXT_DATE.isoformat() in scanned and TARGET_DATE.isoformat() in scanned
