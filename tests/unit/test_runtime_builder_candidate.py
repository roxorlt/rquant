from __future__ import annotations

import json
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
from pydantic import ValidationError

from rquant import runtime_builder_candidate as candidate_module
from rquant.auction_gap_candidate_input import AuctionGapCandidateInputError
from rquant.live_contracts import BatchQualityStatus
from rquant.readside_replica_gate import ReplicaReadGate
from rquant.runtime_builder_candidate import (
    AUCTION_GAP_DEFAULT_INPUT_END,
    AUCTION_GAP_DEFAULT_INPUT_START,
    CandidatePublisherRuntimeSettings,
    candidate_publisher_builder,
    load_candidate_input,
    serialize_candidate_input,
)
from rquant.runtime_market_session import (
    MarketCalendarAuthority,
    MarketSessionCalendarError,
    auction_windows_are_consistent,
)
from rquant.runtime_service_builtin import (
    AUCTION_MATCH_DEFAULT_CAPTURE_END,
    AUCTION_MATCH_DEFAULT_CAPTURE_START,
    AuctionMatchSourceSettings,
)
from rquant.runtime_service_control import RuntimeServicePlane
from rquant.runtime_service_entrypoint import RuntimeServiceKind, RuntimeServiceManifest
from rquant.session_candidate_input import (
    SESSION_CANDIDATE_CONTRACT,
    SessionCandidateInputError,
    candidate_input_batch,
)
from rquant.strategy_candidate_producers import (
    NShapePoolFact,
    PublishedCandidateInputAuthority,
)
from rquant.strategy_candidate_publish_service import (
    AuctionGapCandidateBatch,
    CandidatePublishBatch,
    GrowthBoardCandidateBatch,
    NShapeCandidateBatch,
)
from rquant.strategy_candidate_snapshot import StrategyCandidateSnapshotSpool
from rquant.strategy_evaluators import BuiltinStrategyEvaluatorRegistry
from rquant.strict_json import canonical_json_bytes

TRADE_DATE = date(2026, 7, 31)
REFERENCE_DATE = date(2026, 7, 30)
CAPTURED_AT = datetime(2026, 7, 31, 1, 30, tzinfo=UTC)
AVAILABLE_AT = datetime(2026, 7, 31, 1, 26, tzinfo=UTC)
COMMIT = "a" * 40
SHANGHAI = ZoneInfo("Asia/Shanghai")
REGISTRY = BuiltinStrategyEvaluatorRegistry(producer_commit=COMMIT)


def _exact_strategy_settings(strategy_id: str) -> dict[str, object]:
    definition = REGISTRY.load_definition(strategy_id, 1)
    return {
        "definition_fingerprint": definition.spec.spec_fingerprint,
        "executable_fingerprint": definition.executable_fingerprint,
        "candidate_schema_fingerprint": definition.candidate_schema_fingerprint,
        "static_feature_schema": {
            name: semantic.contract_payload()
            for name, semantic in definition.static_feature_schema.items()
        },
    }


def _authority(
    *,
    authority_snapshot_id: str = "1" * 64,
    producer_commit: str = COMMIT,
) -> PublishedCandidateInputAuthority:
    return PublishedCandidateInputAuthority(
        trade_date=TRADE_DATE,
        captured_at=CAPTURED_AT,
        quality_status=BatchQualityStatus.PUBLISHED,
        authority_snapshot_id=authority_snapshot_id,
        producer_commit=producer_commit,
    )


def _n_shape_fact() -> NShapePoolFact:
    return NShapePoolFact(
        ts_code="300001.SZ",
        variant="pool1",
        reference_trade_date=date(2026, 7, 29),
        t_close_raw=20.0,
        t_high_raw=25.0,
        reference_adj_factor=1.0,
        prior_session_trade_date=REFERENCE_DATE,
        expected_prior_session_trade_date=REFERENCE_DATE,
        prior_session_close_raw=10.0,
        prior_session_adj_factor=2.0,
        available_at=AVAILABLE_AT,
        reference_snapshot_ids={
            "pool": "2" * 64,
            "daily": "3" * 64,
            "adj_factor": "4" * 64,
            "session": "5" * 64,
            "status": "6" * 64,
            "limit": "7" * 64,
            "trade_calendar": "8" * 64,
        },
        session_pre_close_raw=8.0,
        limit_pct=0.2,
        limit_up_price_session_raw=9.6,
        is_st=False,
        is_suspended=False,
        is_listed=True,
        limit_eligible=True,
    )


def _batch(
    strategy_id: str,
    *,
    authority_snapshot_id: str = "1" * 64,
    producer_commit: str = COMMIT,
    with_candidate: bool = False,
) -> CandidatePublishBatch:
    authority = _authority(
        authority_snapshot_id=authority_snapshot_id,
        producer_commit=producer_commit,
    )
    if strategy_id == "n_shape":
        return NShapeCandidateBatch(
            authority=authority,
            facts=(_n_shape_fact(),) if with_candidate else (),
        )
    if strategy_id == "growth_board_surge":
        return GrowthBoardCandidateBatch(authority=authority, facts=())
    if strategy_id == "auction_gap":
        return AuctionGapCandidateBatch(authority=authority, facts=())
    raise AssertionError(strategy_id)


def _write_input(path: Path, batch: CandidatePublishBatch) -> None:
    path.write_bytes(serialize_candidate_input(batch))
    path.chmod(0o600)


def _manifest(
    tmp_path: Path,
    *,
    strategy_id: str = "n_shape",
    kind: RuntimeServiceKind = RuntimeServiceKind.CANDIDATE_PUBLISHER,
    plane: RuntimeServicePlane = RuntimeServicePlane.LIVE,
    candidate_input_path: Path | None = None,
    snapshot_root: Path | None = None,
) -> RuntimeServiceManifest:
    return RuntimeServiceManifest(
        service_id=f"candidate.{strategy_id}.v1",
        service_kind=kind,
        plane=plane,
        interval_seconds=30,
        stale_after_seconds=90,
        producer_commit=COMMIT,
        settings={
            "strategy_id": strategy_id,
            "strategy_version": 1,
            **_exact_strategy_settings(strategy_id),
            "candidate_input_path": str(candidate_input_path or (tmp_path / f"{strategy_id}.json")),
            "snapshot_root": str(snapshot_root or (tmp_path / "live" / strategy_id)),
        },
    )


@pytest.mark.parametrize(
    "strategy_id",
    ("n_shape", "growth_board_surge", "auction_gap"),
)
def test_candidate_publisher_dispatches_all_builtin_strategy_batches(
    tmp_path: Path,
    strategy_id: str,
) -> None:
    input_path = tmp_path / f"{strategy_id}.json"
    root = tmp_path / "live" / strategy_id
    _write_input(input_path, _batch(strategy_id))

    result = candidate_publisher_builder()(
        _manifest(
            tmp_path,
            strategy_id=strategy_id,
            candidate_input_path=input_path,
            snapshot_root=root,
        )
    )()

    snapshot = StrategyCandidateSnapshotSpool(root).read_strategy_as_of(
        CAPTURED_AT,
        strategy_id=strategy_id,
        strategy_version="1",
        **_exact_strategy_settings(strategy_id),
    )
    assert snapshot is not None
    assert result.input_sequence == -1
    assert result.output_sequence == snapshot.sequence == 0
    assert result.processed_count == len(snapshot.rows) == 0
    assert result.backlog_count == 0
    assert result.source_generations == {
        "candidate_input": "1" * 64,
        "strategy_candidate": snapshot.content_sha256,
    }


def test_candidate_publisher_binds_static_strategy_semantics(tmp_path: Path) -> None:
    input_path = tmp_path / "n_shape.json"
    root = tmp_path / "live" / "n_shape"
    identity = _exact_strategy_settings("n_shape")
    definition_fingerprint = str(identity["definition_fingerprint"])
    executable_fingerprint = str(identity["executable_fingerprint"])
    candidate_schema_fingerprint = str(identity["candidate_schema_fingerprint"])
    _write_input(input_path, _batch("n_shape"))
    manifest = _manifest(
        tmp_path,
        candidate_input_path=input_path,
        snapshot_root=root,
    )
    settings = dict(manifest.settings)
    settings.update(
        definition_fingerprint=definition_fingerprint,
        executable_fingerprint=executable_fingerprint,
        candidate_schema_fingerprint=candidate_schema_fingerprint,
        static_feature_schema=identity["static_feature_schema"],
    )

    candidate_publisher_builder()(manifest.model_copy(update={"settings": settings}))()

    snapshot = StrategyCandidateSnapshotSpool(root).read_strategy_as_of(
        CAPTURED_AT,
        strategy_id="n_shape",
        strategy_version="1",
        definition_fingerprint=definition_fingerprint,
        executable_fingerprint=executable_fingerprint,
        candidate_schema_fingerprint=candidate_schema_fingerprint,
        static_feature_schema=identity["static_feature_schema"],
    )
    assert snapshot is not None
    assert snapshot.schema_version == 3
    assert snapshot.authority_binding is not None
    assert snapshot.authority_binding.schema_version == 3
    assert snapshot.authority_binding.definition_fingerprint == definition_fingerprint
    assert snapshot.authority_binding.executable_fingerprint == executable_fingerprint
    assert snapshot.authority_binding.candidate_schema_fingerprint == candidate_schema_fingerprint


def test_an_auction_iteration_outside_its_window_says_it_read_nothing(
    tmp_path: Path,
) -> None:
    """Review MF-1: this publisher acts for four minutes and idles for the rest of the day.

    At its five-second interval that is about a thousand idle iterations, and each one
    must report "opened nothing, read nothing" rather than the window's last real read.
    """

    root = tmp_path / "live" / "auction-gap"
    manifest = RuntimeServiceManifest(
        service_id="candidate.auction-gap.v1",
        service_kind=RuntimeServiceKind.CANDIDATE_PUBLISHER,
        plane=RuntimeServicePlane.LIVE,
        interval_seconds=5,
        stale_after_seconds=60,
        producer_commit=COMMIT,
        settings={
            "strategy_id": "auction_gap",
            "strategy_version": 1,
            **_exact_strategy_settings("auction_gap"),
            "input_mode": "auction_live",
            "auction_spool_root": str(tmp_path / "auction-spool"),
            "daily_database_path": str(tmp_path / "operational-ro.duckdb"),
            "reference_registry_path": str(tmp_path / "reference.sqlite3"),
            "calendar_path": str(tmp_path / "calendar.json"),
            "calendar_expected_commit": COMMIT,
            "calendar_content_sha256": "c" * 64,
            "snapshot_root": str(root),
        },
    )
    #: 18:30 Asia/Shanghai, far outside 09:35-10:10
    step = candidate_publisher_builder(
        auction_input_loader=lambda **_: _batch("auction_gap"),
        clock=lambda: datetime(2026, 7, 31, 10, 30, tzinfo=UTC),
    )(manifest)

    result = step()

    assert result.processed_count == 0
    assert result.replica_opened is False
    assert result.replica_read_bytes == 0


def test_the_publisher_does_not_carry_one_iteration_s_read_into_the_next(
    tmp_path: Path,
) -> None:
    """Review MF-5: the guard for `begin_iteration()` has to drive the **builder**.

    `test_an_auction_iteration_outside_its_window_says_it_read_nothing` never reads
    anything at all, so it passes with or without the call. This one reads for real in the
    09:35-10:10 window and then idles outside it: without `begin_iteration()` the idle
    iteration reports the window's read, which is the whole of MF-1 on this role.

    The loader is a stub that uses the gate it is handed, because what is under test is
    the builder's wiring -- begin the iteration, hand the gate down, summarise it -- and
    not what the real assembler does with the gate (that is covered in
    `test_auction_gap_candidate_input.py`).
    """

    replica = tmp_path / "operational-ro.duckdb"
    replica.write_bytes(b"a replica generation")
    replica.chmod(0o644)
    root = tmp_path / "live" / "auction-gap"
    manifest = RuntimeServiceManifest(
        service_id="candidate.auction-gap.v1",
        service_kind=RuntimeServiceKind.CANDIDATE_PUBLISHER,
        plane=RuntimeServicePlane.LIVE,
        interval_seconds=5,
        stale_after_seconds=60,
        producer_commit=COMMIT,
        settings={
            "strategy_id": "auction_gap",
            "strategy_version": 1,
            **_exact_strategy_settings("auction_gap"),
            "input_mode": "auction_live",
            "auction_spool_root": str(tmp_path / "auction-spool"),
            "daily_database_path": str(replica),
            "reference_registry_path": str(tmp_path / "reference.sqlite3"),
            "calendar_path": str(tmp_path / "calendar.json"),
            "calendar_expected_commit": COMMIT,
            "calendar_content_sha256": "c" * 64,
            "snapshot_root": str(root),
        },
    )

    def reading_loader(*, read_gate: ReplicaReadGate[object], **_: object) -> object:
        read_gate.read(lambda: replica.read_bytes(), key=("auction-gap",))
        return _batch("auction_gap")

    #: 09:40 then 10:15 Asia/Shanghai: inside the window, then outside it
    inside = datetime(2026, 7, 31, 1, 40, tzinfo=UTC)
    clock = {"now": inside}
    step = candidate_publisher_builder(
        auction_input_loader=reading_loader,
        clock=lambda: clock["now"],
    )(manifest)

    published = step()
    clock["now"] = datetime(2026, 7, 31, 2, 15, tzinfo=UTC)
    idled = step()

    assert published.replica_opened is True
    assert idled.processed_count == 0
    assert idled.replica_opened is False
    assert idled.replica_read_bytes == 0


def test_a_torn_read_in_the_auction_window_is_reported_as_the_open_it_was(
    tmp_path: Path,
) -> None:
    """Review SF-7, through the builder: the degraded branch did open the database."""

    replica = tmp_path / "operational-ro.duckdb"
    replica.write_bytes(b"a replica generation")
    replica.chmod(0o644)
    root = tmp_path / "live" / "auction-gap"
    manifest = RuntimeServiceManifest(
        service_id="candidate.auction-gap.v1",
        service_kind=RuntimeServiceKind.CANDIDATE_PUBLISHER,
        plane=RuntimeServicePlane.LIVE,
        interval_seconds=5,
        stale_after_seconds=60,
        producer_commit=COMMIT,
        settings={
            "strategy_id": "auction_gap",
            "strategy_version": 1,
            **_exact_strategy_settings("auction_gap"),
            "input_mode": "auction_live",
            "auction_spool_root": str(tmp_path / "auction-spool"),
            "daily_database_path": str(replica),
            "reference_registry_path": str(tmp_path / "reference.sqlite3"),
            "calendar_path": str(tmp_path / "calendar.json"),
            "calendar_expected_commit": COMMIT,
            "calendar_content_sha256": "c" * 64,
            "snapshot_root": str(root),
        },
    )

    def torn_loader(*, read_gate: ReplicaReadGate[object], **_: object) -> object:
        def read_and_refuse() -> object:
            replica.read_bytes()
            raise AuctionGapCandidateInputError("daily snapshot changed while reading")

        read_gate.read(read_and_refuse, key=("auction-gap",))
        raise AssertionError("unreachable")

    step = candidate_publisher_builder(
        auction_input_loader=torn_loader,
        clock=lambda: datetime(2026, 7, 31, 1, 40, tzinfo=UTC),
    )(manifest)

    degraded = step()

    assert degraded.degraded_reasons == ("auction_gap_input_unavailable",)
    assert degraded.replica_opened is True


def test_a_failing_auction_iteration_still_says_what_it_did_with_the_replica(
    tmp_path: Path,
) -> None:
    """#261: the round that raised reported `null`, and it had opened the replica.

    `AuctionGapCandidateInputError` is the one failure this step turns into a degraded
    return; everything else -- a snapshot the publisher refuses, a spool that will not
    open -- leaves the step as an exception, and those rounds had already read the
    replica. The step hands the loop its gate's own summary so they say so.
    """

    replica = tmp_path / "operational-ro.duckdb"
    replica.write_bytes(b"a replica generation")
    replica.chmod(0o644)
    root = tmp_path / "live" / "auction-gap"
    manifest = RuntimeServiceManifest(
        service_id="candidate.auction-gap.v1",
        service_kind=RuntimeServiceKind.CANDIDATE_PUBLISHER,
        plane=RuntimeServicePlane.LIVE,
        interval_seconds=5,
        stale_after_seconds=60,
        producer_commit=COMMIT,
        settings={
            "strategy_id": "auction_gap",
            "strategy_version": 1,
            **_exact_strategy_settings("auction_gap"),
            "input_mode": "auction_live",
            "auction_spool_root": str(tmp_path / "auction-spool"),
            "daily_database_path": str(replica),
            "reference_registry_path": str(tmp_path / "reference.sqlite3"),
            "calendar_path": str(tmp_path / "calendar.json"),
            "calendar_expected_commit": COMMIT,
            "calendar_content_sha256": "c" * 64,
            "snapshot_root": str(root),
        },
    )

    rounds = {"n": 0}

    def failing_loader(*, read_gate: ReplicaReadGate[object], **_: object) -> object:
        rounds["n"] += 1
        if rounds["n"] == 1:
            read_gate.read(lambda: replica.read_bytes(), key=("auction-gap",))
            raise RuntimeError("auction spool is unreadable")
        raise RuntimeError("auction spool is missing")

    step = candidate_publisher_builder(
        auction_input_loader=failing_loader,
        clock=lambda: datetime(2026, 7, 31, 1, 40, tzinfo=UTC),
    )(manifest)

    summary = getattr(step, "replica_iteration_summary", None)
    assert callable(summary)
    assert summary() == (False, 0)

    with pytest.raises(RuntimeError, match="auction spool is unreadable"):
        step()

    opened, read_bytes = summary()
    assert opened is True
    assert read_bytes is None or read_bytes >= 0

    #: the round after it fails before it reaches the replica at all, and must say so
    #: rather than repeat the numbers of the round that did read it
    with pytest.raises(RuntimeError, match="auction spool is missing"):
        step()

    assert summary() == (False, 0)


def test_a_document_driven_publisher_has_no_replica_to_report_on(tmp_path: Path) -> None:
    """The other two strategies read a sealed document, so they report nothing, not zero."""

    root = tmp_path / "live" / "n-shape"
    path = tmp_path / "n-shape.json"
    path.write_bytes(serialize_candidate_input(_batch("n_shape")))
    path.chmod(0o600)
    manifest = RuntimeServiceManifest(
        service_id="candidate.n_shape.v1",
        service_kind=RuntimeServiceKind.CANDIDATE_PUBLISHER,
        plane=RuntimeServicePlane.LIVE,
        interval_seconds=5,
        stale_after_seconds=60,
        producer_commit=COMMIT,
        settings={
            "strategy_id": "n_shape",
            "strategy_version": 1,
            **_exact_strategy_settings("n_shape"),
            "candidate_input_path": str(path),
            "snapshot_root": str(root),
        },
    )
    step = candidate_publisher_builder(clock=lambda: CAPTURED_AT)(manifest)

    result = step()

    assert result.replica_opened is None
    assert result.replica_read_bytes is None
    #: and a failed round of theirs reports neither too, because the loop finds nothing to
    #: ask -- the same rule the success path applies, on the path that raised (#261)
    assert getattr(step, "replica_iteration_summary", None) is None


def test_the_auction_assembly_window_follows_the_capture_window(tmp_path: Path) -> None:
    """#277：采集窗按 09-23 实测的 T = 09:27:14 收成 09:29-09:44，装配窗必须跟着走，
    否则窗里永远没有料。

    默认起点与 `AUCTION_MATCH_DEFAULT_CAPTURE_START` 逐字相同，终点在
    `AUCTION_MATCH_DEFAULT_CAPTURE_END` 之后——最后一次采集成功之后还装得出来。
    """

    assert AUCTION_GAP_DEFAULT_INPUT_START == AUCTION_MATCH_DEFAULT_CAPTURE_START
    assert AUCTION_GAP_DEFAULT_INPUT_END > AUCTION_MATCH_DEFAULT_CAPTURE_END


def _auction_manifest(tmp_path: Path, **overrides: object) -> RuntimeServiceManifest:
    settings: dict[str, object] = {
        "strategy_id": "auction_gap",
        "strategy_version": 1,
        **_exact_strategy_settings("auction_gap"),
        "input_mode": "auction_live",
        "auction_spool_root": str(tmp_path / "auction-spool"),
        "daily_database_path": str(tmp_path / "operational-ro.duckdb"),
        "reference_registry_path": str(tmp_path / "reference.sqlite3"),
        "calendar_path": str(tmp_path / "calendar.json"),
        "calendar_expected_commit": COMMIT,
        "calendar_content_sha256": "c" * 64,
        "snapshot_root": str(tmp_path / "live" / "auction-gap"),
    }
    settings.update(overrides)
    return RuntimeServiceManifest(
        service_id="candidate.auction-gap.v1",
        service_kind=RuntimeServiceKind.CANDIDATE_PUBLISHER,
        plane=RuntimeServicePlane.LIVE,
        interval_seconds=5,
        stale_after_seconds=60,
        producer_commit=COMMIT,
        settings=settings,
    )


@pytest.mark.parametrize(
    ("hour", "minute", "expected"),
    [
        #: UTC 01:29 = 本地 09:29，UTC 01:49 = 本地 09:49（右界含）
        (1, 28, 0),
        (1, 29, 1),
        (1, 49, 1),
        (1, 50, 0),
    ],
)
def test_the_default_assembly_window_is_nine_twenty_nine_to_nine_forty_nine(
    tmp_path: Path,
    hour: int,
    minute: int,
    expected: int,
) -> None:
    calls: list[object] = []
    step = candidate_publisher_builder(
        auction_input_loader=lambda **kwargs: (calls.append(kwargs), _batch("auction_gap"))[1],
        clock=lambda: datetime(2026, 7, 31, hour, minute, tzinfo=UTC),
    )(_auction_manifest(tmp_path))

    step()

    assert len(calls) == expected


def test_the_assembly_window_can_be_moved_from_the_manifest(tmp_path: Path) -> None:
    """设置里给了装配窗就按给的走（回放与测试用；生产画像不写这两项，定窗只能改常量）。"""

    calls: list[object] = []
    step = candidate_publisher_builder(
        auction_input_loader=lambda **kwargs: (calls.append(kwargs), _batch("auction_gap"))[1],
        clock=lambda: datetime(2026, 7, 31, 1, 31, tzinfo=UTC),
    )(_auction_manifest(tmp_path, auction_input_start="09:36:00", auction_input_end="09:55:00"))

    step()

    assert calls == []


def _auction_match_settings(tmp_path: Path, **overrides: object) -> dict[str, object]:
    """`auction-match.source.v1` 的 manifest settings，与它自己的画像同形。

    这里只需要窗口那几项能被模型校验，所以路径给的是本用例的临时目录。
    """

    tmp_path.mkdir(parents=True, exist_ok=True)
    settings: dict[str, object] = {
        "spool_root": str(tmp_path / "auction-match"),
        "quota_path": str(tmp_path / "auction-match" / "quota.sqlite3"),
        "quota_units_per_window": 500,
        "producer_version": "auction-match-source-v1",
        "calendar_path": str(tmp_path / "calendar.json"),
        "calendar_expected_commit": COMMIT,
        "calendar_content_sha256": "c" * 64,
        "universe_path": str(tmp_path / "universe.json"),
        "max_attempts": 3,
    }
    settings.update(overrides)
    return settings


@pytest.mark.parametrize(
    ("capture", "assembly", "consistent"),
    [
        #: 默认的四个常量（09-23 实测 T = 09:27:14 之后）
        ((time(9, 29), time(9, 44)), (time(9, 29), time(9, 49)), True),
        #: 探测把窗整体后移，两边一起改
        ((time(9, 36), time(9, 50)), (time(9, 36), time(9, 55)), True),
        #: 只改了采集窗，忘了装配窗——竞价链会安静地什么都不产出
        ((time(9, 36), time(9, 50)), (time(9, 31), time(9, 50)), False),
        #: 只改了装配窗
        ((time(9, 31), time(9, 45)), (time(9, 36), time(9, 55)), False),
        #: 装配窗没有留出采集之后的余量
        ((time(9, 31), time(9, 45)), (time(9, 31), time(9, 45)), False),
    ],
)
def test_the_two_window_settings_stay_bound_to_each_other(
    tmp_path: Path,
    capture: tuple[time, time],
    assembly: tuple[time, time],
    consistent: bool,
) -> None:
    """复核代码质量 2：两对窗口只有默认常量被绑住，manifest 设置没有。

    这条用例把**设置对象里的值**（不是常量）喂给同一个判据函数，所以「操作员只改了一边」
    这件事在两个层面上都有人看着：生产画像生成时当场拒绝（常量那一层），以及这里
    （任何一对窗口设置那一层）。
    """

    capture_settings = AuctionMatchSourceSettings.model_validate(
        _auction_match_settings(
            tmp_path / "match",
            capture_start=capture[0].isoformat(),
            capture_end=capture[1].isoformat(),
        )
    )
    assembly_settings = CandidatePublisherRuntimeSettings.model_validate(
        dict(
            _auction_manifest(
                tmp_path / "gap",
                auction_input_start=assembly[0].isoformat(),
                auction_input_end=assembly[1].isoformat(),
            ).settings
        )
    )

    assert (
        auction_windows_are_consistent(
            capture_start=capture_settings.capture_start,
            capture_end=capture_settings.capture_end,
            input_start=assembly_settings.auction_input_start,
            input_end=assembly_settings.auction_input_end,
        )
        is consistent
    )


def test_an_impossible_assembly_window_is_refused(tmp_path: Path) -> None:
    with pytest.raises(ValidationError, match="auction_input_start"):
        CandidatePublisherRuntimeSettings.model_validate(
            dict(
                _auction_manifest(
                    tmp_path,
                    auction_input_start="09:50:00",
                    auction_input_end="09:31:00",
                ).settings
            )
        )


def test_auction_candidate_publisher_builds_live_input_during_auction_window(
    tmp_path: Path,
) -> None:
    calls: list[dict[str, object]] = []

    def auction_loader(**kwargs: object) -> CandidatePublishBatch:
        calls.append(dict(kwargs))
        return _batch("auction_gap")

    root = tmp_path / "live" / "auction-gap"
    manifest = RuntimeServiceManifest(
        service_id="candidate.auction-gap.v1",
        service_kind=RuntimeServiceKind.CANDIDATE_PUBLISHER,
        plane=RuntimeServicePlane.LIVE,
        interval_seconds=15,
        stale_after_seconds=60,
        producer_commit=COMMIT,
        settings={
            "strategy_id": "auction_gap",
            "strategy_version": 1,
            **_exact_strategy_settings("auction_gap"),
            "input_mode": "auction_live",
            "auction_spool_root": str(tmp_path / "auction-spool"),
            "daily_database_path": str(tmp_path / "operational-ro.duckdb"),
            "reference_registry_path": str(tmp_path / "reference.sqlite3"),
            "calendar_path": str(tmp_path / "calendar.json"),
            "calendar_expected_commit": COMMIT,
            "calendar_content_sha256": "c" * 64,
            "snapshot_root": str(root),
        },
    )
    observed_at = datetime(2026, 7, 31, 1, 40, tzinfo=UTC)

    result = candidate_publisher_builder(
        auction_input_loader=auction_loader,
        clock=lambda: observed_at,
    )(manifest)()

    assert len(calls) == 1
    #: the publisher hands the loader its own memory of the replica generation (#256):
    #: one object for the life of the run, pointed at the database this manifest names
    read_gate = calls[0].pop("read_gate")
    assert isinstance(read_gate, ReplicaReadGate)
    assert read_gate.path == tmp_path / "operational-ro.duckdb"
    assert calls[0] == {
        "auction_spool_root": tmp_path / "auction-spool",
        "daily_database_path": tmp_path / "operational-ro.duckdb",
        "reference_registry_path": tmp_path / "reference.sqlite3",
        "calendar_path": tmp_path / "calendar.json",
        "calendar_expected_commit": COMMIT,
        "calendar_content_sha256": "c" * 64,
        "trade_date": TRADE_DATE,
        "observed_at": observed_at,
        "producer_commit": COMMIT,
    }
    snapshot = StrategyCandidateSnapshotSpool(root).read_strategy_as_of(
        CAPTURED_AT,
        strategy_id="auction_gap",
        strategy_version="1",
        **_exact_strategy_settings("auction_gap"),
    )
    assert snapshot is not None
    assert result.output_sequence == snapshot.sequence == 0
    assert result.source_generations["candidate_input"] == "1" * 64


def test_live_auction_input_mode_is_exclusive_and_strategy_specific() -> None:
    base = {
        "strategy_id": "auction_gap",
        "strategy_version": 1,
        **_exact_strategy_settings("auction_gap"),
        "input_mode": "auction_live",
        "auction_spool_root": "/tmp/auction-spool",
        "daily_database_path": "/tmp/operational-ro.duckdb",
        "reference_registry_path": "/tmp/reference.sqlite3",
        "calendar_path": "/tmp/calendar.json",
        "calendar_expected_commit": COMMIT,
        "calendar_content_sha256": "c" * 64,
        "snapshot_root": "/tmp/output",
    }

    CandidatePublisherRuntimeSettings.model_validate(base)
    with pytest.raises(ValidationError, match="schema|auction_gap"):
        CandidatePublisherRuntimeSettings.model_validate({**base, "strategy_id": "n_shape"})
    with pytest.raises(ValidationError, match="candidate_input_path"):
        CandidatePublisherRuntimeSettings.model_validate(
            {**base, "candidate_input_path": "/tmp/input.json"}
        )


@pytest.mark.parametrize(
    "field",
    (
        "definition_fingerprint",
        "executable_fingerprint",
        "candidate_schema_fingerprint",
    ),
)
def test_candidate_runtime_requires_all_exact_strategy_fingerprints(field: str) -> None:
    settings = {
        "strategy_id": "n_shape",
        "strategy_version": 1,
        **_exact_strategy_settings("n_shape"),
        "candidate_input_path": "/tmp/input.json",
        "snapshot_root": "/tmp/output",
    }
    settings.pop(field)

    with pytest.raises(ValidationError, match=field):
        CandidatePublisherRuntimeSettings.model_validate(settings)


def test_auction_candidate_publisher_rejects_naive_runtime_clock(tmp_path: Path) -> None:
    manifest = RuntimeServiceManifest(
        service_id="candidate.auction-gap.v1",
        service_kind=RuntimeServiceKind.CANDIDATE_PUBLISHER,
        plane=RuntimeServicePlane.LIVE,
        interval_seconds=15,
        stale_after_seconds=60,
        producer_commit=COMMIT,
        settings={
            "strategy_id": "auction_gap",
            "strategy_version": 1,
            **_exact_strategy_settings("auction_gap"),
            "input_mode": "auction_live",
            "auction_spool_root": str(tmp_path / "auction-spool"),
            "daily_database_path": str(tmp_path / "operational-ro.duckdb"),
            "reference_registry_path": str(tmp_path / "reference.sqlite3"),
            "calendar_path": str(tmp_path / "calendar.json"),
            "calendar_expected_commit": COMMIT,
            "calendar_content_sha256": "c" * 64,
            "snapshot_root": str(tmp_path / "candidate"),
        },
    )
    step = candidate_publisher_builder(
        auction_input_loader=lambda **_: _batch("auction_gap"),
        clock=lambda: datetime(2026, 7, 31, 9, 27),
    )(manifest)

    with pytest.raises(ValueError, match="timezone-aware"):
        step()


def test_candidate_publisher_reloads_each_step_and_publishes_only_new_semantics(
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "n-shape.json"
    root = tmp_path / "live" / "n-shape"
    _write_input(input_path, _batch("n_shape", with_candidate=True))
    step = candidate_publisher_builder()(
        _manifest(
            tmp_path,
            candidate_input_path=input_path,
            snapshot_root=root,
        )
    )

    first = step()
    duplicate = step()
    _write_input(
        input_path,
        _batch(
            "n_shape",
            authority_snapshot_id="9" * 64,
            with_candidate=True,
        ),
    )
    changed = step()

    assert first.input_sequence == duplicate.input_sequence == changed.input_sequence == -1
    assert first.output_sequence == duplicate.output_sequence == 0
    assert changed.output_sequence == 1
    assert first.processed_count == duplicate.processed_count == changed.processed_count == 1
    assert first.source_generations["candidate_input"] == "1" * 64
    assert duplicate.source_generations == first.source_generations
    assert changed.source_generations["candidate_input"] == "9" * 64
    assert (
        changed.source_generations["strategy_candidate"]
        != first.source_generations["strategy_candidate"]
    )
    assert len(tuple((root / "generations").glob("*.json"))) == 2


def test_candidate_publisher_reloads_an_injected_loader_on_every_step(
    tmp_path: Path,
) -> None:
    calls: list[tuple[Path, str, str]] = []
    batches = iter(
        (
            _batch("n_shape", authority_snapshot_id="1" * 64),
            _batch("n_shape", authority_snapshot_id="2" * 64),
        )
    )

    def loader(
        path: Path,
        *,
        strategy_id: str,
        expected_commit: str,
    ) -> CandidatePublishBatch:
        calls.append((path, strategy_id, expected_commit))
        return next(batches)

    manifest = _manifest(tmp_path)
    step = candidate_publisher_builder(candidate_input_loader=loader)(manifest)

    assert step().output_sequence == 0
    assert step().output_sequence == 1
    assert calls == [
        (Path(manifest.settings["candidate_input_path"]), "n_shape", COMMIT),
        (Path(manifest.settings["candidate_input_path"]), "n_shape", COMMIT),
    ]


def _unpublished(root: Path) -> bool:
    """The owner's empty scaffolding and nothing else: no generation, no current pointer."""

    return (
        root.is_dir()
        and (root / ".publish.lock").is_file()
        and (root / "generations").is_dir()
        and not any((root / "generations").iterdir())
        and not (root / "current.json").exists()
        and not (root / "generation-index.json").exists()
        and not (root / "authority.json").exists()
    )


def test_candidate_publisher_fails_closed_on_input_commit_or_strategy_drift(
    tmp_path: Path,
) -> None:
    bad_commit_path = tmp_path / "bad-commit.json"
    bad_kind_path = tmp_path / "bad-kind.json"
    _write_input(
        bad_commit_path,
        _batch("n_shape", producer_commit="b" * 40),
    )
    _write_input(bad_kind_path, _batch("growth_board_surge"))

    for path, message in (
        (bad_commit_path, "commit"),
        (bad_kind_path, "strategy|kind"),
    ):
        root = tmp_path / "live" / path.stem
        with pytest.raises(ValueError, match=message):
            candidate_publisher_builder()(
                _manifest(
                    tmp_path,
                    candidate_input_path=path,
                    snapshot_root=root,
                )
            )()
        #: #254: the root, its `generations/` and `.publish.lock` are created by the
        #: owner at build, because that lock is what every reader of this store takes a
        #: shared lock on. What a refused step must not leave is a *publication*.
        assert _unpublished(root)


@pytest.mark.parametrize(
    ("mutation", "message"),
    (
        ("public_mode", "0600"),
        ("symlink", "symlink"),
        ("noncanonical", "canonical"),
        ("non_regular", "regular"),
        ("oversized", "size|large"),
    ),
)
def test_default_candidate_loader_rejects_unsafe_input_files(
    tmp_path: Path,
    mutation: str,
    message: str,
) -> None:
    real = tmp_path / "candidate.json"
    _write_input(real, _batch("n_shape"))
    path = real
    if mutation == "public_mode":
        real.chmod(0o644)
    elif mutation == "symlink":
        path = tmp_path / "linked.json"
        path.symlink_to(real)
    elif mutation == "noncanonical":
        parsed = json.loads(real.read_bytes())
        real.write_text(json.dumps(parsed, indent=2))
        real.chmod(0o600)
    elif mutation == "non_regular":
        real.unlink()
        real.mkdir(mode=0o700)
    elif mutation == "oversized":
        real.write_bytes(b" " * (16 * 1024 * 1024 + 1))
        real.chmod(0o600)

    with pytest.raises(ValueError, match=message):
        load_candidate_input(
            path,
            strategy_id="n_shape",
            expected_commit=COMMIT,
        )


def test_default_candidate_loader_rejects_symlinked_parent_and_duplicate_keys(
    tmp_path: Path,
) -> None:
    real_parent = tmp_path / "real"
    real_parent.mkdir()
    real = real_parent / "candidate.json"
    _write_input(real, _batch("n_shape"))
    linked_parent = tmp_path / "linked"
    linked_parent.symlink_to(real_parent, target_is_directory=True)

    with pytest.raises(ValueError, match="symlink"):
        load_candidate_input(
            linked_parent / "candidate.json",
            strategy_id="n_shape",
            expected_commit=COMMIT,
        )

    duplicate = tmp_path / "duplicate.json"
    duplicate.write_bytes(b'{"batch":{},"batch":{},"batch_kind":"n_shape","schema_version":1}')
    duplicate.chmod(0o600)
    with pytest.raises(ValueError, match="canonical|invalid"):
        load_candidate_input(
            duplicate,
            strategy_id="n_shape",
            expected_commit=COMMIT,
        )


def test_default_candidate_loader_rejects_wrong_uid(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "candidate.json"
    _write_input(path, _batch("n_shape"))
    monkeypatch.setattr(candidate_module.os, "getuid", lambda: path.stat().st_uid + 1)

    with pytest.raises(ValueError, match="owned|uid"):
        load_candidate_input(
            path,
            strategy_id="n_shape",
            expected_commit=COMMIT,
        )


def test_default_candidate_loader_rejects_hardlink_and_empty_file(
    tmp_path: Path,
) -> None:
    linked = tmp_path / "linked.json"
    hardlink = tmp_path / "hardlink.json"
    _write_input(linked, _batch("n_shape"))
    hardlink.hardlink_to(linked)

    with pytest.raises(ValueError, match="hardlink|link count"):
        load_candidate_input(
            linked,
            strategy_id="n_shape",
            expected_commit=COMMIT,
        )

    empty = tmp_path / "empty.json"
    empty.write_bytes(b"")
    empty.chmod(0o600)
    with pytest.raises(ValueError, match="size|empty"):
        load_candidate_input(
            empty,
            strategy_id="n_shape",
            expected_commit=COMMIT,
        )


def test_default_candidate_loader_rejects_file_replaced_before_open(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "candidate.json"
    replacement = tmp_path / "replacement.json"
    _write_input(path, _batch("n_shape"))
    _write_input(
        replacement,
        _batch("n_shape", authority_snapshot_id="2" * 64),
    )
    real_open = candidate_module.os.open
    replaced = False

    def replace_before_open(
        target: str | bytes | Path,
        flags: int,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> int:
        nonlocal replaced
        if (
            not replaced
            and target == path.name
            and dir_fd is not None
            and not flags & getattr(candidate_module.os, "O_DIRECTORY", 0)
        ):
            candidate_module.os.replace(replacement, path)
            replaced = True
        return real_open(target, flags, mode, dir_fd=dir_fd)

    monkeypatch.setattr(candidate_module.os, "open", replace_before_open)

    with pytest.raises(ValueError, match="identity changed"):
        load_candidate_input(
            path,
            strategy_id="n_shape",
            expected_commit=COMMIT,
        )
    assert replaced is True


@pytest.mark.parametrize("mutation", ("metadata", "content"))
def test_default_candidate_loader_rejects_change_while_reading(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
) -> None:
    path = tmp_path / "candidate.json"
    _write_input(path, _batch("n_shape"))
    real_read = candidate_module.os.read
    changed = False

    def change_after_read(descriptor: int, count: int) -> bytes:
        nonlocal changed
        payload = real_read(descriptor, count)
        if payload and not changed:
            if mutation == "metadata":
                observed = path.stat()
                candidate_module.os.utime(
                    path,
                    ns=(observed.st_atime_ns, observed.st_mtime_ns + 1_000_000),
                )
            else:
                with path.open("r+b") as stream:
                    stream.write(b"[")
                    stream.flush()
                    candidate_module.os.fsync(stream.fileno())
            changed = True
        return payload

    monkeypatch.setattr(candidate_module.os, "read", change_after_read)

    with pytest.raises(ValueError, match="changed while being read"):
        load_candidate_input(
            path,
            strategy_id="n_shape",
            expected_commit=COMMIT,
        )
    assert changed is True


def test_default_candidate_loader_rejects_discriminator_payload_conflict(
    tmp_path: Path,
) -> None:
    path = tmp_path / "candidate.json"
    growth_payload = _batch("growth_board_surge").model_dump(mode="json")
    growth_payload["facts"] = [{"board_type": "gem"}]
    path.write_bytes(
        canonical_json_bytes(
            {
                "batch": growth_payload,
                "batch_kind": "n_shape",
                "schema_version": 1,
            }
        )
    )
    path.chmod(0o600)

    with pytest.raises(ValueError, match="invalid|kind|batch"):
        load_candidate_input(
            path,
            strategy_id="n_shape",
            expected_commit=COMMIT,
        )


def test_default_candidate_loader_reads_from_one_secure_descriptor(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "candidate.json"
    expected = _batch("n_shape")
    _write_input(path, expected)

    def fail_path_read(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("candidate input path was reopened")

    monkeypatch.setattr(Path, "read_bytes", fail_path_read)
    monkeypatch.setattr(Path, "read_text", fail_path_read)
    payload_descriptors: set[int] = set()
    real_read = candidate_module.os.read

    def observe_descriptor(descriptor: int, count: int) -> bytes:
        payload_descriptors.add(descriptor)
        return real_read(descriptor, count)

    monkeypatch.setattr(candidate_module.os, "read", observe_descriptor)

    assert (
        load_candidate_input(
            path,
            strategy_id="n_shape",
            expected_commit=COMMIT,
        )
        == expected
    )
    assert len(payload_descriptors) == 1


@pytest.mark.parametrize(
    "settings",
    (
        {
            "strategy_id": "unsupported",
            "strategy_version": 1,
            "candidate_input_path": "/tmp/input.json",
            "snapshot_root": "/tmp/output",
        },
        {
            "strategy_id": "n_shape",
            "strategy_version": 2,
            "candidate_input_path": "/tmp/input.json",
            "snapshot_root": "/tmp/output",
        },
        {
            "strategy_id": "n_shape",
            "strategy_version": 1,
            "candidate_input_path": "relative/input.json",
            "snapshot_root": "/tmp/output",
        },
        {
            "strategy_id": "n_shape",
            "strategy_version": 1,
            "candidate_input_path": "/tmp/../tmp/input.json",
            "snapshot_root": "/tmp/output",
        },
        {
            "strategy_id": "n_shape",
            "strategy_version": 1,
            "candidate_input_path": "/tmp/input.json",
            "snapshot_root": "relative/output",
        },
    ),
)
def test_candidate_runtime_settings_are_typed_frozen_and_paths_are_normalized(
    settings: dict[str, object],
) -> None:
    with pytest.raises(ValidationError):
        CandidatePublisherRuntimeSettings.model_validate(settings)

    valid = CandidatePublisherRuntimeSettings(
        strategy_id="n_shape",
        strategy_version=1,
        **_exact_strategy_settings("n_shape"),
        candidate_input_path=Path("/tmp/input.json"),
        snapshot_root=Path("/tmp/output"),
    )
    with pytest.raises(ValidationError):
        valid.strategy_version = 2  # type: ignore[misc]


@pytest.mark.parametrize("invalid_version", (True, 1.0))
def test_candidate_runtime_settings_reject_coerced_versions(
    invalid_version: object,
) -> None:
    with pytest.raises(ValidationError, match="strategy_version"):
        CandidatePublisherRuntimeSettings.model_validate(
            {
                "strategy_id": "n_shape",
                "strategy_version": invalid_version,
                "candidate_input_path": "/tmp/input.json",
                "snapshot_root": "/tmp/output",
            }
        )


@pytest.mark.parametrize("invalid_version", (True, 1.0))
def test_candidate_input_document_rejects_coerced_schema_versions(
    tmp_path: Path,
    invalid_version: object,
) -> None:
    path = tmp_path / "candidate.json"
    payload = json.loads(serialize_candidate_input(_batch("n_shape")))
    payload["schema_version"] = invalid_version
    path.write_bytes(canonical_json_bytes(payload))
    path.chmod(0o600)

    with pytest.raises(ValueError, match="invalid|typed"):
        load_candidate_input(
            path,
            strategy_id="n_shape",
            expected_commit=COMMIT,
        )


def test_candidate_publisher_rejects_wrong_runtime_kind_or_plane(tmp_path: Path) -> None:
    builder = candidate_publisher_builder()
    with pytest.raises(ValueError, match="kind"):
        builder(_manifest(tmp_path, kind=RuntimeServiceKind.FEATURE_LIVE))
    with pytest.raises(ValueError, match="live plane"):
        builder(_manifest(tmp_path, plane=RuntimeServicePlane.RESEARCH))


def test_injected_loader_cannot_cross_strategy_authority(tmp_path: Path) -> None:
    root = tmp_path / "live" / "candidate"

    def loader(*_args: object, **_kwargs: object) -> CandidatePublishBatch:
        return _batch("growth_board_surge")

    with pytest.raises(ValueError, match="strategy|kind"):
        candidate_publisher_builder(candidate_input_loader=loader)(
            _manifest(tmp_path, snapshot_root=root)
        )()

    assert _unpublished(root)


# ---------------------------------------------------------------------------------------
# #254: the reader's shared lock is the publisher's to create, and only a publish made it
# ---------------------------------------------------------------------------------------


def _legacy_unbound_root(root: Path) -> None:
    """`live/candidates/<svc>/` as the host had it: generations, no `authority.json`."""

    StrategyCandidateSnapshotSpool(root).publish_legacy_records_for_migration(
        trade_date=TRADE_DATE,
        captured_at=CAPTURED_AT,
        producer_commit=COMMIT,
        rows=(),
    )
    assert not (root / "authority.json").exists()


def test_the_publisher_creates_its_readers_lock_at_build_not_at_first_publish(
    tmp_path: Path,
) -> None:
    """#254: two source roles read this store, and both refused it for a whole window.

    `market-minute.source.v1` and `watchlist-quote.source.v1` failed every iteration with
    `snapshot authority is damaged: strategy candidate snapshot lock is missing or
    unsafe`. The reader has taken a shared lock on `.publish.lock` since the store was
    introduced -- that requirement did not move -- but only a *publish* ever created the
    file, and the auction_gap publisher published nothing in that window. The build is
    where the owner now creates it, whatever else it finds.
    """

    root = tmp_path / "live" / "candidates" / "svc-auction-gap"
    _legacy_unbound_root(root)
    #: the state the host was in: generations on disk, no lock beside them
    (root / ".publish.lock").unlink()
    reader = StrategyCandidateSnapshotSpool(root)
    with pytest.raises(Exception, match="lock is missing or unsafe"):
        reader.read_legacy_for_migration(CAPTURED_AT)

    candidate_publisher_builder()(_manifest(tmp_path, snapshot_root=root))

    assert (root / ".publish.lock").is_file()
    assert reader.read_legacy_for_migration(CAPTURED_AT) is not None


def test_the_build_creates_the_root_of_a_publisher_that_has_never_published(
    tmp_path: Path,
) -> None:
    """A store no batch has reached yet is young, not damaged, and readable as empty."""

    root = tmp_path / "live" / "candidates" / "svc-fresh"
    candidate_publisher_builder()(_manifest(tmp_path, snapshot_root=root))

    assert _unpublished(root)
    assert StrategyCandidateSnapshotSpool(root).read_as_of(CAPTURED_AT) is None


@pytest.mark.parametrize(
    ("mode", "message"),
    (
        (0o666, "not a private regular file"),
        ("symlink", "lock"),
    ),
)
def test_a_damaged_lock_is_still_refused_at_build(
    tmp_path: Path,
    mode: object,
    message: str,
) -> None:
    """The negative half: creating the lock is not accepting whatever is at its name."""

    import os

    root = tmp_path / "live" / "candidates" / "svc-damaged"
    _legacy_unbound_root(root)
    lock = root / ".publish.lock"
    if mode == "symlink":
        lock.unlink()
        os.symlink(root / "current.json", lock)
    else:
        lock.chmod(mode)  # type: ignore[arg-type]

    with pytest.raises(Exception, match=message):
        candidate_publisher_builder()(_manifest(tmp_path, snapshot_root=root))


def test_a_root_that_is_not_private_is_still_refused_at_build(tmp_path: Path) -> None:
    """And the directory the lock lives in has to be ours and 0700, as it always did."""

    root = tmp_path / "live" / "candidates" / "svc-open"
    _legacy_unbound_root(root)
    root.chmod(0o755)

    with pytest.raises(Exception, match="directory"):
        candidate_publisher_builder()(_manifest(tmp_path, snapshot_root=root))



# ---------------------------------------------------------------------------------------
# #278：n_shape / growth_board_surge 的候选文档按交易日重建
# ---------------------------------------------------------------------------------------

SESSION_TRADE_DATE = date(2026, 8, 12)
SESSION_OPEN_DATES = (date(2026, 8, 10), date(2026, 8, 11), SESSION_TRADE_DATE)
#: 覆盖期比开盘日宽，这样才分得清「今天不开市」与「这本日历覆盖不到今天」（复核 SF-1）
SESSION_COVERAGE_START = date(2026, 8, 10)
SESSION_COVERAGE_END = date(2026, 8, 16)
#: 覆盖期内的周六：不开市，但日历回答得了
SESSION_CLOSED_DATE = date(2026, 8, 15)
#: 覆盖期之外：日历回答不了
SESSION_UNCOVERED_DATE = date(2026, 8, 20)


def _session_calendar_path(tmp_path: Path) -> tuple[Path, MarketCalendarAuthority]:
    tmp_path.mkdir(parents=True, exist_ok=True)
    calendar = MarketCalendarAuthority.create(
        schema_version=1,
        exchange="SSE",
        producer_commit=COMMIT,
        coverage_start=SESSION_COVERAGE_START,
        coverage_end=SESSION_COVERAGE_END,
        open_dates=SESSION_OPEN_DATES,
        generated_at=datetime(2026, 8, 9, 8, 0, tzinfo=UTC),
    )
    path = tmp_path / "calendar.json"
    path.write_text(
        json.dumps(
            calendar.model_dump(mode="json"),
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
    )
    path.chmod(0o600)
    return path, calendar


def _session_manifest(
    tmp_path: Path,
    *,
    strategy_id: str = "n_shape",
    **overrides: object,
) -> RuntimeServiceManifest:
    calendar_path, calendar = _session_calendar_path(tmp_path)
    settings: dict[str, object] = {
        "strategy_id": strategy_id,
        "strategy_version": 1,
        **_exact_strategy_settings(strategy_id),
        "input_mode": "session_document",
        "daily_database_path": str(tmp_path / "rquant_ro.duckdb"),
        "calendar_path": str(calendar_path),
        "calendar_expected_commit": COMMIT,
        "calendar_content_sha256": calendar.content_sha256,
        "snapshot_root": str(tmp_path / "live" / strategy_id),
    }
    settings.update(overrides)
    return RuntimeServiceManifest(
        service_id=f"candidate.{strategy_id}.v1",
        service_kind=RuntimeServiceKind.CANDIDATE_PUBLISHER,
        plane=RuntimeServicePlane.LIVE,
        interval_seconds=5,
        stale_after_seconds=180,
        producer_commit=COMMIT,
        settings=settings,
    )


def _session_batch(strategy_id: str, *, trade_date: date, captured_at: datetime):
    return candidate_input_batch(
        strategy_id=strategy_id,
        producer_commit=COMMIT,
        trade_date=trade_date,
        captured_at=captured_at,
        basis_trade_date=SESSION_OPEN_DATES[-2],
        contract=SESSION_CANDIDATE_CONTRACT,
    )


def _session_loader(calls: list[dict[str, object]]):
    def loader(**kwargs: object):
        calls.append(dict(kwargs))
        return _session_batch(
            str(kwargs["strategy_id"]),
            trade_date=kwargs["trade_date"],  # type: ignore[arg-type]
            captured_at=kwargs["observed_at"],  # type: ignore[arg-type]
        )

    return loader


def _at(hour: int, minute: int, *, day: date = SESSION_TRADE_DATE) -> datetime:
    return datetime.combine(day, time(hour, minute), tzinfo=SHANGHAI).astimezone(UTC)


def test_the_session_publisher_rebuilds_todays_document_once(tmp_path: Path) -> None:
    """#278 的正面：每个交易日一份当日文档，且一天只发一次。

    一天只发一次是硬要求，不是优化：文档里带着生成时刻，每轮重建就是每五秒发一代新快照，
    正是包 W 拆掉的那种「每轮无条件写」。
    """

    calls: list[dict[str, object]] = []
    clock = {"now": _at(8, 45)}
    step = candidate_publisher_builder(
        session_input_loader=_session_loader(calls),
        clock=lambda: clock["now"],
    )(_session_manifest(tmp_path))

    first = step()
    clock["now"] = _at(9, 10)
    second = step()

    assert len(calls) == 1
    assert calls[0]["trade_date"] == SESSION_TRADE_DATE
    assert calls[0]["observed_at"] == _at(8, 45)
    assert first.processed_count == 0  # 空事实列表：候选数为零，但快照发出去了
    assert first.output_sequence == 0
    assert first.degraded_reasons == ()
    #: 第二轮什么都没写，但**输出序号照抄**——心跳不接受回退的序号
    assert second.output_sequence == first.output_sequence
    assert second.processed_count == 0
    assert second.degraded_reasons == ()

    snapshot = StrategyCandidateSnapshotSpool(tmp_path / "live" / "n_shape").read_strategy_as_of(
        _at(9, 10),
        strategy_id="n_shape",
        strategy_version="1",
        **_exact_strategy_settings("n_shape"),
    )
    assert snapshot is not None
    assert snapshot.trade_date == SESSION_TRADE_DATE
    assert snapshot.captured_at == _at(8, 45)


def test_an_idle_round_after_a_publish_does_not_regress_the_output_sequence(
    tmp_path: Path,
) -> None:
    """`RuntimeServiceControl.record_success` 拒绝回退的输出序号。

    发完一代之后窗外的每一轮原来都返回 -1，于是心跳一侧会抛
    `ValueError: output sequence cannot regress`——两种 live 模式都有这条路径。
    auction_gap 从来没真的发出过东西（#254 / #277），所以它一次都没被走到过；
    竞价链一旦真的开始产出，它会在每一轮上抛。
    """

    calls: list[dict[str, object]] = []
    clock = {"now": _at(8, 45)}
    session = candidate_publisher_builder(
        session_input_loader=_session_loader(calls),
        clock=lambda: clock["now"],
    )(_session_manifest(tmp_path / "session"))

    published = session()
    clock["now"] = _at(9, 10)
    idled = session()
    clock["now"] = _at(8, 30, day=SESSION_OPEN_DATES[-2])
    closed = session()

    assert published.output_sequence == 0
    assert idled.output_sequence == 0
    assert closed.output_sequence == 0

    #: auction_live 那一支同样
    gap_clock = {"now": datetime(2026, 7, 31, 1, 40, tzinfo=UTC)}
    gap = candidate_publisher_builder(
        auction_input_loader=lambda **_: _batch("auction_gap"),
        clock=lambda: gap_clock["now"],
    )(_auction_manifest(tmp_path / "gap"))

    gap_published = gap()
    gap_clock["now"] = datetime(2026, 7, 31, 2, 15, tzinfo=UTC)
    gap_idled = gap()

    assert gap_published.output_sequence == 0
    assert gap_idled.output_sequence == 0


def test_the_session_publisher_waits_for_its_start_time(tmp_path: Path) -> None:
    calls: list[dict[str, object]] = []
    step = candidate_publisher_builder(
        session_input_loader=_session_loader(calls),
        clock=lambda: _at(8, 44),
    )(_session_manifest(tmp_path))

    result = step()

    assert calls == []
    assert result.degraded_reasons == ()
    assert result.output_sequence == -1


def test_a_publisher_restarted_after_the_open_still_publishes_today(tmp_path: Path) -> None:
    """10:00 才被拉起来也要发今天这一份，否则两个下游源一整天读不到必需的权威。"""

    calls: list[dict[str, object]] = []
    step = candidate_publisher_builder(
        session_input_loader=_session_loader(calls),
        clock=lambda: _at(10, 0),
    )(_session_manifest(tmp_path))

    result = step()

    assert len(calls) == 1
    assert result.output_sequence == 0


def test_a_closed_date_publishes_nothing_and_is_not_a_degradation(tmp_path: Path) -> None:
    """日历回答得了、答案是「今天不开市」——这不是降级，是正常的周末。"""

    calls: list[dict[str, object]] = []
    step = candidate_publisher_builder(
        session_input_loader=_session_loader(calls),
        clock=lambda: _at(8, 45, day=SESSION_CLOSED_DATE),
    )(_session_manifest(tmp_path))

    result = step()

    assert calls == []
    assert result.degraded_reasons == ()
    assert result.replica_opened is False


def test_a_date_outside_calendar_coverage_is_a_visible_degradation(tmp_path: Path) -> None:
    """复核 SF-1：日历覆盖不到今天时不许安静空转。

    改动前这个分支自己判 `trade_date not in open_dates`，于是「日历过期了」与「今天不开市」
    走同一条静默的路，心跳干干净净——正是 #277 那种看不出来的形状。现在走
    `decide_market_session`，它对覆盖期外是抛，构建器把它翻成一条点名的降级理由。
    """

    calls: list[dict[str, object]] = []
    step = candidate_publisher_builder(
        session_input_loader=_session_loader(calls),
        clock=lambda: _at(8, 45, day=SESSION_UNCOVERED_DATE),
    )(_session_manifest(tmp_path))

    result = step()

    assert calls == []
    assert result.degraded_reasons == (f"calendar_uncovered:{SESSION_UNCOVERED_DATE.isoformat()}",)


def test_a_calendar_generated_after_the_clock_fails_hard(tmp_path: Path) -> None:
    """时钟回拨 / 权威错代是**硬失败**，不是降级（复核裁定 A / B）。

    `decide_market_session` 的两种拒绝原来共用一个 `calendar_uncovered:` 标签，操作员看到
    它会去查日历覆盖期，查完发现覆盖期没问题。现在拆成两条，而且「这台机器现在说的话不
    可信」这一条在**两个 role 里都抛**——auction-match 本来就抛，这里跟上。
    """

    calls: list[dict[str, object]] = []
    step = candidate_publisher_builder(
        session_input_loader=_session_loader(calls),
        #: 日历的 generated_at 是 2026-08-09 08:00 UTC，把钟拨到它之前
        clock=lambda: _at(8, 45, day=SESSION_OPEN_DATES[0]) - timedelta(days=2),
    )(_session_manifest(tmp_path))

    with pytest.raises(MarketSessionCalendarError, match="calendar_clock_regressed:"):
        step()

    assert calls == []


def test_the_next_session_gets_its_own_document(tmp_path: Path) -> None:
    calls: list[dict[str, object]] = []
    clock = {"now": _at(8, 45, day=SESSION_OPEN_DATES[-2])}
    step = candidate_publisher_builder(
        session_input_loader=_session_loader(calls),
        clock=lambda: clock["now"],
    )(_session_manifest(tmp_path))

    step()
    clock["now"] = _at(8, 45)
    step()

    assert [call["trade_date"] for call in calls] == [SESSION_OPEN_DATES[-2], SESSION_TRADE_DATE]


def test_a_replica_that_cannot_answer_is_a_named_degradation(tmp_path: Path) -> None:
    def refusing(**_: object):
        raise SessionCandidateInputError("the read-only replica has no daily result")

    step = candidate_publisher_builder(
        session_input_loader=refusing,
        clock=lambda: _at(8, 45),
    )(_session_manifest(tmp_path))

    result = step()

    assert result.degraded_reasons == ("session_candidate_input_unavailable",)
    assert result.output_sequence == -1


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"strategy_id": "auction_gap"}, "not valid for auction_gap"),
        ({"candidate_input_path": "/tmp/x.json"}, "candidate_input_path is forbidden"),
        ({"auction_spool_root": "/tmp/spool"}, "auction spool paths are forbidden"),
        ({"daily_database_path": None}, "needs the replica and the calendar"),
    ],
)
def test_the_session_mode_refuses_a_settings_shape_it_cannot_run(
    tmp_path: Path,
    overrides: dict[str, object],
    message: str,
) -> None:
    settings = dict(_session_manifest(tmp_path).settings)
    for key, value in overrides.items():
        if value is None:
            settings.pop(key)
        else:
            settings[key] = value
    if overrides.get("strategy_id") == "auction_gap":
        settings.update(_exact_strategy_settings("auction_gap"))

    with pytest.raises(ValidationError, match=message):
        CandidatePublisherRuntimeSettings.model_validate(settings)


def test_a_sealed_document_publisher_is_still_supported_for_replay(tmp_path: Path) -> None:
    """封存模式没有被删掉，只是生产不再用它——回放与测试还要。"""

    settings = CandidatePublisherRuntimeSettings.model_validate(
        dict(
            _manifest(
                tmp_path,
                strategy_id="n_shape",
            ).settings
        )
    )

    assert settings.input_mode == "sealed_document"
