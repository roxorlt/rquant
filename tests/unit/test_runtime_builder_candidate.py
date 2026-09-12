from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

import pytest
from pydantic import ValidationError

from rquant import runtime_builder_candidate as candidate_module
from rquant.auction_gap_candidate_input import AuctionGapCandidateInputError
from rquant.live_contracts import BatchQualityStatus
from rquant.readside_replica_gate import ReplicaReadGate
from rquant.runtime_builder_candidate import (
    CandidatePublisherRuntimeSettings,
    candidate_publisher_builder,
    load_candidate_input,
    serialize_candidate_input,
)
from rquant.runtime_service_control import RuntimeServicePlane
from rquant.runtime_service_entrypoint import RuntimeServiceKind, RuntimeServiceManifest
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
    #: 18:30 Asia/Shanghai, far outside 09:26-09:30
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
    09:26-09:30 window and then idles outside it: without `begin_iteration()` the idle
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

    #: 09:26:30 then 09:40 Asia/Shanghai: inside the window, then outside it
    inside = datetime(2026, 7, 31, 1, 26, 30, tzinfo=UTC)
    clock = {"now": inside}
    step = candidate_publisher_builder(
        auction_input_loader=reading_loader,
        clock=lambda: clock["now"],
    )(manifest)

    published = step()
    clock["now"] = datetime(2026, 7, 31, 1, 40, tzinfo=UTC)
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
        clock=lambda: datetime(2026, 7, 31, 1, 26, 30, tzinfo=UTC),
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
        clock=lambda: datetime(2026, 7, 31, 1, 26, 30, tzinfo=UTC),
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
    observed_at = datetime(2026, 7, 31, 1, 27, tzinfo=UTC)

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

