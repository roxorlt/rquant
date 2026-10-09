from __future__ import annotations

import hashlib
import importlib
import importlib.util
import json
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pyarrow.parquet as parquet
import pytest

from rquant.experiment_platform import NativeMinutePhaseRead
from rquant.experiment_registry import DateRange
from rquant.minute_backtest_contracts import MAX_INPUT_BYTES
from rquant.minute_backtest_producer import (
    core_source_receipt,
    measure_minute_formal_work,
    minute_metadata_identities,
    publish_minute_input,
    verify_minute_source_content,
)
from rquant.minute_backtest_publication_contracts import (
    FrozenMinuteResearchInput,
    MinuteSourceContentSeed,
)
from rquant.minute_backtest_runner import run_minute_runtime_replay
from rquant.research_catalog import ResearchCatalog
from rquant.runtime_contracts import canonical_sha256
from rquant.runtime_market_session import MarketCalendarAuthority
from rquant.signal_contracts import SignalAction
from rquant.storage.duckdb import DuckDBStore
from rquant.strategy_authoring_commands import StrategyTemplateHead
from rquant.strategy_promotion_contracts import (
    NativeMinuteConfiguration,
    NativeMinuteSelection,
    StrategyPromotionTarget,
)
from tests.unit.test_minute_backtest_producer import FIXTURE_SHA, NOW


def _builder() -> Callable[..., MinuteSourceContentSeed]:
    name = "tests.support.native_minute_phase_sources"
    assert importlib.util.find_spec(name) is not None, (
        "complete native phase source factory is missing"
    )
    return importlib.import_module(name).build_native_phase_seed


@pytest.fixture(scope="module")
def base_seed(
    tmp_path_factory: pytest.TempPathFactory, calendar: MarketCalendarAuthority
) -> MinuteSourceContentSeed:
    _builder()
    build_base = importlib.import_module(
        "tests.support.native_minute_phase_sources"
    ).build_native_phase_base_seed
    return build_base(
        tmp_path_factory.getbasetemp() / "native-phase-base",
        native_id="n_shape",
        calendar=calendar,
        window=DateRange(start_date=date(2026, 1, 5), end_date=date(2026, 1, 9)),
        published_at=NOW,
    )


@pytest.fixture(scope="module")
def calendar() -> MarketCalendarAuthority:
    start, end = date(2025, 11, 3), date(2026, 11, 30)
    days = tuple(start + timedelta(days=i) for i in range((end - start).days + 1))
    return MarketCalendarAuthority.create(
        schema_version=1,
        exchange="SSE",
        producer_commit="a" * 40,
        coverage_start=start,
        coverage_end=end,
        open_dates=tuple(day for day in days if day.weekday() < 5),
        generated_at=datetime(2025, 11, 1, tzinfo=UTC),
    )


def _read(seed: MinuteSourceContentSeed, start: date, end: date) -> NativeMinutePhaseRead:
    native, runtime = seed.native_registration, seed.runtime
    target = StrategyPromotionTarget(
        source_kind="builtin",
        owner_id=runtime.owner_id,
        strategy_id=native.logical_id,
        name=native.logical_id,
        head=StrategyTemplateHead(
            version=native.version,
            registration_fingerprint=native.fingerprint,
            record_hash=native.record_hash,
            spec_fingerprint=native.spec.spec_fingerprint,
        ),
        parameter_fingerprint=native.spec.parameter_fingerprint,
        cost_fingerprint=canonical_sha256(runtime.execution_profile.execution_costs),
    )
    selection = NativeMinuteSelection(
        target=target,
        source_key=runtime.source_key,
        source_version=runtime.source_version,
        profile_hash=runtime.execution_profile.profile_hash,
    )
    configuration = NativeMinuteConfiguration(selection=selection, start_date=start, end_date=end)
    return NativeMinutePhaseRead(
        owner=runtime.owner_id,
        family_id="synthetic-phase-source-unit",
        source_identity=canonical_sha256({"synthetic": "source-unit", "selection": selection}),
        source_key=runtime.source_key,
        source_version=runtime.source_version,
        phase="search",
        window=DateRange(start_date=start, end_date=end),
        index=0,
        configuration=configuration,
    )


def _freeze(seed: MinuteSourceContentSeed) -> FrozenMinuteResearchInput:
    audit, snapshot = minute_metadata_identities(seed)
    return seed.freeze(audit_run_id=audit.audit_run_id, dataset_snapshot_id=snapshot.snapshot_id)


def test_complete_phase_factory_is_available() -> None:
    assert callable(_builder())


@pytest.mark.parametrize("strategy_id", ["n_shape", "auction_gap", "growth_board_surge"])
def test_exact_window_uses_original_verifier_and_real_runner(
    tmp_path: Path,
    base_seed: MinuteSourceContentSeed,
    calendar: MarketCalendarAuthority,
    strategy_id: str,
) -> None:
    build = _builder()
    seed_root = tmp_path / "original-base"
    build_base = importlib.import_module(
        "tests.support.native_minute_phase_sources"
    ).build_native_phase_base_seed
    base = build_base(
        seed_root,
        native_id=strategy_id,
        calendar=calendar,
        window=DateRange(start_date=date(2026, 1, 5), end_date=date(2026, 1, 9)),
        published_at=NOW,
    )
    before = base.seed_hash
    read = _read(base, date(2026, 1, 5), date(2026, 1, 9))
    seed = build(
        tmp_path / "physical-source", read=read, base_seed=base, calendar=calendar, published_at=NOW
    )
    frozen = _freeze(seed)
    verify_minute_source_content(frozen, installed_policies=(seed.provenance.visibility_policy,))
    assert (seed.runtime.source_key, seed.runtime.start_date, seed.runtime.end_date) == (
        read.publication_source_key,
        read.window.start_date,
        read.window.end_date,
    )
    assert seed.runtime.market_calendar == calendar
    assert seed.runtime.execution_profile == base.runtime.execution_profile
    assert seed.native_registration == base.native_registration
    assert seed.wrapper_registration == base.wrapper_registration
    assert seed.provenance.source_kind == "reconstructed"
    assert all(item.time_basis == "modeled" for item in seed.provenance.publication_evidence)
    assert all(item.captured_at is None for item in seed.provenance.capture_lineage)
    assert base.seed_hash == before
    manifest = json.loads((tmp_path / "physical-source/source-manifest.json").read_bytes())
    assert manifest["synthetic"] is True and manifest["historical_capture"] is False
    assert manifest["seed_hash"] == seed.seed_hash
    assert manifest["original_fixture_sha256"] == FIXTURE_SHA
    for item in seed.runtime.materials:
        physical = tmp_path / "physical-source/source" / item.relative_path
        assert physical.read_bytes() == item.payload()
    result = run_minute_runtime_replay(
        frozen.runtime,
        expected=core_source_receipt(frozen),
        research_root=tmp_path / "original-runner",
    )
    assert result.status == result.daily_status == "complete"
    assert tuple(item.trade_date for item in result.daily_valuations) == tuple(
        day for day in calendar.open_dates if read.window.start_date <= day <= read.window.end_date
    )
    assert any(signal.action is SignalAction.B_INTENT for signal in result.signals)
    assert result.fills and any(fill.commission > 0 for fill in result.fills)
    (tmp_path / "original-result.json").write_text(result.model_dump_json(indent=2))


def test_long_training_validation_archive_fits_actual_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    base_seed: MinuteSourceContentSeed,
    calendar: MarketCalendarAuthority,
) -> None:
    build = _builder()
    read = _read(base_seed, date(2026, 1, 5), date(2026, 4, 30))
    seed = build(
        tmp_path / "physical-source",
        read=read,
        base_seed=base_seed,
        calendar=calendar,
        published_at=NOW,
    )
    import rquant.storage.duckdb as storage

    monkeypatch.setattr(
        storage, "_settings", lambda: SimpleNamespace(primary_writer_gate_path=None)
    )
    installed = tmp_path / "installed"
    installed.mkdir(mode=0o700)
    with DuckDBStore(tmp_path / "metadata.duckdb") as metadata:
        published = publish_minute_input(
            seed,
            metadata_store=metadata,
            source_path=installed / "input.duckdb",
            receipt_path=installed / "publication.json",
            catalog=ResearchCatalog(tmp_path / "catalog.sqlite"),
            lake_root=tmp_path / "lake",
            installed_policies=(seed.provenance.visibility_policy,),
            now=NOW,
        )
    actual = published.receipt.source_file_bytes + published.receipt.snapshot_artifact_bytes
    actual += (installed / "publication.json").stat().st_size
    assert actual <= MAX_INPUT_BYTES
    assert (
        published.reference.load(installed_policies=(seed.provenance.visibility_policy,))
        == published.receipt
    )
    assert seed.formal_work.runtime_work.daily_observations == 84
    (tmp_path / "actual-publication-budget.json").write_text(
        json.dumps(
            {
                "synthetic": True,
                "limit_bytes": MAX_INPUT_BYTES,
                "seed_bytes": (tmp_path / "physical-source/source-seed.json").stat().st_size,
                "receipt_bytes": (installed / "publication.json").stat().st_size,
                "source_file_bytes": published.receipt.source_file_bytes,
                "snapshot_artifact_bytes": published.receipt.snapshot_artifact_bytes,
                "complete_storage_bytes": actual,
                "formal_work": seed.formal_work.model_dump(mode="json"),
            },
            indent=2,
        )
    )


@pytest.mark.parametrize(
    "defect",
    ["owner", "profile", "window", "version", "calendar", "calendar_identity", "warmup", "future"],
)
def test_incompatible_request_rejected_before_writing(
    tmp_path: Path,
    base_seed: MinuteSourceContentSeed,
    calendar: MarketCalendarAuthority,
    defect: str,
) -> None:
    build = _builder()
    read = _read(base_seed, date(2026, 1, 5), date(2026, 1, 9))
    data = read.model_dump(mode="python")
    clock = NOW
    if defect == "owner":
        data["owner"] = "another-owner"
    elif defect == "profile":
        data["configuration"]["selection"]["profile_hash"] = "b" * 64
    elif defect == "window":
        data["window"]["end_date"] = date(2026, 1, 8)
    elif defect == "version":
        data["source_version"] = 2
    elif defect == "calendar_identity":
        fields = calendar.model_dump(mode="python", exclude={"content_sha256"})
        fields["open_dates"] = calendar.open_dates[:-1]
        calendar = MarketCalendarAuthority.create(**fields)
    elif defect in {"calendar", "warmup"}:
        first = date(2026, 1, 6) if defect == "calendar" else date(2026, 1, 5)
        calendar = MarketCalendarAuthority.create(
            schema_version=1,
            exchange="SSE",
            producer_commit=calendar.producer_commit,
            coverage_start=first,
            coverage_end=calendar.coverage_end,
            open_dates=tuple(day for day in calendar.open_dates if day >= first),
            generated_at=calendar.generated_at,
        )
    else:
        for field in ("start_date", "end_date"):
            data["window"][field] = date(2026, 10, 7)
            data["configuration"][field] = date(2026, 10, 7)
    read = NativeMinutePhaseRead.model_validate(data)
    with pytest.raises((ValueError, PermissionError)):
        build(
            tmp_path / "rejected-source",
            read=read,
            base_seed=base_seed,
            calendar=calendar,
            published_at=clock,
        )
    assert not (tmp_path / "rejected-source").exists()


def test_missing_publication_proof_cannot_pass_original_verifier(
    tmp_path: Path,
    base_seed: MinuteSourceContentSeed,
    calendar: MarketCalendarAuthority,
) -> None:
    build = _builder()
    seed = build(
        tmp_path / "physical-source",
        read=_read(base_seed, date(2026, 1, 5), date(2026, 1, 6)),
        base_seed=base_seed,
        calendar=calendar,
        published_at=NOW,
    )
    data = seed.model_dump(mode="python")
    provenance = seed.provenance.model_copy(
        update={"publication_evidence": seed.provenance.publication_evidence[1:]}
    )
    data["provenance"] = provenance
    data["formal_work"] = measure_minute_formal_work(
        seed.runtime.work,
        origins=seed.origin_materials,
        provenance=provenance,
        derivations=seed.derivations,
    )
    changed = MinuteSourceContentSeed.model_validate(data)
    with pytest.raises(PermissionError, match="per-publication evidence"):
        verify_minute_source_content(
            _freeze(changed), installed_policies=(seed.provenance.visibility_policy,)
        )


def test_old_original_fixture_is_unchanged() -> None:
    path = Path(__file__).resolve().parents[2] / (
        "data/verification/minute-engine-completion-20261007/core-implementation-01/behavior/"
        "daily-n_shape-bar_end.json"
    )
    assert hashlib.sha256(path.read_bytes()).hexdigest() == FIXTURE_SHA


def test_original_market_parents_cover_every_runtime_batch(
    base_seed: MinuteSourceContentSeed,
) -> None:
    origins = {item.object_key: item for item in base_seed.origin_materials}
    assert {"synthetic-market-bars", "synthetic-market-manifests"}.issubset(origins), (
        "complete market parents are still repeated per runtime partition"
    )
    original_bars = parquet.read_table(
        BytesIO(origins["synthetic-market-bars"].payload())
    ).to_pandas()
    original_manifests = json.loads(origins["synthetic-market-manifests"].payload())["rows"]
    raw = [
        item
        for item in base_seed.runtime.materials
        if item.relative_path.startswith("market/batches/")
    ]
    payloads = [item for item in raw if item.relative_path.endswith(".payload")]
    manifests = [item for item in raw if item.relative_path.endswith(".json")]
    actual_bars = pd.concat(
        [parquet.read_table(BytesIO(item.payload())).to_pandas() for item in payloads],
        ignore_index=True,
    )
    pd.testing.assert_frame_equal(actual_bars, original_bars)
    assert [json.loads(item.payload()) for item in manifests] == original_manifests
    assert len(original_bars) == base_seed.runtime.work.raw_rows
    assert len(original_manifests) == base_seed.runtime.work.market_batches
    derivations = {item.material_path: item for item in base_seed.derivations}
    for item in payloads:
        derivation = derivations[item.relative_path]
        assert derivation.method == "research_derivative"
        assert set(derivation.origin_object_keys) == {
            "synthetic-market-bars",
            "synthetic-market-manifests",
        }
        envelope = next(
            value for value in original_manifests if value["content_sha256"] == item.content_sha256
        )
        subset = original_bars.loc[
            (original_bars.trade_time >= envelope["event_time_start"])
            & (original_bars.trade_time <= envelope["event_time_end"])
        ]
        encoder = importlib.import_module("tests.support.native_minute_phase_sources")._parquet
        assert encoder(subset.to_dict(orient="records")) == item.payload()


def test_candidate_parent_contains_only_its_visible_session(
    tmp_path: Path,
    base_seed: MinuteSourceContentSeed,
    calendar: MarketCalendarAuthority,
) -> None:
    read = _read(base_seed, date(2026, 1, 5), date(2026, 1, 6))
    short = _builder()(
        tmp_path / "short-prefix",
        read=read,
        base_seed=base_seed,
        calendar=calendar,
        published_at=NOW,
    )

    def first_candidate(seed: MinuteSourceContentSeed) -> bytes:
        return next(
            item.payload()
            for item in seed.runtime.materials
            if item.relative_path.startswith("candidates/generations/")
            and json.loads(item.payload())["trade_date"] == "2026-01-05"
        )

    candidate = first_candidate(short)
    assert candidate == first_candidate(base_seed), (
        "same visible candidate depends on later phase reference rows"
    )
    source_hash = json.loads(candidate)["source_snapshot_ids"]["daily_state"]
    parent = next(item for item in short.origin_materials if item.content_sha256 == source_hash)
    rows = json.loads(parent.payload())["rows"]
    assert len(rows) == short.runtime.work.union_codes
    assert all(row["effective_trade_date"] == "2026-01-05" for row in rows)
    assert all(row["reference_trade_date"] < "2026-01-05" for row in rows)
