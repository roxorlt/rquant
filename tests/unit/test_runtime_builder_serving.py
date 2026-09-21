from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError

from rquant.runtime_builder_serving import (
    DEFAULT_OPTIONAL_SOURCE_DATASETS,
    ServingReferenceSlowEvidence,
    ServingRuntimeSettings,
    ServingRuntimeSnapshot,
    serving_publisher_builder,
)
from rquant.runtime_contracts import canonical_sha256
from rquant.runtime_service_control import RuntimeServicePlane
from rquant.runtime_service_entrypoint import RuntimeServiceKind, RuntimeServiceManifest
from rquant.runtime_serving_authority import ServingSourceAuthorityPublisher
from rquant.runtime_serving_snapshot import (
    LAB_JOBS_DATASET_ID,
    PAPER_ACCOUNTS_DATASET_ID,
    PROMOTIONS_DATASET_ID,
    REFERENCE_SLOW_AUTHORITY_DATASET_ID,
    REFERENCE_SLOW_CONTRACT_DATASET_ID,
    REFERENCE_SLOW_DATASET_ID,
    RUNTIME_HEALTH_DATASET_ID,
    SIGNALS_DATASET_ID,
    LabJobsPayload,
    PaperAccountsPayload,
    PromotionsPayload,
    ReferenceSlowPayload,
    RuntimeHealthPayload,
    SignalDeliveryPayload,
    SourceReadResult,
)
from rquant.serving_contracts import FreshnessStatus, ServingDatasetWatermark
from rquant.serving_publisher import ServingPublisher, ServingReader
from rquant.serving_read_models import (
    SERVING_TABLE_SPECS,
    ServingReadModelInput,
    serving_physical_table_specs_fingerprint,
)

NOW = datetime(2026, 7, 31, 2, 10, tzinfo=UTC)
COMMIT = "a" * 40
SIGNAL_GENERATION = "b" * 64
PAPER_GENERATION = "c" * 64
REFERENCE_GENERATION = "d" * 64
REFERENCE_AUTHORITY_GENERATION = "e" * 64


def _reference_evidence(*, available_at: datetime = NOW) -> ServingReferenceSlowEvidence:
    return ServingReferenceSlowEvidence(
        reference_generation_id=REFERENCE_GENERATION,
        revision=1,
        price_basis="raw_session",
        adjustment_basis="tushare_adj_factor",
        available_at=available_at,
    )


def _watermark(
    dataset_id: str,
    generation_id: str,
    *,
    sequence: int,
    status: FreshnessStatus = FreshnessStatus.FRESH,
    reason: str | None = None,
    published_at: datetime = NOW,
) -> ServingDatasetWatermark:
    return ServingDatasetWatermark(
        dataset_id=dataset_id,
        generation_id=generation_id,
        event_time=published_at - timedelta(seconds=1),
        published_at=published_at,
        sequence=sequence,
        status=status,
        reason=reason,
    )


def _snapshot(
    *,
    observed_at: datetime = NOW,
    paper_status: FreshnessStatus = FreshnessStatus.FRESH,
    paper_reason: str | None = None,
) -> ServingRuntimeSnapshot:
    reference = _reference_evidence(available_at=observed_at)
    return ServingRuntimeSnapshot(
        read_model=ServingReadModelInput(observed_at=observed_at),
        reference_slow=reference,
        watermarks=(
            _watermark("signal_bus", SIGNAL_GENERATION, sequence=7),
            _watermark(
                "paper",
                PAPER_GENERATION,
                sequence=3,
                status=paper_status,
                reason=paper_reason,
            ),
            _watermark(
                REFERENCE_SLOW_AUTHORITY_DATASET_ID,
                REFERENCE_AUTHORITY_GENERATION,
                sequence=1,
                published_at=observed_at,
            ),
            _watermark(
                REFERENCE_SLOW_DATASET_ID,
                reference.reference_generation_id,
                sequence=reference.revision,
                published_at=reference.available_at,
            ),
            _watermark(
                REFERENCE_SLOW_CONTRACT_DATASET_ID,
                reference.contract_generation_id,
                sequence=reference.revision,
                published_at=reference.available_at,
            ),
        ),
        source_generations={
            "signal_bus": SIGNAL_GENERATION,
            "paper": PAPER_GENERATION,
            REFERENCE_SLOW_AUTHORITY_DATASET_ID: REFERENCE_AUTHORITY_GENERATION,
            REFERENCE_SLOW_DATASET_ID: reference.reference_generation_id,
            REFERENCE_SLOW_CONTRACT_DATASET_ID: reference.contract_generation_id,
        },
    )


def _manifest(
    tmp_path: Path,
    *,
    plane: RuntimeServicePlane = RuntimeServicePlane.SERVING,
    kind: RuntimeServiceKind = RuntimeServiceKind.SERVING_PUBLISHER,
    settings: dict[str, object] | None = None,
) -> RuntimeServiceManifest:
    return RuntimeServiceManifest(
        service_id="serving.publisher",
        service_kind=kind,
        plane=plane,
        interval_seconds=15,
        stale_after_seconds=60,
        producer_commit=COMMIT,
        settings=settings
        or {
            "serving_root": str(tmp_path / "serving"),
            "schema_version": 3,
        },
    )


def _authority_result(
    dataset_id: str,
    payload: object,
    *,
    sequence: int = 1,
    published_at: datetime = NOW - timedelta(seconds=1),
) -> SourceReadResult:
    values: dict[str, object] = {
        "dataset_id": dataset_id,
        "sequence": sequence,
        "event_time": published_at - timedelta(seconds=1),
        "published_at": published_at,
        "status": FreshnessStatus.FRESH,
        "reason": None,
        "payload": payload,
    }
    values["generation_id"] = canonical_sha256(values)
    return SourceReadResult.model_validate(values)


def _authority_settings(
    tmp_path: Path,
    *,
    unpublished: frozenset[str] = frozenset(),
) -> tuple[dict[str, object], dict[str, Path]]:
    """The six owner authorities, minus any the caller says never published.

    An authority root that was never written is exactly what the host shows for the two
    research datasets: the settings still name it, and the read is what finds nothing.
    """

    payloads = {
        SIGNALS_DATASET_ID: SignalDeliveryPayload(),
        PAPER_ACCOUNTS_DATASET_ID: PaperAccountsPayload(),
        RUNTIME_HEALTH_DATASET_ID: RuntimeHealthPayload(),
        LAB_JOBS_DATASET_ID: LabJobsPayload(),
        PROMOTIONS_DATASET_ID: PromotionsPayload(),
        REFERENCE_SLOW_AUTHORITY_DATASET_ID: ReferenceSlowPayload(
            **_reference_evidence(available_at=NOW - timedelta(seconds=1)).model_dump()
        ),
    }
    roots = {dataset_id: tmp_path / "authorities" / dataset_id for dataset_id in payloads}
    (tmp_path / "authorities").mkdir(parents=True)
    for dataset_id, payload in payloads.items():
        if dataset_id in unpublished:
            continue
        ServingSourceAuthorityPublisher(
            root=roots[dataset_id],
            producer_commit=COMMIT,
            dataset_id=dataset_id,
            payload_kind=payload.payload_kind,
            clock=lambda: NOW,
        ).publish(_authority_result(dataset_id, payload))
    return (
        {
            "serving_root": str(tmp_path / "serving"),
            "schema_version": 3,
            "source_authorities": [
                {"dataset_id": dataset_id, "root": str(root)} for dataset_id, root in roots.items()
            ],
        },
        roots,
    )


def test_builder_publishes_snapshot_and_maps_runtime_result(tmp_path: Path) -> None:
    snapshot = _snapshot(
        paper_status=FreshnessStatus.DEGRADED,
        paper_reason="paper snapshot delayed",
    )
    loader_calls: list[datetime] = []

    def load_snapshot(as_of: datetime) -> ServingRuntimeSnapshot:
        loader_calls.append(as_of)
        return snapshot

    step = serving_publisher_builder(
        snapshot_loader=load_snapshot,
        clock=lambda: NOW,
    )(_manifest(tmp_path))

    result = step()
    publisher = ServingPublisher(
        tmp_path / "serving",
        producer_commit=COMMIT,
        schema_version=3,
        table_specs=SERVING_TABLE_SPECS,
    )

    assert loader_calls == [NOW]
    assert result.input_sequence == 7
    assert result.output_sequence == 7
    assert result.processed_count == 1
    assert result.backlog_count == 0
    assert result.degraded_reasons == ("serving:paper:degraded:paper snapshot delayed",)
    assert result.source_generations["paper"] == PAPER_GENERATION
    assert result.source_generations["signal_bus"] == SIGNAL_GENERATION
    assert len(result.source_generations["serving_generation"]) == 64
    assert (
        publisher.current_manifest().generation_id
        == (result.source_generations["serving_generation"])
    )
    assert publisher.current_manifest().row_counts["serving_status"] == 1


def test_builder_acknowledges_schema_only_after_serving_generation_is_published(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, object]] = []

    class Acknowledger:
        def acknowledge_published_generation(self, **kwargs: object) -> None:
            assert (tmp_path / "serving" / "current.json").is_file()
            calls.append(dict(kwargs))

    monkeypatch.setattr(
        "rquant.runtime_builder_serving.current_runtime_schema_consumer_acknowledgers",
        lambda **_kwargs: (Acknowledger(),),
    )
    step = serving_publisher_builder(
        snapshot_loader=lambda _as_of: _snapshot(),
        clock=lambda: NOW,
    )(_manifest(tmp_path))

    result = step()

    assert calls == [
        {
            "serving_generation_id": result.source_generations["serving_generation"],
            "serving_physical_schema_fingerprint": (serving_physical_table_specs_fingerprint()),
            "observed_at": NOW,
        }
    ]


def test_builder_does_not_acknowledge_when_serving_publish_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[object] = []
    monkeypatch.setattr(
        "rquant.runtime_builder_serving.current_runtime_schema_consumer_acknowledgers",
        lambda **_kwargs: calls.append("resolved") or (),
    )
    monkeypatch.setattr(
        "rquant.runtime_builder_serving.ServingPublisher.publish_generation",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("publish failed")),
    )
    step = serving_publisher_builder(
        snapshot_loader=lambda _as_of: _snapshot(),
        clock=lambda: NOW,
    )(_manifest(tmp_path))

    with pytest.raises(RuntimeError, match="publish failed"):
        step()

    assert calls == []


def test_repeated_identical_snapshot_is_idempotent_without_extra_generation(
    tmp_path: Path,
) -> None:
    snapshot = _snapshot()
    step = serving_publisher_builder(
        snapshot_loader=lambda _as_of: snapshot,
        clock=lambda: NOW + timedelta(minutes=5),
    )(_manifest(tmp_path))

    first = step()
    first_paths = tuple((tmp_path / "serving" / "generations").iterdir())
    second = step()
    second_paths = tuple((tmp_path / "serving" / "generations").iterdir())

    # Same six source generations, so the second iteration selects the generation that
    # is already current and never opens a DuckDB file to build another (#271).
    assert first.generation_published is True
    assert second.generation_published is False
    assert second == first.model_copy(update={"generation_published": False})
    assert second.processed_count == 1
    assert len(first_paths) == 1
    assert second_paths == first_paths


def test_step_rejects_snapshot_evidence_after_clock(tmp_path: Path) -> None:
    future = NOW + timedelta(seconds=1)
    snapshot = _snapshot(observed_at=future)
    step = serving_publisher_builder(
        snapshot_loader=lambda _as_of: snapshot,
        clock=lambda: NOW,
    )(_manifest(tmp_path))

    with pytest.raises(ValueError, match="future evidence"):
        step()
    assert not (tmp_path / "serving" / "current.json").exists()


def test_builder_rejects_invalid_kind_plane_settings_and_snapshot_binding(
    tmp_path: Path,
) -> None:
    builder = serving_publisher_builder(
        snapshot_loader=lambda _as_of: _snapshot(),
        clock=lambda: NOW,
    )

    with pytest.raises(ValueError, match="kind"):
        builder(_manifest(tmp_path, kind=RuntimeServiceKind.NOTIFIER))
    with pytest.raises(ValueError, match="serving plane"):
        builder(_manifest(tmp_path, plane=RuntimeServicePlane.LIVE))

    relative_payload = _manifest(tmp_path).model_dump(mode="json")
    relative_payload["settings"]["serving_root"] = "relative/serving"
    with pytest.raises(ValidationError, match="absolute"):
        builder(RuntimeServiceManifest.model_validate(relative_payload))

    bool_schema_payload = _manifest(tmp_path).model_dump(mode="json")
    bool_schema_payload["settings"]["schema_version"] = True
    with pytest.raises(ValidationError, match="schema_version"):
        builder(RuntimeServiceManifest.model_validate(bool_schema_payload))

    with pytest.raises(ValidationError, match="exactly one watermark"):
        ServingRuntimeSnapshot(
            read_model=ServingReadModelInput(observed_at=NOW),
            reference_slow=_reference_evidence(),
            watermarks=(),
            source_generations={"signal_bus": SIGNAL_GENERATION},
        )


def test_default_builder_reads_five_dynamic_owner_authorities(tmp_path: Path) -> None:
    settings, roots = _authority_settings(tmp_path)
    step = serving_publisher_builder(snapshot_loader=None, clock=lambda: NOW)(
        _manifest(tmp_path, settings=settings)
    )

    first = step()
    updated = _authority_result(
        SIGNALS_DATASET_ID,
        SignalDeliveryPayload(),
        sequence=2,
        published_at=NOW,
    )
    ServingSourceAuthorityPublisher(
        root=roots[SIGNALS_DATASET_ID],
        producer_commit=COMMIT,
        dataset_id=SIGNALS_DATASET_ID,
        payload_kind="signal_delivery",
        clock=lambda: NOW,
    ).publish(updated)
    second = step()

    assert first.source_generations[SIGNALS_DATASET_ID] != updated.generation_id
    assert second.source_generations[SIGNALS_DATASET_ID] == updated.generation_id
    assert second.input_sequence == 2
    assert second.output_sequence == 2


def test_the_research_sources_are_optional_by_default_and_reference_slow_never_is() -> None:
    """What a manifest written before #283 gets, and what no manifest may ask for."""

    inherited = ServingRuntimeSettings(serving_root=Path("/srv/serving"), schema_version=3)
    assert inherited.optional_source_datasets == tuple(sorted(DEFAULT_OPTIONAL_SOURCE_DATASETS))

    with pytest.raises(ValidationError, match="reference_slow_authority can never be"):
        ServingRuntimeSettings(
            serving_root=Path("/srv/serving"),
            schema_version=3,
            optional_source_datasets=(LAB_JOBS_DATASET_ID, REFERENCE_SLOW_AUTHORITY_DATASET_ID),
        )

    with pytest.raises(ValidationError, match="not owner datasets"):
        ServingRuntimeSettings(
            serving_root=Path("/srv/serving"),
            schema_version=3,
            optional_source_datasets=(REFERENCE_SLOW_DATASET_ID,),
        )

    with pytest.raises(ValidationError, match="duplicate"):
        ServingRuntimeSettings(
            serving_root=Path("/srv/serving"),
            schema_version=3,
            optional_source_datasets=(LAB_JOBS_DATASET_ID, LAB_JOBS_DATASET_ID),
        )

    #: a profile may still tighten the rule back to nothing, which is what the day the
    #: research plane publishes looks like
    tightened = ServingRuntimeSettings(
        serving_root=Path("/srv/serving"),
        schema_version=3,
        optional_source_datasets=(),
    )
    assert tightened.optional_source_datasets == ()


def test_serving_publishes_while_the_research_authorities_have_never_published(
    tmp_path: Path,
) -> None:
    """#283 through the real builder: four sources answer, two were never written.

    This is the host as it stands -- `research/serving-authorities/lab-jobs` and
    `.../promotions` hold no generation at all -- and before this the six fail-closed
    reads meant serving cut nothing, so criterion (3b) could not be read on a day the
    signal chain worked.
    """

    absent = frozenset({LAB_JOBS_DATASET_ID, PROMOTIONS_DATASET_ID})
    settings, roots = _authority_settings(tmp_path, unpublished=absent)
    assert not roots[LAB_JOBS_DATASET_ID].exists()
    step = serving_publisher_builder(snapshot_loader=None, clock=lambda: NOW)(
        _manifest(tmp_path, settings=settings)
    )

    result = step()

    assert result.generation_published is True
    assert (tmp_path / "serving" / "current.json").is_file()
    assert sorted(result.degraded_reasons) == [
        "serving:lab_jobs:unavailable:ServingSourceAuthorityUnavailableError: "
        "current authority is unavailable",
        "serving:promotions:unavailable:ServingSourceAuthorityUnavailableError: "
        "current authority is unavailable",
    ]
    #: and the four that did answer are still bound to their own evidence -- only the
    #: two named above were degraded
    assert result.source_generations[REFERENCE_SLOW_DATASET_ID] == REFERENCE_GENERATION
    assert result.source_generations[LAB_JOBS_DATASET_ID] != (
        result.source_generations[PROMOTIONS_DATASET_ID]
    )


def test_the_manifest_decides_which_sources_are_optional_not_the_default(
    tmp_path: Path,
) -> None:
    """A profile that shortens the list back to `[]` really tightens the rule.

    The builder's default and what `runtime_production_profile` writes into the manifest
    are the same two datasets today, so every other case here cannot tell the two paths
    apart: wiring `:313` back to the constant leaves all of them green. That matters
    because writing the list into the manifest is the whole reason the profile spells it
    out -- the day the research plane publishes, shortening it to `[]` is meant to be a
    profile change with its own fingerprint, and it has to actually bite.

    So this one asks for the opposite of the default on a world where `lab_jobs` never
    published: with `optional_source_datasets: []` the round is refused, exactly as it
    was before #283.
    """

    settings, _roots = _authority_settings(
        tmp_path,
        unpublished=frozenset({LAB_JOBS_DATASET_ID}),
    )
    settings["optional_source_datasets"] = []
    step = serving_publisher_builder(snapshot_loader=None, clock=lambda: NOW)(
        _manifest(tmp_path, settings=settings)
    )

    with pytest.raises(RuntimeError, match="lab_jobs reader failed"):
        step()

    assert not (tmp_path / "serving" / "current.json").exists()


@pytest.mark.parametrize(
    "dataset_id",
    [SIGNALS_DATASET_ID, RUNTIME_HEALTH_DATASET_ID, REFERENCE_SLOW_AUTHORITY_DATASET_ID],
)
def test_a_source_outside_the_optional_set_stops_the_whole_round(
    tmp_path: Path,
    dataset_id: str,
) -> None:
    """The negative half of #283, at the builder: only the two research sources degrade."""

    settings, _roots = _authority_settings(tmp_path, unpublished=frozenset({dataset_id}))
    step = serving_publisher_builder(snapshot_loader=None, clock=lambda: NOW)(
        _manifest(tmp_path, settings=settings)
    )

    with pytest.raises(RuntimeError, match=f"{dataset_id} reader failed"):
        step()

    assert not (tmp_path / "serving" / "current.json").exists()


def test_sixty_idle_steps_with_the_research_plane_absent_leave_one_generation(
    tmp_path: Path,
) -> None:
    """The #283 degrade must not undo #271.

    An unavailable source used to stamp `as_of` into its generation id and its watermark,
    so a source that stayed away handed the publisher a different input on every
    iteration -- `_generation_already_current` compares both for equality -- and serving
    would have rebuilt, hashed, fsynced and re-pointed `serving.duckdb` every thirty
    seconds for as long as the research plane did not run. The evidence is the directory
    itself: thirty minutes of iterations, byte for byte unchanged.
    """

    from tests.runtime_readonly_sandbox import tree_state

    absent = frozenset({LAB_JOBS_DATASET_ID, PROMOTIONS_DATASET_ID})
    settings, _roots = _authority_settings(tmp_path, unpublished=absent)
    clock = [NOW]
    step = serving_publisher_builder(snapshot_loader=None, clock=lambda: clock[0])(
        _manifest(tmp_path, settings=settings)
    )
    serving_root = tmp_path / "serving"

    first = step()
    assert first.generation_published is True
    settled = tree_state(serving_root)

    published = []
    for iteration in range(1, 61):
        clock[0] = NOW + timedelta(seconds=30 * iteration)
        published.append(step().generation_published)

    assert published == [False] * 60
    assert tree_state(serving_root) == settled, "serving rewrote itself while a source was absent"


def test_reference_revision_publishes_new_generation_and_keeps_old_readable(
    tmp_path: Path,
) -> None:
    settings, roots = _authority_settings(tmp_path)
    step = serving_publisher_builder(snapshot_loader=None, clock=lambda: NOW)(
        _manifest(tmp_path, settings=settings)
    )
    first = step()
    first_generation = first.source_generations["serving_generation"]
    revised_reference = ReferenceSlowPayload(
        reference_generation_id="f" * 64,
        revision=2,
        price_basis="raw_session",
        adjustment_basis="tushare_adj_factor",
        available_at=NOW,
    )
    ServingSourceAuthorityPublisher(
        root=roots[REFERENCE_SLOW_AUTHORITY_DATASET_ID],
        producer_commit=COMMIT,
        dataset_id=REFERENCE_SLOW_AUTHORITY_DATASET_ID,
        payload_kind="reference_slow",
        clock=lambda: NOW,
    ).publish(
        _authority_result(
            REFERENCE_SLOW_AUTHORITY_DATASET_ID,
            revised_reference,
            sequence=2,
            published_at=NOW,
        )
    )

    second = step()

    assert second.source_generations[REFERENCE_SLOW_DATASET_ID] == "f" * 64
    assert second.source_generations["serving_generation"] != first_generation
    reader = ServingReader(tmp_path / "serving")
    with reader.acquire_historical_generation(first_generation) as acquired:
        assert acquired.manifest.generation_id == first_generation
        assert acquired.manifest.source_generations[REFERENCE_SLOW_DATASET_ID] == (
            REFERENCE_GENERATION
        )


def test_default_builder_requires_exact_owner_authority_set(tmp_path: Path) -> None:
    settings, _roots = _authority_settings(tmp_path)
    authorities = list(settings["source_authorities"])
    settings["source_authorities"] = authorities[:-1]

    with pytest.raises(ValidationError, match="exactly.*six|missing"):
        serving_publisher_builder(snapshot_loader=None, clock=lambda: NOW)(
            _manifest(tmp_path, settings=settings)
        )

    settings, _roots = _authority_settings(tmp_path / "duplicate")
    authorities = list(settings["source_authorities"])
    settings["source_authorities"] = [*authorities, authorities[0]]
    with pytest.raises(ValidationError, match="duplicate|exactly.*six"):
        serving_publisher_builder(snapshot_loader=None, clock=lambda: NOW)(
            _manifest(tmp_path, settings=settings)
        )


def test_injected_loader_cannot_mix_with_owner_authorities(tmp_path: Path) -> None:
    settings, _roots = _authority_settings(tmp_path)

    with pytest.raises(ValueError, match="cannot be combined"):
        serving_publisher_builder(
            snapshot_loader=lambda _as_of: _snapshot(),
            clock=lambda: NOW,
        )(_manifest(tmp_path, settings=settings))


def test_sixty_idle_steps_of_the_built_role_leave_one_generation(tmp_path: Path) -> None:
    """#271, through the role's own step: the clock moves, the six sources do not.

    This is what production looks like once runtime health and lab jobs stop restating
    themselves: the assembler still stamps `observed_at = as_of` into the read model, and
    the step still hands it to the publisher as `built_at`. Without a gate in front of the
    build, that alone was a whole new `serving.duckdb` generation every thirty seconds --
    built, verified, hashed, fsynced, and selected.
    """

    base = _snapshot()
    clock = [NOW]

    def loader(as_of: datetime) -> ServingRuntimeSnapshot:
        return base.model_copy(
            update={"read_model": ServingReadModelInput(observed_at=as_of)}
        )

    step = serving_publisher_builder(snapshot_loader=loader, clock=lambda: clock[0])(
        _manifest(tmp_path)
    )
    generations = tmp_path / "serving" / "generations"
    current = tmp_path / "serving" / "current.json"

    first = step()
    assert first.generation_published is True
    settled = {path.name for path in generations.iterdir()}
    pointer_bytes = current.read_bytes()

    published = []
    for iteration in range(1, 61):
        clock[0] = NOW + timedelta(seconds=30 * iteration)
        published.append(step().generation_published)

    assert published == [False] * 60
    assert {path.name for path in generations.iterdir()} == settled
    assert current.read_bytes() == pointer_bytes
