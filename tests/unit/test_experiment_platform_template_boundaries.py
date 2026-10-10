"""Direct original source/descriptor boundaries; no host worker or production IO."""

from __future__ import annotations

from pathlib import Path
from uuid import UUID

import pytest

from tests.unit.test_experiment_platform_templates import (
    configure_template,
    ready_template,
)
from tests.unit.test_experiment_platform_templates import (
    preparation as preparation,
)


def test_c5t05_reserved_original_input_rejects_public_or_symlink_parent_before_read(
    preparation, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import rquant.experiment_platform_commands as commands

    store, producer, _, binding, _, record = ready_template(preparation, tmp_path)
    reservation = store.preparation_reservation("alice", record.family_id, 0)
    prepared = store.preparation("alice", record.family_id, 0).prepared
    version = prepared.catalog.versions[0]
    recovered, publication = binding._recover_input(producer, record, version, reservation)
    assert recovered == prepared.frozen
    assert publication.input_hash == prepared.published.input_hash
    directory = Path(reservation.source_path).parent
    called = []
    original = commands._input_digest

    def observed(path: Path):
        called.append(path)
        return original(path)

    monkeypatch.setattr(commands, "_input_digest", observed)
    directory.chmod(0o755)
    try:
        with pytest.raises(PermissionError, match="directory"):
            binding._recover_input(producer, record, version, reservation)
        assert called == [], "public directory was used before private admission"
    finally:
        directory.chmod(0o700)
    link = producer.input_root / ("f" * 32)
    link.symlink_to(directory, target_is_directory=True)
    with pytest.raises(PermissionError, match="directory"):
        binding._recover_input(
            producer,
            record,
            version,
            reservation.model_copy(update={"source_path": str(link / "input.duckdb")}),
        )
    assert called == []


def test_c5t04_original_template_manifest_wire_budget_is_checked(preparation, tmp_path) -> None:
    from rquant.lab_worker import build_builtin_shard_runtime_manifest

    store, producer, _, binding, _, record = ready_template(preparation, tmp_path)
    prepared = store.preparation("alice", record.family_id, 0).prepared
    common = {
        "catalog_path": tmp_path / "metadata.duckdb",
        "snapshot_root": producer.input_root / "worker-copies",
        "research_lake_root": producer.lake_root,
    }
    original = build_builtin_shard_runtime_manifest(**common, forbidden_paths=())
    accepted = binding.directory.manifest_for_spec(prepared.spec, original)
    assert len(accepted.registry.configuration_json.encode()) <= 512 * 1024
    paths = tuple(
        tmp_path / "blocked" / str(index) / ("a" * 200) / ("b" * 200) / ("c" * 200)
        for index in range(850)
    )
    oversized = build_builtin_shard_runtime_manifest(**common, forbidden_paths=paths)
    assert len(oversized.registry.configuration_json.encode()) > 512 * 1024
    with pytest.raises(ValueError, match="wire budget"):
        binding.directory.manifest_for_spec(prepared.spec, oversized)
    assert len(store.template_slots("alice", record.family_id)) == 4
    assert len(store.registry.list_family_attempts(record.family_id)) == 4


def test_c5t02_c5t05_lost_input_receipt_recovers_original_bound_bytes(
    preparation, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from tests.unit.test_experiment_platform import NOW
    from tests.unit.test_strategy_authoring import draft

    store, producer, _, binding, request = configure_template(preparation, tmp_path)
    record = store.begin_request(
        owner="alice",
        request_id=UUID(int=910),
        body_hash="c" * 64,
        request=request,
        registered_at=NOW,
        template_baseline=binding.baseline(owner="alice", request=request),
    )
    original_save = store.save_preparation
    lost = []

    def interrupted(receipt):
        lost.append(receipt)
        raise RuntimeError("original publication complete, receipt not persisted")

    monkeypatch.setattr(store, "save_preparation", interrupted)
    with pytest.raises(RuntimeError, match="receipt not persisted"):
        producer(record)
    reservation = store.preparation_reservation("alice", record.family_id, 0)
    first = lost[0]
    assert first.source_path == reservation.source_path
    assert store.preparation("alice", record.family_id, 0) is None
    slots = store.template_slots("alice", record.family_id)
    assert len(slots) == 4 and all(s.state == "saved" for s in slots)
    assert store.registry.list_family_attempts(record.family_id) == ()
    assert len(preparation[4]) == 1

    # A newer public head cannot change an already admitted original version.
    baseline = record.template_baseline.version
    binding.original.save(
        draft(strategy_id=baseline.strategy_id, expected_head=baseline.head).model_copy(
            update={"rules": baseline.rules, "name": "后续版本"}
        ),
        owner_id="alice",
        catalog=binding._catalog("alice"),
    )
    assert binding.original.get_current(baseline.strategy_id, owner_id="alice").head.version == 2
    with producer.metadata_store_factory() as metadata:
        sealed = metadata.get_dataset_snapshot_binding(
            first.prepared.published.identity.snapshot_id
        )
    manifest = producer.lake_root / sealed.manifest_relative_path
    hidden = manifest.with_name(manifest.name + ".temporarily-unavailable")
    manifest.rename(hidden)
    monkeypatch.setattr(store, "save_preparation", original_save)
    try:
        with pytest.raises(ValueError, match="binding manifest file missing"):
            producer(record)
        assert len(preparation[4]) == 1, "a broken original binding triggered another source read"
        assert store.preparation_reservation("alice", record.family_id, 0) == reservation
        assert store.template_slots("alice", record.family_id) == slots
        assert store.preparation("alice", record.family_id, 0) is None
        assert store.registry.list_family_attempts(record.family_id) == ()
    finally:
        hidden.rename(manifest)

    ready = producer(record)
    recovered = store.preparation("alice", record.family_id, 0)
    assert ready.state == "ready"
    assert recovered == first, "recovery changed original input, binding or file receipt"
    assert store.preparation_reservation("alice", record.family_id, 0) == reservation
    assert len(preparation[4]) == 2, "only the remaining unpublished inputs require a phase read"
    assert len(store.registry.list_family_attempts(record.family_id)) == 4
    assert store.template_slots("alice", record.family_id) == slots
    assert producer(ready) == ready and len(preparation[4]) == 2


@pytest.mark.parametrize("failure", ["outer_rows", "missing_minute"])
def test_c5t05_original_template_phase_rejects_unbounded_or_fake_minute_material(
    preparation, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    from rquant.runtime_contracts import canonical_sha256
    from rquant.strategy_template import StrategyTemplate
    from rquant.strategy_template_execution import TemplateEntryEvidence
    from rquant.strategy_template_source import StrategyTemplateSourceData, TemplateRawDay
    from tests.unit.test_experiment_platform import NOW
    from tests.unit.test_strategy_authoring import draft

    store, producer, profile, binding, request = configure_template(preparation, tmp_path)
    if failure == "missing_minute":
        original = binding.original.get_version(
            request.template.strategy_id, request.template.head.version, owner_id="alice"
        )
        saved = binding.original.save(
            draft(strategy_id=original.strategy_id, expected_head=original.head).model_copy(
                update={
                    "rules": StrategyTemplate.model_validate(
                        original.rules.model_dump(mode="python") | {"exit": {"exit_time": "14:30"}}
                    )
                }
            ),
            owner_id="alice",
            catalog=binding._catalog("alice"),
        )
        request = type(request).model_validate(
            request.model_dump(mode="python")
            | {"template": {"strategy_id": saved.strategy_id, "head": saved.head}}
        )
    else:
        actual_provider = binding.phase_provider

        def unbounded(read, version):
            actual_provider(read, version)
            whole = type(preparation[2]).model_validate(
                preparation[2].model_dump(mode="python")
                | {"sources": profile.sources, "material_hash": None}
            )
            return StrategyTemplateSourceData(
                owner_id="alice",
                catalog=binding._catalog("alice"),
                portfolio=whole,
                days=tuple(
                    TemplateRawDay(
                        trade_date=d.trade_date,
                        entry=TemplateEntryEvidence(
                            observed_at=d.ranking.observed_at,
                            source_hash=d.ranking.source_identity,
                            rows=tuple(
                                {"ts_code": i.ts_code, "is_st": False} for i in d.instruments
                            ),
                        ),
                    )
                    for d in whole.template.days
                ),
                material_hash=None,
            )

        binding.phase_provider = unbounded
    record = store.begin_request(
        owner="alice",
        request_id=UUID(int=920),
        body_hash=canonical_sha256(request),
        request=request,
        registered_at=NOW,
        template_baseline=binding.baseline(owner="alice", request=request),
    )
    error, reason = (
        (PermissionError, "unbounded rows") if failure == "outer_rows" else (ValueError, "minute")
    )
    with pytest.raises(error, match=reason):
        producer(record)
    assert len(preparation[4]) == 1
    read = preparation[4][0]
    assert read.phase == "search" and read.outer_grant_id is None
    assert read.window.end_date == request.protocol.validation_range.end_date
    assert len(store.template_slots("alice", record.family_id)) == 4
    assert all(s.state == "saved" for s in store.template_slots("alice", record.family_id))
    assert store.preparation_reservation("alice", record.family_id, 0) is None
    assert store.preparation("alice", record.family_id, 0) is None
    assert store.registry.list_family_attempts(record.family_id) == ()
    assert store.registry.list_pending_submissions() == ()


def test_c5t04_actual_sparse_file_capacity_is_rejected_before_read(tmp_path: Path) -> None:
    from rquant.experiment_platform import MAX_FAMILY_INPUT_BYTES
    from rquant.experiment_platform_commands import _input_digest

    path = tmp_path / "input.duckdb"
    with path.open("wb") as stream:
        stream.truncate(MAX_FAMILY_INPUT_BYTES + 1)
    path.chmod(0o600)
    assert path.stat().st_size == 512 * 1024 * 1024 + 1
    with pytest.raises(ValueError, match="512 MiB"):
        _input_digest(path)
