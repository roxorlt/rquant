from __future__ import annotations

import hashlib
import os
import stat
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from uuid import UUID, uuid4

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import rquant.lab_artifact_preview as preview_module
from rquant.lab_artifact_preview import (
    ArtifactCompleteTableBudget,
    ArtifactCompleteTables,
    ArtifactPreviewIntegrityError,
    ArtifactPreviewReader,
    ArtifactPreviewUnavailableError,
)
from rquant.lab_artifacts import (
    LabArtifactFileIdentity,
    LabArtifactIndexEvidence,
    LabJobArtifactFile,
    LabJobArtifactManifest,
    LabParquetIdentity,
    _complete_result_hash_payload,
    _frame_dtype_identities,
    _table_content_hash,
    canonical_json_bytes,
)
from rquant.lab_jobs import (
    ControlIntent,
    JobStatus,
    LabArtifactPreviewAuthority,
    LabJobReader,
    LabJobRecord,
    LabResultState,
)
from rquant.research_run_spec import (
    DatasetSnapshotIdentity,
    ExecutionCostSpec,
    FeatureContractIdentity,
    ResearchJobType,
    ResearchRunParameters,
    ResearchRunSpec,
    ResourceClass,
)

TABLE_NAMES = (
    "signals", "orders", "fills", "paper_queue", "account", "daily_valuations",
    "execution_profile", "replay_summary",
)
ALL_FILES = ("manifest.json", "SHA256SUMS", "metrics.json", "spec.json", "report.md",
    *(f"tables/{name}.parquet" for name in TABLE_NAMES))


class _SyntheticAuthorityReader(LabJobReader):
    """Typed authority boundary for isolated byte checks; no ledger or engine runs."""

    def __init__(self, authority: LabArtifactPreviewAuthority, path: Path) -> None:
        super().__init__(path)
        self.authority: LabArtifactPreviewAuthority | None = authority
        self.calls = 0
        self.on_refresh: Callable[[], None] | None = None

    def get_artifact_preview_authority(self, job_id: UUID) -> LabArtifactPreviewAuthority | None:
        self.calls += 1
        if self.calls == 2 and self.on_refresh is not None:
            self.on_refresh()
        assert self.authority is None or self.authority.job.job_id == job_id
        return self.authority


@dataclass
class _SealedFixture:
    root: Path
    bundle: Path
    manifest: LabJobArtifactManifest
    source: _SyntheticAuthorityReader
    payloads: dict[str, bytes]

    @property
    def authority(self) -> LabArtifactPreviewAuthority:
        assert self.source.authority is not None
        return self.source.authority

    def reader(self, **limits: int) -> ArtifactPreviewReader:
        return ArtifactPreviewReader(reader=self.source, artifact_root=self.root, **limits)

    def reindex_files(self) -> None:
        evidence = self.authority.evidence.model_copy(update={
            "file_identities": tuple(_identity(self.bundle, path) for path in sorted(self.payloads)),
        })
        self.source.authority = self.authority.model_copy(update={"evidence": evidence})

    def replace_bytes(self, relative_path: str, payload: bytes) -> None:
        path = self.bundle / relative_path
        path.chmod(0o600)
        path.write_bytes(payload)
        path.chmod(0o400)


def _identity(bundle: Path, relative_path: str) -> LabArtifactFileIdentity:
    observed = (bundle / relative_path).stat(follow_symlinks=False)
    return LabArtifactFileIdentity(relative_path=relative_path, device=observed.st_dev,
        inode=observed.st_ino, size=observed.st_size, mtime_ns=observed.st_mtime_ns,
        ctime_ns=observed.st_ctime_ns)


def _spec() -> ResearchRunSpec:
    return ResearchRunSpec(job_type=ResearchJobType.STRATEGY_REPLAY,
        parameters=ResearchRunParameters(strategy_name="n_shape", start_date=date(2026, 4, 1),
            end_date=date(2026, 7, 24)), code_sha="1" * 40,
        dataset_snapshot=DatasetSnapshotIdentity(snapshot_id="2" * 64, binding_hash="3" * 64,
            audit_run_id="4" * 64),
        feature_contract=FeatureContractIdentity(contract_id="intraday-core",
            contract_version="v1", contract_hash="5" * 64),
        execution_costs=ExecutionCostSpec(commission_bps=Decimal("2.5"), stamp_duty_bps=Decimal("5"),
            transfer_fee_bps=Decimal("0.1"), slippage_bps=Decimal("3")), random_seed=20260725,
        resource_class=ResourceClass.STANDARD, deadline=datetime(2026, 7, 26, tzinfo=UTC),
        research_status="comparable")


def _build_sealed(
    parent: Path, *, overrides: dict[str, bytes] | None = None,
    manifest_suffix: bytes = b"", declared_size_delta: int = 0,
    declared_rows_delta: int = 0,
) -> _SealedFixture:
    spec = _spec()
    job_id = uuid4()
    now = datetime(2026, 7, 25, tzinfo=UTC)
    root = parent / "artifacts"
    bundle = root / "sealed" / job_id.hex
    (bundle / "tables").mkdir(mode=0o700, parents=True)
    root.chmod(0o700)
    (root / "sealed").chmod(0o700)
    payloads = {"spec.json": spec.canonical_json().encode("utf-8"),
        "metrics.json": canonical_json_bytes({"diagnostic": True, "net_return": -0.12}),
        "report.md": b"# Synthetic frozen byte fixture\nNegative returns are preserved.\n"}
    metadata: dict[str, LabParquetIdentity] = {}
    frame = pd.DataFrame({"value": pd.Series([-2, 1], dtype="int64")})
    for name in TABLE_NAMES:
        sink = pa.BufferOutputStream()
        pq.write_table(pa.Table.from_pandas(frame, preserve_index=False), sink)
        relative_path = f"tables/{name}.parquet"
        payloads[relative_path] = sink.getvalue().to_pybytes()
        metadata[relative_path] = LabParquetIdentity(table_name=name,
            row_count=len(frame) + declared_rows_delta, columns=tuple(frame.columns),
            dtypes=tuple(str(dtype) for dtype in frame.dtypes),
            dtype_identities=_frame_dtype_identities(frame), content_sha256=_table_content_hash(frame))
    payloads.update(overrides or {})
    fixed_types = {"spec.json": "application/json", "metrics.json": "application/json",
        "report.md": "text/markdown; charset=utf-8"}
    files = tuple(LabJobArtifactFile(relative_path=path,
        media_type=fixed_types.get(path, "application/vnd.apache.parquet"),
        size=len(payload) + (declared_size_delta if path in metadata else 0),
        sha256=hashlib.sha256(payload).hexdigest(), parquet=metadata.get(path))
        for path, payload in sorted(payloads.items()))
    manifest_fields = dict(job_id=job_id, spec_hash=spec.spec_hash, plan_hash="6" * 64,
        adapter_id="synthetic-byte-fixture", adapter_version="1", result_contract_version="1",
        code_sha=spec.code_sha, dataset_snapshot=spec.dataset_snapshot, files=files)
    manifest = LabJobArtifactManifest(**manifest_fields, complete_result_hash=hashlib.sha256(
        canonical_json_bytes(_complete_result_hash_payload(**manifest_fields))).hexdigest())
    payloads["manifest.json"] = manifest.canonical_json_bytes() + manifest_suffix
    hashes = {entry.relative_path: entry.sha256 for entry in files}
    hashes["manifest.json"] = manifest.manifest_hash
    payloads["SHA256SUMS"] = "".join(f"{digest}  {path}\n" for path, digest in sorted(hashes.items())).encode("ascii")
    for path, payload in payloads.items():
        target = bundle / path
        target.write_bytes(payload)
        target.chmod(0o400)
    (bundle / "tables").chmod(0o500)
    bundle.chmod(0o500)
    observed = bundle.stat()
    evidence = LabArtifactIndexEvidence(job_id=job_id, sealed_path=bundle,
        manifest_hash=manifest.manifest_hash, complete_result_hash=manifest.complete_result_hash,
        bundle_device=observed.st_dev, bundle_inode=observed.st_ino,
        file_identities=tuple(_identity(bundle, path) for path in sorted(payloads)), indexed_at=now)
    job = LabJobRecord(job_id=job_id, spec=spec, spec_hash=spec.spec_hash, job_type=spec.job_type,
        resource_class=spec.resource_class, deadline=spec.deadline, status=JobStatus.SUCCEEDED,
        control_intent=ControlIntent.NONE, version=2, attempt_count=1, max_attempts=1,
        recoverable=False, result_contract_version="1", requires_complete_result=True,
        result_state=LabResultState.SEALED, created_at=now, updated_at=now)
    source = _SyntheticAuthorityReader(LabArtifactPreviewAuthority(job=job, evidence=evidence),
        parent / "never-opened.sqlite3")
    return _SealedFixture(root=root, bundle=bundle, manifest=manifest, source=source, payloads=payloads)


@pytest.fixture
def frozen_factory(tmp_path: Path) -> Iterator[Callable[..., _SealedFixture]]:
    counter = 0

    def build(**kwargs: object) -> _SealedFixture:
        nonlocal counter
        counter += 1
        parent = tmp_path / f"fixture-{counter}"
        parent.mkdir(mode=0o700)
        return _build_sealed(parent, **kwargs)

    yield build
    # Only test-owned paths are restored for pytest's normal cleanup.
    for parent, directories, files in os.walk(tmp_path, followlinks=False):
        Path(parent).chmod(0o700)
        for name in directories:
            path = Path(parent) / name
            if not path.is_symlink():
                path.chmod(0o700)
        for name in files:
            path = Path(parent) / name
            if not path.is_symlink():
                path.chmod(0o600)


@pytest.fixture(autouse=True)
def _no_open_descriptor_leaks(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    live: set[int] = set()
    before_count = len(tuple(Path("/dev/fd").iterdir()))
    original_open, original_close = os.open, os.close

    def track_open(*args: object, **kwargs: object) -> int:
        descriptor = original_open(*args, **kwargs)
        live.add(descriptor)
        return descriptor

    def track_close(descriptor: int) -> None:
        original_close(descriptor)
        live.discard(descriptor)

    monkeypatch.setattr(os, "open", track_open)
    monkeypatch.setattr(os, "close", track_close)
    yield
    assert not live, f"reader leaked descriptors: {sorted(live)}"
    assert len(tuple(Path("/dev/fd").iterdir())) == before_count


def _read(frozen: _SealedFixture, *, reader: ArtifactPreviewReader | None = None,
    names: tuple[str, ...] = TABLE_NAMES, budget: ArtifactCompleteTableBudget | None = None) -> preview_module.ArtifactCompleteByteEvidence:
    return (reader or frozen.reader()).read_complete_byte_evidence(frozen.authority.job.job_id,
        table_names=names, budget=budget or ArtifactCompleteTableBudget())


def test_synthetic_fixture_is_accepted_by_existing_full_read(frozen_factory: Callable[..., _SealedFixture]) -> None:
    frozen = frozen_factory()
    full = frozen.reader().read_complete_tables(frozen.authority.job.job_id,
        table_names=TABLE_NAMES, budget=ArtifactCompleteTableBudget())
    assert isinstance(full, ArtifactCompleteTables)
    assert len(full.tables) == 8
    assert all(table.rows == ((-2,), (1,)) for table in full.tables)


def test_byte_evidence_keeps_typed_identities_and_exact_charges_without_row_decode(
    frozen_factory: Callable[..., _SealedFixture], monkeypatch: pytest.MonkeyPatch,
) -> None:
    frozen = frozen_factory()

    def forbidden_decode(*args: object, **kwargs: object) -> object:
        pytest.fail("complete byte evidence must not decode Parquet rows")

    monkeypatch.setattr(ArtifactPreviewReader, "_read_parquet_complete_rows", forbidden_decode)
    monkeypatch.setattr(ArtifactPreviewReader, "_read_parquet_preview_rows", forbidden_decode)
    evidence = _read(frozen)
    assert isinstance(evidence, preview_module.ArtifactCompleteByteEvidence)
    assert evidence.authority == frozen.authority and evidence.manifest == frozen.manifest
    assert evidence.spec == frozen.authority.job.spec
    assert evidence.metrics == {"diagnostic": True, "net_return": {"$float": (-0.12).hex()}}
    assert evidence.file_identities == frozen.authority.evidence.file_identities
    assert evidence.tables == tuple(entry.parquet for entry in frozen.manifest.files if entry.parquet)
    assert evidence.encoded_table_bytes == sum(len(value) for path, value in frozen.payloads.items() if path.startswith("tables/"))
    assert evidence.verified_bundle_bytes == sum(len(value) for value in frozen.payloads.values())
    assert frozen.source.calls == 2
    assert "rows" not in evidence.model_dump() and "report_markdown" not in evidence.model_dump()


@pytest.mark.parametrize("relative_path", ALL_FILES)
def test_every_original_file_is_fully_hash_verified(
    frozen_factory: Callable[..., _SealedFixture], relative_path: str,
) -> None:
    frozen = frozen_factory()
    payload = bytearray(frozen.payloads[relative_path])
    payload[len(payload) // 2] ^= 1
    frozen.replace_bytes(relative_path, bytes(payload))
    frozen.reindex_files()
    with pytest.raises(ArtifactPreviewIntegrityError):
        _read(frozen)


def test_stream_hash_checks_middle_bytes_beyond_first_chunk(frozen_factory: Callable[..., _SealedFixture]) -> None:
    frozen = frozen_factory(overrides={"report.md": b"x" * (2 * 1024 * 1024 + 128)})
    payload = bytearray(frozen.payloads["report.md"])
    payload[1024 * 1024 + 17] = ord("y")
    frozen.replace_bytes("report.md", bytes(payload))
    frozen.reindex_files()
    with pytest.raises(ArtifactPreviewIntegrityError, match="hash conflicts: report.md"):
        _read(frozen)


@pytest.mark.parametrize("names", [(), ("signals", "signals"), tuple(f"t{index}" for index in range(9))])
def test_selection_rejects_invalid_request_before_authority_lookup(
    frozen_factory: Callable[..., _SealedFixture], names: tuple[str, ...],
) -> None:
    frozen = frozen_factory()
    with pytest.raises(ValueError):
        _read(frozen, names=names)
    assert frozen.source.calls == 0


def test_missing_authority_prevents_any_filesystem_read(frozen_factory: Callable[..., _SealedFixture]) -> None:
    frozen = frozen_factory()
    job_id = frozen.authority.job.job_id
    frozen.source.authority = None
    with pytest.raises(ArtifactPreviewUnavailableError):
        frozen.reader().read_complete_byte_evidence(job_id, table_names=TABLE_NAMES,
            budget=ArtifactCompleteTableBudget())


@pytest.mark.parametrize("kind", ["subset", "foreign", "missing_identity", "duplicate_identity", "extra_identity", "extra_file"])
def test_exact_inventory_is_required(frozen_factory: Callable[..., _SealedFixture], kind: str) -> None:
    frozen = frozen_factory()
    names = TABLE_NAMES
    identities = frozen.authority.evidence.file_identities
    if kind == "subset":
        names = TABLE_NAMES[:-1]
    elif kind == "foreign":
        names = (*TABLE_NAMES[:-1], "foreign")
    elif kind == "missing_identity":
        identities = identities[:-1]
    elif kind == "duplicate_identity":
        identities = (*identities, identities[-1])
    elif kind == "extra_identity":
        identities = (*identities, identities[-1].model_copy(update={"relative_path": "extra.txt"}))
    elif kind == "extra_file":
        frozen.bundle.chmod(0o700)
        (frozen.bundle / "extra.txt").write_bytes(b"unindexed")
        frozen.bundle.chmod(0o500)
    frozen.source.authority = frozen.authority.model_copy(update={
        "evidence": frozen.authority.evidence.model_copy(update={"file_identities": identities})})
    with pytest.raises(ArtifactPreviewIntegrityError, match="inventory"):
        _read(frozen, names=names)


@pytest.mark.parametrize("kind", ["mode", "symlink", "hardlink", "replacement", "missing"])
def test_file_identity_and_permissions_are_enforced(frozen_factory: Callable[..., _SealedFixture], kind: str) -> None:
    frozen = frozen_factory()
    path = frozen.bundle / "tables/signals.parquet"
    path.parent.chmod(0o700)
    if kind == "mode":
        path.chmod(0o600)
    elif kind == "hardlink":
        os.link(path, frozen.root.parent / "owned-hardlink")
    else:
        path.unlink()
        if kind == "symlink":
            target = frozen.root.parent / "owned-target"
            target.write_bytes(frozen.payloads["tables/signals.parquet"])
            path.symlink_to(target)
        elif kind == "replacement":
            path.write_bytes(frozen.payloads["tables/signals.parquet"])
            path.chmod(0o400)
    path.parent.chmod(0o500)
    with pytest.raises(ArtifactPreviewIntegrityError):
        _read(frozen)


@pytest.mark.parametrize("directory", ["root", "sealed", "bundle", "tables"])
def test_directories_reject_unsafe_permissions(frozen_factory: Callable[..., _SealedFixture], directory: str) -> None:
    frozen = frozen_factory()
    paths = {"root": frozen.root, "sealed": frozen.root / "sealed", "bundle": frozen.bundle,
        "tables": frozen.bundle / "tables"}
    paths[directory].chmod(0o777)
    with pytest.raises(ArtifactPreviewIntegrityError):
        _read(frozen)


@pytest.mark.parametrize("directory", ["root", "sealed", "bundle", "tables"])
def test_directory_path_symlinks_are_rejected(frozen_factory: Callable[..., _SealedFixture], directory: str) -> None:
    frozen = frozen_factory()
    paths = {"root": frozen.root, "sealed": frozen.root / "sealed", "bundle": frozen.bundle,
        "tables": frozen.bundle / "tables"}
    path = paths[directory]
    original_mode = stat.S_IMODE(path.stat().st_mode)
    parent_mode = stat.S_IMODE(path.parent.stat().st_mode)
    path.parent.chmod(0o700)
    path.chmod(0o700)
    moved = path.with_name(path.name + "-moved")
    path.rename(moved)
    moved.chmod(original_mode)
    path.symlink_to(moved, target_is_directory=True)
    path.parent.chmod(parent_mode)
    with pytest.raises(ArtifactPreviewIntegrityError):
        _read(frozen)


@pytest.mark.parametrize("kind", ["withdrawn", "job_version", "indexed_at"])
def test_fresh_authority_changes_block_byte_evidence(frozen_factory: Callable[..., _SealedFixture], kind: str) -> None:
    frozen = frozen_factory()

    def change() -> None:
        if kind == "withdrawn":
            frozen.source.authority = None
        elif kind == "job_version":
            frozen.source.authority = frozen.authority.model_copy(update={
                "job": frozen.authority.job.model_copy(update={"version": 3})})
        else:
            frozen.source.authority = frozen.authority.model_copy(update={
                "evidence": frozen.authority.evidence.model_copy(update={
                    "indexed_at": datetime(2026, 7, 26, tzinfo=UTC)})})

    frozen.source.on_refresh = change
    with pytest.raises(ArtifactPreviewIntegrityError, match="authority"):
        _read(frozen)
    assert frozen.source.calls == 2


@pytest.mark.parametrize("kind", ["file_bytes", "file_path", "root", "sealed", "bundle", "tables"])
def test_changes_during_final_authority_lookup_are_detected(
    frozen_factory: Callable[..., _SealedFixture], kind: str,
) -> None:
    frozen = frozen_factory()
    changed = False

    def change() -> None:
        nonlocal changed
        if kind == "file_bytes":
            frozen.replace_bytes("report.md", b"Changed after hash.\n")
        elif kind == "file_path":
            path = frozen.bundle / "report.md"
            frozen.bundle.chmod(0o700)
            path.unlink()
            path.write_bytes(frozen.payloads["report.md"])
            path.chmod(0o400)
            frozen.bundle.chmod(0o500)
        else:
            path = {"root": frozen.root, "sealed": frozen.root / "sealed", "bundle": frozen.bundle,
                "tables": frozen.bundle / "tables"}[kind]
            original_mode = stat.S_IMODE(path.stat().st_mode)
            parent_mode = stat.S_IMODE(path.parent.stat().st_mode)
            path.parent.chmod(0o700)
            path.chmod(0o700)
            moved = path.with_name(path.name + "-moved")
            path.rename(moved)
            moved.chmod(original_mode)
            path.mkdir(mode=0o700 if kind in {"root", "sealed"} else 0o500)
            path.parent.chmod(parent_mode)
        changed = True

    frozen.source.on_refresh = change
    with pytest.raises(ArtifactPreviewIntegrityError):
        _read(frozen)
    assert frozen.source.calls == 2
    assert changed


@pytest.mark.parametrize("scope", ["table", "total"])
def test_encoded_table_budget_equality_and_one_byte_over(
    frozen_factory: Callable[..., _SealedFixture], scope: str,
) -> None:
    frozen = frozen_factory()
    lengths = [len(value) for path, value in frozen.payloads.items() if path.startswith("tables/")]
    field, limit = ("max_table_bytes", max(lengths)) if scope == "table" else ("max_total_bytes", sum(lengths))
    assert _read(frozen, budget=ArtifactCompleteTableBudget(**{field: limit})).encoded_table_bytes == sum(lengths)
    with pytest.raises(ArtifactPreviewIntegrityError, match="byte budget"):
        _read(frozen, budget=ArtifactCompleteTableBudget(**{field: limit - 1}))


@pytest.mark.parametrize("scope", ["bundle", "file", "manifest", "text"])
def test_original_bundle_file_manifest_text_budgets_are_kept(
    frozen_factory: Callable[..., _SealedFixture], scope: str,
) -> None:
    frozen = frozen_factory(overrides={"report.md": b"r" * 8192})
    limits = {"bundle": sum(len(value) for value in frozen.payloads.values()),
        "file": max(len(value) for path, value in frozen.payloads.items() if path != "manifest.json"),
        "manifest": max(len(frozen.payloads["manifest.json"]), len(frozen.payloads["SHA256SUMS"])),
        "text": max(len(frozen.payloads[path]) for path in ("report.md", "spec.json", "metrics.json"))}
    field = f"max_{scope}_bytes"
    assert _read(frozen, reader=frozen.reader(**{field: limits[scope]})).verified_bundle_bytes > 0
    with pytest.raises(ArtifactPreviewIntegrityError):
        _read(frozen, reader=frozen.reader(**{field: limits[scope] - 1}))


def test_manifest_file_sizes_cannot_understate_encoded_charge(frozen_factory: Callable[..., _SealedFixture]) -> None:
    frozen = frozen_factory(declared_size_delta=-1)
    with pytest.raises(ArtifactPreviewIntegrityError, match="size"):
        _read(frozen)


@pytest.mark.parametrize("kind", ["manifest", "metrics", "spec", "report"])
def test_canonical_and_accepted_text_contracts_are_retained(
    frozen_factory: Callable[..., _SealedFixture], kind: str,
) -> None:
    kwargs: dict[str, object] = {}
    if kind == "manifest":
        kwargs["manifest_suffix"] = b"\n"
    elif kind == "metrics":
        kwargs["overrides"] = {"metrics.json": b'{ "diagnostic":true}'}
    elif kind == "spec":
        kwargs["overrides"] = {"spec.json": _spec().model_copy(update={"random_seed": 7}).canonical_json().encode("utf-8")}
    else:
        kwargs["overrides"] = {"report.md": b"\xff"}
    frozen = frozen_factory(**kwargs)
    with pytest.raises(ArtifactPreviewIntegrityError):
        _read(frozen)


def test_byte_evidence_cannot_claim_parquet_row_semantics(frozen_factory: Callable[..., _SealedFixture]) -> None:
    frozen = frozen_factory(declared_rows_delta=1)
    assert all(table.row_count == 3 for table in _read(frozen).tables)
    with pytest.raises(ArtifactPreviewIntegrityError, match="Parquet metadata conflicts"):
        frozen.reader().read_complete_tables(frozen.authority.job.job_id,
            table_names=TABLE_NAMES, budget=ArtifactCompleteTableBudget())


def test_stream_read_failure_closes_every_opened_descriptor(
    frozen_factory: Callable[..., _SealedFixture], monkeypatch: pytest.MonkeyPatch,
) -> None:
    frozen = frozen_factory()
    original_hash = preview_module._hash_descriptor

    def fail_hash(descriptor: int, *, limit: int, label: str) -> str:
        if label == "tables/fills.parquet":
            raise OSError("synthetic owned stream failure")
        return original_hash(descriptor, limit=limit, label=label)

    monkeypatch.setattr(preview_module, "_hash_descriptor", fail_hash)
    with pytest.raises(ArtifactPreviewIntegrityError):
        _read(frozen)
