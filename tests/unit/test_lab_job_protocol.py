from __future__ import annotations

import ctypes
import errno
import os
import stat
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from threading import Barrier, Thread
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

import rquant.lab_job_protocol as lab_job_protocol
import rquant.private_fs as private_fs
from rquant.lab_job_protocol import (
    CancelJobCommand,
    InvalidCommandEnvelopeError,
    LabCommandEnvelope,
    LabCommandReceipt,
    LabCommandSpool,
    PauseJobCommand,
    RequestContentConflictError,
    ResumeJobCommand,
    RetryJobCommand,
    SubmitJobCommand,
)
from rquant.research_run_spec import (
    DatasetSnapshotIdentity,
    ExecutionCostSpec,
    FeatureContractIdentity,
    ParameterKind,
    ResearchJobType,
    ResearchParameter,
    ResearchRunParameters,
    ResearchRunSpec,
    ResourceClass,
)


class _FakeRenameFunction:
    def __init__(self, errors: list[int], *, repeated_error: int = 0) -> None:
        self._errors = list(errors)
        self._repeated_error = repeated_error
        self.calls: list[tuple[object, ...]] = []
        self.argtypes: tuple[object, ...] = ()
        self.restype: object | None = None

    def __call__(self, *args: object) -> int:
        self.calls.append(args)
        error_number = self._errors.pop(0) if self._errors else self._repeated_error
        if error_number == 0:
            return 0
        ctypes.set_errno(error_number)
        return -1


class _FakeRenameLibc:
    def __init__(
        self,
        *,
        darwin: _FakeRenameFunction | None = None,
        linux: _FakeRenameFunction | None = None,
    ) -> None:
        if darwin is not None:
            self.renameatx_np = darwin
        if linux is not None:
            self.renameat2 = linux


def _spec(
    *,
    threshold: Decimal = Decimal("1.5000"),
    deadline: datetime = datetime(2026, 7, 25, 2, tzinfo=UTC),
) -> ResearchRunSpec:
    return ResearchRunSpec(
        job_type=ResearchJobType.PARAMETER_SEARCH,
        parameters=ResearchRunParameters(
            strategy_name="n_shape",
            start_date=date(2026, 4, 1),
            end_date=date(2026, 7, 14),
            arguments=(
                ResearchParameter(
                    name="threshold",
                    kind=ParameterKind.DECIMAL,
                    value=threshold,
                ),
            ),
        ),
        code_sha="1" * 40,
        dataset_snapshot=DatasetSnapshotIdentity(
            snapshot_id="a" * 64,
            binding_hash="b" * 64,
            audit_run_id="d" * 64,
        ),
        feature_contract=FeatureContractIdentity(
            contract_id="intraday-core",
            contract_version="v1",
            contract_hash="c" * 64,
        ),
        execution_costs=ExecutionCostSpec(
            commission_bps="2.5",
            stamp_duty_bps="5",
            transfer_fee_bps="0.1",
            slippage_bps="3",
        ),
        random_seed=20260724,
        resource_class=ResourceClass.HEAVY,
        deadline=deadline,
        research_status="comparable",
    )


def _submit_envelope(
    *,
    request_id: UUID | None = None,
    job_id: UUID | None = None,
    spec: ResearchRunSpec | None = None,
) -> LabCommandEnvelope:
    return LabCommandEnvelope(
        request_id=request_id or uuid4(),
        command=SubmitJobCommand(
            job_id=job_id or uuid4(),
            spec=spec or _spec().model_copy(update={"research_status": "exploratory"}),
            max_attempts=3,
        ),
    )


def _v1_spec() -> ResearchRunSpec:
    values = _spec().model_dump(mode="python", round_trip=True)
    values["schema_version"] = 1
    snapshot = values["dataset_snapshot"]
    assert isinstance(snapshot, dict)
    snapshot.pop("audit_run_id")
    return ResearchRunSpec.model_validate(values)


def _hidden_audit_v1_spec() -> ResearchRunSpec:
    base = _v1_spec()
    assert base.dataset_snapshot is not None
    hidden_snapshot = DatasetSnapshotIdentity.model_construct(
        snapshot_id=base.dataset_snapshot.snapshot_id,
        binding_hash=base.dataset_snapshot.binding_hash,
        audit_run_id="e" * 64,
        _fields_set={"snapshot_id", "binding_hash"},
    )
    values = {name: getattr(base, name) for name in type(base).model_fields}
    values["dataset_snapshot"] = hidden_snapshot
    return ResearchRunSpec.model_construct(
        **values,
        _fields_set=set(base.model_fields_set),
    )


@pytest.mark.parametrize("interruption_count", [1, 3])
def test_rename_noreplace_retries_eintr_then_succeeds(
    monkeypatch: pytest.MonkeyPatch,
    interruption_count: int,
) -> None:
    function = _FakeRenameFunction([errno.EINTR] * interruption_count + [0])
    libc = _FakeRenameLibc(darwin=function)
    monkeypatch.setattr(private_fs.sys, "platform", "darwin")
    monkeypatch.setattr(private_fs.ctypes, "CDLL", lambda *_args, **_kwargs: libc)

    private_fs.rename_noreplace_at(11, "source", 12, "entry")

    assert len(function.calls) == interruption_count + 1
    assert all(call[-1] == 0x00000004 for call in function.calls)


def test_rename_noreplace_linux_branch_uses_noreplace_flag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    function = _FakeRenameFunction([errno.EINTR, 0])
    libc = _FakeRenameLibc(linux=function)
    monkeypatch.setattr(private_fs.sys, "platform", "linux")
    monkeypatch.setattr(private_fs.ctypes, "CDLL", lambda *_args, **_kwargs: libc)

    private_fs.rename_noreplace_at(21, "source", 22, "entry")

    assert len(function.calls) == 2
    assert all(call[-1] == 0x00000001 for call in function.calls)


def test_rename_noreplace_linux_without_renameat2_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    libc = _FakeRenameLibc()
    monkeypatch.setattr(private_fs.sys, "platform", "linux")
    monkeypatch.setattr(private_fs.ctypes, "CDLL", lambda *_args, **_kwargs: libc)

    with pytest.raises(OSError) as captured:
        private_fs.rename_noreplace_at(21, "source", 22, "entry")

    assert captured.value.errno == errno.ENOTSUP


@pytest.mark.parametrize("error_number", [errno.EEXIST, errno.ENOENT, errno.EXDEV])
def test_rename_noreplace_preserves_non_eintr_errors(
    monkeypatch: pytest.MonkeyPatch,
    error_number: int,
) -> None:
    function = _FakeRenameFunction([error_number])
    libc = _FakeRenameLibc(darwin=function)
    monkeypatch.setattr(private_fs.sys, "platform", "darwin")
    monkeypatch.setattr(private_fs.ctypes, "CDLL", lambda *_args, **_kwargs: libc)

    with pytest.raises(OSError) as captured:
        private_fs.rename_noreplace_at(11, "source", 12, "entry")

    assert captured.value.errno == error_number
    assert isinstance(captured.value, FileExistsError) is (error_number == errno.EEXIST)
    assert len(function.calls) == 1


def test_rename_noreplace_unsupported_platform_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(private_fs.sys, "platform", "unsupported")

    with pytest.raises(OSError) as captured:
        private_fs.rename_noreplace_at(11, "source", 12, "entry")

    assert captured.value.errno == errno.ENOTSUP


def test_persistent_rename_eintr_is_bounded_without_prepared_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    function = _FakeRenameFunction([], repeated_error=errno.EINTR)
    libc = _FakeRenameLibc(darwin=function)
    monkeypatch.setattr(private_fs.sys, "platform", "darwin")
    monkeypatch.setattr(private_fs.ctypes, "CDLL", lambda *_args, **_kwargs: libc)
    spool = LabCommandSpool(tmp_path / "commands")
    source = spool.pending_dir / f"{uuid4()}.json"
    source.write_text("{broken", encoding="utf-8")
    observed = source.lstat()

    for attempt in range(2):
        with pytest.raises(InterruptedError) as captured:
            spool._isolate_owned_entry_locked(source, observed, reason="persistent_eintr")

        assert captured.value.errno == errno.EINTR
        assert len(function.calls) == (attempt + 1) * (private_fs._RENAME_NOREPLACE_MAX_ATTEMPTS)
        assert source.read_text(encoding="utf-8") == "{broken"
        assert tuple(spool.quarantine_dir.glob("owned-entry-*.dead")) == ()


def test_command_receipt_rejects_boolean_job_version() -> None:
    envelope = _submit_envelope()

    with pytest.raises(ValueError, match="job_version"):
        LabCommandReceipt(
            request_id=envelope.request_id,
            content_hash=envelope.content_hash,
            job_id=envelope.command.job_id,
            status="applied",
            reason="submitted",
            job_version=True,
        )


def test_protocol_roundtrips_all_command_variants() -> None:
    job_id = uuid4()
    envelopes = (
        _submit_envelope(job_id=job_id),
        LabCommandEnvelope(
            request_id=uuid4(),
            command=PauseJobCommand(
                job_id=job_id,
                expected_version=1,
                reason="operator pause",
            ),
        ),
        LabCommandEnvelope(
            request_id=uuid4(),
            command=ResumeJobCommand(
                job_id=job_id,
                expected_version=2,
                reason="capacity restored",
            ),
        ),
        LabCommandEnvelope(
            request_id=uuid4(),
            command=CancelJobCommand(
                job_id=job_id,
                expected_version=2,
                reason="operator request",
            ),
        ),
        LabCommandEnvelope(
            request_id=uuid4(),
            command=RetryJobCommand(
                job_id=job_id,
                expected_version=4,
                reason="transient source failure",
            ),
        ),
    )

    for envelope in envelopes:
        restored = LabCommandEnvelope.model_validate_json(envelope.model_dump_json())
        assert type(restored.command) is type(envelope.command)
        assert restored == envelope
        assert restored.content_hash == envelope.content_hash


def test_submit_content_hash_uses_canonical_spec_hash() -> None:
    request_id = uuid4()
    job_id = uuid4()
    shanghai = timezone(timedelta(hours=8))
    first = _submit_envelope(
        request_id=request_id,
        job_id=job_id,
        spec=_spec(threshold=Decimal("1.5000")),
    )
    equivalent = _submit_envelope(
        request_id=request_id,
        job_id=job_id,
        spec=_spec(
            threshold=Decimal("1.5"),
            deadline=datetime(2026, 7, 25, 10, tzinfo=shanghai),
        ),
    )

    assert first.command.spec.spec_hash == equivalent.command.spec.spec_hash
    assert first.content_hash == equivalent.content_hash


def test_submit_envelope_parses_legacy_v1_spec_for_historical_replay() -> None:
    envelope = _submit_envelope(spec=_v1_spec())

    restored = LabCommandEnvelope.model_validate_json(envelope.model_dump_json())

    assert restored.command.spec.schema_version == 1
    assert restored.command.spec.spec_hash == (
        "f7a26c9311d2208eeec24ff172d7e01dabe63398e987dd77314d0fed59c4a9ea"
    )


def test_hidden_v1_audit_cannot_replay_colliding_lab_command_hash() -> None:
    job_id = UUID("00000000-0000-0000-0000-000000000021")
    request_id = UUID("00000000-0000-0000-0000-000000000022")
    baseline = LabCommandEnvelope(
        request_id=request_id,
        command=SubmitJobCommand(job_id=job_id, spec=_v1_spec(), max_attempts=3),
    )
    unsafe_spec = _hidden_audit_v1_spec()
    unsafe_command = SubmitJobCommand.model_construct(
        command_type="submit",
        job_id=job_id,
        spec=unsafe_spec,
        max_attempts=3,
    )
    unsafe_envelope = LabCommandEnvelope.model_construct(
        schema_version=1,
        request_id=request_id,
        command=unsafe_command,
        content_hash=baseline.content_hash,
    )

    assert unsafe_spec.spec_hash == baseline.command.spec.spec_hash
    assert lab_job_protocol._command_hash(unsafe_command) == baseline.content_hash
    with pytest.raises(ValidationError, match="v1.*audit_run_id"):
        LabCommandEnvelope.model_validate(unsafe_envelope)


def test_envelope_rejects_tampered_content_hash() -> None:
    envelope = _submit_envelope()
    payload = envelope.model_dump(mode="json")
    payload["content_hash"] = "f" * 64

    with pytest.raises(ValueError, match="content_hash"):
        LabCommandEnvelope.model_validate(payload)


def test_spool_publish_load_ack_is_durable_and_typed(tmp_path: Path) -> None:
    spool = LabCommandSpool(tmp_path / "commands")
    envelope = _submit_envelope()

    published = spool.publish(envelope)
    assert published.envelope == envelope
    assert published.path.parent == spool.pending_dir
    assert spool.load(published.path).envelope == envelope
    assert spool.pending() == (published,)

    receipt = LabCommandReceipt(
        request_id=envelope.request_id,
        content_hash=envelope.content_hash,
        job_id=envelope.command.job_id,
        status="applied",
        reason="submitted",
        job_version=0,
    )
    acknowledged = spool.ack(published, receipt)

    assert acknowledged.receipt == receipt
    assert acknowledged.path.parent == spool.ack_dir
    assert spool.pending() == ()
    assert spool.load_receipt(acknowledged.path) == receipt


def test_spool_rejects_new_v2_comparable_before_pending_write(tmp_path: Path) -> None:
    spool = LabCommandSpool(tmp_path / "commands")
    formal_v2 = _spec().model_copy(update={"research_status": "comparable"})

    with pytest.raises(InvalidCommandEnvelopeError, match="v2.*comparable"):
        spool.publish(_submit_envelope(spec=formal_v2))

    assert spool.pending() == ()


def _replace_managed_spool_directory(path: Path, external: Path) -> Path:
    displaced = path.with_name(f"{path.name}-displaced")
    path.rename(displaced)
    path.symlink_to(external, target_is_directory=True)
    return displaced


def test_spool_publish_rejects_post_init_pending_symlink_without_external_write(
    tmp_path: Path,
) -> None:
    spool = LabCommandSpool(tmp_path / "commands")
    external = tmp_path / "external-pending"
    external.mkdir(mode=0o700)
    _replace_managed_spool_directory(spool.pending_dir, external)

    with pytest.raises(InvalidCommandEnvelopeError, match="identity"):
        spool.publish(_submit_envelope())

    assert tuple(external.iterdir()) == ()


def test_spool_list_and_load_reject_post_init_pending_replacement(
    tmp_path: Path,
) -> None:
    spool = LabCommandSpool(tmp_path / "commands")
    published = spool.publish(_submit_envelope())
    payload = published.path.read_bytes()
    external = tmp_path / "external-pending"
    external.mkdir(mode=0o700)
    displaced = _replace_managed_spool_directory(spool.pending_dir, external)
    external_entry = external / published.path.name
    external_entry.write_bytes(payload)

    with pytest.raises(InvalidCommandEnvelopeError, match="identity"):
        spool.pending()
    with pytest.raises(InvalidCommandEnvelopeError, match="identity"):
        spool.load(spool.pending_dir / published.path.name)

    assert external_entry.read_bytes() == payload
    assert (displaced / published.path.name).read_bytes() == payload


def test_spool_ack_rejects_post_init_ack_symlink_without_external_write(
    tmp_path: Path,
) -> None:
    spool = LabCommandSpool(tmp_path / "commands")
    envelope = _submit_envelope()
    published = spool.publish(envelope)
    external = tmp_path / "external-ack"
    external.mkdir(mode=0o700)
    _replace_managed_spool_directory(spool.ack_dir, external)
    receipt = LabCommandReceipt(
        request_id=envelope.request_id,
        content_hash=envelope.content_hash,
        job_id=envelope.command.job_id,
        status="applied",
        reason="submitted",
        job_version=0,
    )

    with pytest.raises(InvalidCommandEnvelopeError, match="identity"):
        spool.ack(published, receipt)

    assert tuple(external.iterdir()) == ()
    assert published.path.exists()


def test_spool_quarantine_rejects_post_init_replacement_without_external_write(
    tmp_path: Path,
) -> None:
    spool = LabCommandSpool(tmp_path / "commands")
    bad = spool.pending_dir / f"{uuid4()}.json"
    bad.write_text("{broken", encoding="utf-8")
    external = tmp_path / "external-quarantine"
    external.mkdir(mode=0o700)
    _replace_managed_spool_directory(spool.quarantine_dir, external)

    with pytest.raises(InvalidCommandEnvelopeError, match="identity"):
        spool.quarantine(bad, reason="invalid_json")

    assert tuple(external.iterdir()) == ()
    assert bad.exists()


def test_same_request_and_content_publish_is_idempotent(tmp_path: Path) -> None:
    spool = LabCommandSpool(tmp_path / "commands")
    envelope = _submit_envelope()

    first = spool.publish(envelope)
    second = spool.publish(LabCommandEnvelope.model_validate_json(envelope.model_dump_json()))

    assert first.path == second.path
    assert spool.pending() == (first,)


def test_same_request_with_different_content_never_overwrites(tmp_path: Path) -> None:
    spool = LabCommandSpool(tmp_path / "commands")
    request_id = uuid4()
    first = _submit_envelope(request_id=request_id)
    conflict = _submit_envelope(request_id=request_id)
    original = spool.publish(first)
    original_bytes = original.path.read_bytes()

    with pytest.raises(RequestContentConflictError):
        spool.publish(conflict)

    assert original.path.read_bytes() == original_bytes
    assert spool.load(original.path).envelope == first


def test_concurrent_publish_is_no_clobber(tmp_path: Path) -> None:
    spool = LabCommandSpool(tmp_path / "commands")
    envelope = _submit_envelope()

    with ThreadPoolExecutor(max_workers=8) as executor:
        entries = tuple(executor.map(spool.publish, (envelope,) * 32))

    paths = {entry.path for entry in entries}
    assert len(paths) == 1
    assert next(iter(paths)).name.endswith(f"-{envelope.request_id}.json")
    assert spool.pending() == (entries[0],)


def test_bad_json_can_be_quarantined_without_bare_dict(tmp_path: Path) -> None:
    spool = LabCommandSpool(tmp_path / "commands")
    bad_path = spool.pending_dir / f"{uuid4()}.json"
    bad_path.write_text("{broken", encoding="utf-8")

    with pytest.raises(InvalidCommandEnvelopeError):
        spool.load(bad_path)

    quarantined = spool.quarantine(bad_path, reason="invalid_json")
    assert quarantined.path.parent.parent == spool.quarantine_dir
    assert quarantined.reason == "invalid_json"
    assert not bad_path.exists()
    assert quarantined.path.read_text(encoding="utf-8") == "{broken"


@pytest.mark.parametrize(
    "entry_kind",
    ["regular", "symlink", "hardlink", "fifo", "empty_directory", "nonempty_directory"],
)
def test_owned_entry_isolation_primitive_moves_only_bound_directory_entry(
    tmp_path: Path,
    entry_kind: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    if entry_kind == "fifo" and not hasattr(os, "mkfifo"):
        pytest.skip("FIFO is not supported on this platform")
    spool = LabCommandSpool(tmp_path / "commands")
    source = spool.pending_dir / f"{uuid4()}.json"
    outside = tmp_path / "outside"
    outside.write_text("outside", encoding="utf-8")
    if entry_kind == "regular":
        source.write_text("{broken", encoding="utf-8")
    elif entry_kind == "symlink":
        os.symlink(outside, source)
    elif entry_kind == "hardlink":
        os.link(outside, source)
    elif entry_kind == "fifo":
        os.mkfifo(source)
    else:
        source.mkdir()
        if entry_kind == "nonempty_directory":
            (source / "manual.txt").write_text("retain manually", encoding="utf-8")
    observed = source.lstat()
    real_open = os.open
    real_close = os.close
    bound_open_flags: list[int] = []
    live_bound_descriptors: set[int] = set()

    def audited_open(
        path: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        flags: int,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> int:
        descriptor = real_open(path, flags, mode, dir_fd=dir_fd)
        opened = os.fstat(descriptor)
        if (opened.st_dev, opened.st_ino) == (observed.st_dev, observed.st_ino):
            bound_open_flags.append(flags)
            live_bound_descriptors.add(descriptor)
        return descriptor

    def audited_close(descriptor: int) -> None:
        live_bound_descriptors.discard(descriptor)
        real_close(descriptor)

    monkeypatch.setattr(lab_job_protocol.os, "open", audited_open)
    monkeypatch.setattr(lab_job_protocol.os, "close", audited_close)

    with pytest.raises(InvalidCommandEnvelopeError) as captured:
        spool.load(source)
    identity = captured.value.file_identity
    assert identity is not None
    isolated = spool.quarantine(identity, reason=f"invalid:{entry_kind}")

    assert not os.path.lexists(source)
    assert isolated.path.parent.parent == spool.quarantine_dir
    evidence_path = isolated.path.parent / "evidence.json"
    evidence = lab_job_protocol._LabOwnedEntryIsolationEvidence.model_validate_json(
        evidence_path.read_bytes()
    )
    destination = isolated.path.lstat()
    assert (evidence.device, evidence.inode, evidence.mode, evidence.link_count) == (
        observed.st_dev,
        observed.st_ino,
        observed.st_mode,
        observed.st_nlink,
    )
    assert (destination.st_dev, destination.st_ino, destination.st_mode) == (
        observed.st_dev,
        observed.st_ino,
        observed.st_mode,
    )
    if entry_kind == "symlink":
        assert os.readlink(isolated.path) == str(outside)
    if entry_kind == "hardlink":
        assert outside.read_text(encoding="utf-8") == "outside"
        assert outside.stat().st_ino == destination.st_ino
        assert outside.stat().st_nlink == 2
    if entry_kind == "fifo":
        assert stat.S_ISFIFO(destination.st_mode)
    if entry_kind == "nonempty_directory":
        assert (isolated.path / "manual.txt").read_text(encoding="utf-8") == "retain manually"
        assert evidence.manual_retention is True
    if stat.S_ISREG(observed.st_mode):
        assert any(flags & os.O_NOFOLLOW and flags & os.O_NONBLOCK for flags in bound_open_flags)
        assert live_bound_descriptors == set()
    else:
        assert bound_open_flags == []

    restarted = LabCommandSpool(spool.root)
    reconciled = tuple(restarted.quarantine_dir.glob("owned-entry-*.dead/evidence.json"))
    assert reconciled == (evidence_path,)


def test_owned_entry_isolation_retention_bounds_complete_and_incomplete_records(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    failure_spool = LabCommandSpool(tmp_path / "open-failure-commands")
    failure_source = failure_spool.pending_dir / f"{uuid4()}.json"
    failure_source.write_text("original", encoding="utf-8")
    failure_observed = failure_source.lstat()
    failure_identity = (
        failure_observed.st_dev,
        failure_observed.st_ino,
        failure_observed.st_mode,
        failure_observed.st_nlink,
        failure_observed.st_size,
        failure_observed.st_mtime_ns,
        failure_observed.st_ctime_ns,
    )
    real_open = os.open
    real_close = os.close
    real_dup = os.dup
    real_fsync = os.fsync
    real_link = os.link
    real_mkdir = os.mkdir
    live_descriptors: set[int] = set()
    bound_open_attempts = 0

    def failing_open(
        path: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        flags: int,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> int:
        nonlocal bound_open_attempts
        if path == failure_source.name and flags & os.O_NONBLOCK:
            bound_open_attempts += 1
            raise PermissionError(errno.EACCES, "forced bound open failure", path)
        descriptor = real_open(path, flags, mode, dir_fd=dir_fd)
        live_descriptors.add(descriptor)
        return descriptor

    def tracking_close(descriptor: int) -> None:
        live_descriptors.discard(descriptor)
        real_close(descriptor)

    def tracking_dup(descriptor: int) -> int:
        duplicated = real_dup(descriptor)
        live_descriptors.add(duplicated)
        return duplicated

    def assert_failure_source_unchanged() -> None:
        current = failure_source.lstat()
        assert (
            current.st_dev,
            current.st_ino,
            current.st_mode,
            current.st_nlink,
            current.st_size,
            current.st_mtime_ns,
            current.st_ctime_ns,
        ) == failure_identity
        assert failure_source.read_text(encoding="utf-8") == "original"

    monkeypatch.setattr(lab_job_protocol.os, "open", failing_open)
    monkeypatch.setattr(lab_job_protocol.os, "close", tracking_close)
    monkeypatch.setattr(lab_job_protocol.os, "dup", tracking_dup)

    for _attempt in range(3):
        with pytest.raises(PermissionError, match="forced bound open failure"):
            failure_spool._isolate_owned_entry_locked(
                failure_source,
                failure_observed,
                reason="forced_open_failure",
            )
        assert_failure_source_unchanged()
        assert live_descriptors == set()

    assert bound_open_attempts == 3
    assert tuple(failure_spool.quarantine_dir.iterdir()) == ()

    def tracking_open(
        path: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        flags: int,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> int:
        descriptor = real_open(path, flags, mode, dir_fd=dir_fd)
        live_descriptors.add(descriptor)
        return descriptor

    bound_was_live_at_mkdir = False

    def failing_container_mkdir(
        path: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> None:
        nonlocal bound_was_live_at_mkdir
        if str(path).startswith("owned-entry-"):
            bound_was_live_at_mkdir = any(
                (os.fstat(descriptor).st_dev, os.fstat(descriptor).st_ino)
                == (failure_observed.st_dev, failure_observed.st_ino)
                for descriptor in live_descriptors
            )
            raise PermissionError(errno.EACCES, "forced container mkdir failure", path)
        real_mkdir(path, mode, dir_fd=dir_fd)

    monkeypatch.setattr(lab_job_protocol.os, "open", tracking_open)
    monkeypatch.setattr(lab_job_protocol.os, "mkdir", failing_container_mkdir)

    with pytest.raises(PermissionError, match="forced container mkdir failure"):
        failure_spool._isolate_owned_entry_locked(
            failure_source,
            failure_observed,
            reason="forced_container_failure",
        )
    assert bound_was_live_at_mkdir is True
    assert live_descriptors == set()
    assert tuple(failure_spool.quarantine_dir.iterdir()) == ()
    assert_failure_source_unchanged()

    container_open_attempts = 0

    def failing_owned_container_open(
        path: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        flags: int,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> int:
        nonlocal container_open_attempts
        if str(path).startswith("owned-entry-") and flags & getattr(os, "O_DIRECTORY", 0):
            container_open_attempts += 1
            raise OSError(errno.EIO, "forced owned container open failure", path)
        descriptor = real_open(path, flags, mode, dir_fd=dir_fd)
        live_descriptors.add(descriptor)
        return descriptor

    monkeypatch.setattr(lab_job_protocol.os, "open", failing_owned_container_open)
    monkeypatch.setattr(lab_job_protocol.os, "mkdir", real_mkdir)

    for _attempt in range(3):
        with pytest.raises(InvalidCommandEnvelopeError, match="identity changed"):
            failure_spool._isolate_owned_entry_locked(
                failure_source,
                failure_observed,
                reason="forced_container_open_failure",
            )
        assert tuple(failure_spool.quarantine_dir.iterdir()) == ()
        assert_failure_source_unchanged()
        assert live_descriptors == set()

    assert container_open_attempts == 3

    monkeypatch.setattr(lab_job_protocol.os, "open", tracking_open)

    def failing_evidence_link(
        source: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        target: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        **kwargs: object,
    ) -> None:
        if str(target) == "evidence.json":
            raise OSError(errno.EIO, "forced pre-link evidence publication failure")
        real_link(source, target, **kwargs)

    monkeypatch.setattr(lab_job_protocol.os, "link", failing_evidence_link)

    with pytest.raises(OSError, match="forced pre-link evidence publication failure"):
        failure_spool._isolate_owned_entry_locked(
            failure_source,
            failure_observed,
            reason="forced_pre_link_publication_failure",
        )
    assert tuple(failure_spool.quarantine_dir.iterdir()) == ()
    assert_failure_source_unchanged()
    assert live_descriptors == set()

    published_spool = LabCommandSpool(tmp_path / "visible-evidence-commands")
    published_source = published_spool.pending_dir / f"{uuid4()}.json"
    published_source.write_text("published-source", encoding="utf-8")
    published_observed = published_source.lstat()
    evidence_visible = False

    def tracking_evidence_link(
        source: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        target: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        **kwargs: object,
    ) -> None:
        nonlocal evidence_visible
        real_link(source, target, **kwargs)
        if str(target) == "evidence.json":
            evidence_visible = True

    def failing_visible_evidence_fsync(descriptor: int) -> None:
        nonlocal evidence_visible
        if evidence_visible:
            evidence_visible = False
            raise OSError(errno.EIO, "forced visible evidence fsync failure")
        real_fsync(descriptor)

    monkeypatch.setattr(lab_job_protocol.os, "link", tracking_evidence_link)
    monkeypatch.setattr(lab_job_protocol.os, "fsync", failing_visible_evidence_fsync)

    with pytest.raises(OSError, match="forced visible evidence fsync failure"):
        published_spool._isolate_owned_entry_locked(
            published_source,
            published_observed,
            reason="forced_visible_evidence_fsync_failure",
        )
    published_containers = tuple(published_spool.quarantine_dir.glob("owned-entry-*.dead"))
    assert len(published_containers) == 1
    assert (published_containers[0] / "evidence.json").is_file()
    assert not (published_containers[0] / "entry").exists()
    assert published_source.read_text(encoding="utf-8") == "published-source"
    assert live_descriptors == set()

    monkeypatch.setattr(lab_job_protocol.os, "link", real_link)
    monkeypatch.setattr(lab_job_protocol.os, "fsync", real_fsync)

    recovered_published = LabCommandSpool(published_spool.root)
    assert not published_source.exists()
    assert (published_containers[0] / "entry").read_text(encoding="utf-8") == ("published-source")
    assert recovered_published.pending() == ()

    replacement_spool = LabCommandSpool(tmp_path / "replacement-container-commands")
    replacement_source = replacement_spool.pending_dir / f"{uuid4()}.json"
    replacement_source.write_text("replacement-source", encoding="utf-8")
    replacement_observed = replacement_source.lstat()
    replacement_container: Path | None = None
    replacement_identity: tuple[int, int] | None = None
    replace_on_fsync = False

    def replacing_container_mkdir(
        path: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> None:
        nonlocal replacement_container, replace_on_fsync
        real_mkdir(path, mode, dir_fd=dir_fd)
        if str(path).startswith("owned-entry-"):
            replacement_container = replacement_spool.quarantine_dir / str(path)
            replace_on_fsync = True

    def replace_then_fail_fsync(descriptor: int) -> None:
        nonlocal replace_on_fsync, replacement_identity
        if replace_on_fsync:
            replace_on_fsync = False
            assert replacement_container is not None
            os.rmdir(replacement_container)
            real_mkdir(replacement_container, 0o700)
            replacement_stat = replacement_container.lstat()
            replacement_identity = (replacement_stat.st_dev, replacement_stat.st_ino)
            raise OSError(errno.EIO, "forced bound container replacement")
        real_fsync(descriptor)

    monkeypatch.setattr(lab_job_protocol.os, "mkdir", replacing_container_mkdir)
    monkeypatch.setattr(lab_job_protocol.os, "fsync", replace_then_fail_fsync)

    with pytest.raises(OSError, match="forced bound container replacement"):
        replacement_spool._isolate_owned_entry_locked(
            replacement_source,
            replacement_observed,
            reason="forced_bound_container_replacement",
        )
    assert replacement_container is not None
    replacement_after = replacement_container.lstat()
    assert (replacement_after.st_dev, replacement_after.st_ino) == replacement_identity
    assert tuple(replacement_container.iterdir()) == ()
    assert replacement_source.read_text(encoding="utf-8") == "replacement-source"
    assert live_descriptors == set()

    awaiting_container_fsync = False
    failed_container_fsyncs = 0

    def tracking_container_mkdir(
        path: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> None:
        nonlocal awaiting_container_fsync
        real_mkdir(path, mode, dir_fd=dir_fd)
        if str(path).startswith("owned-entry-"):
            awaiting_container_fsync = True

    def failing_post_mkdir_fsync(descriptor: int) -> None:
        nonlocal awaiting_container_fsync, failed_container_fsyncs
        if awaiting_container_fsync:
            awaiting_container_fsync = False
            failed_container_fsyncs += 1
            raise OSError(errno.EIO, "forced post-mkdir fsync failure")
        real_fsync(descriptor)

    monkeypatch.setattr(lab_job_protocol.os, "mkdir", tracking_container_mkdir)
    monkeypatch.setattr(lab_job_protocol.os, "fsync", failing_post_mkdir_fsync)

    for _attempt in range(3):
        with pytest.raises(OSError, match="forced post-mkdir fsync failure"):
            failure_spool._isolate_owned_entry_locked(
                failure_source,
                failure_observed,
                reason="forced_post_mkdir_fsync_failure",
            )
        assert tuple(failure_spool.quarantine_dir.iterdir()) == ()
        assert_failure_source_unchanged()
        assert live_descriptors == set()

    assert failed_container_fsyncs == 3

    retained_spool = LabCommandSpool(tmp_path / "nonempty-container-commands")
    retained_source = retained_spool.pending_dir / f"{uuid4()}.json"
    retained_source.write_text("retained-source", encoding="utf-8")
    retained_observed = retained_source.lstat()
    retained_container: Path | None = None
    retain_on_fsync = False

    def retaining_container_mkdir(
        path: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> None:
        nonlocal retained_container, retain_on_fsync
        real_mkdir(path, mode, dir_fd=dir_fd)
        if str(path).startswith("owned-entry-"):
            retained_container = retained_spool.quarantine_dir / str(path)
            retain_on_fsync = True

    def populate_then_fail_fsync(descriptor: int) -> None:
        nonlocal retain_on_fsync
        if retain_on_fsync:
            retain_on_fsync = False
            assert retained_container is not None
            (retained_container / "foreign-entry").write_text(
                "do not remove",
                encoding="utf-8",
            )
            raise OSError(errno.EIO, "forced nonempty post-mkdir fsync failure")
        real_fsync(descriptor)

    monkeypatch.setattr(lab_job_protocol.os, "mkdir", retaining_container_mkdir)
    monkeypatch.setattr(lab_job_protocol.os, "fsync", populate_then_fail_fsync)

    with pytest.raises(OSError, match="forced nonempty post-mkdir fsync failure"):
        retained_spool._isolate_owned_entry_locked(
            retained_source,
            retained_observed,
            reason="forced_nonempty_post_mkdir_fsync_failure",
        )
    assert retained_container is not None
    assert (retained_container / "foreign-entry").read_text(encoding="utf-8") == ("do not remove")
    assert not (retained_container / "evidence.json").exists()
    assert retained_source.read_text(encoding="utf-8") == "retained-source"
    assert live_descriptors == set()

    monkeypatch.setattr(lab_job_protocol.os, "open", real_open)
    monkeypatch.setattr(lab_job_protocol.os, "close", real_close)
    monkeypatch.setattr(lab_job_protocol.os, "dup", real_dup)
    monkeypatch.setattr(lab_job_protocol.os, "fsync", real_fsync)
    monkeypatch.setattr(lab_job_protocol.os, "link", real_link)
    monkeypatch.setattr(lab_job_protocol.os, "mkdir", real_mkdir)

    retried = failure_spool._isolate_owned_entry_locked(
        failure_source,
        failure_observed,
        reason="retry_after_open_failure",
    )
    assert retried.path.read_text(encoding="utf-8") == "original"
    assert not os.path.lexists(failure_source)
    retry_containers = tuple(failure_spool.quarantine_dir.glob("owned-entry-*.dead"))
    assert retry_containers == (retried.path.parent,)
    assert (retry_containers[0] / "evidence.json").is_file()

    root = tmp_path / "commands"
    spool = LabCommandSpool(root, max_isolation_records=8, max_isolation_bytes=1024 * 1024)
    for index in range(7):
        source = spool.pending_dir / f"{UUID(int=index + 1)}.json"
        source.write_text("{broken", encoding="utf-8")
        with pytest.raises(InvalidCommandEnvelopeError) as captured:
            spool.load(source)
        assert captured.value.file_identity is not None
        spool.quarantine(captured.value.file_identity, reason="invalid_json")
    incomplete = spool.quarantine_dir / f"owned-entry-{uuid4()}.dead"
    incomplete.mkdir()
    (incomplete / "evidence.json").write_text("{truncated", encoding="utf-8")

    bounded = LabCommandSpool(root, max_isolation_records=1, max_isolation_bytes=1)

    bundles = tuple(bounded.quarantine_dir.glob("owned-entry-*.dead"))
    assert len(bundles) <= 1
    assert all(path.is_dir() for path in bundles)


def test_owned_entry_isolation_startup_rebuilds_corrupt_identity_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "commands"
    spool = LabCommandSpool(root)
    source = spool.pending_dir / f"{uuid4()}.json"
    source.write_text("{broken", encoding="utf-8")
    with pytest.raises(InvalidCommandEnvelopeError) as captured:
        spool.load(source)
    identity = captured.value.file_identity
    assert identity is not None
    isolated = spool.quarantine(identity, reason="invalid_json")
    observed = isolated.path.lstat()
    evidence_path = isolated.path.parent / "evidence.json"
    evidence_path.write_text("{truncated", encoding="utf-8")

    restarted = LabCommandSpool(root)

    rebuilt = lab_job_protocol._LabOwnedEntryIsolationEvidence.model_validate_json(
        evidence_path.read_bytes()
    )
    assert rebuilt.source_area == "recovered"
    assert (rebuilt.device, rebuilt.inode, rebuilt.mode, rebuilt.link_count) == (
        observed.st_dev,
        observed.st_ino,
        observed.st_mode,
        observed.st_nlink,
    )
    assert isolated.path.read_text(encoding="utf-8") == "{broken"
    raw_evidence = tuple(isolated.path.parent.glob("invalid-evidence-*.raw"))
    assert len(raw_evidence) == 1
    assert rebuilt.invalid_evidence is not None
    assert rebuilt.invalid_evidence.name == raw_evidence[0].name
    assert lab_job_protocol.LabCommandSpool._OWNED_INVALID_EVIDENCE_NAME.fullmatch(
        raw_evidence[0].name
    )
    raw_stat = raw_evidence[0].lstat()
    assert (
        rebuilt.invalid_evidence.device,
        rebuilt.invalid_evidence.inode,
        rebuilt.invalid_evidence.mode,
        rebuilt.invalid_evidence.link_count,
    ) == (raw_stat.st_dev, raw_stat.st_ino, raw_stat.st_mode, raw_stat.st_nlink)
    assert restarted.pending() == ()

    empty_unpublished = spool.quarantine_dir / f"owned-entry-{uuid4()}.dead"
    empty_unpublished.mkdir(mode=0o700)
    unknown_nonempty = spool.quarantine_dir / f"owned-entry-{uuid4()}.dead"
    unknown_nonempty.mkdir(mode=0o700)
    unknown_marker = unknown_nonempty / "unknown-content"
    unknown_marker.write_text("retain", encoding="utf-8")

    LabCommandSpool(root)

    assert not empty_unpublished.exists()
    assert unknown_marker.read_text(encoding="utf-8") == "retain"

    crash_root = tmp_path / "crash-commands"
    crash_container_names = tuple(f"owned-entry-{uuid4()}.dead" for _index in range(3))
    crash_script = """
import os
import sys
from pathlib import Path

from rquant.lab_job_protocol import LabCommandSpool

spool = LabCommandSpool(Path(sys.argv[1]))
container = spool.quarantine_dir / sys.argv[2]
container.mkdir(mode=0o700)
spool.mutation_guard = lambda: os._exit(73)
spool._publish_no_clobber(container / "evidence.json", b"crash-evidence")
raise SystemExit(99)
"""
    crashed = subprocess.run(
        [sys.executable, "-c", crash_script, str(crash_root), crash_container_names[0]],
        check=False,
        cwd=Path(__file__).parents[2],
    )
    assert crashed.returncode == 73
    for container_name in crash_container_names[1:]:
        container = crash_root / "quarantine" / container_name
        container.mkdir(mode=0o700)
        temporary = container / f".evidence.json.{uuid4().hex}.tmp"
        temporary.write_bytes(b"crash-evidence")
        temporary.chmod(0o600)

    real_open = os.open
    real_close = os.close
    live_recovery_descriptors: set[int] = set()

    def tracking_recovery_open(
        path: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        flags: int,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> int:
        descriptor = real_open(path, flags, mode, dir_fd=dir_fd)
        live_recovery_descriptors.add(descriptor)
        return descriptor

    def tracking_recovery_close(descriptor: int) -> None:
        live_recovery_descriptors.discard(descriptor)
        real_close(descriptor)

    monkeypatch.setattr(lab_job_protocol.os, "open", tracking_recovery_open)
    monkeypatch.setattr(lab_job_protocol.os, "close", tracking_recovery_close)
    recovered_crashes = LabCommandSpool(
        crash_root,
        max_isolation_records=1,
        max_isolation_bytes=1,
    )

    assert tuple(recovered_crashes.quarantine_dir.glob("owned-entry-*.dead")) == ()
    assert live_recovery_descriptors == set()
    monkeypatch.setattr(lab_job_protocol.os, "open", real_open)
    monkeypatch.setattr(lab_job_protocol.os, "close", real_close)
    retry_source = recovered_crashes.pending_dir / f"{uuid4()}.json"
    retry_source.write_text("{broken", encoding="utf-8")
    with pytest.raises(InvalidCommandEnvelopeError) as retry_captured:
        recovered_crashes.load(retry_source)
    retry_identity = retry_captured.value.file_identity
    assert retry_identity is not None
    retried = recovered_crashes.quarantine(retry_identity, reason="retry_after_crash_cleanup")
    assert retried.path.read_text(encoding="utf-8") == "{broken"

    prepared_root = tmp_path / "prepared-commands"
    prepared_spool = LabCommandSpool(prepared_root)
    prepared_source = prepared_spool.pending_dir / f"{uuid4()}.json"
    prepared_source.write_text("prepared-source", encoding="utf-8")
    prepared_observed = prepared_source.lstat()
    prepared_id = uuid4()
    prepared_container = prepared_spool.quarantine_dir / f"owned-entry-{prepared_id}.dead"
    prepared_container.mkdir(mode=0o700)
    prepared_evidence = lab_job_protocol._LabOwnedEntryIsolationEvidence(
        isolation_id=prepared_id,
        source_area="pending",
        source_name=prepared_source.name,
        reason="recover linked publication temporary",
        device=prepared_observed.st_dev,
        inode=prepared_observed.st_ino,
        mode=prepared_observed.st_mode,
        link_count=prepared_observed.st_nlink,
        file_type="regular",
        byte_count=prepared_observed.st_size,
    )
    prepared_temporary = prepared_container / f".evidence.json.{uuid4().hex}.tmp"
    prepared_temporary.write_bytes(prepared_evidence.canonical_json_bytes())
    prepared_temporary.chmod(0o600)
    prepared_target = prepared_container / "evidence.json"
    os.link(prepared_temporary, prepared_target)

    LabCommandSpool(prepared_root)

    assert not os.path.lexists(prepared_temporary)
    assert prepared_target.lstat().st_nlink == 1
    assert not os.path.lexists(prepared_source)
    assert (prepared_container / "entry").read_text(encoding="utf-8") == "prepared-source"

    retained_root = tmp_path / "retained-crash-commands"
    retained_spool = LabCommandSpool(retained_root)
    malformed_container = retained_spool.quarantine_dir / f"owned-entry-{uuid4()}.dead"
    malformed_container.mkdir(mode=0o700)
    malformed_temporary = malformed_container / ".evidence.json.not-a-uuid.tmp"
    malformed_temporary.write_text("malformed", encoding="utf-8")
    malformed_temporary.chmod(0o600)

    unrelated_source = retained_spool.pending_dir / f"{uuid4()}.json"
    unrelated_source.write_text("unrelated-source", encoding="utf-8")
    unrelated_observed = unrelated_source.lstat()
    unrelated_id = uuid4()
    unrelated_container = retained_spool.quarantine_dir / f"owned-entry-{unrelated_id}.dead"
    unrelated_container.mkdir(mode=0o700)
    unrelated_evidence = lab_job_protocol._LabOwnedEntryIsolationEvidence(
        isolation_id=unrelated_id,
        source_area="pending",
        source_name=unrelated_source.name,
        reason="retain unrelated publication temporary",
        device=unrelated_observed.st_dev,
        inode=unrelated_observed.st_ino,
        mode=unrelated_observed.st_mode,
        link_count=unrelated_observed.st_nlink,
        file_type="regular",
        byte_count=unrelated_observed.st_size,
    )
    unrelated_target = unrelated_container / "evidence.json"
    unrelated_target.write_bytes(unrelated_evidence.canonical_json_bytes())
    unrelated_target.chmod(0o600)
    unrelated_temporary = unrelated_container / f".evidence.json.{uuid4().hex}.tmp"
    unrelated_temporary.write_bytes(b"replacement")
    unrelated_temporary.chmod(0o600)

    fifo_container = retained_spool.quarantine_dir / f"owned-entry-{uuid4()}.dead"
    fifo_container.mkdir(mode=0o700)
    fifo_temporary = fifo_container / f".evidence.json.{uuid4().hex}.tmp"
    os.mkfifo(fifo_temporary, mode=0o600)
    retained_inodes = {
        malformed_temporary: malformed_temporary.lstat().st_ino,
        unrelated_target: unrelated_target.lstat().st_ino,
        unrelated_temporary: unrelated_temporary.lstat().st_ino,
        fifo_temporary: fifo_temporary.lstat().st_ino,
    }

    LabCommandSpool(retained_root)

    assert unrelated_source.read_text(encoding="utf-8") == "unrelated-source"
    assert all(path.lstat().st_ino == inode for path, inode in retained_inodes.items())

    swap_root = tmp_path / "cleanup-swap-commands"
    swap_spool = LabCommandSpool(swap_root)
    swap_container = swap_spool.quarantine_dir / f"owned-entry-{uuid4()}.dead"
    swap_container.mkdir(mode=0o700)
    swap_temporary = swap_container / f".evidence.json.{uuid4().hex}.tmp"
    swap_temporary.write_text("original-temporary", encoding="utf-8")
    swap_temporary.chmod(0o600)
    replacement_inode: int | None = None

    def replace_publication_temporary() -> None:
        nonlocal replacement_inode
        swap_temporary.unlink()
        swap_temporary.write_text("replacement-temporary", encoding="utf-8")
        swap_temporary.chmod(0o600)
        replacement_inode = swap_temporary.lstat().st_ino

    monkeypatch.setattr(swap_spool, "mutation_guard", replace_publication_temporary)

    swap_spool._reconcile_owned_isolation_container_locked(swap_container)

    assert replacement_inode is not None
    assert swap_temporary.lstat().st_ino == replacement_inode
    assert swap_temporary.read_text(encoding="utf-8") == "replacement-temporary"

    container_swap_root = tmp_path / "container-cleanup-swap-commands"
    container_swap_spool = LabCommandSpool(container_swap_root)
    original_container = container_swap_spool.quarantine_dir / f"owned-entry-{uuid4()}.dead"
    original_container.mkdir(mode=0o700)
    original_temporary = original_container / f".evidence.json.{uuid4().hex}.tmp"
    original_temporary.write_text("original-container-temporary", encoding="utf-8")
    original_temporary.chmod(0o600)
    original_temporary_inode = original_temporary.lstat().st_ino
    displaced_container = original_container.with_name(f"{original_container.name}.displaced")
    callback_count = 0

    def replace_container_name_once() -> None:
        nonlocal callback_count
        callback_count += 1
        if callback_count != 1:
            return
        original_container.rename(displaced_container)
        original_container.mkdir(mode=0o700)

    monkeypatch.setattr(
        container_swap_spool,
        "mutation_guard",
        replace_container_name_once,
    )

    container_swap_spool._reconcile_owned_isolation_container_locked(original_container)

    displaced_temporary = displaced_container / original_temporary.name
    assert callback_count == 1
    assert original_container.is_dir()
    assert tuple(original_container.iterdir()) == ()
    assert displaced_container.is_dir()
    assert displaced_temporary.lstat().st_ino == original_temporary_inode
    assert displaced_temporary.read_text(encoding="utf-8") == "original-container-temporary"


def test_owned_entry_isolation_move_is_atomic_no_clobber_when_destination_appears(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class DestinationInjectingSpool(LabCommandSpool):
        def _before_owned_entry_move(self, _source: Path, container: Path) -> None:
            (container / "entry").write_text("injected", encoding="utf-8")

    spool = DestinationInjectingSpool(tmp_path / "commands")
    source = spool.pending_dir / f"{uuid4()}.json"
    source.write_text("original", encoding="utf-8")
    observed = source.lstat()

    with pytest.raises((FileExistsError, InvalidCommandEnvelopeError)):
        spool._isolate_owned_entry_locked(source, observed, reason="race")

    assert source.read_text(encoding="utf-8") == "original"
    containers = tuple(spool.quarantine_dir.glob("owned-entry-*.dead"))
    assert len(containers) == 1
    assert (containers[0] / "entry").read_text(encoding="utf-8") == "injected"
    LabCommandSpool(spool.root, max_isolation_records=1, max_isolation_bytes=1)
    assert source.read_text(encoding="utf-8") == "original"
    assert (containers[0] / "entry").read_text(encoding="utf-8") == "injected"

    swap_spool = LabCommandSpool(tmp_path / "swap-commands")
    swap_source = swap_spool.pending_dir / f"{uuid4()}.json"
    swap_source.write_text("original", encoding="utf-8")
    swap_observed = swap_source.lstat()

    def swap_before_move(_source: Path, _container: Path) -> None:
        swap_source.unlink()
        swap_source.write_text("replacement", encoding="utf-8")

    monkeypatch.setattr(swap_spool, "_before_owned_entry_move", swap_before_move)

    with pytest.raises(InvalidCommandEnvelopeError, match="changed before isolation"):
        swap_spool._isolate_owned_entry_locked(
            swap_source,
            swap_observed,
            reason="path_swap",
        )

    assert swap_source.read_text(encoding="utf-8") == "replacement"
    swap_containers = tuple(swap_spool.quarantine_dir.glob("owned-entry-*.dead"))
    assert len(swap_containers) == 1
    assert not os.path.lexists(swap_containers[0] / "entry")


def test_owned_entry_isolation_concurrent_movers_never_overwrite_an_entry(
    tmp_path: Path,
) -> None:
    move_barrier = Barrier(2)

    class RacingSpool(LabCommandSpool):
        def _before_owned_entry_move(self, _source: Path, _container: Path) -> None:
            move_barrier.wait(timeout=2)

    spool = RacingSpool(tmp_path / "commands")
    first = spool.pending_dir / f"{uuid4()}.json"
    second = spool.pending_dir / f"{uuid4()}.json"
    first.write_text("first", encoding="utf-8")
    second.write_text("second", encoding="utf-8")
    first_stat = first.lstat()
    second_stat = second.lstat()
    container = spool.quarantine_dir / f"owned-entry-{uuid4()}.dead"
    container.mkdir(mode=0o700)
    failures: list[BaseException] = []

    def move(source: Path, observed: os.stat_result) -> None:
        try:
            spool._move_bound_entry_into_container_locked(
                source,
                container,
                observed,
                expected_link_target=None,
            )
        except BaseException as exc:
            failures.append(exc)

    threads = [
        Thread(target=move, args=(first, first_stat)),
        Thread(target=move, args=(second, second_stat)),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=3)

    assert all(not thread.is_alive() for thread in threads)
    surviving = [path for path in (first, second, container / "entry") if path.exists()]
    assert sorted(path.read_text(encoding="utf-8") for path in surviving) == ["first", "second"]
    assert len(failures) == 1
    assert isinstance(failures[0], (FileExistsError, InvalidCommandEnvelopeError))


@pytest.mark.parametrize("lookalike_kind", ["regular", "symlink", "hardlink"])
def test_owned_entry_retention_never_claims_invalid_evidence_lookalikes(
    tmp_path: Path,
    lookalike_kind: str,
) -> None:
    root = tmp_path / "commands"
    spool = LabCommandSpool(root, max_isolation_records=1, max_isolation_bytes=1024 * 1024)
    source = spool.pending_dir / f"{uuid4()}.json"
    source.write_text("{broken", encoding="utf-8")
    with pytest.raises(InvalidCommandEnvelopeError) as captured:
        spool.load(source)
    assert captured.value.file_identity is not None
    isolated = spool.quarantine(captured.value.file_identity, reason="invalid_json")
    container = isolated.path.parent
    outside = tmp_path / "operator-note"
    outside.write_text("operator", encoding="utf-8")
    lookalike = container / "invalid-evidence-operator-note.raw"
    if lookalike_kind == "regular":
        lookalike.write_text("operator", encoding="utf-8")
    elif lookalike_kind == "symlink":
        lookalike.symlink_to(outside)
    else:
        os.link(outside, lookalike)
    exact_temporary = container / "operator-exact-name.tmp"
    os.link(outside, exact_temporary)
    exact_stat = exact_temporary.lstat()
    exact_lookalike = container / LabCommandSpool._invalid_evidence_name(
        exact_stat,
        content_hash=None,
    )
    exact_temporary.rename(exact_lookalike)
    assert LabCommandSpool._OWNED_INVALID_EVIDENCE_NAME.fullmatch(exact_lookalike.name)
    prefix_miss = container / "operator-invalid-evidence-deadbeef.raw"
    suffix_miss = container / "invalid-evidence-deadbeef.raw.note"
    prefix_miss.write_text("prefix", encoding="utf-8")
    suffix_miss.write_text("suffix", encoding="utf-8")
    for path in (container, container / "evidence.json", isolated.path):
        os.utime(path, ns=(1, 1), follow_symlinks=False)

    later = spool.pending_dir / f"{uuid4()}.json"
    later.write_text("{later", encoding="utf-8")
    with pytest.raises(InvalidCommandEnvelopeError) as later_error:
        spool.load(later)
    assert later_error.value.file_identity is not None
    spool.quarantine(later_error.value.file_identity, reason="later")

    assert os.path.lexists(lookalike)
    assert prefix_miss.read_text(encoding="utf-8") == "prefix"
    assert suffix_miss.read_text(encoding="utf-8") == "suffix"
    assert exact_lookalike.read_text(encoding="utf-8") == "operator"
    assert outside.read_text(encoding="utf-8") == "operator"
    assert container.exists()


@pytest.mark.parametrize("invalid_kind", ["regular", "symlink", "hardlink"])
def test_owned_entry_retention_removes_only_typed_exact_invalid_evidence(
    tmp_path: Path,
    invalid_kind: str,
) -> None:
    root = tmp_path / "commands"
    spool = LabCommandSpool(root)
    source = spool.pending_dir / f"{uuid4()}.json"
    source.write_text("{broken", encoding="utf-8")
    with pytest.raises(InvalidCommandEnvelopeError) as captured:
        spool.load(source)
    assert captured.value.file_identity is not None
    isolated = spool.quarantine(captured.value.file_identity, reason="invalid_json")
    container = isolated.path.parent
    evidence_path = container / "evidence.json"
    evidence_path.unlink()
    outside = tmp_path / "outside-invalid-evidence"
    outside.write_text("outside", encoding="utf-8")
    if invalid_kind == "regular":
        evidence_path.write_text("{truncated", encoding="utf-8")
    elif invalid_kind == "symlink":
        evidence_path.symlink_to(outside)
    else:
        os.link(outside, evidence_path)

    LabCommandSpool(root)
    evidence = lab_job_protocol._LabOwnedEntryIsolationEvidence.model_validate_json(
        evidence_path.read_bytes()
    )
    assert evidence.invalid_evidence is not None
    raw_path = container / evidence.invalid_evidence.name
    raw_stat = raw_path.lstat()
    assert LabCommandSpool._OWNED_INVALID_EVIDENCE_NAME.fullmatch(raw_path.name)
    assert (raw_stat.st_dev, raw_stat.st_ino, raw_stat.st_mode, raw_stat.st_nlink) == (
        evidence.invalid_evidence.device,
        evidence.invalid_evidence.inode,
        evidence.invalid_evidence.mode,
        evidence.invalid_evidence.link_count,
    )
    for path in (container, evidence_path, isolated.path, raw_path):
        os.utime(path, ns=(1, 1), follow_symlinks=False)

    bounded = LabCommandSpool(root, max_isolation_records=1, max_isolation_bytes=1024 * 1024)
    later = bounded.pending_dir / f"{uuid4()}.json"
    later.write_text("{later", encoding="utf-8")
    with pytest.raises(InvalidCommandEnvelopeError) as later_error:
        bounded.load(later)
    assert later_error.value.file_identity is not None
    bounded.quarantine(later_error.value.file_identity, reason="later")

    assert not container.exists()
    assert outside.read_text(encoding="utf-8") == "outside"
    assert outside.stat().st_nlink == 1


def test_owned_entry_retention_rejects_typed_invalid_evidence_content_change(
    tmp_path: Path,
) -> None:
    root = tmp_path / "commands"
    spool = LabCommandSpool(root)
    source = spool.pending_dir / f"{uuid4()}.json"
    source.write_text("{broken", encoding="utf-8")
    with pytest.raises(InvalidCommandEnvelopeError) as captured:
        spool.load(source)
    assert captured.value.file_identity is not None
    isolated = spool.quarantine(captured.value.file_identity, reason="invalid_json")
    container = isolated.path.parent
    evidence_path = container / "evidence.json"
    evidence_path.write_text("{truncated", encoding="utf-8")
    LabCommandSpool(root)
    evidence = lab_job_protocol._LabOwnedEntryIsolationEvidence.model_validate_json(
        evidence_path.read_bytes()
    )
    assert evidence.invalid_evidence is not None
    assert evidence.invalid_evidence.content_hash is not None
    raw_path = container / evidence.invalid_evidence.name
    raw_path.write_text("0123456789", encoding="utf-8")
    assert raw_path.stat().st_size == evidence.invalid_evidence.byte_count
    for path in (container, evidence_path, isolated.path, raw_path):
        os.utime(path, ns=(1, 1), follow_symlinks=False)

    bounded = LabCommandSpool(root, max_isolation_records=1, max_isolation_bytes=1024 * 1024)
    later = bounded.pending_dir / f"{uuid4()}.json"
    later.write_text("{later", encoding="utf-8")
    with pytest.raises(InvalidCommandEnvelopeError) as later_error:
        bounded.load(later)
    assert later_error.value.file_identity is not None
    bounded.quarantine(later_error.value.file_identity, reason="later")

    assert container.exists()
    assert raw_path.read_text(encoding="utf-8") == "0123456789"


def test_owned_entry_isolation_retention_unlinks_only_the_owned_hardlink_name(
    tmp_path: Path,
) -> None:
    root = tmp_path / "commands"
    spool = LabCommandSpool(root, max_isolation_records=1, max_isolation_bytes=1024 * 1024)
    outside = tmp_path / "outside-hardlink"
    outside.write_text("outside", encoding="utf-8")
    original = outside.stat()
    source = spool.pending_dir / f"{uuid4()}.json"
    os.link(outside, source)
    with pytest.raises(InvalidCommandEnvelopeError) as captured:
        spool.load(source)
    identity = captured.value.file_identity
    assert identity is not None
    isolated = spool.quarantine(identity, reason="invalid_hardlink")
    os.utime(isolated.path.parent, ns=(1, 1), follow_symlinks=False)
    os.utime(isolated.path.parent / "evidence.json", ns=(1, 1), follow_symlinks=False)

    later = spool.pending_dir / f"{uuid4()}.json"
    later.write_text("{later", encoding="utf-8")
    with pytest.raises(InvalidCommandEnvelopeError) as later_error:
        spool.load(later)
    assert later_error.value.file_identity is not None
    spool.quarantine(later_error.value.file_identity, reason="invalid_json")

    assert not isolated.path.parent.exists()
    assert outside.read_text(encoding="utf-8") == "outside"
    assert (outside.stat().st_dev, outside.stat().st_ino) == (
        original.st_dev,
        original.st_ino,
    )
    assert outside.stat().st_nlink == 1


def test_pending_order_is_fifo_across_reverse_request_ids_and_restart(tmp_path: Path) -> None:
    root = tmp_path / "commands"
    spool = LabCommandSpool(root)
    first = _submit_envelope(request_id=UUID("ffffffff-ffff-ffff-ffff-ffffffffffff"))
    second = _submit_envelope(request_id=UUID("00000000-0000-0000-0000-000000000001"))

    spool.publish(first)
    spool.publish(second)

    assert tuple(entry.envelope for entry in spool.pending()) == (first, second)
    restarted = LabCommandSpool(root)
    assert tuple(entry.envelope for entry in restarted.pending()) == (first, second)


def test_concurrent_process_publish_assigns_unique_persistent_fifo_sequence(
    tmp_path: Path,
) -> None:
    root = tmp_path / "commands"
    envelopes = tuple(_submit_envelope() for _ in range(8))
    script = """
import sys
from pathlib import Path
from rquant.lab_job_protocol import LabCommandEnvelope, LabCommandSpool
entry = LabCommandSpool(Path(sys.argv[1])).publish(
    LabCommandEnvelope.model_validate_json(sys.argv[2])
)
print(entry.path.name)
"""
    processes = tuple(
        subprocess.Popen(
            [sys.executable, "-c", script, str(root), envelope.model_dump_json()],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for envelope in envelopes
    )
    results = tuple(process.communicate(timeout=10) for process in processes)
    assert all(process.returncode == 0 for process in processes), results
    names = tuple(stdout.strip() for stdout, _stderr in results)

    sequences = {int(name.split("-", 1)[0]) for name in names}
    assert len(sequences) == len(envelopes)
    restarted = LabCommandSpool(root)
    pending_sequences = tuple(int(path.name.split("-", 1)[0]) for path in restarted.pending_paths())
    assert pending_sequences == tuple(sorted(sequences))

    later = restarted.publish(_submit_envelope())
    assert int(later.path.name.split("-", 1)[0]) > max(sequences)


def test_submit_precedes_controls_and_cancel_preempts_same_version_controls(
    tmp_path: Path,
) -> None:
    spool = LabCommandSpool(tmp_path / "commands")
    job_id = uuid4()
    submit = _submit_envelope(job_id=job_id)
    pause = LabCommandEnvelope(
        request_id=uuid4(),
        command=PauseJobCommand(job_id=job_id, expected_version=0, reason="pause"),
    )
    resume = LabCommandEnvelope(
        request_id=uuid4(),
        command=ResumeJobCommand(job_id=job_id, expected_version=0, reason="resume"),
    )
    cancel = LabCommandEnvelope(
        request_id=uuid4(),
        command=CancelJobCommand(job_id=job_id, expected_version=0, reason="cancel"),
    )
    for envelope in (submit, pause, resume, cancel):
        spool.publish(envelope)

    assert tuple(entry.envelope.command.command_type for entry in spool.pending()) == (
        "submit",
        "cancel",
        "pause",
        "resume",
    )


def test_submit_precedes_earlier_controls_after_spool_restart(tmp_path: Path) -> None:
    root = tmp_path / "commands"
    spool = LabCommandSpool(root)
    job_id = UUID("11111111-1111-1111-1111-111111111111")
    pause = LabCommandEnvelope(
        request_id=UUID("ffffffff-ffff-ffff-ffff-ffffffffffff"),
        command=PauseJobCommand(job_id=job_id, expected_version=0, reason="pause"),
    )
    cancel = LabCommandEnvelope(
        request_id=UUID("00000000-0000-0000-0000-000000000001"),
        command=CancelJobCommand(job_id=job_id, expected_version=0, reason="cancel"),
    )
    submit = _submit_envelope(
        request_id=UUID("88888888-8888-8888-8888-888888888888"),
        job_id=job_id,
    )
    for envelope in (pause, cancel, submit):
        spool.publish(envelope)

    restarted = LabCommandSpool(root)

    assert tuple(entry.envelope.command.command_type for entry in restarted.pending()) == (
        "submit",
        "cancel",
        "pause",
    )


def test_publish_after_ack_returns_existing_receipt_and_rejects_conflict(
    tmp_path: Path,
) -> None:
    spool = LabCommandSpool(tmp_path / "commands")
    request_id = uuid4()
    envelope = _submit_envelope(request_id=request_id)
    published = spool.publish(envelope)
    receipt = LabCommandReceipt(
        request_id=request_id,
        content_hash=envelope.content_hash,
        job_id=envelope.command.job_id,
        status="applied",
        reason="submitted",
        job_version=0,
    )
    acknowledged = spool.ack(published, receipt)

    replay = spool.publish(envelope)

    assert replay == acknowledged
    assert spool.pending() == ()
    with pytest.raises(RequestContentConflictError):
        spool.publish(_submit_envelope(request_id=request_id))
    assert spool.pending() == ()


def test_command_spool_internal_mutation_fence_prevents_quarantine_and_ack(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "commands"
    setup = LabCommandSpool(root)
    envelope = _submit_envelope()
    entry = setup.publish(envelope)
    calls = 0

    def mutation_guard() -> str:
        nonlocal calls
        calls += 1
        if calls >= 2:
            raise RuntimeError("runtime drifted inside spool lock")
        return "1" * 40

    spool = LabCommandSpool(root, mutation_guard=mutation_guard)
    mutation_guard()
    with pytest.raises(RuntimeError, match="inside spool lock"):
        spool.quarantine(entry, reason="invalid command")

    assert entry.path.exists()
    assert tuple(spool.quarantine_dir.iterdir()) == ()

    calls = 0
    receipt = LabCommandReceipt(
        request_id=envelope.request_id,
        content_hash=envelope.content_hash,
        job_id=envelope.command.job_id,
        status="applied",
        reason="submitted",
        job_version=0,
    )
    mutation_guard()
    with pytest.raises(RuntimeError, match="inside spool lock"):
        spool.ack(entry, receipt)

    assert entry.path.exists()
    assert tuple(spool.ack_dir.iterdir()) == ()

    real_fsync = os.fsync
    real_mkdir = os.mkdir
    bound_spool = LabCommandSpool(tmp_path / "bound-cleanup-commands")
    bound_source = bound_spool.pending_dir / f"{uuid4()}.json"
    bound_source.write_text("bound-source", encoding="utf-8")
    bound_observed = bound_source.lstat()
    bound_container: Path | None = None
    replacement_identity: tuple[int, int] | None = None
    fail_bound_fsync = False
    bound_guard_calls = 0

    def replacing_bound_guard() -> None:
        nonlocal bound_guard_calls, replacement_identity
        bound_guard_calls += 1
        if bound_container is None:
            return
        os.rmdir(bound_container)
        real_mkdir(bound_container, 0o700)
        replacement = bound_container.lstat()
        replacement_identity = (replacement.st_dev, replacement.st_ino)

    def tracking_bound_mkdir(
        path: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> None:
        nonlocal bound_container, fail_bound_fsync
        real_mkdir(path, mode, dir_fd=dir_fd)
        if str(path).startswith("owned-entry-"):
            bound_container = bound_spool.quarantine_dir / str(path)
            fail_bound_fsync = True

    def failing_bound_fsync(descriptor: int) -> None:
        nonlocal fail_bound_fsync
        if fail_bound_fsync:
            fail_bound_fsync = False
            raise OSError(errno.EIO, "forced bound cleanup")
        real_fsync(descriptor)

    bound_spool.mutation_guard = replacing_bound_guard
    with monkeypatch.context() as patch:
        patch.setattr(lab_job_protocol.os, "mkdir", tracking_bound_mkdir)
        patch.setattr(lab_job_protocol.os, "fsync", failing_bound_fsync)
        with pytest.raises(OSError, match="forced bound cleanup"):
            bound_spool._isolate_owned_entry_locked(
                bound_source,
                bound_observed,
                reason="bound_cleanup_guard_order",
            )

    assert bound_guard_calls == 2
    assert bound_container is not None
    replacement = bound_container.lstat()
    assert (replacement.st_dev, replacement.st_ino) == replacement_identity
    assert tuple(bound_container.iterdir()) == ()
    assert bound_source.read_text(encoding="utf-8") == "bound-source"


def test_command_spool_checks_guard_inside_initial_directory_creation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "commands"

    def mutation_guard() -> str:
        raise RuntimeError("runtime drifted before command spool initialization")

    with pytest.raises(RuntimeError, match="command spool initialization"):
        LabCommandSpool(root, mutation_guard=mutation_guard)

    assert not root.exists()

    real_mkdir = os.mkdir
    real_open = os.open
    unbound_spool = LabCommandSpool(tmp_path / "unbound-cleanup-commands")
    unbound_source = unbound_spool.pending_dir / f"{uuid4()}.json"
    unbound_source.write_text("unbound-source", encoding="utf-8")
    unbound_observed = unbound_source.lstat()
    container_created = False
    unbound_guard_phases: list[bool] = []

    def recording_unbound_guard() -> None:
        unbound_guard_phases.append(container_created)

    def tracking_unbound_mkdir(
        path: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> None:
        nonlocal container_created
        real_mkdir(path, mode, dir_fd=dir_fd)
        if str(path).startswith("owned-entry-"):
            container_created = True

    def failing_unbound_container_open(
        path: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        flags: int,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> int:
        if str(path).startswith("owned-entry-") and flags & getattr(os, "O_DIRECTORY", 0):
            raise OSError(errno.EIO, "forced unbound container open failure", path)
        return real_open(path, flags, mode, dir_fd=dir_fd)

    unbound_spool.mutation_guard = recording_unbound_guard
    with monkeypatch.context() as patch:
        patch.setattr(lab_job_protocol.os, "mkdir", tracking_unbound_mkdir)
        patch.setattr(lab_job_protocol.os, "open", failing_unbound_container_open)
        with pytest.raises(InvalidCommandEnvelopeError, match="identity changed"):
            unbound_spool._isolate_owned_entry_locked(
                unbound_source,
                unbound_observed,
                reason="unbound_cleanup_guard_order",
            )

    assert unbound_guard_phases == [False]
    assert tuple(unbound_spool.quarantine_dir.iterdir()) == ()
    assert unbound_source.read_text(encoding="utf-8") == "unbound-source"


def test_command_sequence_never_publishes_into_replaced_spool_root(tmp_path: Path) -> None:
    root = tmp_path / "commands"
    displaced = tmp_path / "commands.displaced"

    class ReplacingSequenceSpool(LabCommandSpool):
        def _after_sequence_stage(self, stage: str, _path: Path) -> None:
            if stage == "temporary_written":
                root.rename(displaced)
                root.mkdir(mode=0o700)

    spool = ReplacingSequenceSpool(root)

    with pytest.raises(InvalidCommandEnvelopeError, match="identity changed"):
        spool.publish(_submit_envelope())

    assert tuple(root.iterdir()) == ()
    assert not (root / ".delivery-sequence").exists()


def test_command_sequence_rejects_symlink_without_touching_external_file(
    tmp_path: Path,
) -> None:
    spool = LabCommandSpool(tmp_path / "commands")
    external = tmp_path / "external-sequence"
    external.write_text("41\n", encoding="ascii")
    spool._sequence_path.symlink_to(external)

    with pytest.raises(OSError):
        spool.publish(_submit_envelope())

    assert external.read_text(encoding="ascii") == "41\n"
    assert spool._sequence_path.is_symlink()


def test_replaced_spool_lock_cannot_create_parallel_mutation_authority(
    tmp_path: Path,
) -> None:
    root = tmp_path / "commands"
    first = LabCommandSpool(root)
    lock_path = first._lock_path
    displaced_lock = lock_path.with_suffix(".displaced")

    with first._exclusive_lock():
        lock_path.rename(displaced_lock)
        lock_path.touch(mode=0o600)
        lock_path.chmod(0o600)
        second = LabCommandSpool(root)
        second.publish(_submit_envelope())
        with pytest.raises(InvalidCommandEnvelopeError, match="lock identity changed"):
            first._guard_mutation()

    assert len(second.pending()) == 1


@pytest.mark.parametrize("unsafe_name", ["pending", "ack", "quarantine"])
def test_command_spool_rejects_symlinked_managed_directory_without_external_write(
    tmp_path: Path,
    unsafe_name: str,
) -> None:
    root = tmp_path / "commands"
    root.mkdir(mode=0o700)
    external = tmp_path / f"external-{unsafe_name}"
    external.mkdir()
    marker = external / "preserve.txt"
    marker.write_text("preserve", encoding="utf-8")
    (root / unsafe_name).symlink_to(external, target_is_directory=True)

    with pytest.raises(InvalidCommandEnvelopeError, match="private directory|unsafe"):
        LabCommandSpool(root)

    assert (root / unsafe_name).is_symlink()
    assert marker.read_text(encoding="utf-8") == "preserve"


@pytest.mark.parametrize("mode", [0o755, 0o711, 0o600])
def test_command_spool_rejects_non_private_managed_directory(
    tmp_path: Path,
    mode: int,
) -> None:
    root = tmp_path / "commands"
    root.mkdir(mode=0o700)
    pending = root / "pending"
    pending.mkdir(mode=0o700)
    pending.chmod(mode)

    with pytest.raises(InvalidCommandEnvelopeError, match="0700"):
        LabCommandSpool(root)

    assert stat.S_IMODE(pending.stat().st_mode) == mode


@pytest.mark.parametrize("unsafe_kind", ["symlink", "hardlink", "loose"])
def test_command_spool_rejects_unsafe_lock_without_touching_target(
    tmp_path: Path,
    unsafe_kind: str,
) -> None:
    spool = LabCommandSpool(tmp_path / "commands")
    lock_path = spool._lock_path
    lock_path.unlink()
    victim = tmp_path / "lock-victim"
    victim.write_text("preserve", encoding="utf-8")
    if unsafe_kind == "symlink":
        lock_path.symlink_to(victim)
    elif unsafe_kind == "hardlink":
        os.link(victim, lock_path)
    else:
        lock_path.write_text("", encoding="utf-8")
        lock_path.chmod(0o644)

    with pytest.raises(InvalidCommandEnvelopeError, match="spool lock"):
        LabCommandSpool(spool.root)

    assert victim.read_text(encoding="utf-8") == "preserve"
    if unsafe_kind == "symlink":
        assert lock_path.is_symlink()
    elif unsafe_kind == "hardlink":
        assert lock_path.stat().st_nlink == 2
    else:
        assert stat.S_IMODE(lock_path.stat().st_mode) == 0o644


def test_load_and_quarantine_reject_external_symlink_and_mismatched_basename(
    tmp_path: Path,
) -> None:
    spool = LabCommandSpool(tmp_path / "commands")
    envelope = _submit_envelope()
    victim = tmp_path / "victim.json"
    victim.write_bytes(lab_job_protocol.canonical_model_json_bytes(envelope))
    symlink = spool.pending_dir / f"{envelope.request_id}.json"
    symlink.symlink_to(victim)

    for candidate in (victim, symlink):
        with pytest.raises(InvalidCommandEnvelopeError):
            spool.load(candidate)
        with pytest.raises(InvalidCommandEnvelopeError):
            spool.quarantine(candidate, reason="unsafe")
    assert victim.exists()
    assert symlink.is_symlink()

    mismatched = spool.pending_dir / f"{uuid4()}.json"
    mismatched.write_bytes(lab_job_protocol.canonical_model_json_bytes(envelope))
    with pytest.raises(InvalidCommandEnvelopeError, match="request_id"):
        spool.load(mismatched)
    with pytest.raises(InvalidCommandEnvelopeError, match="request_id"):
        spool.quarantine(mismatched, reason="mismatch")
    assert mismatched.exists()

    pending = spool.publish(_submit_envelope())
    receipt = LabCommandReceipt(
        request_id=pending.envelope.request_id,
        content_hash=pending.envelope.content_hash,
        job_id=pending.envelope.command.job_id,
        status="applied",
        reason="submitted",
        job_version=0,
    )
    with pytest.raises(InvalidCommandEnvelopeError):
        spool.ack(pending.model_copy(update={"path": victim}), receipt)
    assert victim.exists()


def test_ack_and_quarantine_do_not_unlink_replacement_file(tmp_path: Path) -> None:
    spool = LabCommandSpool(tmp_path / "commands")
    envelope = _submit_envelope()
    entry = spool.publish(envelope)
    receipt = LabCommandReceipt(
        request_id=envelope.request_id,
        content_hash=envelope.content_hash,
        job_id=envelope.command.job_id,
        status="applied",
        reason="submitted",
        job_version=0,
    )
    entry.path.unlink()
    replacement = _submit_envelope(request_id=envelope.request_id)
    entry.path.write_bytes(lab_job_protocol.canonical_model_json_bytes(replacement))

    with pytest.raises(InvalidCommandEnvelopeError, match="replaced"):
        spool.ack(entry, receipt)
    with pytest.raises(InvalidCommandEnvelopeError, match="replaced"):
        spool.quarantine(entry, reason="semantic_conflict")

    assert entry.path.exists()
    assert tuple(spool.ack_dir.glob("*.json")) == ()
    assert tuple(spool.quarantine_dir.glob("*.bad")) == ()


def test_load_rejects_non_direct_lexical_path_alias(tmp_path: Path) -> None:
    spool = LabCommandSpool(tmp_path / "commands")
    entry = spool.publish(_submit_envelope())
    aliased = spool.pending_dir / "nested" / ".." / entry.path.name

    with pytest.raises(InvalidCommandEnvelopeError, match="unsafe spool path"):
        spool.load(aliased)


def test_malformed_load_identity_prevents_quarantine_of_replacement(tmp_path: Path) -> None:
    spool = LabCommandSpool(tmp_path / "commands")
    malformed = spool.pending_dir / "not-a-command.json"
    malformed.write_text("{broken", encoding="utf-8")
    with pytest.raises(InvalidCommandEnvelopeError) as captured:
        spool.load(malformed)
    identity = captured.value.file_identity
    assert identity is not None
    malformed.unlink()
    malformed.write_text("replacement", encoding="utf-8")

    with pytest.raises(InvalidCommandEnvelopeError, match="replaced"):
        spool.quarantine(identity, reason="invalid_envelope")

    assert malformed.read_text(encoding="utf-8") == "replacement"
    assert tuple(spool.quarantine_dir.glob("*.bad")) == ()


def test_symlink_load_identity_prevents_quarantine_of_replacement(tmp_path: Path) -> None:
    spool = LabCommandSpool(tmp_path / "commands")
    victim = tmp_path / "victim.json"
    victim.write_text("external", encoding="utf-8")
    symlink = spool.pending_dir / "not-a-command.json"
    symlink.symlink_to(victim)
    with pytest.raises(InvalidCommandEnvelopeError) as captured:
        spool.load(symlink)
    identity = captured.value.file_identity
    assert identity is not None
    assert identity.file_type == "symlink"
    assert identity.link_target == str(victim)
    symlink.unlink()
    symlink.write_text("replacement", encoding="utf-8")

    with pytest.raises(InvalidCommandEnvelopeError, match="replaced"):
        spool.quarantine(identity, reason="invalid_symlink")

    assert symlink.read_text(encoding="utf-8") == "replacement"
    assert victim.read_text(encoding="utf-8") == "external"
    assert tuple(spool.quarantine_dir.glob("*.symlink.bad.json")) == ()


def test_command_spool_rejects_duplicate_keys_in_pending_and_ack(tmp_path: Path) -> None:
    spool = LabCommandSpool(tmp_path / "commands")
    envelope = _submit_envelope()
    pending = spool.publish(envelope)
    pending.path.write_bytes(
        pending.path.read_bytes().replace(
            b'"schema_version":1',
            b'"schema_version":999,"schema_version":1',
            1,
        )
    )

    with pytest.raises(InvalidCommandEnvelopeError, match="duplicate JSON key"):
        spool.load(pending.path)

    pending.path.write_bytes(lab_job_protocol.canonical_model_json_bytes(envelope))
    receipt = LabCommandReceipt(
        request_id=envelope.request_id,
        content_hash=envelope.content_hash,
        job_id=envelope.command.job_id,
        status="applied",
        reason="submitted",
        job_version=0,
    )
    acknowledged = spool.ack(spool.load(pending.path), receipt)
    acknowledged.path.write_bytes(
        acknowledged.path.read_bytes().replace(
            b'"status":"applied"',
            b'"status":"rejected","status":"applied"',
            1,
        )
    )

    with pytest.raises(InvalidCommandEnvelopeError, match="duplicate JSON key"):
        spool.load_receipt(acknowledged.path)


def test_command_spool_rejects_nested_duplicate_key_during_quarantine_scan(
    tmp_path: Path,
) -> None:
    spool = LabCommandSpool(tmp_path / "commands")
    envelope = _submit_envelope()
    pending = spool.publish(envelope)
    pending.path.write_bytes(
        pending.path.read_bytes().replace(
            b'"command_type":"submit"',
            b'"command_type":"cancel","command_type":"submit"',
            1,
        )
    )

    with pytest.raises(InvalidCommandEnvelopeError) as captured:
        spool.load(pending.path)
    quarantined = spool.quarantine(
        captured.value.file_identity or pending.path,
        reason="duplicate_json_key",
    )
    assert quarantined.path.exists()
    assert spool.pending() == ()
