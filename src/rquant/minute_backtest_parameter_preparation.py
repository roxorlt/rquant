"""Private complete recipe publication and exact recovery from installed facts."""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Literal, Mapping

from rquant.lab_artifacts import _ensure_private_directory
from rquant.metadata_catalog import ImmutableDuckDBMetadataCatalog
from rquant.minute_backtest_contracts import MinuteReplayModel, Sha256
from rquant.minute_backtest_parameter_fact_sources import build_minute_parameter_source_seed
from rquant.minute_backtest_parameter_producer import (
    MinuteParameterPreparedPublication, MinuteParameterPublicationReference,
    MinuteParameterReplayCatalog, PublishedMinuteParameterInput, publish_minute_parameter_input,
)
from rquant.minute_backtest_producer import _allowed_decision, _secure_private_bytes, _strict_json
from rquant.minute_backtest_publication_contracts import MAX_MINUTE_CONTROL_BYTES
from rquant.research_catalog import ResearchCatalog
from rquant.research_run_spec import DatasetSnapshotIdentity
from rquant.runtime_contracts import canonical_sha256
from rquant.storage.duckdb import DuckDBStore

if TYPE_CHECKING:
    from rquant.minute_backtest_commands import MinuteParameterRunConfig


class MinuteParameterPublicationPreparation(MinuteReplayModel):
    published: PublishedMinuteParameterInput
    prepared_publication: MinuteParameterPreparedPublication


class _MinuteParameterOperation(MinuteReplayModel):
    contract: Literal["minute-parameter-publication-operation/v1"] = "minute-parameter-publication-operation/v1"
    operation_key: Sha256
    config_hash: Sha256
    owner_id: str
    prepared_publication: MinuteParameterPreparedPublication


def _field_values(value: Mapping[str, object], prefix: str = "") -> dict[str, object]:
    fields: dict[str, object] = {}
    for name, item in value.items():
        path = prefix + name
        if isinstance(item, dict):
            fields.update(_field_values(item, path + "."))
        else:
            fields[path] = item
    return fields


def _publication_from_prepared(catalog: MinuteParameterReplayCatalog,
    prepared: MinuteParameterPreparedPublication,
) -> MinuteParameterPublicationPreparation:
    with catalog.open_prepared(prepared) as session:
        receipt = session._minute_gate_receipt
        published = PublishedMinuteParameterInput(receipt=receipt,
            reference=MinuteParameterPublicationReference(source_key=prepared.source_key,
                source_version=prepared.source_version, owner_id=prepared.owner_id,
                source=prepared.source, receipt=prepared.receipt),
            identity=DatasetSnapshotIdentity(snapshot_id=receipt.snapshot.snapshot_id,
                binding_hash=receipt.binding.binding_hash, audit_run_id=receipt.audit.audit_run_id),
            gate_decision=_allowed_decision(receipt))
    return MinuteParameterPublicationPreparation(published=published, prepared_publication=prepared)


def prepare_minute_parameter_publication(config: MinuteParameterRunConfig, *, owner_id: str,
    operation_key: str, catalog: MinuteParameterReplayCatalog, definitions_root: Path, now: datetime,
    expected_code_sha: str | None = None,
) -> MinuteParameterPublicationPreparation:
    from rquant.minute_backtest_commands import MinuteParameterRunConfig

    config = MinuteParameterRunConfig.model_validate(config)
    if len(operation_key) != 64 or any(char not in "0123456789abcdef" for char in operation_key):
        raise PermissionError("parameter publication requires its complete backend command hash")
    if catalog.prepared_root is None or catalog.snapshot_root is None or catalog.research_lake_root is None:
        raise PermissionError("parameter publication lacks its installed private producer roots")
    baseline = catalog.resolve_fact(source_key=config.source_key, source_version=config.source_version,
        owner_id=owner_id, full_input_hash=config.full_input_hash)
    reference = next(item for item in catalog.fact_sources if (item.source_key, item.source_version,
        item.owner_id, item.full_input_hash) == (config.source_key, config.source_version, owner_id, config.full_input_hash))
    original = baseline.frozen.runtime
    if expected_code_sha is not None and original.producer_commit != expected_code_sha:
        raise PermissionError("parameter installed facts differ from the current runtime code")
    if (config.parameters.parameters.family, config.parameters.parameters.freq) != (
        original.parameters.parameters.family, original.source_frequency):
        raise PermissionError("parameter recipe family/frequency differs from installed complete facts")
    before = _field_values(original.parameters.parameters.model_dump(mode="json"))
    after = _field_values(config.parameters.parameters.model_dump(mode="json"))
    changed = {name for name in before.keys() | after.keys() if before.get(name) != after.get(name)}
    if not changed.issubset(reference.supported_parameter_names):
        raise PermissionError("parameter source does not support every changed parameter: " + ", ".join(sorted(changed)))
    if now < baseline.frozen.provenance.published_at or config.deadline <= now:
        raise PermissionError("parameter actual source/publication/deadline times differ")
    for window in (config.protocol.train_range, config.protocol.validation_range, config.protocol.frozen_outer_test_range):
        if not original.start_date <= window.start_date <= window.end_date <= original.end_date:
            raise PermissionError("parameter protocol exceeds its full installed source range")
    policy = baseline.frozen.provenance.visibility_policy
    if policy is None or policy not in catalog.installed_policies:
        raise PermissionError("parameter derivative lacks an explicitly installed historical visibility policy")
    directory = catalog.prepared_root / operation_key
    operation_path = directory / "prepared.json"
    config_hash = canonical_sha256(config.model_dump(mode="json"))
    if directory.exists() or directory.is_symlink():
        _ensure_private_directory(directory, manage_existing=False, require_private_existing=True)
        if not operation_path.is_file() or operation_path.is_symlink():
            raise PermissionError("parameter original operation is incomplete; no complete publication can be recovered")
        data, physical = _secure_private_bytes(operation_path)
        if len(data) > MAX_MINUTE_CONTROL_BYTES:
            raise PermissionError("parameter operation exceeds original control budget")
        _strict_json(data)
        operation = _MinuteParameterOperation.model_validate_json(data)
        if (operation.operation_key, operation.config_hash, operation.owner_id) != (operation_key, config_hash, owner_id):
            raise PermissionError("parameter original operation conflicts with the complete request/owner")
        result = _publication_from_prepared(catalog, operation.prepared_publication)
        _secure_private_bytes(operation_path, physical)
        return result
    _ensure_private_directory(catalog.prepared_root, manage_existing=False, require_private_existing=True)
    directory.mkdir(mode=0o700)
    derived = build_minute_parameter_source_seed(baseline.frozen, parameters=config.parameters,
        definitions_root=definitions_root, candidate_root=directory / "candidate-originals",
        source_key="parameter."+canonical_sha256({"command": operation_key, "owner": owner_id,
            "config": config.model_dump(mode="json"), "baseline": baseline.frozen.full_input_hash}), now=now,
        visibility_policy=policy, study=config.study, formal_protocol=config.protocol,
        random_seed=config.random_seed, request_hash=config_hash)
    with DuckDBStore(directory / "metadata.duckdb") as metadata:
        metadata.path.chmod(0o600)
        published = publish_minute_parameter_input(derived, metadata_store=metadata,
            source_path=directory / "source.duckdb", receipt_path=directory / "receipt.json",
            catalog=ResearchCatalog(directory / "catalog.duckdb"), lake_root=catalog.research_lake_root,
            installed_policies=catalog.installed_policies, now=now)
    with ImmutableDuckDBMetadataCatalog.open(directory / "metadata.duckdb", forbidden_paths=catalog.forbidden_paths,
        snapshot_root=catalog.snapshot_root) as metadata:
        identity = metadata.descriptor
    carrier = MinuteParameterPreparedPublication.from_published(published, baseline_reference=reference,
        baseline_receipt=baseline, metadata_identity=identity)
    catalog.resolve_prepared(carrier)
    operation = _MinuteParameterOperation(operation_key=operation_key, config_hash=config_hash,
        owner_id=owner_id, prepared_publication=carrier)
    payload = operation.model_dump_json(exclude_computed_fields=True).encode("utf-8")
    if len(payload) > MAX_MINUTE_CONTROL_BYTES:
        raise PermissionError("parameter operation exceeds original control budget")
    descriptor = os.open(operation_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0), 0o600)
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())
    _secure_private_bytes(operation_path)
    return MinuteParameterPublicationPreparation(published=published, prepared_publication=carrier)
