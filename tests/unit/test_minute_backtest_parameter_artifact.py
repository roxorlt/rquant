from __future__ import annotations

import importlib
import importlib.util
import json
from pathlib import Path
from types import ModuleType, SimpleNamespace
from uuid import uuid4

import pytest

from tests.unit.test_minute_backtest_parameter_adapter import (
    ParameterExecution,
)
from tests.unit.test_minute_backtest_parameter_adapter import (
    complete_seed as complete_seed,
)
from tests.unit.test_minute_backtest_parameter_adapter import (
    original_execution as original_execution,
)
from tests.unit.test_minute_backtest_parameter_adapter import (
    source_root as source_root,
)


def reader_module() -> ModuleType:
    assert importlib.util.find_spec("rquant.minute_backtest_parameter_artifact") is not None
    return importlib.import_module("rquant.minute_backtest_parameter_artifact")


@pytest.fixture(scope="module")
def wire_tables(original_execution: ParameterExecution) -> SimpleNamespace:
    from rquant.lab_artifact_preview import ArtifactCompleteTable
    from rquant.lab_artifacts import (
        LabParquetIdentity,
        _frame_dtype_identities,
        _table_content_hash,
    )

    restored = original_execution.wire.to_result()
    tables = []
    for item in restored.tables:
        frame = item.frame
        tables.append(
            ArtifactCompleteTable(
                parquet=LabParquetIdentity(
                    table_name=item.name,
                    row_count=len(frame),
                    columns=tuple(frame.columns),
                    dtypes=tuple(str(dtype) for dtype in frame.dtypes),
                    dtype_identities=_frame_dtype_identities(frame),
                    content_sha256=_table_content_hash(frame),
                ),
                rows=tuple(tuple(row) for row in frame.itertuples(index=False, name=None)),
            )
        )
    # This carrier exercises the real wire format only; it has no sealed authority.
    return SimpleNamespace(tables=tuple(tables))


def test_parameter_reader_keeps_original_full_read_and_typed_result() -> None:
    current = reader_module()
    from rquant.minute_backtest_artifact import MinuteSealedReplayReader, MinuteSealedReplayResult
    from rquant.minute_backtest_parameter_adapter import (
        MinuteParameterFormalParameters,
        MinuteParameterFormalReplayResult,
    )
    from rquant.minute_backtest_parameter_producer import MinuteParameterReplayCatalog

    reader = current.MinuteParameterSealedReplayReader
    result = current.MinuteParameterSealedReplayResult
    assert issubclass(reader, MinuteSealedReplayReader)
    assert issubclass(result, MinuteSealedReplayResult)
    assert reader.read is MinuteSealedReplayReader.read
    assert reader._complete_result.__func__ is MinuteSealedReplayReader._complete_result.__func__
    assert reader._catalog_model() is MinuteParameterReplayCatalog
    assert reader._parameter_model() is MinuteParameterFormalParameters
    assert reader._formal_result_model() is MinuteParameterFormalReplayResult
    assert reader._sealed_result_model() is result
    assert result.model_fields["kind"].default == "minute_parameter_replay"
    assert result.model_fields["result"].annotation is MinuteParameterFormalReplayResult


def test_parameter_plan_uses_exact_derived_identity_and_original_work(
    original_execution: ParameterExecution,
) -> None:
    current = reader_module()
    reader = object.__new__(current.MinuteParameterSealedReplayReader)
    reader.catalog = original_execution.catalog
    spec = original_execution.validated.spec
    adapter = reader._adapter()
    parameters = adapter.parameters(spec)
    assert adapter.expected(parameters) == original_execution.published.receipt
    assert len(parameters.native_strategy_id) == 55
    assert parameters.native_strategy_id == original_execution.result.replay.strategy_id
    assert parameters.native_strategy_version == 1
    (definition,) = reader._registry().plan(spec)
    assert definition.work_plan is not None
    assert (definition.adapter_id, definition.adapter_version, definition.work_plan.work_units) == (
        "minute-parameter-replay",
        "1",
        parameters.work_units,
    )
    assert definition.payload_hash == original_execution.validated.claim.payload_hash


def test_complete_parameter_wire_is_restored_without_claiming_a_seal(
    original_execution: ParameterExecution, wire_tables: SimpleNamespace
) -> None:
    result = reader_module().MinuteParameterSealedReplayReader._complete_result(wire_tables)
    assert result == original_execution.result
    assert result.publication == original_execution.published.receipt
    assert result.replay.parameters == result.publication.frozen.runtime.parameters
    assert (result.full_input_hash, result.core_input_hash, result.seed_hash) == (
        original_execution.result.full_input_hash,
        original_execution.result.core_input_hash,
        original_execution.result.seed_hash,
    )


@pytest.mark.parametrize(
    "table_name", ["signals", "fills", "daily_valuations", "execution_profile"]
)
def test_complete_parameter_wire_rejects_missing_or_repeated_tables(
    wire_tables: SimpleNamespace, table_name: str
) -> None:
    from rquant.minute_backtest_artifact import MinuteSealedReplayIntegrityError

    remaining = tuple(item for item in wire_tables.tables if item.parquet.table_name != table_name)
    reader = reader_module().MinuteParameterSealedReplayReader
    with pytest.raises(MinuteSealedReplayIntegrityError, match="exactly eight"):
        reader._complete_result(SimpleNamespace(tables=remaining))
    with pytest.raises(MinuteSealedReplayIntegrityError, match="exactly eight"):
        reader._complete_result(SimpleNamespace(tables=(*remaining, remaining[0])))


@pytest.mark.parametrize(
    "field", ["full_input_hash", "core_input_hash", "seed_hash", "parameter_hash"]
)
def test_complete_payload_rejects_binding_mutation(
    wire_tables: SimpleNamespace, field: str
) -> None:
    from rquant.minute_backtest_artifact import MinuteSealedReplayIntegrityError

    (summary,) = (
        item for item in wire_tables.tables if item.parquet.table_name == "replay_summary"
    )
    rows = [list(row) for row in summary.rows]
    index = summary.parquet.columns.index("payload")
    payload = json.loads(rows[0][index])
    if field == "parameter_hash":
        payload["replay"]["parameters"]["parameters"]["max_hold_days"] += 1
    else:
        payload[field] = "0" * 64
    rows[0][index] = json.dumps(payload)
    changed = summary.model_copy(update={"rows": tuple(tuple(row) for row in rows)})
    tables = tuple(changed if item is summary else item for item in wire_tables.tables)
    with pytest.raises(MinuteSealedReplayIntegrityError, match="payload is invalid"):
        reader_module().MinuteParameterSealedReplayReader._complete_result(
            SimpleNamespace(tables=tables)
        )


@pytest.mark.parametrize("table_name", ["fills", "daily_valuations", "execution_profile"])
def test_complete_parameter_wire_rejects_changed_cells(
    wire_tables: SimpleNamespace, table_name: str
) -> None:
    from rquant.minute_backtest_artifact import MinuteSealedReplayIntegrityError

    (target,) = (item for item in wire_tables.tables if item.parquet.table_name == table_name)
    assert target.rows
    rows = [list(row) for row in target.rows]
    rows[0][0] = "changed-original-cell"
    changed = target.model_copy(update={"rows": tuple(tuple(row) for row in rows)})
    tables = tuple(changed if item is target else item for item in wire_tables.tables)
    with pytest.raises(MinuteSealedReplayIntegrityError, match=f"conflicts: {table_name}"):
        reader_module().MinuteParameterSealedReplayReader._complete_result(
            SimpleNamespace(tables=tables)
        )


def test_parameter_reader_cannot_mix_original_lab_authorities(tmp_path: Path) -> None:
    from rquant.lab_artifact_preview import ArtifactPreviewReader
    from rquant.lab_jobs import LabJobReader

    first = LabJobReader(tmp_path / "first.sqlite3")
    second = LabJobReader(tmp_path / "second.sqlite3")
    preview = ArtifactPreviewReader(reader=first, artifact_root=tmp_path / "artifacts")
    with pytest.raises(ValueError, match="one original Lab authority"):
        reader_module().MinuteParameterSealedReplayReader(
            reader=first,
            artifact_reader=preview,
            submission_facade=SimpleNamespace(reader=second),
            catalog=None,
        )


def test_parameter_wire_does_not_authorize_a_missing_sealed_job(
    original_execution: ParameterExecution, tmp_path: Path
) -> None:
    from rquant.definition_registry import ImmutableDefinitionRegistry
    from rquant.experiment_registry import ExperimentRegistry
    from rquant.lab_artifact_preview import ArtifactPreviewReader
    from rquant.lab_job_center import LabCommandSubmissionFacade
    from rquant.lab_job_protocol import LabCommandSpool
    from rquant.lab_jobs import LabJobReader, LabJobStore
    from rquant.minute_backtest_parameter_definition import minute_parameter_research_registry

    current = reader_module()
    runtime = original_execution.published.receipt.frozen.runtime
    store = LabJobStore(tmp_path / "jobs.sqlite3")
    store.initialize()
    authority = LabJobReader(store.path)
    trust = tmp_path / "experiment-trust"
    trust.mkdir(mode=0o700)
    facade = LabCommandSubmissionFacade(
        reader=authority,
        spool=LabCommandSpool(tmp_path / "commands"),
        experiment_registry=ExperimentRegistry(
            trust / "registry.sqlite3", managed_trust_root=trust
        ),
        definition_registry=ImmutableDefinitionRegistry(
            tmp_path / "definitions",
            execution_registry=minute_parameter_research_registry(
                runtime.parameters, producer_commit=runtime.producer_commit
            ),
        ),
    )
    reader = current.MinuteParameterSealedReplayReader(
        reader=authority,
        artifact_reader=ArtifactPreviewReader(
            reader=authority, artifact_root=tmp_path / "artifacts"
        ),
        submission_facade=facade,
        catalog=original_execution.catalog,
    )
    assert (
        reader.read(
            uuid4(),
            owner_id=runtime.owner_id,
            native_id=runtime.strategy.strategy_id,
            native_version=runtime.strategy.strategy_version,
            as_of=runtime.available_at,
        )
        is None
    )
