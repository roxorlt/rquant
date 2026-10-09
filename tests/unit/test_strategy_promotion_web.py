"""The real command/effect and role authorities; synthetic sealed-evidence transport."""

from __future__ import annotations

import os
from datetime import timedelta
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from rquant.collaboration_roles import RoleEntry, RoleState
from rquant.page_control import PageControlConsumer, PageControlOutbox, PageControlService
from rquant.strategy_promotion import StrategyPromotionPageControlBackend
from rquant.strategy_promotion_commands import (
    ApprovePromotion,
    PreparePromotionApproval,
    RequestPromotionReview,
)
from rquant.strategy_promotion_contracts import PromotionEvidenceSelection, StrategyPromotionReview
from tests.unit.test_strategy_promotion import NOW, backend_fixture


def test_native_original_family_and_planned_row_keep_source_identity() -> None:
    from datetime import date
    from rquant.experiment_platform import HoldoutPolicy, NativeMinuteExperimentRequest
    from rquant.experiment_platform_projection import ExperimentFamilyFact, ExperimentPlannedSlotFact, ExperimentSearchContext
    from rquant.experiment_registry import DateRange
    from rquant.minute_backtest_formal import MinuteExperimentProtocol
    from rquant.strategy_promotion_contracts import NativeMinuteConfiguration, NativeMinuteSelection
    from rquant.web.experiment_platform_service import ExperimentWebService
    from tests.unit.test_strategy_promotion_contracts import target

    selected = target().model_copy(update={"source_kind": "builtin", "strategy_id": "n_shape"})
    configuration = NativeMinuteConfiguration(selection=NativeMinuteSelection(target=selected,
        source_key="original-minute", source_version=1, profile_hash="f" * 64),
        start_date=date(2026, 1, 1), end_date=date(2026, 1, 2))
    request = NativeMinuteExperimentRequest(name="原生研究", configurations=(configuration,),
        protocol=MinuteExperimentProtocol(train_range=DateRange(start_date=date(2026, 1, 1), end_date=date(2026, 1, 1)),
            validation_range=DateRange(start_date=date(2026, 1, 2), end_date=date(2026, 1, 2)),
            frozen_outer_test_range=DateRange(start_date=date(2026, 1, 3), end_date=date(2026, 1, 3))))
    family = ExperimentFamilyFact(owner=selected.owner_id, family_id="original-family", request_id=uuid4(),
        name=request.name, request=ExperimentSearchContext.from_request(request), registered_at=NOW,
        policy=HoldoutPolicy(version=1, months=0, updated_at=NOW), phase="search", parent_family_id=None,
        planned_count=1, potential_count=1, search_count=1)
    row = ExperimentWebService()._preparation(ExperimentPlannedSlotFact(index=0, configuration=configuration,
        definition_state="saved", input_prepared=True), family)
    assert row.configuration == configuration
    assert row.strategy_name == selected.name and row.strategy_version == selected.head.version
    assert row.rules is None


@pytest.mark.parametrize("actor,expected_status", [("alice", 200), ("viewer", 403)])
def test_original_private_context_handler_preserves_typed_head_to_real_owner(
    tmp_path: Path, actor: str, expected_status: int
) -> None:
    import json
    from email.message import Message
    from io import BytesIO
    from types import SimpleNamespace

    from rquant.strategy_authoring_admission import StrategyAuthoringAdmission, _handler
    from rquant.strict_json import canonical_json_bytes
    from tests.support.strategy_promotion_fixture import build_original_variant_fixture

    fixture = build_original_variant_fixture(tmp_path)
    try:
        parent = fixture.record.request.template
        original = fixture.backend.context(
            actor_id="alice",
            source_kind="template",
            strategy_id=parent.strategy_id,
            head=parent.head,
        )
        admission = StrategyAuthoringAdmission(
            fixture.control,
            source_catalog_provider=lambda _actor, _generation: fixture.binding.catalogs[0],
        )
        body = canonical_json_bytes(
            {
                "authenticated_actor_id": actor,
                "source_kind": "template",
                "strategy_id": parent.strategy_id,
                "head": parent.head.model_dump(mode="json"),
            }
        )
        handler = object.__new__(_handler())
        handler.server = SimpleNamespace(admission=admission)
        handler.path = "/v1/strategy-authoring-admission/promotion/context"
        handler.request_version = "HTTP/1.1"
        handler.command = "POST"
        handler.requestline = f"POST {handler.path} HTTP/1.1"
        handler.headers = Message()
        handler.headers["Content-Type"] = "application/json"
        handler.headers["Content-Length"] = str(len(body))
        handler.rfile = BytesIO(body)
        handler.wfile = BytesIO()
        handler.do_POST()
        headers, response = handler.wfile.getvalue().split(b"\r\n\r\n", 1)
        assert headers.split(b"\r\n", 1)[0].split()[1] == str(expected_status).encode()
        if expected_status == 200:
            assert json.loads(response) == original.model_dump(mode="json")
            assert len(original.candidates) == 4
            assert original.candidates[0].template_parent.head == parent.head
        else:
            assert json.loads(response) == {"error": "actor_forbidden"}
    finally:
        fixture.close()


def test_original_snapshot_reuses_typed_family_preparations_only_in_its_own_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.experiment_platform import ExperimentPreparationReceipt
    from tests.support.strategy_promotion_fixture import build_original_variant_fixture

    fixture = build_original_variant_fixture(tmp_path)
    try:
        observed = fixture.time[0]
        baseline = fixture.projection.snapshot(observed)
        assert baseline is not None
        with fixture.platform.registry._connect() as connection:
            original_rows = connection.execute(
                "SELECT payload_json FROM experiment_prepared_child "
                "WHERE family_id=? ORDER BY child_index",
                (fixture.record.family_id,),
            ).fetchall()
        expected = tuple(row[0] for row in original_rows)
        assert len(expected) == len(fixture.record.actual_configurations) == 4
        checked: list[str] = []
        validate = ExperimentPreparationReceipt.model_validate_json

        def actual_validate(raw: str, **options: object) -> ExperimentPreparationReceipt:
            checked.append(raw)
            return validate(raw, **options)

        monkeypatch.setattr(
            ExperimentPreparationReceipt, "model_validate_json", staticmethod(actual_validate)
        )
        current = fixture.projection.snapshot(observed)
        assert current == baseline
        assert tuple(checked) == expected
        assert len(current.attempts) == 4 and current.families == baseline.families
        checked.clear()
        assert fixture.projection.snapshot(observed) == baseline
        assert tuple(checked) == expected
    finally:
        fixture.close()


@pytest.mark.parametrize("damage", ["invalid_json", "missing_preparation", "duplicate_spec"])
def test_original_snapshot_rechecks_full_prepared_rows_and_exact_matches_on_next_read(
    tmp_path: Path, damage: str,
) -> None:
    from rquant.experiment_registry import ExperimentRegistryError
    from tests.support.strategy_promotion_fixture import build_original_variant_fixture

    fixture = build_original_variant_fixture(tmp_path)
    try:
        assert len(fixture.projection.snapshot(fixture.time[0]).attempts) == 4
        with fixture.platform.registry._connect() as connection:
            if damage == "invalid_json":
                connection.execute(
                    "UPDATE experiment_prepared_child SET payload_json='{}' "
                    "WHERE family_id=? AND child_index=0",
                    (fixture.record.family_id,),
                )
            elif damage == "missing_preparation":
                connection.execute(
                    "DELETE FROM experiment_prepared_child WHERE family_id=? AND child_index=0",
                    (fixture.record.family_id,),
                )
            else:
                original = connection.execute(
                    "SELECT payload_json FROM experiment_prepared_child "
                    "WHERE family_id=? AND child_index=0",
                    (fixture.record.family_id,),
                ).fetchone()[0]
                connection.execute(
                    "UPDATE experiment_prepared_child SET payload_json=? "
                    "WHERE family_id=? AND child_index=1",
                    (original, fixture.record.family_id),
                )
            connection.commit()
        with pytest.raises(ExperimentRegistryError, match="path changed during read") as rejected:
            fixture.projection.snapshot(fixture.time[0])
        assert isinstance(rejected.value.__cause__, ValueError)
        if damage == "invalid_json":
            assert "validation errors for ExperimentPreparationReceipt" in str(
                rejected.value.__cause__
            )
        else:
            assert str(rejected.value.__cause__) == "private attempt has no exact preparation"
    finally:
        fixture.close()


def test_original_context_reuses_complete_candidate_inputs_and_jobs_only_in_one_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.experiment_platform import ExperimentPreparationReceipt
    from rquant.lab_jobs import LabJobReader
    from tests.support.strategy_promotion_fixture import build_original_variant_fixture

    fixture = build_original_variant_fixture(tmp_path)
    try:
        parent = fixture.record.request.template
        options = dict(
            actor_id=fixture.record.owner,
            source_kind="template",
            strategy_id=parent.strategy_id,
            head=parent.head,
        )
        baseline = fixture.backend.context(**options)
        checked: list[str] = []
        jobs: list[UUID] = []
        validate = ExperimentPreparationReceipt.model_validate_json
        parse_job = LabJobReader._job_from_row

        def actual_validate(raw: str, **kwargs: object) -> ExperimentPreparationReceipt:
            checked.append(raw)
            return validate(raw, **kwargs)

        def actual_job(row: object) -> object:
            job = parse_job(row)
            jobs.append(job.job_id)
            return job

        monkeypatch.setattr(
            ExperimentPreparationReceipt, "model_validate_json", staticmethod(actual_validate)
        )
        monkeypatch.setattr(LabJobReader, "_job_from_row", staticmethod(actual_job))
        for _ in range(2):
            checked.clear()
            jobs.clear()
            current = fixture.backend.context(**options)
            assert current == baseline
            assert len(current.candidates) == len(checked) == len(set(checked)) == 4
            assert len(jobs) == len(set(jobs)) == 4
            assert set(jobs) == {candidate.job_id for candidate in current.candidates}
    finally:
        fixture.close()


def test_original_context_registration_cannot_accept_other_facts_or_escape_its_read(
    tmp_path: Path,
) -> None:
    from tests.support.strategy_promotion_fixture import build_original_variant_fixture

    fixture = build_original_variant_fixture(tmp_path)
    try:
        with fixture.projection.context_read(fixture.time[0]) as read:
            fact = read.snapshot.attempts[0]
            expected = fixture.backend.domain.source._registration(fact)
            assert read.registration(fact) == expected
            for change in (
                {"owner": "viewer"},
                {"family_id": "experiment-search:" + "0" * 64},
                {"spec_hash": "0" * 64},
            ):
                forged = type(fact).model_validate(fact.model_dump(mode="python") | change)
                with pytest.raises(PermissionError, match="original full read"):
                    read.registration(forged)
        with pytest.raises(ValueError, match="read has ended"):
            read.registration(fact)
    finally:
        fixture.close()


def test_original_context_rejects_lab_storage_changed_during_its_full_read(tmp_path: Path) -> None:
    from rquant.experiment_registry import ExperimentRegistryError
    from tests.support.strategy_promotion_fixture import build_original_variant_fixture

    fixture = build_original_variant_fixture(tmp_path)
    try:
        with (
            pytest.raises(ExperimentRegistryError) as rejected,
            fixture.projection.context_read(fixture.time[0]) as read,
        ):
            fact = read.snapshot.attempts[0]
            assert read.registration(fact).spec.spec_fingerprint == (
                fact.attempt.spec.strategy_spec_fingerprint
            )
            path = fixture.foundation.reader.path
            original = path.stat()
            os.utime(path, ns=(original.st_atime_ns, original.st_mtime_ns + 1))
        assert str(rejected.value.__cause__) == "private Lab source changed during context read"
        with pytest.raises(ValueError, match="read has ended"):
            read.registration(fact)
    finally:
        fixture.close()


def test_original_private_snapshot_uses_one_full_lab_snapshot_for_all_parent_jobs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.lab_jobs import LabJobReader
    from tests.support.strategy_promotion_fixture import build_original_variant_fixture

    fixture = build_original_variant_fixture(tmp_path)
    try:
        baseline = fixture.projection.snapshot(fixture.time[0])
        connected: list[Path] = []
        connect = LabJobReader._connect

        def original_connect(reader: LabJobReader) -> object:
            connected.append(reader.path)
            return connect(reader)

        monkeypatch.setattr(LabJobReader, "_connect", original_connect)
        for _ in range(2):
            connected.clear()
            current = fixture.projection.snapshot(fixture.time[0])
            assert current == baseline and len(current.attempts) == 4
            assert connected == [fixture.foundation.reader.path]
    finally:
        fixture.close()


def test_original_review_authority_reuses_full_prepared_only_inside_its_current_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.experiment_platform import ExperimentPreparationReceipt
    from tests.support.strategy_promotion_fixture import build_original_variant_fixture

    fixture = build_original_variant_fixture(tmp_path)
    try:
        checked: list[str] = []
        validate = ExperimentPreparationReceipt.model_validate_json

        def actual_validate(raw: str, **options: object) -> ExperimentPreparationReceipt:
            checked.append(raw)
            return validate(raw, **options)

        monkeypatch.setattr(
            ExperimentPreparationReceipt, "model_validate_json", staticmethod(actual_validate)
        )
        expected = None
        for _ in range(2):
            checked.clear()
            with fixture.projection.context_read(fixture.time[0]) as read:
                assert len(read.snapshot.attempts) == 4
                current = []
                for fact in read.snapshot.attempts:
                    job = fixture.foundation.reader.get_job(fact.child.job_id)
                    receipt = fixture.projection.authority.authorize(job, fact.owner)
                    current.append(receipt)
                    assert receipt.prepared.registration == read.registration(fact)
                    assert read.authorize(job, fact.owner) == receipt
                    for changed_job, changed_owner in (
                        (job, "viewer"),
                        (job.model_copy(update={"version": job.version + 1}), fact.owner),
                    ):
                        with pytest.raises(PermissionError, match="original full read"):
                            fixture.projection.authority.authorize(changed_job, changed_owner)
                assert len(checked) == len(set(checked)) == 4
            with pytest.raises(ValueError, match="read has ended"):
                read.authorize(job, fact.owner)
            if expected is None:
                expected = current
            assert current == expected
        checked.clear()
        assert fixture.projection.authority.authorize(job, fact.owner) == current[-1]
        assert len(checked) == 1
    finally:
        fixture.close()


@pytest.mark.parametrize("change_lab", [False, True])
def test_original_review_missing_seal_still_finishes_its_complete_source_fence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, change_lab: bool,
) -> None:
    from rquant.experiment_registry import ExperimentRegistryError
    from tests.support.strategy_promotion_fixture import build_original_variant_fixture

    fixture = build_original_variant_fixture(tmp_path)
    try:
        parent = fixture.record.request.template
        selected = fixture.backend.context(
            actor_id=fixture.record.owner, source_kind="template",
            strategy_id=parent.strategy_id, head=parent.head,
        ).candidates[0]
        source = fixture.backend.domain.source

        def unscoped_read(*args: object, **kwargs: object) -> object:
            raise AssertionError("review must use the original complete read scope")

        monkeypatch.setattr(fixture.projection, "snapshot", unscoped_read)
        snapshot = fixture.projection._snapshot_in_connection

        def original_snapshot(*args: object, **kwargs: object) -> object:
            value = snapshot(*args, **kwargs)
            if change_lab:
                path = fixture.foundation.reader.path
                before = path.stat()
                os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns + 1))
            return value

        monkeypatch.setattr(fixture.projection, "_snapshot_in_connection", original_snapshot)

        def read_original() -> object:
            return source.read(
                selected.target, family_id=selected.selection.family_id,
                experiment_id=selected.selection.experiment_id, as_of=fixture.time[0],
            )

        if change_lab:
            with pytest.raises(ExperimentRegistryError) as rejected:
                read_original()
            assert str(rejected.value.__cause__) == "private Lab source changed during context read"
        else:
            bundle = read_original()
            assert bundle.target == selected.target
            assert bundle.validation is None
            assert bundle.missing == ("原验证结果尚未完整封存。",)
    finally:
        fixture.close()


@pytest.mark.parametrize("damage", ["preparation_owner", "prepared_spec", "family_count"])
def test_original_context_next_read_rechecks_mutated_full_preparation_and_family(
    tmp_path: Path, damage: str,
) -> None:
    import json

    from rquant.experiment_registry import ExperimentRegistryError
    from tests.support.strategy_promotion_fixture import build_original_variant_fixture

    fixture = build_original_variant_fixture(tmp_path)
    try:
        parent = fixture.record.request.template
        options = dict(
            actor_id=fixture.record.owner,
            source_kind="template",
            strategy_id=parent.strategy_id,
            head=parent.head,
        )
        assert len(fixture.backend.context(**options).candidates) == 4
        with fixture.platform.registry._connect() as connection:
            if damage == "family_count":
                row = connection.execute(
                    "SELECT payload_json FROM experiment_family_request WHERE family_id=?",
                    (fixture.record.family_id,),
                ).fetchone()
                changed = json.loads(row[0])
                changed["actual_configurations"].pop()
                connection.execute(
                    "UPDATE experiment_family_request SET payload_json=? WHERE family_id=?",
                    (json.dumps(changed), fixture.record.family_id),
                )
            else:
                row = connection.execute(
                    "SELECT payload_json FROM experiment_prepared_child "
                    "WHERE family_id=? AND child_index=0",
                    (fixture.record.family_id,),
                ).fetchone()
                changed = json.loads(row[0])
                if damage == "preparation_owner":
                    changed["owner"] = "viewer"
                else:
                    changed["prepared"]["formal_plan"]["spec"][
                        "strategy_spec_fingerprint"
                    ] = "0" * 64
                connection.execute(
                    "UPDATE experiment_prepared_child SET payload_json=? "
                    "WHERE family_id=? AND child_index=0",
                    (json.dumps(changed), fixture.record.family_id),
                )
            connection.commit()
        with pytest.raises(ExperimentRegistryError) as rejected:
            fixture.backend.context(**options)
        assert isinstance(rejected.value.__cause__, ValueError)
    finally:
        fixture.close()


def test_original_m13_parent_selects_exact_private_candidate_versions(tmp_path: Path) -> None:
    from tests.support.strategy_promotion_fixture import build_original_variant_fixture

    fixture = build_original_variant_fixture(tmp_path)
    try:
        parent = fixture.record.request.template
        context = fixture.backend.context(
            actor_id="alice",
            source_kind="template",
            strategy_id=parent.strategy_id,
            head=parent.head,
        )
        assert context.original_metadata_identity == fixture.binding.original.identity()
        assert context.metadata_identity == fixture.binding.private.identity()
        assert len(context.candidates) == len(fixture.record.actual_configurations) == 4
        assert context.can_evaluate and context.can_approve
        snapshot = fixture.projection.snapshot(fixture.time[0])
        for candidate in context.candidates:
            fact = next(
                f
                for f in snapshot.attempts
                if f.attempt.spec.experiment_id == candidate.selection.experiment_id
            )
            assert candidate.template_parent == parent
            assert candidate.target.strategy_id != parent.strategy_id
            version = fixture.binding.private.get_current(
                candidate.target.strategy_id, owner_id="alice"
            )
            assert version.head == candidate.target.head and candidate.is_current
            assert (
                candidate.input_hash,
                candidate.spec_hash,
                candidate.manifest_hash,
                candidate.result_hash,
            ) == (fact.input_hash, fact.spec_hash, fact.manifest_hash, fact.result_hash)
            selected = fixture.backend.context(
                actor_id="alice",
                source_kind="template",
                strategy_id=candidate.target.strategy_id,
                head=candidate.target.head,
            )
            assert selected.candidates == (candidate,)
        assert (
            fixture.binding.original.get_current(parent.strategy_id, owner_id="alice").head
            == parent.head
        )
        with pytest.raises((PermissionError, KeyError)):
            fixture.backend.context(
                actor_id="viewer",
                source_kind="template",
                strategy_id=parent.strategy_id,
                head=parent.head,
            )
    finally:
        fixture.close()


def test_full_history_fixture_installs_original_wf_and_full_parent_without_terminal_status(
    tmp_path: Path,
) -> None:
    from rquant.experiment_registry import ExperimentStatus
    from rquant.strategy_promotion_walk_forward import StrategyPromotionWalkForwardBackend
    from tests.support.strategy_promotion_fixture import (
        NEUTRAL_GENERATION,
        build_original_promotion_fixture,
        install_original_empty_paper,
        install_original_paper_research,
        record_original_twenty_paper_closes,
    )

    fixture = build_original_promotion_fixture(tmp_path)
    try:
        source = fixture.backend.domain.source
        assert type(source.walk_forward) is StrategyPromotionWalkForwardBackend
        assert source.walk_forward.runs.store is fixture.binding.private
        assert source.walk_forward.runs.facade is fixture.foundation.commands
        assert source.walk_forward.runs.preparer.experiments is fixture.platform.registry
        assert len(fixture.record.actual_configurations) == 4
        assert (
            fixture.record.request.protocol.validation_range.end_date
            < fixture.record.request.protocol.frozen_outer_test_range.start_date
        )
        prepared = fixture.platform.preparation("alice", fixture.record.family_id, 0).prepared
        dates = tuple(day.trade_date for day in prepared.frozen.request.days)
        assert len(dates) == 84 and len(prepared.frozen.request.calendar.dates) >= 106
        attempts = fixture.platform.registry.list_family_attempts(fixture.record.family_id)
        assert len(attempts) == 4 and all(
            attempt.status in {ExperimentStatus.REGISTERED, ExperimentStatus.RUNNING}
            for attempt in attempts
        )
        for attempt in attempts:
            assert attempt.outcome is None
        snapshot = fixture.projection.snapshot(fixture.clock())
        assert len(snapshot.attempts) == 4 and all(
            fact.manifest_hash is None and fact.result_hash is None for fact in snapshot.attempts
        )
        assert (
            fixture.binding.original.list_current(owner_id="alice")[0].head
            == fixture.record.request.template.head
        )
        parent = fixture.record.request.template
        context = fixture.backend.context(
            actor_id="alice",
            source_kind="template",
            strategy_id=parent.strategy_id,
            head=parent.head,
        )
        chosen = context.candidates[0]
        from rquant.strategy_template_run_commands import RunStrategyTemplate

        runs = source.walk_forward.runs
        child = RunStrategyTemplate(
            command_id=str(UUID(int=50000)),
            requested_at=fixture.clock(),
            generation_id=fixture.binding.catalogs[0].generation_id,
            strategy_id=chosen.target.strategy_id,
            head=chosen.target.head,
            expected_head=chosen.target.head,
            start_date=dates[0],
            end_date=dates[23],
            initial_cash="100000",
        )
        compiled = runs.compile(child, owner_id="alice", expected_identity=runs.expected_identity)
        assert compiled.accepted.request == child
        assert (
            compiled.accepted.spec.parameters.start_date == dates[0]
            and compiled.accepted.spec.parameters.end_date == dates[23]
        )
        assert (
            fixture.binding.private.accepted_run(
                child, owner_id="alice", expected_identity=runs.expected_identity
            )
            == compiled.accepted
        )
        from rquant.strategy_template_adapter import build_strategy_template_adapter_catalog
        from rquant.strategy_template_source import freeze_strategy_template_source

        version = build_strategy_template_adapter_catalog(
            fixture.binding.private,
            expected_identity=fixture.binding.expected_private_identity,
            selected_keys=((chosen.target.strategy_id, chosen.target.head.version),),
        ).versions[0]
        neutral_source = runs.preparer.source_provider("alice", NEUTRAL_GENERATION, version)
        neutral_request = child.model_copy(update={"generation_id": NEUTRAL_GENERATION})
        neutral = freeze_strategy_template_source(neutral_source, version, neutral_request)
        assert neutral.definition == version.definition and neutral.rules == version.rules
        assert neutral.request.execution_cost_spec == prepared.frozen.request.execution_cost_spec
        assert len(neutral.days) == 24 and all(
            day.entry.eligible_codes == () for day in neutral.days
        )
        assert neutral.source_material_hash != prepared.frozen.source_material_hash
        paper = install_original_empty_paper(fixture, chosen.target)
        assert (
            paper.runtime.state.configuration.binding.cost_spec_id != chosen.target.cost_fingerprint
        )
        context = fixture.backend.context(
            actor_id="alice",
            source_kind="template",
            strategy_id=parent.strategy_id,
            head=parent.head,
        )
        assert (
            len(context.paper_accounts) == 1
            and context.paper_accounts[0].target_key == chosen.target.version_key
        )
        install_original_paper_research(fixture, paper)
        assert (
            fixture.foundation.commands.paper_directory
            is fixture.foundation.scheduler.paper_directory
        )
        approved_at = fixture.clock()
        forward_dates = record_original_twenty_paper_closes(fixture, paper, approved_at=approved_at)
        view = paper.read(as_of=fixture.clock())
        assert len(forward_dates) == len(view.nav) == 20
        assert view.complete_comparison_dates() == forward_dates
        assert view.frame.history == () and view.frame.account.nav == 1000
        assert all(
            point.status == "complete" and point.normalized_nav == 1 and point.daily_return == 0
            for point in view.nav
        )
        assert (
            paper.research_results.backend.preparer.backtest_reader.store is fixture.binding.private
        )
        assert view.band is None and view.recent_research == ()
    finally:
        fixture.close()


def test_worker_fixture_drains_original_pending_child_receipts_before_precheck(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.strategy_template_run_commands import RunStrategyTemplate
    from tests.support.strategy_promotion_fixture import (
        build_original_promotion_fixture,
        seal_original_promotion_jobs,
    )

    fixture = build_original_promotion_fixture(tmp_path)
    try:
        runs = fixture.backend.domain.source.walk_forward.runs
        parent = fixture.record.request.template
        candidate = fixture.backend.context(
            actor_id="alice",
            source_kind="template",
            strategy_id=parent.strategy_id,
            head=parent.head,
        ).candidates[0]
        prepared = fixture.platform.preparation("alice", fixture.record.family_id, 0).prepared
        dates = tuple(day.trade_date for day in prepared.frozen.request.days)
        children = tuple(
            RunStrategyTemplate(
                command_id=str(UUID(int=51000 + index)),
                requested_at=fixture.clock(),
                generation_id=fixture.binding.catalogs[0].generation_id,
                strategy_id=candidate.target.strategy_id,
                head=candidate.target.head,
                expected_head=candidate.target.head,
                start_date=dates[0],
                end_date=dates[23 + index],
                initial_cash="100000",
            )
            for index in range(6)
        )
        compiled = tuple(
            runs.compile(child, owner_id="alice", expected_identity=runs.expected_identity)
            for child in children
        )
        receipts = tuple(runs.submit(command) for command in compiled)
        jobs = tuple(receipt.job_id for receipt in receipts)
        assert all(fixture.foundation.reader.get_job(job_id) is None for job_id in jobs)
        assert len(fixture.foundation.scheduler.spool.pending_paths(limit=64)) == 6

        class WorkerConstructionBoundaryError(Exception):
            pass

        def stop_before_worker(**_: object) -> object:
            for command, receipt in zip(compiled, receipts, strict=True):
                job = fixture.foundation.reader.get_job(receipt.job_id)
                assert job is not None and job.spec == command.accepted.spec
                assert fixture.foundation.reader.get_finalization_snapshot(receipt.job_id) is None
                assert (
                    runs.store.lookup_command(
                        command.original(),
                        owner_id="alice",
                        expected_identity=runs.expected_identity,
                    )
                    == receipt
                )
            raise WorkerConstructionBoundaryError

        monkeypatch.setattr(
            "rquant.lab_worker.build_builtin_shard_runtime_manifest", stop_before_worker
        )
        with pytest.raises(WorkerConstructionBoundaryError):
            seal_original_promotion_jobs(fixture, jobs)
        assert fixture.foundation.scheduler.spool.pending_paths(limit=64) == ()
    finally:
        fixture.close()


def test_worker_fixture_keeps_missing_job_guard_before_worker(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from tests.support.strategy_promotion_fixture import (
        build_original_promotion_fixture,
        seal_original_promotion_jobs,
    )

    fixture = build_original_promotion_fixture(tmp_path)
    try:

        def unexpected_worker(**_: object) -> object:
            raise AssertionError("unknown UUID must not reach worker construction")

        monkeypatch.setattr(
            "rquant.lab_worker.build_builtin_shard_runtime_manifest", unexpected_worker
        )
        missing = UUID(int=999001)
        with pytest.raises(ValueError, match="original Lab job must already be admitted"):
            seal_original_promotion_jobs(fixture, (missing,))
        assert fixture.foundation.reader.get_job(missing) is None
    finally:
        fixture.close()


def test_forward_clock_jump_releases_original_lease_and_reacquires_without_expiry_override(
    tmp_path: Path,
) -> None:
    from rquant.lab_jobs import SchedulerLeaseFencedError
    from tests.support.strategy_promotion_fixture import (
        build_original_promotion_fixture,
        install_original_empty_paper,
        record_original_twenty_paper_closes,
    )

    fixture = build_original_promotion_fixture(tmp_path)
    try:
        parent = fixture.record.request.template
        chosen = fixture.backend.context(
            actor_id=fixture.record.owner,
            source_kind="template",
            strategy_id=parent.strategy_id,
            head=parent.head,
        ).candidates[0]
        scheduler = fixture.foundation.scheduler
        prior = scheduler.lease
        assert prior is not None and prior.expires_at > fixture.clock()
        source = install_original_empty_paper(fixture, chosen.target)
        dates = record_original_twenty_paper_closes(fixture, source, approved_at=fixture.clock())
        before = source.read(as_of=fixture.clock()).nav
        assert len(dates) == len(before) == 20
        assert scheduler.lease is None
        with pytest.raises(SchedulerLeaseFencedError, match="stale or expired"):
            scheduler.store.renew_scheduler_lease(
                prior, lease_seconds=scheduler.lease_seconds, now=fixture.clock()
            )
        scheduler.run_once()
        fresh = scheduler.lease
        assert fresh is not None and fresh.fencing_token > prior.fencing_token
        assert fresh.expires_at > fixture.clock() and fresh.owner_id == prior.owner_id
        assert source.read(as_of=fixture.clock()).nav == before
        scheduler.release()
        assert scheduler.lease is None
    finally:
        fixture.close()


def test_fixture_experiment_ingress_preserves_public_refusal_and_original_effect(
    tmp_path: Path,
) -> None:
    from rquant.experiment_platform_commands import ExperimentCommandResult, SetExperimentNote
    from tests.support.strategy_promotion_fixture import (
        build_original_variant_fixture,
        submit_original_experiment_command,
    )

    fixture = build_original_variant_fixture(tmp_path)
    try:
        command = SetExperimentNote(
            command_id=str(uuid4()),
            requested_at=fixture.clock(),
            actor_id=fixture.record.owner,
            family_id=fixture.record.family_id,
            expected_version=0,
            text="核对原完整封存后再解封样本外。",
        )
        identity = fixture.binding.private.identity()
        with pytest.raises(PermissionError, match="trusted private ingress"):
            fixture.control.submit(command)
        assert fixture.control.outbox.receipt(command.command_id) is None
        receipt = submit_original_experiment_command(fixture, command)
        result = ExperimentCommandResult.model_validate(receipt.result)
        assert receipt.status.value == "succeeded" and result.status == "note_saved"
        assert (result.command_id, result.owner, result.family_id) == (
            UUID(command.command_id),
            fixture.record.owner,
            fixture.record.family_id,
        )
        assert fixture.control.outbox.effect(command.command_id).result == receipt.result
        assert submit_original_experiment_command(fixture, command) == receipt
        assert fixture.binding.private.identity() == identity
        assert fixture.platform.list_outer_grants(command.actor_id) == ()
    finally:
        fixture.close()


def test_original_experiment_proof_binds_actor_body_and_current_role(tmp_path: Path) -> None:
    from rquant.collaboration_commands import CommandAuthorization
    from rquant.experiment_platform_commands import SetExperimentNote
    from rquant.web.models.collaboration import CollaborationPrivateRequest
    from tests.support.strategy_promotion_fixture import build_original_variant_fixture

    fixture = build_original_variant_fixture(tmp_path)
    try:
        command = SetExperimentNote(
            command_id=str(uuid4()),
            requested_at=fixture.clock(),
            actor_id=fixture.record.owner,
            family_id=fixture.record.family_id,
            expected_version=0,
            text="完整原证据",
        )
        body = command.model_dump(mode="json")
        proof = fixture.control.collaboration_request(
            CollaborationPrivateRequest(
                schema_version=1,
                operation="authorize_command",
                authenticated_actor_id=fixture.record.owner,
                original_command=body,
            )
        )
        assert type(proof) is CommandAuthorization
        assert proof.actor_id == command.actor_id and proof.command_id == command.command_id
        with pytest.raises(PermissionError, match="private authorization"):
            fixture.control.submit_authorized(
                command.model_copy(update={"text": "另一正文"}), proof
            )
        with pytest.raises(PermissionError, match="original authenticated actor"):
            fixture.control.collaboration_request(
                CollaborationPrivateRequest(
                    schema_version=1,
                    operation="authorize_command",
                    authenticated_actor_id="admin",
                    original_command=body,
                )
            )
        with pytest.raises(PermissionError, match="current role"):
            fixture.control.collaboration_request(
                CollaborationPrivateRequest(
                    schema_version=1,
                    operation="authorize_command",
                    authenticated_actor_id="viewer",
                    original_command=command.model_copy(update={"actor_id": "viewer"}).model_dump(
                        mode="json"
                    ),
                )
            )
        assert fixture.control.outbox.receipt(command.command_id) is None
    finally:
        fixture.close()


def test_real_published_variants_get_review_restore_and_stale_evidence_rejection(
    tmp_path: Path,
) -> None:
    import asyncio

    import httpx

    from rquant.web.app import create_app
    from rquant.web.collaboration_gateway import CollaborationGateway
    from rquant.web.settings import WebSettings
    from tests.support.strategy_promotion_fixture import build_original_variant_fixture

    fixture = build_original_variant_fixture(tmp_path)
    app = None
    try:
        manifest = fixture.publish()
        proof = tmp_path / "proxy-proof"
        proof.write_text("a" * 64)
        proof.chmod(0o400)
        gateway = CollaborationGateway(
            Path("/private/tmp") / ("sp-unused-" + uuid4().hex + ".sock"),
            expected_service_uid=os.geteuid() + 1,
            shared_gid=os.getegid(),
            transport=lambda message: (
                fixture.control.collaboration_request(message).model_dump_json().encode()
            ),
        )
        settings = WebSettings(
            serving_root=tmp_path / "serving",
            strategy_promotion_enabled=True,
            strategy_promotion_users=frozenset({"alice"}),
            collaboration_mode="enforced",
            ingress_socket_path=Path("/private/tmp") / ("sp-web-unused-" + uuid4().hex + ".sock"),
            proxy_proof_file=proof,
        )
        app = create_app(
            settings,
            clock=fixture.clock,
            background=False,
            collaboration_gateway=gateway,
            strategy_promotion_gateway=fixture.admission,
        )
        headers = {"x-rquant-user": "alice", "x-rquant-proxy-proof": "a" * 64, "x-rquant-csrf": "1"}
        parent = fixture.record.request.template

        async def scenario() -> None:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://offline.test"
            ) as client:
                response = await client.get(
                    f"/api/v1/strategy-promotions/{parent.strategy_id}", headers=headers
                )
                assert response.status_code == 200, response.text
                data = response.json()["data"]
                assert data["can_evaluate"] and len(data["candidates"]) == 4
                choice = data["candidates"][0]
                assert choice["target"]["strategy_id"] != parent.strategy_id
                assert choice["template_parent"] == parent.model_dump(mode="json")
                request = RequestPromotionReview(
                    command_id=str(uuid4()),
                    requested_at=fixture.clock(),
                    generation_id=manifest.generation_id,
                    target=choice["target"],
                    expected_revision=0,
                    selection=choice["selection"],
                )
                reviewed = await client.post(
                    "/api/v1/strategy-promotions/commands",
                    json=request.model_dump(mode="json"),
                    headers=headers,
                )
                assert reviewed.status_code == 200, reviewed.text
                result = reviewed.json()["data"]
                assert (
                    result["review"]["target"] == choice["target"]
                    and not StrategyPromotionReview.model_validate(result["review"]).eligible
                )
                assert result["status"] == "completed_waiting_publication"
                fixture.publish(sequence=1)
                restored = await client.post(
                    "/api/v1/strategy-promotions/commands/lookup",
                    json=request.model_dump(mode="json"),
                    headers=headers,
                )
                assert (
                    restored.status_code == 200 and restored.json()["data"]["status"] == "published"
                )
                old_generation = request.model_copy(update={"command_id": str(uuid4())})
                denied = await client.post(
                    "/api/v1/strategy-promotions/commands",
                    json=old_generation.model_dump(mode="json"),
                    headers=headers,
                )
                assert (
                    denied.status_code == 409
                    and fixture.control.outbox.receipt(old_generation.command_id) is None
                )
                other = await client.get(
                    f"/api/v1/strategy-promotions/{parent.strategy_id}",
                    headers={**headers, "x-rquant-user": "viewer"},
                )
                assert (
                    other.status_code == 200
                    and other.json()["data"]["candidates"] == []
                    and other.json()["data"]["reviews"] == []
                )

        asyncio.run(scenario())
    finally:
        if app is not None:
            app.state.web.tracker.close()
        fixture.close()


@pytest.mark.parametrize(
    "case", ["valid", "not_found", "rejected", "large", "length", "duplicate", "owner"]
)
def test_original_private_context_transport_is_bounded_and_preserves_absence(
    case: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    from email.message import Message

    from rquant.strategy_authoring_admission import (
        StrategyAuthoringAdmissionClient,
        StrategyAuthoringAdmissionNotFoundError,
        StrategyAuthoringAdmissionRejectedError,
        StrategyAuthoringAdmissionUnavailableError,
    )
    from rquant.strategy_authoring_commands import StrategyAuthoringIdentity
    from rquant.strategy_promotion_contracts import StrategyPromotionContext
    from rquant.strict_json import canonical_json_bytes
    from tests.unit.test_strategy_promotion_contracts import target

    chosen = target()
    context = StrategyPromotionContext(
        owner_id="alice",
        metadata_identity=StrategyAuthoringIdentity(
            instance_id="1" * 32, path="/synthetic/private.sqlite", st_dev=1, st_ino=2
        ),
        source_kind="template",
        requested_strategy_id=chosen.strategy_id,
        requested_head=chosen.head,
        candidates=(),
        walk_forward=(),
        paper_accounts=(),
        can_evaluate=False,
        can_approve=False,
        can_run_walk_forward=False,
    )
    payload = context.model_dump(mode="json")
    status = 200
    if case == "not_found":
        status, payload = 404, {"error": "original_not_found"}
    elif case == "rejected":
        status, payload = 403, {"error": "actor_forbidden"}
    elif case == "owner":
        payload["owner_id"] = "bob"
    data = canonical_json_bytes(payload)
    if case == "duplicate":
        data = b'{"owner_id":"alice","owner_id":"bob"}'
    headers = Message()
    headers["Content-Type"] = "application/json"
    headers["Content-Length"] = str(
        65537 if case == "large" else len(data) + (1 if case == "length" else 0)
    )
    calls: list[tuple[object, ...]] = []
    closed: list[bool] = []

    class Response:
        def __init__(self) -> None:
            self.status, self.headers = status, headers

        def getheader(self, name: str) -> str | None:
            return self.headers.get(name)

        def read(self, amount: int) -> bytes:
            assert amount == 65537
            return data

    class Connection:
        def request(self, method: str, path: str, *, body: bytes, headers: dict[str, str]) -> None:
            calls.append((method, path, body, headers))

        def getresponse(self) -> Response:
            return Response()

        def close(self) -> None:
            closed.append(True)

    monkeypatch.setattr(
        "rquant.strategy_authoring_admission._UnixHTTPConnection",
        lambda *args, **kwargs: Connection(),
    )
    client = StrategyAuthoringAdmissionClient(
        Path("/private/tmp") / ("sp-mock-" + uuid4().hex + ".sock"),
        expected_service_uid=os.geteuid() + 1,
        shared_gid=os.getegid(),
    )
    expected = (
        StrategyAuthoringAdmissionNotFoundError
        if case == "not_found"
        else StrategyAuthoringAdmissionRejectedError
        if case == "rejected"
        else StrategyAuthoringAdmissionUnavailableError
    )
    if case == "valid":
        assert (
            client.promotion_context(
                authenticated_actor_id="alice",
                source_kind="template",
                strategy_id=chosen.strategy_id,
                head=chosen.head,
            )
            == context
        )
    else:
        with pytest.raises(expected):
            client.promotion_context(
                authenticated_actor_id="alice",
                source_kind="template",
                strategy_id=chosen.strategy_id,
                head=chosen.head,
            )
    assert closed == [True] and len(calls) == 1
    assert calls[0][0:2] == ("POST", "/v1/strategy-authoring-admission/promotion/context")


@pytest.mark.parametrize("configured", [False, True])
def test_promotion_wait_is_separate_from_context_lookup_template_and_base_client(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, configured: bool,
) -> None:
    from rquant.factor_definition_admission import FactorDefinitionAdmissionClient
    from rquant.strategy_authoring_admission import (
        StrategyAuthoringAdmissionClient,
        StrategyAuthoringAdmissionUnavailableError,
    )
    from rquant.strategy_authoring_commands import SaveStrategyTemplate
    from rquant.strategy_template import StrategyTemplate

    _, installed, request, _, _, _, _ = journal_fixture(tmp_path, monkeypatch)
    identity = installed.domain.store.identity()
    template = SaveStrategyTemplate(
        command_id=str(uuid4()), requested_at=NOW, generation_id="test-gen", name="研究策略",
        rules=StrategyTemplate.model_validate({
            "entry": {"kind": "conditions", "conditions": [{"key": "not_st"}]},
            "weight_rule": {"max_positions": 10}, "rebalance_rule": {"kind": "daily"},
        }),
    )
    calls: list[tuple[str, float]] = []
    closed: list[bool] = []

    class Connection:
        def __init__(self, path: Path, **options: object) -> None:
            assert path == socket_path
            assert options["expected_service_uid"] == os.geteuid() + 1
            assert options["shared_gid"] == os.getegid()
            self.wait = options["timeout_seconds"]

        def request(self, method: str, path: str, *, body: bytes, headers: dict[str, str]) -> None:
            assert method == "POST" and headers == {"Content-Type": "application/json"}
            assert body
            calls.append((path, self.wait))

        def getresponse(self) -> None:
            raise TimeoutError("deliberate single-response timeout")

        def close(self) -> None:
            closed.append(True)

    socket_path = Path("/private/tmp") / ("sp-wait-" + uuid4().hex + ".sock")
    monkeypatch.setattr("rquant.strategy_authoring_admission._UnixHTTPConnection", Connection)
    options = {"timeout_seconds": 0.25, "promotion_timeout_seconds": 0.01} if configured else {}
    client = StrategyAuthoringAdmissionClient(
        socket_path, expected_service_uid=os.geteuid() + 1, shared_gid=os.getegid(), **options,
    )
    actions = (
        lambda: client.promotion_submit(request, authenticated_actor_id="alice", verified_metadata_identity=identity),
        lambda: client.promotion_resume(request, authenticated_actor_id="alice"),
        lambda: client.promotion_lookup(request, authenticated_actor_id="alice"),
        lambda: client.promotion_context(authenticated_actor_id="alice", source_kind="template", strategy_id=request.target.strategy_id, head=request.target.head),
        lambda: client.submit(template, authenticated_actor_id="alice", verified_metadata_identity=identity),
        lambda: client.lookup(template, authenticated_actor_id="alice"),
        lambda: client.resume(template, authenticated_actor_id="alice"),
        lambda: client.run_available(authenticated_actor_id="alice"),
    )
    for action in actions:
        with pytest.raises(StrategyAuthoringAdmissionUnavailableError):
            action()
    prefix = "/v1/strategy-authoring-admission"
    promotion_wait, other_wait = (0.01, 0.25) if configured else (5.0, 1.0)
    assert calls == [
        (prefix + "/promotion/submit", promotion_wait),
        (prefix + "/promotion/resume", promotion_wait),
        (prefix + "/promotion/lookup", other_wait),
        (prefix + "/promotion/context", other_wait),
        (prefix + "/submit", other_wait),
        (prefix + "/lookup", other_wait),
        (prefix + "/resume", other_wait),
        (prefix + "/run-availability", other_wait),
    ]
    assert closed == [True] * 8
    base = FactorDefinitionAdmissionClient(
        socket_path, expected_service_uid=os.geteuid() + 1, shared_gid=os.getegid(),
    )
    assert base.timeout_seconds == 1.0


@pytest.mark.parametrize("wait", [0, -0.1, float("inf"), float("-inf"), float("nan"), True, "5", None])
def test_promotion_wait_rejects_nonpositive_or_nonfinite_configuration(wait: object) -> None:
    from rquant.strategy_authoring_admission import StrategyAuthoringAdmissionClient

    with pytest.raises(ValueError):
        StrategyAuthoringAdmissionClient(
            Path("/private/tmp") / ("sp-invalid-wait-" + uuid4().hex + ".sock"),
            expected_service_uid=os.geteuid() + 1, shared_gid=os.getegid(),
            promotion_timeout_seconds=wait,
        )


def test_short_promotion_response_recovers_same_original_uuid_without_resubmit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from email.message import Message

    from rquant.strategy_authoring_admission import (
        StrategyAuthoringAdmission,
        StrategyAuthoringAdmissionClient,
        StrategyAuthoringAdmissionUnavailableError,
        decode_strategy_promotion_request,
    )
    from rquant.strict_json import canonical_json_bytes

    service, installed, request, _, _, reads, _ = journal_fixture(tmp_path, monkeypatch)
    admission = StrategyAuthoringAdmission(service, source_catalog_provider=lambda *_: None)
    issued: list[tuple[str, object, float]] = []
    closed: list[bool] = []

    class Response:
        status = 200

        def __init__(self, data: bytes) -> None:
            self.data = data
            self.headers = Message()
            self.headers["Content-Type"] = "application/json"
            self.headers["Content-Length"] = str(len(data))

        def getheader(self, name: str) -> str | None:
            return self.headers.get(name)

        def read(self, amount: int) -> bytes:
            assert amount == 65537
            return self.data

    class Connection:
        def __init__(self, path: Path, **options: object) -> None:
            assert path == socket_path
            self.wait = options["timeout_seconds"]

        def request(self, method: str, path: str, *, body: bytes, headers: dict[str, str]) -> None:
            assert method == "POST" and headers == {"Content-Type": "application/json"}
            self.mode = path.rsplit("/", 1)[1]
            parsed = decode_strategy_promotion_request(body, mode=self.mode)
            assert parsed.request == request and parsed.authenticated_actor_id == "alice"
            issued.append((self.mode, parsed.request, self.wait))
            if self.mode == "submit":
                self.result = admission.promotion_submit(
                    parsed.request, authenticated_actor_id=parsed.authenticated_actor_id,
                    verified_metadata_identity=parsed.verified_metadata_identity,
                )
            else:
                assert self.mode == "lookup"
                self.result = admission.promotion_lookup(parsed.request, authenticated_actor_id="alice")

        def getresponse(self) -> Response:
            if self.mode == "submit":
                raise TimeoutError("lost response after original journal completed")
            assert self.result is not None
            return Response(canonical_json_bytes({"found": True, "result": self.result.model_dump(mode="json")}))

        def close(self) -> None:
            closed.append(True)

    socket_path = Path("/private/tmp") / ("sp-recovery-wait-" + uuid4().hex + ".sock")
    monkeypatch.setattr("rquant.strategy_authoring_admission._UnixHTTPConnection", Connection)
    client = StrategyAuthoringAdmissionClient(
        socket_path, expected_service_uid=os.geteuid() + 1, shared_gid=os.getegid(),
        promotion_timeout_seconds=0.01,
    )
    with pytest.raises(StrategyAuthoringAdmissionUnavailableError):
        client.promotion_submit(request, authenticated_actor_id="alice", verified_metadata_identity=installed.domain.store.identity())
    original_receipt = service.outbox.receipt(request.command_id)
    assert original_receipt is not None and original_receipt.status.value == "succeeded"
    read_count = len(reads)
    restored = client.promotion_lookup(request, authenticated_actor_id="alice")
    assert restored is not None and restored.receipt == original_receipt
    restored.bind(request, actor_id="alice")
    assert issued == [("submit", request, 0.01), ("lookup", request, 1.0)]
    assert len(reads) == read_count and closed == [True, True]


def journal_fixture(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    domain, target, clock, bundle, reads, roles_path = backend_fixture(tmp_path, monkeypatch)
    outbox = PageControlOutbox(roles_path.parent / "page-control.sqlite")
    os.chmod(outbox.path, 0o600)
    installed = StrategyPromotionPageControlBackend(domain, operator_users=frozenset({"alice"}))
    service = PageControlService(
        outbox=outbox,
        collaboration=domain.roles,
        consumer=PageControlConsumer(
            outbox=outbox,
            data_dir=tmp_path / "data",
            log_dir=tmp_path / "logs",
            strategy_promotion_backend=installed,
            clock=lambda: clock[0],
        ),
    )
    request = RequestPromotionReview(
        command_id=str(uuid4()),
        requested_at=NOW,
        generation_id="test-gen",
        target=target,
        expected_revision=0,
        selection=PromotionEvidenceSelection(family_id="original-parent", experiment_id="7" * 64),
    )
    return service, installed, request, clock, bundle, reads, roles_path


def submit(service: PageControlService, installed: StrategyPromotionPageControlBackend, request):
    return service._submit_trusted_strategy_promotion(
        request,
        authenticated_actor_id="alice",
        verified_metadata_identity=installed.domain.store.identity(),
    )


def test_original_journal_review_prepare_approve_and_old_uuid_first(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    service, installed, request, clock, _, reads, roles_path = journal_fixture(
        tmp_path, monkeypatch
    )
    reviewed = submit(service, installed, request)
    assert reviewed.status.value == "succeeded"
    review = StrategyPromotionReview.model_validate(reviewed.result)
    assert review.eligible and installed.domain.store.promotion_state(request.target).revision == 0
    preparing = PreparePromotionApproval(
        command_id=str(uuid4()),
        requested_at=NOW,
        generation_id=request.generation_id,
        target=request.target,
        review_id=review.review_id,
    )
    prepared = submit(service, installed, preparing)
    from rquant.strategy_promotion_contracts import (
        PreparedPromotionApproval,
        StrategyPromotionApproval,
    )

    approving = ApprovePromotion(
        command_id=str(uuid4()),
        requested_at=NOW,
        generation_id=request.generation_id,
        target=request.target,
        preparation=PreparedPromotionApproval.model_validate(prepared.result),
        entered_name=request.target.name,
    )
    receipt = submit(service, installed, approving)
    effect = StrategyPromotionApproval.model_validate(receipt.result)
    assert effect.effect_id == UUID(approving.command_id) and effect.after.revision == 1
    assert service.outbox.effect(approving.command_id).result == receipt.result
    count = len(reads)
    clock[0] += timedelta(days=1)
    state = RoleState.create(
        revision=2,
        users=(
            RoleEntry(username="alice", role="viewer"),
            RoleEntry(username="root", role="admin"),
        ),
    )
    roles_path.write_text(state.model_dump_json())
    installed.domain.enabled = False
    assert submit(service, installed, approving) == receipt
    assert (
        service._lookup_trusted_strategy_promotion(approving, authenticated_actor_id="alice")
        == receipt
    )
    assert len(reads) == count
    from rquant.page_control import PageControlCommandConflictError

    with pytest.raises(PermissionError, match="current role authority is unavailable") as rejected:
        submit(service, installed, approving.model_copy(update={"entered_name": "另一个名称"}))
    assert isinstance(rejected.value.__cause__, PageControlCommandConflictError)


def test_started_unknown_is_not_redispatched(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    service, installed, request, clock, _, reads, _ = journal_fixture(tmp_path, monkeypatch)
    owned = installed.compile(
        request, actor_id="alice", expected_identity=installed.domain.store.identity()
    )
    service._enqueue_strategy_promotion(owned, actor_id="alice")
    claim = service.outbox.claim_records(
        limit=1,
        owner_id="lost-process",
        lease_seconds=1,
        now=clock[0],
        target_command_id=request.command_id,
    )[0]
    service.outbox.begin_effect(
        owned, owner_id=claim.owner_id, claim_token=claim.claim_token, now=clock[0]
    )
    clock[0] += timedelta(seconds=2)
    receipt = service._resume_trusted_strategy_promotion(request, authenticated_actor_id="alice")
    assert receipt.status.value == "ambiguous" and reads == []
    assert installed.domain.lookup(request, actor_id="alice") is None


def test_persisted_metadata_after_lost_effect_receipt_recovers_after_downgrade(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    service, installed, request, clock, _, reads, roles_path = journal_fixture(
        tmp_path, monkeypatch
    )
    owned = installed.compile(
        request, actor_id="alice", expected_identity=installed.domain.store.identity()
    )
    service._enqueue_strategy_promotion(owned, actor_id="alice")
    claim = service.outbox.claim_records(
        limit=1,
        owner_id="lost-process",
        lease_seconds=1,
        now=clock[0],
        target_command_id=request.command_id,
    )[0]
    service.outbox.begin_effect(
        owned, owner_id=claim.owner_id, claim_token=claim.claim_token, now=clock[0]
    )
    exact = installed.submit(owned)
    state = RoleState.create(
        revision=2,
        users=(
            RoleEntry(username="alice", role="viewer"),
            RoleEntry(username="root", role="admin"),
        ),
    )
    roles_path.write_text(state.model_dump_json())
    clock[0] += timedelta(seconds=2)
    count = len(reads)
    recovered = service._resume_trusted_strategy_promotion(request, authenticated_actor_id="alice")
    assert recovered.status.value == "succeeded" and recovered.result == exact
    assert len(reads) == count


def test_same_original_sqlite_minute_budget_uses_server_admission_time(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    service, installed, request, clock, _, _, _ = journal_fixture(tmp_path, monkeypatch)
    first = submit(service, installed, request)
    for _ in range(29):
        fresh = request.model_copy(
            update={"command_id": str(uuid4()), "requested_at": NOW - timedelta(days=100)}
        )
        assert submit(service, installed, fresh).status.value == "succeeded"
    assert submit(service, installed, request) == first
    with pytest.raises(RuntimeError, match="分钟|minute"):
        submit(service, installed, request.model_copy(update={"command_id": str(uuid4())}))
    clock[0] += timedelta(seconds=61)
    assert (
        submit(
            service, installed, request.model_copy(update={"command_id": str(uuid4())})
        ).status.value
        == "succeeded"
    )


def test_unknown_current_role_and_wrong_actor_do_not_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    service, installed, request, _, _, reads, _ = journal_fixture(tmp_path, monkeypatch)
    with pytest.raises(PermissionError):
        service._submit_trusted_strategy_promotion(
            request,
            authenticated_actor_id="bob",
            verified_metadata_identity=installed.domain.store.identity(),
        )
    assert service.outbox.receipt(request.command_id) is None and reads == []


def test_private_promotion_decoder_and_actual_admission_restore_original(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.strategy_authoring_admission import (
        StrategyAuthoringAdmission,
        decode_strategy_promotion_request,
    )
    from rquant.strict_json import canonical_json_bytes

    service, installed, request, _, _, reads, _ = journal_fixture(tmp_path, monkeypatch)
    admission = StrategyAuthoringAdmission(service, source_catalog_provider=lambda *_: None)
    body = {
        "authenticated_actor_id": "alice",
        "request": request.model_dump(mode="json"),
        "verified_metadata_identity": installed.domain.store.identity().model_dump(mode="json"),
    }
    parsed = decode_strategy_promotion_request(canonical_json_bytes(body), mode="submit")
    assert parsed.request == request
    for payload in (
        {**body, "owner_id": "bob"},
        {**body, "verified_metadata_identity": None},
        {**body, "request": {**body["request"], "owner_id": "alice"}},
    ):
        with pytest.raises(ValueError):
            decode_strategy_promotion_request(canonical_json_bytes(payload), mode="submit")
    first = admission.promotion_submit(
        request,
        authenticated_actor_id="alice",
        verified_metadata_identity=installed.domain.store.identity(),
    )
    first.bind(request, actor_id="alice")
    count = len(reads)
    assert admission.promotion_lookup(request, authenticated_actor_id="alice") == first
    assert admission.promotion_resume(request, authenticated_actor_id="alice") == first
    assert len(reads) == count
    with pytest.raises(PermissionError):
        admission.promotion_lookup(request, authenticated_actor_id="bob")
    with pytest.raises(ValueError):
        first.bind(request.model_copy(update={"expected_revision": 1}), actor_id="alice")


def test_promotion_public_and_private_entry_do_not_share_owner_authority(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.page_control import parse_page_control_command

    service, installed, request, _, _, reads, _ = journal_fixture(tmp_path, monkeypatch)
    with pytest.raises((TypeError, ValueError)):
        parse_page_control_command(request.model_dump(mode="json"))
    with pytest.raises((TypeError, ValueError, PermissionError)):
        service.submit(request)
    assert service.outbox.receipt(request.command_id) is None and reads == []


def test_real_asgi_restores_original_before_closed_switch_and_revoked_write_role(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import asyncio

    import httpx

    from rquant.strategy_authoring_admission import StrategyAuthoringAdmission
    from rquant.web.app import create_app
    from rquant.web.collaboration_gateway import CollaborationGateway
    from rquant.web.settings import WebSettings

    service, installed, request, _, _, reads, roles_path = journal_fixture(tmp_path, monkeypatch)
    first = submit(service, installed, request)
    count = len(reads)
    roles_path.write_text(
        RoleState.create(
            revision=2,
            users=(
                RoleEntry(username="alice", role="viewer"),
                RoleEntry(username="root", role="admin"),
            ),
        ).model_dump_json()
    )
    installed.domain.enabled = False
    proof = tmp_path / "proxy-proof"
    proof.write_text("a" * 64)
    proof.chmod(0o400)
    gateway = CollaborationGateway(
        Path("/private/tmp") / ("sp-unused-" + uuid4().hex + ".sock"),
        expected_service_uid=os.geteuid() + 1,
        shared_gid=os.getegid(),
        transport=lambda message: service.collaboration_request(message).model_dump_json().encode(),
    )
    settings = WebSettings(
        serving_root=tmp_path / "absent-serving",
        collaboration_mode="enforced",
        ingress_socket_path=Path("/private/tmp") / ("sp-web-unused-" + uuid4().hex + ".sock"),
        proxy_proof_file=proof,
    )
    app = create_app(
        settings,
        clock=lambda: NOW,
        background=False,
        collaboration_gateway=gateway,
        strategy_promotion_gateway=StrategyAuthoringAdmission(
            service, source_catalog_provider=lambda *_: None
        ),
    )
    headers = {"x-rquant-user": "alice", "x-rquant-proxy-proof": "a" * 64, "x-rquant-csrf": "1"}

    async def scenario() -> None:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://offline.test"
        ) as client:
            missing = await client.post(
                "/api/v1/strategy-promotions/commands/lookup",
                json=request.model_dump(mode="json"),
                headers={"x-rquant-user": "alice"},
            )
            assert missing.status_code == 401
            no_csrf = await client.post(
                "/api/v1/strategy-promotions/commands/resume",
                json=request.model_dump(mode="json"),
                headers={key: value for key, value in headers.items() if key != "x-rquant-csrf"},
            )
            assert no_csrf.status_code == 403
            for action in ("commands", "commands/lookup", "commands/resume"):
                response = await client.post(
                    "/api/v1/strategy-promotions/" + action,
                    json=request.model_dump(mode="json"),
                    headers=headers,
                )
                assert response.status_code == 200, response.text
                assert response.json()["data"]["review"]["review_id"] == first.result["review_id"]
                assert response.json()["data"]["status"] == "completed_waiting_publication"
            fresh = request.model_copy(update={"command_id": str(uuid4())})
            denied = await client.post(
                "/api/v1/strategy-promotions/commands",
                json=fresh.model_dump(mode="json"),
                headers=headers,
            )
            assert denied.status_code == 403
            assert service.outbox.receipt(fresh.command_id) is None
            oversized = await client.post(
                "/api/v1/strategy-promotions/commands",
                content=b"x" * (32 * 1024 + 1),
                headers={**headers, "content-type": "application/json"},
            )
            assert oversized.status_code == 413

    try:
        asyncio.run(scenario())
    finally:
        app.state.web.tracker.close()
    assert len(reads) == count


def test_manual_settings_default_closed_and_require_real_original_roles(tmp_path: Path) -> None:
    from rquant.web.settings import WebSettings

    baseline = WebSettings(serving_root=tmp_path)
    assert (
        baseline.strategy_promotion_enabled is False
        and baseline.strategy_promotion_users == frozenset()
    )
    with pytest.raises(ValueError, match="enforced"):
        WebSettings(
            serving_root=tmp_path,
            strategy_promotion_enabled=True,
            strategy_promotion_users={"alice"},
        )


def test_high_risk_same_strategy_budget_rejects_new_uuid_before_an_effect(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.strategy_promotion_commands import StrategyPromotionRateLimitError
    from rquant.strategy_promotion_contracts import PreparedPromotionApproval

    service, installed, request, _, _, reads, _ = journal_fixture(tmp_path, monkeypatch)
    review = StrategyPromotionReview.model_validate(submit(service, installed, request).result)
    preparing = PreparePromotionApproval(
        command_id=str(uuid4()),
        requested_at=NOW,
        generation_id=request.generation_id,
        target=request.target,
        review_id=review.review_id,
    )
    preparation = PreparedPromotionApproval.model_validate(
        submit(service, installed, preparing).result
    )
    approving = ApprovePromotion(
        command_id=str(uuid4()),
        requested_at=NOW,
        generation_id=request.generation_id,
        target=request.target,
        preparation=preparation,
        entered_name=request.target.name,
    )
    first = submit(service, installed, approving)
    count = len(reads)
    retry = approving.model_copy(
        update={"command_id": str(uuid4()), "requested_at": NOW - timedelta(days=100)}
    )
    with pytest.raises(StrategyPromotionRateLimitError):
        submit(service, installed, retry)
    assert (
        service.outbox.receipt(retry.command_id) is None
        and service.outbox.effect(retry.command_id) is None
    )
    assert submit(service, installed, approving) == first and len(reads) == count


def test_concurrent_admission_shares_original_sqlite_minute_cap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from concurrent.futures import ThreadPoolExecutor

    from rquant.strategy_promotion_commands import StrategyPromotionRateLimitError

    service, installed, request, _, _, _, _ = journal_fixture(tmp_path, monkeypatch)
    requests = tuple(request.model_copy(update={"command_id": str(uuid4())}) for _ in range(32))

    def admitting(body: RequestPromotionReview) -> str:
        try:
            return submit(service, installed, body).status.value
        except StrategyPromotionRateLimitError:
            return "rate_limited"

    with ThreadPoolExecutor(max_workers=4) as executor:
        results = tuple(executor.map(admitting, requests))
    assert results.count("succeeded") == 30 and results.count("rate_limited") == 2
    assert sum(service.outbox.receipt(body.command_id) is not None for body in requests) == 30
