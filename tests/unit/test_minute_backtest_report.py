from __future__ import annotations

import hashlib
import json
from html.parser import HTMLParser
from pathlib import Path
from typing import Literal

import pytest

from rquant.data_metadata import DataAuditRunFinalization, DatasetSnapshotFinalization
from rquant.minute_backtest_artifact import MinuteSealedReplayResult
from rquant.minute_backtest_producer import minute_coverages, minute_metadata_identities, minute_watermarks
from rquant.minute_backtest_publication_contracts import MinuteSourceContentSeed, MinuteVisibilityPolicy
from rquant.minute_backtest_report import build_minute_html_report, minute_artifact_fact
from rquant.sealed_result_html import MAX_HTML_BYTES, validate_offline_html
from rquant.sealed_result_ownership import OriginalEffectFact, OriginalSubmissionFact, SealedJobFact, SealedOwnerBinding, bind_sealed_owner
from tests.support.minute_report_fixture import build_minute_report_fixture


def report_inputs() -> tuple[MinuteSealedReplayResult, SealedOwnerBinding]:
    path = Path(__file__).resolve().parents[1] / "fixtures/minute-report-sealed-result.json"
    raw = path.read_bytes()
    assert hashlib.sha256(raw).hexdigest() == "8316a5db151347f73074fd84bb4939a007278400e7299c1706ac9c0ded292cdc"
    sealed = build_minute_report_fixture(raw).result
    return sealed, typed_unit_binding(sealed)


def test_renderer_fixture_retains_complete_reference_values_and_current_binding() -> None:
    from rquant.minute_backtest_validation import original_builtin_minute_plan

    path = Path(__file__).resolve().parents[1] / "fixtures/minute-report-sealed-result.json"
    raw = path.read_bytes()
    reference = json.loads(raw)
    fixture = build_minute_report_fixture(raw)
    sealed = fixture.result
    current = sealed.model_dump(mode="json", exclude_computed_fields=True)
    original_replay = reference["result"]["replay"]
    for key in original_replay:
        if key not in {"input_hash", "daily_valuations"}:
            assert current["result"]["replay"][key] == original_replay[key]
    for old, new in zip(
        original_replay["daily_valuations"],
        current["result"]["replay"]["daily_valuations"],
        strict=True,
    ):
        assert {key: value for key, value in old.items() if key != "input_hash"} == {
            key: value for key, value in new.items() if key != "input_hash"
        }
    old_seed = reference["result"]["publication"]["seed"]
    new_seed = current["result"]["publication"]["seed"]
    assert new_seed["origin_materials"] == old_seed["origin_materials"]
    assert new_seed["runtime"]["materials"] == old_seed["runtime"]["materials"]
    assert new_seed["runtime"]["source_key"] != old_seed["runtime"]["source_key"]
    plan = original_builtin_minute_plan(
        producer_commit=sealed.result.publication.frozen.runtime.producer_commit
    )
    assert sealed.result.publication.frozen.runtime.strategy in plan.strategies
    assert fixture.compared_leaf_values > 0
    assert not fixture.physical_owner_verified and not fixture.formal_history_passed
    assert sealed.full_input_hash != reference["result"]["full_input_hash"]
    assert path.read_bytes() == raw


def test_renderer_fixture_rejects_changed_reference_and_detached_executable() -> None:
    path = Path(__file__).resolve().parents[1] / "fixtures/minute-report-sealed-result.json"
    raw = path.read_bytes()
    with pytest.raises(ValueError, match="reference bytes changed"):
        build_minute_report_fixture(raw + b"\n")
    data = build_minute_report_fixture(raw).result.model_dump(
        mode="json", exclude_computed_fields=True
    )
    data["result"]["publication"]["frozen"]["runtime"]["strategy"]["executable_fingerprint"] = (
        "0" * 64
    )
    with pytest.raises(ValueError, match="strategy registration or executable"):
        MinuteSealedReplayResult.model_validate_json(json.dumps(data))


def typed_unit_binding(sealed: MinuteSealedReplayResult) -> SealedOwnerBinding:
    # Renderer-only facts do not attest a physical private source or current role.
    command_id = "0d66a786-d60c-4932-9121-6e859f9c7b40"
    submit = OriginalSubmissionFact(domain="minute", command_id=command_id,
        command_kind="submit_minute_replay", command_sha256="1" * 64, actor_id=sealed.owner_id,
        job_id=str(sealed.job_id), spec_hash=sealed.spec_hash, origin_verified=True)
    effect = OriginalEffectFact(command_id=command_id, command_kind=submit.command_kind,
        command_sha256=submit.command_sha256, status="succeeded", submitted_job_id=submit.job_id,
        submitted_spec_hash=submit.spec_hash, worker_owner_id="unit-report-worker")
    job = SealedJobFact(domain="minute", job_id=str(sealed.job_id), spec_hash=sealed.spec_hash,
        status="succeeded", manifest_hash=sealed.manifest_hash, complete_result_hash=sealed.complete_result_hash)
    binding = bind_sealed_owner(submit, effect, job, minute_artifact_fact(sealed))
    assert binding is not None
    return binding


def approved_historical_policy() -> MinuteVisibilityPolicy:
    return MinuteVisibilityPolicy(
        policy_id="retained-minute-research", version=1, timestamp_semantics="bar_end",
        market_event_basis="Model each original one-minute trade_time as minute start; bar end is start plus one minute. Preserve all raw bytes and hashes; identical provider duplicate prices/volume/amount may derive one deterministic execution record with both origins retained.",
        market_visibility_basis="Only the modeled completed bar is visible at its modeled end. No created_at proof claim and no backfilled future bar.",
        candidate_visibility_basis="Use only previous completed-day screen records, replay-visible at next-session initialization; retain full original archive and derivation. Intraday candidates without original publication timing stay unavailable. Pool rows without verifiable historical version stay unavailable.",
        constraint_visibility_basis="Use only prior-day reference values and the installed original simulation profile. Missing eligibility/ST/limit/PIT facts stay unavailable; do not invent production limits or fill unknown constraints.",
        native_definition_basis="Research replay runs the currently registered exact native definition/profile, modeled available at replay bootstrap; retain actual current registry timestamp separately.",
        limitations="Modeled historical visibility is research data, not proof of what production observed. Missing candidate, constraint, warm history, previous complete NAV or coverage remains explicit; no interpolation or zero fill. This one-day source does not prove 32-day history or twenty actual forward days.",
    )


def report_with_policy(policy: MinuteVisibilityPolicy) -> MinuteSealedReplayResult:
    # In-memory typed renderer fixture; no physical publication or installed source claim.
    sealed, _ = report_inputs()
    original = sealed.result.publication
    seed_data = original.seed.model_dump(mode="python", exclude_computed_fields=True)
    seed_data["provenance"]["visibility_policy"] = policy.model_dump(mode="python")
    seed = MinuteSourceContentSeed.model_validate(seed_data)
    audit, snapshot = minute_metadata_identities(seed)
    frozen = seed.freeze(audit_run_id=audit.audit_run_id, dataset_snapshot_id=snapshot.snapshot_id)
    manifest = type(original.binding.manifest).model_validate(
        original.binding.manifest.model_dump(mode="python", exclude_computed_fields=True)
        | {"snapshot_id": snapshot.snapshot_id})
    binding = type(original.binding).model_validate(
        original.binding.model_dump(mode="python", exclude_computed_fields=True)
        | {"snapshot_id": snapshot.snapshot_id, "manifest_hash": manifest.manifest_hash,
           "manifest": manifest, "manifest_relative_path": f"snapshots/{snapshot.snapshot_id}/manifest.json"})
    publication = type(original).model_validate(
        original.model_dump(mode="python", exclude_computed_fields=True)
        | {"seed": seed, "frozen": frozen, "binding": binding,
           "audit": audit.finalize(DataAuditRunFinalization(p0_count=0, completed_at=seed.provenance.published_at)),
           "snapshot": snapshot.finalize(DatasetSnapshotFinalization(
               table_watermarks=minute_watermarks(frozen), completed_at=seed.provenance.published_at)),
           "coverages": minute_coverages(frozen)})
    data = sealed.model_dump(mode="python", exclude_computed_fields=True)
    result = data["result"]
    result.update(publication=publication.model_dump(mode="python", exclude_computed_fields=True),
        full_input_hash=frozen.full_input_hash, core_input_hash=frozen.core_input_hash, seed_hash=seed.seed_hash)
    result["replay"]["input_hash"] = frozen.core_input_hash
    for day in result["replay"]["daily_valuations"]:
        day["input_hash"] = frozen.core_input_hash
    return MinuteSealedReplayResult.model_validate(data)


class BodyText(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.details = 0
        self.primary: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "details":
            self.details += 1

    def handle_endtag(self, tag: str) -> None:
        if tag == "details":
            self.details -= 1

    def handle_data(self, data: str) -> None:
        if not self.details:
            self.primary.append(data)


def test_original_full_minute_values_produce_deterministic_offline_html() -> None:
    sealed, binding = report_inputs()
    value = build_minute_html_report(sealed, owner=binding, requester=sealed.owner_id)
    assert value == build_minute_html_report(sealed, owner=binding, requester=sealed.owner_id)
    assert value.performance.status == "complete" and value.performance.metrics is not None
    assert value.performance.metrics.summary.total_return == pytest.approx(-0.0074349)
    assert value.performance.metrics.round_trips[0].net_pnl == pytest.approx(-743.49)
    assert value.html_sha256 == hashlib.sha256(value.html_bytes()).hexdigest()
    validate_offline_html(value.html_bytes())
    assert len(value.html_bytes()) <= MAX_HTML_BYTES
    assert "每日净值" in value.html and "按15:00已确认的行情估值" in value.html
    assert "100092.899999999999000" in value.html and "99256.5100" in value.html
    assert sealed.result.replay.daily_valuations[0].price_proofs[0].quote.event_time.isoformat() in value.html
    assert "真实历史捕获" not in value.html
    assert sealed.full_input_hash in value.html and "Synthetic parity fixture only" in value.html
    primary = BodyText()
    primary.feed(value.html)
    assert sealed.full_input_hash not in "".join(primary.primary)
    assert sealed.result.replay.signals[0].signal_id not in "".join(primary.primary)
    assert sealed.result.replay.signals[0].candidate_id in value.html
    assert "/private/" not in value.html


def test_opaque_signal_candidate_identity_is_available_only_in_report_details() -> None:
    sealed, _ = report_inputs()
    data = sealed.model_dump(mode="python", exclude_computed_fields=True)
    opaque = "min1:opaque:" + "8" * 64
    data["result"]["replay"]["signals"][0]["candidate_id"] = opaque
    data["result"]["replay"]["signals"][0]["signal_id"] = None
    view_fixture = MinuteSealedReplayResult.model_validate(data)
    report = build_minute_html_report(view_fixture, owner=typed_unit_binding(view_fixture), requester=sealed.owner_id)
    primary = BodyText()
    primary.feed(report.html)
    assert opaque not in "".join(primary.primary)
    assert opaque in report.html


def test_report_rejects_current_other_owner_and_changed_full_result_binding() -> None:
    sealed, binding = report_inputs()
    with pytest.raises(PermissionError):
        build_minute_html_report(sealed, owner=binding, requester="other-owner")
    changed = MinuteSealedReplayResult.model_validate(sealed.model_dump(mode="python", exclude_computed_fields=True) | {
        "payload_hash": "0" * 64})
    with pytest.raises(PermissionError):
        build_minute_html_report(changed, owner=binding, requester=sealed.owner_id)


def test_html_whole_budget_rejects_without_truncating_original_values() -> None:
    sealed, binding = report_inputs()
    with pytest.raises(ValueError, match="capacity"):
        build_minute_html_report(sealed, owner=binding, requester=sealed.owner_id, max_bytes=1024)


def test_exact_approved_historical_policy_is_labeled_with_model_details() -> None:
    policy = approved_historical_policy()
    assert policy.fingerprint == "744546c1df988b502d8f09bfc189737561ebc7072f3794682fa60dd217b87838"
    sealed = report_with_policy(policy)
    report = build_minute_html_report(sealed, owner=typed_unit_binding(sealed), requester=sealed.owner_id)
    primary = BodyText()
    primary.feed(report.html)
    text = "".join(primary.primary)
    assert "历史重建" in text and "重建研究源" not in text
    assert "模型可见时刻不代表当时实际捕获" in text
    assert policy.policy_id not in text and policy.fingerprint not in text
    assert policy.market_visibility_basis not in text and policy.limitations not in text
    assert policy.market_visibility_basis in report.html and policy.limitations in report.html
    provenance = sealed.result.publication.frozen.provenance
    assert provenance.model_dump(mode="json")["native_definition_replay_available_at"] in report.html
    assert provenance.publication_evidence[0].model_dump(mode="json")["published_at"] in report.html
    assert "真实历史捕获" not in report.html
    validate_offline_html(report.html_bytes())


def test_same_historical_policy_name_with_different_limits_keeps_generic_label() -> None:
    policy = MinuteVisibilityPolicy.model_validate(approved_historical_policy().model_dump(mode="python")
        | {"limitations": "Different unapproved visibility assumptions; no historical source attestation."})
    sealed = report_with_policy(policy)
    report = build_minute_html_report(sealed, owner=typed_unit_binding(sealed), requester=sealed.owner_id)
    primary = BodyText()
    primary.feed(report.html)
    text = "".join(primary.primary)
    assert "重建研究源" in text and "历史重建" not in text


@pytest.mark.parametrize("nature", ("historical_reconstruction", "real_retained", "synthetic_validation"))
def test_native_report_rejects_unbound_source_nature_override(
    nature: Literal["historical_reconstruction", "real_retained", "synthetic_validation"],
) -> None:
    sealed, binding = report_inputs()
    with pytest.raises(PermissionError, match="native report.*bound provenance"):
        build_minute_html_report(sealed, owner=binding, requester=sealed.owner_id,
            parameter_source_nature=nature)


def test_historical_policy_keeps_current_owner_and_full_source_binding_gate() -> None:
    original, original_binding = report_inputs()
    sealed = report_with_policy(approved_historical_policy())
    with pytest.raises(PermissionError):
        build_minute_html_report(sealed, owner=original_binding, requester=original.owner_id)
    with pytest.raises(PermissionError):
        build_minute_html_report(sealed, owner=typed_unit_binding(sealed), requester="other-owner")
