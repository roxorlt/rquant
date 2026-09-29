"""Display data stays derived from one sealed research artifact."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from pathlib import Path

import pytest

from rquant.factor.historical_adapter import (
    HistoricalFactorResearch,
    HistoricalFactorSourceReceipt,
    HistoricalPanelDate,
    HistoricalReturnWindow,
)
from rquant.factor.result import assemble_factor_research_result
from rquant.factor.result_artifact import (
    FactorResearchArtifactV1,
    load_factor_research_artifact,
    publish_factor_research_artifact,
)
from rquant.strict_json import canonical_json_bytes
from tests.unit.test_factor_result_artifact import _CODE_REVISION, _research
from tests.unit.test_factor_result_assembly import _DAYS, _request


def _digest(payload: object) -> str:
    import hashlib

    return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def _full(tmp_path: Path) -> tuple[Path, FactorResearchArtifactV1]:
    root = tmp_path / "artifacts"
    root.mkdir(mode=0o700)
    receipt = publish_factor_research_artifact(_research(), _CODE_REVISION, root)
    return root, load_factor_research_artifact(root, receipt.sha256)


def _two_day_full(root: Path) -> FactorResearchArtifactV1:
    original = _research().receipt
    request = _request()
    fields = original.model_dump(mode="python", exclude={"source_sha256"})
    fields.update(
        stock_codes=request.factor_input.universe,
        pool_sha256=_digest(request.factor_input.universe),
        query_start_date=_DAYS[0] - timedelta(days=1),
        query_end_date=_DAYS[-1],
        calculation_days=_DAYS,
        evaluation_days=_DAYS,
        snapshot_as_of_time=request.as_of,
        panel_dates=tuple(
            HistoricalPanelDate(
                decision_date=day,
                panel_date=day - timedelta(days=1),
                first_visible_at=decision - timedelta(minutes=1),
            )
            for day, decision in zip(
                _DAYS,
                (item.decision_at for item in request.factor_input.decision_times),
                strict=True,
            )
        ),
        return_windows=tuple(
            HistoricalReturnWindow(
                decision_date=day,
                end_date=row.return_end_at.date(),
                return_end_at=row.return_end_at,
                expected_available_at=request.as_of,
            )
            for day, row in zip(_DAYS, request.forward_returns[::3], strict=True)
        ),
    )
    unsigned = HistoricalFactorSourceReceipt.model_construct(**fields, source_sha256="0" * 64)
    receipt = HistoricalFactorSourceReceipt(
        **fields,
        source_sha256=_digest(unsigned.model_dump(mode="json", exclude={"source_sha256"})),
    )
    bound_request = request.model_copy(
        update={
            "factor_source_id": receipt.source_sha256,
            "return_source_id": receipt.source_sha256,
        }
    )
    research = HistoricalFactorResearch(
        receipt=receipt,
        request=bound_request,
        result=assemble_factor_research_result(bound_request),
    )
    stored = publish_factor_research_artifact(research, _CODE_REVISION, root)
    return load_factor_research_artifact(root, stored.sha256)


def test_hand_values_and_explicit_unavailable_groups_are_projected(tmp_path: Path) -> None:
    from rquant.factor.display_artifact import project_factor_display_artifact

    _root, full = _full(tmp_path)
    display = project_factor_display_artifact(full)
    point = display.ic_points[0]
    assert display.full_artifact_sha256 == full.content_sha256
    assert display.result_sha256 == full.research.result.sha256
    assert display.input_sha256 == full.research.result.input_sha256
    assert display.factor_version == full.research.result.factor_version == 1
    assert display.ic_cumulative_kind == "sum_of_valid_daily_ic"
    assert point.normal_ic.value == pytest.approx(0.3273268353539886)
    assert point.rank_ic.value == pytest.approx(0.5)
    assert point.normal_ic_cumulative_sum == pytest.approx(0.3273268353539886)
    assert point.rank_ic_cumulative_sum == pytest.approx(0.5)
    assert display.decay_periods[0].lag == 1
    assert display.decay_periods[0].ic_summary.normal_ic.mean == pytest.approx(0.3273268353539886)
    assert display.decay_periods[1].status == "no_target_period"
    assert display.decay_periods[1].ic_summary is None
    groupings = display.portfolio_days[0].groupings
    assert tuple(grouping.group_count for grouping in groupings) == (3, 5, 10)
    assert [point.cumulative_return for point in groupings[0].groups] == pytest.approx(
        [0.1, -0.1, 0.2]
    )
    assert all(point.target_weight_turnover is None for point in groupings[0].groups)
    assert all(
        grouping.status == "insufficient_samples" and not grouping.groups
        for grouping in groupings[1:]
    )
    assert display.coverage_days[0].coverage.valid_count == 3
    assert "observations" not in display.model_dump_json()


def test_two_day_ic_sum_compounding_and_turnover_have_hand_values(tmp_path: Path) -> None:
    from rquant.factor.display_artifact import project_factor_display_artifact

    root, _ = _full(tmp_path)
    display = project_factor_display_artifact(_two_day_full(root))
    assert len(display.ic_points) == 2
    assert display.ic_points[0].normal_ic_cumulative_sum == pytest.approx(0.5)
    assert display.ic_points[1].normal_ic_cumulative_sum == pytest.approx(0.5 + 0.9607689228305228)
    assert display.ic_points[1].rank_ic_cumulative_sum == pytest.approx(1.5)
    assert display.ic_cumulative_kind == "sum_of_valid_daily_ic"
    groups = display.portfolio_days[1].groupings[0].groups
    assert [point.cumulative_return for point in groups] == pytest.approx([-0.01, 0.05, 0.32])
    assert [point.target_weight_turnover for point in groups] == pytest.approx([1.0, 1.0, 1.0])


def test_missing_samples_remain_null_and_input_reordering_preserves_diagnostics(
    tmp_path: Path,
) -> None:
    from rquant.factor.display_artifact import project_factor_display_artifact

    root, full = _full(tmp_path)
    original = _research()
    request = original.request
    missing_input = request.factor_input.model_copy(
        update={
            "observations": tuple(
                observation.model_copy(update={"value": None})
                for observation in request.factor_input.observations
            )
        }
    )
    missing_request = request.model_copy(update={"factor_input": missing_input})
    missing = HistoricalFactorResearch(
        receipt=original.receipt,
        request=missing_request,
        result=assemble_factor_research_result(missing_request),
    )
    missing_receipt = publish_factor_research_artifact(missing, _CODE_REVISION, root)
    missing_display = project_factor_display_artifact(
        load_factor_research_artifact(root, missing_receipt.sha256)
    )
    assert missing_display.summary_status == "no_samples"
    assert missing_display.ic_summary is None
    assert missing_display.ic_points[0].normal_ic is None
    assert missing_display.ic_points[0].normal_ic_cumulative_sum is None
    assert missing_display.portfolio_status == "insufficient_data"
    assert missing_display.portfolio_days == ()
    assert missing_display.coverage_days[0].status == "no_samples"
    assert missing_display.coverage_days[0].coverage.valid_count == 0

    reordered_input = request.factor_input.model_copy(
        update={"observations": tuple(reversed(request.factor_input.observations))}
    )
    reordered_request = request.model_copy(
        update={
            "factor_input": reordered_input,
            "forward_returns": tuple(reversed(request.forward_returns)),
        }
    )
    reordered = HistoricalFactorResearch(
        receipt=original.receipt,
        request=reordered_request,
        result=assemble_factor_research_result(reordered_request),
    )
    reordered_receipt = publish_factor_research_artifact(reordered, _CODE_REVISION, root)
    reordered_display = project_factor_display_artifact(
        load_factor_research_artifact(root, reordered_receipt.sha256)
    )
    baseline = project_factor_display_artifact(full)
    assert reordered_display.result_sha256 == baseline.result_sha256
    assert reordered_display.ic_points == baseline.ic_points
    assert reordered_display.coverage_days == baseline.coverage_days
    assert reordered_display.portfolio_days == baseline.portfolio_days
    assert reordered_display.full_artifact_sha256 != baseline.full_artifact_sha256


def test_publish_reload_idempotent_and_rejects_damage_or_symlink(tmp_path: Path) -> None:
    from rquant.factor.display_artifact import (
        load_factor_display_artifact,
        project_factor_display_artifact,
        publish_factor_display_artifact,
    )

    root, full = _full(tmp_path)
    receipt = publish_factor_display_artifact(full, root)
    assert receipt.filename == f"factor-display-v1-{receipt.sha256}.json"
    assert receipt.byte_count == (root / receipt.filename).stat().st_size
    assert load_factor_display_artifact(root, receipt.sha256) == project_factor_display_artifact(
        full
    )
    assert publish_factor_display_artifact(full, root) == receipt
    target = root / receipt.filename
    original = target.read_bytes()
    target.write_bytes(original[: len(original) // 2])
    with pytest.raises(ValueError):
        load_factor_display_artifact(root, receipt.sha256)
    assert target.read_bytes() != original
    target.unlink()
    outside = tmp_path / "outside"
    outside.write_bytes(b"outside remains")
    target.symlink_to(outside)
    with pytest.raises(OSError):
        load_factor_display_artifact(root, receipt.sha256)
    assert outside.read_bytes() == b"outside remains"


def test_existing_corrupt_display_name_is_not_replaced(tmp_path: Path) -> None:
    from rquant.factor.display_artifact import (
        project_factor_display_artifact,
        publish_factor_display_artifact,
    )

    root, full = _full(tmp_path)
    projected = project_factor_display_artifact(full)
    target = root / f"factor-display-v1-{projected.content_sha256}.json"
    target.write_bytes(b"occupied")
    target.chmod(0o600)
    with pytest.raises(ValueError):
        publish_factor_display_artifact(full, root)
    assert target.read_bytes() == b"occupied"
    assert not list(root.glob(".factor-display-*.tmp"))


def test_byte_budget_and_two_publishers_preserve_one_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.factor import display_artifact as module

    root, full = _full(tmp_path)
    monkeypatch.setattr(module, "MAX_FACTOR_DISPLAY_ARTIFACT_BYTES", 64)
    with pytest.raises(ValueError, match="8 MiB"):
        module.publish_factor_display_artifact(full, root)
    assert not any(path.name.startswith("factor-display") for path in root.iterdir())
    monkeypatch.undo()
    with ThreadPoolExecutor(max_workers=2) as workers:
        receipts = tuple(
            workers.map(lambda _: module.publish_factor_display_artifact(full, root), range(2))
        )
    assert receipts[0] == receipts[1]
    assert len([path for path in root.iterdir() if path.name.startswith("factor-display")]) == 1
