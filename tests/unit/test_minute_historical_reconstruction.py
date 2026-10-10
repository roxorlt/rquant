from __future__ import annotations

import base64
import hashlib
import importlib
import importlib.util
import json
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from types import ModuleType
from typing import Any
from zoneinfo import ZoneInfo

import pytest
from pydantic import ValidationError

from rquant.minute_backtest_publication_contracts import (
    MinuteOriginMaterial,
    MinuteVisibilityPolicy,
)

SH = ZoneInfo("Asia/Shanghai")
DAY = date(2026, 9, 28)
PREVIOUS = date(2026, 9, 25)
CODE = "600000.SH"
FACT_KINDS = (
    "eligibility",
    "risk_warning",
    "suspension",
    "price_limits",
    "quotes",
    "warm_history",
    "prior_reference",
    "prior_state",
)


def _api() -> ModuleType:
    name = "rquant.minute_historical_reconstruction"
    if importlib.util.find_spec(name) is None:
        pytest.fail("The typed offline historical preparation interface is not implemented")
    return importlib.import_module(name)


def _origin(key: str, value: object) -> MinuteOriginMaterial:
    payload = json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode()
    return MinuteOriginMaterial(
        object_key=key,
        format="json",
        content_base64=base64.b64encode(payload).decode(),
        content_sha256=hashlib.sha256(payload).hexdigest(),
    )


def _policy() -> MinuteVisibilityPolicy:
    return MinuteVisibilityPolicy(
        policy_id="retained-minute-research",
        version=1,
        timestamp_semantics="bar_end",
        market_event_basis="Confirmed original realtime minute starts become ends plus one minute.",
        market_visibility_basis="Modeled completed bar is visible at its end; not actual capture.",
        candidate_visibility_basis="Previous completed-day screens at next-session initialization.",
        constraint_visibility_basis="Missing original PIT facts remain unavailable.",
        native_definition_basis="Current exact definition at modeled replay bootstrap.",
        limitations="No publication, trusted installation, or real forward-day proof.",
    )


def _minutes() -> list[dict[str, object]]:
    rows = []
    for start in (time(9, 30), time(13, 0)):
        initial = datetime.combine(DAY, start, SH)
        for index in range(120):
            rows.append(
                {
                    "ts_code": CODE,
                    "trade_time": (initial + timedelta(minutes=index)).isoformat(),
                    "freq": "1min",
                    "source": "confirmed-rt",
                    "open": "10.1234567890123456789",
                    "high": "10.2",
                    "low": "10.0",
                    "close": "10.15",
                    "vol": "123.456",
                    "amount": "1234.56789",
                }
            )
    return rows


def _facts() -> list[dict[str, object]]:
    return [
        {
            "kind": kind,
            "ts_code": CODE,
            "trade_date": DAY.isoformat(),
            "status": "complete",
            "value": kind not in {"risk_warning", "suspension"},
            "available_at": datetime.combine(PREVIOUS, time(15), SH).isoformat(),
            "reference_trade_date": PREVIOUS.isoformat()
            if kind in {"prior_reference", "prior_state"}
            else None,
        }
        for kind in FACT_KINDS
    ]


def _request(
    *,
    minutes: list[dict[str, object]] | None = None,
    candidates: list[dict[str, object]] | None = None,
    facts: list[dict[str, object]] | None = None,
    candidate_kind: str = "completed_screen",
    completed: bool = True,
    timestamp_semantics: str = "minute_start",
    source_timezone: str | None = "Asia/Shanghai",
) -> Any:
    api = _api()
    materials = (
        _origin("original-minute-archive", {"rows": _minutes() if minutes is None else minutes}),
        _origin(
            "original-screen-archive",
            {
                "rows": [
                    {"ts_code": CODE, "trade_date": PREVIOUS.isoformat(), "preset_name": "n-shape"}
                ]
                if candidates is None
                else candidates
            },
        ),
        _origin("original-fact-archive", {"rows": _facts() if facts is None else facts}),
        _origin("original-config", {"frozen_execution_costs": "original", "commission_bps": "3.1"}),
    )
    windows = (
        api.HistoricalSessionWindow(opens_at=time(9, 30), closes_at=time(11, 30)),
        api.HistoricalSessionWindow(opens_at=time(13), closes_at=time(15)),
    )
    days = tuple(
        api.HistoricalCalendarDay(
            trade_date=PREVIOUS + timedelta(days=i),
            is_open=i in {0, 3},
            windows=windows if i in {0, 3} else (),
        )
        for i in range(4)
    )
    return api.HistoricalReconstructionRequest(
        policy=_policy(),
        origin_materials=materials,
        minute_archives=(
            api.HistoricalRowsArchive(
                origin_object_key=materials[0].object_key, rows_pointer="/rows"
            ),
        ),
        candidate_archives=(
            api.HistoricalCandidateArchive(
                origin_object_key=materials[1].object_key,
                rows_pointer="/rows",
                kind=candidate_kind,
                completed_trade_dates=(PREVIOUS,) if completed else (),
            ),
        ),
        fact_archives=(
            api.HistoricalRowsArchive(
                origin_object_key=materials[2].object_key, rows_pointer="/rows"
            ),
        ),
        timestamp_bases=(
            api.HistoricalTimestampBasis(
                provider_label="confirmed-rt",
                semantics=timestamp_semantics,
                source_timezone=source_timezone,
                explanation="Explicit research interpretation of this provider label.",
            ),
        ),
        calendar_timezone="Asia/Shanghai",
        calendar=days,
        replay_days=(
            api.HistoricalReplayDay(
                trade_date=DAY, initialized_at=datetime.combine(DAY, time(9, 30), SH)
            ),
        ),
        registration=api.HistoricalNativeRegistration(
            strategy_id="n_shape",
            strategy_version="7",
            definition_fingerprint="a" * 64,
            configuration_origin_key=materials[3].object_key,
            configuration_sha256=materials[3].content_sha256,
            registered_at=datetime(2026, 10, 8, 10, tzinfo=SH),
        ),
        prepared_at=datetime(2026, 10, 8, 11, tzinfo=SH),
    )


def _prepare(**kwargs: Any) -> Any:
    return _api().prepare_historical_minute_reconstruction(_request(**kwargs))


def test_full_original_values_prepare_but_do_not_claim_formal_readiness() -> None:
    request = _request()
    result = _api().prepare_historical_minute_reconstruction(request)
    assert result.materials_ready and result.facts_ready
    assert result.preparation_status == "complete"
    assert result.ready_for_original_executor is False
    assert result.formal_authorization == result.policy_installation == "not_assessed"
    assert result.formal_source_published is False
    assert result.source_kind == "reconstructed" and result.display_label == "历史重建"
    assert len(result.minutes) == 240
    assert result.minutes[0].bar_end == datetime.combine(DAY, time(9, 31), SH)
    assert result.minutes[120].bar_end == datetime.combine(DAY, time(13, 1), SH)
    assert result.minutes[-1].bar_end == datetime.combine(DAY, time(15), SH)
    assert result.minutes[0].open == Decimal("10.1234567890123456789")
    assert result.minutes[0].volume == Decimal("123.456")
    assert result.origin_materials == request.origin_materials
    assert result.registration == request.registration
    assert result.registration.registered_at > result.candidates[0].modeled_available_at
    assert result.candidates[0].screen_trade_date == PREVIOUS
    assert result.candidates[0].modeled_available_at == request.replay_days[0].initialized_at
    assert result.candidates[0].time_basis == "modeled"
    assert result.derivations and all(d.time_basis == "modeled" for d in result.derivations)


def test_current_exact_definition_has_modeled_bootstrap_without_registry_backdating() -> None:
    request = _request()
    result = _api().prepare_historical_minute_reconstruction(request)
    visibility = result.native_definition_visibility[0]
    assert visibility.replay_trade_date == DAY
    assert visibility.modeled_available_at == request.replay_days[0].initialized_at
    assert visibility.time_basis == "modeled"
    assert result.registration == request.registration
    assert result.registration.registered_at == datetime(2026, 10, 8, 10, tzinfo=SH)
    assert result.registration.registered_at > visibility.modeled_available_at


def test_end_labeled_source_is_not_shifted_again() -> None:
    rows = _minutes()
    for row in rows:
        row["trade_time"] = (
            datetime.fromisoformat(str(row["trade_time"])) + timedelta(minutes=1)
        ).isoformat()
    result = _prepare(minutes=rows, timestamp_semantics="bar_end")
    assert result.materials_ready
    assert result.minutes[0].bar_end == datetime.combine(DAY, time(9, 31), SH)
    assert result.minutes[-1].bar_end == datetime.combine(DAY, time(15), SH)


def test_timezone_aware_utc_input_retains_the_same_market_instant() -> None:
    rows = _minutes()
    for row in rows:
        row["trade_time"] = (
            datetime.fromisoformat(str(row["trade_time"])).astimezone(ZoneInfo("UTC")).isoformat()
        )
    result = _prepare(minutes=rows, source_timezone=None)
    assert result.materials_ready
    assert result.minutes[0].bar_end == datetime.combine(DAY, time(9, 31), SH)


def test_naive_times_require_explicit_source_timezone() -> None:
    rows = _minutes()
    for row in rows:
        row["trade_time"] = (
            datetime.fromisoformat(str(row["trade_time"])).replace(tzinfo=None).isoformat()
        )
    known = _prepare(minutes=rows)
    unknown = _prepare(minutes=rows, source_timezone=None)
    assert known.materials_ready
    assert not unknown.materials_ready and not unknown.minutes
    assert any(x.code == "minute_timezone_unknown" for x in unknown.unavailable_reasons)


def test_unknown_provider_time_basis_does_not_guess_end_times() -> None:
    result = _prepare(timestamp_semantics="unknown")
    assert not result.materials_ready and not result.minutes
    assert any(x.code == "minute_timestamp_basis_unknown" for x in result.unavailable_reasons)


@pytest.mark.parametrize("bad_start", ["11:30", "12:59", "15:00"])
def test_lunch_or_closed_boundary_records_remain_unavailable(bad_start: str) -> None:
    rows = _minutes()
    extra = dict(rows[0], trade_time=f"{DAY.isoformat()}T{bad_start}:00+08:00")
    result = _prepare(minutes=rows + [extra])
    assert len(result.minutes) == 240
    assert any(x.code == "minute_outside_session" for x in result.unavailable_reasons)


def test_identical_values_deduplicate_with_every_original_row_retained() -> None:
    rows = _minutes()
    result = _prepare(minutes=rows + [dict(rows[0])])
    assert result.materials_ready and len(result.minutes) == 240
    assert len(result.minutes[0].origins) == 2
    assert {x.json_pointer for x in result.minutes[0].origins} == {"/rows/0", "/rows/240"}


@pytest.mark.parametrize("field", ["open", "high", "low", "close", "vol", "amount"])
def test_conflicting_original_value_never_selects_a_winner(field: str) -> None:
    rows = _minutes()
    changed = dict(rows[0], **{field: "999.999"})
    result = _prepare(minutes=rows + [changed])
    assert not result.materials_ready
    assert len(result.minutes) == 239
    assert any(
        x.code == "minute_value_conflict" and len(x.origins) == 2
        for x in result.unavailable_reasons
    )


def test_missing_completed_minute_is_not_filled_or_hidden() -> None:
    rows = _minutes()
    result = _prepare(minutes=rows[:120] + rows[121:])
    assert len(result.minutes) == 239 and not result.materials_ready
    missing = [x for x in result.unavailable_reasons if x.code == "minute_coverage_missing"]
    assert len(missing) == 1 and missing[0].ts_code == CODE
    assert "13:01" in missing[0].detail


def test_candidate_selection_uses_previous_completed_open_day_not_yesterday() -> None:
    result = _prepare(
        candidates=[
            {"ts_code": CODE, "trade_date": PREVIOUS.isoformat(), "preset_name": "n-shape"},
            {
                "ts_code": "future",
                "trade_date": DAY.isoformat(),
                "preset_name": "n-shape",
                "created_at": "2026-09-28T08:00:00+08:00",
            },
        ]
    )
    assert len(result.candidates) == 1 and result.candidates[0].ts_code == CODE
    assert result.candidates[0].screen_trade_date == PREVIOUS
    assert any(x.code == "candidate_not_previous_completed_day" for x in result.unavailable_reasons)


def test_screen_completion_must_be_explicit() -> None:
    result = _prepare(completed=False)
    assert not result.candidates and not result.materials_ready
    assert any(x.code == "candidate_screen_completion_unknown" for x in result.unavailable_reasons)


@pytest.mark.parametrize(
    "kind,code",
    [
        ("intraday", "candidate_intraday_publication_unknown"),
        ("pool", "candidate_pool_version_unknown"),
    ],
)
def test_unpublished_intraday_and_unversioned_pool_stay_unavailable(kind: str, code: str) -> None:
    result = _prepare(candidate_kind=kind)
    assert not result.candidates and not result.materials_ready
    assert any(x.code == code for x in result.unavailable_reasons)


@pytest.mark.parametrize("kind", FACT_KINDS)
def test_each_required_original_fact_gap_is_explicit(kind: str) -> None:
    result = _prepare(facts=[f for f in _facts() if f["kind"] != kind])
    assert result.materials_ready and not result.facts_ready
    gaps = [x for x in result.unavailable_reasons if x.code == "fact_missing"]
    assert len(gaps) == 1 and gaps[0].fact_kind == kind
    assert result.ready_for_original_executor is False


def test_late_or_missing_publication_is_not_replaced_with_created_at() -> None:
    rows = _facts()
    rows[0]["available_at"] = datetime.combine(DAY, time(9, 31), SH).isoformat()
    rows[1]["available_at"] = None
    rows[1]["created_at"] = datetime.combine(PREVIOUS, time(14), SH).isoformat()
    result = _prepare(facts=rows)
    assert not result.facts_ready
    assert {x.code for x in result.unavailable_reasons} >= {
        "fact_not_visible_at_bootstrap",
        "fact_publication_unknown",
    }


def test_prior_reference_cannot_use_current_day_reference_values() -> None:
    rows = _facts()
    next(f for f in rows if f["kind"] == "prior_reference")["reference_trade_date"] = (
        DAY.isoformat()
    )
    result = _prepare(facts=rows)
    assert not result.facts_ready
    assert any(x.code == "fact_reference_day_mismatch" for x in result.unavailable_reasons)


def test_zero_and_false_are_known_values_but_none_is_a_gap() -> None:
    rows = _facts()
    rows[0]["value"] = 0
    assert _prepare(facts=rows).facts_ready
    rows[0]["value"] = None
    result = _prepare(facts=rows)
    assert not result.facts_ready
    assert any(
        x.fact_kind == "eligibility" and x.code == "fact_value_unknown"
        for x in result.unavailable_reasons
    )


def test_calendar_gap_and_naive_bootstrap_are_rejected_by_typed_input() -> None:
    api = _api()
    request = _request()
    bad = request.model_dump(mode="python")
    bad["calendar"] = (request.calendar[0], request.calendar[-1])
    with pytest.raises(ValidationError, match="contiguous"):
        api.HistoricalReconstructionRequest.model_validate(bad)
    with pytest.raises(ValidationError):
        api.HistoricalReplayDay(trade_date=DAY, initialized_at=datetime.combine(DAY, time(9, 30)))


def test_preparation_revalidates_origin_hash_and_config_identity() -> None:
    api = _api()
    request = _request()
    broken = request.origin_materials[0].model_copy(update={"content_sha256": "f" * 64})
    bad = request.model_copy(update={"origin_materials": (broken, *request.origin_materials[1:])})
    with pytest.raises(ValidationError, match="hash"):
        api.prepare_historical_minute_reconstruction(bad)
    bad = request.model_dump(mode="python")
    bad["registration"]["configuration_sha256"] = "b" * 64
    with pytest.raises(ValidationError, match="configuration"):
        api.HistoricalReconstructionRequest.model_validate(bad)


def test_unknown_timezone_is_a_typed_input_rejection() -> None:
    with pytest.raises(ValidationError, match="timezone"):
        _api().HistoricalTimestampBasis(
            provider_label="unconfirmed",
            semantics="minute_start",
            source_timezone="Not/A-Timezone",
            explanation="No usable timezone evidence.",
        )


def test_conflicting_fact_literals_do_not_merge_zero_and_false() -> None:
    rows = _facts()
    rows[0]["value"] = 0
    result = _prepare(facts=rows + [dict(rows[0], value=False)])
    assert not result.facts_ready
    assert any(
        x.code == "fact_value_conflict" and x.fact_kind == "eligibility" and len(x.origins) == 2
        for x in result.unavailable_reasons
    )


def test_numeric_json_original_keeps_precision_without_binary_float_rounding() -> None:
    api = _api()
    request = _request()
    original = request.origin_materials[0]
    payload = original.payload().replace(
        b'"open":"10.1234567890123456789"', b'"open":10.1234567890123456789'
    )
    numeric = MinuteOriginMaterial(
        object_key=original.object_key,
        format="json",
        content_base64=base64.b64encode(payload).decode(),
        content_sha256=hashlib.sha256(payload).hexdigest(),
    )
    request = request.model_copy(
        update={"origin_materials": (numeric, *request.origin_materials[1:])}
    )
    result = api.prepare_historical_minute_reconstruction(request)
    assert result.minutes[0].open == Decimal("10.1234567890123456789")
    assert result.origin_materials[0].payload() == payload


MINUTE_EXPORT_COLUMNS = (
    "ts_code",
    "trade_time",
    "freq",
    "open",
    "high",
    "low",
    "close",
    "vol",
    "amount",
    "source",
    "created_at",
)
SCREEN_EXPORT_COLUMNS = (
    "trade_date",
    "preset_name",
    "ts_code",
    "name",
    "close",
    "pct_chg",
    "extra",
    "created_at",
)


def _columnar_request(role: str = "minute") -> Any:
    api = _api()
    request = _request()
    index = {"minute": 0, "candidate": 1, "fact": 2}[role]
    original = request.origin_materials[index]
    object_rows = json.loads(original.payload())["rows"]
    columns = (
        MINUTE_EXPORT_COLUMNS
        if role == "minute"
        else SCREEN_EXPORT_COLUMNS
        if role == "candidate"
        else tuple(object_rows[0])
    )
    archive = {
        "columns": columns,
        "rows": [[row.get(column) for column in columns] for row in object_rows],
        "row_count": len(object_rows),
    }
    material = _origin(original.object_key, archive)
    parts = request.model_dump(mode="python")
    parts["origin_materials"] = tuple(
        material if at == index else item for at, item in enumerate(request.origin_materials)
    )
    key = {"minute": "minute_archives", "candidate": "candidate_archives", "fact": "fact_archives"}[
        role
    ]
    parts[key][0]["columns_pointer"] = "/columns"
    return api.HistoricalReconstructionRequest.model_validate(parts)


def _alter_columnar_original(request: Any, mutate: Any) -> Any:
    original = request.origin_materials[0]
    archive = json.loads(original.payload())
    mutate(archive)
    material = _origin(original.object_key, archive)
    return request.model_copy(
        update={"origin_materials": (material, *request.origin_materials[1:])}
    )


def test_columnar_export_layout_retains_original_bytes_and_every_cell_pointer() -> None:
    request = _columnar_request()
    result = _api().prepare_historical_minute_reconstruction(request)
    assert result.materials_ready and result.facts_ready
    assert result.origin_materials == request.origin_materials
    origin = result.minutes[0].origins[0]
    assert origin.object_key == request.origin_materials[0].object_key
    assert origin.json_pointer == "/rows/0"
    assert tuple(field.field_name for field in origin.fields) == MINUTE_EXPORT_COLUMNS
    for index, field in enumerate(origin.fields):
        assert field.value_json_pointer == f"/rows/0/{index}"
        assert field.column_name_json_pointer == f"/columns/{index}"
    assert result.minutes[0].volume == Decimal("123.456")
    assert result.minutes[0].amount == Decimal("1234.56789")
    assert result.minutes[0].open == Decimal("10.1234567890123456789")
    assert result.registration == request.registration
    assert not result.ready_for_original_executor and not result.formal_source_published


@pytest.mark.parametrize("role", ["candidate", "fact"])
def test_columnar_candidate_and_fact_rows_keep_original_provenance(role: str) -> None:
    request = _columnar_request(role)
    result = _api().prepare_historical_minute_reconstruction(request)
    assert result.materials_ready and result.facts_ready
    origin = result.candidates[0].origin if role == "candidate" else result.facts[0].origins[0]
    assert origin.json_pointer == "/rows/0"
    assert all(field.column_name_json_pointer is not None for field in origin.fields)
    assert result.origin_materials == request.origin_materials


def test_columnar_identical_duplicates_retain_each_original_cell_reference() -> None:
    request = _alter_columnar_original(
        _columnar_request(), lambda archive: archive["rows"].append(list(archive["rows"][0]))
    )
    result = _api().prepare_historical_minute_reconstruction(request)
    assert result.materials_ready
    first, duplicate = result.minutes[0].origins
    assert first.json_pointer == "/rows/0" and duplicate.json_pointer == "/rows/240"
    assert first.fields[7].value_json_pointer == "/rows/0/7"
    assert duplicate.fields[7].value_json_pointer == "/rows/240/7"
    assert first.fields[7].column_name_json_pointer == duplicate.fields[7].column_name_json_pointer


@pytest.mark.parametrize("header", [[], ["ts_code", "ts_code"], [None], [""], "not-columns"])
def test_columnar_ambiguous_or_unknown_headers_remain_unavailable(header: object) -> None:
    request = _alter_columnar_original(
        _columnar_request(), lambda archive: archive.update(columns=header)
    )
    result = _api().prepare_historical_minute_reconstruction(request)
    assert not result.materials_ready and not result.minutes
    assert any(gap.code == "archive_original_layout_invalid" for gap in result.unavailable_reasons)
    assert result.origin_materials == request.origin_materials


@pytest.mark.parametrize("mutation", ["short", "long", "object"])
def test_columnar_row_width_or_type_is_not_silently_repaired(mutation: str) -> None:
    def alter(archive: dict[str, Any]) -> None:
        row = archive["rows"][0]
        archive["rows"][0] = (
            row[:-1] if mutation == "short" else row + [0] if mutation == "long" else {}
        )

    request = _alter_columnar_original(_columnar_request(), alter)
    result = _api().prepare_historical_minute_reconstruction(request)
    assert not result.materials_ready and not result.minutes
    assert any(gap.code == "archive_original_layout_invalid" for gap in result.unavailable_reasons)


@pytest.mark.parametrize("pointer", ["/missing", "/columns/999", "/rows/00"])
def test_columnar_wrong_original_pointer_remains_unavailable(pointer: str) -> None:
    request = _columnar_request()
    archive = request.minute_archives[0].model_copy(update={"columns_pointer": pointer})
    request = request.model_copy(update={"minute_archives": (archive,)})
    result = _api().prepare_historical_minute_reconstruction(request)
    assert not result.materials_ready
    assert any(gap.code == "archive_original_layout_invalid" for gap in result.unavailable_reasons)


def test_columnar_header_pointer_syntax_and_same_rows_pointer_are_typed_rejections() -> None:
    api = _api()
    with pytest.raises(ValidationError, match="JSON pointer"):
        api.HistoricalRowsArchive(origin_object_key="original", columns_pointer="/bad~2")
    with pytest.raises(ValidationError, match="differ"):
        api.HistoricalRowsArchive(
            origin_object_key="original", rows_pointer="/rows", columns_pointer="/rows"
        )


@pytest.mark.parametrize("field,unknown_name", [("vol", "vol_lots"), ("amount", "amount_wan")])
def test_columnar_unknown_numeric_field_or_unit_alias_is_not_inferred(
    field: str,
    unknown_name: str,
) -> None:
    def alter(archive: dict[str, Any]) -> None:
        archive["columns"][archive["columns"].index(field)] = unknown_name

    request = _alter_columnar_original(_columnar_request(), alter)
    result = _api().prepare_historical_minute_reconstruction(request)
    assert not result.materials_ready and not result.minutes
    assert any(gap.code == "minute_original_record_invalid" for gap in result.unavailable_reasons)
    assert result.origin_materials == request.origin_materials


def test_object_row_compatibility_uses_escaped_original_field_pointers() -> None:
    rows = _minutes()
    rows[0]["source/trace~id"] = "uninterpreted original extra"
    result = _prepare(minutes=rows)
    assert result.materials_ready and result.facts_ready
    origin = result.minutes[0].origins[0]
    extra = next(field for field in origin.fields if field.field_name == "source/trace~id")
    assert origin.json_pointer == "/rows/0"
    assert extra.value_json_pointer == "/rows/0/source~1trace~0id"
    assert extra.column_name_json_pointer is None


def test_columnar_layout_is_never_inferred_without_declared_header_pointer() -> None:
    request = _columnar_request()
    archive = request.minute_archives[0].model_copy(update={"columns_pointer": None})
    request = request.model_copy(update={"minute_archives": (archive,)})
    result = _api().prepare_historical_minute_reconstruction(request)
    assert not result.materials_ready and not result.minutes
    assert any(gap.code == "minute_original_record_invalid" for gap in result.unavailable_reasons)
