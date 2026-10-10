"""Daily pools use caller-declared A-share facts, never present-day fallback lists."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta, timezone
from typing import TYPE_CHECKING, Literal

import pytest
from pydantic import ValidationError

if TYPE_CHECKING:
    from rquant.factor.universe import (
        DailyIndexConstituentBatch,
        DailySecurityBatch,
        DailySecurityFact,
        FactorUniverseRequest,
    )

_TZ = timezone(timedelta(hours=8))
_DAY = date(2026, 9, 29)
_OBSERVED = datetime(2026, 9, 30, 10, tzinfo=_TZ)
_AS_OF = datetime(2026, 9, 30, 12, tzinfo=_TZ)
Selection = Literal["all", "hs300", "zz1000", "gem"]
Board = Literal["main", "gem", "star", "bse"]


def _fact(
    stock_code: str,
    *,
    board: Board | None = "main",
    is_listed: bool = True,
    is_st: bool | None = False,
) -> DailySecurityFact:
    from rquant.factor.universe import DailySecurityFact

    return DailySecurityFact(
        stock_code=stock_code,
        exchange=stock_code[-2:],
        board=board,
        is_listed=is_listed,
        is_st=is_st,
    )


def _batch(
    facts: tuple[DailySecurityFact, ...],
    *,
    trade_date: date = _DAY,
    observed_at: datetime = _OBSERVED,
    source_id: str = "synthetic-a-share-facts",
    source_sha256: str = "a" * 64,
) -> DailySecurityBatch:
    from rquant.factor.universe import DailySecurityBatch

    return DailySecurityBatch(
        trade_date=trade_date,
        source_id=source_id,
        source_sha256=source_sha256,
        source_mode="historical_retrospective",
        security_scope="china_a_share",
        observed_at=observed_at,
        complete_stock_codes=tuple(fact.stock_code for fact in facts),
        facts=facts,
    )


def _membership(
    selection: Literal["hs300", "zz1000"],
    stock_codes: tuple[str, ...],
    *,
    trade_date: date = _DAY,
    observed_at: datetime = _OBSERVED,
) -> DailyIndexConstituentBatch:
    from rquant.factor.universe import DailyIndexConstituentBatch

    return DailyIndexConstituentBatch(
        selection=selection,
        trade_date=trade_date,
        source_id="synthetic-index-archive",
        source_sha256="b" * 64,
        source_mode="historical_retrospective",
        source_kind="daily_complete_membership",
        observed_at=observed_at,
        stock_codes=stock_codes,
    )


def _request(
    selection: Selection,
    securities: DailySecurityBatch | None,
    membership: DailyIndexConstituentBatch | None = None,
    *,
    trade_date: date = _DAY,
    as_of: datetime = _AS_OF,
) -> FactorUniverseRequest:
    from rquant.factor.universe import FactorUniverseRequest

    return FactorUniverseRequest(
        selection=selection,
        trade_date=trade_date,
        as_of=as_of,
        securities=securities,
        membership=membership,
    )


def _example_facts() -> tuple[DailySecurityFact, ...]:
    return (
        _fact("000001.SZ"),
        _fact("000002.SZ", is_listed=False, is_st=None),
        _fact("600000.SH", is_st=True),
        _fact("300001.SZ", board="gem", is_st=True),
        _fact("688001.SH", board="star"),
        _fact("430001.BJ", board="bse", is_st=None),
    )


@pytest.mark.parametrize(
    ("selection", "member_codes", "expected"),
    [
        ("all", None, ("000001.SZ", "688001.SH")),
        ("gem", None, ("300001.SZ", "688001.SH")),
        ("hs300", ("600000.SH", "000001.SZ"), ("000001.SZ", "600000.SH")),
        ("zz1000", ("688001.SH", "300001.SZ"), ("300001.SZ", "688001.SH")),
    ],
)
def test_four_prototype_selections_have_distinct_daily_pools(
    selection: Selection,
    member_codes: tuple[str, ...] | None,
    expected: tuple[str, ...],
) -> None:
    from rquant.factor.universe import select_factor_universe

    members = (
        _membership(selection, member_codes)
        if selection in ("hs300", "zz1000") and member_codes is not None
        else None
    )
    result = select_factor_universe(_request(selection, _batch(_example_facts()), members))

    assert result.selection == selection
    assert result.trade_date == _DAY
    assert result.stock_codes == expected
    assert result.selected_count == len(expected)
    assert result.security_count == 6
    assert result.input_count == (6 if member_codes is None else len(member_codes))
    assert result.source_mode == "historical_retrospective"
    assert result.security_observed_at == _OBSERVED
    assert result.index_observed_at == (_OBSERVED if member_codes is not None else None)
    assert result.excluded.model_dump() == {
        "not_listed": 1 if member_codes is None else 0,
        "beijing": 1 if selection == "all" else 0,
        "st": 2 if selection == "all" else 0,
        "non_target_board": 3 if selection == "gem" else 0,
    }
    assert len(result.input_sha256) == 64


def test_daily_listing_and_st_changes_are_not_filled_from_another_day() -> None:
    from rquant.factor.universe import select_factor_universe

    yesterday = _DAY - timedelta(days=1)
    before = _batch(
        (
            _fact("000001.SZ"),
            _fact("600000.SH", is_st=True),
            _fact("300001.SZ", board="gem", is_listed=False),
        ),
        trade_date=yesterday,
    )
    after = _batch(
        (
            _fact("000001.SZ", is_listed=False),
            _fact("600000.SH"),
            _fact("300001.SZ", board="gem", is_st=True),
        )
    )
    assert select_factor_universe(_request("all", before, trade_date=yesterday)).stock_codes == (
        "000001.SZ",
    )
    assert select_factor_universe(_request("all", after)).stock_codes == ("600000.SH",)
    assert select_factor_universe(_request("gem", before, trade_date=yesterday)).stock_codes == ()
    assert select_factor_universe(_request("gem", after)).stock_codes == ("300001.SZ",)


@pytest.mark.parametrize("stock_code", ["000001.SZ", "600000.SH"])
def test_all_refuses_unknown_st_for_a_listed_sse_or_szse_security(stock_code: str) -> None:
    from rquant.factor.universe import FactorUniverseError, select_factor_universe

    with pytest.raises(FactorUniverseError) as caught:
        select_factor_universe(_request("all", _batch((_fact(stock_code, is_st=None),))))
    assert caught.value.reason == "st_status_unknown"


def test_all_does_not_require_board_facts_or_st_for_already_excluded_securities() -> None:
    from rquant.factor.universe import select_factor_universe

    batch = _batch(
        (
            _fact("000001.SZ", board=None),
            _fact("000002.SZ", board=None, is_listed=False, is_st=None),
            _fact("430001.BJ", board=None, is_st=None),
        )
    )
    result = select_factor_universe(_request("all", batch))
    assert result.stock_codes == ("000001.SZ",)
    assert result.excluded.not_listed == 1
    assert result.excluded.beijing == 1


@pytest.mark.parametrize("stock_code", ["000001.SZ", "600000.SH"])
def test_gem_refuses_unknown_board_for_each_listed_sse_or_szse_security(
    stock_code: str,
) -> None:
    from rquant.factor.universe import FactorUniverseError, select_factor_universe

    batch = _batch((_fact(stock_code, board=None, is_st=None),))
    with pytest.raises(FactorUniverseError) as caught:
        select_factor_universe(_request("gem", batch))
    assert caught.value.reason == "missing_board_fact"


def test_gem_does_not_require_st_or_board_for_non_candidates() -> None:
    from rquant.factor.universe import select_factor_universe

    batch = _batch(
        (
            _fact("300001.SZ", board="gem", is_st=None),
            _fact("688001.SH", board="star", is_st=True),
            _fact("000001.SZ", board=None, is_listed=False, is_st=None),
            _fact("430001.BJ", board=None, is_st=None),
        )
    )
    result = select_factor_universe(_request("gem", batch))
    assert result.stock_codes == ("300001.SZ", "688001.SH")
    assert result.excluded.not_listed == 1
    assert result.excluded.non_target_board == 1
    assert result.excluded.st == result.excluded.beijing == 0


@pytest.mark.parametrize("selection", ["hs300", "zz1000"])
def test_indices_do_not_add_st_or_board_filters(selection: Literal["hs300", "zz1000"]) -> None:
    from rquant.factor.universe import select_factor_universe

    codes = ("000001.SZ", "600000.SH")
    batch = _batch(
        (_fact(codes[0], board=None, is_st=None), _fact(codes[1], board=None, is_st=True))
    )
    assert (
        select_factor_universe(
            _request(selection, batch, _membership(selection, codes))
        ).stock_codes
        == codes
    )


def test_board_uses_supplied_fact_without_inferring_a_code_prefix() -> None:
    from rquant.factor.universe import select_factor_universe

    batch = _batch(
        (
            _fact("000001.SZ", board="gem"),
            _fact("600000.SH", board="star"),
            _fact("300001.SZ", board="main"),
            _fact("688001.SH", board="main"),
        )
    )
    assert select_factor_universe(_request("gem", batch)).stock_codes == (
        "000001.SZ",
        "600000.SH",
    )


@pytest.mark.parametrize("selection", ["all", "hs300", "zz1000", "gem"])
def test_missing_security_source_is_refused(selection: Selection) -> None:
    from rquant.factor.universe import FactorUniverseError, select_factor_universe

    with pytest.raises(FactorUniverseError) as caught:
        select_factor_universe(_request(selection, None))
    assert caught.value.reason == "security_source_missing"


def test_another_security_day_is_refused() -> None:
    from rquant.factor.universe import FactorUniverseError, select_factor_universe

    batch = _batch((_fact("000001.SZ"),), trade_date=_DAY + timedelta(days=1))
    with pytest.raises(FactorUniverseError) as caught:
        select_factor_universe(_request("all", batch))
    assert caught.value.reason == "security_date_mismatch"


@pytest.mark.parametrize("selection", ["hs300", "zz1000"])
def test_missing_index_source_does_not_fall_back_to_all(selection: Selection) -> None:
    from rquant.factor.universe import FactorUniverseError, select_factor_universe

    with pytest.raises(FactorUniverseError) as caught:
        select_factor_universe(_request(selection, _batch((_fact("000001.SZ"),))))
    assert caught.value.reason == "index_source_missing"


def test_other_index_source_is_refused() -> None:
    from rquant.factor.universe import FactorUniverseError, select_factor_universe

    batch = _batch((_fact("000001.SZ"),))
    with pytest.raises(FactorUniverseError) as caught:
        select_factor_universe(_request("hs300", batch, _membership("zz1000", ("000001.SZ",))))
    assert caught.value.reason == "index_selection_mismatch"


@pytest.mark.parametrize("selection", ["all", "gem"])
def test_non_index_selection_refuses_unused_index_provenance(selection: Selection) -> None:
    from rquant.factor.universe import FactorUniverseError, select_factor_universe

    batch = _batch((_fact("000001.SZ"),))
    members = _membership("hs300", ("000001.SZ",), observed_at=_AS_OF + timedelta(seconds=1))
    with pytest.raises(FactorUniverseError) as caught:
        select_factor_universe(_request(selection, batch, members))
    assert caught.value.reason == "unexpected_index_source"


def test_today_constituents_cannot_be_passed_to_yesterdays_request() -> None:
    from rquant.factor.universe import FactorUniverseError, select_factor_universe

    members = _membership("hs300", ("000001.SZ",), trade_date=_DAY + timedelta(days=1))
    with pytest.raises(FactorUniverseError) as caught:
        select_factor_universe(_request("hs300", _batch((_fact("000001.SZ"),)), members))
    assert caught.value.reason == "index_date_mismatch"


@pytest.mark.parametrize("source_kind", ["monthly_weight", "latest_membership"])
def test_index_model_rejects_monthly_or_latest_list_kind(source_kind: str) -> None:
    from rquant.factor.universe import DailyIndexConstituentBatch

    members = _membership("hs300", ("000001.SZ",))
    with pytest.raises(ValidationError) as caught:
        DailyIndexConstituentBatch.model_validate(
            {**members.model_dump(), "source_kind": source_kind}
        )
    assert caught.value.errors()[0]["type"] == "literal_error"


@pytest.mark.parametrize("member_listed", [True, False])
def test_index_rejects_missing_or_unlisted_constituents(member_listed: bool) -> None:
    from rquant.factor.universe import FactorUniverseError, select_factor_universe

    members = _membership("hs300", ("600000.SH",))
    batch = _batch(
        (_fact("000001.SZ"),) if member_listed else (_fact("600000.SH", is_listed=False),)
    )
    with pytest.raises(FactorUniverseError) as caught:
        select_factor_universe(_request("hs300", batch, members))
    assert caught.value.reason == (
        "index_member_missing" if member_listed else "index_member_not_listed"
    )


@pytest.mark.parametrize(
    ("source", "observed_at", "reason"),
    [
        ("security", _AS_OF + timedelta(seconds=1), "security_source_not_visible"),
        ("security", datetime(2026, 9, 28, 23, 59, tzinfo=_TZ), "security_date_in_future"),
        ("index", _AS_OF + timedelta(seconds=1), "index_source_not_visible"),
        ("index", datetime(2026, 9, 28, 23, 59, tzinfo=_TZ), "index_date_in_future"),
    ],
)
def test_source_observation_is_bounded_by_its_day_and_request(
    source: str, observed_at: datetime, reason: str
) -> None:
    from rquant.factor.universe import FactorUniverseError, select_factor_universe

    batch = _batch(
        (_fact("000001.SZ"),),
        observed_at=observed_at if source == "security" else _OBSERVED,
    )
    members = (
        _membership("hs300", ("000001.SZ",), observed_at=observed_at) if source == "index" else None
    )
    selection = "hs300" if source == "index" else "all"
    with pytest.raises(FactorUniverseError) as caught:
        select_factor_universe(_request(selection, batch, members))
    assert caught.value.reason == reason


def test_civil_day_is_shanghai_day_and_real_observation_instant_is_preserved() -> None:
    from rquant.factor.universe import select_factor_universe

    observed_at = datetime(2026, 9, 28, 16, tzinfo=UTC)
    batch = _batch((_fact("000001.SZ"),), observed_at=observed_at)
    result = select_factor_universe(_request("all", batch, as_of=observed_at))
    assert result.stock_codes == ("000001.SZ",)
    assert result.security_observed_at == observed_at
    assert result.source_mode == "historical_retrospective"


@pytest.mark.parametrize("selection", ["all", "gem", "hs300", "zz1000"])
def test_empty_trusted_selection_is_success(selection: Selection) -> None:
    from rquant.factor.universe import select_factor_universe

    batch = _batch((_fact("000001.SZ", is_listed=False, is_st=None),))
    members = _membership(selection, ()) if selection in ("hs300", "zz1000") else None
    result = select_factor_universe(_request(selection, batch, members))
    assert result.stock_codes == ()
    assert result.selected_count == 0
    assert result.input_count == (1 if members is None else 0)
    assert result.excluded.not_listed == (1 if members is None else 0)


def test_empty_declared_security_batch_is_distinct_from_missing_source() -> None:
    from rquant.factor.universe import select_factor_universe

    result = select_factor_universe(_request("all", _batch(())))
    assert result.stock_codes == ()
    assert result.input_count == result.security_count == 0


@pytest.mark.parametrize(
    "stock_code",
    ["60000.SH", "600000.sh", "６０００００.SH", "600000.SH\n", " 600000.SH", "600000.US"],
)
def test_security_codes_are_six_ascii_digits_and_an_exact_exchange_suffix(
    stock_code: str,
) -> None:
    from rquant.factor.universe import DailySecurityFact

    with pytest.raises(ValidationError) as caught:
        DailySecurityFact(
            stock_code=stock_code, exchange="SH", board="main", is_listed=True, is_st=False
        )
    assert caught.value.errors()[0]["type"] == "factor_universe_invalid_stock_code"


@pytest.mark.parametrize("model_kind", ["security_manifest", "index_members"])
def test_codes_are_also_strict_in_source_manifests(model_kind: str) -> None:
    from rquant.factor.universe import DailyIndexConstituentBatch, DailySecurityBatch

    batch = _batch((_fact("000001.SZ"),))
    members = _membership("hs300", ("000001.SZ",))
    payload = batch.model_dump() if model_kind == "security_manifest" else members.model_dump()
    payload["complete_stock_codes" if model_kind == "security_manifest" else "stock_codes"] = (
        "000001",
    )
    model = DailySecurityBatch if model_kind == "security_manifest" else DailyIndexConstituentBatch
    with pytest.raises(ValidationError) as caught:
        model.model_validate(payload)
    assert caught.value.errors()[0]["type"] == "factor_universe_invalid_stock_code"


def test_exchange_must_match_code_suffix() -> None:
    from rquant.factor.universe import DailySecurityFact

    with pytest.raises(ValidationError) as caught:
        DailySecurityFact(
            stock_code="600000.SH", exchange="SZ", board="main", is_listed=True, is_st=False
        )
    assert caught.value.errors()[0]["type"] == "factor_universe_exchange_suffix_mismatch"


@pytest.mark.parametrize(
    ("stock_code", "board"),
    [
        ("000001.SZ", "bse"),
        ("430001.BJ", "main"),
        ("600000.SH", "gem"),
        ("000001.SZ", "star"),
    ],
)
def test_known_board_and_exchange_must_agree(stock_code: str, board: Board) -> None:
    with pytest.raises(ValidationError) as caught:
        _fact(stock_code, board=board)
    assert caught.value.errors()[0]["type"] == "factor_universe_exchange_board_mismatch"


@pytest.mark.parametrize("is_listed", [None, "listed", 1])
def test_listing_status_must_be_known_and_strict_boolean(is_listed: object) -> None:
    from rquant.factor.universe import DailySecurityFact

    with pytest.raises(ValidationError) as caught:
        DailySecurityFact.model_validate(
            {**_fact("000001.SZ").model_dump(), "is_listed": is_listed}
        )
    assert caught.value.errors()[0]["type"] == "bool_type"


@pytest.mark.parametrize("is_st", ["false", 0])
def test_st_status_is_strict_boolean_or_explicit_unknown(is_st: object) -> None:
    from rquant.factor.universe import DailySecurityFact

    with pytest.raises(ValidationError) as caught:
        DailySecurityFact.model_validate({**_fact("000001.SZ").model_dump(), "is_st": is_st})
    assert caught.value.errors()[0]["type"] == "bool_type"


@pytest.mark.parametrize(
    ("model_kind", "invalid_day"),
    [
        ("request", "2026-09-29"),
        ("security", datetime(2026, 9, 29, tzinfo=_TZ)),
        ("index", 20260929),
    ],
)
def test_date_inputs_are_civil_dates_without_python_coercion(
    model_kind: str, invalid_day: object
) -> None:
    model = {
        "request": _request("all", _batch((_fact("000001.SZ"),))),
        "security": _batch((_fact("000001.SZ"),)),
        "index": _membership("hs300", ("000001.SZ",)),
    }[model_kind]
    with pytest.raises(ValidationError) as caught:
        type(model).model_validate({**model.model_dump(), "trade_date": invalid_day})
    assert caught.value.errors()[0]["type"] == "date_type"


@pytest.mark.parametrize("model_kind", ["request", "security", "index"])
def test_observation_and_cutoff_must_be_timezone_aware(model_kind: str) -> None:
    model = {
        "request": _request("all", _batch((_fact("000001.SZ"),))),
        "security": _batch((_fact("000001.SZ"),)),
        "index": _membership("hs300", ("000001.SZ",)),
    }[model_kind]
    field = "as_of" if model_kind == "request" else "observed_at"
    with pytest.raises(ValidationError) as caught:
        type(model).model_validate({**model.model_dump(), field: _OBSERVED.replace(tzinfo=None)})
    assert caught.value.errors()[0]["type"] == "timezone_aware"


@pytest.mark.parametrize(
    ("changed_field", "changed_value", "reason"),
    [
        ("complete_stock_codes", ("000001.SZ", "600000.SH"), "security_manifest_mismatch"),
        ("complete_stock_codes", (), "security_manifest_mismatch"),
        ("facts", (), "security_manifest_mismatch"),
        ("complete_stock_codes", ("000001.SZ", "000001.SZ"), "duplicate_security_manifest"),
    ],
)
def test_security_manifest_and_facts_must_match_exactly(
    changed_field: str, changed_value: object, reason: str
) -> None:
    from rquant.factor.universe import DailySecurityBatch

    batch = _batch((_fact("000001.SZ"),))
    with pytest.raises(ValidationError) as caught:
        DailySecurityBatch.model_validate({**batch.model_dump(), changed_field: changed_value})
    assert caught.value.errors()[0]["type"] == f"factor_universe_{reason}"


@pytest.mark.parametrize("conflicting", [False, True])
def test_duplicate_security_facts_are_refused_even_if_identical(conflicting: bool) -> None:
    from rquant.factor.universe import DailySecurityBatch

    fact = _fact("000001.SZ")
    batch = _batch((fact,))
    with pytest.raises(ValidationError) as caught:
        DailySecurityBatch.model_validate(
            {
                **batch.model_dump(),
                "facts": (fact, _fact("000001.SZ", is_st=conflicting)),
            }
        )
    assert caught.value.errors()[0]["type"] == "factor_universe_duplicate_security_fact"


def test_duplicate_index_members_are_refused() -> None:
    from rquant.factor.universe import DailyIndexConstituentBatch

    members = _membership("hs300", ("000001.SZ",))
    with pytest.raises(ValidationError) as caught:
        DailyIndexConstituentBatch.model_validate(
            {**members.model_dump(), "stock_codes": ("000001.SZ", "000001.SZ")}
        )
    assert caught.value.errors()[0]["type"] == "factor_universe_duplicate_index_member"


@pytest.mark.parametrize("model_kind", ["security", "index"])
@pytest.mark.parametrize("source_id", ["", " source", "source\n"])
def test_source_identity_must_be_explicit_and_unambiguous(model_kind: str, source_id: str) -> None:
    model = (
        _batch((_fact("000001.SZ"),))
        if model_kind == "security"
        else _membership("hs300", ("000001.SZ",))
    )
    with pytest.raises(ValidationError):
        type(model).model_validate({**model.model_dump(), "source_id": source_id})


@pytest.mark.parametrize("model_kind", ["security", "index"])
def test_source_content_digest_is_required_to_be_sha256(model_kind: str) -> None:
    model = (
        _batch((_fact("000001.SZ"),))
        if model_kind == "security"
        else _membership("hs300", ("000001.SZ",))
    )
    with pytest.raises(ValidationError):
        type(model).model_validate({**model.model_dump(), "source_sha256": "not-a-digest"})


@pytest.mark.parametrize("field", ["security_scope", "source_mode"])
def test_source_claim_cannot_be_other_security_scope_or_collection_mode(field: str) -> None:
    from rquant.factor.universe import DailySecurityBatch

    batch = _batch((_fact("000001.SZ"),))
    with pytest.raises(ValidationError):
        DailySecurityBatch.model_validate({**batch.model_dump(), field: "unspecified"})


def test_input_order_does_not_change_selected_result_or_digest() -> None:
    from rquant.factor.universe import (
        DailyIndexConstituentBatch,
        DailySecurityBatch,
        select_factor_universe,
    )

    batch = _batch(_example_facts())
    members = _membership("hs300", ("600000.SH", "000001.SZ"))
    original = select_factor_universe(_request("hs300", batch, members))
    reordered_batch = DailySecurityBatch.model_validate(
        {
            **batch.model_dump(),
            "complete_stock_codes": tuple(reversed(batch.complete_stock_codes)),
            "facts": tuple(reversed(batch.facts)),
        }
    )
    reordered_members = DailyIndexConstituentBatch.model_validate(
        {**members.model_dump(), "stock_codes": tuple(reversed(members.stock_codes))}
    )
    reordered = select_factor_universe(_request("hs300", reordered_batch, reordered_members))
    assert reordered == original


@pytest.mark.parametrize("field", ["source_id", "source_sha256", "observed_at"])
@pytest.mark.parametrize("source", ["security", "index"])
def test_any_source_provenance_change_changes_digest(field: str, source: str) -> None:
    from rquant.factor.universe import select_factor_universe

    batch = _batch((_fact("000001.SZ"),))
    members = _membership("hs300", ("000001.SZ",))
    original = select_factor_universe(_request("hs300", batch, members))
    model = batch if source == "security" else members
    changed = type(model).model_validate(
        {
            **model.model_dump(),
            field: {
                "source_id": "different-source",
                "source_sha256": "c" * 64,
                "observed_at": _OBSERVED + timedelta(seconds=1),
            }[field],
        }
    )
    result = select_factor_universe(
        _request(
            "hs300",
            changed if source == "security" else batch,
            changed if source == "index" else members,
        )
    )
    assert result.stock_codes == original.stock_codes
    assert result.input_sha256 != original.input_sha256


def test_ignored_st_fact_and_selection_change_still_change_digest() -> None:
    from rquant.factor.universe import select_factor_universe

    batch = _batch((_fact("300001.SZ", board="gem", is_st=None),))
    changed_batch = _batch((_fact("300001.SZ", board="gem", is_st=True),))
    original = select_factor_universe(_request("gem", batch))
    changed = select_factor_universe(_request("gem", changed_batch))
    index = select_factor_universe(_request("hs300", batch, _membership("hs300", ("300001.SZ",))))
    assert original.stock_codes == changed.stock_codes == index.stock_codes
    assert len({original.input_sha256, changed.input_sha256, index.input_sha256}) == 3


def test_daily_security_boundary_of_seven_thousand_is_not_truncated() -> None:
    from rquant.factor.universe import MAX_UNIVERSE_SECURITIES, select_factor_universe

    batch = _batch(tuple(_fact(f"{index:06d}.SZ") for index in range(MAX_UNIVERSE_SECURITIES)))
    result = select_factor_universe(_request("all", batch))
    assert result.stock_codes == batch.complete_stock_codes
    assert result.input_count == result.selected_count == result.security_count == 7_000


def test_daily_index_boundary_of_seven_thousand_is_not_truncated() -> None:
    from rquant.factor.universe import MAX_UNIVERSE_SECURITIES, select_factor_universe

    batch = _batch(
        tuple(
            _fact(f"{index:06d}.SZ", board=None, is_st=None)
            for index in range(MAX_UNIVERSE_SECURITIES)
        )
    )
    members = _membership("zz1000", batch.complete_stock_codes)
    result = select_factor_universe(_request("zz1000", batch, members))
    assert result.stock_codes == members.stock_codes
    assert result.input_count == result.selected_count == result.security_count == 7_000


@pytest.mark.parametrize("component", ["facts", "security_manifest", "index_members"])
def test_oversized_daily_sources_are_refused_instead_of_sliced(component: str) -> None:
    from rquant.factor.universe import DailyIndexConstituentBatch, DailySecurityBatch

    codes = tuple(f"{index:06d}.SZ" for index in range(7_001))
    if component == "index_members":
        model = DailyIndexConstituentBatch
        payload = {**_membership("hs300", ()).model_dump(), "stock_codes": codes}
    else:
        model = DailySecurityBatch
        payload = _batch(()).model_dump()
        field = "facts" if component == "facts" else "complete_stock_codes"
        payload[field] = tuple(_fact(code) for code in codes) if component == "facts" else codes
    with pytest.raises(ValidationError) as caught:
        model.model_validate(payload)
    assert caught.value.errors()[0]["type"] == "too_long"


def test_selector_revalidates_copied_instances_instead_of_silently_intersecting() -> None:
    from rquant.factor.universe import select_factor_universe

    batch = _batch((_fact("000001.SZ"),))
    copied = batch.model_copy(update={"complete_stock_codes": ("000001.SZ", "600000.SH")})
    request = _request("all", batch).model_copy(update={"securities": copied})
    with pytest.raises(ValidationError) as caught:
        select_factor_universe(request)
    assert caught.value.errors()[0]["type"] == "factor_universe_security_manifest_mismatch"


def test_cross_layer_models_are_immutable_and_reject_extra_fields() -> None:
    from rquant.factor.universe import select_factor_universe

    batch = _batch((_fact("000001.SZ"),))
    members = _membership("hs300", ("000001.SZ",))
    request = _request("hs300", batch, members)
    result = select_factor_universe(request)
    for model in (batch.facts[0], batch, members, request, result.excluded, result):
        with pytest.raises(ValidationError) as extra:
            type(model).model_validate({**model.model_dump(), "undeclared": True})
        assert extra.value.errors()[0]["type"] == "extra_forbidden"
        field, current = next(iter(model.model_dump().items()))
        with pytest.raises(ValidationError) as frozen:
            setattr(model, field, current)
        assert frozen.value.errors()[0]["type"] == "frozen_instance"
