"""Hand-computable evidence for the daily-bar quality rules."""

from datetime import date
from decimal import Decimal

import pytest

from rquant import data_audit_quality as quality
from rquant.data_audit_quality import DailyBarQualityRequest, DailyBarQualityRow, FieldNullCount

TRADE_DATE = date(2026, 9, 25)


def _row(
    ts_code: str,
    *,
    close: str | None = "10",
    volume: str | None = "100",
    limit_up: str | None = "11",
    limit_down: str | None = "9",
    limits_authoritative: bool = True,
    is_suspended: bool | None = False,
    suspension_authoritative: bool = True,
) -> DailyBarQualityRow:
    return quality.DailyBarQualityRow(
        ts_code=ts_code,
        close=close,
        volume=volume,
        limit_up=limit_up,
        limit_down=limit_down,
        limits_authoritative=limits_authoritative,
        is_suspended=is_suspended,
        suspension_authoritative=suspension_authoritative,
    )


def _request(
    *rows: DailyBarQualityRow,
    fields: tuple[FieldNullCount, ...] = (),
    dataset_id: str = "daily_bar",
) -> DailyBarQualityRequest:
    return quality.DailyBarQualityRequest(
        snapshot_id="verified-snapshot-1",
        dataset_id=dataset_id,
        trade_date=TRADE_DATE,
        rows=rows,
        fields=fields,
    )


def test_price_above_authoritative_limit_and_missing_first_day_limit() -> None:
    report = quality.audit_daily_bar_quality(
        _request(
            _row("600001.SH", close="11.01"),
            _row("600002.SH", close="12", limit_up=None, limit_down=None),
        )
    )

    assert report.snapshot_id == "verified-snapshot-1"
    assert report.trade_date == TRADE_DATE
    assert [
        (issue.rule_id, issue.ts_code, issue.observed_value, issue.reference_value)
        for issue in report.issues
    ] == [("daily_bar.close_above_limit", "600001.SH", Decimal("11.01"), Decimal("11"))]
    assert [(item.rule_id, item.reason, item.count) for item in report.unassessed] == [
        ("daily_bar.close_limit", "limits_unavailable", 1)
    ]


def test_price_below_lower_limit_and_exact_boundary_is_healthy() -> None:
    report = quality.audit_daily_bar_quality(
        _request(
            _row("600002.SH", close="8.99"),
            _row("600001.SH", close="9"),
            _row("600003.SH", close="11"),
        )
    )

    assert [(issue.rule_id, issue.ts_code, issue.reference_value) for issue in report.issues] == [
        ("daily_bar.close_below_limit", "600002.SH", Decimal("9"))
    ]
    assert report.unassessed == ()


def test_zero_volume_requires_authoritative_unsuspended_status() -> None:
    report = quality.audit_daily_bar_quality(
        _request(
            _row("600003.SH", volume="0", is_suspended=None),
            _row("600001.SH", volume="0", is_suspended=False),
            _row("600002.SH", volume="0", is_suspended=True),
            _row("600004.SH", volume="0", is_suspended=False, suspension_authoritative=False),
        )
    )

    assert [(issue.rule_id, issue.ts_code, issue.observed_value) for issue in report.issues] == [
        ("daily_bar.zero_volume_unsuspended", "600001.SH", Decimal("0"))
    ]
    assert [(item.rule_id, item.reason, item.count) for item in report.unassessed] == [
        ("daily_bar.zero_volume", "suspension_unknown", 2)
    ]


def test_missing_close_and_volume_are_unassessed_not_healthy() -> None:
    report = quality.audit_daily_bar_quality(_request(_row("600001.SH", close=None, volume=None)))

    assert report.issues == ()
    assert {(item.rule_id, item.reason, item.count) for item in report.unassessed} == {
        ("daily_bar.close_limit", "close_missing", 1),
        ("daily_bar.zero_volume", "volume_missing", 1),
    }


def test_untrusted_or_partial_limits_cannot_prove_price_healthy() -> None:
    report = quality.audit_daily_bar_quality(
        _request(
            _row("600001.SH", close="12", limits_authoritative=False),
            _row("600002.SH", close="12", limit_down=None),
        )
    )

    assert report.issues == ()
    assert [(item.rule_id, item.reason, item.count) for item in report.unassessed] == [
        ("daily_bar.close_limit", "limits_unavailable", 2)
    ]


def test_input_stock_codes_are_unique_and_row_count_is_bounded() -> None:
    with pytest.raises(ValueError, match="duplicate|ts_code"):
        _request(_row("600001.SH"), _row("600001.SH"))

    max_rows = tuple(_row(f"{i:06d}.SH") for i in range(10_000))
    assert len(_request(*max_rows).rows) == 10_000
    with pytest.raises(ValueError, match="10,?000|rows"):
        _request(*max_rows, _row("010000.SH"))


@pytest.mark.parametrize(
    ("field", "invalid"),
    [
        ("close", "NaN"),
        ("volume", "Infinity"),
        ("limit_up", "-0.01"),
        ("limit_down", "-1"),
    ],
)
def test_rejects_nonfinite_or_negative_numeric_evidence(field: str, invalid: str) -> None:
    with pytest.raises(ValueError, match=field):
        _row("600001.SH", **{field: invalid})


def test_rejects_inverted_authoritative_price_limits() -> None:
    with pytest.raises(ValueError, match="limit_down|limit_up"):
        _row("600001.SH", limit_up="9", limit_down="11")


def test_stock_findings_have_stable_rule_then_stock_order() -> None:
    report = quality.audit_daily_bar_quality(
        _request(
            _row("600003.SH", close="12", volume="0"),
            _row("600002.SH", close="8", volume="0"),
            _row("600001.SH", close="12", volume="0"),
        )
    )

    assert [(item.rule_id, item.ts_code) for item in report.issues] == [
        ("daily_bar.close_above_limit", "600001.SH"),
        ("daily_bar.close_above_limit", "600003.SH"),
        ("daily_bar.close_below_limit", "600002.SH"),
        ("daily_bar.zero_volume_unsuspended", "600001.SH"),
        ("daily_bar.zero_volume_unsuspended", "600002.SH"),
        ("daily_bar.zero_volume_unsuspended", "600003.SH"),
    ]


def test_null_ratio_only_checks_explicit_fields_with_integer_cross_products() -> None:
    report = quality.audit_daily_bar_quality(
        _request(
            fields=(
                quality.FieldNullCount(
                    field_name="close",
                    observed_rows=100,
                    null_rows=11,
                    max_null_numerator=1,
                    max_null_denominator=10,
                ),
                quality.FieldNullCount(
                    field_name="open",
                    observed_rows=100,
                    null_rows=10,
                    max_null_numerator=1,
                    max_null_denominator=10,
                ),
                quality.FieldNullCount(
                    field_name="volume",
                    observed_rows=3,
                    null_rows=1,
                    max_null_numerator=1,
                    max_null_denominator=3,
                ),
            )
        )
    )

    assert len(report.issues) == 1
    issue = report.issues[0]
    assert (
        issue.rule_id,
        issue.trade_date,
        issue.field_name,
        issue.null_rows,
        issue.observed_rows,
        issue.max_null_numerator,
        issue.max_null_denominator,
    ) == ("daily_bar.field_null_ratio", TRADE_DATE, "close", 11, 100, 1, 10)
    assert report.unassessed == ()


def test_null_ratio_with_no_observations_is_unassessed() -> None:
    report = quality.audit_daily_bar_quality(
        _request(
            fields=(
                quality.FieldNullCount(
                    field_name="close",
                    observed_rows=0,
                    null_rows=0,
                    max_null_numerator=1,
                    max_null_denominator=10,
                ),
            )
        )
    )

    assert report.issues == ()
    assert [
        (item.rule_id, item.reason, item.field_name, item.count) for item in report.unassessed
    ] == [("daily_bar.field_null_ratio", "no_observations", "close", 1)]


@pytest.mark.parametrize(
    ("observed", "null", "numerator", "denominator"),
    [
        (4, 5, 1, 10),
        (-1, 0, 1, 10),
        (4, -1, 1, 10),
        (4, 1, -1, 10),
        (4, 1, 11, 10),
        (4, 1, 0, 0),
        (4, 1, True, 10),
    ],
)
def test_rejects_invalid_null_counts_or_threshold(
    observed: int, null: int, numerator: int, denominator: int
) -> None:
    with pytest.raises(ValueError):
        quality.FieldNullCount(
            field_name="close",
            observed_rows=observed,
            null_rows=null,
            max_null_numerator=numerator,
            max_null_denominator=denominator,
        )


def test_rejects_duplicate_null_fields_and_wrong_dataset_scope() -> None:
    field = quality.FieldNullCount(
        field_name="close",
        observed_rows=10,
        null_rows=1,
        max_null_numerator=1,
        max_null_denominator=10,
    )
    with pytest.raises(ValueError, match="field_name|duplicate"):
        _request(fields=(field, field))
    with pytest.raises(ValueError, match="dataset_id"):
        _request(fields=(field,), dataset_id="minute_bar")


def test_null_field_findings_sort_by_field_name_not_input_order() -> None:
    report = quality.audit_daily_bar_quality(
        _request(
            fields=tuple(
                quality.FieldNullCount(
                    field_name=name,
                    observed_rows=10,
                    null_rows=2,
                    max_null_numerator=1,
                    max_null_denominator=10,
                )
                for name in ("volume", "close", "open")
            )
        )
    )
    assert [item.field_name for item in report.issues] == ["close", "open", "volume"]
