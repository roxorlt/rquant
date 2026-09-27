"""Pure daily-bar quality rules over one verified, bounded snapshot day."""

from collections import Counter
from datetime import date
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class _EvidenceModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, revalidate_instances="always")


class DailyBarQualityRow(_EvidenceModel):
    ts_code: str = Field(min_length=1)
    close: Decimal | None = Field(ge=0, allow_inf_nan=False)
    volume: Decimal | None = Field(ge=0, allow_inf_nan=False)
    limit_up: Decimal | None = Field(ge=0, allow_inf_nan=False)
    limit_down: Decimal | None = Field(ge=0, allow_inf_nan=False)
    limits_authoritative: bool = Field(strict=True)
    is_suspended: bool | None = Field(strict=True)
    suspension_authoritative: bool = Field(strict=True)

    @model_validator(mode="after")
    def validate_price_limits(self) -> "DailyBarQualityRow":
        if (
            self.limits_authoritative
            and self.limit_up is not None
            and self.limit_down is not None
            and self.limit_down > self.limit_up
        ):
            raise ValueError("limit_down cannot exceed limit_up")
        return self


class FieldNullCount(_EvidenceModel):
    field_name: str = Field(min_length=1)
    observed_rows: int = Field(ge=0, le=10_000, strict=True)
    null_rows: int = Field(ge=0, strict=True)
    max_null_numerator: int = Field(ge=0, strict=True)
    max_null_denominator: int = Field(ge=1, strict=True)

    @model_validator(mode="after")
    def validate_counts_and_threshold(self) -> "FieldNullCount":
        if self.null_rows > self.observed_rows:
            raise ValueError("null_rows cannot exceed observed_rows")
        if self.max_null_numerator > self.max_null_denominator:
            raise ValueError("max_null_numerator cannot exceed max_null_denominator")
        return self


class DailyBarQualityRequest(_EvidenceModel):
    snapshot_id: str = Field(min_length=1)
    dataset_id: Literal["daily_bar"]
    trade_date: date
    rows: tuple[DailyBarQualityRow, ...] = Field(max_length=10_000)
    fields: tuple[FieldNullCount, ...] = ()

    @model_validator(mode="after")
    def validate_unique_evidence(self) -> "DailyBarQualityRequest":
        codes = [row.ts_code for row in self.rows]
        if len(codes) != len(set(codes)):
            raise ValueError("rows must have unique ts_code values")
        names = [field.field_name for field in self.fields]
        if len(names) != len(set(names)):
            raise ValueError("fields must have unique field_name values")
        return self


class DailyBarQualityIssue(_EvidenceModel):
    rule_id: Literal[
        "daily_bar.close_above_limit",
        "daily_bar.close_below_limit",
        "daily_bar.zero_volume_unsuspended",
        "daily_bar.field_null_ratio",
    ]
    trade_date: date
    ts_code: str | None = None
    field_name: str | None = None
    observed_value: Decimal | None = None
    reference_value: Decimal | None = None
    null_rows: int | None = None
    observed_rows: int | None = None
    max_null_numerator: int | None = None
    max_null_denominator: int | None = None


class DailyBarQualityUnassessed(_EvidenceModel):
    rule_id: Literal["daily_bar.close_limit", "daily_bar.zero_volume", "daily_bar.field_null_ratio"]
    reason: Literal[
        "close_missing",
        "limits_unavailable",
        "volume_missing",
        "suspension_unknown",
        "no_observations",
    ]
    field_name: str | None = None
    count: int = Field(ge=1)


class DailyBarQualityReport(_EvidenceModel):
    snapshot_id: str
    trade_date: date
    issues: tuple[DailyBarQualityIssue, ...]
    unassessed: tuple[DailyBarQualityUnassessed, ...]


def audit_daily_bar_quality(request: DailyBarQualityRequest) -> DailyBarQualityReport:
    """Evaluate only facts established by the caller's verified snapshot."""
    issues: list[DailyBarQualityIssue] = []
    unassessed: Counter[tuple[str, str, str | None]] = Counter()
    for row in request.rows:
        if row.close is None:
            unassessed[("daily_bar.close_limit", "close_missing", None)] += 1
        elif not row.limits_authoritative or row.limit_up is None or row.limit_down is None:
            unassessed[("daily_bar.close_limit", "limits_unavailable", None)] += 1
        elif row.close > row.limit_up:
            issues.append(
                DailyBarQualityIssue(
                    rule_id="daily_bar.close_above_limit",
                    trade_date=request.trade_date,
                    ts_code=row.ts_code,
                    observed_value=row.close,
                    reference_value=row.limit_up,
                )
            )
        elif row.close < row.limit_down:
            issues.append(
                DailyBarQualityIssue(
                    rule_id="daily_bar.close_below_limit",
                    trade_date=request.trade_date,
                    ts_code=row.ts_code,
                    observed_value=row.close,
                    reference_value=row.limit_down,
                )
            )

        if row.volume is None:
            unassessed[("daily_bar.zero_volume", "volume_missing", None)] += 1
        elif row.volume == 0:
            if not row.suspension_authoritative or row.is_suspended is None:
                unassessed[("daily_bar.zero_volume", "suspension_unknown", None)] += 1
            elif not row.is_suspended:
                issues.append(
                    DailyBarQualityIssue(
                        rule_id="daily_bar.zero_volume_unsuspended",
                        trade_date=request.trade_date,
                        ts_code=row.ts_code,
                        observed_value=row.volume,
                    )
                )

    for field in request.fields:
        if field.observed_rows == 0:
            unassessed[("daily_bar.field_null_ratio", "no_observations", field.field_name)] += 1
        elif (
            field.null_rows * field.max_null_denominator
            > field.observed_rows * field.max_null_numerator
        ):
            issues.append(
                DailyBarQualityIssue(
                    rule_id="daily_bar.field_null_ratio",
                    trade_date=request.trade_date,
                    field_name=field.field_name,
                    null_rows=field.null_rows,
                    observed_rows=field.observed_rows,
                    max_null_numerator=field.max_null_numerator,
                    max_null_denominator=field.max_null_denominator,
                )
            )

    return DailyBarQualityReport(
        snapshot_id=request.snapshot_id,
        trade_date=request.trade_date,
        issues=tuple(
            sorted(
                issues, key=lambda item: (item.rule_id, item.ts_code or "", item.field_name or "")
            )
        ),
        unassessed=tuple(
            DailyBarQualityUnassessed(
                rule_id=rule_id, reason=reason, field_name=field_name, count=count
            )
            for (rule_id, reason, field_name), count in sorted(
                unassessed.items(), key=lambda item: (item[0][0], item[0][1], item[0][2] or "")
            )
        ),
    )
