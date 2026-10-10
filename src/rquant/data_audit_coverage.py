"""Pure whole-market daily-bar coverage calculation from verified snapshot evidence."""

from datetime import date, timedelta
from decimal import ROUND_HALF_UP, Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class _EvidenceModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, revalidate_instances="always")


class CalendarDay(_EvidenceModel):
    day: date
    is_open: bool = Field(strict=True)


class CalendarEvidence(_EvidenceModel):
    snapshot_id: str = Field(min_length=1)
    exchange: Literal["SSE"]
    days: tuple[CalendarDay, ...]


class DailyBarCount(_EvidenceModel):
    day: date
    row_count: int = Field(ge=0, strict=True)


class DailyBarCountEvidence(_EvidenceModel):
    snapshot_id: str = Field(min_length=1)
    days: tuple[DailyBarCount, ...]


class DailyBarCoverageRequest(_EvidenceModel):
    audit_start: date
    completed_through: date
    calendar: CalendarEvidence
    daily_bar_counts: DailyBarCountEvidence

    @model_validator(mode="after")
    def validate_evidence(self) -> "DailyBarCoverageRequest":
        if self.audit_start > self.completed_through:
            raise ValueError("audit_start must be on or before completed_through")
        if self.calendar.snapshot_id != self.daily_bar_counts.snapshot_id:
            raise ValueError("calendar and daily_bar_counts must have the same snapshot_id")

        expected_days = (self.completed_through - self.audit_start).days + 1
        if len(self.calendar.days) != expected_days or any(
            item.day != self.audit_start + timedelta(days=index)
            for index, item in enumerate(self.calendar.days)
        ):
            raise ValueError(
                "calendar must contain each date in audit range exactly once, in order"
            )
        if not self.calendar.days[-1].is_open:
            raise ValueError("completed_through must be an open SSE trading day")

        previous: date | None = None
        for item in self.daily_bar_counts.days:
            if not self.audit_start <= item.day <= self.completed_through:
                raise ValueError("daily_bar_counts date is outside audit range")
            if previous is not None and item.day <= previous:
                raise ValueError("daily_bar_counts dates must be unique and increasing")
            previous = item.day
        return self


class MonthlyCoverage(_EvidenceModel):
    month: date
    expected_open_days: int = Field(ge=0)
    covered_open_days: int = Field(ge=0)
    coverage_ratio: Decimal | None
    status: Literal["measured", "no_expected_sessions"]


class TradingDayGap(_EvidenceModel):
    start: date
    end: date
    missing_open_days: int = Field(ge=1)


class ClosedDayRows(_EvidenceModel):
    day: date
    row_count: int = Field(gt=0)


class DailyBarCoverageReport(_EvidenceModel):
    snapshot_id: str
    exchange: Literal["SSE"]
    audit_start: date
    completed_through: date
    monthly: tuple[MonthlyCoverage, ...]
    gaps: tuple[TradingDayGap, ...]
    closed_day_rows: tuple[ClosedDayRows, ...]


def audit_daily_bar_coverage(request: DailyBarCoverageRequest) -> DailyBarCoverageReport:
    """Measure whole-market date presence through the caller's last completed SSE session.

    The caller must verify the snapshot and choose ``completed_through``; this
    function cannot infer whether today's upstream collection has finished.
    Ratios are rounded half up to four fractional digits.
    """
    counts_by_day = {item.day: item.row_count for item in request.daily_bar_counts.days}
    monthly_counts: dict[date, list[int]] = {}
    gaps: list[TradingDayGap] = []
    closed_day_rows: list[ClosedDayRows] = []
    gap_start: date | None = None
    gap_end: date | None = None
    gap_size = 0

    for item in request.calendar.days:
        month = date(item.day.year, item.day.month, 1)
        expected_and_covered = monthly_counts.setdefault(month, [0, 0])
        row_count = counts_by_day.get(item.day, 0)
        if not item.is_open:
            if row_count > 0:
                closed_day_rows.append(ClosedDayRows(day=item.day, row_count=row_count))
            continue

        expected_and_covered[0] += 1
        if row_count > 0:
            expected_and_covered[1] += 1
            if gap_start is not None:
                assert gap_end is not None
                gaps.append(TradingDayGap(start=gap_start, end=gap_end, missing_open_days=gap_size))
                gap_start = gap_end = None
                gap_size = 0
        else:
            if gap_start is None:
                gap_start = item.day
            gap_end = item.day
            gap_size += 1

    if gap_start is not None:
        assert gap_end is not None
        gaps.append(TradingDayGap(start=gap_start, end=gap_end, missing_open_days=gap_size))

    monthly = tuple(
        MonthlyCoverage(
            month=month,
            expected_open_days=expected,
            covered_open_days=covered,
            coverage_ratio=(
                (Decimal(covered) / Decimal(expected)).quantize(
                    Decimal("0.0001"), rounding=ROUND_HALF_UP
                )
                if expected
                else None
            ),
            status="measured" if expected else "no_expected_sessions",
        )
        for month, (expected, covered) in monthly_counts.items()
    )
    return DailyBarCoverageReport(
        snapshot_id=request.calendar.snapshot_id,
        exchange=request.calendar.exchange,
        audit_start=request.audit_start,
        completed_through=request.completed_through,
        monthly=monthly,
        gaps=tuple(gaps),
        closed_day_rows=tuple(closed_day_rows),
    )
