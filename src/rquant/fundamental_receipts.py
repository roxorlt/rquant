"""Shared validation of immutable fundamental head-to-version receipts."""

from __future__ import annotations

import math
from datetime import date
from typing import Any

from rquant.fundamental_daily import (
    FinancialSource,
    FundamentalDailyQuery,
    FundamentalDailyVersion,
    ValuationSource,
    _decision_at,
    _load_fields,
    _version_identity,
)

FIELD_NAMES = ("pe_ttm", "pb", "dv_ttm", "roe", "or_yoy", "netprofit_yoy")

# Keep this column order shared by the screen and financial data-center readers.
RECEIPT_SELECT = """
    h.ts_code, h.trade_date, h.version_id, h.revision,
    v.version_id, v.ts_code, v.trade_date, v.revision, v.decision_at,
    v.target_report_period, v.target_period_reason,
    CASE WHEN length(v.financial_source_json) <= 4096 THEN v.financial_source_json END,
    CASE WHEN length(v.valuation_source_json) <= 4096 THEN v.valuation_source_json END,
    CASE WHEN length(v.fields_json) <= 16384 THEN v.fields_json END,
    v.pe_ttm, v.pb, v.dv_ttm, v.roe, v.or_yoy, v.netprofit_yoy,
    (SELECT MAX(latest.revision) FROM fundamental_daily_version AS latest
     WHERE latest.ts_code = h.ts_code AND latest.trade_date = h.trade_date)
"""

RECEIPT_JOIN = """
    FROM fundamental_daily_head AS h
    LEFT JOIN fundamental_daily_version AS v
      ON v.version_id = h.version_id
     AND v.ts_code = h.ts_code AND v.trade_date = h.trade_date
"""


class FundamentalReceiptError(ValueError):
    """A head cannot prove one exact, latest, fixed-decision version."""


def checked_fundamental_version(
    row: tuple[Any, ...], *, expected_date: date
) -> FundamentalDailyVersion:
    """Validate the same T17/identity/numeric receipt for both consumers."""
    if len(row) != 21 or row[4] is None or any(value is None for value in row[11:14]):
        raise FundamentalReceiptError("fundamental daily head is incomplete")
    try:
        version = FundamentalDailyVersion.model_validate(
            {
                "version_id": row[4],
                "ts_code": row[5],
                "trade_date": row[6],
                "revision": row[7],
                "decision_at": row[8],
                "target_report_period": row[9],
                "target_period_reason": row[10],
                "financial_source": FinancialSource.model_validate_json(row[11]),
                "valuation_source": ValuationSource.model_validate_json(row[12]),
                "fields": _load_fields(row[13]),
            }
        )
        identity = _version_identity(
            FundamentalDailyQuery(ts_code=version.ts_code, trade_date=version.trade_date),
            version.decision_at,
            version.target_report_period,
            version.target_period_reason,
            version.financial_source,
            version.valuation_source,
            version.fields,
        )
        expected_values = tuple(
            float(version.fields[name].value) if version.fields[name].value is not None else None
            for name in FIELD_NAMES
        )
    except (AttributeError, OverflowError, TypeError, ValueError) as error:
        raise FundamentalReceiptError("fundamental daily version is invalid") from error
    if any(value is not None and not math.isfinite(value) for value in expected_values):
        raise FundamentalReceiptError("fundamental daily value is not representable")
    if (
        row[0] != version.ts_code
        or row[1] != expected_date
        or row[2] != version.version_id
        or row[3] != version.revision
        or row[20] != version.revision
        or version.decision_at != _decision_at(expected_date)
        or identity != version.version_id
        or row[14:20] != expected_values
    ):
        raise FundamentalReceiptError("fundamental daily version receipt mismatch")
    return version
