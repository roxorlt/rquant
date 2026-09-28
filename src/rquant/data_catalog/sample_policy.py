"""Small, explicit public sample columns and value rules for the data directory."""

from __future__ import annotations

import math
import re
from datetime import date, datetime
from typing import Final

from rquant.data_catalog.models import CatalogField, SampleValue

# Deliberately excludes source codes, status enums, paths, hashes, JSON and audit details.
# A newly added catalog dataset has no public sample until its columns are reviewed here.
SAMPLE_FIELDS: Final[dict[str, tuple[str, ...]]] = {
    "daily_bar": ("ts_code", "trade_date", "close", "pct_chg", "vol"),
    "stock_status_daily": ("ts_code", "trade_date", "name", "is_st"),
    "minute_bar": ("ts_code", "trade_time", "close", "vol"),
    "auction_bar": ("ts_code", "trade_date", "price", "vol", "volume_ratio"),
    "stock_suspend_event": ("ts_code", "trade_date", "available_at"),
    "stock_suspend_coverage": ("trade_date", "row_count", "queried_at"),
    "adj_factor": ("ts_code", "trade_date", "adj_factor"),
    "limit_list_daily": ("ts_code", "trade_date", "name", "close", "pct_chg", "turnover_ratio"),
    "ths_daily": ("ts_code", "trade_date", "close", "pct_change", "vol"),
    "dc_daily": ("ts_code", "trade_date", "close", "pct_change", "amount"),
    "ths_index": ("ts_code", "name", "member_count", "updated_at"),
    "ths_member": ("board_code", "con_code", "con_name", "updated_at"),
    "dc_index": ("ts_code", "trade_date", "name", "pct_change", "up_num", "down_num"),
    "dc_member": ("board_code", "con_code", "con_name", "trade_date"),
    "kpl_concept": ("board_code", "board_name", "con_code", "con_name", "trade_date"),
    "kpl_concept_daily": (
        "trade_date",
        "board_code",
        "board_name",
        "con_code",
        "con_name",
        "hot_num",
    ),
    "moneyflow": ("ts_code", "trade_date", "large_net_amount"),
    "moneyflow_dc": ("ts_code", "trade_date", "name", "net_amount", "net_amount_rate"),
    "moneyflow_ths": ("ts_code", "trade_date", "name", "net_amount"),
    "moneyflow_ind_ths": ("ts_code", "trade_date", "industry", "pct_change", "net_amount"),
    "moneyflow_ind_dc": ("ts_code", "trade_date", "name", "net_amount", "net_amount_rate"),
    "moneyflow_cnt_ths": ("ts_code", "trade_date", "name", "net_amount"),
    "moneyflow_mkt_dc": (
        "trade_date",
        "close_sh",
        "pct_change_sh",
        "close_sz",
        "pct_change_sz",
        "net_amount",
    ),
    "limit_up_pool_daily": (
        "ts_code",
        "trade_date",
        "name",
        "close",
        "pct_chg",
        "consecutive_boards",
    ),
}

CODE_FIELDS: Final = frozenset({"ts_code", "board_code", "con_code"})
NAME_FIELDS: Final = frozenset({"name", "con_name", "board_name", "industry"})
CODE_RE = re.compile(r"[A-Z0-9]{2,12}(?:\.[A-Z]{2,4})?\Z")
NAME_RE = re.compile(r"[\u3400-\u9fffA-Za-z0-9 *+()（）·ⅠⅡⅢ]{1,60}\Z")
MAX_SAFE_INTEGER = 2**53 - 1


def sample_fields(dataset_id: str, fields: list[CatalogField]) -> list[CatalogField]:
    """Return only reviewed columns, in their intentional display order."""
    selected = SAMPLE_FIELDS.get(dataset_id, ())
    by_key = {field.key: field for field in fields}
    missing = set(selected) - by_key.keys()
    if missing:
        raise ValueError(f"sample columns missing from catalog: {dataset_id}: {sorted(missing)}")
    return [by_key[key] for key in selected]


def public_sample_value(field: CatalogField, value: object) -> SampleValue:
    """Normalize a selected value; untrusted names and codes become blank."""
    if value is None:
        return None
    kind = field.data_type.upper()
    if kind == "VARCHAR":
        if not isinstance(value, str):
            raise ValueError("sample text has wrong type")
        cleaned = value.strip()
        if field.key in CODE_FIELDS:
            return cleaned if CODE_RE.fullmatch(cleaned) else None
        if field.key in NAME_FIELDS:
            if not NAME_RE.fullmatch(cleaned):
                return None
            # These A-share names need a Hanzi character; ASCII-only text can be an internal code.
            return cleaned if any("\u3400" <= char <= "\u9fff" for char in cleaned) else None
        raise ValueError(f"unapproved sample text field: {field.key}")
    if kind == "DATE":
        if isinstance(value, datetime):
            raise ValueError("date field received datetime")
        if isinstance(value, date):
            return value.isoformat()
        if isinstance(value, str):
            if date.fromisoformat(value).isoformat() != value:
                raise ValueError("sample date must be canonical ISO")
            return value
        raise ValueError("sample date has wrong type")
    if kind.startswith("TIMESTAMP"):
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, str):
            if datetime.fromisoformat(value).isoformat() != value:
                raise ValueError("sample time must be canonical ISO")
            return value
        raise ValueError("sample time has wrong type")
    if kind == "BOOLEAN":
        if type(value) is not bool:
            raise ValueError("sample boolean has wrong type")
        return value
    if kind in {"INTEGER", "BIGINT", "SMALLINT"}:
        if isinstance(value, str):
            if (
                value.lstrip("-").isdecimal()
                and str(int(value)) == value
                and abs(int(value)) > MAX_SAFE_INTEGER
            ):
                return value
            raise ValueError("sample integer has wrong format")
        if type(value) is not int:
            raise ValueError("sample integer has wrong type")
        return str(value) if abs(value) > MAX_SAFE_INTEGER else value
    if kind in {"DOUBLE", "FLOAT"}:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError("sample number has wrong type")
        try:
            number = float(value)
        except OverflowError as exc:
            raise ValueError("sample number is out of range") from exc
        return number if math.isfinite(number) else None
    raise ValueError(f"unapproved sample type: {field.data_type}")
