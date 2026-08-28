"""Build the next-session opening-auction universe from a readonly daily snapshot."""

from __future__ import annotations

import os
import re
import stat
from datetime import date, datetime, time
from pathlib import Path
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict

from rquant.auction_universe_publisher import (
    AuctionUniversePublicationError,
    publish_auction_universe_authority,
)
from rquant.runtime_contracts import canonical_sha256, normalize_aware_utc
from rquant.runtime_market_session import MarketCalendarAuthority

_SHANGHAI = ZoneInfo("Asia/Shanghai")
_TS_CODE_PATTERN = re.compile(r"^[0-9]{6}\.(?:BJ|SH|SZ)$")
_PROTECTION_START = time(9, 15)
_PROTECTION_END = time(15, 10)


class AuctionUniverseSourceError(RuntimeError):
    """The readonly daily snapshot cannot produce a trustworthy universe."""


class AuctionUniverseSourceReceipt(BaseModel):
    model_config = ConfigDict(frozen=True)

    published: bool
    generation_path: Path
    content_sha256: str
    source_snapshot_id: str
    effective_trade_date: date
    reference_trade_date: date
    code_count: int


def _normalized_absolute_path(path: Path) -> Path:
    candidate = Path(path)
    normalized = Path(os.path.normpath(os.fspath(candidate)))
    if not candidate.is_absolute() or candidate != normalized:
        raise ValueError("daily snapshot path must be absolute and normalized")
    return candidate


def _validate_snapshot_stat(value: os.stat_result) -> None:
    if stat.S_ISLNK(value.st_mode) or not stat.S_ISREG(value.st_mode):
        raise AuctionUniverseSourceError("daily snapshot is a symlink or unsafe file")
    if value.st_uid != os.geteuid():
        raise AuctionUniverseSourceError("daily snapshot owner does not match the process")
    if stat.S_IMODE(value.st_mode) != 0o600:
        raise AuctionUniverseSourceError("daily snapshot must have mode 0600")
    if value.st_nlink != 1:
        raise AuctionUniverseSourceError("daily snapshot must have one hard link")


def _snapshot_identity(value: os.stat_result) -> tuple[int, int, int, int, int]:
    return (value.st_dev, value.st_ino, value.st_uid, value.st_mode, value.st_nlink)


def auction_universe_publication_dates(
    calendar: MarketCalendarAuthority,
    observed_at: datetime,
) -> tuple[date, date]:
    observed = normalize_aware_utc(observed_at)
    if calendar.generated_at > observed:
        raise AuctionUniverseSourceError("calendar authority is future evidence")
    local = observed.astimezone(_SHANGHAI)
    local_date = local.date()
    local_time = local.timetz().replace(tzinfo=None)
    if not calendar.coverage_start <= local_date <= calendar.coverage_end:
        raise AuctionUniverseSourceError("observed date is outside calendar coverage")
    is_open = local_date in calendar.open_dates
    if is_open and _PROTECTION_START <= local_time <= _PROTECTION_END:
        raise AuctionUniverseSourceError("auction universe protection window is active")

    if is_open and local_time < _PROTECTION_START:
        effective = local_date
    else:
        effective = next(
            (item for item in calendar.open_dates if item > local_date),
            None,
        )
        if effective is None:
            raise AuctionUniverseSourceError("calendar has no next open date")
    reference = next(
        (item for item in reversed(calendar.open_dates) if item < effective),
        None,
    )
    if reference is None:
        raise AuctionUniverseSourceError("calendar has no prior open date")
    return effective, reference


def _load_codes(database_path: Path, *, reference_trade_date: date) -> tuple[str, ...]:
    import duckdb

    try:
        before = database_path.lstat()
    except OSError as exc:
        raise AuctionUniverseSourceError("daily snapshot is unavailable or unsafe") from exc
    _validate_snapshot_stat(before)
    connection = None
    try:
        connection = duckdb.connect(str(database_path), read_only=True)
        rows = connection.execute(
            """
            SELECT DISTINCT ts_code
            FROM daily_bar
            WHERE trade_date = ?
            ORDER BY ts_code
            """,
            [reference_trade_date],
        ).fetchall()
    except duckdb.Error as exc:
        raise AuctionUniverseSourceError("daily snapshot query failed") from exc
    finally:
        if connection is not None:
            connection.close()
    try:
        after = database_path.lstat()
    except OSError as exc:
        raise AuctionUniverseSourceError("daily snapshot changed while reading") from exc
    _validate_snapshot_stat(after)
    if _snapshot_identity(before) != _snapshot_identity(after):
        raise AuctionUniverseSourceError("daily snapshot changed while reading")

    codes = tuple(str(row[0]).strip().upper() for row in rows)
    if not codes:
        raise AuctionUniverseSourceError("daily snapshot has no rows for the exact prior open date")
    if any(_TS_CODE_PATTERN.fullmatch(code) is None for code in codes):
        raise AuctionUniverseSourceError("daily snapshot contains an invalid ts_code")
    if codes != tuple(sorted(set(codes))):
        raise AuctionUniverseSourceError("daily snapshot universe is not canonical")
    return codes


def publish_auction_universe_from_daily_snapshot(
    *,
    database_path: Path,
    authority_root: Path,
    calendar: MarketCalendarAuthority,
    observed_at: datetime,
    producer_commit: str,
) -> AuctionUniverseSourceReceipt:
    """Publish the next open day's expected auction coverage from prior daily bars."""

    observed = normalize_aware_utc(observed_at)
    effective, reference = auction_universe_publication_dates(calendar, observed)
    snapshot_path = _normalized_absolute_path(database_path)
    codes = _load_codes(snapshot_path, reference_trade_date=reference)
    source_snapshot_id = canonical_sha256(
        {
            "contract": "auction-universe-source/v1",
            "dataset_id": "daily_bar",
            "reference_trade_date": reference,
            "codes": codes,
        }
    )
    try:
        publication = publish_auction_universe_authority(
            authority_root,
            effective_trade_date=effective,
            reference_trade_date=reference,
            available_at=observed,
            producer_commit=producer_commit,
            source_snapshot_id=source_snapshot_id,
            codes=codes,
        )
    except AuctionUniversePublicationError as exc:
        raise AuctionUniverseSourceError("auction universe publication failed") from exc
    return AuctionUniverseSourceReceipt(
        published=publication.published,
        generation_path=publication.generation_path,
        content_sha256=publication.content_sha256,
        source_snapshot_id=source_snapshot_id,
        effective_trade_date=effective,
        reference_trade_date=reference,
        code_count=len(codes),
    )


__all__ = [
    "AuctionUniverseSourceError",
    "AuctionUniverseSourceReceipt",
    "auction_universe_publication_dates",
    "publish_auction_universe_from_daily_snapshot",
]
