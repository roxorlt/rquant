"""Point-in-time assembly of live auction-gap candidate inputs."""

from __future__ import annotations

import math
import os
import stat
from datetime import UTC, date, datetime
from numbers import Real
from pathlib import Path
from zoneinfo import ZoneInfo

from rquant.auction_match_gateway import AuctionMatchGateway
from rquant.live_contracts import BatchQualityStatus, LiveChannel
from rquant.live_spool import LiveBatchSpool, LiveSpoolIntegrityError
from rquant.reference_data_registry import (
    ReadonlyReferenceRegistry,
    ReferenceDataIntegrityError,
    ReferenceDataset,
    ReferenceDataUnavailableError,
    ReferenceLookup,
)
from rquant.runtime_contracts import canonical_sha256, normalize_aware_utc
from rquant.runtime_market_session import MarketCalendarAuthority
from rquant.strategy_candidate_producers import (
    AuctionMatchFact,
    PriorDailyVolumeFact,
    PublishedCandidateInputAuthority,
)
from rquant.strategy_candidate_publish_service import AuctionGapCandidateBatch

_SHANGHAI = ZoneInfo("Asia/Shanghai")
_DIRECTORY_FLAGS = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)


class AuctionGapCandidateInputError(RuntimeError):
    """Published evidence cannot form a trustworthy auction-gap input batch."""


def _directory_identity(value: os.stat_result) -> tuple[int, int, int]:
    return (value.st_dev, value.st_ino, value.st_mode)


def _verify_no_symlink_ancestors(path: Path) -> None:
    descriptor = -1
    try:
        descriptor = os.open(path.anchor, _DIRECTORY_FLAGS)
        for component in path.parts[1:-1]:
            before = os.stat(component, dir_fd=descriptor, follow_symlinks=False)
            if stat.S_ISLNK(before.st_mode) or not stat.S_ISDIR(before.st_mode):
                raise AuctionGapCandidateInputError("daily snapshot path is unsafe")
            child = os.open(component, _DIRECTORY_FLAGS, dir_fd=descriptor)
            opened = os.fstat(child)
            current = os.stat(component, dir_fd=descriptor, follow_symlinks=False)
            if _directory_identity(before) != _directory_identity(opened) or _directory_identity(
                opened
            ) != _directory_identity(current):
                os.close(child)
                raise AuctionGapCandidateInputError("daily snapshot path is unsafe")
            os.close(descriptor)
            descriptor = child
    except AuctionGapCandidateInputError:
        raise
    except OSError as exc:
        raise AuctionGapCandidateInputError("daily snapshot path is unsafe") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _prior_five_dates(calendar: MarketCalendarAuthority, trade_date: date) -> tuple[date, ...]:
    if trade_date not in calendar.open_dates:
        raise AuctionGapCandidateInputError("trade_date is not an open session")
    previous = tuple(item for item in calendar.open_dates if item < trade_date)
    if len(previous) < 5:
        raise AuctionGapCandidateInputError("calendar has fewer than five prior open sessions")
    return previous[-5:]


def _private_snapshot_identity(path: Path) -> tuple[os.stat_result, Path]:
    normalized = Path(os.path.normpath(os.fspath(path)))
    if not path.is_absolute() or path != normalized:
        raise ValueError("daily snapshot path must be absolute and normalized")
    _verify_no_symlink_ancestors(path)
    try:
        observed = path.lstat()
    except OSError as exc:
        raise AuctionGapCandidateInputError("daily snapshot is unavailable") from exc
    if (
        stat.S_ISLNK(observed.st_mode)
        or not stat.S_ISREG(observed.st_mode)
        or observed.st_uid != os.geteuid()
        or stat.S_IMODE(observed.st_mode) != 0o600
        or observed.st_nlink != 1
    ):
        raise AuctionGapCandidateInputError("daily snapshot path is unsafe")
    return observed, path


def _same_snapshot(left: os.stat_result, right: os.stat_result) -> bool:
    return (
        left.st_dev,
        left.st_ino,
        left.st_mode,
        left.st_uid,
        left.st_nlink,
        left.st_size,
        left.st_mtime_ns,
        left.st_ctime_ns,
    ) == (
        right.st_dev,
        right.st_ino,
        right.st_mode,
        right.st_uid,
        right.st_nlink,
        right.st_size,
        right.st_mtime_ns,
        right.st_ctime_ns,
    )


def _daily_volume_rows(
    path: Path,
    *,
    ts_codes: tuple[str, ...],
    trade_dates: tuple[date, ...],
) -> tuple[tuple[tuple[str, date, float], ...], datetime]:
    before, normalized = _private_snapshot_identity(path)
    import duckdb

    connection = None
    try:
        connection = duckdb.connect(str(normalized), read_only=True)
        placeholders_codes = ",".join("?" for _ in ts_codes)
        placeholders_dates = ",".join("?" for _ in trade_dates)
        rows = connection.execute(
            f"""
            SELECT ts_code, trade_date, vol
            FROM daily_bar
            WHERE ts_code IN ({placeholders_codes})
              AND trade_date IN ({placeholders_dates})
            ORDER BY ts_code, trade_date
            """,  # noqa: S608 - placeholders bind every selected value
            [*ts_codes, *trade_dates],
        ).fetchall()
    except duckdb.Error as exc:
        raise AuctionGapCandidateInputError("daily snapshot query failed") from exc
    finally:
        if connection is not None:
            connection.close()
    try:
        after = normalized.lstat()
    except OSError as exc:
        raise AuctionGapCandidateInputError("daily snapshot changed while reading") from exc
    _private_snapshot_identity(normalized)
    if not _same_snapshot(before, after):
        raise AuctionGapCandidateInputError("daily snapshot changed while reading")

    normalized_rows: list[tuple[str, date, float]] = []
    for code, row_date, raw_volume in rows:
        if isinstance(raw_volume, bool) or not isinstance(raw_volume, Real):
            raise AuctionGapCandidateInputError("daily volume must be numeric")
        volume = float(raw_volume)
        if not math.isfinite(volume) or volume < 0:
            raise AuctionGapCandidateInputError("daily volume must be finite and nonnegative")
        normalized_rows.append((str(code), row_date, volume))
    expected = {(code, row_date) for code in ts_codes for row_date in trade_dates}
    observed = [(code, row_date) for code, row_date, _ in normalized_rows]
    if len(observed) != len(expected) or set(observed) != expected:
        raise AuctionGapCandidateInputError(
            "daily snapshot must contain exactly one row for every prior-five session"
        )
    available_at = datetime.fromtimestamp(before.st_mtime_ns / 1_000_000_000, tz=UTC)
    return tuple(normalized_rows), available_at


def _required_bool(lookup: ReferenceLookup, field: str) -> bool:
    value = lookup.record.payload.get(field)
    if not isinstance(value, bool):
        raise AuctionGapCandidateInputError(f"{lookup.record.key} {field} must be boolean")
    return value


def _required_number(lookup: ReferenceLookup, field: str, *, positive: bool) -> float:
    value = lookup.record.payload.get(field)
    if isinstance(value, bool) or not isinstance(value, Real):
        raise AuctionGapCandidateInputError(f"{lookup.record.key} {field} must be numeric")
    result = float(value)
    if not math.isfinite(result) or (result <= 0 if positive else result < 0):
        raise AuctionGapCandidateInputError(f"{lookup.record.key} {field} is invalid")
    return result


def _auction_record(spool: LiveBatchSpool, *, trade_date: date, observed_at: datetime):
    current = spool.current(LiveChannel.AUCTION_MATCH)
    if current is None:
        raise AuctionGapCandidateInputError("auction-match current batch is missing")
    records = spool.list_after(LiveChannel.AUCTION_MATCH, sequence=current.sequence - 1)
    if len(records) != 1:
        raise AuctionGapCandidateInputError("auction-match current batch is ambiguous")
    record = records[0]
    envelope = record.envelope
    if envelope.quality_status is not BatchQualityStatus.PUBLISHED:
        raise AuctionGapCandidateInputError("auction-match current batch is not published")
    if envelope.available_at > observed_at:
        raise AuctionGapCandidateInputError("auction-match batch is future evidence")
    local_dates = {
        envelope.event_time_start.astimezone(_SHANGHAI).date(),
        envelope.event_time_end.astimezone(_SHANGHAI).date(),
    }
    if local_dates != {trade_date}:
        raise AuctionGapCandidateInputError("auction-match batch belongs to another session")
    return record


def assemble_auction_gap_candidate_batch(
    *,
    auction_spool: LiveBatchSpool,
    daily_database_path: Path,
    reference_registry: ReadonlyReferenceRegistry,
    calendar: MarketCalendarAuthority,
    trade_date: date,
    observed_at: datetime,
    producer_commit: str,
) -> AuctionGapCandidateBatch:
    """Assemble a live candidate batch without using evidence after ``observed_at``."""

    observed = normalize_aware_utc(observed_at)
    if calendar.generated_at > observed:
        raise AuctionGapCandidateInputError("calendar is future evidence")
    if observed.astimezone(_SHANGHAI).date() != trade_date:
        raise AuctionGapCandidateInputError("observed_at must fall on trade_date")
    prior_dates = _prior_five_dates(calendar, trade_date)

    try:
        record = _auction_record(
            auction_spool,
            trade_date=trade_date,
            observed_at=observed,
        )
        payload = auction_spool.read_payload(record)
    except LiveSpoolIntegrityError as exc:
        raise AuctionGapCandidateInputError("auction-match evidence is invalid") from exc
    envelope = record.envelope
    if envelope.producer_commit != producer_commit:
        raise AuctionGapCandidateInputError("auction-match producer_commit does not match")
    frame = AuctionMatchGateway.decode_payload(payload)
    if len(frame) != envelope.row_count or frame["ts_code"].duplicated().any():
        raise AuctionGapCandidateInputError("auction-match payload row identity is invalid")
    if frame.empty:
        raise AuctionGapCandidateInputError("auction-match payload is empty")
    if set(frame["trade_date"]) != {trade_date}:
        raise AuctionGapCandidateInputError("auction-match payload trade_date does not match")
    ts_codes = tuple(sorted(str(value) for value in frame["ts_code"]))

    try:
        pointer = reference_registry.current_pointer()
        manifest = reference_registry.current_manifest()
    except (ReferenceDataIntegrityError, ReferenceDataUnavailableError) as exc:
        raise AuctionGapCandidateInputError("reference generation is unavailable") from exc
    if pointer.generation_id != manifest.generation_id:
        raise AuctionGapCandidateInputError("reference pointer and manifest disagree")
    if pointer.switched_at > observed or manifest.published_at > observed:
        raise AuctionGapCandidateInputError("reference generation is future evidence")

    daily_rows, daily_available_at = _daily_volume_rows(
        daily_database_path,
        ts_codes=ts_codes,
        trade_dates=prior_dates,
    )
    if daily_available_at > observed:
        raise AuctionGapCandidateInputError("daily snapshot is future evidence")
    daily_snapshot_id = canonical_sha256(
        {
            "contract": "auction-gap-daily-volume/v1",
            "trade_date": trade_date,
            "prior_dates": prior_dates,
            "rows": daily_rows,
        }
    )
    volumes_by_code = {
        code: tuple(row for row in daily_rows if row[0] == code) for code in ts_codes
    }

    facts: list[AuctionMatchFact] = []
    for row in frame.itertuples(index=False):
        event_time = envelope.event_time_end
        try:
            st = reference_registry.as_of(
                dataset_id=ReferenceDataset.ST_STATUS,
                key=row.ts_code,
                event_time=event_time,
                decision_time=observed,
                generation_id=manifest.generation_id,
            )
            suspension = reference_registry.as_of(
                dataset_id=ReferenceDataset.SUSPENSION_STATUS,
                key=row.ts_code,
                event_time=event_time,
                decision_time=observed,
                generation_id=manifest.generation_id,
            )
            listing = reference_registry.as_of(
                dataset_id=ReferenceDataset.LISTING_STATUS,
                key=row.ts_code,
                event_time=event_time,
                decision_time=observed,
                generation_id=manifest.generation_id,
            )
            price_limit = reference_registry.as_of(
                dataset_id=ReferenceDataset.PRICE_LIMIT_REGIME,
                key=row.ts_code,
                event_time=event_time,
                decision_time=observed,
                generation_id=manifest.generation_id,
            )
        except (ReferenceDataIntegrityError, ReferenceDataUnavailableError) as exc:
            raise AuctionGapCandidateInputError(
                f"{row.ts_code} required reference evidence is unavailable"
            ) from exc
        status = listing.record.payload.get("status")
        if not isinstance(status, str):
            raise AuctionGapCandidateInputError(f"{row.ts_code} listing status must be a string")
        reference_available = max(
            pointer.switched_at,
            manifest.published_at,
            st.record.first_available_at,
            suspension.record.first_available_at,
            listing.record.first_available_at,
            price_limit.record.first_available_at,
        )
        fact_available = max(envelope.available_at, reference_available, daily_available_at)
        status_snapshot_id = canonical_sha256(
            {
                "generation_id": manifest.generation_id,
                "records": (
                    st.record.record_id,
                    suspension.record.record_id,
                    listing.record.record_id,
                ),
            }
        )
        limit_snapshot_id = canonical_sha256(
            {
                "generation_id": manifest.generation_id,
                "record": price_limit.record.record_id,
            }
        )
        limit_up_price = _required_number(price_limit, "limit_up_price", positive=True)
        limit_down_price = _required_number(price_limit, "limit_down_price", positive=True)
        if limit_down_price >= limit_up_price:
            raise AuctionGapCandidateInputError(
                f"{row.ts_code} price limit boundaries are not ordered"
            )
        facts.append(
            AuctionMatchFact(
                ts_code=row.ts_code,
                trade_date=trade_date,
                auction_price_raw=row.price,
                auction_vol_shares=row.vol,
                source_volume_ratio=row.volume_ratio,
                session_pre_close_raw=row.pre_close,
                limit_pct=_required_number(price_limit, "limit_percent", positive=True),
                limit_up_price_session_raw=limit_up_price,
                is_st=_required_bool(st, "is_st"),
                is_suspended=_required_bool(suspension, "is_suspended"),
                is_listed=status == "listed",
                limit_eligible=_required_bool(price_limit, "limit_eligible"),
                available_at=fact_available,
                source_snapshot_id=envelope.identity_sha256,
                expected_prior5_trade_dates=prior_dates,
                calendar_available_at=calendar.generated_at,
                calendar_snapshot_id=calendar.content_sha256,
                prior5_daily_volumes=tuple(
                    PriorDailyVolumeFact(
                        trade_date=row_date,
                        daily_volume_lots=volume,
                        available_at=daily_available_at,
                    )
                    for _code, row_date, volume in volumes_by_code[row.ts_code]
                ),
                daily_snapshot_id=daily_snapshot_id,
                reference_snapshot_ids={
                    "session": envelope.identity_sha256,
                    "status": status_snapshot_id,
                    "limit": limit_snapshot_id,
                },
            )
        )

    captured_at = max(
        calendar.generated_at,
        envelope.available_at,
        daily_available_at,
        *(fact.available_at for fact in facts),
    )
    authority_snapshot_id = canonical_sha256(
        {
            "contract": "auction-gap-candidate-input/v1",
            "trade_date": trade_date,
            "captured_at": captured_at,
            "auction_match": envelope.identity_sha256,
            "daily_volume": daily_snapshot_id,
            "reference_generation": manifest.generation_id,
            "trade_calendar": calendar.content_sha256,
        }
    )
    authority = PublishedCandidateInputAuthority(
        trade_date=trade_date,
        captured_at=captured_at,
        quality_status=BatchQualityStatus.PUBLISHED,
        authority_snapshot_id=authority_snapshot_id,
        producer_commit=producer_commit,
    )
    return AuctionGapCandidateBatch(authority=authority, facts=tuple(facts))


__all__ = (
    "AuctionGapCandidateInputError",
    "assemble_auction_gap_candidate_batch",
)
