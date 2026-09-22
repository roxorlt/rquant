"""Immutable SSE calendar authority and market-minute session gate."""

from __future__ import annotations

import os
import re
import stat
from datetime import date, datetime, time
from enum import StrEnum
from pathlib import Path
from typing import Annotated, Literal, Self
from zoneinfo import ZoneInfo

from pydantic import Field, StringConstraints, ValidationError, model_validator

from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    canonical_sha256,
    normalize_aware_utc,
)
from rquant.strict_json import StrictJsonError, strict_json_loads

CommitSha = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{40}$")]
Sha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]

_COMMIT_PATTERN = re.compile(r"^[0-9a-f]{40}$")
#: The market clock every session decision is taken in. Exported because the read-side
#: gate's no-read window has to be the *same* clock as `may_fetch_market_minute`, not a
#: second copy of the string that can drift from it (#268).
MARKET_TIMEZONE = ZoneInfo("Asia/Shanghai")
_SHANGHAI = MARKET_TIMEZONE
_DIRECTORY_FLAGS = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
_FILE_FLAGS = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
_READ_CHUNK_BYTES = 1024 * 1024
_MAX_CALENDAR_BYTES = 4 * 1024 * 1024


class MarketSessionCalendarError(RuntimeError):
    """The frozen calendar cannot safely establish the market session."""


class MarketSessionPhase(StrEnum):
    PRE_OPEN = "pre_open"
    MORNING = "morning"
    LUNCH = "lunch"
    AFTERNOON = "afternoon"
    CLOSED = "closed"


class MarketCalendarAuthority(RuntimeContractModel):
    schema_version: int = Field(ge=1)
    exchange: Literal["SSE"]
    producer_commit: CommitSha
    coverage_start: date
    coverage_end: date
    open_dates: tuple[date, ...]
    generated_at: AwareUtcDatetime
    content_sha256: Sha256

    @model_validator(mode="after")
    def validate_authority(self) -> Self:
        if self.coverage_start > self.coverage_end:
            raise ValueError("coverage_start must not be after coverage_end")
        if any(
            left >= right for left, right in zip(self.open_dates, self.open_dates[1:], strict=False)
        ):
            raise ValueError("open_dates must be strictly increasing and unique")
        if any(item < self.coverage_start or item > self.coverage_end for item in self.open_dates):
            raise ValueError("open_dates must be within calendar coverage")
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"content_sha256"}))
        if self.content_sha256 != expected:
            raise ValueError("content_sha256 does not bind canonical calendar content")
        return self

    @classmethod
    def create(
        cls,
        *,
        schema_version: int,
        exchange: Literal["SSE"],
        producer_commit: str,
        coverage_start: date,
        coverage_end: date,
        open_dates: tuple[date, ...],
        generated_at: datetime,
    ) -> MarketCalendarAuthority:
        identity = {
            "schema_version": schema_version,
            "exchange": exchange,
            "producer_commit": producer_commit,
            "coverage_start": coverage_start,
            "coverage_end": coverage_end,
            "open_dates": tuple(open_dates),
            "generated_at": normalize_aware_utc(generated_at),
        }
        return cls(**identity, content_sha256=canonical_sha256(identity))


class MarketSessionDecision(RuntimeContractModel):
    observed_at: AwareUtcDatetime
    local_trade_date: date
    phase: MarketSessionPhase
    is_open_date: bool
    may_fetch_market_minute: bool

    @model_validator(mode="after")
    def validate_decision(self) -> Self:
        local = self.observed_at.astimezone(_SHANGHAI)
        if self.local_trade_date != local.date():
            raise ValueError("local_trade_date conflicts with observed_at")
        expected_phase = (
            _phase_at(local.timetz().replace(tzinfo=None))
            if self.is_open_date
            else MarketSessionPhase.CLOSED
        )
        if self.phase is not expected_phase:
            raise ValueError("market session phase conflicts with observed_at")
        expected_fetch = self.is_open_date and self.phase in {
            MarketSessionPhase.MORNING,
            MarketSessionPhase.AFTERNOON,
        }
        if self.may_fetch_market_minute != expected_fetch:
            raise ValueError("may_fetch_market_minute conflicts with market session phase")
        if not self.is_open_date and self.phase is not MarketSessionPhase.CLOSED:
            raise ValueError("non-open dates must be closed")
        return self


def _identity(value: os.stat_result) -> tuple[int, int, int, int, int, int, int, int]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_uid,
        value.st_nlink,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _require_normalized_absolute_path(path: Path) -> Path:
    candidate = Path(path)
    normalized = Path(os.path.normpath(os.fspath(candidate)))
    if not candidate.is_absolute() or candidate != normalized:
        raise ValueError("calendar path must be absolute normalized")
    return candidate


def _open_parent_without_symlinks(path: Path) -> int:
    descriptor = -1
    try:
        descriptor = os.open(path.anchor, _DIRECTORY_FLAGS)
        traversed = Path(path.anchor)
        for component in path.parts[1:]:
            traversed /= component
            try:
                before = os.stat(component, dir_fd=descriptor, follow_symlinks=False)
            except OSError as exc:
                raise MarketSessionCalendarError(
                    f"calendar parent is unavailable: {traversed}"
                ) from exc
            if stat.S_ISLNK(before.st_mode):
                raise MarketSessionCalendarError(f"calendar path contains a symlink: {traversed}")
            if not stat.S_ISDIR(before.st_mode):
                raise MarketSessionCalendarError(f"calendar parent is not a directory: {traversed}")
            try:
                child = os.open(component, _DIRECTORY_FLAGS, dir_fd=descriptor)
            except OSError as exc:
                raise MarketSessionCalendarError(
                    f"calendar parent changed while opening: {traversed}"
                ) from exc
            opened = os.fstat(child)
            if (before.st_dev, before.st_ino, before.st_mode) != (
                opened.st_dev,
                opened.st_ino,
                opened.st_mode,
            ):
                os.close(child)
                raise MarketSessionCalendarError(f"calendar parent identity changed: {traversed}")
            os.close(descriptor)
            descriptor = child
        return descriptor
    except BaseException:
        if descriptor >= 0:
            os.close(descriptor)
        raise


def _read_private_authority(path: Path) -> bytes:
    parent_descriptor = _open_parent_without_symlinks(path.parent)
    descriptor = -1
    try:
        try:
            before = os.stat(path.name, dir_fd=parent_descriptor, follow_symlinks=False)
        except OSError as exc:
            raise MarketSessionCalendarError(f"calendar authority is unavailable: {path}") from exc
        if stat.S_ISLNK(before.st_mode):
            raise MarketSessionCalendarError(f"calendar authority is a symlink: {path}")
        if not stat.S_ISREG(before.st_mode):
            raise MarketSessionCalendarError(f"calendar authority is not a regular file: {path}")
        if before.st_uid != os.geteuid():
            raise MarketSessionCalendarError(
                "calendar authority owner does not match the current process"
            )
        if stat.S_IMODE(before.st_mode) != 0o600:
            raise MarketSessionCalendarError("calendar authority must have mode 0600")
        if before.st_nlink != 1:
            raise MarketSessionCalendarError("calendar authority must have one hard link")
        if before.st_size > _MAX_CALENDAR_BYTES:
            raise MarketSessionCalendarError("calendar authority exceeds size limit")
        try:
            descriptor = os.open(path.name, _FILE_FLAGS, dir_fd=parent_descriptor)
        except OSError as exc:
            raise MarketSessionCalendarError(
                f"calendar authority changed while opening: {path}"
            ) from exc
        opened = os.fstat(descriptor)
        active = os.stat(path.name, dir_fd=parent_descriptor, follow_symlinks=False)
        if _identity(opened) != _identity(before) or _identity(active) != _identity(opened):
            raise MarketSessionCalendarError(
                f"calendar authority identity changed while opening: {path}"
            )
        chunks: list[bytes] = []
        total = 0
        while chunk := os.read(descriptor, _READ_CHUNK_BYTES):
            total += len(chunk)
            if total > _MAX_CALENDAR_BYTES:
                raise MarketSessionCalendarError("calendar authority exceeds size limit")
            chunks.append(chunk)
        after = os.fstat(descriptor)
        current = os.stat(path.name, dir_fd=parent_descriptor, follow_symlinks=False)
        if _identity(after) != _identity(opened) or _identity(current) != _identity(after):
            raise MarketSessionCalendarError(f"calendar authority changed while reading: {path}")
        return b"".join(chunks)
    except MarketSessionCalendarError:
        raise
    except OSError as exc:
        raise MarketSessionCalendarError(
            f"calendar authority changed while reading: {path}"
        ) from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        os.close(parent_descriptor)


def load_market_calendar_authority(path: Path, *, expected_commit: str) -> MarketCalendarAuthority:
    """Load one immutable calendar without mutating its filesystem lifecycle."""

    candidate = _require_normalized_absolute_path(path)
    if not _COMMIT_PATTERN.fullmatch(expected_commit):
        raise ValueError("expected_commit must be a lowercase 40-character Git SHA")
    payload = _read_private_authority(candidate)
    try:
        decoded = strict_json_loads(payload)
        authority = MarketCalendarAuthority.model_validate(decoded)
    except (StrictJsonError, ValidationError, TypeError, ValueError) as exc:
        raise MarketSessionCalendarError(f"invalid calendar authority: {exc}") from exc
    if authority.producer_commit != expected_commit:
        raise MarketSessionCalendarError("calendar producer_commit does not match expected_commit")
    return authority


def _phase_at(local_time: time) -> MarketSessionPhase:
    if local_time < time(9, 30):
        return MarketSessionPhase.PRE_OPEN
    if local_time <= time(11, 30):
        return MarketSessionPhase.MORNING
    if local_time < time(13, 0):
        return MarketSessionPhase.LUNCH
    if local_time <= time(15, 0):
        return MarketSessionPhase.AFTERNOON
    return MarketSessionPhase.CLOSED


def decide_market_session(
    authority: MarketCalendarAuthority, observed_at: datetime
) -> MarketSessionDecision:
    """Return a fail-closed point-in-time decision in the Shanghai market clock."""

    observed_utc = normalize_aware_utc(observed_at)
    local = observed_utc.astimezone(_SHANGHAI)
    local_date = local.date()
    if local_date < authority.coverage_start or local_date > authority.coverage_end:
        raise MarketSessionCalendarError(
            f"local trade date {local_date.isoformat()} is outside calendar coverage"
        )
    if authority.generated_at > observed_utc:
        raise MarketSessionCalendarError("calendar authority was generated after observed_at")
    is_open = local_date in authority.open_dates
    phase = _phase_at(local.timetz().replace(tzinfo=None)) if is_open else MarketSessionPhase.CLOSED
    may_fetch = phase in {MarketSessionPhase.MORNING, MarketSessionPhase.AFTERNOON}
    return MarketSessionDecision(
        observed_at=observed_utc,
        local_trade_date=local_date,
        phase=phase,
        is_open_date=is_open,
        may_fetch_market_minute=may_fetch,
    )


def calendar_refusal_reason(
    calendar: MarketCalendarAuthority,
    observed_at: datetime,
    error: MarketSessionCalendarError,
) -> tuple[str, bool]:
    """把 `decide_market_session` 的两种拒绝分开，并说明各自是软是硬（复核裁定 A / B）。

    `MarketSessionCalendarError` 盖着两件完全不同的事：

    1. **日期超出日历覆盖期**（`runtime_market_session.py:286-289`）——冻结的日历过期了。
       这是「该刷日历了」，是**软降级**：报 `calendar_uncovered:<date>`，role 继续活着。
    2. **日历权威的生成时刻晚于观测时刻**（`:290-291`）——要么时钟被回拨，要么装了错代的
       权威。这不是「数据没到」，是**这台机器现在说的话不可信**，所以**硬失败**：
       原样抛出去，`record_failure` 把它记进 `last_error`，两个 role 一致。

    两者同时成立时按第 2 种处理：时钟不对是更根本的那一个，先修它。
    返回 `(理由标签, 是否硬失败)`。
    """

    if calendar.generated_at > normalize_aware_utc(observed_at):
        return (f"calendar_clock_regressed:{calendar.generated_at.isoformat()}", True)
    local_date = normalize_aware_utc(observed_at).astimezone(MARKET_TIMEZONE).date()
    return (f"calendar_uncovered:{local_date.isoformat()}", False)


def raise_or_label_calendar_refusal(
    calendar: MarketCalendarAuthority,
    observed_at: datetime,
    error: MarketSessionCalendarError,
) -> str:
    """硬的那一种当场抛，软的那一种把标签交回去让调用者降级。"""

    label, hard = calendar_refusal_reason(calendar, observed_at, error)
    if hard:
        raise MarketSessionCalendarError(f"{label}: {error}") from error
    return label


# ---------------------------------------------------------------------------------------
# 本地挂钟窗口的算术（#277 复核 MF-1 / 代码质量 1）
#
# auction-match 的采集窗与 candidate.auction_gap 的装配窗是同一个概念的两个实例，改动前
# 两边各写一套：一边把 `time` 换成整秒去比，一边直接比 `time` 对象。同一个概念两种表达，
# 改窗的时候很容易只改一边。下面这几个函数是两边共用的那一份。
# ---------------------------------------------------------------------------------------


def seconds_of_day(value: time) -> int:
    """本地挂钟时刻的「当日第几秒」。窗口比较一律走整秒，微秒在设置层就被拒了。"""

    return value.hour * 3600 + value.minute * 60 + value.second


def local_window_contains(value: time, *, start: time, end: time) -> bool:
    """`value` 是否落在闭区间 `[start, end]` 里（本地挂钟，整秒）。"""

    observed = seconds_of_day(value)
    return seconds_of_day(start) <= observed <= seconds_of_day(end)


def spread_interval_seconds(*, start: time, end: time, attempts: int) -> int:
    """把 `attempts` 次尝试摊在 `[start, end)` 里的间隔。

    **除的是 `attempts` 不是 `attempts - 1`**，这是复核 MF-1 指出的相位缺陷的修法：
    按 `attempts - 1` 摊开时最后一次的到期时刻**正好等于** `end`，而窗口闸门是「过了 `end`
    就整轮空转」，于是最后一次只在 `end` 那一整秒内可达——2 秒轮询下有一半的相位永远拿不到
    第三次尝试，配置写着 3 次、实际只发 2 次，被吃掉的恰恰是为「数据晚到」准备的那一次。
    除以 `attempts` 之后，最后一次到期后离窗口右界还留着整整一个间隔。
    """

    if attempts < 1:
        raise ValueError("attempts must be positive")
    span = seconds_of_day(end) - seconds_of_day(start)
    return max(span // attempts, 1)


def window_schedule_fits(*, start: time, end: time, attempts: int, interval_seconds: int) -> bool:
    """`attempts` 次尝试（第 k 次在 `start + k*interval`）是否全部落在 `[start, end)` 里。"""

    if attempts < 1 or interval_seconds < 1:
        return False
    last = seconds_of_day(start) + (attempts - 1) * interval_seconds
    return last < seconds_of_day(end)


def auction_windows_are_consistent(
    *,
    capture_start: time,
    capture_end: time,
    input_start: time,
    input_end: time,
) -> bool:
    """采集窗与装配窗是否还对得上。

    装配窗必须与采集窗同时开始（批次最早在采集窗起点之后才可能出现），并且在采集窗结束之后
    才关闭（最后一次成功采集仍要留出装配的余量）。探测定窗时四个常量要一起改，只改一边会让
    竞价链安安静静地什么都不产出——生产画像在生成两份 manifest 时调这个函数，把「只改了一边」
    变成一次当场的拒绝。
    """

    return capture_start == input_start and seconds_of_day(input_end) > seconds_of_day(capture_end)


__all__ = [
    "MARKET_TIMEZONE",
    "MarketCalendarAuthority",
    "MarketSessionCalendarError",
    "MarketSessionDecision",
    "MarketSessionPhase",
    "auction_windows_are_consistent",
    "calendar_refusal_reason",
    "decide_market_session",
    "load_market_calendar_authority",
    "local_window_contains",
    "raise_or_label_calendar_refusal",
    "seconds_of_day",
    "spread_interval_seconds",
    "window_schedule_fits",
]
