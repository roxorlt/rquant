"""Follow the current Serving generation for the web API.

One verified lease is held for the current generation and shared by every request.
``current.json`` is re-read at most every ``pointer_check_seconds`` on the request path
(about 300 bytes) and every ``background_check_seconds`` by a background thread; a new
``generation_id`` is acquired through ``ServingReader.acquire_generation()``, which hashes
and verifies the whole database before opening it read-only.

A new generation that fails verification never replaces the one being served: the API
keeps answering from the last good generation and reports ``degraded`` with the reason.
A replaced lease is closed only after the last request borrowing it has finished, so an
in-flight request always finishes on the generation it started on.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from rquant.dashboard.serving_only_page_data import ServingFreshness, manifest_freshness
from rquant.serving_contracts import (
    FreshnessStatus,
    ServingCurrentPointer,
    ServingGenerationManifest,
)
from rquant.serving_publisher import ServingGenerationLease, ServingReader
from rquant.web.envelope import ServingMeta, ServingState

_MAX_DETAIL_CHARS = 600


def _error_text(error: BaseException) -> str:
    message = " ".join(str(error).split())
    return f"{type(error).__name__}: {message}" if message else type(error).__name__


@dataclass
class _Tracked:
    lease: ServingGenerationLease
    refs: int = 0
    retired: bool = False


@dataclass(frozen=True)
class BorrowedGeneration:
    """What one request sees: a manifest, its pointer, and a private DuckDB cursor."""

    manifest: ServingGenerationManifest
    pointer: ServingCurrentPointer | None
    cursor: Any
    #: Set when a newer generation exists but could not be verified.
    fallback_detail: str | None


class GenerationTracker:
    def __init__(
        self,
        root: str | Path,
        *,
        pointer_check_seconds: float = 2.0,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self.root = Path(root)
        self._pointer_check_seconds = pointer_check_seconds
        self._monotonic = monotonic
        self._lock = threading.Lock()
        self._refresh_lock = threading.Lock()
        self._current: _Tracked | None = None
        self._failure: str | None = None
        self._last_check: float | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._closed = False

    # ---------------------------------------------------------------- refreshing

    def refresh(self) -> None:
        """Check ``current.json`` now and switch to a new, verified generation if any."""

        with self._refresh_lock:
            if self._closed:
                return
            self._last_check = self._monotonic()
            try:
                reader = ServingReader(self.root)
                pointer = reader.current_pointer()
            except Exception as error:  # the root or pointer is unreadable
                self._record_failure(f"serving 指针不可读：{_error_text(error)}")
                return
            with self._lock:
                current = self._current
            if current is not None and current.lease.manifest.generation_id == (
                pointer.generation_id
            ):
                self._record_failure(None)
                return
            try:
                lease = reader.acquire_generation()
            except Exception as error:
                self._record_failure(
                    f"新数据代 {pointer.generation_id[:8]} 校验失败：{_error_text(error)}"
                )
                return
            if self._closed:
                lease.close()
                return
            self._install(lease)

    def maybe_refresh(self) -> None:
        last = self._last_check
        if last is None or self._monotonic() - last >= self._pointer_check_seconds:
            self.refresh()

    def _record_failure(self, detail: str | None) -> None:
        with self._lock:
            self._failure = None if detail is None else detail[:_MAX_DETAIL_CHARS]

    def _install(self, lease: ServingGenerationLease) -> None:
        with self._lock:
            previous = self._current
            self._current = _Tracked(lease=lease)
            self._failure = None
            close_previous = False
            if previous is not None:
                previous.retired = True
                close_previous = previous.refs == 0
        if previous is not None and close_previous:
            previous.lease.close()

    # ---------------------------------------------------------------- borrowing

    @property
    def failure(self) -> str | None:
        with self._lock:
            return self._failure

    @property
    def current_generation_id(self) -> str | None:
        with self._lock:
            return None if self._current is None else self._current.lease.manifest.generation_id

    @contextmanager
    def borrow(self) -> Iterator[BorrowedGeneration | None]:
        """Borrow the current generation for one request; None when nothing is servable."""

        self.maybe_refresh()
        with self._lock:
            tracked = self._current
            failure = self._failure
            if tracked is not None:
                tracked.refs += 1
        if tracked is None:
            yield None
            return
        try:
            cursor = tracked.lease.connection.cursor()
            try:
                yield BorrowedGeneration(
                    manifest=tracked.lease.manifest,
                    pointer=tracked.lease.pointer,
                    cursor=cursor,
                    fallback_detail=failure,
                )
            finally:
                cursor.close()
        finally:
            with self._lock:
                tracked.refs -= 1
                close_now = tracked.retired and tracked.refs == 0
            if close_now:
                tracked.lease.close()

    # ---------------------------------------------------------------- lifecycle

    def start(self, interval_seconds: float) -> None:
        if self._thread is not None:
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run,
            args=(interval_seconds,),
            name="rquant-web-generation-tracker",
            daemon=True,
        )
        self._thread.start()

    def _run(self, interval_seconds: float) -> None:
        while not self._stop.wait(interval_seconds):
            try:
                self.refresh()
            except Exception as error:  # never let the watcher thread die silently
                self._record_failure(f"数据代检查失败：{_error_text(error)}")

    def close(self) -> None:
        """Stop the watcher and release the lease; a closed tracker serves nothing."""

        self._closed = True
        self._stop.set()
        thread = self._thread
        if thread is not None:
            thread.join(timeout=5)
            self._thread = None
        with self._lock:
            current = self._current
            self._current = None
            close_now = current is not None and current.refs == 0
            if current is not None:
                current.retired = True
        if current is not None and close_now:
            current.lease.close()


def serving_meta(
    borrowed: BorrowedGeneration | None,
    *,
    now: datetime,
    stale_after: timedelta,
    failure: str | None = None,
) -> ServingMeta:
    """The envelope's ``serving`` block, with the Streamlit pages' state rules.

    ``manifest_freshness`` decides fresh / stale / degraded from the generation's age and
    every dataset watermark, exactly as ``query_acquired_serving_frame`` does; a newer
    generation that failed verification additionally marks the answer degraded.
    """

    if borrowed is None:
        return ServingMeta(
            generation_id=None,
            built_at=None,
            state=ServingState.UNAVAILABLE,
            detail=(failure or "没有可用的 serving 数据代")[:_MAX_DETAIL_CHARS],
        )
    manifest = borrowed.manifest
    if manifest.built_at > now:
        return ServingMeta(
            generation_id=manifest.generation_id,
            built_at=manifest.built_at,
            state=ServingState.UNAVAILABLE,
            detail="serving generation contains future evidence",
        )
    freshness = manifest_freshness(
        manifest,
        age=max(now - manifest.built_at, timedelta(0)),
        stale_after=stale_after,
    )
    state = {
        ServingFreshness.FRESH: ServingState.READY,
        ServingFreshness.STALE: ServingState.STALE,
        ServingFreshness.DEGRADED: ServingState.DEGRADED,
        ServingFreshness.UNAVAILABLE: ServingState.DEGRADED,
    }[freshness]
    if state is ServingState.READY:
        detail = "serving generation verified"
    else:
        non_fresh = tuple(
            f"{item.dataset_id}:{item.status.value}:{item.reason or 'unspecified'}"
            for item in manifest.watermarks
            if item.status is not FreshnessStatus.FRESH
        )
        detail = f"serving generation {state.value}: " + (
            "; ".join(non_fresh) or "built_at exceeded freshness budget"
        )
    if borrowed.fallback_detail:
        if state is ServingState.READY:
            state = ServingState.DEGRADED
            detail = borrowed.fallback_detail
        else:
            detail = f"{detail}; {borrowed.fallback_detail}"
    return ServingMeta(
        generation_id=manifest.generation_id,
        built_at=manifest.built_at,
        state=state,
        detail=detail[:_MAX_DETAIL_CHARS],
    )
