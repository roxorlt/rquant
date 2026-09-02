from __future__ import annotations

import ast
import builtins
import hashlib
import hmac
import inspect
import json
import multiprocessing
import multiprocessing.connection
import os
import random
import re
import shutil
import signal
import socket
import sqlite3
import stat
import struct
import subprocess
import sys
import threading
import time
import tracemalloc
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Literal
from uuid import UUID, uuid4

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
from pydantic import ValidationError

from rquant.lab_daemon import LabDaemonConfigurationError
from rquant.lab_job_protocol import InvalidCommandEnvelopeError
from rquant.lab_result_digest import (
    CURRENT_CONTENT_DIGEST_ALGORITHM,
    CURRENT_RESULT_MANIFEST_SCHEMA_VERSION,
)
from rquant.lab_shard_protocol import (
    LabClaimRevokedError,
    LabClaimSpool,
    LabClaimSupersededError,
    LabReportReceipt,
    LabReportSpool,
    LabShardClaim,
    LabShardFailed,
    LabShardHeartbeat,
    LabShardSucceeded,
    LabShardTelemetry,
    LabWorkerReport,
    LabWorkerStopped,
)
from rquant.lab_worker import LabWorkerFailure
from rquant.research_run_spec import DatasetSnapshotIdentity, ResearchRunSpec
from rquant.strategy_job_adapters import (
    LabShardExecutionResult,
    LabShardTable,
    StrategyShardPayload,
    ValidatedStrategyShard,
    default_strategy_job_adapter_registry,
)
from tests.unit.test_strategy_job_adapters import (
    _claim,
    _nshape_compare_spec,
    _p13_frozen_claim,
)

NOW = datetime(2026, 7, 24, 0, 1, tzinfo=UTC)
# Almost every budget in this file waits for a spawned child to become
# observable, and that child is a fresh CPython. How long one costs is a
# property of the host, not of the assertion, so the literals below are read as
# multiples of a measured interpreter start rather than as wall-clock constants:
# on a shared x64 CI runner 44 of these cases went red at once because the
# literals were a fast developer machine's, and raising the literals only moves
# the cliff.
#
# The measurement has to be the cost that actually varies. A bare interpreter
# start does not: GitHub's x64 runner starts one as fast as this laptop, and
# calibrating on it left the scale at exactly 1.0 while 30 cases stayed red.
# What a spawned child really pays is starting an interpreter *and re-importing
# this module* - pandas, pyarrow and the whole rquant surface - so that is what
# gets measured. _OBSERVE_REFERENCE_SECONDS is the cost the literals were sized
# against - half of what this module's import costs on the development machine
# today, because the module has roughly doubled since those numbers were
# written and they are marginal even here. Every observation window is that
# many child start-ups; a slower host gets proportionally more.
_OBSERVE_REFERENCE_SECONDS = 0.35
_observe_scale = 1.0


@pytest.fixture(scope="module", autouse=True)
def _calibrate_observation_budgets() -> None:
    """Measure one child's start-up-and-import once, before any case runs.

    Measuring lazily inside whichever case happens to call `_observe` first is
    not safe here: these cases install signal handlers, patch multiprocessing
    and hold containment state, and a probe fired inside one of them can block.
    A module fixture pays the cost once, in pytest's own context.
    """
    global _observe_scale
    root = Path(__file__).parents[2]
    environment = {**os.environ, "PYTHONPATH": str(root)}
    samples: list[float] = []
    for _ in range(2):
        started = time.monotonic()
        subprocess.run(
            [sys.executable, "-c", "import tests.unit.test_lab_worker"],
            cwd=root,
            env=environment,
            check=True,
            capture_output=True,
            timeout=600,
        )
        samples.append(time.monotonic() - started)
    _observe_scale = max(1.0, min(samples) / _OBSERVE_REFERENCE_SECONDS)


def _observe(seconds: float) -> float:
    """Scale a budget for observing a spawned child by this host's spawn cost."""
    return seconds * _observe_scale


def _reap_leaked_spawned_child(child: multiprocessing.process.BaseProcess) -> None:
    """Kill a child that outlived its case, and the session it may lead.

    The children these cases start can be process-group leaders that ignore
    SIGTERM, and their own descendants are not multiprocessing objects at all,
    so `Process.kill()` alone would leave the rest of the tree behind. Kill the
    group first, but only when this child really leads one of its own - passing
    a non-leader's pid to `killpg` would name some unrelated group, and passing
    the runner's own would kill pytest.
    """
    pid = child.pid
    if pid is not None:
        with suppress(BaseException):
            if os.getpgid(pid) == pid != os.getpgid(0):
                os.killpg(pid, signal.SIGKILL)
    with suppress(BaseException):
        child.kill()
    with suppress(BaseException):
        child.join(_observe(2))
    with suppress(BaseException):
        child.close()


@pytest.fixture(autouse=True)
def _fail_on_leaked_spawned_child() -> Iterator[None]:
    """Fail the case that leaks a spawned child instead of hanging the session.

    `multiprocessing.util._exit_function` joins every *non-daemon* child at
    interpreter shutdown, with no timeout, and `threading._shutdown` does the
    same for non-daemon threads. Several cases here start children that install
    SIG_IGN for SIGTERM deliberately, so one that escapes its own cleanup does
    not just fail the next `active_children()` assertion in this module: it
    parks the whole session in that atexit join *after* the summary has been
    printed, which is how one CI shard displayed `5 failed, 3620 passed` and
    then burned its 75-minute job limit with nothing left to report. Naming the
    case that leaked - and reaping what it left, so the run still ends - keeps
    that failure mode inside the case that caused it. The guard reports; it
    does not excuse: a leak is a red result either way.
    """
    yield
    # A case can return a moment before a child it has already terminated is
    # reaped. `active_children()` joins whatever has exited, so poll for that
    # rather than race it; anything still standing afterwards is a real leak.
    deadline = time.monotonic() + _observe(2)
    while (
        multiprocessing.active_children() or _live_non_daemon_threads()
    ) and time.monotonic() < deadline:
        time.sleep(0.01)
    children = multiprocessing.active_children()
    threads = _live_non_daemon_threads()
    if not children and not threads:
        return
    described = sorted(f"child {child.name}(pid={child.pid})" for child in children)
    described += sorted(f"thread {thread.name}" for thread in threads)
    for child in children:
        _reap_leaked_spawned_child(child)
    pytest.fail(
        "case leaked something that blocks interpreter exit: "
        + ", ".join(described)
        + " (multiprocessing and threading both join non-daemon survivors at "
        "shutdown, with no timeout)"
    )


def _live_non_daemon_threads() -> tuple[threading.Thread, ...]:
    return tuple(
        thread
        for thread in threading.enumerate()
        if thread is not threading.main_thread() and thread.is_alive() and not thread.daemon
    )


# The markers those windows wait for are written by a spawned CPython that
# re-imports this module first; on the machine these cases were written that is
# roughly a second and a half, so a flat two seconds left almost no margin even
# before the host scale applies.
_CHILD_STARTUP_SECONDS = 1.5


def _child_startups(count: float) -> float:
    """Budget for a marker a spawned child writes `count` start-ups from now."""
    return _observe(_CHILD_STARTUP_SECONDS * count)


def _await_markers(*paths: Path, budget: float) -> None:
    """Wait for every marker a spawned child writes before acting on it.

    These cases used to synchronise on the blocked callback's marker alone and
    then assert, afterwards, that the shard child had recorded its session pid.
    Both children exist by then on a fast host, so the second fact looked
    implied by the first; on a slower one the shard child was still starting
    when the case requested a stop, and it was reaped before it ever wrote.
    The precondition the later assertions need is that *both* children are
    observable, so both are waited for.
    """
    deadline = time.monotonic() + budget
    for path in paths:
        while not path.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        assert path.exists(), f"child marker never appeared: {path}"


def _probe_ceiling(timeout_seconds: float) -> float:
    """The ceiling a standalone probe declares: its timeout plus one child.

    Read from the worker's own budget rather than copied as a literal. The
    subject of the cases that use it is "the probe is bounded and reaps what it
    started", and the bound they are entitled to assert is exactly the one the
    module publishes - a copied number goes stale the moment the published one
    is resized, which is what just happened to it.
    """
    from rquant.lab_worker import _AUTHORITY_SPAWN_ALLOWANCE_MICROSECONDS

    return timeout_seconds + _AUTHORITY_SPAWN_ALLOWANCE_MICROSECONDS / 1_000_000


def _deadline_reachable_in(count: float) -> timedelta:
    """A ResearchRunSpec deadline a run can actually *reach* on this host.

    For the cases that force the deadline with an injected clock at an exact
    point - after execute, mid bundle write, at the atomic rename - the literal
    is not the subject; the injection is. All the literal has to do is leave
    enough real time to get there, and getting there costs a shard child
    start-up. Written as one flat second it was a fast machine's number, and on
    a slower host the run hit the real deadline while the child was still
    importing, so the tick came back 'stopped' instead of producing the failure
    the injection exists to produce.
    """
    return timedelta(seconds=_child_startups(count))


# The shared temporary root the wire-session tests bind AF_UNIX sockets under.
# "/tmp" resolves to the macOS spelling of that directory and stays "/tmp" on
# Linux; the tests used to hard-code the macOS path, which Linux does not have.
SHARED_TMP_ROOT = Path("/tmp").resolve(strict=True)
# CPython's multiprocessing client only offers the SHA-256 challenge from 3.12
# on; on 3.11 it answers the legacy MD5 challenge, which the wire listener still
# accepts and reports as "legacy-md5". The negotiated mode is therefore a
# property of the running stdlib, not a constant.
STDLIB_WIRE_DIGEST_MODE = (
    "sha256" if hasattr(multiprocessing.connection, "_ALLOWED_DIGESTS") else "legacy-md5"
)


def test_stop_signal_wakes_an_active_waiter_immediately() -> None:
    from rquant.lab_worker import LabStopSignal

    stop = LabStopSignal()
    waiting = threading.Event()
    returned = threading.Event()

    def wait_for_stop() -> None:
        waiting.set()
        stop.wait(5)
        returned.set()

    waiter = threading.Thread(target=wait_for_stop, daemon=True)
    waiter.start()
    assert waiting.wait(timeout=_observe(1))

    stop.request()

    assert returned.wait(timeout=_observe(0.2))
    waiter.join(timeout=_observe(0.2))
    assert not waiter.is_alive()


def _legacy_canonical_shard_frame_digest(frame: pd.DataFrame) -> str:
    raw = frame.to_json(
        orient="table",
        date_format="iso",
        date_unit="us",
        double_precision=15,
        force_ascii=True,
        index=False,
    )
    canonical = json.dumps(
        json.loads(raw),
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def test_canonical_shard_frame_digest_matches_legacy_small_fixture() -> None:
    from rquant.lab_worker import canonical_shard_frame_digest

    frame = pd.DataFrame(
        {
            "code": ["000001.SZ", "\u6d66\u53d1|`<b>"],
            "ret_pct": [1.25, float("nan")],
            "trade_time": pd.to_datetime(["2026-07-20 09:31:00", "2026-07-20 09:32:00"]),
        }
    )
    raw = frame.to_json(
        orient="table",
        date_format="iso",
        date_unit="us",
        double_precision=15,
        force_ascii=True,
        index=False,
    )
    legacy = json.dumps(
        json.loads(raw),
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")

    assert canonical_shard_frame_digest(frame) == hashlib.sha256(legacy).hexdigest()


@pytest.mark.parametrize(
    ("frame", "expected"),
    [
        (
            pd.DataFrame({"v": pd.to_timedelta(["1 days 01:02:03.123456", None])}),
            "fb7349beeeb2d0c907eda458e203cd7ddc01f5ab6677db383daa67e3545679d7",
        ),
        (
            pd.DataFrame({"v": pd.Categorical(pd.Series([1, None, 2], dtype="Int64"))}),
            "4b3be4ac9e133429202e5a7c2d6b6943395355b156ad35c9842797b0ee2cb9f2",
        ),
    ],
    ids=["timedelta-nat", "nullable-integer-categorical"],
)
def test_canonical_shard_frame_digest_matches_legacy_context_fixed_vectors(
    frame: pd.DataFrame,
    expected: str,
) -> None:
    from rquant.lab_worker import canonical_shard_frame_digest

    assert _legacy_canonical_shard_frame_digest(frame) == expected
    assert canonical_shard_frame_digest(frame) == expected


@pytest.mark.parametrize(
    ("frame", "expected"),
    [
        (
            pd.DataFrame({"v": pd.Series([0, 2**63, 2**64 - 1], dtype="uint64")}),
            "dd4738d19023d905d86161cac1756b829316bfd6a452c02df488c0697ef3cff2",
        ),
        (
            pd.DataFrame({"v": pd.Series([0, None, 2**63, 2**64 - 1], dtype="UInt64")}),
            "034ddd9774438dcd5af3b94ba4f2023ae6fb6110f33cc5d820929d36c4ba675c",
        ),
        (
            pd.DataFrame({"v": pd.Categorical(pd.Series([0, 2**63, 2**64 - 1], dtype="UInt64"))}),
            "807457ac947b02abb6fbd67a920b8bd1d41a216c597ce310651d94e0f8dc92c0",
        ),
        (
            pd.DataFrame(
                {"v": pd.Categorical(pd.Series([0, None, 2**63, 2**64 - 1], dtype="UInt64"))}
            ),
            "3e55fb177e3ca66c033b30a3aa01332d18da11ac92b453b06c722194f20b5031",
        ),
    ],
    ids=["uint64", "nullable-uint64", "unsigned-category", "unsigned-category-na"],
)
def test_canonical_shard_frame_digest_matches_legacy_unsigned_fixed_vectors(
    frame: pd.DataFrame,
    expected: str,
) -> None:
    from rquant.lab_worker import canonical_shard_frame_digest

    assert _legacy_canonical_shard_frame_digest(frame) == expected
    assert canonical_shard_frame_digest(frame) == expected


def test_canonical_shard_frame_digest_matches_random_unsigned_legacy_values() -> None:
    from rquant.lab_worker import canonical_shard_frame_digest

    rng = random.Random(20260729)
    values = [0, 2**63, 2**64 - 1, *(rng.getrandbits(64) for _ in range(257))]
    frame = pd.DataFrame({"v": pd.Series(values, dtype="uint64")})

    assert canonical_shard_frame_digest(frame) == _legacy_canonical_shard_frame_digest(frame)


@pytest.mark.parametrize(
    ("frame", "expected"),
    [
        (
            pd.DataFrame({"v": np.array([0, 1.5, np.nan, np.inf, -np.inf], dtype=np.float16)}),
            "1a5db50f4ab947d79bc1846f793a5881f33d82d037d310cd9101ac2071070bee",
        ),
        (
            pd.DataFrame(
                {
                    "v": pd.Series(
                        [
                            pd.Timedelta("0 days 00:03:38.028229560"),
                            pd.Timedelta("-1 days 23:56:21.971770440"),
                            pd.Timedelta(seconds=1),
                            pd.Timedelta(microseconds=123456),
                            pd.Timedelta(nanoseconds=1),
                            pd.NaT,
                        ],
                        dtype="timedelta64[ns]",
                    )
                }
            ),
            "ac9a327bd0aa824cdc0bbb383d4f9da4485228d00abd22515316b46d51171c5d",
        ),
        (
            pd.DataFrame({"a\x00b": [1, 2], "z": [3, 4]}),
            "850e90e5b6cab1ba470b6264362d5da712f03d0b285fcdc5e76dcfa850a039f0",
        ),
        (
            pd.DataFrame(index=[1, 2, 3]),
            "66de46f5a39d9742939c233fea94d7a305616b2a7e7f3a8a985ce5baa3fb8838",
        ),
    ],
    ids=["float16", "timedelta-ns", "nul-column", "zero-columns-with-rows"],
)
def test_canonical_shard_frame_digest_matches_broad_legacy_fixed_vectors(
    frame: pd.DataFrame,
    expected: str,
) -> None:
    from rquant.lab_worker import canonical_shard_frame_digest

    assert _legacy_canonical_shard_frame_digest(frame) == expected
    assert canonical_shard_frame_digest(frame) == expected


@pytest.mark.parametrize(
    "values",
    [
        np.array([0, 1.5, np.nan, np.inf, -np.inf], dtype=np.float16),
        np.array([0, 1.5, np.nan, np.inf, -np.inf], dtype=np.float32),
        np.array([0, 1.5, np.nan, np.inf, -np.inf], dtype=np.float64),
        pd.Series([0, 1.5, None, np.inf, -np.inf], dtype="Float32"),
        pd.Series([0, 1.5, None, np.inf, -np.inf], dtype="Float64"),
        pd.Categorical(pd.Series(np.array([0, 1.5, np.nan, np.inf, -np.inf], dtype=np.float32))),
        pd.Categorical(pd.Series(np.array([0, 1.5, np.nan, np.inf, -np.inf], dtype=np.float64))),
    ],
    ids=["float16", "float32", "float64", "Float32", "Float64", "cat32", "cat64"],
)
def test_canonical_shard_frame_digest_matches_legacy_float_matrix(
    values: object,
) -> None:
    from rquant.lab_worker import canonical_shard_frame_digest

    frame = pd.DataFrame({"v": values})
    assert canonical_shard_frame_digest(frame) == _legacy_canonical_shard_frame_digest(frame)


@pytest.mark.parametrize(
    ("dtype", "seed"),
    [
        (np.dtype("float16"), 2026073001),
        (np.dtype("float32"), 2026073002),
        (np.dtype("float64"), 2026073003),
    ],
    ids=["float16", "float32", "float64"],
)
def test_canonical_shard_frame_digest_matches_legacy_float_tokens(
    dtype: np.dtype[np.floating],
    seed: int,
) -> None:
    from rquant.lab_worker import canonical_shard_frame_digest

    bit_dtype = {
        2: np.dtype("uint16"),
        4: np.dtype("uint32"),
        8: np.dtype("uint64"),
    }[dtype.itemsize]
    rng = np.random.default_rng(seed)
    random_bits = rng.integers(
        0,
        np.iinfo(bit_dtype).max,
        size=513,
        dtype=bit_dtype,
        endpoint=True,
    )
    random_values = random_bits.view(dtype)
    if dtype == np.dtype("float64"):
        random_values = random_values[
            ~np.isfinite(random_values) | (np.abs(random_values) <= 1e300)
        ]
    info = np.finfo(dtype)
    max_value = info.max if dtype != np.dtype("float64") else dtype.type(info.max / 2)
    boundaries = np.array(
        [
            dtype.type(0.0),
            dtype.type(-0.0),
            np.nextafter(dtype.type(0), dtype.type(1), dtype=dtype),
            np.nextafter(dtype.type(0), dtype.type(-1), dtype=dtype),
            info.tiny,
            -info.tiny,
            max_value,
            -max_value,
            dtype.type(np.nan),
            dtype.type(np.inf),
            dtype.type(-np.inf),
        ],
        dtype=dtype,
    )
    frame = pd.DataFrame({"v": np.concatenate((boundaries, random_values))})

    assert canonical_shard_frame_digest(frame) == _legacy_canonical_shard_frame_digest(frame)


def test_canonical_shard_frame_digest_matches_legacy_float_review_vectors() -> None:
    from rquant.lab_worker import canonical_shard_frame_digest

    frame = pd.DataFrame(
        {
            "float16_tiny": np.array(
                [np.finfo(np.float16).tiny, -np.finfo(np.float16).tiny],
                dtype=np.float16,
            ),
            "float32_rounding": np.array(
                [-394.478118896484375, 394.478118896484375],
                dtype=np.float32,
            ),
        }
    )

    assert canonical_shard_frame_digest(frame) == _legacy_canonical_shard_frame_digest(frame)


@pytest.mark.parametrize(
    "values",
    [
        pd.Series(
            [np.float32(-394.478118896484375), None, np.inf, -np.inf],
            dtype="Float32",
        ),
        pd.Series(
            [np.float64(np.finfo(np.float64).tiny), None, np.inf, -np.inf],
            dtype="Float64",
        ),
        pd.Categorical(np.array([-394.478118896484375, np.inf, np.nan], dtype=np.float32)),
        pd.Categorical(np.array([np.finfo(np.float64).tiny, np.inf, np.nan], dtype=np.float64)),
    ],
    ids=["Float32", "Float64", "category32", "category64"],
)
def test_canonical_shard_frame_digest_matches_legacy_nullable_and_categorical_float_tokens(
    values: object,
) -> None:
    from rquant.lab_worker import canonical_shard_frame_digest

    frame = pd.DataFrame({"v": values})
    assert canonical_shard_frame_digest(frame) == _legacy_canonical_shard_frame_digest(frame)


def test_canonical_shard_frame_digest_matches_legacy_float64_max_failure() -> None:
    from rquant.lab_worker import canonical_shard_frame_digest

    frame = pd.DataFrame({"v": np.array([np.finfo(np.float64).max], dtype=np.float64)})

    with pytest.raises(ValueError, match="Out of range float"):
        _legacy_canonical_shard_frame_digest(frame)
    with pytest.raises(ValueError, match="Out of range float"):
        canonical_shard_frame_digest(frame)


@pytest.mark.parametrize(
    ("values", "expected"),
    [
        (
            pd.Series(
                pd.to_timedelta([0, 1, 10, 1000, -1, -10, -1000, None]),
                dtype="timedelta64[ns]",
            ),
            [
                "P0DT0H0M0S",
                "P0DT0H0M0.000000001S",
                "P0DT0H0M0.00000001S",
                "P0DT0H0M0.000001S",
                "P-1DT23H59M59.999999999S",
                "P-1DT23H59M59.99999999S",
                "P-1DT23H59M59.999999S",
                "NaT",
            ],
        ),
        (
            pd.Categorical(pd.to_timedelta([0, 1, 10, 1000, -1, -10, -1000, None])),
            [
                "P0DT0H0M0S",
                "P0DT0H0M0.000000001S",
                "P0DT0H0M0.000000010S",
                "P0DT0H0M0.000001S",
                "P-1DT23H59M59.999999999S",
                "P-1DT23H59M59.999999990S",
                "P-1DT23H59M59.999999S",
                None,
            ],
        ),
    ],
    ids=["duration", "categorical-duration"],
)
def test_canonical_shard_frame_digest_matches_legacy_timedelta_column_context(
    values: object,
    expected: list[str | None],
) -> None:
    from rquant.lab_worker import canonical_shard_frame_digest

    frame = pd.DataFrame({"v": values})
    raw = frame.to_json(
        orient="table",
        date_format="iso",
        date_unit="us",
        double_precision=15,
        force_ascii=True,
        index=False,
    )

    assert [item["v"] for item in json.loads(raw)["data"]] == expected
    assert canonical_shard_frame_digest(frame) == _legacy_canonical_shard_frame_digest(frame)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (b"x" * 4095 + b"\xe2\x82\xac", "x" * 4095 + "\u20ac"),
        (b"x" * 4095 + b"\xe2\x82\x00", "x" * 4095 + "\u2080"),
        (b"a\x00b", "a\x00b"),
        ("\U0001f600".encode("utf-8"), "\U0001f600"),
        (b"\xed\xa0\x80", "\ud800"),
        (b"\xf4\x90\x80\x80", "\udc00\udc00"),
    ],
    ids=[
        "valid-across-chunk",
        "truncated-across-chunk",
        "embedded-nul",
        "valid-astral",
        "surrogate",
        "above-unicode-range",
    ],
)
def test_canonical_shard_frame_digest_matches_legacy_bytes_tokens(
    value: bytes,
    expected: str,
) -> None:
    from rquant.lab_worker import canonical_shard_frame_digest

    frame = pd.DataFrame({"v": pd.Series([value], dtype=object)})
    raw = frame.to_json(
        orient="table",
        date_format="iso",
        date_unit="us",
        double_precision=15,
        force_ascii=True,
        index=False,
    )

    assert json.loads(raw)["data"][0]["v"] == expected
    assert canonical_shard_frame_digest(frame) == _legacy_canonical_shard_frame_digest(frame)


@pytest.mark.parametrize(
    ("value", "expected_digest"),
    [
        (b"\xc3", "c8708819a08439e8499de616b18df66df1c67d13eee1ee8b8728e8b89fc3c742"),
        (b"\xe2\x82", "37ceed389f24211ba9d9077a0e1fb7b02b69b8f5dc8400ea35ad8a0fc04e38da"),
    ],
    ids=["truncated-two-byte", "truncated-three-byte"],
)
def test_canonical_shard_frame_digest_stabilizes_terminal_truncated_bytes(
    value: bytes,
    expected_digest: str,
) -> None:
    from rquant.lab_worker import canonical_shard_frame_digest

    frame = pd.DataFrame({"v": pd.Series([value], dtype=object)})

    assert canonical_shard_frame_digest(frame) == expected_digest


@pytest.mark.parametrize("input_chunk_bytes", (1, 2, 3, 4, 4095, 4096, 4097, 64 * 1024))
@pytest.mark.parametrize("boundary_offset", (-1, 0), ids=("lead-before", "lead-at"))
@pytest.mark.parametrize(
    "suffix",
    (
        b"\xd0",
        b"\xe2\x82",
        b"\xf0\x90\x80",
        b"\xd0\x80",
        b"\xe2\x82\xac",
        b"\xf0\x9f\x98\x80",
    ),
    ids=(
        "truncated-two-byte",
        "truncated-three-byte",
        "truncated-four-byte",
        "valid-two-byte",
        "valid-three-byte",
        "valid-four-byte",
    ),
)
def test_legacy_pandas_bytes_stream_is_input_chunk_invariant(
    input_chunk_bytes: int,
    boundary_offset: int,
    suffix: bytes,
) -> None:
    from rquant.canonical_json_stream import CanonicalJsonStreamWriter

    prefix = b"x" * max(0, input_chunk_bytes + boundary_offset)
    value = prefix + suffix
    truncated = suffix in (b"\xd0", b"\xe2\x82", b"\xf0\x90\x80")
    legacy_value = value + (b"\x00" if truncated else b"")
    legacy_frame = pd.DataFrame({"v": pd.Series([legacy_value], dtype=object)})
    legacy_raw = legacy_frame.to_json(
        orient="table",
        date_format="iso",
        date_unit="us",
        double_precision=15,
        force_ascii=True,
        index=False,
    )
    legacy_token = json.dumps(
        json.loads(legacy_raw)["data"][0]["v"],
        ensure_ascii=True,
        separators=(",", ":"),
    ).encode("ascii")
    expected_digest = hashlib.sha256()
    expected_digest.update(legacy_token)
    actual_digest = hashlib.sha256()
    CanonicalJsonStreamWriter(actual_digest.update).write_legacy_pandas_bytes(
        value,
        input_chunk_bytes=input_chunk_bytes,
    )

    assert actual_digest.hexdigest() == expected_digest.hexdigest()


def test_canonical_shard_frame_digest_stabilizes_truncated_byte_at_4096_boundary() -> None:
    from rquant.lab_worker import canonical_shard_frame_digest

    frame = pd.DataFrame({"v": pd.Series([b"x" * 4096 + bytes.fromhex("d0")], dtype=object)})

    assert (
        canonical_shard_frame_digest(frame)
        == "c186ddb315d79a62279ada7f238325366c4717ca24b32789108fa0956015b380"
    )


@pytest.mark.parametrize(
    ("value", "error_type"),
    [
        (b"\x80", UnicodeDecodeError),
        (b"\xbf", UnicodeDecodeError),
        (b"\xc0\xaf", OverflowError),
        (b"\xe0\x80\xaf", OverflowError),
        (b"\xf0\x80\x80\xaf", OverflowError),
        (b"\xe0", OverflowError),
        (b"\xf0\x90", OverflowError),
        (b"\xf8\x88\x80\x80\x80", OverflowError),
        (b"\xfe", UnicodeDecodeError),
    ],
    ids=[
        "isolated-continuation-low",
        "isolated-continuation-high",
        "overlong-two-byte",
        "overlong-three-byte",
        "overlong-four-byte",
        "unterminated-three-byte",
        "unterminated-four-byte",
        "unsupported-five-byte",
        "invalid-lead",
    ],
)
def test_canonical_shard_frame_digest_matches_legacy_bytes_rejections(
    value: bytes,
    error_type: type[Exception],
) -> None:
    from rquant.lab_worker import canonical_shard_frame_digest

    frame = pd.DataFrame({"v": pd.Series([value], dtype=object)})

    with pytest.raises(error_type):
        _legacy_canonical_shard_frame_digest(frame)
    with pytest.raises(error_type):
        canonical_shard_frame_digest(frame)


def test_canonical_shard_frame_digest_matches_random_legacy_bytes() -> None:
    from rquant.lab_worker import canonical_shard_frame_digest

    rng = random.Random(20260731)
    for _ in range(1024):
        value = bytes(rng.randrange(256) for _ in range(rng.randrange(0, 33))) + b"\x00\x00\x00"
        frame = pd.DataFrame({"v": pd.Series([value], dtype=object)})
        try:
            expected = _legacy_canonical_shard_frame_digest(frame)
        except (OverflowError, UnicodeDecodeError) as exc:
            with pytest.raises(type(exc)):
                canonical_shard_frame_digest(frame)
        else:
            assert canonical_shard_frame_digest(frame) == expected


def test_canonical_shard_frame_digest_rejects_nul_truncated_column_collision() -> None:
    from rquant.lab_worker import canonical_shard_frame_digest

    frame = pd.DataFrame([[1, 2]], columns=["a", "a\x00b"])

    with pytest.raises(ValueError, match="NUL.*collide"):
        canonical_shard_frame_digest(frame)


def test_canonical_shard_frame_digest_matches_legacy_mixed_frame() -> None:
    from rquant.lab_worker import canonical_shard_frame_digest

    rng = random.Random(20260728)
    size = 257
    integer_categories = [1, 2, None, 3]
    boolean_categories = [True, False, None]
    float_categories = [1.25, 2.5, None]
    timestamp_categories = [pd.Timestamp("2026-01-01"), pd.Timestamp("2026-01-02"), None]
    arrow_values = ["alpha", None, "\u4e2d|`<b>", "omega"]
    byte_values = [b"ascii", "\u4e2d".encode(), b'quote"\\slash']
    arrow_chunked = pa.chunked_array(
        [
            pa.array(
                [arrow_values[rng.randrange(len(arrow_values))]],
                type=pa.string(),
            )
            for _ in range(size)
        ]
    )
    frame = pd.DataFrame(
        {
            "arrow": pd.Series(pd.arrays.ArrowStringArray(arrow_chunked)),
            "blob": pd.Series(
                [byte_values[rng.randrange(len(byte_values))] for _ in range(size)],
                dtype=object,
            ),
            "bool_category": pd.Categorical(
                [boolean_categories[rng.randrange(len(boolean_categories))] for _ in range(size)]
            ),
            "float": [rng.random() if index % 11 else float("nan") for index in range(size)],
            "float32": pd.Series(
                [
                    float("nan")
                    if index % 17 == 0
                    else float("inf")
                    if index % 19 == 0
                    else rng.random()
                    for index in range(size)
                ],
                dtype="float32",
            ),
            "float_category": pd.Categorical(
                [float_categories[rng.randrange(len(float_categories))] for _ in range(size)]
            ),
            "int_category": pd.Categorical(
                [integer_categories[rng.randrange(len(integer_categories))] for _ in range(size)]
            ),
            "nullable_int": pd.Series(
                [index if index % 13 else None for index in range(size)],
                dtype="Int64",
            ),
            "time_category": pd.Categorical(
                [
                    timestamp_categories[rng.randrange(len(timestamp_categories))]
                    for _ in range(size)
                ]
            ),
            "timedelta": pd.to_timedelta(
                [
                    f"{index % 7} days {index % 24}:00:00" if index % 9 else None
                    for index in range(size)
                ]
            ),
        }
    )

    assert canonical_shard_frame_digest(frame) == _legacy_canonical_shard_frame_digest(frame)


def test_canonical_shard_frame_digest_bounds_large_bytes_scratch() -> None:
    from rquant.lab_worker import canonical_shard_frame_digest

    size = 64 * 1024 * 1024
    value = (b"ascii-bytes" * ((size + 10) // 11))[:size]
    frame = pd.DataFrame({"blob": pd.Series([value], dtype=object)})

    tracemalloc.start()
    digest = canonical_shard_frame_digest(frame)
    _current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    assert len(digest) == 64
    assert peak <= 2 * 1024 * 1024


@pytest.mark.parametrize(
    ("arrow_type", "value"),
    [
        (pa.string(), "x" * (64 * 1024 * 1024)),
        (pa.binary(), b"x" * (64 * 1024 * 1024)),
    ],
    ids=["string", "binary"],
)
def test_canonical_shard_frame_digest_streams_large_arrow_dtype_buffers(
    arrow_type: pa.DataType,
    value: str | bytes,
) -> None:
    from rquant.canonical_json_stream import CANONICAL_JSON_STREAM_SCRATCH_BYTES
    from rquant.lab_worker import canonical_shard_frame_digest

    array = pd.arrays.ArrowExtensionArray(pa.array([value], type=arrow_type))
    frame = pd.DataFrame({"value": pd.Series(array)})

    tracemalloc.start()
    digest = canonical_shard_frame_digest(frame)
    _current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    assert len(digest) == 64
    assert CANONICAL_JSON_STREAM_SCRATCH_BYTES == 128 * 1024
    assert peak <= 2 * 1024 * 1024


def test_legacy_pandas_bytes_stream_bounds_truncated_64_mib_scratch() -> None:
    from rquant.canonical_json_stream import CanonicalJsonStreamWriter

    size = 64 * 1024 * 1024
    value = (b"x" * (size - 1)) + b"\xd0"
    digest = hashlib.sha256()

    tracemalloc.start()
    CanonicalJsonStreamWriter(digest.update).write_legacy_pandas_bytes(
        value,
        input_chunk_bytes=64 * 1024,
    )
    _current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    assert len(digest.hexdigest()) == 64
    assert peak <= 2 * 1024 * 1024


def test_canonical_shard_frame_digest_has_bounded_python_memory() -> None:
    from rquant.lab_worker import canonical_shard_frame_digest

    frame = pd.DataFrame(
        {
            "code": [f"{index:06d}.SZ" for index in range(100_000)],
            "value": range(100_000),
        }
    )
    frame_bytes = int(frame.memory_usage(index=True, deep=True).sum())

    tracemalloc.start()
    canonical_shard_frame_digest(frame)
    _current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    assert peak <= max(16 * 1024 * 1024, frame_bytes * 6)


def test_canonical_shard_frame_digest_bounds_wide_string_scratch() -> None:
    from rquant.canonical_json_stream import CANONICAL_JSON_STREAM_SCRATCH_BYTES
    from rquant.lab_worker import canonical_shard_frame_digest

    value = "x" * (64 * 1024)
    frame = pd.DataFrame({"wide": [value] * 1024})

    tracemalloc.start()
    digest = canonical_shard_frame_digest(frame)
    _current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    assert len(digest) == 64
    assert CANONICAL_JSON_STREAM_SCRATCH_BYTES <= 256 * 1024
    assert peak <= 8 * 1024 * 1024


@pytest.mark.parametrize(
    "categorical",
    [
        pd.Categorical(pd.Series([1, 2, 1], dtype="Int64")),
        pd.Categorical(pd.Series([True, False, True], dtype="boolean")),
        pd.Categorical(pd.Series([1.25, 2.5, 1.25], dtype="Float64")),
        pd.Categorical(pd.to_datetime(["2026-01-01", "2026-01-02"])),
    ],
    ids=["integer", "boolean", "float", "timestamp"],
)
def test_canonical_shard_frame_digest_preserves_categorical_scalar_semantics(
    categorical: pd.Categorical,
) -> None:
    from rquant.lab_worker import canonical_shard_frame_digest

    frame = pd.DataFrame({"value": categorical})
    raw = frame.to_json(
        orient="table",
        date_format="iso",
        date_unit="us",
        double_precision=15,
        force_ascii=True,
        index=False,
    )
    expected = hashlib.sha256(
        json.dumps(
            json.loads(raw),
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()

    assert canonical_shard_frame_digest(frame) == expected


@contextmanager
def _raising_loguru_sink() -> Iterator[None]:
    from loguru import logger

    def fail_sink(_message: object) -> None:
        raise RuntimeError("injected catch-false log sink failure")

    sink = logger.add(fail_sink, level="WARNING", catch=False)
    try:
        yield
    finally:
        logger.remove(sink)


def _accept_report(
    report: LabWorkerReport,
    _timeout_seconds: float,
    _stop: object,
) -> LabReportReceipt:
    return LabReportReceipt.from_report(
        report,
        status="accepted",
        reason=f"accepted:{report.body.report_type}",
        accepted_at=NOW,
    )


@contextmanager
def _store_factory(store: object = object()) -> Iterator[object]:
    yield store


class RecordingRegistry:
    def __init__(
        self,
        *,
        delay_seconds: float = 0.0,
        failure: BaseException | None = None,
        legacy_profile: str | None = None,
    ) -> None:
        self.delegate = default_strategy_job_adapter_registry()
        self.delay_seconds = delay_seconds
        self.failure = failure
        self.legacy_profile = legacy_profile
        self._executions = multiprocessing.get_context("spawn").Value("i", 0)
        self.stores: list[object] = []

    @property
    def executions(self) -> int:
        counter_path = getattr(self, "_closed_execution_counter_path", None)
        if isinstance(counter_path, Path) and counter_path.exists():
            return int(counter_path.read_text(encoding="ascii"))
        return self._executions.value

    def validate_claim(self, claim: LabShardClaim) -> ValidatedStrategyShard:
        return self.delegate.validate_claim(claim)

    def for_spec(self, spec: ResearchRunSpec):
        return self.delegate.for_spec(spec)

    def execute_shard(
        self,
        validated: ValidatedStrategyShard,
        store: object,
    ) -> LabShardExecutionResult:
        with self._executions.get_lock():
            self._executions.value += 1
        counter_path = getattr(self, "_closed_execution_counter_path", None)
        if isinstance(counter_path, Path):
            _fixture_counter_increment(counter_path)
        self.stores.append(store)
        if self.delay_seconds:
            time.sleep(self.delay_seconds)
        if self.failure is not None:
            raise self.failure
        return LabShardExecutionResult.from_validated(
            validated,
            tables=(
                LabShardTable(
                    name="trades",
                    frame=pd.DataFrame([{"hold_days": validated.shard.hold_days, "ret_pct": 1.25}]),
                ),
            ),
        )


class ParentReduceTrap:
    def __init__(self, *, reduce_marker: Path, execution_marker: Path) -> None:
        self.reduce_marker = reduce_marker
        self.execution_marker = execution_marker

    def __reduce__(self) -> object:
        self.reduce_marker.write_text("parent reduce invoked", encoding="ascii")
        raise AssertionError("parent attempted to pickle an unregistered object")

    def __call__(self, *_args: object, **_kwargs: object) -> object:
        self.execution_marker.write_text("parent callback invoked", encoding="ascii")
        raise AssertionError("unregistered parent callback was invoked")

    def execute_shard(self, *_args: object, **_kwargs: object) -> object:
        self.execution_marker.write_text("parent adapter invoked", encoding="ascii")
        raise AssertionError("unregistered parent adapter was invoked")


class MaliciousResultRegistry(RecordingRegistry):
    def __init__(self, *, reduce_marker: Path) -> None:
        super().__init__()
        self.reduce_marker = reduce_marker


class PlanBypassRecordingRegistry(RecordingRegistry):
    def validate_claim(self, claim: LabShardClaim) -> ValidatedStrategyShard:
        payload = StrategyShardPayload.model_validate_json(claim.definition.payload_json)
        return ValidatedStrategyShard(
            claim=claim,
            spec=payload.spec,
            shard=payload.shard,
        )


class HungLiveRegistry(RecordingRegistry):
    def __init__(self, *, pid_path: Path) -> None:
        super().__init__()
        self.pid_path = pid_path
        self._validated: ValidatedStrategyShard | None = None
        self._blocked = multiprocessing.get_context("spawn").Event()

    def validate_claim(self, claim: LabShardClaim) -> ValidatedStrategyShard:
        payload = StrategyShardPayload.model_validate_json(claim.definition.payload_json)
        self._validated = ValidatedStrategyShard(
            claim=claim,
            spec=payload.spec,
            shard=payload.shard,
        )
        return self._validated

    def execute_shard(
        self,
        validated: ValidatedStrategyShard,
        store: object,
    ) -> LabShardExecutionResult:
        del validated, store
        self.pid_path.write_text(str(os.getpid()), encoding="ascii")
        self._blocked.wait()
        raise AssertionError("hung registry was unexpectedly released")


class SlowPidRegistry(HungLiveRegistry):
    def __init__(self, *, pid_path: Path, delay_seconds: float) -> None:
        super().__init__(pid_path=pid_path)
        self.delay_seconds = delay_seconds

    def execute_shard(
        self,
        validated: ValidatedStrategyShard,
        store: object,
    ) -> LabShardExecutionResult:
        self.pid_path.write_text(str(os.getpid()), encoding="ascii")
        time.sleep(self.delay_seconds)
        return LabShardExecutionResult.from_validated(
            validated,
            tables=(
                LabShardTable(
                    name="trades",
                    frame=pd.DataFrame([{"hold_days": 1, "ret_pct": 1.25}]),
                ),
            ),
        )


class BlockingRegistry(RecordingRegistry):
    def __init__(self, *, executing: object, release: object) -> None:
        super().__init__()
        self.executing = executing
        self.release = release

    def execute_shard(
        self,
        validated: ValidatedStrategyShard,
        store: object,
    ) -> LabShardExecutionResult:
        self.executing.set()
        if not self.release.wait(2):
            raise TimeoutError("blocking registry release timed out")
        return super().execute_shard(validated, store)


class DeadlineRegistry(RecordingRegistry):
    def __init__(self, *, deadline_reached: object) -> None:
        super().__init__()
        self.deadline_reached = deadline_reached

    def execute_shard(
        self,
        validated: ValidatedStrategyShard,
        store: object,
    ) -> LabShardExecutionResult:
        result = super().execute_shard(validated, store)
        self.deadline_reached.value = True
        return result


class SigtermIgnoringProcessTreeRegistry(HungLiveRegistry):
    def __init__(
        self,
        *,
        pid_path: Path,
        grandchild_pid_path: Path,
    ) -> None:
        super().__init__(pid_path=pid_path)
        self.grandchild_pid_path = grandchild_pid_path

    def execute_shard(
        self,
        validated: ValidatedStrategyShard,
        store: object,
    ) -> LabShardExecutionResult:
        del validated, store
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        grandchild_pid = os.fork()
        if grandchild_pid == 0:
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
            self.grandchild_pid_path.write_text(str(os.getpid()), encoding="ascii")
            threading.Event().wait()
        self.pid_path.write_text(str(os.getpid()), encoding="ascii")
        threading.Event().wait()
        raise AssertionError("process tree registry was unexpectedly released")


def _standalone_sigterm_ignoring_process_tree(
    pid_path: Path,
    grandchild_pid_path: Path,
) -> None:
    os.setsid()
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    grandchild_pid = os.fork()
    if grandchild_pid == 0:
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        grandchild_pid_path.write_text(str(os.getpid()), encoding="ascii")
        threading.Event().wait()
    pid_path.write_text(str(os.getpid()), encoding="ascii")
    threading.Event().wait()


class TermExitingLeaderProcessTreeRegistry(HungLiveRegistry):
    def __init__(
        self,
        *,
        pid_path: Path,
        grandchild_pid_path: Path,
    ) -> None:
        super().__init__(pid_path=pid_path)
        self.grandchild_pid_path = grandchild_pid_path

    def execute_shard(
        self,
        validated: ValidatedStrategyShard,
        store: object,
    ) -> LabShardExecutionResult:
        del validated, store

        def exit_on_term(_signum: int, _frame: object) -> None:
            os._exit(0)

        signal.signal(signal.SIGTERM, exit_on_term)
        grandchild_pid = os.fork()
        if grandchild_pid == 0:
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
            self.grandchild_pid_path.write_text(str(os.getpid()), encoding="ascii")
            threading.Event().wait()
        self.pid_path.write_text(str(os.getpid()), encoding="ascii")
        threading.Event().wait()
        raise AssertionError("process tree registry was unexpectedly released")


class SuccessfulProcessTreeRegistry(HungLiveRegistry):
    def __init__(
        self,
        *,
        pid_path: Path,
        grandchild_pid_path: Path,
    ) -> None:
        super().__init__(pid_path=pid_path)
        self.grandchild_pid_path = grandchild_pid_path

    def execute_shard(
        self,
        validated: ValidatedStrategyShard,
        store: object,
    ) -> LabShardExecutionResult:
        del store
        grandchild_pid = os.fork()
        if grandchild_pid == 0:
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
            self.grandchild_pid_path.write_text(str(os.getpid()), encoding="ascii")
            threading.Event().wait()
        self.pid_path.write_text(str(os.getpid()), encoding="ascii")
        return LabShardExecutionResult.from_validated(
            validated,
            tables=(
                LabShardTable(
                    name="trades",
                    frame=pd.DataFrame([{"hold_days": 1, "ret_pct": 1.25}]),
                ),
            ),
        )


class SpawnMethodRegistry(SlowPidRegistry):
    def execute_shard(
        self,
        validated: ValidatedStrategyShard,
        store: object,
    ) -> LabShardExecutionResult:
        self.pid_path.write_text(multiprocessing.get_start_method(), encoding="ascii")
        return LabShardExecutionResult.from_validated(
            validated,
            tables=(
                LabShardTable(
                    name="trades",
                    frame=pd.DataFrame([{"hold_days": 1, "ret_pct": 1.25}]),
                ),
            ),
        )


class UnserializableRegistry(RecordingRegistry):
    def __init__(self) -> None:
        super().__init__()
        self.unserializable = lambda: None


class FailingWorkerRegistry(RecordingRegistry):
    def execute_shard(
        self,
        validated: ValidatedStrategyShard,
        store: object,
    ) -> LabShardExecutionResult:
        del validated, store
        raise RuntimeError("worker exploded")


class PermanentlyBlockingAfterFirstSnapshotProvider:
    def __init__(
        self,
        *,
        marker_path: Path,
        snapshot: object,
        block_after_calls: int = 1,
    ) -> None:
        self.marker_path = marker_path
        self.snapshot = snapshot
        self.block_after_calls = block_after_calls
        self._blocked = multiprocessing.get_context("spawn").Event()

    def __call__(self) -> object:
        if not self.marker_path.exists():
            self.marker_path.write_text("first", encoding="ascii")
            return self.snapshot
        self._blocked.wait()
        raise AssertionError("blocked resource probe was unexpectedly released")


def _ignore_term_forever(pid_path: Path, ready: object) -> None:
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    pid_path.write_text(str(os.getpid()), encoding="ascii")
    ready.set()
    threading.Event().wait()


class SpawnDescendantBlockingResourceSnapshotProvider:
    def __init__(self, *, probe_pid_path: Path, descendant_pid_path: Path) -> None:
        self.probe_pid_path = probe_pid_path
        self.descendant_pid_path = descendant_pid_path

    def __call__(self) -> object:
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        context = multiprocessing.get_context("spawn")
        ready = context.Event()
        descendant = context.Process(
            target=_ignore_term_forever,
            args=(self.descendant_pid_path, ready),
            daemon=False,
        )
        descendant.start()
        self.probe_pid_path.write_text(str(os.getpid()), encoding="ascii")
        if not ready.wait(2):
            raise TimeoutError("resource probe descendant did not start")
        threading.Event().wait()
        raise AssertionError("blocked resource probe was unexpectedly released")


class StaticResourceSnapshotProvider:
    def __init__(self, snapshot: object) -> None:
        self.snapshot = snapshot

    def __call__(self) -> object:
        return self.snapshot


class ExportingResourceSnapshotProvider:
    def __init__(self, snapshot: object, state: object) -> None:
        self.snapshot = snapshot
        self.state = state

    def __call__(self) -> object:
        return self.snapshot

    def export_probe_state(self) -> object:
        return self.state


class RejectingProbeStateProvider:
    def __init__(self, snapshot: object, state: object) -> None:
        self.snapshot = snapshot
        self.state = state

    def spawn_probe_provider(self) -> ExportingResourceSnapshotProvider:
        return ExportingResourceSnapshotProvider(self.snapshot, self.state)

    def accept_probe_state(self, _state: object) -> bool:
        return False

    def __call__(self) -> object:
        return self.snapshot


class AdversarialSnapshotHooksProvider:
    def __init__(
        self,
        *,
        hook: Literal["spawn_probe_provider", "accept_probe_state"],
        behavior: Literal["block", "exception", "recursive"],
        snapshot: object,
        entered_path: Path,
        hook_pid_path: Path,
        release_path: Path,
        descendant_pid_path: Path | None = None,
    ) -> None:
        self.hook = hook
        self.behavior = behavior
        self.snapshot = snapshot
        self.entered_path = entered_path
        self.hook_pid_path = hook_pid_path
        self.release_path = release_path
        self.descendant_pid_path = descendant_pid_path

    def _exercise_hook(self) -> None:
        self.hook_pid_path.write_text(str(os.getpid()), encoding="ascii")
        self.entered_path.write_text("entered", encoding="ascii")
        if self.behavior == "exception":
            raise RuntimeError(f"{self.hook} exploded")
        descendant = None
        if self.behavior == "recursive":
            if self.descendant_pid_path is None:
                raise AssertionError("recursive hook requires a descendant PID path")
            context = multiprocessing.get_context("spawn")
            ready = context.Event()
            descendant = context.Process(
                target=_ignore_term_forever,
                args=(self.descendant_pid_path, ready),
                daemon=False,
            )
            descendant.start()
            if not ready.wait(2):
                raise TimeoutError("snapshot hook descendant did not start")
        deadline = time.monotonic() + _observe(10)
        while not self.release_path.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        if not self.release_path.exists():
            raise TimeoutError("test snapshot hook was not cancelled")
        if descendant is not None:
            descendant.kill()
            descendant.join(1)
            descendant.close()

    def spawn_probe_provider(self) -> ExportingResourceSnapshotProvider:
        if self.hook == "spawn_probe_provider":
            self._exercise_hook()
        return ExportingResourceSnapshotProvider(self.snapshot, {"sequence": 1})

    def accept_probe_state(self, _state: object) -> bool:
        if self.hook == "accept_probe_state":
            self._exercise_hook()
        return True

    def __call__(self) -> object:
        return self.snapshot


def test_rejected_resource_authority_state_cannot_admit_worker_execution(
    tmp_path: Path,
) -> None:
    from rquant.lab_daemon import LabDaemonConfigurationError

    claims = LabClaimSpool(tmp_path / "claims")
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims.publish(claim)
    registry = RecordingRegistry()
    worker = _worker(
        tmp_path,
        claims=claims,
        registry=registry,
        resource_snapshot_provider=RejectingProbeStateProvider(
            _healthy_resource_snapshot(),
            {"sequence": 1},
        ),
        admission_policy_provider=StaticAdmissionPolicyProvider(_permissive_admission_policy()),
        require_resource_admission=True,
    )

    with pytest.raises(LabDaemonConfigurationError, match="authority state"):
        worker.run_once()

    assert registry.executions == 0
    assert tuple(entry.claim for entry in claims.pending()) == (claim,)


@pytest.mark.parametrize("hook", ("spawn_probe_provider", "accept_probe_state"))
@pytest.mark.parametrize("termination", ("stop", "deadline"))
def test_snapshot_hook_blocking_is_bounded_before_adapter_execution(
    tmp_path: Path,
    hook: Literal["spawn_probe_provider", "accept_probe_state"],
    termination: str,
) -> None:
    from rquant.runtime_resource_admission import SQLiteResourceReservationStore

    spec = _nshape_compare_spec(hold_days=(1,))
    # A deadline can only bound the blocked hook once the hook is running, and
    # the hook runs in a spawned probe child. The literal eight hundred
    # milliseconds this used to carry is shorter than one child start-up on a
    # slow host, so the child was killed mid-import and never wrote the entry
    # marker the case waits for.
    #
    # Reaching that marker is not one child start but three - the admission
    # authority round trip, the shard child, and the recheck that finally
    # blocks - which is why it is the same budget `_await_markers` is given for
    # the same event below. One start-up was enough on this laptop and still
    # cut the run short on the runner.
    spec_deadline = _deadline_reachable_in(3)
    if termination == "deadline":
        spec = spec.model_copy(update={"deadline": NOW + spec_deadline})
    claim = _claim(spec)
    claims = LabClaimSpool(tmp_path / "claims")
    registry = RecordingRegistry()
    entered_path = tmp_path / f"{hook}-{termination}.entered"
    hook_pid_path = tmp_path / f"{hook}-{termination}.pid"
    release_path = tmp_path / f"{hook}-{termination}.release"
    store = SQLiteResourceReservationStore(
        tmp_path / f"{hook}-{termination}.sqlite3",
        clock=lambda: NOW,
    )
    claims.publish(claim)
    worker = _worker(
        tmp_path,
        claims=claims,
        registry=registry,
        resource_probe_timeout_seconds=3,
        resource_snapshot_provider=AdversarialSnapshotHooksProvider(
            hook=hook,
            behavior="block",
            snapshot=_healthy_resource_snapshot(),
            entered_path=entered_path,
            hook_pid_path=hook_pid_path,
            release_path=release_path,
        ),
        admission_policy_provider=StaticAdmissionPolicyProvider(_permissive_admission_policy()),
        resource_reservation_store=store,
        require_resource_admission=True,
    )
    outcomes: list[object] = []
    failures: list[BaseException] = []

    def run_worker() -> None:
        try:
            outcomes.append(worker.run_once())
        except BaseException as exc:
            failures.append(exc)

    started = time.monotonic()
    runner = threading.Thread(target=run_worker)
    runner.start()
    try:
        entered_deadline = time.monotonic() + _observe(1.2)
        while not entered_path.exists() and time.monotonic() < entered_deadline:
            time.sleep(0.01)
        assert entered_path.exists()
        assert int(hook_pid_path.read_text(encoding="ascii")) != os.getpid()
        if termination == "stop":
            worker.request_stop()
        termination_budget = _observe(1.6) + (
            0.0 if termination == "stop" else spec_deadline.total_seconds()
        )
        runner.join(timeout=termination_budget)
        bounded = not runner.is_alive()
        elapsed = time.monotonic() - started
    finally:
        release_path.write_text("release", encoding="ascii")
        worker.request_stop()
        runner.join(timeout=_observe(2))

    assert bounded
    assert elapsed < termination_budget
    assert failures == []
    assert outcomes[0].status == "stopped"
    assert registry.executions == 0
    _assert_process_gone(int(hook_pid_path.read_text(encoding="ascii")))
    assert store.active_leases() == ()


@pytest.mark.parametrize("hook", ("spawn_probe_provider", "accept_probe_state"))
def test_snapshot_hook_exception_fails_closed_before_adapter_execution(
    tmp_path: Path,
    hook: Literal["spawn_probe_provider", "accept_probe_state"],
) -> None:
    from rquant.runtime_resource_admission import SQLiteResourceReservationStore

    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims = LabClaimSpool(tmp_path / "claims")
    registry = RecordingRegistry()
    store = SQLiteResourceReservationStore(
        tmp_path / f"{hook}-exception.sqlite3",
        clock=lambda: NOW,
    )
    claims.publish(claim)
    worker = _worker(
        tmp_path,
        claims=claims,
        registry=registry,
        resource_probe_timeout_seconds=3,
        resource_snapshot_provider=AdversarialSnapshotHooksProvider(
            hook=hook,
            behavior="exception",
            snapshot=_healthy_resource_snapshot(),
            entered_path=tmp_path / f"{hook}-exception.entered",
            hook_pid_path=tmp_path / f"{hook}-exception.pid",
            release_path=tmp_path / f"{hook}-exception.release",
        ),
        admission_policy_provider=StaticAdmissionPolicyProvider(_permissive_admission_policy()),
        resource_reservation_store=store,
        require_resource_admission=True,
    )

    with pytest.raises(LabDaemonConfigurationError):
        worker.run_once()

    assert registry.executions == 0
    hook_pid = int((tmp_path / f"{hook}-exception.pid").read_text(encoding="ascii"))
    assert hook_pid != os.getpid()
    _assert_process_gone(hook_pid)
    assert store.active_leases() == ()


@pytest.mark.parametrize("hook", ("spawn_probe_provider", "accept_probe_state"))
def test_snapshot_hook_recursive_child_ignoring_term_is_killed_and_reaped(
    tmp_path: Path,
    hook: Literal["spawn_probe_provider", "accept_probe_state"],
) -> None:
    from rquant.runtime_resource_admission import SQLiteResourceReservationStore

    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims = LabClaimSpool(tmp_path / "claims")
    registry = RecordingRegistry()
    entered_path = tmp_path / f"{hook}-recursive.entered"
    hook_pid_path = tmp_path / f"{hook}-recursive.pid"
    descendant_pid_path = tmp_path / f"{hook}-recursive-descendant.pid"
    release_path = tmp_path / f"{hook}-recursive.release"
    store = SQLiteResourceReservationStore(
        tmp_path / f"{hook}-recursive.sqlite3",
        clock=lambda: NOW,
    )
    claims.publish(claim)
    worker = _worker(
        tmp_path,
        claims=claims,
        registry=registry,
        resource_probe_timeout_seconds=3,
        resource_snapshot_provider=AdversarialSnapshotHooksProvider(
            hook=hook,
            behavior="recursive",
            snapshot=_healthy_resource_snapshot(),
            entered_path=entered_path,
            hook_pid_path=hook_pid_path,
            release_path=release_path,
            descendant_pid_path=descendant_pid_path,
        ),
        admission_policy_provider=StaticAdmissionPolicyProvider(_permissive_admission_policy()),
        resource_reservation_store=store,
        require_resource_admission=True,
    )
    outcomes: list[object] = []
    failures: list[BaseException] = []

    def run_worker() -> None:
        try:
            outcomes.append(worker.run_once())
        except BaseException as exc:
            failures.append(exc)

    runner = threading.Thread(target=run_worker)
    runner.start()
    try:
        entered_deadline = time.monotonic() + _child_startups(2)
        while (
            not entered_path.exists() or not descendant_pid_path.exists()
        ) and time.monotonic() < entered_deadline:
            time.sleep(0.01)
        assert entered_path.exists()
        assert descendant_pid_path.exists()
        started = time.monotonic()
        worker.request_stop()
        runner.join(timeout=_observe(1.2))
        bounded = not runner.is_alive()
        elapsed = time.monotonic() - started
    finally:
        release_path.write_text("release", encoding="ascii")
        worker.request_stop()
        runner.join(timeout=_observe(2))

    assert bounded
    assert elapsed < _observe(1.2)
    assert failures == []
    assert outcomes[0].status == "stopped"
    assert registry.executions == 0
    hook_pid = int(hook_pid_path.read_text(encoding="ascii"))
    descendant_pid = int(descendant_pid_path.read_text(encoding="ascii"))
    assert hook_pid != os.getpid()
    _assert_process_gone(hook_pid)
    _assert_process_gone(descendant_pid)
    assert store.active_leases() == ()


@pytest.mark.parametrize(
    "injection_point",
    (
        "resource_snapshot_provider",
        "admission_policy_provider",
        "source_quota_lease_provider",
        "spawn_probe_provider",
        "accept_probe_state",
        "export_probe_state",
    ),
)
def test_resource_authority_injection_points_have_no_parent_direct_invocation(
    injection_point: str,
) -> None:
    from rquant.lab_worker import LabWorker

    worker_node = next(
        node
        for node in ast.parse(inspect.getsource(LabWorker)).body
        if isinstance(node, ast.ClassDef) and node.name == "LabWorker"
    )
    violations: list[str] = []
    for method in worker_node.body:
        if not isinstance(method, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        for node in ast.walk(method):
            if (
                injection_point.endswith("_provider")
                and isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and isinstance(node.func.value, ast.Name)
                and node.func.value.id == "self"
                and node.func.attr == injection_point
            ):
                violations.append(f"{method.name}:{node.lineno}")
            if (
                injection_point
                in {"spawn_probe_provider", "accept_probe_state", "export_probe_state"}
                and isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == "getattr"
                and any(
                    isinstance(argument, ast.Constant) and argument.value == injection_point
                    for argument in node.args
                )
            ):
                violations.append(f"{method.name}:{node.lineno}")

    assert violations == []


def test_process_boundaries_use_bytes_transport_and_primitive_start_args() -> None:
    import rquant.lab_worker as lab_worker

    source = inspect.getsource(lab_worker)
    tree = ast.parse(source)
    object_transport_calls = [
        f"{node.func.attr}:{node.lineno}"
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr in {"send", "recv"}
        and not (
            isinstance(node.func.value, ast.Attribute)
            and node.func.value.attr == "_socket"
            and isinstance(node.func.value.value, ast.Name)
            and node.func.value.value.id == "self"
        )
    ]

    assert object_transport_calls == []
    assert "Pipe(" not in source
    lab_worker._assert_primitive_process_start(
        lab_worker._authority_wire_child,
        (b"{}", f"{SHARED_TMP_ROOT}/test.sock", b"key", 1024),
    )
    with pytest.raises(LabDaemonConfigurationError, match="target is not registered"):
        lab_worker._assert_primitive_process_start(
            lambda: None,
            (b"{}", f"{SHARED_TMP_ROOT}/test.sock", b"key", 1024),
        )
    with pytest.raises(LabDaemonConfigurationError, match="primitive wire values"):
        lab_worker._assert_primitive_process_start(
            lab_worker._shard_wire_child,
            (b"{}", f"{SHARED_TMP_ROOT}/test.sock", object(), 1024),
        )


def test_closed_registry_hash_mismatch_fails_before_process_start(tmp_path: Path) -> None:
    import rquant.lab_worker as lab_worker

    assert hasattr(lab_worker, "LabClosedRegistryBinding")
    assert hasattr(lab_worker, "LabShardRuntimeManifest")
    binding = lab_worker.LabClosedRegistryBinding(
        registry_id="rquant.lab-shard.builtin",
        registry_version=1,
        registry_hash="f" * 64,
        configuration_json="{}",
    )
    manifest = lab_worker.LabShardRuntimeManifest(registry=binding)
    worker = _worker(tmp_path, shard_runtime_manifest=manifest)

    with pytest.raises(LabDaemonConfigurationError, match="registry hash"):
        worker.run_once()


def test_parent_rejects_unregistered_adapter_registry_without_truthiness_call(
    tmp_path: Path,
) -> None:
    from rquant.lab_worker import LabWorker

    marker = tmp_path / "adapter-registry-bool.marker"

    class UnregisteredRegistry:
        def __bool__(self) -> bool:
            marker.write_text("called", encoding="ascii")
            raise AssertionError("unregistered adapter registry truthiness was invoked")

    with pytest.raises(LabDaemonConfigurationError, match="adapter registry is not registered"):
        LabWorker(
            worker_id="worker-a",
            claim_spool=LabClaimSpool(tmp_path / "claims"),
            report_spool=LabReportSpool(tmp_path / "reports"),
            artifact_root=tmp_path / "artifacts",
            adapter_registry=UnregisteredRegistry(),
        )

    assert not marker.exists()


@pytest.mark.parametrize(
    "payload",
    (
        pytest.param(b"{", id="truncated"),
        pytest.param(b'{"message_type":"readiness", "ready":true}', id="noncanonical"),
        pytest.param(b'{"message_type":"unknown"}', id="unknown-type"),
        # Without an explicit id pytest names the case after the payload, which
        # puts a megabyte of "x" in the nodeid, the shard manifest and the JUnit
        # report.
        pytest.param(b"x" * (1024 * 1024 + 1), id="oversize"),
    ),
)
def test_wire_decoder_rejects_malformed_noncanonical_and_oversize_bytes(
    payload: bytes,
) -> None:
    import rquant.lab_worker as lab_worker

    assert hasattr(lab_worker, "_decode_wire_message")
    assert hasattr(lab_worker, "_IsolationReadiness")
    with pytest.raises((LabDaemonConfigurationError, ValueError)):
        lab_worker._decode_wire_message(
            payload,
            model=lab_worker._IsolationReadiness,
            max_bytes=1024 * 1024,
            label="test wire message",
        )


@pytest.mark.parametrize(
    "payload",
    (pytest.param(b"{", id="truncated"), pytest.param(b"x" * 1025, id="oversize")),
)
def test_recv_wire_rejects_malformed_and_oversize_send_bytes(payload: bytes) -> None:
    import rquant.lab_worker as lab_worker

    receiver, sender = multiprocessing.Pipe(duplex=False)
    try:
        sender.send_bytes(payload)
        with pytest.raises(LabDaemonConfigurationError, match="transport|malformed"):
            lab_worker._recv_wire(
                receiver,
                model=lab_worker._IsolationReadiness,
                max_bytes=1024,
                label="adversarial wire",
            )
    finally:
        receiver.close()
        sender.close()

    class SelectorCloseFailure:
        def __init__(self, selector: object, message: str) -> None:
            self.selector = selector
            self.message = message

        def close(self) -> None:
            self.selector.close()
            raise OSError(self.message)

    selector_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    selector_listener = lab_worker._RawWireListener(selector_socket, b"selector-close-authkey")
    selector_listener._selector = SelectorCloseFailure(
        selector_listener._selector,
        "selector close denied",
    )
    with pytest.raises(OSError, match="selector close denied"):
        selector_listener.close()
    assert selector_socket.fileno() == -1
    selector_listener.close()

    class SocketCloseFailure:
        def __init__(self) -> None:
            self.socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)

        def setblocking(self, blocking: bool) -> None:
            self.socket.setblocking(blocking)

        def fileno(self) -> int:
            return self.socket.fileno()

        def close(self) -> None:
            self.socket.close()
            raise OSError("socket close denied")

    failing_socket = SocketCloseFailure()
    failing_listener = lab_worker._RawWireListener(
        failing_socket,
        b"double-close-authkey",
    )
    failing_listener._selector = SelectorCloseFailure(
        failing_listener._selector,
        "selector close denied",
    )
    with pytest.raises(BaseExceptionGroup) as captured:
        failing_listener.close()
    close_errors = _collect_base_exceptions(captured.value)
    assert any("selector close denied" in str(error) for error in close_errors)
    assert any("socket close denied" in str(error) for error in close_errors)
    assert failing_socket.fileno() == -1
    failing_listener.close()


def test_result_wire_outbound_gate_matches_parent_receive_limit_without_large_allocation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    import rquant.lab_worker as lab_worker
    from rquant.strategy_job_adapters import MAX_RESULT_WIRE_BYTES

    assert lab_worker._MAX_SHARD_RESULT_WIRE_BYTES == MAX_RESULT_WIRE_BYTES
    lab_worker._validate_outbound_wire_size(
        MAX_RESULT_WIRE_BYTES,
        max_bytes=MAX_RESULT_WIRE_BYTES,
        label="isolated shard outcome",
    )
    with pytest.raises(LabDaemonConfigurationError, match="outbound wire size limit"):
        lab_worker._validate_outbound_wire_size(
            MAX_RESULT_WIRE_BYTES + 1,
            max_bytes=MAX_RESULT_WIRE_BYTES,
            label="isolated shard outcome",
        )

    class RecordingConnection:
        sent = False

        def send_bytes(self, _payload: bytes) -> None:
            self.sent = True

    connection = RecordingConnection()
    with monkeypatch.context() as outbound_patch:
        outbound_patch.setattr(lab_worker, "_encode_wire_message", lambda _value: b"12345")
        with pytest.raises(LabDaemonConfigurationError, match="outbound wire size limit"):
            lab_worker._send_wire(
                connection,
                lab_worker._IsolationStartAck(
                    accepted=True,
                    not_after_monotonic_microseconds=None,
                ),
                max_bytes=4,
                label="test outbound",
            )
    assert not connection.sent
    first = lab_worker._new_wire_session(roots=(SHARED_TMP_ROOT,))
    second = lab_worker._new_wire_session(roots=(SHARED_TMP_ROOT,))
    accepted: dict[str, bytes] = {}
    responses: dict[str, bytes] = {}
    digest_modes: dict[str, str | None] = {}
    accept_deadline = time.monotonic_ns() // 1_000 + 1_000_000

    def accept_once(name: str, session: object) -> None:
        accepted_connection = session.listener.accept(
            deadline_microseconds=accept_deadline,
            cancel_requested=lambda: False,
        )
        try:
            digest_modes[name] = accepted_connection._peer_digest_mode
            payload = accepted_connection.recv_bytes()
            accepted[name] = payload
            accepted_connection.send_bytes(b"ack:" + payload)
        finally:
            accepted_connection.close()

    def connect_once(name: str, session: object) -> None:
        authenticated_connection = lab_worker.Client(
            session.address,
            family="AF_UNIX",
            authkey=session.authkey,
        )
        try:
            authenticated_connection.send_bytes(name.encode("ascii"))
            responses[name] = authenticated_connection.recv_bytes()
        finally:
            authenticated_connection.close()

    accept_threads = [
        threading.Thread(target=accept_once, args=("first", first)),
        threading.Thread(target=accept_once, args=("second", second)),
    ]
    client_threads = [
        threading.Thread(target=connect_once, args=("first", first)),
        threading.Thread(target=connect_once, args=("second", second)),
    ]
    for thread in accept_threads + client_threads:
        thread.start()
    for thread in client_threads + accept_threads:
        thread.join(timeout=_observe(1))
    try:
        assert not any(thread.is_alive() for thread in accept_threads + client_threads)
        assert accepted == {"first": b"first", "second": b"second"}
        assert responses == {"first": b"ack:first", "second": b"ack:second"}
        assert digest_modes == {
            "first": STDLIB_WIRE_DIGEST_MODE,
            "second": STDLIB_WIRE_DIGEST_MODE,
        }
        assert first.address != second.address
        assert first.path != second.path
        assert first.endpoint != second.endpoint
        assert first.session_identity != second.session_identity
        assert first.endpoint_identity != second.endpoint_identity
        assert first.path.name.startswith("rqlw-")
        assert re.fullmatch(r"rqlw-[0-9a-f]{32}", first.path.name)
        assert re.fullmatch(r"rqlw-[0-9a-f]{32}", second.path.name)
        assert first.endpoint.name == "wire.sock"
        assert second.endpoint.name == "wire.sock"
        directory = first.path.lstat()
        endpoint = first.endpoint.lstat()
        directory_fd = os.fstat(first.session_fd)
        assert stat.S_ISDIR(directory.st_mode)
        assert not first.path.is_symlink()
        assert directory.st_uid == os.geteuid()
        assert directory.st_gid == os.getegid()
        assert stat.S_IMODE(directory.st_mode) == 0o700
        assert (directory.st_dev, directory.st_ino) == (directory_fd.st_dev, directory_fd.st_ino)
        assert stat.S_ISSOCK(endpoint.st_mode)
        assert not first.endpoint.is_symlink()
        assert endpoint.st_uid == os.geteuid()
        assert endpoint.st_gid == os.getegid()
        assert stat.S_IMODE(endpoint.st_mode) == 0o600
        assert first.listener.fileno() >= 0
        assert (endpoint.st_dev, endpoint.st_ino) == (
            first.endpoint_identity.device,
            first.endpoint_identity.inode,
        )
    finally:
        first.cleanup()
        second.cleanup()

    def connect_raw(session: object) -> socket.socket:
        peer = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        peer.connect(session.address)
        return peer

    active_child_pids = {child.pid for child in multiprocessing.active_children()}
    for partial_auth in (b"", b"\x00\x00", struct.pack("!i", 40) + b"{s"):
        partial_session = lab_worker._new_wire_session(roots=(SHARED_TMP_ROOT,))
        raw_peer = connect_raw(partial_session)
        try:
            if partial_auth:
                raw_peer.sendall(partial_auth)
            started = time.monotonic()
            with pytest.raises(TimeoutError, match="timed out"):
                partial_session.listener.accept(
                    deadline_microseconds=time.monotonic_ns() // 1_000 + 50_000,
                    cancel_requested=lambda: False,
                )
            assert time.monotonic() - started < _observe(0.5)
        finally:
            raw_peer.close()
            partial_session.cleanup()

    stopped_auth = lab_worker._new_wire_session(roots=(SHARED_TMP_ROOT,))
    stopped_peer = connect_raw(stopped_auth)
    try:
        with pytest.raises(InterruptedError, match="cancel|stop"):
            stopped_auth.listener.accept(
                deadline_microseconds=time.monotonic_ns() // 1_000 + 1_000_000,
                cancel_requested=lambda: True,
            )
    finally:
        stopped_peer.close()
        stopped_auth.cleanup()

    def authenticated_pair() -> tuple[object, object, threading.Thread]:
        session = lab_worker._new_wire_session(roots=(SHARED_TMP_ROOT,))
        holder: dict[str, object] = {}

        def connect() -> None:
            holder["client"] = lab_worker.Client(
                session.address,
                family="AF_UNIX",
                authkey=session.authkey,
            )

        client_thread = threading.Thread(target=connect)
        client_thread.start()
        endpoint = session.listener.accept(
            deadline_microseconds=time.monotonic_ns() // 1_000 + 1_000_000,
            cancel_requested=lambda: False,
        )
        client_thread.join(timeout=_observe(1))
        assert not client_thread.is_alive()
        return session, endpoint, holder["client"]

    partial_control_session, partial_control, partial_control_client = authenticated_pair()
    try:
        os.write(partial_control_client.fileno(), struct.pack("!i", 80) + b"{")
        with pytest.raises(TimeoutError, match="timed out"):
            lab_worker._recv_wire(
                partial_control,
                model=lab_worker._IsolationReadiness,
                max_bytes=1024,
                label="partial readiness",
                deadline_microseconds=time.monotonic_ns() // 1_000 + 50_000,
                cancel_requested=lambda: False,
            )
        assert partial_control.closed
    finally:
        partial_control_client.close()
        partial_control.close()
        partial_control_session.cleanup()

    stopped_control_session, stopped_control, stopped_control_client = authenticated_pair()
    try:
        os.write(stopped_control_client.fileno(), struct.pack("!i", 80) + b"{")
        with pytest.raises(InterruptedError, match="cancel|stop"):
            lab_worker._recv_wire(
                stopped_control,
                model=lab_worker._IsolatedExecutionWireOutcome,
                max_bytes=1024,
                label="partial result",
                deadline_microseconds=time.monotonic_ns() // 1_000 + 1_000_000,
                cancel_requested=lambda: True,
            )
        assert stopped_control.closed
    finally:
        stopped_control_client.close()
        stopped_control.close()
        stopped_control_session.cleanup()
    assert {child.pid for child in multiprocessing.active_children()} == active_child_pids

    def recv_exact(peer: socket.socket, size: int) -> bytes:
        received = bytearray()
        while len(received) < size:
            chunk = peer.recv(size - len(received))
            if not chunk:
                raise EOFError("legacy probe peer closed")
            received.extend(chunk)
        return bytes(received)

    def recv_frame(peer: socket.socket, *, maximum: int = 1024 * 1024) -> bytes:
        size = struct.unpack("!i", recv_exact(peer, 4))[0]
        if size == -1:
            size = struct.unpack("!Q", recv_exact(peer, 8))[0]
        assert 0 <= size <= maximum
        return recv_exact(peer, size)

    def send_frame(peer: socket.socket, payload: bytes) -> None:
        peer.sendall(struct.pack("!i", len(payload)) + payload)

    legacy_session = lab_worker._new_wire_session(roots=(SHARED_TMP_ROOT,))
    legacy_peer = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    legacy_peer.settimeout(1)
    accepted_legacy: dict[str, object] = {}
    legacy_errors: list[BaseException] = []
    legacy_deadline = time.monotonic_ns() // 1_000 + 1_000_000

    def accept_legacy() -> None:
        try:
            accepted_legacy["endpoint"] = legacy_session.listener.accept(
                deadline_microseconds=legacy_deadline,
                cancel_requested=lambda: False,
            )
        except BaseException as exc:
            legacy_errors.append(exc)

    legacy_acceptor = threading.Thread(target=accept_legacy)
    legacy_acceptor.start()
    legacy_endpoint = None
    try:
        legacy_peer.connect(legacy_session.address)
        server_challenge_frame = recv_frame(legacy_peer, maximum=256)
        assert server_challenge_frame.startswith(b"#CHALLENGE#")
        server_challenge = server_challenge_frame[len(b"#CHALLENGE#") :]
        assert server_challenge.startswith(b"{sha256}")
        send_frame(
            legacy_peer,
            hmac.new(legacy_session.authkey, server_challenge, "md5").digest(),
        )
        assert recv_frame(legacy_peer, maximum=256) == b"#WELCOME#"

        peer_challenge = b"legacy-client-nonce!"
        assert len(peer_challenge) == 20
        send_frame(legacy_peer, b"#CHALLENGE#" + peer_challenge)
        peer_response = recv_frame(legacy_peer, maximum=256)
        assert (
            peer_response
            == hmac.new(
                legacy_session.authkey,
                peer_challenge,
                "md5",
            ).digest()
        )
        send_frame(legacy_peer, b"#WELCOME#")
        legacy_acceptor.join(timeout=_observe(1))
        assert not legacy_acceptor.is_alive()
        assert legacy_errors == []
        legacy_endpoint = accepted_legacy["endpoint"]
        assert legacy_endpoint._peer_digest_mode == "legacy-md5"

        lab_worker._send_wire(
            legacy_endpoint,
            lab_worker._IsolationStartAck(
                accepted=True,
                not_after_monotonic_microseconds=legacy_deadline,
            ),
            deadline_microseconds=legacy_deadline,
            cancel_requested=lambda: False,
        )
        start_ack = lab_worker._IsolationStartAck.model_validate_json(
            recv_frame(legacy_peer),
            strict=True,
        )
        assert start_ack.accepted is True
        assert start_ack.not_after_monotonic_microseconds == legacy_deadline

        for invalid_response in (
            b"{sha256}" + b"x" * 31,
            b"x" * 32,
            b"x" * 16,
        ):
            with pytest.raises(lab_worker.AuthenticationError):
                lab_worker._DeadlineWireEndpoint._verify_authentication_response(
                    legacy_session.authkey,
                    server_challenge,
                    invalid_response,
                )
    finally:
        legacy_peer.close()
        if legacy_endpoint is not None:
            legacy_endpoint.close()
        legacy_session.cleanup()
        legacy_acceptor.join(timeout=_observe(1))

    post_bind_root = SHARED_TMP_ROOT / f"lwl-post-bind-{uuid4().hex[:8]}"
    post_bind_root.mkdir(mode=0o700)
    original_chown = os.chown
    original_chmod = os.chmod
    original_stat = os.stat
    original_urandom = os.urandom

    def endpoint_name(value: object) -> str:
        if isinstance(value, int):
            return ""
        return Path(os.fspath(value)).name

    def fail_endpoint_chown(path: object, *args: object, **kwargs: object) -> None:
        if endpoint_name(path) == "wire.sock":
            raise OSError("post-bind chown failed")
        original_chown(path, *args, **kwargs)

    def fail_endpoint_chmod(path: object, *args: object, **kwargs: object) -> None:
        if endpoint_name(path) == "wire.sock":
            raise OSError("post-bind chmod failed")
        original_chmod(path, *args, **kwargs)

    endpoint_stat_calls = 0

    def fail_endpoint_validation_stat(path: object, *args: object, **kwargs: object):
        nonlocal endpoint_stat_calls
        if endpoint_name(path) == "wire.sock":
            endpoint_stat_calls += 1
            if endpoint_stat_calls == 2:
                raise OSError("post-bind stat failed")
        return original_stat(path, *args, **kwargs)

    def fail_auth_entropy(size: int) -> bytes:
        if size == 32:
            raise OSError("post-bind urandom failed")
        return original_urandom(size)

    for attribute, failure in (
        ("chown", fail_endpoint_chown),
        ("chmod", fail_endpoint_chmod),
        ("stat", fail_endpoint_validation_stat),
        ("urandom", fail_auth_entropy),
    ):
        endpoint_stat_calls = 0
        try:
            with monkeypatch.context() as post_bind_patch:
                post_bind_patch.setattr(lab_worker.os, attribute, failure)
                with pytest.raises(lab_worker.LabWireSessionStartupError):
                    lab_worker._new_wire_session(roots=(post_bind_root,))
            assert tuple(post_bind_root.iterdir()) == ()
        finally:
            for residue in post_bind_root.iterdir():
                shutil.rmtree(residue)
    post_bind_root.rmdir()

    replacement_root = SHARED_TMP_ROOT / f"lwl-replace-{uuid4().hex[:8]}"
    replacement_root.mkdir(mode=0o700)
    replacement_bytes = b"replacement endpoint"

    def replace_endpoint_then_fail(path: object, *args: object, **kwargs: object) -> None:
        if endpoint_name(path) == "wire.sock":
            endpoint = Path(os.fspath(path))
            endpoint.unlink()
            endpoint.write_bytes(replacement_bytes)
            raise OSError("post-bind replacement failure")
        original_chown(path, *args, **kwargs)

    with monkeypatch.context() as replacement_patch:
        replacement_patch.setattr(lab_worker.os, "chown", replace_endpoint_then_fail)
        with pytest.raises(lab_worker.LabWireSessionStartupError):
            lab_worker._new_wire_session(roots=(replacement_root,))
    replacement_directories = tuple(replacement_root.iterdir())
    assert len(replacement_directories) == 1
    assert (replacement_directories[0] / "wire.sock").read_bytes() == replacement_bytes
    shutil.rmtree(replacement_directories[0])
    replacement_root.rmdir()

    renamed_root = SHARED_TMP_ROOT / f"lwl-rename-{uuid4().hex[:8]}"
    renamed_root.mkdir(mode=0o700)
    moved_session = renamed_root / "moved-session"

    def replace_directory_then_fail(path: object, *args: object, **kwargs: object) -> None:
        if endpoint_name(path) == "wire.sock":
            endpoint = Path(os.fspath(path))
            session_path = endpoint.parent
            session_path.rename(moved_session)
            session_path.mkdir(mode=0o700)
            (session_path / "replacement").write_text("keep", encoding="ascii")
            raise OSError("post-bind directory replacement failure")
        original_chown(path, *args, **kwargs)

    with monkeypatch.context() as renamed_patch:
        renamed_patch.setattr(lab_worker.os, "chown", replace_directory_then_fail)
        with pytest.raises(lab_worker.LabWireSessionStartupError):
            lab_worker._new_wire_session(roots=(renamed_root,))
    original_name_replacement = next(
        path for path in renamed_root.iterdir() if path.name.startswith("rqlw-")
    )
    assert (original_name_replacement / "replacement").read_text(encoding="ascii") == "keep"
    assert (moved_session / "wire.sock").exists()
    shutil.rmtree(original_name_replacement)
    shutil.rmtree(moved_session)
    renamed_root.rmdir()

    long_root = SHARED_TMP_ROOT / f"long-{uuid4().hex}{'x' * 96}"
    long_root.mkdir(mode=0o700)
    fallback_session = None
    try:
        fallback_session = lab_worker._new_wire_session(roots=(long_root, SHARED_TMP_ROOT))
        assert fallback_session.root_path == SHARED_TMP_ROOT
    finally:
        if fallback_session is not None:
            fallback_session.cleanup()
        long_root.rmdir()

    all_bad_root = SHARED_TMP_ROOT / f"long-{uuid4().hex}{'x' * 96}"
    all_bad_root.mkdir(mode=0o700)
    try:
        with pytest.raises(lab_worker.LabWireSessionStartupError):
            lab_worker._new_wire_session(roots=(all_bad_root,))
        authority_claims = LabClaimSpool(tmp_path / "authority-root-fail-claims")
        authority_reports = LabReportSpool(tmp_path / "authority-root-fail-reports")
        authority_claim = _claim(_nshape_compare_spec(hold_days=(1,)))
        authority_claims.publish(authority_claim)
        authority_worker = _worker(
            tmp_path,
            claims=authority_claims,
            reports=authority_reports,
            resource_snapshot_provider=StaticResourceSnapshotProvider(_healthy_resource_snapshot()),
            admission_policy_provider=StaticAdmissionPolicyProvider(_permissive_admission_policy()),
            require_resource_admission=True,
        )
        with monkeypatch.context() as authority_patch:
            authority_patch.setattr(
                lab_worker,
                "_wire_root_candidates",
                lambda _roots: (all_bad_root,),
            )
            with pytest.raises(lab_worker.LabWireSessionStartupError):
                authority_worker.run_once()
        assert tuple(entry.claim for entry in authority_claims.pending()) == (authority_claim,)
        assert authority_reports.pending() == ()

        claims = LabClaimSpool(tmp_path / "all-root-fail-claims")
        reports = LabReportSpool(tmp_path / "all-root-fail-reports")
        failed_claim = _claim(_nshape_compare_spec(hold_days=(1,)))
        claims.publish(failed_claim)
        worker = _worker(tmp_path, claims=claims, reports=reports)
        with monkeypatch.context() as startup_patch:
            startup_patch.setattr(
                lab_worker,
                "_wire_root_candidates",
                lambda _roots: (all_bad_root,),
            )
            result = worker.run_once()
        failure = _reported_failure(reports)
        assert result.status == "failed"
        assert failure.phase == "session"
        assert failure.failure_kind == "session_startup"
        assert (claims.ack_dir / f"{failed_claim.claim_token}.json").is_file()
        assert (claims.admitted_dir / f"{failed_claim.claim_token}.json").is_file()
    finally:
        all_bad_root.rmdir()

    replaced_socket = lab_worker._new_wire_session(roots=(SHARED_TMP_ROOT,))
    replacement = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        os.unlink(replaced_socket.endpoint)
        replacement.bind(str(replaced_socket.endpoint))
        replaced_socket.cleanup()
        assert replaced_socket.path.is_dir()
        assert replaced_socket.endpoint.exists()
    finally:
        replacement.close()
        if replaced_socket.endpoint.exists():
            replaced_socket.endpoint.unlink()
        if replaced_socket.path.exists():
            replaced_socket.path.rmdir()

    replaced_directory = lab_worker._new_wire_session(roots=(SHARED_TMP_ROOT,))
    original = replaced_directory.path
    moved = replaced_directory.root_path / f"moved-session-{uuid4().hex}"
    os.rename(original, moved)
    original.mkdir(mode=0o700)
    try:
        replaced_directory.cleanup()
        assert original.is_dir()
        assert (moved / "wire.sock").exists()
    finally:
        (moved / "wire.sock").unlink()
        moved.rmdir()
        original.rmdir()

    unknown_child = lab_worker._new_wire_session(roots=(SHARED_TMP_ROOT,))
    unknown = unknown_child.path / "unknown"
    unknown.write_text("keep", encoding="ascii")
    unknown_child.cleanup()
    unknown_child.cleanup()
    assert unknown_child.path.is_dir()
    assert unknown.read_text(encoding="ascii") == "keep"
    assert not unknown_child.endpoint.exists()
    unknown.unlink()
    unknown_child.path.rmdir()

    normal_cleanup = lab_worker._new_wire_session(roots=(SHARED_TMP_ROOT,))
    normal_path = normal_cleanup.path
    normal_cleanup.cleanup()
    normal_cleanup.cleanup()
    assert not normal_path.exists()

    for phase, expected_kind in (
        ("claim", "claim_validation"),
        ("session", "session"),
        ("execute", "execution"),
        ("deadline", "deadline"),
        ("fence", "fence"),
        ("seal", "seal"),
    ):
        failure = lab_worker.LabWorkerFailure.model_validate_json(
            json.dumps({"phase": phase, "error_type": "RuntimeError", "message": "failed"})
        )
        assert failure.failure_kind == expected_kind
        assert '"failure_kind"' in failure.canonical_json()
        assert json.loads(failure.model_dump_json())["failure_kind"] == expected_kind
    explicit_startup = lab_worker.LabWorkerFailure(
        phase="session",
        failure_kind="session_startup",
        error_type="LabWireSessionStartupError",
        message="bind failed",
    )
    assert explicit_startup.model_dump(mode="json")["failure_kind"] == "session_startup"

    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    worker = _worker(tmp_path)
    validated = worker._validate_closed_claim(claim)
    monkeypatch.setattr(
        worker,
        "_start_wire_child",
        lambda **_kwargs: (_ for _ in ()).throw(lab_worker.LabWireSessionStartupError("bind")),
    )
    control = worker._execute_shard_isolated(
        claim,
        validated,
        runtime_code_sha="1" * 40,
        hard_limit_seconds=1,
        initial_session=lab_worker.TradingSession.CLOSED,
    )
    assert control.session_failure is not None
    assert control.session_failure.failure_kind == "session_startup"
    assert isinstance(control.session_failure.error, lab_worker.LabWireSessionStartupError)
    assert control.resource_error is None


class SequenceResourceSnapshotProvider:
    def __init__(self, *snapshots: object) -> None:
        if not snapshots:
            raise ValueError("at least one resource snapshot is required")
        self.snapshots = snapshots
        self._calls = multiprocessing.get_context("spawn").Value("i", 0)

    @property
    def calls(self) -> int:
        counter_path = getattr(self, "_closed_call_counter_path", None)
        if isinstance(counter_path, Path) and counter_path.exists():
            return int(counter_path.read_text(encoding="ascii"))
        return self._calls.value

    def __call__(self) -> object:
        with self._calls.get_lock():
            index = self._calls.value
            self._calls.value += 1
        return self.snapshots[min(index, len(self.snapshots) - 1)]


class MutableResourceSnapshotProvider:
    def __init__(self, *snapshots: object) -> None:
        if not snapshots:
            raise ValueError("at least one resource snapshot is required")
        self.snapshots = snapshots
        self._selected = multiprocessing.get_context("spawn").Value("i", 0)

    def select(self, index: int) -> None:
        if index < 0 or index >= len(self.snapshots):
            raise ValueError("resource snapshot selection is out of range")
        selection_path = getattr(self, "_closed_selection_path", None)
        if isinstance(selection_path, Path):
            selection_path.write_text(str(index), encoding="ascii")
        with self._selected.get_lock():
            self._selected.value = index

    def __call__(self) -> object:
        with self._selected.get_lock():
            return self.snapshots[self._selected.value]


class FileSelectedResourceSnapshotProvider:
    def __init__(self, selection_path: Path, *snapshots: object) -> None:
        if not snapshots:
            raise ValueError("at least one resource snapshot is required")
        self.selection_path = selection_path
        self.snapshots = snapshots

    def select(self, index: int) -> None:
        if index < 0 or index >= len(self.snapshots):
            raise ValueError("resource snapshot selection is out of range")
        self.selection_path.write_text(str(index), encoding="ascii")

    def __call__(self) -> object:
        index = (
            int(self.selection_path.read_text(encoding="ascii"))
            if self.selection_path.exists()
            else 0
        )
        return self.snapshots[index]


class FailingResourceSnapshotProvider:
    def __init__(self, message: str) -> None:
        self.message = message

    def __call__(self) -> object:
        raise RuntimeError(self.message)


class MutableUtcClock:
    def __init__(self, value: datetime) -> None:
        self._timestamp = multiprocessing.get_context("spawn").Value("d", value.timestamp())

    def set(self, value: datetime) -> None:
        with self._timestamp.get_lock():
            self._timestamp.value = value.timestamp()

    def __call__(self) -> datetime:
        with self._timestamp.get_lock():
            timestamp = self._timestamp.value
        return datetime.fromtimestamp(timestamp, tz=UTC)


class SlowStoreContext:
    def __init__(self, *, delay_seconds: float) -> None:
        self.delay_seconds = delay_seconds

    def __enter__(self) -> object:
        time.sleep(self.delay_seconds)
        return object()

    def __exit__(self, _exc_type: object, _exc: object, _traceback: object) -> None:
        return None


class SlowStoreFactory:
    def __init__(self, *, delay_seconds: float) -> None:
        self.delay_seconds = delay_seconds

    def __call__(self) -> SlowStoreContext:
        return SlowStoreContext(delay_seconds=self.delay_seconds)


class FailingSessionInitializer:
    def __call__(self) -> None:
        raise PermissionError("setsid denied")


class RecordingSessionInitializer:
    def __init__(self, pid_path: Path) -> None:
        self.pid_path = pid_path

    def __call__(self) -> None:
        os.setsid()
        self.pid_path.write_text(str(os.getpid()), encoding="ascii")


class BlockingRecordingSessionInitializer(RecordingSessionInitializer):
    def __init__(self, pid_path: Path, release_path: Path) -> None:
        super().__init__(pid_path)
        self.release_path = release_path

    def __call__(self) -> None:
        super().__call__()
        deadline = time.monotonic() + _observe(5)
        while not self.release_path.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        if not self.release_path.exists():
            raise TimeoutError("test did not release isolated session readiness")


class SequenceAdmissionPolicyProvider:
    def __init__(self, *policies: object) -> None:
        if not policies:
            raise ValueError("at least one admission policy is required")
        self.policies = policies
        self._calls = multiprocessing.get_context("spawn").Value("i", 0)

    @property
    def calls(self) -> int:
        counter_path = getattr(self, "_closed_call_counter_path", None)
        if isinstance(counter_path, Path) and counter_path.exists():
            return int(counter_path.read_text(encoding="ascii"))
        return self._calls.value

    def __call__(self, _spec: ResearchRunSpec) -> object:
        with self._calls.get_lock():
            index = self._calls.value
            self._calls.value += 1
        return self.policies[min(index, len(self.policies) - 1)]


class StaticAdmissionPolicyProvider:
    def __init__(self, policy: object) -> None:
        self.policy = policy

    def __call__(self, _spec: ResearchRunSpec) -> object:
        return self.policy


class SlowAdmissionPolicyProvider:
    def __init__(
        self,
        policy: object,
        *,
        delay_seconds: float,
        second_call_entered_path: Path,
    ) -> None:
        self.policy = policy
        self.delay_seconds = delay_seconds
        self.second_call_entered_path = second_call_entered_path


class BlockingInitialAdmissionPolicyProvider:
    def __init__(
        self,
        policy: object,
        *,
        entered_path: Path,
        pid_path: Path,
        release_path: Path,
    ) -> None:
        self.policy = policy
        self.entered_path = entered_path
        self.pid_path = pid_path
        self.release_path = release_path

    def __call__(self, _spec: ResearchRunSpec) -> object:
        self.pid_path.write_text(str(os.getpid()), encoding="ascii")
        self.entered_path.write_text("entered", encoding="ascii")
        deadline = time.monotonic() + _observe(10)
        while not self.release_path.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        if not self.release_path.exists():
            raise TimeoutError("test initial policy callback was not cancelled")
        return self.policy


class BlockingInitialSourceQuotaLeaseProvider:
    def __init__(
        self,
        *,
        entered_path: Path,
        pid_path: Path,
        release_path: Path,
    ) -> None:
        self.entered_path = entered_path
        self.pid_path = pid_path
        self.release_path = release_path

    def __call__(self, request: object, _snapshot: object) -> object:
        from rquant.resource_admission import AdmissionRequest, SourceQuotaLease

        validated_request = AdmissionRequest.model_validate(request)
        self.pid_path.write_text(str(os.getpid()), encoding="ascii")
        self.entered_path.write_text("entered", encoding="ascii")
        deadline = time.monotonic() + _observe(10)
        while not self.release_path.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        if not self.release_path.exists():
            raise TimeoutError("test initial quota callback was not cancelled")
        return SourceQuotaLease(
            source=validated_request.source or "test-source",
            owner=validated_request.job_id,
            units=validated_request.expected_quota_units,
            granted_at=NOW,
            expires_at=NOW + timedelta(minutes=1),
            quota_reset_at=NOW + timedelta(minutes=2),
        )


class SpawnDescendantBlockingAdmissionPolicyProvider:
    def __init__(
        self,
        policy: object,
        *,
        authority_pid_path: Path,
        descendant_pid_path: Path,
        release_path: Path,
    ) -> None:
        self.policy = policy
        self.authority_pid_path = authority_pid_path
        self.descendant_pid_path = descendant_pid_path
        self.release_path = release_path

    def __call__(self, _spec: ResearchRunSpec) -> object:
        context = multiprocessing.get_context("spawn")
        ready = context.Event()
        descendant = context.Process(
            target=_ignore_term_forever,
            args=(self.descendant_pid_path, ready),
            daemon=False,
        )
        descendant.start()
        self.authority_pid_path.write_text(str(os.getpid()), encoding="ascii")
        if not ready.wait(2):
            raise TimeoutError("authority callback descendant did not start")
        deadline = time.monotonic() + _observe(10)
        while not self.release_path.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        if not self.release_path.exists():
            raise TimeoutError("test recursive authority callback was not cancelled")
        descendant.kill()
        descendant.join(1)
        descendant.close()
        return self.policy


class FailingInitialAdmissionPolicyProvider:
    def __call__(self, _spec: ResearchRunSpec) -> object:
        raise RuntimeError("initial policy exploded")


class FailingInitialSourceQuotaLeaseProvider:
    def __call__(self, _request: object, _snapshot: object) -> object:
        raise RuntimeError("initial quota exploded")


class BlockingSecondAdmissionPolicyProvider:
    def __init__(
        self,
        policy: object,
        *,
        entered_path: Path,
        release_path: Path,
    ) -> None:
        self.policy = policy
        self.entered_path = entered_path
        self.release_path = release_path
        self._calls = multiprocessing.get_context("spawn").Value("i", 0)

    def __call__(self, _spec: ResearchRunSpec) -> object:
        with self._calls.get_lock():
            self._calls.value += 1
            call_number = self._calls.value
        if call_number > 1:
            self.entered_path.write_text("entered", encoding="ascii")
            deadline = time.monotonic() + _observe(10)
            while not self.release_path.exists() and time.monotonic() < deadline:
                time.sleep(0.01)
            if not self.release_path.exists():
                raise TimeoutError("test admission policy callback was not cancelled")
        return self.policy


class BlockingSecondSourceQuotaLeaseProvider:
    def __init__(
        self,
        *,
        entered_path: Path,
        release_path: Path,
        authority_pid_path: Path | None = None,
    ) -> None:
        self.entered_path = entered_path
        self.release_path = release_path
        self.authority_pid_path = authority_pid_path
        self._calls = multiprocessing.get_context("spawn").Value("i", 0)

    def __call__(self, request: object, _snapshot: object) -> object:
        from rquant.resource_admission import AdmissionRequest, SourceQuotaLease

        validated_request = AdmissionRequest.model_validate(request)
        with self._calls.get_lock():
            self._calls.value += 1
            call_number = self._calls.value
        if call_number > 1:
            self.entered_path.write_text("entered", encoding="ascii")
            deadline = time.monotonic() + _observe(10)
            while not self.release_path.exists() and time.monotonic() < deadline:
                time.sleep(0.01)
            if not self.release_path.exists():
                raise TimeoutError("test source quota callback was not cancelled")
        return SourceQuotaLease(
            source=validated_request.source or "test-source",
            owner=validated_request.job_id,
            units=validated_request.expected_quota_units,
            granted_at=NOW,
            expires_at=NOW + timedelta(minutes=1),
            quota_reset_at=NOW + timedelta(minutes=2),
        )


def _recorded_pid(path: Path, *, timeout_seconds: float = 2.0) -> int | None:
    """Read a pid a child writes, tolerating the gap between create and write.

    The child creates the file and writes into it as two steps, so `exists()`
    can be true while the content is still empty. Return None only when the
    child never got as far as recording a pid.
    """
    deadline = time.monotonic() + timeout_seconds
    while True:
        try:
            recorded = path.read_text(encoding="ascii").strip()
        except FileNotFoundError:
            recorded = ""
        if recorded:
            return int(recorded)
        if time.monotonic() >= deadline:
            return None
        time.sleep(0.01)


def _assert_process_gone(pid: int, *, timeout_seconds: float = 2.0) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return
        time.sleep(0.01)
    pytest.fail(f"process remained alive after isolation cleanup: {pid}")


def _kill_process_if_alive(pid: int) -> None:
    with suppress(ProcessLookupError):
        os.kill(pid, signal.SIGKILL)


def _collect_base_exceptions(error: BaseException) -> list[BaseException]:
    if isinstance(error, BaseExceptionGroup):
        return [
            nested for member in error.exceptions for nested in _collect_base_exceptions(member)
        ]
    return [error]


def _cleanup_spawned_probe_processes(*, group_id: int | None, pids: tuple[int, ...]) -> None:
    if group_id is not None:
        with suppress(ProcessLookupError):
            os.killpg(group_id, signal.SIGKILL)
    for pid in pids:
        _kill_process_if_alive(pid)
    for child in multiprocessing.active_children():
        if child.name == "lab-resource-probe":
            with suppress(BaseException):
                child.kill()
            with suppress(BaseException):
                child.join(1)


def _short_live_claim() -> LabShardClaim:
    return _short_claim_for_spec(_nshape_compare_spec(hold_days=(1,)))


def _short_claim_for_spec(spec: ResearchRunSpec) -> LabShardClaim:
    from rquant.lab_shard_protocol import LabShardDefinition

    claim = _claim(spec)
    work_plan = claim.definition.work_plan
    assert work_plan is not None
    definition = LabShardDefinition.from_payload(
        shard_index=claim.definition.shard_index,
        adapter_id=claim.definition.adapter_id,
        adapter_version=claim.definition.adapter_version,
        plan_hash=claim.definition.plan_hash,
        payload_json=claim.definition.payload_json,
        work_plan=work_plan.model_copy(update={"static_duration_ms": 50}),
    )
    return LabShardClaim.model_validate(
        {
            **claim.model_dump(mode="python"),
            "definition": definition,
        }
    )


def _fixture_counter_path(tmp_path: Path, owner: object, label: str) -> Path:
    path = tmp_path / f".{label}-{id(owner):x}.count"
    if not path.exists():
        path.write_text("0", encoding="ascii")
    return path


def _fixture_counter_increment(path_value: object) -> int:
    path = Path(str(path_value))
    current = int(path.read_text(encoding="ascii")) if path.exists() else 0
    current += 1
    path.write_text(str(current), encoding="ascii")
    return current


def _fixture_model(value: object, model: type[object]) -> dict[str, object]:
    validated = model.model_validate(value)
    return validated.model_dump(mode="json", round_trip=True)


def _snapshot_fixture_config(provider: object, tmp_path: Path) -> dict[str, object]:
    from rquant.resource_admission import ResourceSnapshot

    if type(provider) is StaticResourceSnapshotProvider:
        return {
            "kind": "static",
            "snapshot": _fixture_model(provider.snapshot, ResourceSnapshot),
        }
    if type(provider) is SequenceResourceSnapshotProvider:
        counter = _fixture_counter_path(tmp_path, provider, "snapshot")
        provider._closed_call_counter_path = counter
        return {
            "kind": "sequence",
            "counter_path": str(counter),
            "snapshots": [
                _fixture_model(snapshot, ResourceSnapshot) for snapshot in provider.snapshots
            ],
        }
    if type(provider) is MutableResourceSnapshotProvider:
        selection = tmp_path / f".snapshot-selection-{id(provider):x}"
        with provider._selected.get_lock():
            selection.write_text(str(provider._selected.value), encoding="ascii")
        provider._closed_selection_path = selection
        return {
            "kind": "selected",
            "selection_path": str(selection),
            "snapshots": [
                _fixture_model(snapshot, ResourceSnapshot) for snapshot in provider.snapshots
            ],
        }
    if type(provider) is FileSelectedResourceSnapshotProvider:
        return {
            "kind": "selected",
            "selection_path": str(provider.selection_path),
            "snapshots": [
                _fixture_model(snapshot, ResourceSnapshot) for snapshot in provider.snapshots
            ],
        }
    if type(provider) is PermanentlyBlockingAfterFirstSnapshotProvider:
        return {
            "kind": "block-after-calls",
            "block_after_calls": provider.block_after_calls,
            "counter_path": str(provider.marker_path),
            "snapshot": _fixture_model(provider.snapshot, ResourceSnapshot),
        }
    if type(provider) is SpawnDescendantBlockingResourceSnapshotProvider:
        return {
            "kind": "recursive-block",
            "pid_path": str(provider.probe_pid_path),
            "descendant_pid_path": str(provider.descendant_pid_path),
        }
    if type(provider) is FailingResourceSnapshotProvider:
        return {
            "kind": "failure",
            "message": f"resource snapshot provider failed: {provider.message}",
        }
    if type(provider) is ExportingResourceSnapshotProvider:
        return {
            "kind": "exporting",
            "snapshot": _fixture_model(provider.snapshot, ResourceSnapshot),
            "state": provider.state,
        }
    if type(provider) is RejectingProbeStateProvider:
        return {
            "kind": "reject-state",
            "snapshot": _fixture_model(provider.snapshot, ResourceSnapshot),
            "state": provider.state,
        }
    if type(provider) is AdversarialSnapshotHooksProvider:
        return {
            "kind": "adversarial-hook",
            "hook": provider.hook,
            "behavior": provider.behavior,
            "snapshot": _fixture_model(provider.snapshot, ResourceSnapshot),
            "entered_path": str(provider.entered_path),
            "pid_path": str(provider.hook_pid_path),
            "release_path": str(provider.release_path),
            "descendant_pid_path": (
                None if provider.descendant_pid_path is None else str(provider.descendant_pid_path)
            ),
        }
    return {"kind": "unregistered", "message": "snapshot provider is not spawn-serializable"}


def _policy_fixture_config(provider: object, tmp_path: Path) -> dict[str, object]:
    from rquant.resource_admission import AdmissionPolicy

    if type(provider) is StaticAdmissionPolicyProvider:
        return {
            "kind": "static",
            "policy": _fixture_model(provider.policy, AdmissionPolicy),
        }
    if type(provider) is SlowAdmissionPolicyProvider:
        counter = _fixture_counter_path(tmp_path, provider, "policy")
        return {
            "kind": "slow",
            "policy": _fixture_model(provider.policy, AdmissionPolicy),
            "counter_path": str(counter),
            "delay_seconds": provider.delay_seconds,
            "second_call_entered_path": str(provider.second_call_entered_path),
        }
    if type(provider) is SequenceAdmissionPolicyProvider:
        counter = _fixture_counter_path(tmp_path, provider, "policy")
        provider._closed_call_counter_path = counter
        return {
            "kind": "sequence",
            "counter_path": str(counter),
            "policies": [_fixture_model(policy, AdmissionPolicy) for policy in provider.policies],
        }
    if type(provider) is BlockingInitialAdmissionPolicyProvider:
        return {
            "kind": "block",
            "policy": _fixture_model(provider.policy, AdmissionPolicy),
            "entered_path": str(provider.entered_path),
            "pid_path": str(provider.pid_path),
            "release_path": str(provider.release_path),
        }
    if type(provider) is SpawnDescendantBlockingAdmissionPolicyProvider:
        return {
            "kind": "recursive-block",
            "policy": _fixture_model(provider.policy, AdmissionPolicy),
            "pid_path": str(provider.authority_pid_path),
            "descendant_pid_path": str(provider.descendant_pid_path),
            "release_path": str(provider.release_path),
        }
    if type(provider) is FailingInitialAdmissionPolicyProvider:
        return {
            "kind": "failure",
            "message": "admission policy provider failed: initial policy exploded",
        }
    if type(provider) is BlockingSecondAdmissionPolicyProvider:
        counter = _fixture_counter_path(tmp_path, provider, "policy")
        return {
            "kind": "block-after-first",
            "policy": _fixture_model(provider.policy, AdmissionPolicy),
            "counter_path": str(counter),
            "entered_path": str(provider.entered_path),
            "release_path": str(provider.release_path),
        }
    return {"kind": "unregistered", "message": "policy provider is not spawn-serializable"}


def _quota_fixture_config(provider: object | None, tmp_path: Path) -> dict[str, object]:
    if provider is None:
        return {"kind": "none"}
    if type(provider) is BlockingInitialSourceQuotaLeaseProvider:
        return {
            "kind": "block",
            "entered_path": str(provider.entered_path),
            "pid_path": str(provider.pid_path),
            "release_path": str(provider.release_path),
        }
    if type(provider) is FailingInitialSourceQuotaLeaseProvider:
        return {
            "kind": "failure",
            "message": "source quota lease provider failed: initial quota exploded",
        }
    if type(provider) is BlockingSecondSourceQuotaLeaseProvider:
        counter = _fixture_counter_path(tmp_path, provider, "quota")
        configuration: dict[str, object] = {
            "kind": "block-after-first",
            "counter_path": str(counter),
            "entered_path": str(provider.entered_path),
            "release_path": str(provider.release_path),
        }
        if provider.authority_pid_path is not None:
            configuration["pid_path"] = str(provider.authority_pid_path)
        return configuration
    return {"kind": "unregistered", "message": "quota provider is not spawn-serializable"}


def _test_authority_manifest(
    tmp_path: Path,
    *,
    snapshot_provider: object,
    policy_provider: object,
    quota_provider: object | None,
):
    from rquant.lab_worker import (
        LabClosedRegistryBinding,
        LabResourceAuthorityManifest,
    )

    configuration = {
        "policy": _policy_fixture_config(policy_provider, tmp_path),
        "quota": _quota_fixture_config(quota_provider, tmp_path),
        "snapshot": _snapshot_fixture_config(snapshot_provider, tmp_path),
    }
    configuration_json = json.dumps(
        configuration,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    return LabResourceAuthorityManifest(
        registry=LabClosedRegistryBinding(
            registry_id="rquant.lab-authority.test-fixture",
            registry_version=1,
            registry_hash=hashlib.sha256(b"rquant:lab-authority:test-fixture:v1").hexdigest(),
            configuration_json=configuration_json,
        )
    )


def _wait_fixture_release(path_value: object, *, timeout_seconds: float = 10) -> None:
    path = Path(str(path_value))
    deadline = time.monotonic() + timeout_seconds
    while not path.exists() and time.monotonic() < deadline:
        time.sleep(0.01)
    if not path.exists():
        raise TimeoutError("test authority fixture was not released")


def _spawn_fixture_descendant(path_value: object) -> int:
    path = Path(str(path_value))
    child_pid = os.fork()
    if child_pid == 0:
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        path.write_text(str(os.getpid()), encoding="ascii")
        threading.Event().wait()
        os._exit(1)
    deadline = time.monotonic() + _observe(2)
    while not path.exists() and time.monotonic() < deadline:
        time.sleep(0.01)
    if not path.exists():
        raise TimeoutError("test authority descendant did not start")
    return child_pid


def evaluate_lab_authority_fixture(
    configuration: object,
    *,
    operation: str,
    spec: ResearchRunSpec | None,
    admission_request: object | None,
    snapshot: object | None,
    authority_state: object | None,
) -> dict[str, object]:
    from rquant.lab_worker import (
        LabSnapshotAuthorityState,
        _AuthorityWireResult,
    )
    from rquant.resource_admission import (
        AdmissionRequest,
        ResourceSnapshot,
        SourceQuotaLease,
    )

    assert isinstance(configuration, dict)
    if operation == "admission":
        policy_result = _AuthorityWireResult.model_validate(
            evaluate_lab_authority_fixture(
                configuration,
                operation="policy",
                spec=spec,
                admission_request=admission_request,
                snapshot=snapshot,
                authority_state=authority_state,
            )
        )
        snapshot_result = _AuthorityWireResult.model_validate(
            evaluate_lab_authority_fixture(
                configuration,
                operation="snapshot",
                spec=spec,
                admission_request=admission_request,
                snapshot=snapshot,
                authority_state=authority_state,
            )
        )
        quota_lease = None
        if (
            admission_request is not None
            and AdmissionRequest.model_validate(admission_request).expected_quota_units > 0
        ):
            quota_result = _AuthorityWireResult.model_validate(
                evaluate_lab_authority_fixture(
                    configuration,
                    operation="quota",
                    spec=spec,
                    admission_request=admission_request,
                    snapshot=snapshot_result.snapshot,
                    authority_state=snapshot_result.authority_state,
                )
            )
            quota_lease = quota_result.quota_lease
        return _AuthorityWireResult(
            operation="admission",
            policy=policy_result.policy,
            snapshot=snapshot_result.snapshot,
            quota_lease=quota_lease,
            authority_state=snapshot_result.authority_state,
        ).model_dump(mode="python")
    del spec, snapshot, authority_state
    component = configuration[operation]
    assert isinstance(component, dict)
    kind = component["kind"]
    if kind == "unregistered":
        raise LabDaemonConfigurationError(str(component["message"]))
    if operation == "policy":
        if kind in {"block", "recursive-block"}:
            Path(str(component["pid_path"])).write_text(str(os.getpid()), encoding="ascii")
            if "entered_path" in component:
                Path(str(component["entered_path"])).write_text("entered", encoding="ascii")
            if kind == "recursive-block":
                _spawn_fixture_descendant(component["descendant_pid_path"])
            _wait_fixture_release(component["release_path"])
        elif kind == "block-after-first":
            call = _fixture_counter_increment(component["counter_path"])
            if call > 1:
                Path(str(component["entered_path"])).write_text("entered", encoding="ascii")
                _wait_fixture_release(component["release_path"])
        elif kind == "failure":
            raise RuntimeError(str(component["message"]))
        if kind == "sequence":
            call = _fixture_counter_increment(component["counter_path"])
            policies = component["policies"]
            assert isinstance(policies, list)
            policy = policies[min(call - 1, len(policies) - 1)]
        else:
            policy = component["policy"]
        return _AuthorityWireResult(operation="policy", policy=policy).model_dump(mode="python")
    if operation == "snapshot":
        if kind == "failure":
            raise RuntimeError(str(component["message"]))
        if kind == "recursive-block":
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
            Path(str(component["pid_path"])).write_text(str(os.getpid()), encoding="ascii")
            _spawn_fixture_descendant(component["descendant_pid_path"])
            threading.Event().wait()
        if kind == "block-after-first":
            marker = Path(str(component["marker_path"]))
            if marker.exists():
                threading.Event().wait()
            marker.write_text("first", encoding="ascii")
        if kind == "block-after-calls" and _fixture_counter_increment(
            component["counter_path"]
        ) > int(component["block_after_calls"]):
            threading.Event().wait()
        if kind == "adversarial-hook":
            Path(str(component["pid_path"])).write_text(str(os.getpid()), encoding="ascii")
            Path(str(component["entered_path"])).write_text("entered", encoding="ascii")
            if component["behavior"] == "exception":
                raise RuntimeError(f"{component['hook']} exploded")
            if component["behavior"] == "recursive":
                _spawn_fixture_descendant(component["descendant_pid_path"])
            _wait_fixture_release(component["release_path"])
        if kind == "reject-state":
            raise LabDaemonConfigurationError("resource snapshot authority state was not accepted")
        if kind == "sequence":
            call = _fixture_counter_increment(component["counter_path"])
            snapshots = component["snapshots"]
            assert isinstance(snapshots, list)
            raw_snapshot = snapshots[min(call - 1, len(snapshots) - 1)]
        elif kind == "selected":
            selection_path = Path(str(component["selection_path"]))
            index = (
                int(selection_path.read_text(encoding="ascii")) if selection_path.exists() else 0
            )
            snapshots = component["snapshots"]
            assert isinstance(snapshots, list)
            raw_snapshot = snapshots[index]
        else:
            raw_snapshot = component["snapshot"]
        state = None
        if kind in {"exporting", "reject-state", "adversarial-hook"}:
            raw_state = component.get("state", {"sequence": 1})
            state = LabSnapshotAuthorityState(
                state_kind="test-fixture",
                state_json=json.dumps(
                    raw_state,
                    ensure_ascii=True,
                    sort_keys=True,
                    separators=(",", ":"),
                    allow_nan=False,
                ),
            )
        return _AuthorityWireResult(
            operation="snapshot",
            snapshot=ResourceSnapshot.model_validate(raw_snapshot),
            authority_state=state,
        ).model_dump(mode="python")
    if kind == "none":
        return _AuthorityWireResult(operation="quota").model_dump(mode="python")
    if kind in {"block", "block-after-first"}:
        should_block = True
        if kind == "block-after-first":
            should_block = _fixture_counter_increment(component["counter_path"]) > 1
        if should_block:
            if "pid_path" in component:
                Path(str(component["pid_path"])).write_text(str(os.getpid()), encoding="ascii")
            Path(str(component["entered_path"])).write_text("entered", encoding="ascii")
            _wait_fixture_release(component["release_path"])
    if kind == "failure":
        raise RuntimeError(str(component["message"]))
    request = AdmissionRequest.model_validate(admission_request)
    lease = SourceQuotaLease(
        source=request.source or "test-source",
        owner=request.job_id,
        units=request.expected_quota_units,
        granted_at=NOW,
        expires_at=NOW + timedelta(minutes=1),
        quota_reset_at=NOW + timedelta(minutes=2),
    )
    return _AuthorityWireResult(operation="quota", quota_lease=lease).model_dump(mode="python")


def _test_shard_manifest(
    tmp_path: Path,
    *,
    registry: object,
    exploratory_store_factory: object,
    metadata_store_factory: object,
    lake_root: Path | None,
    isolation_session_initializer: object,
    execution_session_factory: object,
    research_store_opener: object,
):
    from rquant.lab_worker import LabClosedRegistryBinding, LabShardRuntimeManifest

    counter = _fixture_counter_path(tmp_path, registry, "adapter")
    if isinstance(registry, RecordingRegistry):
        registry._closed_execution_counter_path = counter
    adapter: dict[str, object]
    if type(registry) is UnserializableRegistry:
        adapter = {"kind": "unregistered", "message": "adapter is not spawn-serializable"}
    elif type(registry) is ParentReduceTrap:
        adapter = {"kind": "unregistered", "message": "adapter is not registered"}
    elif type(registry) is MaliciousResultRegistry:
        adapter = {"kind": "malicious-result", "path": str(registry.reduce_marker)}
    elif type(registry) is FailingWorkerRegistry:
        adapter = {"kind": "failure", "message": "worker exploded"}
    elif type(registry) is SpawnMethodRegistry:
        adapter = {"kind": "spawn-method", "path": str(registry.pid_path)}
    elif type(registry) is SlowPidRegistry:
        adapter = {
            "kind": "slow",
            "path": str(registry.pid_path),
            "delay_seconds": registry.delay_seconds,
        }
    elif type(registry) is SigtermIgnoringProcessTreeRegistry:
        adapter = {
            "kind": "sigterm-tree",
            "path": str(registry.pid_path),
            "descendant_path": str(registry.grandchild_pid_path),
        }
    elif type(registry) is TermExitingLeaderProcessTreeRegistry:
        adapter = {
            "kind": "term-exit-tree",
            "path": str(registry.pid_path),
            "descendant_path": str(registry.grandchild_pid_path),
        }
    elif type(registry) is SuccessfulProcessTreeRegistry:
        adapter = {
            "kind": "successful-tree",
            "path": str(registry.pid_path),
            "descendant_path": str(registry.grandchild_pid_path),
        }
    elif type(registry) is HungLiveRegistry:
        adapter = {"kind": "hung", "path": str(registry.pid_path)}
    elif type(registry) is BlockingRegistry:
        entered_path = tmp_path / f".adapter-entered-{id(registry):x}"
        release_path = tmp_path / f".adapter-release-{id(registry):x}"
        registry._closed_entered_path = entered_path
        registry._closed_release_path = release_path
        adapter = {
            "kind": "blocking",
            "entered_path": str(entered_path),
            "release_path": str(release_path),
        }
    elif type(registry) is DeadlineRegistry:
        marker_path = tmp_path / f".adapter-deadline-{id(registry):x}"
        registry._closed_deadline_path = marker_path
        adapter = {"kind": "deadline", "path": str(marker_path)}
    elif isinstance(registry, RecordingRegistry):
        adapter = {
            "kind": "recording",
            "delay_seconds": registry.delay_seconds,
            "failure": (
                None
                if registry.failure is None
                else {
                    "error_type": type(registry.failure).__name__,
                    "message": str(registry.failure),
                }
            ),
            "legacy_profile": registry.legacy_profile,
        }
        artifact_profile = getattr(registry, "artifact_profile", None)
        if artifact_profile == "nshape_projection":
            adapter["artifact_profile"] = artifact_profile
    else:
        adapter = {"kind": "unregistered", "message": "adapter is not spawn-serializable"}
    session: dict[str, object] = {"kind": "default"}
    if type(isolation_session_initializer) is RecordingSessionInitializer:
        session = {"kind": "record", "pid_path": str(isolation_session_initializer.pid_path)}
    elif type(isolation_session_initializer) is BlockingRecordingSessionInitializer:
        session = {
            "kind": "block",
            "pid_path": str(isolation_session_initializer.pid_path),
            "release_path": str(isolation_session_initializer.release_path),
        }
    elif type(isolation_session_initializer) is FailingSessionInitializer:
        session = {"kind": "failure"}
    store: dict[str, object] = {"kind": "default"}
    if type(exploratory_store_factory) is SlowStoreFactory:
        store = {"kind": "slow", "delay_seconds": exploratory_store_factory.delay_seconds}
    formal: dict[str, object] | None = None
    if metadata_store_factory is not None:
        if (
            type(metadata_store_factory) is MetadataStoreFactory
            and type(metadata_store_factory.store) is _MetadataStore
        ):
            metadata = metadata_store_factory.store
            formal = {
                "binding_hash": metadata.binding.binding_hash,
                "snapshot_strategy_name": metadata.snapshot.strategy_name,
                "p0_count": metadata.audit.p0_count,
            }
        else:
            formal = {"unregistered": True}
    if type(execution_session_factory) is FakeExecutionSessionFactory:
        opened_path = tmp_path / f".formal-session-opened-{id(execution_session_factory):x}"
        execution_session_factory._closed_opened_path = opened_path
        if formal is None:
            formal = {}
        formal["opened_path"] = str(opened_path)
    if type(research_store_opener) is RecordingResearchStoreOpener:
        request_path = tmp_path / f".formal-request-{id(research_store_opener):x}.json"
        research_store_opener._closed_request_path = request_path
        if formal is None:
            formal = {}
        formal["request_path"] = str(request_path)
    configuration = {
        "adapter": adapter,
        "bypass_parent_validation": isinstance(
            registry,
            (PlanBypassRecordingRegistry, HungLiveRegistry),
        ),
        "counter_path": str(counter),
        "formal": formal,
        "lake_root": None if lake_root is None else str(lake_root),
        "session": session,
        "store": store,
    }
    configuration_json = json.dumps(
        configuration,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    return LabShardRuntimeManifest(
        registry=LabClosedRegistryBinding(
            registry_id="rquant.lab-shard.test-fixture",
            registry_version=1,
            registry_hash=hashlib.sha256(b"rquant:lab-shard:test-fixture:v1").hexdigest(),
            configuration_json=configuration_json,
        )
    )


def prepare_lab_shard_fixture(configuration: object) -> None:
    assert isinstance(configuration, dict)
    session = configuration["session"]
    assert isinstance(session, dict)
    kind = session["kind"]
    if kind in {"record", "block"}:
        Path(str(session["pid_path"])).write_text(str(os.getpid()), encoding="ascii")
    if kind == "block":
        _wait_fixture_release(session["release_path"], timeout_seconds=5)
    if kind == "failure":
        raise PermissionError("setsid denied")


def execute_lab_shard_fixture(
    configuration: object,
    validated: ValidatedStrategyShard,
    *,
    runtime_code_sha: str,
) -> LabShardExecutionResult:
    assert isinstance(configuration, dict)
    formal = configuration["formal"]
    if formal is not None:
        from rquant.research_gate import ResearchGateRequest

        assert isinstance(formal, dict)
        identity = validated.spec.dataset_snapshot
        if identity is None or configuration["lake_root"] is None:
            raise PermissionError("formal worker execution requires dataset snapshot and lake")
        if formal.get("unregistered") is True:
            raise LabDaemonConfigurationError("formal test store is not registered")
        if formal.get("binding_hash") != identity.binding_hash:
            raise PermissionError("formal dataset snapshot binding hash mismatch")
        registry = default_strategy_job_adapter_registry()
        strategy_name = registry.for_spec(validated.spec).snapshot_strategy_name
        if formal.get("snapshot_strategy_name") != strategy_name or formal.get("p0_count") != 0:
            raise PermissionError("formal research gate rejected fixture evidence")
        gate_request = ResearchGateRequest(
            mode="formal",
            strategy_name=strategy_name,
            start_date=validated.spec.parameters.start_date,
            end_date=validated.spec.parameters.end_date,
            audit_run_id=identity.audit_run_id,
            dataset_snapshot_id=identity.snapshot_id,
            dataset_binding_hash=identity.binding_hash,
            code_commit=runtime_code_sha,
        )
        if "request_path" in formal:
            Path(str(formal["request_path"])).write_text(
                gate_request.model_dump_json(), encoding="utf-8"
            )
        if "opened_path" in formal:
            Path(str(formal["opened_path"])).write_text("opened", encoding="ascii")
    store = configuration["store"]
    assert isinstance(store, dict)
    if store["kind"] == "slow":
        time.sleep(float(store["delay_seconds"]))
    adapter = configuration["adapter"]
    assert isinstance(adapter, dict)
    kind = adapter["kind"]
    if kind == "unregistered":
        raise LabDaemonConfigurationError(str(adapter["message"]))
    _fixture_counter_increment(configuration["counter_path"])
    if kind == "failure":
        raise RuntimeError(str(adapter["message"]))
    if kind == "recording":
        time.sleep(float(adapter["delay_seconds"]))
        failure = adapter["failure"]
        if isinstance(failure, dict):
            raise RuntimeError(str(failure["message"]))
    elif kind == "slow":
        Path(str(adapter["path"])).write_text(str(os.getpid()), encoding="ascii")
        time.sleep(float(adapter["delay_seconds"]))
    elif kind == "spawn-method":
        Path(str(adapter["path"])).write_text(multiprocessing.get_start_method(), encoding="ascii")
    elif kind == "hung":
        Path(str(adapter["path"])).write_text(str(os.getpid()), encoding="ascii")
        threading.Event().wait()
    elif kind == "blocking":
        Path(str(adapter["entered_path"])).write_text("entered", encoding="ascii")
        _wait_fixture_release(adapter["release_path"])
    elif kind == "deadline":
        Path(str(adapter["path"])).write_text("reached", encoding="ascii")
    elif kind in {"sigterm-tree", "term-exit-tree", "successful-tree"}:
        if kind == "sigterm-tree":
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
        elif kind == "term-exit-tree":
            signal.signal(signal.SIGTERM, lambda *_args: os._exit(0))
        _spawn_fixture_descendant(adapter["descendant_path"])
        Path(str(adapter["path"])).write_text(str(os.getpid()), encoding="ascii")
        if kind != "successful-tree":
            threading.Event().wait()
    return LabShardExecutionResult.from_validated(
        validated,
        tables=(
            LabShardTable(
                name="trades",
                frame=pd.DataFrame(
                    [{"hold_days": getattr(validated.shard, "hold_days", 1), "ret_pct": 1.25}]
                ),
            ),
        ),
    )


def _worker(
    tmp_path: Path,
    *,
    worker_id: str = "worker-a",
    registry: RecordingRegistry | None = None,
    claims: LabClaimSpool | None = None,
    reports: LabReportSpool | None = None,
    claim_publication_verifier=None,
    v2_claim_publication_enabled: bool = False,
    heartbeat_interval_seconds: float = 60.0,
    resource_recheck_interval_seconds: float = 1.0,
    # Back to the original values. Raising them to 10.0 / 5.0 fixed one CI case
    # and broke four whose subject is the timeout they inherited, and scaling
    # them by the host has the same failure mode: a longer budget means the
    # recheck or timeout under test never fires inside the run. Only budgets
    # that *observe* are scaled; budgets that *gate behaviour* are not.
    resource_probe_timeout_seconds: float | None = None,
    lease_extension_seconds: int = 30,
    quarantine_reconcile_interval_seconds: float = 300.0,
    receipt_timeout_seconds: float | None = None,
    exploratory_store_factory=_store_factory,
    metadata_store_factory=None,
    lake_root: Path | None = None,
    receipt_waiter=_accept_report,
    verified_code_sha_provider=lambda: "1" * 40,
    resource_snapshot_provider=None,
    admission_policy_provider=None,
    source_quota_lease_provider=None,
    resource_reservation_store=None,
    require_resource_admission: bool = False,
    clock=lambda: NOW,
    monotonic_clock=time.monotonic,
    isolation_monotonic_clock=time.monotonic,
    isolation_session_initializer=None,
    execution_session_factory=None,
    research_store_opener=None,
    shard_runtime_manifest=None,
    resource_authority_manifest=None,
    production_mode: bool = False,
):
    from rquant.lab_worker import LabWorker

    chosen_registry = registry or RecordingRegistry()
    closed_shard_manifest = shard_runtime_manifest or _test_shard_manifest(
        tmp_path,
        registry=chosen_registry,
        exploratory_store_factory=exploratory_store_factory,
        metadata_store_factory=metadata_store_factory,
        lake_root=lake_root,
        isolation_session_initializer=isolation_session_initializer,
        execution_session_factory=execution_session_factory,
        research_store_opener=research_store_opener,
    )
    closed_authority_manifest = resource_authority_manifest
    if closed_authority_manifest is None and (
        resource_snapshot_provider is not None or admission_policy_provider is not None
    ):
        if resource_snapshot_provider is None or admission_policy_provider is None:
            raise LabDaemonConfigurationError(
                "test resource authority providers must be configured together"
            )
        closed_authority_manifest = _test_authority_manifest(
            tmp_path,
            snapshot_provider=resource_snapshot_provider,
            policy_provider=admission_policy_provider,
            quota_provider=source_quota_lease_provider,
        )
    return LabWorker(
        worker_id=worker_id,
        claim_spool=claims or LabClaimSpool(tmp_path / "claims"),
        claim_publication_verifier=claim_publication_verifier,
        v2_claim_publication_enabled=v2_claim_publication_enabled,
        report_spool=reports or LabReportSpool(tmp_path / "reports"),
        artifact_root=tmp_path / "artifacts",
        adapter_registry=default_strategy_job_adapter_registry(),
        heartbeat_interval_seconds=heartbeat_interval_seconds,
        resource_recheck_interval_seconds=resource_recheck_interval_seconds,
        resource_probe_timeout_seconds=(
            1.0 if resource_probe_timeout_seconds is None else resource_probe_timeout_seconds
        ),
        lease_extension_seconds=lease_extension_seconds,
        quarantine_reconcile_interval_seconds=quarantine_reconcile_interval_seconds,
        poll_interval_ms=5,
        receipt_timeout_seconds=(
            0.2 if receipt_timeout_seconds is None else receipt_timeout_seconds
        ),
        receipt_waiter=receipt_waiter,
        verified_code_sha_provider=verified_code_sha_provider,
        resource_authority_manifest=closed_authority_manifest,
        resource_reservation_store=resource_reservation_store,
        require_resource_admission=require_resource_admission,
        clock=clock,
        monotonic_clock=monotonic_clock,
        isolation_monotonic_clock=isolation_monotonic_clock,
        shard_runtime_manifest=closed_shard_manifest,
        production_mode=production_mode,
    )


def _healthy_resource_snapshot(*, observed_at: datetime = NOW, session: object = None):
    from rquant.resource_admission import ResourceSnapshot, TradingSession

    return ResourceSnapshot(
        observed_at=observed_at,
        session=TradingSession.POST_MARKET if session is None else session,
        live_backlog_age_seconds=0,
        live_p95_latency_seconds=0,
        available_memory_bytes=16 * 1024**3,
        available_disk_bytes=100 * 1024**3,
        io_pressure_pct=0,
        cpu_load_pct=0,
        source_quota_remaining=0,
        live_healthy=True,
    )


def _permissive_admission_policy(**overrides: object):
    from rquant.resource_admission import AdmissionPolicy

    payload: dict[str, object] = {
        "allow_live_session": True,
        "max_live_shard_duration_ms": 100,
        "max_snapshot_age_seconds": 5,
        "max_live_backlog_age_seconds": 10,
        "max_live_p95_latency_seconds": 5,
        "min_available_memory_bytes": 0,
        "min_available_disk_bytes": 0,
        "max_io_pressure_pct": 100,
        "max_cpu_load_pct": 100,
        "max_expected_memory_bytes": 8 * 1024**3,
        "max_expected_disk_bytes": 50 * 1024**3,
        "max_expected_quota_units": 0,
        "retry_delay_seconds": 60,
    }
    payload.update(overrides)
    if "max_snapshot_age_microseconds" in overrides:
        payload.pop("max_snapshot_age_seconds", None)
    if "max_live_backlog_age_microseconds" in overrides:
        payload.pop("max_live_backlog_age_seconds", None)
    if "max_live_p95_latency_microseconds" in overrides:
        payload.pop("max_live_p95_latency_seconds", None)
    return AdmissionPolicy(**payload)


def test_worker_rejects_boolean_resource_lease_duration(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="lease_extension_seconds"):
        _worker(tmp_path, lease_extension_seconds=True)


@pytest.mark.parametrize("value", (0, 1, "false"))
def test_worker_requires_canonical_resource_admission_flag(
    tmp_path: Path,
    value: object,
) -> None:
    with pytest.raises(ValueError, match="require_resource_admission"):
        _worker(tmp_path, require_resource_admission=value)


def test_worker_bounds_reservation_lock_wait_and_propagates_stop_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.lab_worker import (
        _RESOURCE_RESERVATION_LOCK_WAIT_MAX_MICROSECONDS,
        _microseconds_to_seconds,
    )
    from rquant.runtime_resource_admission import SQLiteResourceReservationStore

    reservation_lock_bound = _microseconds_to_seconds(
        _RESOURCE_RESERVATION_LOCK_WAIT_MAX_MICROSECONDS
    )

    spec = _nshape_compare_spec(hold_days=(1,))
    claim = _short_claim_for_spec(spec)
    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    claims.publish(claim)
    store = SQLiteResourceReservationStore(
        tmp_path / "resource-reservations.sqlite3",
        clock=lambda: NOW,
    )
    original_reserve = store.reserve
    original_recheck = store.recheck
    original_release = store.release
    observed: list[tuple[str, float, bool]] = []
    released: list[float] = []

    def checked_reserve(**kwargs: object):
        stop_requested = kwargs["stop_requested"]
        timeout = kwargs["lock_wait_timeout_seconds"]
        assert callable(stop_requested)
        assert isinstance(timeout, float)
        observed.append(("reserve", timeout, stop_requested()))
        return original_reserve(**kwargs)

    def checked_recheck(**kwargs: object):
        stop_requested = kwargs["stop_requested"]
        timeout = kwargs["lock_wait_timeout_seconds"]
        assert callable(stop_requested)
        assert isinstance(timeout, float)
        observed.append(("recheck", timeout, stop_requested()))
        return original_recheck(**kwargs)

    def checked_release(*args: object, **kwargs: object):
        timeout = kwargs["lock_wait_timeout_seconds"]
        assert isinstance(timeout, float)
        released.append(timeout)
        return original_release(*args, **kwargs)

    monkeypatch.setattr(store, "reserve", checked_reserve)
    monkeypatch.setattr(store, "recheck", checked_recheck)
    monkeypatch.setattr(store, "release", checked_release)
    worker = _worker(
        tmp_path,
        registry=SlowPidRegistry(
            pid_path=tmp_path / "bounded-lock-wait-child.pid",
            delay_seconds=0.12,
        ),
        claims=claims,
        reports=reports,
        resource_recheck_interval_seconds=0.02,
        resource_snapshot_provider=StaticResourceSnapshotProvider(_healthy_resource_snapshot()),
        admission_policy_provider=StaticAdmissionPolicyProvider(_permissive_admission_policy()),
        resource_reservation_store=store,
        require_resource_admission=True,
    )

    result = worker.run_once()

    assert result.status == "succeeded"
    assert {operation for operation, _timeout, _stopped in observed} == {"reserve", "recheck"}
    # Every reservation call is capped by the store's own lock bound and then
    # narrowed again by whatever is left of the tick and spec deadlines.
    assert all(0 < timeout <= reservation_lock_bound for _operation, timeout, _stopped in observed)
    assert all(not stopped for _operation, _timeout, stopped in observed)
    # The release path has no caller deadline to narrow it, so it carries the
    # bound itself - which issue #159 left as a bare 50ms literal.
    assert released == [pytest.approx(reservation_lock_bound)]
    assert reservation_lock_bound > 0.05


@pytest.mark.parametrize("refusal", ("contention", "contract"))
def test_worker_separates_reservation_contention_from_configuration_faults(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    refusal: str,
) -> None:
    """A lost race must not read as a permanent configuration fault.

    `run_forever` has no handler above `run_once`, so folding transient
    contention into `LabDaemonConfigurationError` took the worker down for the
    duration of somebody else's commit (issue #159).  A real contract failure
    still has to end the same way it always did.
    """
    from rquant.runtime_resource_admission import (
        RuntimeResourceAdmissionError,
        RuntimeResourceAdmissionLockWaitTimeoutError,
        SQLiteResourceReservationStore,
    )

    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    claims.publish(claim)
    registry = RecordingRegistry()
    store = SQLiteResourceReservationStore(
        tmp_path / "resource-reservations.sqlite3",
        clock=lambda: NOW,
    )

    def refusing_reserve(**_kwargs: object):
        if refusal == "contention":
            raise RuntimeResourceAdmissionLockWaitTimeoutError(
                "resource reservation lock wait timeout"
            )
        raise RuntimeResourceAdmissionError("resource reservation schema identity mismatch")

    monkeypatch.setattr(store, "reserve", refusing_reserve)
    worker = _worker(
        tmp_path,
        claims=claims,
        reports=reports,
        registry=registry,
        resource_snapshot_provider=StaticResourceSnapshotProvider(_healthy_resource_snapshot()),
        admission_policy_provider=StaticAdmissionPolicyProvider(_permissive_admission_policy()),
        resource_reservation_store=store,
        require_resource_admission=True,
    )

    if refusal == "contract":
        with pytest.raises(LabDaemonConfigurationError, match="schema identity mismatch"):
            worker.run_once()
        assert registry.executions == 0
        return

    result = worker.run_once()

    assert result.status == "idle"
    assert registry.executions == 0
    assert store.active_leases() == ()
    assert worker._active_resource_reservation is None
    # The claim survives the contended tick and is backed off, not consumed.
    assert tuple(entry.claim for entry in claims.pending()) == (claim,)
    assert worker._resource_retry_at[claim.claim_token] > NOW
    assert reports.pending() == ()


def test_worker_backs_off_when_the_real_authority_socket_is_contended(
    tmp_path: Path,
) -> None:
    """Production reserves through the socket adapter, not a direct store.

    A lost race inside the remote authority used to come back as
    `ResourceAuthorityAdapterRemoteError` and got folded into
    `LabDaemonConfigurationError`, so a competitor's commit still took the
    worker down through the topology that actually ships (RQ-CTB-P1-01).
    """
    from rquant.lab_resource_authority_adapter import LabResourceAuthorityReservationAdapter
    from tests.unit.test_lab_resource_authority_adapter import _LockingPolicyProvider, _Server

    authority_root = tmp_path / "authority"
    authority_root.mkdir(mode=0o700)
    gate = _LockingPolicyProvider(authority_root / "resource.sqlite3")
    server = _Server(authority_root, timeout_milliseconds=5_000, policy_provider=gate)
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    claims.publish(claim)
    registry = RecordingRegistry()
    try:
        worker = _worker(
            tmp_path,
            claims=claims,
            reports=reports,
            registry=registry,
            resource_snapshot_provider=StaticResourceSnapshotProvider(_healthy_resource_snapshot()),
            admission_policy_provider=StaticAdmissionPolicyProvider(_permissive_admission_policy()),
            resource_reservation_store=LabResourceAuthorityReservationAdapter(server.configuration),
            require_resource_admission=True,
        )
        gate.armed = True

        result = worker.run_once()

        assert gate.locked.is_set()
        assert result.status == "idle"
        assert registry.executions == 0
        assert worker._active_resource_reservation is None
        # The claim survives the contended tick and is backed off, not consumed.
        assert tuple(entry.claim for entry in claims.pending()) == (claim,)
        assert worker._resource_retry_at[claim.claim_token] > NOW
        assert reports.pending() == ()
    finally:
        gate.close()
        server.close()


def test_worker_recheck_contention_fails_the_shard_without_a_configuration_fault(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Mid-execution contention is a shard outcome, not a broken deployment.

    The recheck dispatch folded the same transient refusal into
    `LabDaemonConfigurationError`, and that one is re-raised out of the tick
    (`lab_worker.py`'s resource-error triage), so a lost race during execution
    also killed the worker.
    """
    from rquant.runtime_resource_admission import (
        RuntimeResourceAdmissionLockWaitTimeoutError,
        SQLiteResourceReservationStore,
    )

    spec = _nshape_compare_spec(hold_days=(1,))
    claim = _short_claim_for_spec(spec)
    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    claims.publish(claim)
    store = SQLiteResourceReservationStore(
        tmp_path / "resource-reservations.sqlite3",
        clock=lambda: NOW,
    )
    rechecks: list[int] = []

    def contended_recheck(**_kwargs: object):
        rechecks.append(1)
        raise RuntimeResourceAdmissionLockWaitTimeoutError("resource reservation lock wait timeout")

    monkeypatch.setattr(store, "recheck", contended_recheck)
    worker = _worker(
        tmp_path,
        registry=SlowPidRegistry(
            pid_path=tmp_path / "contended-recheck-child.pid",
            delay_seconds=0.12,
        ),
        claims=claims,
        reports=reports,
        resource_recheck_interval_seconds=0.02,
        resource_snapshot_provider=StaticResourceSnapshotProvider(_healthy_resource_snapshot()),
        admission_policy_provider=StaticAdmissionPolicyProvider(_permissive_admission_policy()),
        resource_reservation_store=store,
        require_resource_admission=True,
    )

    result = worker.run_once()

    assert rechecks
    assert result.status == "failed"
    assert store.active_leases() == ()
    assert worker._active_resource_reservation is None


def test_initial_resource_rejection_is_a_hard_gate_before_adapter_execution(
    tmp_path: Path,
) -> None:
    from rquant.runtime_resource_admission import SQLiteResourceReservationStore

    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    child_pid_path = tmp_path / "gated-child.pid"
    registry = RecordingRegistry()
    store = SQLiteResourceReservationStore(
        tmp_path / "resource-reservations.sqlite3",
        clock=lambda: NOW,
    )
    policy_provider = SequenceAdmissionPolicyProvider(
        _permissive_admission_policy(),
        _permissive_admission_policy(min_available_memory_bytes=32 * 1024**3),
    )
    claims.publish(claim)
    worker = _worker(
        tmp_path,
        claims=claims,
        reports=reports,
        registry=registry,
        resource_recheck_interval_seconds=0.01,
        resource_snapshot_provider=StaticResourceSnapshotProvider(_healthy_resource_snapshot()),
        admission_policy_provider=policy_provider,
        resource_reservation_store=store,
        require_resource_admission=True,
        isolation_session_initializer=RecordingSessionInitializer(child_pid_path),
    )

    result = worker.run_once()

    assert result.status == "stopped"
    assert policy_provider.calls >= 2
    assert registry.executions == 0
    assert child_pid_path.exists()
    _assert_process_gone(int(child_pid_path.read_text(encoding="ascii")))
    assert store.active_leases() == ()


def test_top_level_static_policy_provider_passes_adapter_ack_gate(
    tmp_path: Path,
) -> None:
    from rquant.runtime_resource_admission import SQLiteResourceReservationStore

    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims = LabClaimSpool(tmp_path / "claims")
    registry = RecordingRegistry()
    store = SQLiteResourceReservationStore(
        tmp_path / "static-policy-reservations.sqlite3",
        clock=lambda: NOW,
    )
    claims.publish(claim)
    worker = _worker(
        tmp_path,
        claims=claims,
        registry=registry,
        resource_recheck_interval_seconds=0.01,
        resource_snapshot_provider=StaticResourceSnapshotProvider(_healthy_resource_snapshot()),
        admission_policy_provider=StaticAdmissionPolicyProvider(_permissive_admission_policy()),
        resource_reservation_store=store,
        require_resource_admission=True,
    )

    result = worker.run_once()

    assert result.status == "succeeded"
    assert registry.executions == 1
    assert store.active_leases() == ()


@pytest.mark.parametrize(
    "provider_kind",
    ("lambda", "local-function", "forged-frozen-version"),
)
def test_unspawnable_dynamic_policy_fails_closed_before_first_authority_evaluation(
    tmp_path: Path,
    provider_kind: str,
) -> None:
    from rquant.runtime_resource_admission import SQLiteResourceReservationStore

    allow = _permissive_admission_policy()
    deny = _permissive_admission_policy(min_available_memory_bytes=32 * 1024**3)
    calls = [0]

    def next_policy(_spec: ResearchRunSpec) -> object:
        calls[0] += 1
        return allow if calls[0] == 1 else deny

    def lambda_policy_provider() -> object:
        return lambda spec: next_policy(spec)

    provider = lambda_policy_provider() if provider_kind == "lambda" else next_policy
    if provider_kind == "forged-frozen-version":
        provider.__dict__.update(
            frozen_authority_version="f" * 64,
            immutable_authority=True,
        )

    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims = LabClaimSpool(tmp_path / "claims")
    registry = RecordingRegistry()
    store = SQLiteResourceReservationStore(
        tmp_path / f"unspawnable-policy-{provider_kind}.sqlite3",
        clock=lambda: NOW,
    )
    claims.publish(claim)
    worker = _worker(
        tmp_path,
        claims=claims,
        registry=registry,
        resource_recheck_interval_seconds=0.01,
        resource_snapshot_provider=StaticResourceSnapshotProvider(_healthy_resource_snapshot()),
        admission_policy_provider=provider,
        resource_reservation_store=store,
        require_resource_admission=True,
    )

    with pytest.raises(LabDaemonConfigurationError, match="spawn-serializable"):
        worker.run_once()

    assert calls == [0]
    assert registry.executions == 0
    assert store.active_leases() == ()
    with worker._managed_authority_children_lock:
        assert worker._managed_authority_children == {}
    assert all(
        child.name != "lab-resource-authority" for child in multiprocessing.active_children()
    )


@pytest.mark.parametrize("provider_kind", ("lambda", "local-function"))
def test_unspawnable_dynamic_quota_fails_closed_before_first_authority_evaluation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    provider_kind: str,
) -> None:
    import rquant.lab_worker as lab_worker
    from rquant.resource_admission import SourceQuotaLease
    from rquant.runtime_resource_admission import SQLiteResourceReservationStore

    original_derive = lab_worker.derive_lab_admission_request

    def derive_positive_quota_request(**kwargs: object):
        request = original_derive(**kwargs)
        return request.model_copy(update={"expected_quota_units": 1, "source": "test-source"})

    monkeypatch.setattr(
        lab_worker,
        "derive_lab_admission_request",
        derive_positive_quota_request,
    )
    calls = [0]

    def next_quota(request: object, _snapshot: object) -> object:
        calls[0] += 1
        if calls[0] > 1:
            return None
        return SourceQuotaLease(
            source="test-source",
            owner=request.job_id,
            units=1,
            granted_at=NOW,
            expires_at=NOW + timedelta(minutes=1),
            quota_reset_at=NOW + timedelta(minutes=2),
        )

    def lambda_quota_provider() -> object:
        return lambda request, snapshot: next_quota(request, snapshot)

    provider = lambda_quota_provider() if provider_kind == "lambda" else next_quota

    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims = LabClaimSpool(tmp_path / "claims")
    registry = RecordingRegistry()
    store = SQLiteResourceReservationStore(
        tmp_path / f"unspawnable-quota-{provider_kind}.sqlite3",
        clock=lambda: NOW,
    )
    claims.publish(claim)
    worker = _worker(
        tmp_path,
        claims=claims,
        registry=registry,
        resource_recheck_interval_seconds=0.01,
        resource_snapshot_provider=StaticResourceSnapshotProvider(
            _healthy_resource_snapshot().model_copy(update={"source_quota_remaining": 10})
        ),
        admission_policy_provider=StaticAdmissionPolicyProvider(
            _permissive_admission_policy(max_expected_quota_units=1)
        ),
        source_quota_lease_provider=provider,
        resource_reservation_store=store,
        require_resource_admission=True,
    )

    with pytest.raises(LabDaemonConfigurationError, match="spawn-serializable"):
        worker.run_once()

    assert calls == [0]
    assert registry.executions == 0
    assert store.active_leases() == ()


@pytest.mark.parametrize("slot", ("policy", "snapshot", "quota", "adapter"))
@pytest.mark.parametrize("termination", ("deadline", "stop"))
def test_parent_never_reduces_unregistered_authority_or_adapter_objects(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    slot: str,
    termination: str,
) -> None:
    import rquant.lab_worker as lab_worker
    from rquant.runtime_resource_admission import SQLiteResourceReservationStore

    original_derive = lab_worker.derive_lab_admission_request

    def derive_request(**kwargs: object):
        request = original_derive(**kwargs)
        if slot != "quota":
            return request
        return request.model_copy(update={"expected_quota_units": 1, "source": "test-source"})

    monkeypatch.setattr(lab_worker, "derive_lab_admission_request", derive_request)
    reduce_marker = tmp_path / f"{slot}-{termination}.reduce"
    execution_marker = tmp_path / f"{slot}-{termination}.execute"
    trap = ParentReduceTrap(
        reduce_marker=reduce_marker,
        execution_marker=execution_marker,
    )
    spec = _nshape_compare_spec(hold_days=(1,)).model_copy(
        update={"deadline": NOW + timedelta(milliseconds=800)}
    )
    claim = _claim(spec)
    claims = LabClaimSpool(tmp_path / "claims")
    registry = RecordingRegistry()
    store = SQLiteResourceReservationStore(
        tmp_path / f"{slot}-{termination}.sqlite3",
        clock=lambda: NOW,
    )
    claims.publish(claim)
    worker = _worker(
        tmp_path,
        claims=claims,
        registry=trap if slot == "adapter" else registry,
        resource_snapshot_provider=(
            trap
            if slot == "snapshot"
            else StaticResourceSnapshotProvider(
                _healthy_resource_snapshot().model_copy(update={"source_quota_remaining": 10})
            )
        ),
        admission_policy_provider=(
            trap
            if slot == "policy"
            else StaticAdmissionPolicyProvider(
                _permissive_admission_policy(max_expected_quota_units=1)
            )
        ),
        source_quota_lease_provider=trap if slot == "quota" else None,
        resource_reservation_store=store,
        require_resource_admission=True,
    )
    stopper: threading.Thread | None = None
    if termination == "stop":
        stopper = threading.Thread(
            target=lambda: (time.sleep(0.02), worker.request_stop()),
            name="lab-malicious-reduce-stop",
        )
        stopper.start()

    started = time.monotonic()
    try:
        with suppress(LabDaemonConfigurationError):
            worker.run_once()
    finally:
        if stopper is not None:
            stopper.join(timeout=_observe(1))
    elapsed = time.monotonic() - started

    assert elapsed < _observe(1.5)
    assert not reduce_marker.exists()
    assert not execution_marker.exists()
    assert registry.executions == 0
    assert store.active_leases() == ()


def test_child_result_with_blocking_reduce_is_never_pickled_and_remains_bounded(
    tmp_path: Path,
) -> None:
    reduce_marker = tmp_path / "malicious-result.reduce"
    registry = MaliciousResultRegistry(reduce_marker=reduce_marker)
    # The deadline is what bounds the blocking __reduce__, so the shard child
    # has to be up and executing before it fires; eight hundred milliseconds is
    # less than one child start-up on a slow host and the tick came back
    # 'stopped' from the deadline instead of 'failed' from the adversarial
    # result.
    spec_deadline = _deadline_reachable_in(1)
    spec = _nshape_compare_spec(hold_days=(1,)).model_copy(update={"deadline": NOW + spec_deadline})
    claims = LabClaimSpool(tmp_path / "claims")
    claims.publish(_claim(spec))
    worker = _worker(tmp_path, claims=claims, registry=registry)

    started = time.monotonic()
    result = worker.run_once()
    elapsed = time.monotonic() - started

    assert result.status == "failed"
    assert elapsed < spec_deadline.total_seconds() + _observe(0.7)
    assert registry.executions == 1
    assert not reduce_marker.exists()


def test_first_policy_callback_stop_is_bounded_before_reservation(
    tmp_path: Path,
) -> None:
    from rquant.runtime_resource_admission import SQLiteResourceReservationStore

    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims = LabClaimSpool(tmp_path / "claims")
    registry = RecordingRegistry()
    entered_path = tmp_path / "first-policy.entered"
    authority_pid_path = tmp_path / "first-policy.pid"
    release_path = tmp_path / "first-policy.release"
    store = SQLiteResourceReservationStore(
        tmp_path / "first-policy-reservations.sqlite3",
        clock=lambda: NOW,
    )
    claims.publish(claim)
    worker = _worker(
        tmp_path,
        claims=claims,
        registry=registry,
        resource_probe_timeout_seconds=3,
        resource_snapshot_provider=StaticResourceSnapshotProvider(_healthy_resource_snapshot()),
        admission_policy_provider=BlockingInitialAdmissionPolicyProvider(
            _permissive_admission_policy(),
            entered_path=entered_path,
            pid_path=authority_pid_path,
            release_path=release_path,
        ),
        resource_reservation_store=store,
        require_resource_admission=True,
    )
    outcomes: list[object] = []
    failures: list[BaseException] = []

    def run_worker() -> None:
        try:
            outcomes.append(worker.run_once())
        except BaseException as exc:
            failures.append(exc)

    runner = threading.Thread(target=run_worker)
    runner.start()
    try:
        entered_deadline = time.monotonic() + _child_startups(2)
        while not entered_path.exists() and time.monotonic() < entered_deadline:
            time.sleep(0.01)
        assert entered_path.exists()
        assert int(authority_pid_path.read_text(encoding="ascii")) != os.getpid()
        started = time.monotonic()
        worker.request_stop()
        runner.join(timeout=_observe(1.1))
        bounded = not runner.is_alive()
        elapsed = time.monotonic() - started
    finally:
        release_path.write_text("release", encoding="ascii")
        worker.request_stop()
        runner.join(timeout=_observe(2))

    assert bounded
    assert elapsed < _observe(1.1)
    assert failures == []
    assert outcomes[0].status == "stopped"
    assert registry.executions == 0
    _assert_process_gone(int(authority_pid_path.read_text(encoding="ascii")))
    assert store.active_leases() == ()


def test_first_policy_callback_obeys_100ms_deadline_before_reservation(
    tmp_path: Path,
) -> None:
    from rquant.runtime_resource_admission import SQLiteResourceReservationStore

    spec = _nshape_compare_spec(hold_days=(1,)).model_copy(
        update={"deadline": NOW + timedelta(milliseconds=100)}
    )
    claim = _claim(spec)
    claims = LabClaimSpool(tmp_path / "claims")
    registry = RecordingRegistry()
    release_path = tmp_path / "deadline-policy.release"
    store = SQLiteResourceReservationStore(
        tmp_path / "deadline-policy-reservations.sqlite3",
        clock=lambda: NOW,
    )
    claims.publish(claim)
    worker = _worker(
        tmp_path,
        claims=claims,
        registry=registry,
        resource_probe_timeout_seconds=3,
        resource_snapshot_provider=StaticResourceSnapshotProvider(_healthy_resource_snapshot()),
        admission_policy_provider=BlockingInitialAdmissionPolicyProvider(
            _permissive_admission_policy(),
            entered_path=tmp_path / "deadline-policy.entered",
            pid_path=tmp_path / "deadline-policy.pid",
            release_path=release_path,
        ),
        resource_reservation_store=store,
        require_resource_admission=True,
    )
    outcomes: list[object] = []
    failures: list[BaseException] = []

    def run_worker() -> None:
        try:
            outcomes.append(worker.run_once())
        except BaseException as exc:
            failures.append(exc)

    started = time.monotonic()
    runner = threading.Thread(target=run_worker)
    runner.start()
    runner.join(timeout=_observe(1.2))
    bounded = not runner.is_alive()
    elapsed = time.monotonic() - started
    release_path.write_text("release", encoding="ascii")
    worker.request_stop()
    runner.join(timeout=_observe(2))

    assert bounded
    assert elapsed < _observe(1.2)
    assert failures == []
    assert outcomes[0].status == "stopped"
    assert registry.executions == 0
    assert store.active_leases() == ()


def test_first_quota_callback_does_not_hold_sqlite_write_lock_and_stop_is_bounded(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.lab_worker as lab_worker
    from rquant.runtime_resource_admission import SQLiteResourceReservationStore

    original_derive = lab_worker.derive_lab_admission_request

    def derive_positive_quota_request(**kwargs: object) -> object:
        request = original_derive(**kwargs)
        return request.model_copy(update={"expected_quota_units": 1, "source": "test-source"})

    monkeypatch.setattr(
        lab_worker,
        "derive_lab_admission_request",
        derive_positive_quota_request,
    )
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims = LabClaimSpool(tmp_path / "claims")
    registry = RecordingRegistry()
    entered_path = tmp_path / "first-quota.entered"
    authority_pid_path = tmp_path / "first-quota.pid"
    release_path = tmp_path / "first-quota.release"
    store = SQLiteResourceReservationStore(
        tmp_path / "first-quota-reservations.sqlite3",
        clock=lambda: NOW,
    )
    claims.publish(claim)
    worker = _worker(
        tmp_path,
        claims=claims,
        registry=registry,
        resource_probe_timeout_seconds=3,
        resource_snapshot_provider=StaticResourceSnapshotProvider(
            _healthy_resource_snapshot().model_copy(update={"source_quota_remaining": 10})
        ),
        admission_policy_provider=StaticAdmissionPolicyProvider(
            _permissive_admission_policy(max_expected_quota_units=1)
        ),
        source_quota_lease_provider=BlockingInitialSourceQuotaLeaseProvider(
            entered_path=entered_path,
            pid_path=authority_pid_path,
            release_path=release_path,
        ),
        resource_reservation_store=store,
        require_resource_admission=True,
    )
    outcomes: list[object] = []
    failures: list[BaseException] = []

    def run_worker() -> None:
        try:
            outcomes.append(worker.run_once())
        except BaseException as exc:
            failures.append(exc)

    runner = threading.Thread(target=run_worker)
    runner.start()
    try:
        entered_deadline = time.monotonic() + _child_startups(2)
        while not entered_path.exists() and time.monotonic() < entered_deadline:
            time.sleep(0.01)
        assert entered_path.exists()
        assert int(authority_pid_path.read_text(encoding="ascii")) != os.getpid()
        with sqlite3.connect(store.path, timeout=0.1, isolation_level=None) as connection:
            assert connection.execute("SELECT COUNT(*) FROM resource_reservation").fetchone() == (
                0,
            )
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                "UPDATE resource_reservation_authority "
                "SET last_clock_at = last_clock_at WHERE singleton = 1"
            )
            connection.execute("ROLLBACK")
        started = time.monotonic()
        worker.request_stop()
        runner.join(timeout=_observe(1.1))
        bounded = not runner.is_alive()
        elapsed = time.monotonic() - started
    finally:
        release_path.write_text("release", encoding="ascii")
        worker.request_stop()
        runner.join(timeout=_observe(2))

    assert bounded
    assert elapsed < _observe(1.1)
    assert failures == []
    assert outcomes[0].status == "stopped"
    assert registry.executions == 0
    _assert_process_gone(int(authority_pid_path.read_text(encoding="ascii")))
    assert store.active_leases() == ()


def test_first_policy_callback_recursive_child_is_killed_and_reaped_on_stop(
    tmp_path: Path,
) -> None:
    from rquant.runtime_resource_admission import SQLiteResourceReservationStore

    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims = LabClaimSpool(tmp_path / "claims")
    registry = RecordingRegistry()
    authority_pid_path = tmp_path / "recursive-policy-authority.pid"
    descendant_pid_path = tmp_path / "recursive-policy-descendant.pid"
    release_path = tmp_path / "recursive-policy.release"
    store = SQLiteResourceReservationStore(
        tmp_path / "recursive-policy-reservations.sqlite3",
        clock=lambda: NOW,
    )
    claims.publish(claim)
    worker = _worker(
        tmp_path,
        claims=claims,
        registry=registry,
        resource_probe_timeout_seconds=3,
        resource_snapshot_provider=StaticResourceSnapshotProvider(_healthy_resource_snapshot()),
        admission_policy_provider=SpawnDescendantBlockingAdmissionPolicyProvider(
            _permissive_admission_policy(),
            authority_pid_path=authority_pid_path,
            descendant_pid_path=descendant_pid_path,
            release_path=release_path,
        ),
        resource_reservation_store=store,
        require_resource_admission=True,
    )
    outcomes: list[object] = []
    failures: list[BaseException] = []

    def run_worker() -> None:
        try:
            outcomes.append(worker.run_once())
        except BaseException as exc:
            failures.append(exc)

    runner = threading.Thread(target=run_worker)
    runner.start()
    try:
        entered_deadline = time.monotonic() + _child_startups(2)
        while (
            not authority_pid_path.exists() or not descendant_pid_path.exists()
        ) and time.monotonic() < entered_deadline:
            time.sleep(0.01)
        assert authority_pid_path.exists()
        assert descendant_pid_path.exists()
        started = time.monotonic()
        worker.request_stop()
        runner.join(timeout=_observe(1.2))
        bounded = not runner.is_alive()
        elapsed = time.monotonic() - started
    finally:
        release_path.write_text("release", encoding="ascii")
        worker.request_stop()
        runner.join(timeout=_observe(2))

    assert bounded
    assert elapsed < _observe(1.2)
    assert failures == []
    assert outcomes[0].status == "stopped"
    assert registry.executions == 0
    authority_pid = int(authority_pid_path.read_text(encoding="ascii"))
    descendant_pid = int(descendant_pid_path.read_text(encoding="ascii"))
    assert authority_pid != os.getpid()
    _assert_process_gone(authority_pid)
    _assert_process_gone(descendant_pid)
    assert store.active_leases() == ()


def test_first_policy_callback_exception_fails_closed_without_reservation(
    tmp_path: Path,
) -> None:
    from rquant.runtime_resource_admission import SQLiteResourceReservationStore

    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims = LabClaimSpool(tmp_path / "claims")
    registry = RecordingRegistry()
    store = SQLiteResourceReservationStore(
        tmp_path / "failing-policy-reservations.sqlite3",
        clock=lambda: NOW,
    )
    claims.publish(claim)
    worker = _worker(
        tmp_path,
        claims=claims,
        registry=registry,
        resource_snapshot_provider=StaticResourceSnapshotProvider(_healthy_resource_snapshot()),
        admission_policy_provider=FailingInitialAdmissionPolicyProvider(),
        resource_reservation_store=store,
        require_resource_admission=True,
    )

    with pytest.raises(LabDaemonConfigurationError, match="admission policy provider failed"):
        worker.run_once()

    assert registry.executions == 0
    assert store.active_leases() == ()


def test_first_quota_callback_exception_fails_closed_without_reservation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.lab_worker as lab_worker
    from rquant.runtime_resource_admission import SQLiteResourceReservationStore

    original_derive = lab_worker.derive_lab_admission_request

    def derive_positive_quota_request(**kwargs: object) -> object:
        request = original_derive(**kwargs)
        return request.model_copy(update={"expected_quota_units": 1, "source": "test-source"})

    monkeypatch.setattr(
        lab_worker,
        "derive_lab_admission_request",
        derive_positive_quota_request,
    )
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims = LabClaimSpool(tmp_path / "claims")
    registry = RecordingRegistry()
    store = SQLiteResourceReservationStore(
        tmp_path / "failing-quota-reservations.sqlite3",
        clock=lambda: NOW,
    )
    claims.publish(claim)
    worker = _worker(
        tmp_path,
        claims=claims,
        registry=registry,
        resource_probe_timeout_seconds=3,
        resource_snapshot_provider=StaticResourceSnapshotProvider(
            _healthy_resource_snapshot().model_copy(update={"source_quota_remaining": 10})
        ),
        admission_policy_provider=StaticAdmissionPolicyProvider(
            _permissive_admission_policy(max_expected_quota_units=1)
        ),
        source_quota_lease_provider=FailingInitialSourceQuotaLeaseProvider(),
        resource_reservation_store=store,
        require_resource_admission=True,
    )

    with pytest.raises(LabDaemonConfigurationError, match="source quota lease provider failed"):
        worker.run_once()

    assert registry.executions == 0
    assert store.active_leases() == ()
    with worker._managed_authority_children_lock:
        assert worker._managed_authority_children == {}
    assert all(
        child.name != "lab-resource-authority" for child in multiprocessing.active_children()
    )


@pytest.mark.parametrize("termination", ("stop", "deadline"))
def test_blocked_initial_policy_gate_is_bounded_and_never_executes_adapter(
    tmp_path: Path,
    termination: str,
) -> None:
    from rquant.runtime_resource_admission import SQLiteResourceReservationStore

    spec = _nshape_compare_spec(hold_days=(1,))
    # Reaching the blocked policy callback costs three child start-ups - the
    # admission authority round trip, the shard child, and the recheck that
    # finally blocks - so the deadline mode has to leave room for all three
    # before it can bound anything. Two flat seconds was enough here and cut
    # the run short on a slower host, where the callback marker this case waits
    # for never appeared at all.
    spec_deadline = _deadline_reachable_in(3)
    if termination == "deadline":
        spec = spec.model_copy(update={"deadline": NOW + spec_deadline})
    claim = _short_claim_for_spec(spec)
    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    child_pid_path = tmp_path / f"blocked-policy-{termination}-child.pid"
    callback_entered_path = tmp_path / f"blocked-policy-{termination}.entered"
    callback_release_path = tmp_path / f"blocked-policy-{termination}.release"
    registry = PlanBypassRecordingRegistry()
    store = SQLiteResourceReservationStore(
        tmp_path / f"resource-reservations-{termination}.sqlite3",
        clock=lambda: NOW,
    )
    policy_provider = BlockingSecondAdmissionPolicyProvider(
        _permissive_admission_policy(),
        entered_path=callback_entered_path,
        release_path=callback_release_path,
    )
    claims.publish(claim)
    worker = _worker(
        tmp_path,
        claims=claims,
        reports=reports,
        registry=registry,
        resource_recheck_interval_seconds=0.01,
        resource_probe_timeout_seconds=3,
        resource_snapshot_provider=StaticResourceSnapshotProvider(_healthy_resource_snapshot()),
        admission_policy_provider=policy_provider,
        resource_reservation_store=store,
        require_resource_admission=True,
        isolation_session_initializer=RecordingSessionInitializer(child_pid_path),
    )
    outcomes: list[object] = []
    failures: list[BaseException] = []

    def run_worker() -> None:
        try:
            outcomes.append(worker.run_once())
        except BaseException as exc:
            failures.append(exc)

    runner = threading.Thread(target=run_worker)
    runner.start()
    try:
        _await_markers(
            callback_entered_path,
            child_pid_path,
            budget=_child_startups(3),
        )
        started = time.monotonic()
        if termination == "stop":
            worker.request_stop()
        termination_budget = _observe(2.3) + (
            0.0 if termination == "stop" else spec_deadline.total_seconds()
        )
        runner.join(timeout=termination_budget)
        bounded = not runner.is_alive()
        elapsed = time.monotonic() - started
    finally:
        worker.request_stop()
        callback_release_path.write_text("release", encoding="ascii")
        runner.join(timeout=_observe(2))

    assert bounded
    assert elapsed < termination_budget
    assert failures == []
    assert outcomes[0].status == "stopped"
    assert registry.executions == 0
    assert child_pid_path.exists()
    _assert_process_gone(int(child_pid_path.read_text(encoding="ascii")))
    assert store.active_leases() == ()


def test_blocked_initial_quota_gate_stop_reaps_authority_child_and_lease(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.lab_worker as lab_worker
    from rquant.runtime_resource_admission import SQLiteResourceReservationStore

    original_derive = lab_worker.derive_lab_admission_request

    def derive_positive_quota_request(**kwargs: object):
        request = original_derive(**kwargs)
        return request.model_copy(update={"expected_quota_units": 1, "source": "test-source"})

    monkeypatch.setattr(
        lab_worker,
        "derive_lab_admission_request",
        derive_positive_quota_request,
    )
    spec = _nshape_compare_spec(hold_days=(1,))
    claim = _short_claim_for_spec(spec)
    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    child_pid_path = tmp_path / "blocked-quota-child.pid"
    adapter_pid_path = tmp_path / "blocked-quota-adapter.pid"
    callback_entered_path = tmp_path / "blocked-quota.entered"
    callback_release_path = tmp_path / "blocked-quota.release"
    authority_pid_path = tmp_path / "blocked-quota-authority.pid"
    store = SQLiteResourceReservationStore(
        tmp_path / "blocked-quota-reservations.sqlite3",
        clock=lambda: NOW,
    )
    quota_provider = BlockingSecondSourceQuotaLeaseProvider(
        entered_path=callback_entered_path,
        release_path=callback_release_path,
        authority_pid_path=authority_pid_path,
    )
    claims.publish(claim)
    worker = _worker(
        tmp_path,
        claims=claims,
        reports=reports,
        registry=SlowPidRegistry(pid_path=adapter_pid_path, delay_seconds=1),
        resource_recheck_interval_seconds=0.01,
        resource_probe_timeout_seconds=3,
        resource_snapshot_provider=StaticResourceSnapshotProvider(
            _healthy_resource_snapshot().model_copy(update={"source_quota_remaining": 10})
        ),
        admission_policy_provider=StaticAdmissionPolicyProvider(
            _permissive_admission_policy(max_expected_quota_units=1)
        ),
        source_quota_lease_provider=quota_provider,
        resource_reservation_store=store,
        require_resource_admission=True,
        isolation_session_initializer=RecordingSessionInitializer(child_pid_path),
    )
    outcomes: list[object] = []
    failures: list[BaseException] = []

    def run_worker() -> None:
        try:
            outcomes.append(worker.run_once())
        except BaseException as exc:
            failures.append(exc)

    runner = threading.Thread(target=run_worker)
    runner.start()
    try:
        _await_markers(
            callback_entered_path,
            child_pid_path,
            authority_pid_path,
            budget=_child_startups(3),
        )
        started = time.monotonic()
        worker.request_stop()
        runner.join(timeout=_observe(1.1))
        bounded = not runner.is_alive()
        elapsed = time.monotonic() - started
    finally:
        worker.request_stop()
        callback_release_path.write_text("release", encoding="ascii")
        runner.join(timeout=_observe(2))

    assert bounded
    assert elapsed < _observe(1.1)
    assert failures == []
    assert outcomes[0].status == "stopped"
    assert not adapter_pid_path.exists()
    _assert_process_gone(int(child_pid_path.read_text(encoding="ascii")))
    _assert_process_gone(int(authority_pid_path.read_text(encoding="ascii")))
    assert store.active_leases() == ()
    active_authority_children = tuple(
        (child.name, child.pid, child.exitcode)
        for child in multiprocessing.active_children()
        if child.name == "lab-resource-authority"
    )
    assert active_authority_children == ()
    with worker._managed_authority_children_lock:
        assert worker._managed_authority_children == {}


def test_stop_cancels_locked_recheck_and_reaps_child_before_releasing_lease(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import sqlite3

    from rquant.runtime_resource_admission import SQLiteResourceReservationStore

    spec = _nshape_compare_spec(hold_days=(1,))
    claim = _short_claim_for_spec(spec)
    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    child_pid_path = tmp_path / "locked-recheck-child.pid"
    adapter_pid_path = tmp_path / "locked-recheck-adapter.pid"
    child_release_path = tmp_path / "locked-recheck-child.release"
    claims.publish(claim)
    database_path = tmp_path / "resource-reservations.sqlite3"
    store = SQLiteResourceReservationStore(database_path, clock=lambda: NOW)
    original_recheck = store.recheck
    recheck_entered = threading.Event()
    recheck_finished = threading.Event()

    def observed_recheck(**kwargs: object):
        recheck_entered.set()
        try:
            return original_recheck(**kwargs)
        finally:
            recheck_finished.set()

    monkeypatch.setattr(store, "recheck", observed_recheck)
    worker = _worker(
        tmp_path,
        registry=SlowPidRegistry(pid_path=adapter_pid_path, delay_seconds=1),
        claims=claims,
        reports=reports,
        resource_recheck_interval_seconds=0.2,
        resource_snapshot_provider=StaticResourceSnapshotProvider(_healthy_resource_snapshot()),
        admission_policy_provider=StaticAdmissionPolicyProvider(_permissive_admission_policy()),
        resource_reservation_store=store,
        require_resource_admission=True,
        isolation_session_initializer=BlockingRecordingSessionInitializer(
            child_pid_path,
            child_release_path,
        ),
    )
    outcomes: list[object] = []
    failures: list[BaseException] = []

    def run_worker() -> None:
        try:
            outcomes.append(worker.run_once())
        except BaseException as exc:
            failures.append(exc)

    runner = threading.Thread(target=run_worker)
    holder: sqlite3.Connection | None = None
    runner.start()
    try:
        pid_deadline = time.monotonic() + _child_startups(2)
        while not child_pid_path.exists() and time.monotonic() < pid_deadline:
            time.sleep(0.01)
        assert child_pid_path.exists()
        holder = sqlite3.connect(database_path, isolation_level=None)
        holder.execute("BEGIN IMMEDIATE")
        child_release_path.write_text("release", encoding="ascii")
        assert recheck_entered.wait(timeout=_observe(2))

        time.sleep(0.01)
        worker.request_stop()
        assert recheck_finished.wait(timeout=_observe(1))
        holder.rollback()
        holder.close()
        holder = None
        runner.join(timeout=_observe(3))
    finally:
        worker.request_stop()
        child_release_path.write_text("release", encoding="ascii")
        if holder is not None:
            holder.rollback()
            holder.close()
        runner.join(timeout=_observe(1))

    assert not runner.is_alive()
    assert failures == []
    assert outcomes[0].status == "stopped"
    assert not adapter_pid_path.exists()
    _assert_process_gone(int(child_pid_path.read_text(encoding="ascii")))
    assert store.active_leases() == ()


def test_resource_admission_defers_without_consuming_or_executing_claim(
    tmp_path: Path,
) -> None:
    from rquant.resource_admission import AdmissionPolicy, ResourceSnapshot, TradingSession

    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    registry = RecordingRegistry()
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims.publish(claim)
    snapshot = ResourceSnapshot(
        observed_at=NOW,
        session=TradingSession.MORNING,
        live_backlog_age_seconds=0,
        live_p95_latency_seconds=0,
        available_memory_bytes=16 * 1024**3,
        available_disk_bytes=100 * 1024**3,
        io_pressure_pct=0,
        cpu_load_pct=0,
        source_quota_remaining=0,
        live_healthy=True,
    )
    policy = AdmissionPolicy(
        allow_live_session=False,
        max_live_backlog_age_seconds=10,
        max_live_p95_latency_seconds=5,
        min_available_memory_bytes=512 * 1024**2,
        min_available_disk_bytes=1024**3,
        max_io_pressure_pct=80,
        max_cpu_load_pct=80,
        max_expected_memory_bytes=8 * 1024**3,
        max_expected_disk_bytes=50 * 1024**3,
        max_expected_quota_units=0,
        retry_delay_seconds=60,
    )

    worker = _worker(
        tmp_path,
        claims=claims,
        reports=reports,
        registry=registry,
        resource_snapshot_provider=StaticResourceSnapshotProvider(snapshot),
        admission_policy_provider=StaticAdmissionPolicyProvider(policy),
        require_resource_admission=True,
    )

    result = worker.run_once()

    assert result.status == "deferred"
    assert result.claim_token == claim.claim_token
    assert result.admission_decision is not None
    assert result.admission_decision.reason_codes == ("live_session_blocked",)
    assert registry.executions == 0
    assert reports.pending() == ()
    assert tuple(entry.claim for entry in claims.pending()) == (claim,)
    assert not (claims.admitted_dir / f"{claim.claim_token}.json").exists()


def test_resource_reservation_exists_before_shard_spawn_and_releases_after_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from multiprocessing.process import BaseProcess

    from rquant.runtime_resource_admission import SQLiteResourceReservationStore

    claims = LabClaimSpool(tmp_path / "claims")
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims.publish(claim)
    store = SQLiteResourceReservationStore(
        tmp_path / "resource-reservations.sqlite3",
        clock=lambda: NOW,
    )
    original_start = BaseProcess.start
    observed_identity: list[object] = []

    def assert_reserved_before_spawn(process: BaseProcess) -> None:
        if process.name.startswith("lab-shard-"):
            leases = store.active_leases()
            assert len(leases) == 1
            observed_identity.append(leases[0].identity)
        original_start(process)

    monkeypatch.setattr(BaseProcess, "start", assert_reserved_before_spawn)
    worker = _worker(
        tmp_path,
        claims=claims,
        resource_snapshot_provider=StaticResourceSnapshotProvider(_healthy_resource_snapshot()),
        admission_policy_provider=StaticAdmissionPolicyProvider(_permissive_admission_policy()),
        resource_reservation_store=store,
        require_resource_admission=True,
    )

    result = worker.run_once()

    assert result.status == "succeeded"
    assert len(observed_identity) == 1
    identity = observed_identity[0]
    assert identity.job_id == claim.job_id
    assert identity.run_id == claim.spec_hash
    assert identity.shard_id == claim.shard_id
    assert identity.attempt_id == claim.claim_token
    assert identity.claim_generation == claim.claim_generation
    assert identity.scheduler_fencing_token == claim.scheduler_fencing_token
    assert identity.worker_id == claim.worker_id
    assert store.active_leases() == ()


def test_resource_reservation_releases_when_shard_spawn_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from multiprocessing.process import BaseProcess

    import rquant.lab_worker as lab_worker
    from rquant.runtime_resource_admission import SQLiteResourceReservationStore

    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims.publish(claim)
    store = SQLiteResourceReservationStore(
        tmp_path / "resource-reservations.sqlite3",
        clock=lambda: NOW,
    )
    worker = _worker(
        tmp_path,
        claims=claims,
        reports=reports,
        registry=RecordingRegistry(),
        resource_snapshot_provider=StaticResourceSnapshotProvider(_healthy_resource_snapshot()),
        admission_policy_provider=StaticAdmissionPolicyProvider(_permissive_admission_policy()),
        resource_reservation_store=store,
        require_resource_admission=True,
    )

    original_start = BaseProcess.start

    def fail_shard_spawn(process: BaseProcess) -> None:
        if process.name.startswith("lab-shard-"):
            raise OSError("injected shard spawn failure")
        original_start(process)

    with monkeypatch.context() as spawn_patch:
        spawn_patch.setattr(BaseProcess, "start", fail_shard_spawn)
        result = worker.run_once()

    failure = _reported_failure(reports)
    assert result.status == "failed"
    assert failure.phase == "session"
    assert failure.failure_kind == "session_startup"
    assert "shard spawn failure" in failure.message
    assert store.active_leases() == ()

    authority_claims = LabClaimSpool(tmp_path / "runtime-authority-claims")
    authority_reports = LabReportSpool(tmp_path / "runtime-authority-reports")
    authority_claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    authority_claims.publish(authority_claim)
    authority_worker = _worker(
        tmp_path,
        claims=authority_claims,
        reports=authority_reports,
        resource_snapshot_provider=StaticResourceSnapshotProvider(_healthy_resource_snapshot()),
        admission_policy_provider=StaticAdmissionPolicyProvider(_permissive_admission_policy()),
        require_resource_admission=True,
    )

    monkeypatch.setattr(
        authority_worker,
        "_resource_admission_evaluation",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            lab_worker.LabWireSessionStartupError("runtime authority bind failed")
        ),
    )

    authority_result = authority_worker.run_once()

    authority_failure = _reported_failure(authority_reports)
    assert authority_result.status == "failed"
    assert authority_failure.phase == "session"
    assert authority_failure.failure_kind == "session_startup"
    assert "runtime authority bind failed" in authority_failure.message

    grouped_cases = (
        (
            lab_worker.LabWireSessionStartupError("runtime authority ACK transport failed"),
            "session_startup",
        ),
        (
            lab_worker.LabWireSessionError("runtime authority receive transport failed"),
            "session",
        ),
    )
    for index, (wire_error, expected_kind) in enumerate(grouped_cases):
        grouped_claims = LabClaimSpool(tmp_path / f"grouped-authority-{index}-claims")
        grouped_reports = LabReportSpool(tmp_path / f"grouped-authority-{index}-reports")
        grouped_claim = _claim(_nshape_compare_spec(hold_days=(1,)))
        grouped_claims.publish(grouped_claim)
        grouped_worker = _worker(
            tmp_path,
            claims=grouped_claims,
            reports=grouped_reports,
            resource_snapshot_provider=StaticResourceSnapshotProvider(_healthy_resource_snapshot()),
            admission_policy_provider=StaticAdmissionPolicyProvider(_permissive_admission_policy()),
            require_resource_admission=True,
        )
        grouped_error = BaseExceptionGroup(
            "runtime authority and cleanup failed",
            [wire_error, OSError("runtime authority cleanup denied")],
        )
        monkeypatch.setattr(
            grouped_worker,
            "_resource_admission_evaluation",
            lambda *_args, _error=grouped_error, **_kwargs: (_ for _ in ()).throw(_error),
        )

        grouped_result = grouped_worker.run_once()

        grouped_failure = _reported_failure(grouped_reports)
        assert grouped_result.status == "failed"
        assert grouped_failure.phase == "session"
        assert grouped_failure.failure_kind == expected_kind
        assert str(wire_error) in grouped_failure.message
        assert "runtime authority cleanup denied" in grouped_failure.message
        assert (grouped_claims.ack_dir / f"{grouped_claim.claim_token}.json").is_file()
        assert (grouped_claims.admitted_dir / f"{grouped_claim.claim_token}.json").is_file()

    nested_wire_error = lab_worker.LabWireSessionError("nested receive failed")
    nested_group = BaseExceptionGroup(
        "outer authority failure",
        [
            OSError("outer cleanup failed"),
            BaseExceptionGroup("nested authority failure", [nested_wire_error]),
        ],
    )
    assert lab_worker._extract_wire_session_error(nested_group) is nested_wire_error
    assert (
        lab_worker._extract_wire_session_error(
            BaseExceptionGroup("cleanup only", [OSError("cleanup failed")])
        )
        is None
    )


def test_resource_admission_rejects_a_stale_snapshot_before_claim_consumption(
    tmp_path: Path,
) -> None:
    claims = LabClaimSpool(tmp_path / "claims")
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims.publish(claim)
    worker = _worker(
        tmp_path,
        claims=claims,
        resource_snapshot_provider=StaticResourceSnapshotProvider(
            _healthy_resource_snapshot(observed_at=NOW - timedelta(seconds=6))
        ),
        admission_policy_provider=StaticAdmissionPolicyProvider(
            _permissive_admission_policy(max_snapshot_age_seconds=5)
        ),
        require_resource_admission=True,
    )

    with pytest.raises(LabDaemonConfigurationError, match="stale"):
        worker.run_once()

    assert tuple(entry.claim for entry in claims.pending()) == (claim,)


def test_resource_admission_accepts_snapshot_at_exact_microsecond_age_limit(
    tmp_path: Path,
) -> None:
    claims = LabClaimSpool(tmp_path / "claims")
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims.publish(claim)
    worker = _worker(
        tmp_path,
        claims=claims,
        resource_snapshot_provider=StaticResourceSnapshotProvider(
            _healthy_resource_snapshot(observed_at=NOW - timedelta(microseconds=180))
        ),
        admission_policy_provider=StaticAdmissionPolicyProvider(
            _permissive_admission_policy(max_snapshot_age_microseconds=180)
        ),
        require_resource_admission=True,
    )

    result = worker.run_once()

    assert result.status == "succeeded"


def test_resource_admission_rejects_a_future_snapshot_before_claim_consumption(
    tmp_path: Path,
) -> None:
    claims = LabClaimSpool(tmp_path / "claims")
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims.publish(claim)
    worker = _worker(
        tmp_path,
        claims=claims,
        resource_snapshot_provider=StaticResourceSnapshotProvider(
            _healthy_resource_snapshot(observed_at=NOW + timedelta(microseconds=1))
        ),
        admission_policy_provider=StaticAdmissionPolicyProvider(_permissive_admission_policy()),
        require_resource_admission=True,
    )

    with pytest.raises(LabDaemonConfigurationError, match="future"):
        worker.run_once()

    assert tuple(entry.claim for entry in claims.pending()) == (claim,)


@pytest.mark.parametrize(
    ("runtime_observed_at", "message"),
    (
        (NOW - timedelta(seconds=6), "stale"),
        (NOW + timedelta(microseconds=1), "future"),
    ),
)
def test_resource_admission_rejects_invalid_snapshot_during_execution(
    tmp_path: Path,
    runtime_observed_at: datetime,
    message: str,
) -> None:
    claims = LabClaimSpool(tmp_path / "claims")
    child_pid_path = tmp_path / f"runtime-{message}-child.pid"
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims.publish(claim)
    worker = _worker(
        tmp_path,
        claims=claims,
        registry=SlowPidRegistry(pid_path=child_pid_path, delay_seconds=1),
        resource_recheck_interval_seconds=0.01,
        resource_snapshot_provider=SequenceResourceSnapshotProvider(
            _healthy_resource_snapshot(observed_at=NOW),
            _healthy_resource_snapshot(observed_at=runtime_observed_at),
        ),
        admission_policy_provider=StaticAdmissionPolicyProvider(
            _permissive_admission_policy(max_snapshot_age_seconds=5)
        ),
        require_resource_admission=True,
    )

    with pytest.raises(LabDaemonConfigurationError, match=message):
        worker.run_once()

    if child_pid_path.exists():
        _assert_process_gone(int(child_pid_path.read_text(encoding="ascii")))
    assert not worker.sealed_bundle_path(claim).exists()


def test_resource_admission_retries_same_pending_claim_exactly_once(tmp_path: Path) -> None:
    from rquant.resource_admission import AdmissionPolicy, TradingSession

    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    registry = RecordingRegistry()
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims.publish(claim)
    clock = MutableUtcClock(NOW)
    snapshot_provider = MutableResourceSnapshotProvider(
        _healthy_resource_snapshot(observed_at=NOW, session=TradingSession.MORNING),
        _healthy_resource_snapshot(
            observed_at=NOW + timedelta(minutes=2),
            session=TradingSession.POST_MARKET,
        ),
    )

    policy = AdmissionPolicy(
        allow_live_session=False,
        max_live_backlog_age_seconds=10,
        max_live_p95_latency_seconds=5,
        min_available_memory_bytes=512 * 1024**2,
        min_available_disk_bytes=1024**3,
        max_io_pressure_pct=80,
        max_cpu_load_pct=80,
        max_expected_memory_bytes=8 * 1024**3,
        max_expected_disk_bytes=50 * 1024**3,
        max_expected_quota_units=0,
        retry_delay_seconds=60,
    )

    def accept_at_observed(
        report: LabWorkerReport,
        _timeout_seconds: float,
        _stop: object,
    ) -> LabReportReceipt:
        return LabReportReceipt.from_report(
            report,
            status="accepted",
            reason=f"accepted:{report.body.report_type}",
            accepted_at=clock(),
        )

    worker = _worker(
        tmp_path,
        claims=claims,
        reports=reports,
        registry=registry,
        resource_snapshot_provider=snapshot_provider,
        admission_policy_provider=StaticAdmissionPolicyProvider(policy),
        require_resource_admission=True,
        receipt_waiter=accept_at_observed,
        clock=clock,
    )

    def assert_no_authority_child_accumulation() -> None:
        with worker._managed_authority_children_lock:
            assert worker._managed_authority_children == {}
        assert all(
            child.name != "lab-resource-authority" for child in multiprocessing.active_children()
        )

    assert worker.run_once().status == "deferred"
    assert_no_authority_child_accumulation()
    assert worker.run_once().status == "idle"
    assert_no_authority_child_accumulation()
    clock.set(NOW + timedelta(minutes=2))
    snapshot_provider.select(1)
    assert worker.run_once().status == "succeeded"
    assert_no_authority_child_accumulation()
    assert worker.run_once().status == "idle"
    assert_no_authority_child_accumulation()
    assert sum(isinstance(report.body, LabShardSucceeded) for report in _reports(reports)) == 1
    assert worker.sealed_bundle_path(claim).is_dir()


def test_revoked_resource_deferred_claim_never_executes(tmp_path: Path) -> None:
    from rquant.resource_admission import AdmissionPolicy, ResourceSnapshot, TradingSession

    claims = LabClaimSpool(tmp_path / "claims")
    registry = RecordingRegistry()
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims.publish(claim)
    policy = AdmissionPolicy(
        allow_live_session=False,
        max_live_backlog_age_seconds=10,
        max_live_p95_latency_seconds=5,
        min_available_memory_bytes=0,
        min_available_disk_bytes=0,
        max_io_pressure_pct=100,
        max_cpu_load_pct=100,
        max_expected_memory_bytes=8 * 1024**3,
        max_expected_disk_bytes=50 * 1024**3,
        max_expected_quota_units=0,
        retry_delay_seconds=60,
    )
    worker = _worker(
        tmp_path,
        claims=claims,
        registry=registry,
        resource_snapshot_provider=StaticResourceSnapshotProvider(
            ResourceSnapshot(
                observed_at=NOW,
                session=TradingSession.MORNING,
                live_backlog_age_seconds=0,
                live_p95_latency_seconds=0,
                available_memory_bytes=16 * 1024**3,
                available_disk_bytes=100 * 1024**3,
                io_pressure_pct=0,
                cpu_load_pct=0,
                source_quota_remaining=0,
                live_healthy=True,
            )
        ),
        admission_policy_provider=StaticAdmissionPolicyProvider(policy),
        require_resource_admission=True,
    )

    assert worker.run_once().status == "deferred"
    claims.revoke(claim, reason="scheduler cancelled deferred claim")
    assert worker.run_once().status == "idle"
    assert registry.executions == 0


def test_zero_quota_replay_does_not_call_source_lease_provider(tmp_path: Path) -> None:
    from rquant.resource_admission import AdmissionPolicy, ResourceSnapshot, TradingSession

    claims = LabClaimSpool(tmp_path / "claims")
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims.publish(claim)
    worker = _worker(
        tmp_path,
        claims=claims,
        resource_snapshot_provider=StaticResourceSnapshotProvider(
            ResourceSnapshot(
                observed_at=NOW,
                session=TradingSession.POST_MARKET,
                live_backlog_age_seconds=0,
                live_p95_latency_seconds=0,
                available_memory_bytes=16 * 1024**3,
                available_disk_bytes=100 * 1024**3,
                io_pressure_pct=0,
                cpu_load_pct=0,
                source_quota_remaining=0,
                live_healthy=True,
            )
        ),
        admission_policy_provider=StaticAdmissionPolicyProvider(
            AdmissionPolicy(
                allow_live_session=False,
                max_live_backlog_age_seconds=10,
                max_live_p95_latency_seconds=5,
                min_available_memory_bytes=0,
                min_available_disk_bytes=0,
                max_io_pressure_pct=100,
                max_cpu_load_pct=100,
                max_expected_memory_bytes=8 * 1024**3,
                max_expected_disk_bytes=50 * 1024**3,
                max_expected_quota_units=0,
                retry_delay_seconds=60,
            )
        ),
        source_quota_lease_provider=lambda _request, _snapshot: pytest.fail(
            "zero-quota replay must not acquire a source lease"
        ),
        require_resource_admission=True,
    )

    assert worker.run_once().status == "succeeded"


def test_resource_admission_is_rechecked_during_execution_and_preempts_publish(
    tmp_path: Path,
) -> None:
    from rquant.resource_admission import AdmissionPolicy, TradingSession

    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    pid_path = tmp_path / "resource-preempted-child.pid"
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims.publish(claim)
    snapshot_provider = SequenceResourceSnapshotProvider(
        _healthy_resource_snapshot(session=TradingSession.MORNING),
        _healthy_resource_snapshot(session=TradingSession.MORNING).model_copy(
            update={"live_healthy": False}
        ),
    )

    policy = AdmissionPolicy(
        allow_live_session=True,
        max_live_shard_duration_ms=10**9,
        max_live_backlog_age_seconds=10,
        max_live_p95_latency_seconds=5,
        min_available_memory_bytes=0,
        min_available_disk_bytes=0,
        max_io_pressure_pct=100,
        max_cpu_load_pct=100,
        max_expected_memory_bytes=8 * 1024**3,
        max_expected_disk_bytes=50 * 1024**3,
        max_expected_quota_units=0,
        retry_delay_seconds=60,
    )
    worker = _worker(
        tmp_path,
        claims=claims,
        reports=reports,
        registry=HungLiveRegistry(pid_path=pid_path),
        heartbeat_interval_seconds=1,
        resource_recheck_interval_seconds=0.01,
        resource_snapshot_provider=snapshot_provider,
        admission_policy_provider=StaticAdmissionPolicyProvider(policy),
        require_resource_admission=True,
    )

    result = worker.run_once()

    assert result.status == "stopped"
    assert snapshot_provider.calls >= 2
    assert isinstance(_reports(reports)[-1].body, LabWorkerStopped)
    assert not worker.sealed_bundle_path(claim).exists()
    assert not pid_path.exists()


def test_resource_degradation_after_final_heartbeat_blocks_atomic_publish(
    tmp_path: Path,
) -> None:
    from rquant.resource_admission import AdmissionPolicy, TradingSession

    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims.publish(claim)
    snapshot_provider = MutableResourceSnapshotProvider(
        _healthy_resource_snapshot(session=TradingSession.MORNING),
        _healthy_resource_snapshot(session=TradingSession.MORNING).model_copy(
            update={"live_healthy": False}
        ),
    )

    def degrade_after_final_fence(
        report: LabWorkerReport,
        _timeout_seconds: float,
        _stop: object,
    ) -> LabReportReceipt:
        if isinstance(report.body, LabShardHeartbeat):
            snapshot_provider.select(1)
        return LabReportReceipt.from_report(
            report,
            status="accepted",
            reason=f"accepted:{report.body.report_type}",
            accepted_at=NOW,
        )

    policy = AdmissionPolicy(
        allow_live_session=True,
        max_live_shard_duration_ms=10**9,
        max_live_backlog_age_seconds=10,
        max_live_p95_latency_seconds=5,
        min_available_memory_bytes=0,
        min_available_disk_bytes=0,
        max_io_pressure_pct=100,
        max_cpu_load_pct=100,
        max_expected_memory_bytes=8 * 1024**3,
        max_expected_disk_bytes=50 * 1024**3,
        max_expected_quota_units=0,
        retry_delay_seconds=60,
    )
    worker = _worker(
        tmp_path,
        claims=claims,
        reports=reports,
        heartbeat_interval_seconds=10,
        resource_recheck_interval_seconds=10,
        resource_snapshot_provider=snapshot_provider,
        admission_policy_provider=StaticAdmissionPolicyProvider(policy),
        require_resource_admission=True,
        receipt_waiter=degrade_after_final_fence,
    )

    result = worker.run_once()

    assert result.status == "stopped"
    assert isinstance(_reports(reports)[-1].body, LabWorkerStopped)
    assert not worker.sealed_bundle_path(claim).exists()


def test_live_hung_adapter_is_hard_preempted_without_leaking_child(
    tmp_path: Path,
) -> None:
    from rquant.resource_admission import AdmissionPolicy, ResourceSnapshot, TradingSession

    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    pid_path = tmp_path / "hung-child.pid"
    claim = _short_live_claim()
    claims.publish(claim)
    worker = _worker(
        tmp_path,
        claims=claims,
        reports=reports,
        registry=HungLiveRegistry(pid_path=pid_path),
        heartbeat_interval_seconds=1,
        resource_recheck_interval_seconds=0.01,
        resource_snapshot_provider=StaticResourceSnapshotProvider(
            ResourceSnapshot(
                observed_at=NOW,
                session=TradingSession.MORNING,
                live_backlog_age_seconds=0,
                live_p95_latency_seconds=0,
                available_memory_bytes=16 * 1024**3,
                available_disk_bytes=100 * 1024**3,
                io_pressure_pct=0,
                cpu_load_pct=0,
                source_quota_remaining=0,
                live_healthy=True,
            )
        ),
        admission_policy_provider=StaticAdmissionPolicyProvider(
            AdmissionPolicy(
                allow_live_session=True,
                max_live_shard_duration_ms=600,
                max_live_backlog_age_seconds=10,
                max_live_p95_latency_seconds=5,
                min_available_memory_bytes=0,
                min_available_disk_bytes=0,
                max_io_pressure_pct=100,
                max_cpu_load_pct=100,
                max_expected_memory_bytes=8 * 1024**3,
                max_expected_disk_bytes=50 * 1024**3,
                max_expected_quota_units=0,
                retry_delay_seconds=60,
            )
        ),
        require_resource_admission=True,
    )

    started = time.monotonic()
    result = worker.run_once()
    elapsed = time.monotonic() - started

    assert result.status == "stopped"
    assert elapsed < _observe(2)
    assert pid_path.is_file()
    child_pid = int(pid_path.read_text(encoding="ascii"))
    with pytest.raises(ProcessLookupError):
        os.kill(child_pid, 0)
    terminal = _reports(reports)[-1].body
    assert isinstance(terminal, LabWorkerStopped)
    assert "hard live execution limit" in terminal.reason
    assert not worker.sealed_bundle_path(claim).exists()
    assert tuple((tmp_path / "artifacts").glob("**/.attempt-*")) == ()


def test_post_market_preemptible_shard_crossing_into_live_is_hard_preempted(
    tmp_path: Path,
) -> None:
    from rquant.resource_admission import AdmissionPolicy, TradingSession

    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    pid_path = tmp_path / "cross-session-child.pid"
    claim = _short_live_claim()
    claims.publish(claim)
    snapshot_provider = SequenceResourceSnapshotProvider(
        _healthy_resource_snapshot(session=TradingSession.POST_MARKET),
        _healthy_resource_snapshot(session=TradingSession.MORNING),
    )

    worker = _worker(
        tmp_path,
        claims=claims,
        reports=reports,
        registry=SlowPidRegistry(pid_path=pid_path, delay_seconds=1),
        resource_recheck_interval_seconds=0.01,
        resource_snapshot_provider=snapshot_provider,
        admission_policy_provider=StaticAdmissionPolicyProvider(
            AdmissionPolicy(
                allow_live_session=True,
                max_live_shard_duration_ms=100,
                max_live_backlog_age_seconds=10,
                max_live_p95_latency_seconds=5,
                min_available_memory_bytes=0,
                min_available_disk_bytes=0,
                max_io_pressure_pct=100,
                max_cpu_load_pct=100,
                max_expected_memory_bytes=8 * 1024**3,
                max_expected_disk_bytes=50 * 1024**3,
                max_expected_quota_units=0,
                retry_delay_seconds=60,
            )
        ),
        require_resource_admission=True,
    )

    started = time.monotonic()
    result = worker.run_once()
    elapsed = time.monotonic() - started

    assert result.status == "stopped"
    assert elapsed < _observe(1.5)
    child_pid = int(pid_path.read_text(encoding="ascii"))
    assert child_pid != os.getpid()
    _assert_process_gone(child_pid)
    terminal = _reports(reports)[-1].body
    assert isinstance(terminal, LabWorkerStopped)
    assert "hard live execution limit" in terminal.reason
    assert not worker.sealed_bundle_path(claim).exists()


def test_child_result_received_after_monotonic_hard_deadline_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.lab_worker as lab_worker
    from rquant.resource_admission import TradingSession

    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    claim = _short_live_claim()
    claims.publish(claim)
    pid_path = tmp_path / "late-result-child.pid"
    original_recv = lab_worker._recv_wire
    outcome_received = threading.Event()

    def delayed_recv(connection: object, **kwargs: object) -> object:
        value = original_recv(connection, **kwargs)
        if kwargs.get("model") is lab_worker._IsolatedExecutionWireOutcome:
            outcome_received.set()
        return value

    def controlled_monotonic() -> float:
        return time.monotonic() + (1.0 if outcome_received.is_set() else 0.0)

    monkeypatch.setattr(lab_worker, "_recv_wire", delayed_recv)
    worker = _worker(
        tmp_path,
        claims=claims,
        reports=reports,
        registry=SlowPidRegistry(pid_path=pid_path, delay_seconds=0),
        resource_recheck_interval_seconds=0.01,
        isolation_monotonic_clock=controlled_monotonic,
    )
    validated = worker._validate_closed_claim(claim)
    control = worker._execute_shard_isolated(
        claim,
        validated,
        runtime_code_sha="1" * 40,
        hard_limit_seconds=1.0,
        initial_session=TradingSession.MORNING,
    )

    assert control.stop_reason is not None
    assert "hard live execution limit" in control.stop_reason
    assert pid_path.exists()
    _assert_process_gone(int(pid_path.read_text(encoding="ascii")))
    assert not worker.sealed_bundle_path(claim).exists()


def test_slow_authority_reservation_and_pre_ack_recheck_preserve_live_budget(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.resource_admission import TradingSession
    from rquant.runtime_resource_admission import SQLiteResourceReservationStore

    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    adapter_pid_path = tmp_path / "post-ack-budget-adapter.pid"
    second_recheck_path = tmp_path / "post-ack-budget-recheck.entered"
    claim = _short_live_claim()
    claims.publish(claim)
    store = SQLiteResourceReservationStore(
        tmp_path / "post-ack-budget.sqlite3",
        clock=lambda: NOW,
    )
    original_reserve = store.reserve

    def slow_reserve(**kwargs: object):
        time.sleep(0.14)
        return original_reserve(**kwargs)

    monkeypatch.setattr(store, "reserve", slow_reserve)
    worker = _worker(
        tmp_path,
        claims=claims,
        reports=reports,
        registry=SlowPidRegistry(pid_path=adapter_pid_path, delay_seconds=0.04),
        resource_probe_timeout_seconds=1,
        resource_snapshot_provider=StaticResourceSnapshotProvider(
            _healthy_resource_snapshot(session=TradingSession.MORNING)
        ),
        admission_policy_provider=SlowAdmissionPolicyProvider(
            _permissive_admission_policy(max_live_shard_duration_ms=300),
            delay_seconds=0.14,
            second_call_entered_path=second_recheck_path,
        ),
        resource_reservation_store=store,
        require_resource_admission=True,
    )
    outcomes: list[object] = []
    runner = threading.Thread(target=lambda: outcomes.append(worker.run_once()))

    started = time.monotonic()
    runner.start()
    entered_deadline = time.monotonic() + _child_startups(2)
    while not second_recheck_path.exists() and time.monotonic() < entered_deadline:
        time.sleep(0.01)
    assert second_recheck_path.exists()
    assert not adapter_pid_path.exists()
    runner.join(timeout=_observe(3))
    elapsed = time.monotonic() - started

    assert not runner.is_alive()
    assert elapsed > 0.42
    assert elapsed < _observe(3)
    assert outcomes[0].status == "succeeded"
    assert adapter_pid_path.exists()
    _assert_process_gone(int(adapter_pid_path.read_text(encoding="ascii")))
    assert store.active_leases() == ()
    assert any(isinstance(entry.report.body, LabShardSucceeded) for entry in reports.pending())


def test_resource_admission_shared_tick_deadline_does_not_start_second_authority_call(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.lab_worker import _ResourceAdmissionEvaluation
    from rquant.resource_admission import TradingSession, evaluate_admission

    spec = _nshape_compare_spec(hold_days=(1,))
    claim = _short_claim_for_spec(spec)
    clock_microseconds = [1_000_000]
    authority_calls: list[int] = []
    worker = _worker(
        tmp_path,
        resource_snapshot_provider=StaticResourceSnapshotProvider(
            _healthy_resource_snapshot(session=TradingSession.MORNING)
        ),
        admission_policy_provider=StaticAdmissionPolicyProvider(
            _permissive_admission_policy(max_live_shard_duration_ms=300)
        ),
        require_resource_admission=True,
        monotonic_clock=lambda: clock_microseconds[0] / 1_000_000,
    )

    def delayed_authority(**kwargs: object):
        authority_calls.append(int(kwargs["timeout_microseconds"]))
        clock_microseconds[0] += 60_000
        request = kwargs["request"]
        assert request is not None
        snapshot = _healthy_resource_snapshot(session=TradingSession.MORNING)
        policy = _permissive_admission_policy(max_live_shard_duration_ms=300)
        return _ResourceAdmissionEvaluation(
            decision=evaluate_admission(request, snapshot, policy),
            request=request,
            snapshot=snapshot,
            policy=policy,
            quota_lease=None,
        )

    monkeypatch.setattr(worker, "_run_admission_authority", delayed_authority)
    deadline_microseconds = clock_microseconds[0] + 50_000

    with pytest.raises(TimeoutError, match="pre-publication admission deadline"):
        worker._resource_admission_evaluation(
            claim,
            spec,
            tick_deadline_microseconds=deadline_microseconds,
        )

    with pytest.raises(TimeoutError, match="pre-publication admission deadline"):
        worker._resource_admission_evaluation(
            claim,
            spec,
            tick_deadline_microseconds=deadline_microseconds,
        )

    assert authority_calls == [50_000]


def test_receipt_phase_does_not_consume_prepublication_budget(
    tmp_path: Path,
) -> None:
    from rquant.resource_admission import TradingSession

    clock_microseconds = [1_000_000]
    worker = _worker(
        tmp_path,
        resource_snapshot_provider=StaticResourceSnapshotProvider(
            _healthy_resource_snapshot(session=TradingSession.MORNING)
        ),
        admission_policy_provider=StaticAdmissionPolicyProvider(
            _permissive_admission_policy(max_live_shard_duration_ms=300)
        ),
        require_resource_admission=True,
        monotonic_clock=lambda: clock_microseconds[0] / 1_000_000,
        # This case is about the receipt budget itself, so it pins the value
        # instead of inheriting the helper's CI-sized default.
        receipt_timeout_seconds=0.2,
    )
    assert worker._receipt_wait_timeout_seconds() == pytest.approx(0.2)

    clock_microseconds[0] += 1_000_000
    assert worker._receipt_wait_timeout_seconds() == pytest.approx(0.2)


def test_pre_ack_refresh_stop_is_bounded_and_discards_late_recheck(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.resource_admission import TradingSession
    from rquant.runtime_resource_admission import SQLiteResourceReservationStore

    claims = LabClaimSpool(tmp_path / "authority-block-claims")
    reports = LabReportSpool(tmp_path / "authority-block-reports")
    claim = _short_live_claim()
    claims.publish(claim)
    authority_pid_path = tmp_path / "pre-ack-authority.pid"
    descendant_pid_path = tmp_path / "pre-ack-authority-descendant.pid"
    release_authority_path = tmp_path / "release-pre-ack-authority"
    initial_snapshot = StaticResourceSnapshotProvider(
        _healthy_resource_snapshot(session=TradingSession.MORNING)
    )
    initial_policy = StaticAdmissionPolicyProvider(
        _permissive_admission_policy(max_live_shard_duration_ms=300)
    )
    worker = _worker(
        tmp_path,
        claims=claims,
        reports=reports,
        registry=PlanBypassRecordingRegistry(),
        resource_snapshot_provider=initial_snapshot,
        admission_policy_provider=initial_policy,
        require_resource_admission=True,
    )
    blocking_manifest = _test_authority_manifest(
        tmp_path,
        snapshot_provider=initial_snapshot,
        policy_provider=SpawnDescendantBlockingAdmissionPolicyProvider(
            _permissive_admission_policy(max_live_shard_duration_ms=300),
            authority_pid_path=authority_pid_path,
            descendant_pid_path=descendant_pid_path,
            release_path=release_authority_path,
        ),
        quota_provider=None,
    )
    original_consume = worker._consume_selected_claim

    def consume_then_block_pre_ack(entry: object):
        consumed = original_consume(entry)
        if consumed is not None:
            worker.resource_authority_manifest = blocking_manifest
        return consumed

    monkeypatch.setattr(worker, "_consume_selected_claim", consume_then_block_pre_ack)
    outcomes: list[object] = []
    errors: list[BaseException] = []

    def run_blocked_authority() -> None:
        try:
            outcomes.append(worker.run_once())
        except BaseException as exc:
            errors.append(exc)

    runner = threading.Thread(target=run_blocked_authority)
    runner.start()
    authority_pid: int | None = None
    descendant_pid: int | None = None
    try:
        deadline = time.monotonic() + _observe(4)
        while (
            not authority_pid_path.exists() or not descendant_pid_path.exists()
        ) and time.monotonic() < deadline:
            time.sleep(0.01)
        assert authority_pid_path.exists()
        assert descendant_pid_path.exists()
        authority_pid = int(authority_pid_path.read_text(encoding="ascii"))
        descendant_pid = int(descendant_pid_path.read_text(encoding="ascii"))
        worker.request_stop()
        runner.join(timeout=_observe(2))
        assert not runner.is_alive()
        assert errors == []
        assert len(outcomes) == 1
        assert outcomes[0].status == "stopped"
        assert not any(
            thread.name.startswith("lab-pre-ack-resource-") and thread.is_alive()
            for thread in threading.enumerate()
        )
        assert worker._managed_authority_children == {}
        _assert_process_gone(authority_pid)
        _assert_process_gone(descendant_pid)
    finally:
        release_authority_path.touch()
        runner.join(timeout=_observe(3))
        if authority_pid is not None:
            _kill_process_if_alive(authority_pid)
        if descendant_pid is not None:
            _kill_process_if_alive(descendant_pid)

    claims = LabClaimSpool(tmp_path / "callback-block-claims")
    reports = LabReportSpool(tmp_path / "callback-block-reports")
    callback_claim = _short_live_claim()
    claims.publish(callback_claim)
    store = SQLiteResourceReservationStore(
        tmp_path / "pre-ack-cancel.sqlite3",
        clock=lambda: NOW,
    )
    entered_recheck = threading.Event()
    release_recheck = threading.Event()
    original_recheck = store.recheck

    def blocking_recheck(**kwargs: object):
        entered_recheck.set()
        assert release_recheck.wait(timeout=_observe(3))
        return original_recheck(**kwargs)

    monkeypatch.setattr(store, "recheck", blocking_recheck)
    callback_worker = _worker(
        tmp_path,
        claims=claims,
        reports=reports,
        registry=PlanBypassRecordingRegistry(),
        resource_snapshot_provider=StaticResourceSnapshotProvider(
            _healthy_resource_snapshot(session=TradingSession.MORNING)
        ),
        admission_policy_provider=StaticAdmissionPolicyProvider(
            _permissive_admission_policy(max_live_shard_duration_ms=300)
        ),
        resource_reservation_store=store,
        require_resource_admission=True,
    )
    callback_outcomes: list[object] = []
    callback_errors: list[BaseException] = []

    def run_blocked_callback() -> None:
        try:
            callback_outcomes.append(callback_worker.run_once())
        except BaseException as exc:
            callback_errors.append(exc)

    callback_runner = threading.Thread(target=run_blocked_callback)
    callback_runner.start()
    try:
        assert entered_recheck.wait(timeout=_observe(3))
        callback_worker.request_stop()
        callback_runner.join(timeout=_observe(2))
        assert not callback_runner.is_alive()
        assert callback_outcomes == []
        assert len(callback_errors) == 1
        assert isinstance(callback_errors[0], BaseExceptionGroup)
        diagnostics = _collect_base_exceptions(callback_errors[0])
        assert any(isinstance(error, InterruptedError) for error in diagnostics)
        assert any(isinstance(error, TimeoutError) for error in diagnostics)
        assert callback_worker._pre_ack_admission_diagnostics
    finally:
        release_recheck.set()
        callback_runner.join(timeout=_observe(3))

    assert store.active_leases() == ()
    assert reports.pending() == ()


def test_delayed_ack_send_starts_full_live_budget_only_after_send_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.lab_worker as lab_worker
    from rquant.resource_admission import TradingSession

    claim = _short_live_claim()
    adapter_pid_path = tmp_path / "delayed-ack-adapter.pid"
    worker = _worker(
        tmp_path,
        registry=SlowPidRegistry(pid_path=adapter_pid_path, delay_seconds=0.02),
    )
    validated = worker._validate_closed_claim(claim)
    original_send = lab_worker._send_wire

    def delayed_send(connection: object, value: object, **kwargs: object) -> None:
        if isinstance(value, lab_worker._IsolationStartAck):
            time.sleep(0.55)
        original_send(connection, value, **kwargs)

    monkeypatch.setattr(lab_worker, "_send_wire", delayed_send)

    started = time.monotonic()
    control = worker._execute_shard_isolated(
        claim,
        validated,
        runtime_code_sha="1" * 40,
        hard_limit_seconds=0.5,
        initial_session=TradingSession.MORNING,
    )
    elapsed = time.monotonic() - started

    assert elapsed >= 0.55
    assert control.stop_reason is None
    assert control.resource_error is None
    assert control.outcome is not None and control.outcome.result is not None
    assert adapter_pid_path.exists()
    _assert_process_gone(int(adapter_pid_path.read_text(encoding="ascii")))


def test_failed_ack_send_never_executes_adapter_and_reaps_child(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.lab_worker as lab_worker

    claim = _short_live_claim()
    claims = LabClaimSpool(tmp_path / "ack-failure-claims")
    reports = LabReportSpool(tmp_path / "ack-failure-reports")
    claims.publish(claim)
    adapter_pid_path = tmp_path / "failed-ack-adapter.pid"
    worker = _worker(
        tmp_path,
        claims=claims,
        reports=reports,
        registry=SlowPidRegistry(pid_path=adapter_pid_path, delay_seconds=0),
    )
    original_send = lab_worker._send_wire

    def fail_ack_send(connection: object, value: object, **kwargs: object) -> None:
        if isinstance(value, lab_worker._IsolationStartAck):
            raise OSError("injected ACK send failure")
        original_send(connection, value, **kwargs)

    with monkeypatch.context() as ack_patch:
        ack_patch.setattr(lab_worker, "_send_wire", fail_ack_send)
        result = worker.run_once()

    failure = _reported_failure(reports)
    assert result.status == "failed"
    assert failure.phase == "session"
    assert failure.failure_kind == "session_startup"
    assert "ACK send failure" in failure.message
    assert not adapter_pid_path.exists()
    assert all(
        not child.name.startswith("lab-shard-") for child in multiprocessing.active_children()
    )

    exited_claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    exited_claims = LabClaimSpool(tmp_path / "child-exit-claims")
    exited_reports = LabReportSpool(tmp_path / "child-exit-reports")
    exited_claims.publish(exited_claim)
    exited_pid_path = tmp_path / "child-exit-adapter.pid"
    exited_worker = _worker(
        tmp_path,
        claims=exited_claims,
        reports=exited_reports,
        registry=SlowPidRegistry(pid_path=exited_pid_path, delay_seconds=1),
    )
    killed_pids: list[int] = []

    def send_ack_then_kill(connection: object, value: object, **kwargs: object) -> None:
        original_send(connection, value, **kwargs)
        if isinstance(value, lab_worker._IsolationStartAck):
            shard_children = [
                child
                for child in multiprocessing.active_children()
                if child.name.startswith("lab-shard-") and child.pid is not None
            ]
            assert len(shard_children) == 1
            child_pid = shard_children[0].pid
            assert child_pid is not None
            killed_pids.append(child_pid)
            os.kill(child_pid, signal.SIGKILL)

    with monkeypatch.context() as exit_patch:
        exit_patch.setattr(lab_worker, "_send_wire", send_ack_then_kill)
        exited_result = exited_worker.run_once()

    exited_failure = _reported_failure(exited_reports)
    assert exited_result.status == "failed"
    assert exited_failure.phase == "session"
    assert exited_failure.failure_kind == "session"
    assert "isolated shard" in exited_failure.message
    assert len(killed_pids) == 1
    _assert_process_gone(killed_pids[0])
    assert all(
        not child.name.startswith("lab-shard-") for child in multiprocessing.active_children()
    )


def test_stop_and_start_ack_commit_share_one_atomic_gate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A stop racing inside start commit observes ACK as committed, never half-started."""

    import rquant.lab_worker as lab_worker
    from rquant.resource_admission import TradingSession

    claim = _short_live_claim()
    adapter_pid_path = tmp_path / "atomic-start-gate-adapter.pid"
    worker = _worker(
        tmp_path,
        registry=SlowPidRegistry(pid_path=adapter_pid_path, delay_seconds=0.2),
    )
    validated = worker._validate_closed_claim(claim)
    inside_commit = threading.Event()
    release_commit = threading.Event()
    stop_returned = threading.Event()
    ack_sent = threading.Event()
    original_send = lab_worker._send_wire

    def block_inside_commit() -> None:
        inside_commit.set()
        assert release_commit.wait(timeout=_observe(2))

    def record_send(connection: object, value: object, **kwargs: object) -> None:
        original_send(connection, value, **kwargs)
        if isinstance(value, lab_worker._IsolationStartAck):
            ack_sent.set()

    monkeypatch.setattr(worker, "_during_isolation_start_commit_for_test", block_inside_commit)
    monkeypatch.setattr(lab_worker, "_send_wire", record_send)
    controls: list[object] = []
    runner = threading.Thread(
        target=lambda: controls.append(
            worker._execute_shard_isolated(
                claim,
                validated,
                runtime_code_sha="1" * 40,
                hard_limit_seconds=1,
                initial_session=TradingSession.CLOSED,
            )
        )
    )
    runner.start()
    assert inside_commit.wait(timeout=_observe(2))

    stopper = threading.Thread(
        target=lambda: (worker.request_stop(), stop_returned.set()),
    )
    stopper.start()
    assert not stop_returned.wait(timeout=0.05)
    release_commit.set()
    assert ack_sent.wait(timeout=_observe(2))
    assert stop_returned.wait(timeout=_observe(2))
    stopper.join(timeout=_observe(2))
    runner.join(timeout=_observe(3))

    assert not stopper.is_alive()
    assert not runner.is_alive()
    assert controls[0].stop_reason is not None
    assert "stop requested" in controls[0].stop_reason
    adapter_pid = _recorded_pid(adapter_pid_path)
    if adapter_pid is not None:
        _assert_process_gone(adapter_pid)


def test_stop_before_start_commit_never_sends_ack_or_executes_adapter(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.lab_worker as lab_worker
    from rquant.resource_admission import TradingSession

    claim = _short_live_claim()
    adapter_pid_path = tmp_path / "pre-commit-stop-adapter.pid"
    worker = _worker(
        tmp_path,
        registry=SlowPidRegistry(pid_path=adapter_pid_path, delay_seconds=0),
    )
    validated = worker._validate_closed_claim(claim)
    before_commit = threading.Event()
    release_commit = threading.Event()
    ack_sent = threading.Event()
    original_send = lab_worker._send_wire

    def block_before_commit() -> None:
        before_commit.set()
        assert release_commit.wait(timeout=_observe(2))

    def record_send(connection: object, value: object, **kwargs: object) -> None:
        if isinstance(value, lab_worker._IsolationStartAck):
            ack_sent.set()
        original_send(connection, value, **kwargs)

    monkeypatch.setattr(worker, "_before_isolation_start_commit_for_test", block_before_commit)
    monkeypatch.setattr(lab_worker, "_send_wire", record_send)
    controls: list[object] = []
    runner = threading.Thread(
        target=lambda: controls.append(
            worker._execute_shard_isolated(
                claim,
                validated,
                runtime_code_sha="1" * 40,
                hard_limit_seconds=1,
                initial_session=TradingSession.CLOSED,
            )
        )
    )
    runner.start()
    assert before_commit.wait(timeout=_observe(2))
    worker.request_stop()
    release_commit.set()
    runner.join(timeout=_observe(3))

    assert not runner.is_alive()
    assert controls[0].stop_reason == "worker stop requested before isolated shard start"
    assert not ack_sent.is_set()
    assert not adapter_pid_path.exists()


def test_process_start_latency_does_not_consume_post_ack_execution_budget(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from multiprocessing.process import BaseProcess

    from rquant.resource_admission import TradingSession

    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    claim = _short_live_claim()
    claims.publish(claim)
    execution_marker = tmp_path / "late-process-start-adapter.pid"
    original_start = BaseProcess.start
    start_returned = threading.Event()

    def delayed_process_start(process: BaseProcess) -> None:
        original_start(process)
        start_returned.set()

    def controlled_monotonic() -> float:
        return time.monotonic() + (1.0 if start_returned.is_set() else 0.0)

    monkeypatch.setattr(BaseProcess, "start", delayed_process_start)
    worker = _worker(
        tmp_path,
        claims=claims,
        reports=reports,
        registry=HungLiveRegistry(pid_path=execution_marker),
        resource_recheck_interval_seconds=0.01,
        isolation_monotonic_clock=controlled_monotonic,
    )

    validated = worker._validate_closed_claim(claim)
    control = worker._execute_shard_isolated(
        claim,
        validated,
        runtime_code_sha="1" * 40,
        hard_limit_seconds=1.0,
        initial_session=TradingSession.MORNING,
    )

    assert control.stop_reason is not None
    assert "hard live execution limit" in control.stop_reason
    assert execution_marker.exists()
    _assert_process_gone(int(execution_marker.read_text(encoding="ascii")))
    assert not worker.sealed_bundle_path(claim).exists()


def test_readiness_identity_verification_does_not_consume_post_ack_execution_budget(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.resource_admission import TradingSession

    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    claim = _short_live_claim()
    claims.publish(claim)
    execution_marker = tmp_path / "late-readiness-adapter.pid"
    original_getpgid = os.getpgid
    parent_pid = os.getpid()
    verification_returned = threading.Event()

    def delayed_parent_getpgid(pid: int) -> int:
        group_id = original_getpgid(pid)
        if os.getpid() == parent_pid:
            verification_returned.set()
        return group_id

    def controlled_monotonic() -> float:
        return time.monotonic() + (1.0 if verification_returned.is_set() else 0.0)

    monkeypatch.setattr(os, "getpgid", delayed_parent_getpgid)
    worker = _worker(
        tmp_path,
        claims=claims,
        reports=reports,
        registry=HungLiveRegistry(pid_path=execution_marker),
        resource_recheck_interval_seconds=0.01,
        isolation_monotonic_clock=controlled_monotonic,
    )

    validated = worker._validate_closed_claim(claim)
    control = worker._execute_shard_isolated(
        claim,
        validated,
        runtime_code_sha="1" * 40,
        hard_limit_seconds=1.0,
        initial_session=TradingSession.MORNING,
    )

    assert control.stop_reason is not None
    assert "hard live execution limit" in control.stop_reason
    assert execution_marker.exists()
    _assert_process_gone(int(execution_marker.read_text(encoding="ascii")))
    assert not worker.sealed_bundle_path(claim).exists()


def test_child_rejects_ack_delivered_after_spec_deadline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.lab_worker as lab_worker
    from rquant.resource_admission import AdmissionPolicy, ResourceSnapshot, TradingSession

    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    claim = _short_live_claim()
    claims.publish(claim)
    execution_marker = tmp_path / "late-ack-delivery-adapter.pid"
    original_send = lab_worker._send_wire

    def expire_ack_at_delivery(
        connection: object,
        value: object,
        **kwargs: object,
    ) -> None:
        if (
            isinstance(value, lab_worker._IsolationStartAck)
            and value.execution_limit_microseconds is not None
        ):
            value = lab_worker._IsolationStartAck(
                accepted=value.accepted,
                not_after_monotonic_microseconds=(time.monotonic_ns() // 1_000 - 1_000_000),
                execution_limit_microseconds=value.execution_limit_microseconds,
            )
        original_send(connection, value, **kwargs)

    monkeypatch.setattr(lab_worker, "_send_wire", expire_ack_at_delivery)
    worker = _worker(
        tmp_path,
        claims=claims,
        reports=reports,
        registry=HungLiveRegistry(pid_path=execution_marker),
        resource_recheck_interval_seconds=0.01,
        resource_snapshot_provider=StaticResourceSnapshotProvider(
            ResourceSnapshot(
                observed_at=NOW,
                session=TradingSession.MORNING,
                live_backlog_age_seconds=0,
                live_p95_latency_seconds=0,
                available_memory_bytes=16 * 1024**3,
                available_disk_bytes=100 * 1024**3,
                io_pressure_pct=0,
                cpu_load_pct=0,
                source_quota_remaining=0,
                live_healthy=True,
            )
        ),
        admission_policy_provider=StaticAdmissionPolicyProvider(
            AdmissionPolicy(
                allow_live_session=True,
                max_live_shard_duration_ms=100,
                max_live_backlog_age_seconds=10,
                max_live_p95_latency_seconds=5,
                min_available_memory_bytes=0,
                min_available_disk_bytes=0,
                max_io_pressure_pct=100,
                max_cpu_load_pct=100,
                max_expected_memory_bytes=8 * 1024**3,
                max_expected_disk_bytes=50 * 1024**3,
                max_expected_quota_units=0,
                retry_delay_seconds=60,
            )
        ),
        require_resource_admission=True,
    )

    result = worker.run_once()

    assert result.status == "failed"
    failure = _reported_failure(reports)
    assert failure.phase == "session"
    assert failure.failure_kind == "session"
    assert "transport failed" in failure.message
    assert not execution_marker.exists()
    assert not worker.sealed_bundle_path(claim).exists()


def test_hard_preemption_sigkills_entire_sigterm_ignoring_process_group(
    tmp_path: Path,
) -> None:
    from rquant.resource_admission import AdmissionPolicy, ResourceSnapshot, TradingSession

    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    child_pid_path = tmp_path / "sigkill-child.pid"
    grandchild_pid_path = tmp_path / "sigkill-grandchild.pid"
    claim = _short_live_claim()
    claims.publish(claim)
    worker = _worker(
        tmp_path,
        claims=claims,
        reports=reports,
        registry=SigtermIgnoringProcessTreeRegistry(
            pid_path=child_pid_path,
            grandchild_pid_path=grandchild_pid_path,
        ),
        resource_recheck_interval_seconds=0.01,
        resource_snapshot_provider=StaticResourceSnapshotProvider(
            ResourceSnapshot(
                observed_at=NOW,
                session=TradingSession.MORNING,
                live_backlog_age_seconds=0,
                live_p95_latency_seconds=0,
                available_memory_bytes=16 * 1024**3,
                available_disk_bytes=100 * 1024**3,
                io_pressure_pct=0,
                cpu_load_pct=0,
                source_quota_remaining=0,
                live_healthy=True,
            )
        ),
        admission_policy_provider=StaticAdmissionPolicyProvider(
            AdmissionPolicy(
                allow_live_session=True,
                max_live_shard_duration_ms=600,
                max_live_backlog_age_seconds=10,
                max_live_p95_latency_seconds=5,
                min_available_memory_bytes=0,
                min_available_disk_bytes=0,
                max_io_pressure_pct=100,
                max_cpu_load_pct=100,
                max_expected_memory_bytes=8 * 1024**3,
                max_expected_disk_bytes=50 * 1024**3,
                max_expected_quota_units=0,
                retry_delay_seconds=60,
            )
        ),
        require_resource_admission=True,
    )

    result = worker.run_once()

    assert result.status == "stopped"
    assert child_pid_path.is_file()
    assert grandchild_pid_path.is_file()
    _assert_process_gone(int(child_pid_path.read_text(encoding="ascii")))
    _assert_process_gone(int(grandchild_pid_path.read_text(encoding="ascii")))
    assert not worker.sealed_bundle_path(claim).exists()


def test_isolated_cleanup_bounds_sigterm_grace_and_reaps_ignoring_process_group(
    tmp_path: Path,
) -> None:
    from rquant.lab_worker import LabWorker

    child_pid_path = tmp_path / "bounded-cleanup-child.pid"
    grandchild_pid_path = tmp_path / "bounded-cleanup-grandchild.pid"
    process = multiprocessing.get_context("spawn").Process(
        target=_standalone_sigterm_ignoring_process_tree,
        args=(child_pid_path, grandchild_pid_path),
        name="lab-shard-bounded-cleanup",
        daemon=False,
    )
    process.start()
    group_id = process.pid
    grandchild_pid: int | None = None
    try:
        assert group_id is not None
        # Both processes in this tree install SIG_IGN for SIGTERM on purpose,
        # so the only thing that ever ends them is the group kill in the
        # finally below - nothing between start() and that block may raise.
        # This used to: the pids were parsed after waiting on is_file() alone,
        # and a writer creates its pid file and fills it as two separate steps,
        # so a loaded host can hand the reader a zero-byte file. A 2-vCPU CI
        # runner did exactly that, int("") raised past the cleanup that had not
        # been entered yet, and the escaped tree then failed every later
        # active_children() assertion in this module and parked the session in
        # multiprocessing's atexit join. _recorded_pid waits for content rather
        # than for the directory entry, which is the fact the case needs; the
        # budget is the module's own spawn scale rather than a flat literal,
        # because what is being waited for is a spawned CPython re-importing
        # this module and then forking once more before either write.
        grandchild_pid = _recorded_pid(
            grandchild_pid_path, timeout_seconds=_child_startups(2)
        )
        child_pid = _recorded_pid(child_pid_path, timeout_seconds=_child_startups(2))
        assert child_pid is not None
        assert grandchild_pid is not None

        started = time.monotonic()
        LabWorker._terminate_isolated_process(
            process,
            isolated_group_id=group_id,
        )
        elapsed = time.monotonic() - started

        assert elapsed < _observe(0.2)
        assert process.exitcode == -signal.SIGKILL
        _assert_process_gone(group_id)
        _assert_process_gone(grandchild_pid)
    finally:
        if group_id is not None:
            with suppress(ProcessLookupError):
                os.killpg(group_id, signal.SIGKILL)
            _kill_process_if_alive(group_id)
        if grandchild_pid is not None:
            _kill_process_if_alive(grandchild_pid)
        with suppress(BaseException):
            process.join(1)
        with suppress(BaseException):
            process.close()


def test_heavy_shard_is_prestarted_but_never_acked_when_session_turns_live(
    tmp_path: Path,
) -> None:
    from rquant.research_run_spec import ResourceClass
    from rquant.resource_admission import AdmissionPolicy, TradingSession

    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    child_pid_path = tmp_path / "heavy-cross-session-child.pid"
    adapter_pid_path = tmp_path / "heavy-cross-session-adapter.pid"
    spec = _nshape_compare_spec(hold_days=(1,)).model_copy(
        update={"resource_class": ResourceClass.HEAVY}
    )
    claim = _short_claim_for_spec(spec)
    claims.publish(claim)
    snapshot_provider = SequenceResourceSnapshotProvider(
        _healthy_resource_snapshot(session=TradingSession.POST_MARKET),
        _healthy_resource_snapshot(session=TradingSession.MORNING),
    )

    worker = _worker(
        tmp_path,
        claims=claims,
        reports=reports,
        registry=SlowPidRegistry(pid_path=adapter_pid_path, delay_seconds=1),
        resource_recheck_interval_seconds=0.01,
        resource_snapshot_provider=snapshot_provider,
        admission_policy_provider=StaticAdmissionPolicyProvider(
            AdmissionPolicy(
                allow_live_session=True,
                max_live_shard_duration_ms=100,
                max_live_backlog_age_seconds=10,
                max_live_p95_latency_seconds=5,
                min_available_memory_bytes=0,
                min_available_disk_bytes=0,
                max_io_pressure_pct=100,
                max_cpu_load_pct=100,
                max_expected_memory_bytes=8 * 1024**3,
                max_expected_disk_bytes=50 * 1024**3,
                max_expected_quota_units=0,
                retry_delay_seconds=60,
            )
        ),
        require_resource_admission=True,
        isolation_session_initializer=RecordingSessionInitializer(child_pid_path),
    )

    started = time.monotonic()
    result = worker.run_once()
    elapsed = time.monotonic() - started

    assert result.status == "stopped"
    assert elapsed < _observe(1.5)
    assert not adapter_pid_path.exists()
    child_pid = int(child_pid_path.read_text(encoding="ascii"))
    assert child_pid != os.getpid()
    _assert_process_gone(child_pid)
    assert not worker.sealed_bundle_path(claim).exists()


def test_result_ready_before_scheduled_recheck_is_gated_before_serialization(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.resource_admission import AdmissionPolicy, TradingSession

    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    pid_path = tmp_path / "pre-live-fast-result.pid"
    prepared_marker = tmp_path / "result-was-serialized"
    claim = _short_live_claim()
    claims.publish(claim)
    snapshot_provider = SequenceResourceSnapshotProvider(
        _healthy_resource_snapshot(session=TradingSession.POST_MARKET),
        _healthy_resource_snapshot(session=TradingSession.MORNING).model_copy(
            update={"live_healthy": False}
        ),
    )

    worker = _worker(
        tmp_path,
        claims=claims,
        reports=reports,
        registry=SlowPidRegistry(pid_path=pid_path, delay_seconds=0),
        resource_recheck_interval_seconds=10,
        resource_snapshot_provider=snapshot_provider,
        admission_policy_provider=StaticAdmissionPolicyProvider(
            AdmissionPolicy(
                allow_live_session=True,
                max_live_shard_duration_ms=100,
                max_live_backlog_age_seconds=10,
                max_live_p95_latency_seconds=5,
                min_available_memory_bytes=0,
                min_available_disk_bytes=0,
                max_io_pressure_pct=100,
                max_cpu_load_pct=100,
                max_expected_memory_bytes=8 * 1024**3,
                max_expected_disk_bytes=50 * 1024**3,
                max_expected_quota_units=0,
                retry_delay_seconds=60,
            )
        ),
        require_resource_admission=True,
    )
    original_prepare = worker._prepare_result

    def mark_prepare(*args: object, **kwargs: object):
        prepared_marker.write_text("serialized", encoding="ascii")
        return original_prepare(*args, **kwargs)

    monkeypatch.setattr(worker, "_prepare_result", mark_prepare)

    result = worker.run_once()

    assert result.status == "stopped"
    assert not prepared_marker.exists()
    assert not worker.sealed_bundle_path(claim).exists()


def test_resource_is_rechecked_after_result_receive_before_acceptance(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.lab_worker as lab_worker
    from rquant.resource_admission import AdmissionPolicy, TradingSession

    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    claim = _short_live_claim()
    claims.publish(claim)
    prepared_marker = tmp_path / "post-receive-result-was-serialized"
    snapshot_provider = FileSelectedResourceSnapshotProvider(
        tmp_path / "post-receive-snapshot-selection",
        _healthy_resource_snapshot(session=TradingSession.POST_MARKET),
        _healthy_resource_snapshot(session=TradingSession.MORNING).model_copy(
            update={"live_healthy": False}
        ),
    )
    original_recv = lab_worker._recv_wire
    outcome_received = threading.Event()

    def degrade_after_outcome_receive(connection: object, **kwargs: object) -> object:
        received = original_recv(connection, **kwargs)
        if kwargs.get("model") is lab_worker._IsolatedExecutionWireOutcome:
            outcome_received.set()
        return received

    monkeypatch.setattr(lab_worker, "_recv_wire", degrade_after_outcome_receive)

    worker = _worker(
        tmp_path,
        claims=claims,
        reports=reports,
        registry=SlowPidRegistry(
            pid_path=tmp_path / "post-receive-recheck.pid",
            delay_seconds=0,
        ),
        resource_recheck_interval_seconds=10,
        resource_snapshot_provider=snapshot_provider,
        admission_policy_provider=StaticAdmissionPolicyProvider(
            AdmissionPolicy(
                allow_live_session=True,
                max_live_shard_duration_ms=100,
                max_live_backlog_age_seconds=10,
                max_live_p95_latency_seconds=5,
                min_available_memory_bytes=0,
                min_available_disk_bytes=0,
                max_io_pressure_pct=100,
                max_cpu_load_pct=100,
                max_expected_memory_bytes=8 * 1024**3,
                max_expected_disk_bytes=50 * 1024**3,
                max_expected_quota_units=0,
                retry_delay_seconds=60,
            )
        ),
        require_resource_admission=True,
    )
    original_evaluation = worker._resource_admission_evaluation

    def degrade_before_post_receive_recheck(*args: object, **kwargs: object):
        if outcome_received.is_set():
            snapshot_provider.select(1)
        return original_evaluation(*args, **kwargs)

    monkeypatch.setattr(
        worker,
        "_resource_admission_evaluation",
        degrade_before_post_receive_recheck,
    )
    original_prepare = worker._prepare_result

    def mark_prepare(*args: object, **kwargs: object):
        prepared_marker.write_text("serialized", encoding="ascii")
        return original_prepare(*args, **kwargs)

    monkeypatch.setattr(worker, "_prepare_result", mark_prepare)

    result = worker.run_once()

    assert result.status == "stopped"
    assert snapshot_provider().live_healthy is False
    assert not prepared_marker.exists()
    assert not worker.sealed_bundle_path(claim).exists()


def test_term_exited_group_leader_still_escalates_kill_to_descendant(
    tmp_path: Path,
) -> None:
    from rquant.resource_admission import AdmissionPolicy, ResourceSnapshot, TradingSession

    claims = LabClaimSpool(tmp_path / "claims")
    child_pid_path = tmp_path / "term-exit-child.pid"
    grandchild_pid_path = tmp_path / "term-ignore-grandchild.pid"
    claim = _short_live_claim()
    claims.publish(claim)
    worker = _worker(
        tmp_path,
        claims=claims,
        registry=TermExitingLeaderProcessTreeRegistry(
            pid_path=child_pid_path,
            grandchild_pid_path=grandchild_pid_path,
        ),
        resource_recheck_interval_seconds=0.01,
        resource_snapshot_provider=StaticResourceSnapshotProvider(
            ResourceSnapshot(
                observed_at=NOW,
                session=TradingSession.MORNING,
                live_backlog_age_seconds=0,
                live_p95_latency_seconds=0,
                available_memory_bytes=16 * 1024**3,
                available_disk_bytes=100 * 1024**3,
                io_pressure_pct=0,
                cpu_load_pct=0,
                source_quota_remaining=0,
                live_healthy=True,
            )
        ),
        admission_policy_provider=StaticAdmissionPolicyProvider(
            AdmissionPolicy(
                allow_live_session=True,
                max_live_shard_duration_ms=600,
                max_live_backlog_age_seconds=10,
                max_live_p95_latency_seconds=5,
                min_available_memory_bytes=0,
                min_available_disk_bytes=0,
                max_io_pressure_pct=100,
                max_cpu_load_pct=100,
                max_expected_memory_bytes=8 * 1024**3,
                max_expected_disk_bytes=50 * 1024**3,
                max_expected_quota_units=0,
                retry_delay_seconds=60,
            )
        ),
        require_resource_admission=True,
    )

    grandchild_pid: int | None = None
    try:
        result = worker.run_once()
        grandchild_pid = int(grandchild_pid_path.read_text(encoding="ascii"))

        assert result.status == "stopped"
        _assert_process_gone(grandchild_pid, timeout_seconds=0.5)
    finally:
        if grandchild_pid is not None:
            _kill_process_if_alive(grandchild_pid)


def test_successful_shard_cleans_up_descendants_before_publishing(
    tmp_path: Path,
) -> None:
    claims = LabClaimSpool(tmp_path / "claims")
    child_pid_path = tmp_path / "successful-child.pid"
    grandchild_pid_path = tmp_path / "successful-grandchild.pid"
    claim = _short_live_claim()
    claims.publish(claim)
    worker = _worker(
        tmp_path,
        claims=claims,
        registry=SuccessfulProcessTreeRegistry(
            pid_path=child_pid_path,
            grandchild_pid_path=grandchild_pid_path,
        ),
    )

    grandchild_pid: int | None = None
    try:
        result = worker.run_once()
        grandchild_pid = int(grandchild_pid_path.read_text(encoding="ascii"))

        assert result.status == "succeeded"
        _assert_process_gone(int(child_pid_path.read_text(encoding="ascii")))
        _assert_process_gone(grandchild_pid, timeout_seconds=0.5)
    finally:
        if grandchild_pid is not None:
            _kill_process_if_alive(grandchild_pid)


def test_isolated_shard_uses_spawn_and_cleans_its_process_group(tmp_path: Path) -> None:
    claims = LabClaimSpool(tmp_path / "claims")
    start_method_path = tmp_path / "start-method.txt"
    claim = _short_live_claim()
    claims.publish(claim)
    worker = _worker(
        tmp_path,
        claims=claims,
        registry=SpawnMethodRegistry(pid_path=start_method_path, delay_seconds=0),
    )

    result = worker.run_once()

    assert result.status == "succeeded"
    assert start_method_path.read_text(encoding="ascii") == "spawn"


def test_unserializable_isolated_runtime_fails_closed_at_spawn_boundary(
    tmp_path: Path,
) -> None:
    claims = LabClaimSpool(tmp_path / "claims")
    claims.publish(_claim(_nshape_compare_spec(hold_days=(1,))))
    worker = _worker(tmp_path, claims=claims, registry=UnserializableRegistry())

    with pytest.raises(LabDaemonConfigurationError, match="spawn-serializable"):
        worker.run_once()


def test_blocked_resource_probe_cannot_delay_hard_deadline_cleanup(tmp_path: Path) -> None:
    from rquant.resource_admission import TradingSession

    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    child_pid_path = tmp_path / "blocked-probe-child.pid"
    first_probe_marker = tmp_path / "first-probe.marker"
    claim = _short_live_claim()
    claims.publish(claim)
    worker = _worker(
        tmp_path,
        claims=claims,
        reports=reports,
        registry=HungLiveRegistry(pid_path=child_pid_path),
        resource_recheck_interval_seconds=0.01,
        resource_probe_timeout_seconds=5,
        resource_snapshot_provider=PermanentlyBlockingAfterFirstSnapshotProvider(
            marker_path=first_probe_marker,
            snapshot=_healthy_resource_snapshot(session=TradingSession.MORNING),
            block_after_calls=2,
        ),
        admission_policy_provider=StaticAdmissionPolicyProvider(
            _permissive_admission_policy(max_live_shard_duration_ms=100)
        ),
        require_resource_admission=True,
    )

    outcomes: list[object] = []
    runner = threading.Thread(target=lambda: outcomes.append(worker.run_once()))
    runner.start()
    started_deadline = time.monotonic() + _child_startups(2)
    while not child_pid_path.exists() and time.monotonic() < started_deadline:
        time.sleep(0.01)
    assert child_pid_path.exists()
    live_started = time.monotonic()
    runner.join(timeout=_observe(1))
    elapsed = time.monotonic() - live_started

    assert not runner.is_alive()
    assert outcomes[0].status == "stopped"
    assert elapsed < _observe(0.6)
    _assert_process_gone(int(child_pid_path.read_text(encoding="ascii")))
    assert all(child.name != "lab-resource-probe" for child in multiprocessing.active_children())


def test_resource_probe_timeout_kills_spawned_descendant_process_group(tmp_path: Path) -> None:
    probe_pid_path = tmp_path / "resource-probe.pid"
    descendant_pid_path = tmp_path / "resource-probe-descendant.pid"
    worker = _worker(
        tmp_path,
        resource_snapshot_provider=SpawnDescendantBlockingResourceSnapshotProvider(
            probe_pid_path=probe_pid_path,
            descendant_pid_path=descendant_pid_path,
        ),
        admission_policy_provider=StaticAdmissionPolicyProvider(_permissive_admission_policy()),
        require_resource_admission=True,
    )

    try:
        started = time.monotonic()
        with pytest.raises(LabDaemonConfigurationError, match="timed out"):
            worker._bounded_resource_snapshot(timeout_seconds=1.5)
        elapsed = time.monotonic() - started

        assert elapsed < _probe_ceiling(1.5) + _observe(0.7)
        assert probe_pid_path.is_file()
        assert descendant_pid_path.is_file()
        _assert_process_gone(int(probe_pid_path.read_text(encoding="ascii")))
        _assert_process_gone(int(descendant_pid_path.read_text(encoding="ascii")))
    finally:
        for path in (probe_pid_path, descendant_pid_path):
            if path.is_file():
                _kill_process_if_alive(int(path.read_text(encoding="ascii")))


def test_resource_probe_cleanup_reaps_child_when_initial_join_raises(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from multiprocessing.process import BaseProcess

    probe_pid_path = tmp_path / "join-failure-resource-probe.pid"
    descendant_pid_path = tmp_path / "join-failure-resource-descendant.pid"
    worker = _worker(
        tmp_path,
        resource_snapshot_provider=SpawnDescendantBlockingResourceSnapshotProvider(
            probe_pid_path=probe_pid_path,
            descendant_pid_path=descendant_pid_path,
        ),
        admission_policy_provider=StaticAdmissionPolicyProvider(_permissive_admission_policy()),
        require_resource_admission=True,
    )
    original_join = BaseProcess.join
    join_failed = False

    def fail_first_nonblocking_join(
        process: BaseProcess,
        timeout: float | None = None,
    ) -> None:
        nonlocal join_failed
        if process.name == "lab-resource-probe" and timeout == 0 and not join_failed:
            join_failed = True
            raise OSError("initial join denied")
        original_join(process, timeout)

    monkeypatch.setattr(BaseProcess, "join", fail_first_nonblocking_join)

    try:
        with pytest.raises(BaseExceptionGroup) as captured:
            worker._bounded_resource_snapshot(timeout_seconds=1.5)

        assert join_failed is True
        errors: list[BaseException] = []

        def collect(error: BaseException) -> None:
            if isinstance(error, BaseExceptionGroup):
                for nested in error.exceptions:
                    collect(nested)
                return
            errors.append(error)

        collect(captured.value)
        assert any("initial join denied" in str(error) for error in errors)
        assert probe_pid_path.is_file()
        assert descendant_pid_path.is_file()
        _assert_process_gone(int(probe_pid_path.read_text(encoding="ascii")))
        _assert_process_gone(int(descendant_pid_path.read_text(encoding="ascii")))
        assert all(
            child.name != "lab-resource-probe" for child in multiprocessing.active_children()
        )
    finally:
        for path in (probe_pid_path, descendant_pid_path):
            if path.is_file():
                _kill_process_if_alive(int(path.read_text(encoding="ascii")))


def test_resource_probe_cleanup_retries_group_and_pid_kill_after_baseexceptions(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from multiprocessing.process import BaseProcess

    probe_pid_path = tmp_path / "kill-retry-resource-probe.pid"
    descendant_pid_path = tmp_path / "kill-retry-resource-descendant.pid"
    worker = _worker(
        tmp_path,
        resource_snapshot_provider=SpawnDescendantBlockingResourceSnapshotProvider(
            probe_pid_path=probe_pid_path,
            descendant_pid_path=descendant_pid_path,
        ),
        admission_policy_provider=StaticAdmissionPolicyProvider(_permissive_admission_policy()),
        require_resource_admission=True,
    )
    original_killpg = os.killpg
    original_process_kill = BaseProcess.kill
    group_kill_attempts = 0
    pid_kill_attempts = 0

    def interrupt_first_group_kill(group_id: int, sig: int) -> None:
        nonlocal group_kill_attempts
        if sig == signal.SIGKILL:
            group_kill_attempts += 1
            if group_kill_attempts == 1:
                raise KeyboardInterrupt("group kill interrupted")
        original_killpg(group_id, sig)

    def interrupt_first_pid_kill(process: BaseProcess) -> None:
        nonlocal pid_kill_attempts
        if process.name == "lab-resource-probe":
            pid_kill_attempts += 1
            if pid_kill_attempts == 1:
                raise KeyboardInterrupt("pid kill interrupted")
        original_process_kill(process)

    monkeypatch.setattr(os, "killpg", interrupt_first_group_kill)
    monkeypatch.setattr(BaseProcess, "kill", interrupt_first_pid_kill)

    group_id: int | None = None
    pids: tuple[int, ...] = ()
    try:
        with pytest.raises(BaseExceptionGroup) as captured:
            worker._bounded_resource_snapshot(timeout_seconds=1.5)

        assert probe_pid_path.is_file()
        assert descendant_pid_path.is_file()
        group_id = int(probe_pid_path.read_text(encoding="ascii"))
        pids = (
            group_id,
            int(descendant_pid_path.read_text(encoding="ascii")),
        )
        assert group_kill_attempts >= 2
        assert pid_kill_attempts >= 2
        errors = _collect_base_exceptions(captured.value)
        assert any("resource snapshot provider timed out" in str(error) for error in errors)
        assert any("group kill interrupted" in str(error) for error in errors)
        assert any("pid kill interrupted" in str(error) for error in errors)
        for pid in pids:
            _assert_process_gone(pid)
        assert all(
            child.name != "lab-resource-probe" for child in multiprocessing.active_children()
        )
    finally:
        _cleanup_spawned_probe_processes(group_id=group_id, pids=pids)


def test_isolated_shard_is_alive_baseexception_does_not_interrupt_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from multiprocessing.process import BaseProcess

    from rquant.lab_worker import _DeadlineWireEndpoint
    from rquant.resource_admission import TradingSession

    claims = LabClaimSpool(tmp_path / "claims")
    claim = _short_live_claim()
    claims.publish(claim)
    child_pid_path = tmp_path / "is-alive-baseexception-child.pid"
    worker = _worker(
        tmp_path,
        claims=claims,
        registry=HungLiveRegistry(pid_path=child_pid_path),
    )
    validated = worker._validate_closed_claim(claim)
    original_connection_close = _DeadlineWireEndpoint.close
    original_is_alive = BaseProcess.is_alive
    parent_pid = os.getpid()
    close_failed = False
    is_alive_failed = False

    def fail_first_parent_child_close(connection: _DeadlineWireEndpoint) -> None:
        nonlocal close_failed
        original_connection_close(connection)
        if os.getpid() == parent_pid and not close_failed:
            close_failed = True
            raise KeyboardInterrupt("child connection cleanup interrupted")

    def fail_first_is_alive(process: BaseProcess) -> bool:
        nonlocal is_alive_failed
        if (
            process.name.startswith("lab-shard-")
            and child_pid_path.is_file()
            and not is_alive_failed
        ):
            is_alive_failed = True
            raise BaseExceptionGroup(
                "is_alive interrupted",
                [KeyboardInterrupt("is_alive keyboard interrupt"), OSError("is_alive denied")],
            )
        return original_is_alive(process)

    monkeypatch.setattr(_DeadlineWireEndpoint, "close", fail_first_parent_child_close)
    monkeypatch.setattr(BaseProcess, "is_alive", fail_first_is_alive)

    try:
        with pytest.raises(BaseExceptionGroup) as captured:
            worker._execute_shard_isolated(
                claim,
                validated,
                runtime_code_sha="1" * 40,
                hard_limit_seconds=1,
                initial_session=TradingSession.CLOSED,
            )

        assert close_failed is True
        assert is_alive_failed is True
        errors = _collect_base_exceptions(captured.value)
        assert any("child connection cleanup interrupted" in str(error) for error in errors)
        assert any("is_alive keyboard interrupt" in str(error) for error in errors)
        assert any("is_alive denied" in str(error) for error in errors)
        if child_pid_path.is_file():
            _assert_process_gone(int(child_pid_path.read_text(encoding="ascii")))
        assert all(
            not child.name.startswith("lab-shard-") for child in multiprocessing.active_children()
        )
    finally:
        if child_pid_path.is_file():
            _kill_process_if_alive(int(child_pid_path.read_text(encoding="ascii")))


@pytest.mark.parametrize(
    "field_name",
    (
        "heartbeat_interval_seconds",
        "resource_recheck_interval_seconds",
        "resource_probe_timeout_seconds",
        "receipt_timeout_seconds",
        "quarantine_reconcile_interval_seconds",
    ),
)
@pytest.mark.parametrize("value", (float("nan"), float("inf"), float("-inf")))
def test_worker_rejects_non_finite_timing_inputs(
    tmp_path: Path,
    field_name: str,
    value: float,
) -> None:
    with pytest.raises(ValueError, match="finite"):
        _worker(tmp_path, **{field_name: value})


def test_worker_canonicalizes_internal_monotonic_boundaries_to_integer_microseconds(
    tmp_path: Path,
) -> None:
    worker = _worker(
        tmp_path,
        heartbeat_interval_seconds=0.00018000000000000004,
        resource_recheck_interval_seconds=0.00018000000000000004,
        resource_probe_timeout_seconds=0.00018000000000000004,
        receipt_timeout_seconds=0.00018000000000000004,
        quarantine_reconcile_interval_seconds=0.00018000000000000004,
        monotonic_clock=lambda: 0.00018000000000000004,
        isolation_monotonic_clock=lambda: 0.00018000000000000004,
    )

    assert worker.heartbeat_interval_microseconds == 180
    assert worker.resource_recheck_interval_microseconds == 180
    assert worker.resource_probe_timeout_microseconds == 180
    assert worker.receipt_timeout_microseconds == 180
    assert worker.quarantine_reconcile_interval_microseconds == 180
    assert worker.monotonic_microseconds_clock() == 180
    assert worker.isolation_monotonic_microseconds_clock() == 180


@pytest.mark.parametrize("timeout_seconds", (float("nan"), float("inf"), float("-inf")))
def test_bounded_resource_probe_rejects_non_finite_timeout_at_entry(
    tmp_path: Path,
    timeout_seconds: float,
) -> None:
    worker = _worker(
        tmp_path,
        resource_snapshot_provider=StaticResourceSnapshotProvider(_healthy_resource_snapshot()),
        admission_policy_provider=StaticAdmissionPolicyProvider(_permissive_admission_policy()),
        require_resource_admission=True,
    )

    with pytest.raises(ValueError, match="finite"):
        worker._bounded_resource_snapshot(timeout_seconds=timeout_seconds)


@pytest.mark.parametrize("hard_limit_seconds", (float("nan"), float("inf"), float("-inf")))
def test_isolated_shard_rejects_non_finite_hard_limit_before_spawn(
    tmp_path: Path,
    hard_limit_seconds: float,
) -> None:
    from rquant.resource_admission import TradingSession

    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    worker = _worker(tmp_path)
    validated = worker._validate_closed_claim(claim)

    with pytest.raises(ValueError, match="finite"):
        worker._execute_shard_isolated(
            claim,
            validated,
            runtime_code_sha="1" * 40,
            hard_limit_seconds=hard_limit_seconds,
            initial_session=TradingSession.MORNING,
        )


def test_fast_resource_probe_waits_for_parent_process_group_verification(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.lab_worker as lab_worker

    original_recv = lab_worker._recv_wire
    readiness_received = threading.Event()
    release_readiness = threading.Event()
    child_observed: list[bool] = []

    def delayed_readiness_recv(connection: object, **kwargs: object) -> object:
        value = original_recv(connection, **kwargs)
        if kwargs.get("model") is lab_worker._IsolationReadiness:
            readiness_received.set()
            if not release_readiness.wait(2):
                raise TimeoutError("test did not release resource readiness")
        return value

    def verify_child_then_release() -> None:
        if not readiness_received.wait(2):
            return
        child_observed.append(
            any(child.name == "lab-resource-probe" for child in multiprocessing.active_children())
        )
        release_readiness.set()

    monkeypatch.setattr(lab_worker, "_recv_wire", delayed_readiness_recv)
    worker = _worker(
        tmp_path,
        resource_probe_timeout_seconds=1.5,
        resource_snapshot_provider=StaticResourceSnapshotProvider(_healthy_resource_snapshot()),
        admission_policy_provider=StaticAdmissionPolicyProvider(_permissive_admission_policy()),
        require_resource_admission=True,
    )
    verifier = threading.Thread(target=verify_child_then_release)
    verifier.start()

    try:
        snapshot = worker._bounded_resource_snapshot(timeout_seconds=1.5)
    finally:
        release_readiness.set()
        verifier.join()

    assert snapshot == _healthy_resource_snapshot()
    assert child_observed == [True]
    assert all(child.name != "lab-resource-probe" for child in multiprocessing.active_children())


def test_deadline_crossed_while_store_opens_never_executes_adapter(tmp_path: Path) -> None:
    from rquant.resource_admission import TradingSession

    claims = LabClaimSpool(tmp_path / "claims")
    execution_marker = tmp_path / "adapter-executed.marker"
    claim = _short_live_claim()
    claims.publish(claim)
    worker = _worker(
        tmp_path,
        claims=claims,
        registry=SlowPidRegistry(pid_path=execution_marker, delay_seconds=0),
        exploratory_store_factory=SlowStoreFactory(delay_seconds=0.2),
        resource_snapshot_provider=StaticResourceSnapshotProvider(
            _healthy_resource_snapshot(session=TradingSession.MORNING)
        ),
        admission_policy_provider=StaticAdmissionPolicyProvider(
            _permissive_admission_policy(max_live_shard_duration_ms=100)
        ),
        require_resource_admission=True,
    )

    result = worker.run_once()

    assert result.status == "stopped"
    assert not execution_marker.exists()


def test_cleanup_failure_preserves_original_deadline_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.resource_admission import TradingSession

    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    claim = _short_live_claim()
    claims.publish(claim)
    worker = _worker(
        tmp_path,
        claims=claims,
        reports=reports,
        registry=HungLiveRegistry(pid_path=tmp_path / "cleanup-failure-child.pid"),
        resource_snapshot_provider=StaticResourceSnapshotProvider(
            _healthy_resource_snapshot(session=TradingSession.MORNING)
        ),
        admission_policy_provider=StaticAdmissionPolicyProvider(
            _permissive_admission_policy(max_live_shard_duration_ms=600)
        ),
        require_resource_admission=True,
    )
    original_terminate = worker._terminate_isolated_process

    def terminate_then_fail(*args: object, **kwargs: object) -> None:
        original_terminate(*args, **kwargs)
        process = args[0]
        if getattr(process, "name", "").startswith("lab-shard-"):
            raise OSError("cleanup denied")

    monkeypatch.setattr(worker, "_terminate_isolated_process", terminate_then_fail)

    result = worker.run_once()

    assert result.status == "failed"
    failure = _reports(reports)[-1].body
    assert isinstance(failure, LabShardFailed)
    assert "hard live execution limit" in failure.failure_json
    assert "cleanup denied" in failure.failure_json


def test_cleanup_failure_preserves_original_worker_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims.publish(claim)

    worker = _worker(
        tmp_path,
        claims=claims,
        reports=reports,
        registry=FailingWorkerRegistry(),
    )
    original_terminate = worker._terminate_isolated_process

    def terminate_then_fail(*args: object, **kwargs: object) -> None:
        original_terminate(*args, **kwargs)
        process = args[0]
        if getattr(process, "name", "").startswith("lab-shard-"):
            raise OSError("cleanup denied")

    monkeypatch.setattr(worker, "_terminate_isolated_process", terminate_then_fail)

    result = worker.run_once()

    assert result.status == "failed"
    failure = _reports(reports)[-1].body
    assert isinstance(failure, LabShardFailed)
    assert "worker exploded" in failure.failure_json
    assert "cleanup denied" in failure.failure_json


def test_resource_probe_close_failure_preserves_original_provider_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from multiprocessing.process import BaseProcess

    worker = _worker(
        tmp_path,
        resource_snapshot_provider=FailingResourceSnapshotProvider("probe exploded"),
        admission_policy_provider=StaticAdmissionPolicyProvider(_permissive_admission_policy()),
        require_resource_admission=True,
    )
    original_close = BaseProcess.close

    def close_then_fail(process: BaseProcess) -> None:
        original_close(process)
        if process.name == "lab-resource-probe":
            raise OSError("probe close denied")

    monkeypatch.setattr(BaseProcess, "close", close_then_fail)

    with pytest.raises(BaseExceptionGroup) as captured:
        worker._bounded_resource_snapshot(timeout_seconds=1)

    messages = tuple(str(error) for error in _collect_base_exceptions(captured.value))
    assert any("probe exploded" in message for message in messages)
    assert any("probe close denied" in message for message in messages)


@pytest.mark.parametrize(
    "close_failure",
    [OSError("child connection close denied"), KeyboardInterrupt("child connection close aborted")],
    ids=("oserror", "baseexception"),
)
def test_isolated_shard_child_connection_close_failure_cleans_started_process(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    close_failure: BaseException,
) -> None:
    from rquant.lab_worker import _DeadlineWireEndpoint
    from rquant.resource_admission import TradingSession

    claims = LabClaimSpool(tmp_path / "claims")
    claim = _short_live_claim()
    claims.publish(claim)
    child_pid_path = tmp_path / "close-failure-child.pid"
    worker = _worker(
        tmp_path,
        claims=claims,
        registry=SlowPidRegistry(pid_path=child_pid_path, delay_seconds=0),
    )
    validated = worker._validate_closed_claim(claim)
    original_close = _DeadlineWireEndpoint.close
    parent_pid = os.getpid()
    raised = False

    def close_then_fail(connection: _DeadlineWireEndpoint) -> None:
        nonlocal raised
        original_close(connection)
        if os.getpid() == parent_pid and not raised:
            raised = True
            raise close_failure

    monkeypatch.setattr(_DeadlineWireEndpoint, "close", close_then_fail)

    try:
        if isinstance(close_failure, Exception):
            control = worker._execute_shard_isolated(
                claim,
                validated,
                runtime_code_sha="1" * 40,
                hard_limit_seconds=1,
                initial_session=TradingSession.CLOSED,
            )
            assert control.resource_error is not None
            errors = _collect_base_exceptions(control.resource_error)
        else:
            with pytest.raises(BaseExceptionGroup) as captured:
                worker._execute_shard_isolated(
                    claim,
                    validated,
                    runtime_code_sha="1" * 40,
                    hard_limit_seconds=1,
                    initial_session=TradingSession.CLOSED,
                )
            errors = _collect_base_exceptions(captured.value)

        assert raised is True
        assert any("child connection close" in str(error) for error in errors)
        assert child_pid_path.is_file()
        _assert_process_gone(int(child_pid_path.read_text(encoding="ascii")))
        assert all(
            not child.name.startswith("lab-shard-") for child in multiprocessing.active_children()
        )
    finally:
        if child_pid_path.is_file():
            _kill_process_if_alive(int(child_pid_path.read_text(encoding="ascii")))


def test_isolated_shard_preserves_child_close_and_cleanup_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from multiprocessing.process import BaseProcess

    from rquant.lab_worker import _DeadlineWireEndpoint
    from rquant.resource_admission import TradingSession

    claims = LabClaimSpool(tmp_path / "claims")
    claim = _short_live_claim()
    claims.publish(claim)
    worker = _worker(
        tmp_path,
        claims=claims,
        registry=SlowPidRegistry(pid_path=tmp_path / "double-failure-child.pid", delay_seconds=0),
    )
    validated = worker._validate_closed_claim(claim)
    original_connection_close = _DeadlineWireEndpoint.close
    original_process_close = BaseProcess.close
    parent_pid = os.getpid()
    child_close_failed = False

    def fail_first_parent_child_close(connection: _DeadlineWireEndpoint) -> None:
        nonlocal child_close_failed
        original_connection_close(connection)
        if os.getpid() == parent_pid and not child_close_failed:
            child_close_failed = True
            raise OSError("child pipe close denied")

    def fail_process_cleanup(process: BaseProcess) -> None:
        original_process_close(process)
        if process.name.startswith("lab-shard-"):
            raise OSError("process sentinel close denied")

    monkeypatch.setattr(_DeadlineWireEndpoint, "close", fail_first_parent_child_close)
    monkeypatch.setattr(BaseProcess, "close", fail_process_cleanup)

    control = worker._execute_shard_isolated(
        claim,
        validated,
        runtime_code_sha="1" * 40,
        hard_limit_seconds=1,
        initial_session=TradingSession.CLOSED,
    )

    assert control.resource_error is not None
    messages = tuple(str(error) for error in _collect_base_exceptions(control.resource_error))
    assert any("child pipe close denied" in message for message in messages)
    assert any("process sentinel close denied" in message for message in messages)
    assert all(
        not child.name.startswith("lab-shard-") for child in multiprocessing.active_children()
    )


def test_setsid_failure_fails_closed_before_adapter_execution(
    tmp_path: Path,
) -> None:
    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    execution_marker = tmp_path / "adapter-executed.pid"
    claim = _short_live_claim()
    claims.publish(claim)
    worker = _worker(
        tmp_path,
        claims=claims,
        reports=reports,
        registry=SlowPidRegistry(pid_path=execution_marker, delay_seconds=0),
        isolation_session_initializer=FailingSessionInitializer(),
    )

    result = worker.run_once()

    failure = _reported_failure(reports)
    assert result.status == "failed"
    assert failure.phase == "session"
    assert failure.failure_kind == "session_startup"
    assert "setsid denied" in failure.message
    assert not execution_marker.exists()
    assert not worker.sealed_bundle_path(claim).exists()


def test_resource_retry_cache_prunes_revoked_replaced_and_consumed_claims(
    tmp_path: Path,
) -> None:
    from rquant.resource_admission import AdmissionPolicy, TradingSession

    claims = LabClaimSpool(tmp_path / "claims")
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims.publish(claim)
    snapshot_provider = MutableResourceSnapshotProvider(
        _healthy_resource_snapshot(session=TradingSession.MORNING),
        _healthy_resource_snapshot(session=TradingSession.POST_MARKET),
    )
    worker = _worker(
        tmp_path,
        claims=claims,
        resource_snapshot_provider=snapshot_provider,
        admission_policy_provider=StaticAdmissionPolicyProvider(
            AdmissionPolicy(
                allow_live_session=False,
                max_live_backlog_age_seconds=10,
                max_live_p95_latency_seconds=5,
                min_available_memory_bytes=0,
                min_available_disk_bytes=0,
                max_io_pressure_pct=100,
                max_cpu_load_pct=100,
                max_expected_memory_bytes=8 * 1024**3,
                max_expected_disk_bytes=50 * 1024**3,
                max_expected_quota_units=0,
                retry_delay_seconds=60,
            )
        ),
        require_resource_admission=True,
    )

    assert worker.run_once().status == "deferred"
    assert set(worker._resource_retry_at) == {claim.claim_token}
    claims.revoke(claim, reason="scheduler cancelled deferred claim")

    assert worker.run_once().status == "idle"
    assert worker._resource_retry_at == {}

    replacement = _retry_claim(claim)
    worker._resource_retry_at[claim.claim_token] = NOW + timedelta(hours=1)
    claims.publish(replacement)
    assert worker.run_once().status == "deferred"
    assert claim.claim_token not in worker._resource_retry_at
    assert replacement.claim_token in worker._resource_retry_at
    snapshot_provider.select(1)
    worker._resource_retry_at[replacement.claim_token] = NOW - timedelta(seconds=1)

    assert worker.run_once().status == "succeeded"
    assert replacement.claim_token not in worker._resource_retry_at


def test_resource_degradation_after_atomic_publish_rolls_back_before_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.resource_admission import AdmissionPolicy, TradingSession

    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims.publish(claim)
    snapshot_provider = MutableResourceSnapshotProvider(
        _healthy_resource_snapshot(session=TradingSession.MORNING),
        _healthy_resource_snapshot(session=TradingSession.MORNING).model_copy(
            update={"live_healthy": False}
        ),
    )
    worker = _worker(
        tmp_path,
        claims=claims,
        reports=reports,
        resource_snapshot_provider=snapshot_provider,
        admission_policy_provider=StaticAdmissionPolicyProvider(
            AdmissionPolicy(
                allow_live_session=True,
                max_live_shard_duration_ms=10**9,
                max_live_backlog_age_seconds=10,
                max_live_p95_latency_seconds=5,
                min_available_memory_bytes=0,
                min_available_disk_bytes=0,
                max_io_pressure_pct=100,
                max_cpu_load_pct=100,
                max_expected_memory_bytes=8 * 1024**3,
                max_expected_disk_bytes=50 * 1024**3,
                max_expected_quota_units=0,
                retry_delay_seconds=60,
            )
        ),
        require_resource_admission=True,
    )
    original_publish = worker._publish_candidate

    def publish_then_degrade(*args: object, **kwargs: object):
        bundle = original_publish(*args, **kwargs)
        snapshot_provider.select(1)
        return bundle

    monkeypatch.setattr(worker, "_publish_candidate", publish_then_degrade)

    result = worker.run_once()

    assert result.status == "stopped"
    assert not worker.sealed_bundle_path(claim).exists()
    assert isinstance(_reports(reports)[-1].body, LabWorkerStopped)
    assert not any(isinstance(report.body, LabShardSucceeded) for report in _reports(reports))


def test_post_publish_admission_timeout_rolls_back_without_false_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.resource_admission import AdmissionPolicy, TradingSession

    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims.publish(claim)
    worker = _worker(
        tmp_path,
        claims=claims,
        reports=reports,
        resource_snapshot_provider=StaticResourceSnapshotProvider(
            _healthy_resource_snapshot(session=TradingSession.MORNING)
        ),
        admission_policy_provider=StaticAdmissionPolicyProvider(
            AdmissionPolicy(
                allow_live_session=True,
                max_live_shard_duration_ms=10**9,
                max_live_backlog_age_seconds=10,
                max_live_p95_latency_seconds=5,
                min_available_memory_bytes=0,
                min_available_disk_bytes=0,
                max_io_pressure_pct=100,
                max_cpu_load_pct=100,
                max_expected_memory_bytes=8 * 1024**3,
                max_expected_disk_bytes=50 * 1024**3,
                max_expected_quota_units=0,
                retry_delay_seconds=60,
            )
        ),
        require_resource_admission=True,
    )
    published = threading.Event()
    prestarted_stages: list[object] = []
    original_publish = worker._publish_candidate
    original_evaluation = worker._resource_admission_evaluation
    original_prestart = worker._prestart_authority_stage

    def publish_then_mark(*args: object, **kwargs: object):
        bundle = original_publish(*args, **kwargs)
        published.set()
        return bundle

    def timeout_only_after_publish(*args: object, **kwargs: object):
        if published.is_set():
            raise TimeoutError("injected post-publish admission timeout")
        return original_evaluation(*args, **kwargs)

    def record_prestart(*args: object, **kwargs: object):
        stage = original_prestart(*args, **kwargs)
        prestarted_stages.append(stage)
        return stage

    monkeypatch.setattr(worker, "_publish_candidate", publish_then_mark)
    monkeypatch.setattr(worker, "_resource_admission_evaluation", timeout_only_after_publish)
    monkeypatch.setattr(worker, "_prestart_authority_stage", record_prestart)

    result = worker.run_once()

    assert result.status == "failed"
    assert not worker.sealed_bundle_path(claim).exists()
    assert isinstance(_reports(reports)[-1].body, LabShardFailed)
    assert not any(isinstance(report.body, LabShardSucceeded) for report in _reports(reports))
    assert worker._pending_success is None
    assert len(prestarted_stages) == 1
    stage = prestarted_stages[0]
    assert stage.cleanup_complete.is_set()
    assert stage.startup_complete.is_set()
    assert stage.startup_thread is not None
    assert not stage.startup_thread.is_alive()
    assert stage.child is None
    assert stage.owner == "closed"
    assert all(
        child.name != "lab-resource-authority" for child in multiprocessing.active_children()
    )


def test_prestarted_authority_cancel_during_handoff_reaps_owned_child(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.lab_worker as lab_worker

    worker = _worker(
        tmp_path,
        resource_snapshot_provider=StaticResourceSnapshotProvider(_healthy_resource_snapshot()),
        admission_policy_provider=StaticAdmissionPolicyProvider(_permissive_admission_policy()),
        require_resource_admission=True,
    )
    child_ready = multiprocessing.get_context("spawn").Event()
    child_pid_path = tmp_path / "prestarted-authority-handoff.pid"
    child_pid: list[int] = []
    handoff_entered = threading.Event()
    release_handoff = threading.Event()
    cancel_finished = threading.Event()

    def start_child(**_kwargs: object):
        process = multiprocessing.get_context("spawn").Process(
            target=_ignore_term_forever,
            args=(child_pid_path, child_ready),
            name="lab-prestarted-authority-handoff",
            daemon=False,
        )
        process.start()
        assert process.pid is not None
        assert child_ready.wait(timeout=_observe(2))
        receiver, sender = multiprocessing.Pipe(duplex=False)
        sender.close()
        child_pid.append(process.pid)
        return lab_worker._WireChild(
            process=process,
            connection=receiver,
            group_id=process.pid,
            address="test-prestarted-authority-handoff",
        )

    def pause_before_handoff(_stage: object, _child: object) -> None:
        handoff_entered.set()
        assert release_handoff.wait(timeout=_observe(2))

    monkeypatch.setattr(worker, "_start_wire_child", start_child)
    monkeypatch.setattr(
        worker,
        "_before_prestarted_authority_handoff_for_test",
        pause_before_handoff,
    )
    stage = worker._prestart_authority_stage(
        operation="admission",
        spec=None,
        admission_request=None,
        deadline_microseconds=lab_worker._monotonic_microseconds() + 2_000_000,
    )
    canceller = threading.Thread(
        target=lambda: (
            worker._cancel_prestarted_authority_stage(stage, operation="admission"),
            cancel_finished.set(),
        )
    )
    try:
        assert handoff_entered.wait(timeout=_observe(2))
        canceller.start()
        assert stage.cancelled.wait(timeout=_observe(2))
        release_handoff.set()
        assert cancel_finished.wait(timeout=_observe(2))
        canceller.join(timeout=_observe(2))

        assert stage.handoff.is_set()
        assert stage.cleanup_complete.is_set()
        assert stage.startup_complete.is_set()
        assert stage.startup_thread is not None
        assert not stage.startup_thread.is_alive()
        assert stage.child is None
        assert stage.owner == "closed"
        assert child_pid
        _assert_process_gone(child_pid[0])
        assert all(
            child.name != "lab-prestarted-authority-handoff"
            for child in multiprocessing.active_children()
        )
    finally:
        release_handoff.set()
        worker._cancel_prestarted_authority_stage(stage, operation="admission")
        canceller.join(timeout=_observe(2))
        if child_pid:
            _kill_process_if_alive(child_pid[0])


def test_prestarted_authority_cancel_before_child_start_is_terminal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.lab_worker as lab_worker

    worker = _worker(
        tmp_path,
        resource_snapshot_provider=StaticResourceSnapshotProvider(_healthy_resource_snapshot()),
        admission_policy_provider=StaticAdmissionPolicyProvider(_permissive_admission_policy()),
        require_resource_admission=True,
    )
    startup_entered = threading.Event()
    release_startup = threading.Event()
    cancel_finished = threading.Event()
    start_calls: list[object] = []

    def pause_before_start(_stage: object) -> None:
        startup_entered.set()
        assert release_startup.wait(timeout=_observe(2))

    monkeypatch.setattr(
        worker,
        "_before_prestarted_authority_start_for_test",
        pause_before_start,
    )
    monkeypatch.setattr(
        worker,
        "_start_wire_child",
        lambda **kwargs: start_calls.append(kwargs),
    )
    stage = worker._prestart_authority_stage(
        operation="admission",
        spec=None,
        admission_request=None,
        deadline_microseconds=lab_worker._monotonic_microseconds() + 2_000_000,
    )
    canceller = threading.Thread(
        target=lambda: (
            worker._cancel_prestarted_authority_stage(stage, operation="admission"),
            cancel_finished.set(),
        )
    )
    try:
        assert startup_entered.wait(timeout=_observe(2))
        canceller.start()
        assert stage.cancelled.wait(timeout=_observe(2))
        release_startup.set()
        assert cancel_finished.wait(timeout=_observe(2))
        canceller.join(timeout=_observe(2))

        assert start_calls == []
        assert stage.handoff.is_set()
        assert stage.cleanup_complete.is_set()
        assert stage.startup_complete.is_set()
        assert stage.startup_thread is not None
        assert not stage.startup_thread.is_alive()
        assert stage.child is None
        assert stage.owner == "closed"
    finally:
        release_startup.set()
        worker._cancel_prestarted_authority_stage(stage, operation="admission")
        canceller.join(timeout=_observe(2))


def test_prestarted_authority_start_failure_completes_terminal_events(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.lab_worker as lab_worker

    worker = _worker(
        tmp_path,
        resource_snapshot_provider=StaticResourceSnapshotProvider(_healthy_resource_snapshot()),
        admission_policy_provider=StaticAdmissionPolicyProvider(_permissive_admission_policy()),
        require_resource_admission=True,
    )
    monkeypatch.setattr(
        worker,
        "_start_wire_child",
        lambda **_kwargs: (_ for _ in ()).throw(OSError("injected authority start failure")),
    )
    stage = worker._prestart_authority_stage(
        operation="admission",
        spec=None,
        admission_request=None,
        deadline_microseconds=lab_worker._monotonic_microseconds() + 2_000_000,
    )

    assert stage.handoff.wait(timeout=_observe(2))
    worker._cancel_prestarted_authority_stage(stage, operation="admission")
    worker._cancel_prestarted_authority_stage(stage, operation="admission")

    assert stage.cleanup_complete.is_set()
    assert stage.startup_complete.is_set()
    assert stage.startup_thread is not None
    assert not stage.startup_thread.is_alive()
    assert isinstance(stage.error, OSError)
    assert stage.child is None
    assert stage.owner == "closed"


def test_prestarted_authority_cancel_after_handoff_is_idempotent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.lab_worker as lab_worker

    worker = _worker(
        tmp_path,
        resource_snapshot_provider=StaticResourceSnapshotProvider(_healthy_resource_snapshot()),
        admission_policy_provider=StaticAdmissionPolicyProvider(_permissive_admission_policy()),
        require_resource_admission=True,
    )
    child_ready = multiprocessing.get_context("spawn").Event()
    child_pid_path = tmp_path / "prestarted-authority-after-handoff.pid"
    child_pid: list[int] = []
    close_calls: list[object] = []
    original_close = worker._close_managed_authority_child

    def start_child(**_kwargs: object):
        process = multiprocessing.get_context("spawn").Process(
            target=_ignore_term_forever,
            args=(child_pid_path, child_ready),
            name="lab-prestarted-authority-after-handoff",
            daemon=False,
        )
        process.start()
        assert process.pid is not None
        assert child_ready.wait(timeout=_observe(2))
        receiver, sender = multiprocessing.Pipe(duplex=False)
        sender.close()
        child_pid.append(process.pid)
        return lab_worker._WireChild(
            process=process,
            connection=receiver,
            group_id=process.pid,
            address="test-prestarted-authority-after-handoff",
        )

    def record_close(*args: object, **kwargs: object) -> object:
        close_calls.append(args[0])
        return original_close(*args, **kwargs)

    monkeypatch.setattr(worker, "_start_wire_child", start_child)
    monkeypatch.setattr(worker, "_close_managed_authority_child", record_close)
    stage = worker._prestart_authority_stage(
        operation="admission",
        spec=None,
        admission_request=None,
        deadline_microseconds=lab_worker._monotonic_microseconds() + 2_000_000,
    )
    try:
        assert stage.handoff.wait(timeout=_observe(2))
        worker._cancel_prestarted_authority_stage(stage, operation="admission")
        worker._cancel_prestarted_authority_stage(stage, operation="admission")

        assert stage.cleanup_complete.is_set()
        assert stage.startup_complete.is_set()
        assert stage.startup_thread is not None
        assert not stage.startup_thread.is_alive()
        assert stage.child is None
        assert stage.owner == "closed"
        assert len(close_calls) == 1
        assert child_pid
        _assert_process_gone(child_pid[0])
        assert all(
            child.name != "lab-prestarted-authority-after-handoff"
            for child in multiprocessing.active_children()
        )
    finally:
        worker._cancel_prestarted_authority_stage(stage, operation="admission")
        if child_pid:
            _kill_process_if_alive(child_pid[0])


def test_managed_authority_reaper_retains_failed_handle_until_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.lab_worker as lab_worker

    class FakeProcess:
        def __init__(self) -> None:
            self.pid = 74123
            self.join_calls = 0
            self.close_calls = 0

        def join(self, *, timeout: float) -> None:
            del timeout
            self.join_calls += 1

        def close(self) -> None:
            self.close_calls += 1
            if self.close_calls == 1:
                raise OSError("injected process handle close failure")

    class FakeConnection:
        def __init__(self) -> None:
            self.closed = False

        def close(self) -> None:
            self.closed = True

    worker = _worker(tmp_path)
    process = FakeProcess()
    child = lab_worker._WireChild(
        process=process,
        connection=FakeConnection(),
        group_id=74123,
        address="managed-authority-retry",
    )
    managed = worker._register_authority_child(child, operation="admission", owner="direct")
    terminate_calls = 0

    def no_op_terminate(*_args: object, **_kwargs: object) -> None:
        nonlocal terminate_calls
        terminate_calls += 1

    monkeypatch.setattr(worker, "_terminate_isolated_process", no_op_terminate)

    first_error = worker._close_managed_authority_child(
        managed,
        owner="direct",
        label="test authority",
    )

    assert isinstance(first_error, OSError)
    assert managed.owner == "reap_pending"
    assert managed.cleanup_retry_count == 1
    assert managed.cleanup_error is first_error
    assert not managed.cleanup_complete.is_set()
    assert managed.os_process_exited_verified
    assert managed.ipc_closed
    assert not managed.process_handle_closed
    assert process.join_calls == 1
    assert terminate_calls == 1
    with worker._managed_authority_children_lock:
        assert worker._managed_authority_children == {74123: managed}

    worker.close()

    assert process.close_calls == 2
    assert process.join_calls == 1
    assert terminate_calls == 1
    assert managed.owner == "closed"
    assert managed.cleanup_complete.is_set()
    with worker._managed_authority_children_lock:
        assert worker._managed_authority_children == {}


def test_managed_authority_reaper_permanent_failure_keeps_handle(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.lab_worker as lab_worker

    class FakeProcess:
        pid = 74124

        def join(self, *, timeout: float) -> None:
            del timeout

        def close(self) -> None:
            return None

    class FailingConnection:
        closed = False

        def close(self) -> None:
            raise OSError("persistent IPC cleanup failure")

    worker = _worker(tmp_path)
    child = lab_worker._WireChild(
        process=FakeProcess(),
        connection=FailingConnection(),
        group_id=74124,
        address="managed-authority-permanent-failure",
    )
    managed = worker._register_authority_child(child, operation="admission", owner="direct")
    monkeypatch.setattr(worker, "_terminate_isolated_process", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(lab_worker, "_AUTHORITY_CHILD_CLEANUP_BUDGET_MICROSECONDS", 1)

    with pytest.raises(BaseExceptionGroup, match="authority child cleanup failed"):
        worker.close()

    assert managed.owner == "reap_pending"
    assert managed.cleanup_retry_count >= 1
    assert not managed.cleanup_complete.is_set()
    assert managed.os_process_exited_verified
    assert not managed.ipc_closed
    assert managed.process_handle_closed
    assert managed.last_errors
    with worker._managed_authority_children_lock:
        assert worker._managed_authority_children == {74124: managed}


def test_managed_authority_reaper_converges_after_ipc_close_error(
    tmp_path: Path,
) -> None:
    import rquant.lab_worker as lab_worker

    class CloseThenRaise:
        def __init__(self, connection: object) -> None:
            self.connection = connection

        @property
        def closed(self) -> bool:
            return bool(self.connection.closed)

        def close(self) -> None:
            self.connection.close()
            raise OSError("injected IPC close diagnostic")

    worker = _worker(tmp_path)
    ready = multiprocessing.get_context("spawn").Event()
    pid_path = tmp_path / "managed-authority-ipc-error.pid"
    process = multiprocessing.get_context("spawn").Process(
        target=_ignore_term_forever,
        args=(pid_path, ready),
        name="lab-resource-authority",
        daemon=False,
    )
    process.start()
    assert process.pid is not None
    process_id = process.pid
    assert ready.wait(timeout=_observe(2))
    receiver, sender = multiprocessing.Pipe(duplex=False)
    sender.close()
    managed = worker._register_authority_child(
        lab_worker._WireChild(
            process=process,
            connection=CloseThenRaise(receiver),
            group_id=process_id,
            address="managed-authority-ipc-error",
        ),
        operation="admission",
        owner="direct",
    )

    try:
        first_error = worker._close_managed_authority_child(
            managed,
            owner="direct",
            label="test authority",
        )

        assert isinstance(first_error, OSError)
        assert managed.os_process_exited_verified
        assert managed.ipc_closed
        assert managed.process_handle_closed
        assert managed.cleanup_complete.is_set()
        assert managed.owner == "closed"
        with worker._managed_authority_children_lock:
            assert worker._managed_authority_children == {}

        worker.close()
        _assert_process_gone(process_id)
        assert all(child.pid != process_id for child in multiprocessing.active_children())
    finally:
        _kill_process_if_alive(process_id)


def test_managed_authority_reaper_marks_closed_handle_before_post_close_interrupt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.lab_worker as lab_worker

    worker = _worker(tmp_path)
    ready = multiprocessing.get_context("spawn").Event()
    pid_path = tmp_path / "managed-authority-close-interrupt.pid"
    process = multiprocessing.get_context("spawn").Process(
        target=_ignore_term_forever,
        args=(pid_path, ready),
        name="lab-resource-authority",
        daemon=False,
    )
    process.start()
    assert process.pid is not None
    process_id = process.pid
    assert ready.wait(timeout=_observe(2))
    receiver, sender = multiprocessing.Pipe(duplex=False)
    sender.close()
    managed = worker._register_authority_child(
        lab_worker._WireChild(
            process=process,
            connection=receiver,
            group_id=process_id,
            address="managed-authority-close-interrupt",
        ),
        operation="admission",
        owner="direct",
    )
    monkeypatch.setattr(
        worker,
        "_after_managed_authority_process_close_for_test",
        lambda _managed: (_ for _ in ()).throw(KeyboardInterrupt("post-close interrupt")),
    )

    try:
        first_error = worker._close_managed_authority_child(
            managed,
            owner="direct",
            label="test authority",
        )

        assert isinstance(first_error, KeyboardInterrupt)
        assert managed.os_process_exited_verified
        assert managed.ipc_closed
        assert managed.process_handle_closed
        assert managed.cleanup_complete.is_set()
        assert managed.owner == "closed"
        with worker._managed_authority_children_lock:
            assert worker._managed_authority_children == {}

        worker.close()
        _assert_process_gone(process_id)
        assert all(child.pid != process_id for child in multiprocessing.active_children())
    finally:
        _kill_process_if_alive(process_id)


@pytest.mark.parametrize(
    "primary_error",
    (RuntimeError("primary"), TimeoutError("primary timeout"), KeyboardInterrupt("primary stop")),
)
def test_run_once_preserves_primary_before_cleanup_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    primary_error: BaseException,
) -> None:
    worker = _worker(tmp_path)
    reaper_calls = 0

    def reaper() -> None:
        nonlocal reaper_calls
        reaper_calls += 1
        if reaper_calls == 2:
            raise OSError("cleanup failed")

    def raise_primary(**_kwargs: object) -> object:
        raise primary_error

    monkeypatch.setattr(worker, "_reap_managed_authority_children", reaper)
    monkeypatch.setattr(worker, "_run_claim_once", raise_primary)

    with pytest.raises(BaseExceptionGroup) as raised:
        worker.run_once()

    assert raised.value.exceptions[0] is primary_error
    assert type(raised.value.exceptions[1]) is OSError
    assert str(raised.value.exceptions[1]) == "cleanup failed"


def test_run_once_preserves_primary_before_reservation_release_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker = _worker(tmp_path)
    primary_error = RuntimeError("primary")
    worker._active_resource_reservation = object()  # type: ignore[assignment]

    def raise_primary(**_kwargs: object) -> object:
        raise primary_error

    monkeypatch.setattr(worker, "_run_claim_once", raise_primary)
    monkeypatch.setattr(
        worker,
        "_release_resource_reservation",
        lambda: (_ for _ in ()).throw(OSError("reservation release failed")),
    )

    with pytest.raises(BaseExceptionGroup) as raised:
        worker.run_once()

    assert raised.value.exceptions[0] is primary_error
    assert type(raised.value.exceptions[1]) is OSError
    assert str(raised.value.exceptions[1]) == "reservation release failed"


def test_run_once_does_not_return_success_when_final_reap_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.lab_worker import LabWorkerTickResult

    worker = _worker(tmp_path)
    reaper_calls = 0

    def reaper() -> None:
        nonlocal reaper_calls
        reaper_calls += 1
        if reaper_calls == 2:
            raise OSError("final cleanup failed")

    monkeypatch.setattr(worker, "_reap_managed_authority_children", reaper)
    monkeypatch.setattr(
        worker,
        "_run_claim_once",
        lambda **_kwargs: LabWorkerTickResult(status="succeeded"),
    )

    with pytest.raises(OSError, match="final cleanup failed"):
        worker.run_once()


def test_post_publish_rollback_failure_reports_failure_without_false_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.lab_worker import LabArtifactConflictError
    from rquant.resource_admission import AdmissionPolicy, TradingSession

    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims.publish(claim)
    snapshot_provider = MutableResourceSnapshotProvider(
        _healthy_resource_snapshot(session=TradingSession.MORNING),
        _healthy_resource_snapshot(session=TradingSession.MORNING).model_copy(
            update={"live_healthy": False}
        ),
    )
    worker = _worker(
        tmp_path,
        claims=claims,
        reports=reports,
        resource_snapshot_provider=snapshot_provider,
        admission_policy_provider=StaticAdmissionPolicyProvider(
            AdmissionPolicy(
                allow_live_session=True,
                max_live_shard_duration_ms=10**9,
                max_live_backlog_age_seconds=10,
                max_live_p95_latency_seconds=5,
                min_available_memory_bytes=0,
                min_available_disk_bytes=0,
                max_io_pressure_pct=100,
                max_cpu_load_pct=100,
                max_expected_memory_bytes=8 * 1024**3,
                max_expected_disk_bytes=50 * 1024**3,
                max_expected_quota_units=0,
                retry_delay_seconds=60,
            )
        ),
        require_resource_admission=True,
    )
    original_publish = worker._publish_candidate

    def publish_then_degrade(*args: object, **kwargs: object):
        bundle = original_publish(*args, **kwargs)
        snapshot_provider.select(1)
        return bundle

    monkeypatch.setattr(worker, "_publish_candidate", publish_then_degrade)
    monkeypatch.setattr(
        worker,
        "_rollback_sealed",
        lambda _claim, _bundle: (_ for _ in ()).throw(
            LabArtifactConflictError("injected rollback failure")
        ),
    )

    result = worker.run_once()

    assert result.status == "failed"
    assert worker.sealed_bundle_path(claim).is_dir()
    assert isinstance(_reports(reports)[-1].body, LabShardFailed)
    assert not any(isinstance(report.body, LabShardSucceeded) for report in _reports(reports))
    assert worker._pending_success is None


def test_isolated_resource_admission_fails_closed_without_or_on_failed_provider(
    tmp_path: Path,
) -> None:
    from rquant.lab_worker import LabWorker
    from rquant.resource_admission import AdmissionPolicy

    common = {
        "worker_id": "worker-a",
        "claim_spool": LabClaimSpool(tmp_path / "claims"),
        "report_spool": LabReportSpool(tmp_path / "reports"),
        "artifact_root": tmp_path / "artifacts",
        "verified_code_sha_provider": lambda: "1" * 40,
        "require_resource_admission": True,
    }
    with pytest.raises(LabDaemonConfigurationError, match="resource admission providers"):
        LabWorker(**common)

    claims = common["claim_spool"]
    assert isinstance(claims, LabClaimSpool)
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims.publish(claim)
    policy = AdmissionPolicy(
        allow_live_session=False,
        max_live_backlog_age_seconds=10,
        max_live_p95_latency_seconds=5,
        min_available_memory_bytes=0,
        min_available_disk_bytes=0,
        max_io_pressure_pct=100,
        max_cpu_load_pct=100,
        max_expected_memory_bytes=8 * 1024**3,
        max_expected_disk_bytes=50 * 1024**3,
        max_expected_quota_units=0,
        retry_delay_seconds=60,
    )
    worker = _worker(
        tmp_path,
        claims=claims,
        reports=common["report_spool"],
        resource_snapshot_provider=FailingResourceSnapshotProvider("probe down"),
        admission_policy_provider=StaticAdmissionPolicyProvider(policy),
        require_resource_admission=True,
    )

    with pytest.raises(LabDaemonConfigurationError, match="resource snapshot provider failed"):
        worker.run_once()
    assert tuple(entry.claim for entry in claims.pending()) == (claim,)
    assert common["report_spool"].pending() == ()


def test_production_worker_requires_explicit_v2_resource_authority_manifest(
    tmp_path: Path,
) -> None:
    from rquant.lab_resource_authority_adapter import ResourceAuthorityAdapterConfig
    from rquant.lab_worker import (
        LabWorker,
        build_resource_journal_authority_manifest,
    )
    from rquant.runtime_resource_admission import StaticAdmissionPolicyProvider

    common = {
        "worker_id": "worker-a",
        "claim_spool": LabClaimSpool(tmp_path / "claims"),
        "report_spool": LabReportSpool(tmp_path / "reports"),
        "artifact_root": tmp_path / "artifacts",
        "verified_code_sha_provider": lambda: "1" * 40,
        "require_resource_admission": True,
        "production_mode": True,
    }
    with pytest.raises(LabDaemonConfigurationError, match="explicit V2"):
        LabWorker(
            **common,
            resource_snapshot_provider=SequenceResourceSnapshotProvider(
                _healthy_resource_snapshot()
            ),
            admission_policy_provider=StaticAdmissionPolicyProvider(_permissive_admission_policy()),
        )

    test_manifest = _test_authority_manifest(
        tmp_path,
        snapshot_provider=SequenceResourceSnapshotProvider(_healthy_resource_snapshot()),
        policy_provider=StaticAdmissionPolicyProvider(_permissive_admission_policy()),
        quota_provider=None,
    )
    with pytest.raises(LabDaemonConfigurationError, match="explicit V2"):
        LabWorker(**common, resource_authority_manifest=test_manifest)

    standalone_v2 = build_resource_journal_authority_manifest(
        ResourceAuthorityAdapterConfig(
            mode="test-standalone",
            endpoint=Path("/tmp/rqa-prod-gate.sock"),
            expected_uid=os.getuid(),
            expected_gid=os.getgid(),
            authority_id="test-resource-authority",
            trusted_role_inventory_hash="a" * 64,
        )
    )
    with pytest.raises(LabDaemonConfigurationError, match="production V2"):
        LabWorker(**common, resource_authority_manifest=standalone_v2)


def _retry_claim(claim: LabShardClaim) -> LabShardClaim:
    return LabShardClaim(
        job_id=claim.job_id,
        spec_hash=claim.spec_hash,
        definition=claim.definition,
        worker_id=claim.worker_id,
        claim_token=uuid4(),
        claim_generation=claim.claim_generation + 1,
        scheduler_fencing_token=claim.scheduler_fencing_token,
        claimed_at=claim.claimed_at + timedelta(minutes=1),
        lease_expires_at=claim.lease_expires_at + timedelta(minutes=1),
    )


def _reports(spool: LabReportSpool):
    return tuple(entry.report for entry in spool.pending())


def _reported_failure(spool: LabReportSpool) -> LabWorkerFailure:
    body = _reports(spool)[-1].body
    assert isinstance(body, LabShardFailed)
    return LabWorkerFailure.model_validate_json(body.failure_json)


def _sigterm_publication_child(root_value: str, phase: str) -> None:
    root = Path(root_value)
    claims = LabClaimSpool(root / "claims")
    reports = LabReportSpool(root / "reports")
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims.publish(claim)
    worker_ref: list[object] = []

    def receipt_waiter(
        report: LabWorkerReport,
        timeout_seconds: float,
        stop: object,
    ) -> LabReportReceipt:
        if phase == "after" and isinstance(report.body, LabShardSucceeded):
            worker = worker_ref[0]
            return worker._wait_for_receipt(report, timeout_seconds, stop)
        return _accept_report(report, timeout_seconds, stop)

    worker = _worker(
        root,
        claims=claims,
        reports=reports,
        receipt_waiter=receipt_waiter,
    )
    worker_ref.append(worker)
    original_publish = reports.publish
    signalled = False

    def signal_at_boundary(report: LabWorkerReport) -> object:
        nonlocal signalled
        before = phase == "before" and isinstance(report.body, LabShardHeartbeat)
        after = phase == "after" and isinstance(report.body, LabShardSucceeded)
        if not signalled and (before or after):
            signalled = True
            os.kill(os.getpid(), signal.SIGTERM)
        return original_publish(report)

    reports.publish = signal_at_boundary  # type: ignore[method-assign]
    signal.signal(signal.SIGTERM, lambda _signum, _frame: worker.request_stop())
    result = worker.run_once()
    bodies = tuple(report.body for report in _reports(reports))
    (root / "result.json").write_text(
        json.dumps(
            {
                "failed": sum(isinstance(body, LabShardFailed) for body in bodies),
                "sealed": worker.sealed_bundle_path(claim).exists(),
                "status": result.status,
                "stopped": sum(isinstance(body, LabWorkerStopped) for body in bodies),
                "succeeded": sum(isinstance(body, LabShardSucceeded) for body in bodies),
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )


def _crash_after_atomic_rename_child(root_value: str) -> None:
    root = Path(root_value)
    claims = LabClaimSpool(root / "claims")
    reports = LabReportSpool(root / "reports")
    worker = _worker(root, claims=claims, reports=reports)
    original_publish = reports.publish

    def crash_before_success_publish(report: LabWorkerReport) -> object:
        if isinstance(report.body, LabShardSucceeded):
            os._exit(77)
        return original_publish(report)

    reports.publish = crash_before_success_publish  # type: ignore[method-assign]
    worker.run_once()
    os._exit(78)


def test_artifact_reclaimer_checks_runtime_inside_evidence_lock(tmp_path: Path) -> None:
    from rquant.lab_worker import LabArtifactReclaimer

    reports = LabReportSpool(tmp_path / "reports")
    calls = 0
    armed = False

    def mutation_guard() -> str:
        nonlocal calls, armed
        if not armed:
            return "1" * 40
        calls += 1
        if calls >= 2:
            raise LabDaemonConfigurationError("runtime drifted inside reclaim lock")
        return "1" * 40

    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=reports,
        mutation_guard=mutation_guard,
    )
    armed = True
    calls = 0
    claim = _claim(_nshape_compare_spec())
    ledger_dir = reclaimer._ledger_dir(claim)
    ledger_dir.mkdir(parents=True, exist_ok=True)

    mutation_guard()
    with pytest.raises(LabDaemonConfigurationError, match="inside reclaim lock"):
        reclaimer.reclaim(claim)

    assert ledger_dir.is_dir()


def test_worker_temporary_quarantine_is_fenced_inside_reclaimer_lock(
    tmp_path: Path,
) -> None:
    armed = False

    def runtime_guard() -> str:
        if armed:
            raise LabDaemonConfigurationError("runtime drifted inside quarantine lock")
        return "1" * 40

    worker = _worker(tmp_path, verified_code_sha_provider=runtime_guard)
    temporary = worker.artifact_root / "candidate-temporary"
    temporary.mkdir(mode=0o700)
    (temporary / "payload.json").write_text("{}", encoding="utf-8")
    armed = True

    with pytest.raises(LabDaemonConfigurationError, match="inside quarantine lock"):
        worker._cleanup_temporary(temporary)

    assert temporary.is_dir()
    assert tuple(worker.artifact_reclaimer.garbage_deferred_dir.iterdir()) == ()
    assert tuple(worker.artifact_reclaimer.garbage_intent_dir.iterdir()) == ()


def _crash_reclaimer_after_tombstone_rename_child(
    root_value: str,
    claim_payload: str,
) -> None:
    from rquant.lab_worker import LabArtifactReclaimer

    root = Path(root_value)
    reclaimer = LabArtifactReclaimer(
        artifact_root=root / "artifacts",
        report_spool=LabReportSpool(root / "reports"),
    )
    original_delete = reclaimer._delete_isolated_tombstone

    def crash_before_tombstone_delete(path: Path, ledger: object) -> None:
        if path.name.startswith(".reclaim-"):
            os._exit(79)
        original_delete(path, ledger)

    reclaimer._delete_isolated_tombstone = crash_before_tombstone_delete  # type: ignore[method-assign]
    reclaimer.reclaim(LabShardClaim.model_validate_json(claim_payload))
    os._exit(80)


def _crash_reclaimer_after_prepared_ledger_child(
    root_value: str,
    claim_payload: str,
) -> None:
    import rquant.lab_worker as lab_worker_module
    from rquant.lab_worker import LabArtifactReclaimer

    root = Path(root_value)
    reclaimer = LabArtifactReclaimer(
        artifact_root=root / "artifacts",
        report_spool=LabReportSpool(root / "reports"),
    )
    original_rename = lab_worker_module.os.rename

    def crash_before_isolation(source: Path, target: Path) -> None:
        if Path(target).name.startswith(".reclaim-v1-"):
            os._exit(81)
        original_rename(source, target)

    lab_worker_module.os.rename = crash_before_isolation
    reclaimer.reclaim(LabShardClaim.model_validate_json(claim_payload))
    os._exit(82)


def _crash_sealed_rollback_after_payload_isolation_child(
    root_value: str,
    claim_payload: str,
) -> None:
    from rquant.lab_worker import LabSealedShardBundle

    root = Path(root_value)
    claim = LabShardClaim.model_validate_json(claim_payload)
    worker = _worker(root)
    sealed = worker.sealed_bundle_path(claim)
    manifest = worker._validate_bundle(sealed, claim)
    device, inode = worker._bundle_file_identity(sealed)
    bundle = LabSealedShardBundle(
        path=sealed,
        manifest=manifest,
        created=True,
        device=device,
        inode=inode,
    )
    original_promote = worker.artifact_reclaimer._promote_garbage_bundle

    def crash_before_deferred_gc(source: Path, target: Path) -> None:
        if source.parent == worker.artifact_reclaimer.garbage_staging_dir:
            os._exit(83)
        original_promote(source, target)

    worker.artifact_reclaimer._promote_garbage_bundle = crash_before_deferred_gc  # type: ignore[method-assign]
    worker._rollback_sealed(claim, bundle)
    os._exit(84)


def _crash_sealed_rollback_after_prepared_phase_child(
    root_value: str,
    claim_payload: str,
    crash_phase: str,
) -> None:
    from rquant.lab_worker import LabSealedShardBundle

    root = Path(root_value)
    claim = LabShardClaim.model_validate_json(claim_payload)
    worker = _worker(root)
    sealed = worker.sealed_bundle_path(claim)
    manifest = worker._validate_bundle(sealed, claim)
    device, inode = worker._bundle_file_identity(sealed)
    bundle = LabSealedShardBundle(
        path=sealed,
        manifest=manifest,
        created=True,
        device=device,
        inode=inode,
    )
    reclaimer = worker.artifact_reclaimer
    method_by_phase = {
        "intent": "_write_prepared_intent",
        "staging": "_ensure_garbage_staging",
        "global_owner": "_ensure_global_garbage_owner",
        "bundle_owner": "_ensure_bundle_garbage_owner",
        "prepared_ledger": "_write_garbage_ledger",
    }
    method_name = method_by_phase[crash_phase]
    original = getattr(reclaimer, method_name)

    def crash_after_phase(*args, **kwargs):
        result = original(*args, **kwargs)
        state = kwargs.get("state")
        if len(args) > 1:
            state = args[1]
        if crash_phase != "prepared_ledger" or state == "prepared":
            os._exit(85)
        return result

    setattr(reclaimer, method_name, crash_after_phase)
    worker._rollback_sealed(claim, bundle)
    os._exit(86)


def _publish_report_child(root_value: str, report_payload: str) -> None:
    root = Path(root_value)
    report = LabWorkerReport.model_validate_json(report_payload)
    LabReportSpool(root / "reports").publish(report)


def _run_worker_child(
    helper_name: str,
    root: Path,
    *arguments: str,
) -> subprocess.CompletedProcess[str]:
    source = (
        f"from tests.unit.test_lab_worker import {helper_name}; "
        f"{helper_name}(*__import__('sys').argv[1:])"
    )
    environment = os.environ.copy()
    repo_root = Path(__file__).parents[2]
    environment["PYTHONPATH"] = os.pathsep.join((str(repo_root / "src"), str(repo_root)))
    process = subprocess.Popen(
        [sys.executable, "-c", source, str(root), *arguments],
        cwd=Path(__file__).parents[2],
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        stdout, stderr = process.communicate(timeout=4)
    except subprocess.TimeoutExpired:
        process.kill()
        stdout, stderr = process.communicate(timeout=1)
        pytest.fail(f"worker child timed out: stdout={stdout!r} stderr={stderr!r}")
    return subprocess.CompletedProcess(
        process.args,
        process.returncode,
        stdout,
        stderr,
    )


def test_worker_consumes_only_its_owned_unexpired_claim(tmp_path: Path) -> None:
    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    registry = RecordingRegistry()
    wrong = _claim(_nshape_compare_spec()).model_copy(update={"worker_id": "worker-b"})
    owned = _claim(_nshape_compare_spec())
    claims.publish(wrong)
    claims.publish(owned)
    worker = _worker(
        tmp_path,
        registry=registry,
        claims=claims,
        reports=reports,
    )

    result = worker.run_once()

    assert result.status == "succeeded"
    assert registry.executions == 1
    assert [entry.claim for entry in claims.pending()] == [wrong]
    report = _reports(reports)[-1]
    assert isinstance(report.body, LabShardSucceeded)
    assert report.job_id == owned.job_id
    assert report.spec_hash == owned.spec_hash
    assert report.payload_hash == owned.payload_hash
    assert report.claim_generation == owned.claim_generation
    assert report.scheduler_fencing_token == owned.scheduler_fencing_token
    assert report.claim_token == owned.claim_token
    assert report.worker_id == owned.worker_id
    assert report.body.result_manifest_schema_version == CURRENT_RESULT_MANIFEST_SCHEMA_VERSION
    assert report.body.content_digest_algorithm == CURRENT_CONTENT_DIGEST_ALGORITHM
    assert report.body.worker_code_sha == "1" * 40


def test_worker_runtime_drift_before_claim_boundary_does_not_consume_claim(
    tmp_path: Path,
) -> None:
    claims = LabClaimSpool(tmp_path / "claims")
    claim = _claim(_nshape_compare_spec())
    claims.publish(claim)
    armed = False

    def runtime_guard() -> str:
        if armed:
            raise LabDaemonConfigurationError("runtime checkout drifted")
        return "1" * 40

    worker = _worker(
        tmp_path,
        claims=claims,
        verified_code_sha_provider=runtime_guard,
    )
    armed = True

    with pytest.raises(LabDaemonConfigurationError, match="drifted"):
        worker.run_once()

    assert [entry.claim for entry in claims.pending()] == [claim]


def test_worker_does_not_rehash_large_quarantine_for_each_claim(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    registry = RecordingRegistry()
    spec = _nshape_compare_spec(hold_days=(1,))
    for _ in range(2):
        claims.publish(_claim(spec))
    worker = _worker(
        tmp_path,
        claims=claims,
        reports=reports,
        registry=registry,
        quarantine_reconcile_interval_seconds=3_600,
    )
    hash_calls = 0

    def simulated_large_recovery(*, max_entries: int) -> None:
        nonlocal hash_calls
        assert max_entries == 16
        hash_calls += max_entries

    monkeypatch.setattr(
        worker.artifact_reclaimer,
        "recover_active",
        simulated_large_recovery,
    )

    first = worker.run_once()
    second = worker.run_once()

    assert first.status == "succeeded"
    assert second.status == "succeeded"
    assert hash_calls == 16
    assert registry.executions == 2


def test_unrelated_quarantine_recovery_failure_is_typed_and_does_not_block_claim(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    claims = LabClaimSpool(tmp_path / "claims")
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims.publish(claim)
    worker = _worker(tmp_path, claims=claims)

    def fail_recovery(*, max_entries: int) -> None:
        assert max_entries == 16
        raise RuntimeError("unrelated deferred quarantine is corrupt")

    monkeypatch.setattr(worker.artifact_reclaimer, "recover_active", fail_recovery)

    result = worker.run_once()

    assert result.status == "succeeded"
    assert len(result.health_warnings) == 1
    assert result.health_warnings[0].category == "quarantine_reconcile_failed"
    assert result.health_warnings[0].error_type == "RuntimeError"


@pytest.mark.parametrize("bundle_count", [1, 10, 40])
def test_bounded_quarantine_recovery_never_rehashes_deferred_payloads(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    bundle_count: int,
) -> None:
    from rquant.lab_worker import LabArtifactReclaimer

    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    for index in range(bundle_count):
        victim = tmp_path / "artifacts" / "cold" / f"result-{index:03d}.bin"
        victim.parent.mkdir(parents=True, exist_ok=True)
        victim.write_bytes(f"cold-{index}".encode())
        assert reclaimer.logical_quarantine_tree(victim, purpose=f"cold fixture {index}")

    inventory_calls = 0

    def reject_inventory(_path: Path) -> tuple[object, ...]:
        nonlocal inventory_calls
        inventory_calls += 1
        raise AssertionError("bounded recovery must not traverse deferred payload inventory")

    monkeypatch.setattr(reclaimer, "_garbage_inventory", reject_inventory)

    result = reclaimer.recover_active(max_entries=3)

    assert result.inspected + result.cold_metadata_checked <= 3
    assert result.cold_metadata_checked == 1
    archived = len(tuple(reclaimer.garbage_cold_intent_dir.iterdir()))
    pending_health = len(tuple(reclaimer.garbage_cold_health_dir.iterdir()))
    assert archived + pending_health == bundle_count
    assert inventory_calls == 0


def test_bounded_quarantine_recovery_is_fair_across_restarts(tmp_path: Path) -> None:
    from rquant.lab_worker import LabArtifactReclaimer

    artifact_root = tmp_path / "artifacts"
    reports = LabReportSpool(tmp_path / "reports")
    reclaimer = LabArtifactReclaimer(
        artifact_root=artifact_root,
        report_spool=reports,
    )
    victims: list[Path] = []
    for index in range(5):
        victim = artifact_root / "active" / f"result-{index:03d}.bin"
        victim.parent.mkdir(parents=True, exist_ok=True)
        victim.write_bytes(f"active-{index}".encode())
        owner = reclaimer._garbage_owner(victim, purpose=f"active fixture {index}")
        reclaimer._write_prepared_intent(reclaimer._prepared_intent(owner))
        victims.append(victim)

    for _ in range(5):
        restarted = LabArtifactReclaimer(
            artifact_root=artifact_root,
            report_spool=LabReportSpool(tmp_path / "reports"),
        )
        result = restarted.recover_active(max_entries=1)
        assert result.inspected == 1

    assert all(not victim.exists() for victim in victims)
    assert restarted.quarantine_summary().bundle_count == 5


def test_quarantine_recovery_uses_created_at_not_uuid_across_restarts(
    tmp_path: Path,
) -> None:
    from rquant.lab_worker import LabArtifactReclaimer, LabGarbageOwner

    artifact_root = tmp_path / "artifacts"
    reports = LabReportSpool(tmp_path / "reports")
    reclaimer = LabArtifactReclaimer(
        artifact_root=artifact_root,
        report_spool=reports,
    )
    fixtures: list[tuple[LabGarbageOwner, Path]] = []
    for index in range(121):
        victim = artifact_root / "active-created-at" / f"result-{index:03d}.bin"
        victim.parent.mkdir(parents=True, exist_ok=True)
        victim.write_bytes(f"active-{index}".encode())
        fixtures.append((reclaimer._garbage_owner(victim, purpose="fairness fixture"), victim))
    fixtures.sort(key=lambda item: item[0].garbage_id.hex)
    old_owner, old_victim = fixtures[-1]
    reclaimer._write_prepared_intent(
        reclaimer._prepared_intent(old_owner, created_at=NOW),
    )

    old_processed_at: int | None = None
    for index, (owner, _victim) in enumerate(fixtures[:-1], start=1):
        reclaimer._write_prepared_intent(
            reclaimer._prepared_intent(
                owner,
                created_at=NOW + timedelta(seconds=index),
            )
        )
        restarted = LabArtifactReclaimer(
            artifact_root=artifact_root,
            report_spool=LabReportSpool(tmp_path / "reports"),
        )
        restarted.recover_active(max_entries=1)
        if old_processed_at is None and not old_victim.exists():
            old_processed_at = index

    assert old_processed_at is not None, "oldest active intent was starved by newer UUIDs"
    assert old_processed_at <= 3


def test_quarantine_recovery_does_not_parse_cold_archive(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.lab_worker import LabArtifactReclaimer

    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    for index in range(10_000):
        marker = reclaimer.garbage_cold_intent_dir / (
            f"{UUID(int=index + 1).hex}-prepared-intent-v1.json"
        )
        marker.write_bytes(b"cold history must not be parsed")
    original_load = reclaimer._load_prepared_intent

    def reject_cold_parse(path: Path) -> object:
        if path.parent == reclaimer.garbage_cold_intent_dir:
            raise AssertionError("ordinary recovery parsed cold quarantine history")
        return original_load(path)

    monkeypatch.setattr(reclaimer, "_load_prepared_intent", reject_cold_parse)

    result = reclaimer.recover_active(max_entries=1)

    assert result.inspected == 0
    assert result.cold_metadata_checked == 0


def test_recovery_queue_bounds_ten_thousand_valid_cold_health_intents(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.lab_worker import (
        LabArtifactReclaimer,
        LabGarbageInventoryEntry,
        LabGarbageOwner,
        LabGarbagePreparedIntent,
        LabQuarantineQueueEntry,
        LabQuarantineQueueSequence,
    )

    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    first_intent: LabGarbagePreparedIntent | None = None
    for index in range(1, 10_001):
        owner = LabGarbageOwner(
            purpose=f"bounded cold health fixture {index}",
            original_relative_path=f"synthetic-cold/result-{index:05d}.bin",
            payload_type="regular",
            inventory=(
                LabGarbageInventoryEntry(
                    relative_path=".",
                    file_type="regular",
                    device=1,
                    inode=index,
                    size=1,
                    sha256=f"{index:064x}",
                ),
            ),
        )
        intent = LabGarbagePreparedIntent(
            source_relative_path=owner.original_relative_path,
            staging_relative_path=f".garbage-v1/staging/{owner.garbage_id.hex}",
            owner=owner,
            created_at=NOW + timedelta(microseconds=index),
        )
        if first_intent is None:
            first_intent = intent
        entry = LabQuarantineQueueEntry(
            sequence=index,
            phase="cold_health",
            intent=intent,
        )
        reclaimer._recovery_queue_path(index).write_text(
            entry.canonical_json(),
            encoding="utf-8",
        )
        reclaimer._prepared_intent_path(intent.owner.garbage_id).write_text(
            intent.canonical_json(),
            encoding="utf-8",
        )
        reclaimer._intent_marker_path(
            reclaimer.garbage_cold_health_dir,
            intent.owner.garbage_id,
        ).write_text(intent.canonical_json(), encoding="utf-8")
    assert first_intent is not None
    reclaimer._write_recovery_queue_sequence_locked(
        LabQuarantineQueueSequence(last_sequence=10_000)
    )
    prepared_intent_loads = 0
    original_load = reclaimer._load_prepared_intent

    def count_prepared_intent_load(path: Path) -> LabGarbagePreparedIntent:
        nonlocal prepared_intent_loads
        prepared_intent_loads += 1
        return original_load(path)

    queue_parses = 0
    original_entry_load = reclaimer._load_recovery_queue_entry
    original_marker_load = reclaimer._load_recovery_queue_marker

    def count_queue_entry(path: Path) -> LabQuarantineQueueEntry:
        nonlocal queue_parses
        queue_parses += 1
        return original_entry_load(path)

    def count_queue_marker(path: Path) -> LabQuarantineQueueEntry:
        nonlocal queue_parses
        queue_parses += 1
        return original_marker_load(path)

    recovery_metadata_reads = 0
    original_metadata_read = reclaimer._read_recovery_metadata

    def count_recovery_metadata_read(path: Path, *, label: str) -> str:
        nonlocal recovery_metadata_reads
        recovery_metadata_reads += 1
        return original_metadata_read(path, label=label)

    enumerations = 0
    verification_calls = 0
    payload_rehashes = 0
    original_scandir = os.scandir
    original_listdir = os.listdir
    hot_directories = {
        reclaimer.garbage_intent_dir,
        reclaimer.garbage_active_intent_dir,
        reclaimer.garbage_cold_health_dir,
        reclaimer.garbage_recovery_queue_pending_dir,
    }

    def count_hot_enumeration(path: os.PathLike[str] | str) -> object:
        nonlocal enumerations
        if Path(path) in hot_directories:
            enumerations += 1
        return original_scandir(path)

    def count_hot_listdir(path: os.PathLike[str] | str) -> list[str]:
        nonlocal enumerations
        if Path(path) in hot_directories:
            enumerations += 1
        return original_listdir(path)

    def count_verification(_bundle: Path, *, expected_owner: object) -> None:
        nonlocal verification_calls
        verification_calls += 1

    def reject_payload_rehash(_path: Path) -> tuple[object, ...]:
        nonlocal payload_rehashes
        payload_rehashes += 1
        raise AssertionError("ordinary recovery rehashed retained business payload")

    monkeypatch.setattr(reclaimer, "_load_prepared_intent", count_prepared_intent_load)
    monkeypatch.setattr(reclaimer, "_load_recovery_queue_entry", count_queue_entry)
    monkeypatch.setattr(reclaimer, "_load_recovery_queue_marker", count_queue_marker)
    monkeypatch.setattr(reclaimer, "_read_recovery_metadata", count_recovery_metadata_read)
    monkeypatch.setattr(os, "scandir", count_hot_enumeration)
    monkeypatch.setattr(os, "listdir", count_hot_listdir)
    monkeypatch.setattr(
        reclaimer,
        "_validate_deferred_bundle_metadata",
        count_verification,
    )
    monkeypatch.setattr(reclaimer, "_garbage_inventory", reject_payload_rehash)

    result = reclaimer.recover_active(max_entries=1)

    assert result.cold_metadata_checked == 1
    assert prepared_intent_loads == 3
    assert queue_parses == 4
    assert recovery_metadata_reads == 6
    assert enumerations == 0
    assert verification_calls == 1
    assert payload_rehashes == 0


@pytest.mark.parametrize("replacement_kind", ["symlink", "hardlink", "regular"])
def test_recovery_metadata_fd_rejects_directory_entry_aba(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    replacement_kind: str,
) -> None:
    from rquant.lab_worker import (
        LabArtifactConflictError,
        LabArtifactReclaimer,
        LabQuarantineQueueEntry,
        LabQuarantineQueueSequence,
    )

    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    victims = [
        tmp_path / "artifacts" / "metadata-aba" / "a.bin",
        tmp_path / "artifacts" / "metadata-aba" / "b.bin",
    ]
    victims[0].parent.mkdir(parents=True)
    victims[0].write_bytes(b"business-a")
    victims[1].write_bytes(b"business-b")
    intents = [
        reclaimer._prepared_intent(
            reclaimer._garbage_owner(victim, purpose=f"metadata aba {index}")
        )
        for index, victim in enumerate(victims)
    ]
    entries = [
        LabQuarantineQueueEntry(sequence=1, phase="active", intent=intent) for intent in intents
    ]
    target = reclaimer._recovery_queue_path(1)
    alternate = tmp_path / "alternate-entry.json"
    target.write_text(entries[0].canonical_json(), encoding="utf-8")
    alternate.write_text(entries[1].canonical_json(), encoding="utf-8")
    reclaimer._write_recovery_queue_sequence_locked(LabQuarantineQueueSequence(last_sequence=1))
    alternate_before = alternate.lstat()
    original_open = os.open
    attacked = False

    def open_with_aba(
        path: os.PathLike[str] | str,
        flags: int,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> int:
        nonlocal attacked
        descriptor = original_open(path, flags, mode, dir_fd=dir_fd)
        opens_target = (dir_fd is None and Path(path) == target) or (
            dir_fd is not None and Path(path) == Path(target.name)
        )
        if not attacked and opens_target:
            attacked = True
            held = target.with_suffix(".held")
            os.rename(target, held)
            if replacement_kind == "symlink":
                target.symlink_to(alternate)
            elif replacement_kind == "hardlink":
                os.link(alternate, target)
            else:
                target.write_bytes(alternate.read_bytes())
            target.unlink()
            os.rename(held, target)
        return descriptor

    monkeypatch.setattr(os, "open", open_with_aba)

    with pytest.raises(LabArtifactConflictError, match="changed while reading"):
        reclaimer._load_recovery_queue_entry(target)

    assert attacked
    assert [victim.read_bytes() for victim in victims] == [b"business-a", b"business-b"]
    alternate_after = alternate.lstat()
    assert (alternate_after.st_dev, alternate_after.st_ino, alternate_after.st_nlink) == (
        alternate_before.st_dev,
        alternate_before.st_ino,
        alternate_before.st_nlink,
    )


def test_recovery_queue_repairs_crash_before_enqueued_marker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.lab_worker import LabArtifactReclaimer

    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    victim = tmp_path / "artifacts" / "queue-crash" / "result.bin"
    victim.parent.mkdir(parents=True)
    victim.write_bytes(b"queue-before-marker")
    owner = reclaimer._garbage_owner(victim, purpose="queue marker crash fixture")
    intent = reclaimer._prepared_intent(owner)
    original_marker = reclaimer._ensure_recovery_queue_marker

    def interrupt_marker(_entry: object) -> None:
        raise InterruptedError("crash before enqueued marker")

    monkeypatch.setattr(reclaimer, "_ensure_recovery_queue_marker", interrupt_marker)
    with pytest.raises(InterruptedError, match="enqueued marker"):
        reclaimer._write_prepared_intent(intent)
    monkeypatch.setattr(reclaimer, "_ensure_recovery_queue_marker", original_marker)

    restarted = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    active = restarted.recover_active(max_entries=1)
    health = restarted.recover_active(max_entries=1)

    assert active.reconciled == 1
    assert health.cold_metadata_checked == 1
    assert not victim.exists()
    assert len(tuple(restarted.garbage_cold_intent_dir.iterdir())) == 1


def test_recovery_queue_repairs_crash_after_entry_before_sequence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.lab_worker import LabArtifactReclaimer, LabQuarantineQueueSequence

    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    victim = tmp_path / "artifacts" / "queue-sequence-crash" / "result.bin"
    victim.parent.mkdir(parents=True)
    victim.write_bytes(b"queue-before-sequence")
    owner = reclaimer._garbage_owner(victim, purpose="queue sequence crash fixture")
    intent = reclaimer._prepared_intent(owner)
    original_sequence_write = reclaimer._write_recovery_queue_sequence_locked

    def interrupt_sequence(_state: LabQuarantineQueueSequence) -> None:
        raise InterruptedError("crash before queue sequence")

    monkeypatch.setattr(
        reclaimer,
        "_write_recovery_queue_sequence_locked",
        interrupt_sequence,
    )
    with pytest.raises(InterruptedError, match="queue sequence"):
        reclaimer._write_prepared_intent(intent)
    monkeypatch.setattr(
        reclaimer,
        "_write_recovery_queue_sequence_locked",
        original_sequence_write,
    )

    assert reclaimer._recovery_queue_path(1).is_file()
    assert reclaimer._load_recovery_queue_sequence_locked().last_sequence == 0

    restarted = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    active = restarted.recover_active(max_entries=1)
    health = restarted.recover_active(max_entries=1)

    assert active.reconciled == 1
    assert health.cold_metadata_checked == 1
    assert not victim.exists()
    assert len(tuple(restarted.garbage_cold_intent_dir.iterdir())) == 1


def test_recovery_queue_repairs_crash_between_cold_enqueue_and_marker_move(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.lab_worker as lab_worker_module
    from rquant.lab_worker import LabArtifactReclaimer

    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    victim = tmp_path / "artifacts" / "queue-move-crash" / "result.bin"
    victim.parent.mkdir(parents=True)
    victim.write_bytes(b"queue-before-marker-move")
    original_rename = lab_worker_module.os.rename

    def interrupt_marker_move(source: Path, target: Path) -> None:
        if (
            Path(source).parent == reclaimer.garbage_active_intent_dir
            and Path(target).parent == reclaimer.garbage_cold_health_dir
        ):
            raise InterruptedError("crash before cold marker move")
        original_rename(source, target)

    monkeypatch.setattr(lab_worker_module.os, "rename", interrupt_marker_move)
    with pytest.raises(InterruptedError, match="cold marker move"):
        reclaimer.logical_quarantine_tree(victim, purpose="cold marker move crash fixture")
    monkeypatch.setattr(lab_worker_module.os, "rename", original_rename)

    restarted = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    assert restarted.recover_active(max_entries=1).reconciled == 1
    assert restarted.recover_active(max_entries=1).cold_metadata_checked == 1
    assert len(tuple(restarted.garbage_cold_intent_dir.iterdir())) == 1


def test_recovery_queue_restarts_after_health_check_before_queue_retirement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.lab_worker import LabArtifactReclaimer

    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    victim = tmp_path / "artifacts" / "queue-retire-crash" / "result.bin"
    victim.parent.mkdir(parents=True)
    victim.write_bytes(b"health-before-retirement")
    assert reclaimer.logical_quarantine_tree(victim, purpose="queue retirement crash fixture")
    assert reclaimer.recover_active(max_entries=1).reconciled == 1
    original_retire = reclaimer._retire_recovery_queue_entry

    def interrupt_retirement(entry: object) -> None:
        raise InterruptedError("crash before queue retirement")

    monkeypatch.setattr(reclaimer, "_retire_recovery_queue_entry", interrupt_retirement)
    with pytest.raises(InterruptedError, match="queue retirement"):
        reclaimer.recover_active(max_entries=1)
    monkeypatch.setattr(reclaimer, "_retire_recovery_queue_entry", original_retire)

    restarted = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    result = restarted.recover_active(max_entries=1)

    assert result.cold_metadata_checked == 1
    assert len(tuple(restarted.garbage_cold_intent_dir.iterdir())) == 1


@pytest.mark.parametrize("failure_kind", ["missing", "corrupt"])
def test_recovery_queue_dead_letters_then_repairs_committed_sequence(
    tmp_path: Path,
    failure_kind: str,
) -> None:
    from rquant.lab_worker import LabArtifactReclaimer

    artifact_root = tmp_path / "artifacts"
    reports_root = tmp_path / "reports"
    reclaimer = LabArtifactReclaimer(
        artifact_root=artifact_root,
        report_spool=LabReportSpool(reports_root),
    )
    victims = [
        artifact_root / "queue-conflict" / "first.bin",
        artifact_root / "queue-conflict" / "second.bin",
    ]
    victims[0].parent.mkdir(parents=True)
    victims[0].write_bytes(b"first-business-payload")
    victims[1].write_bytes(b"second-business-payload")
    intents = []
    for index, victim in enumerate(victims, start=1):
        owner = reclaimer._garbage_owner(victim, purpose=f"queue conflict {index}")
        intent = reclaimer._prepared_intent(owner, created_at=NOW + timedelta(seconds=index))
        reclaimer._write_prepared_intent(intent)
        intents.append(intent)
    first_delivery = reclaimer._recovery_queue_path(1)
    if failure_kind == "missing":
        first_delivery.unlink()
        corrupt_bytes = None
    else:
        corrupt_bytes = b"{corrupt queue delivery"
        first_delivery.write_bytes(corrupt_bytes)

    conflicted = reclaimer.recover_active(max_entries=1)
    healthy = reclaimer.recover_active(max_entries=1)

    assert conflicted.queue_conflicts == 1
    assert conflicted.inspected == 0
    assert healthy.reconciled == 1
    assert victims[0].read_bytes() == b"first-business-payload"
    assert not victims[1].exists()
    conflict = reclaimer._load_recovery_queue_conflict(reclaimer._recovery_queue_conflict_path(1))
    assert conflict.sequence == 1
    assert conflict.reason == f"{failure_kind}_pending"
    if corrupt_bytes is not None:
        assert first_delivery.read_bytes() == corrupt_bytes
        assert conflict.pending.raw_bytes == corrupt_bytes

    repaired = reclaimer.repair_recovery_queue_conflict(
        sequence=1,
        intent=intents[0],
        phase="active",
    )
    replayed = reclaimer.repair_recovery_queue_conflict(
        sequence=1,
        intent=intents[0],
        phase="active",
    )
    assert replayed == repaired
    assert repaired.new_sequence > 2

    for _ in range(8):
        reclaimer.recover_active(max_entries=1)
        if not victims[0].exists() and reclaimer.quarantine_summary().bundle_count == 2:
            break
    assert not victims[0].exists()
    assert reclaimer.quarantine_summary().bundle_count == 2
    if corrupt_bytes is not None:
        assert first_delivery.read_bytes() == corrupt_bytes


@pytest.mark.parametrize(
    "crash_stage",
    ["before_conflict", "after_conflict", "after_cursor"],
)
def test_recovery_queue_conflict_crash_boundaries_converge(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    crash_stage: str,
) -> None:
    from rquant.lab_worker import LabArtifactReclaimer, LabQuarantineQueueCursor

    artifact_root = tmp_path / "artifacts"
    reports_root = tmp_path / "reports"
    reclaimer = LabArtifactReclaimer(
        artifact_root=artifact_root,
        report_spool=LabReportSpool(reports_root),
    )
    victims = [
        artifact_root / "queue-conflict-crash" / "first.bin",
        artifact_root / "queue-conflict-crash" / "second.bin",
    ]
    victims[0].parent.mkdir(parents=True)
    intents = []
    for index, victim in enumerate(victims, start=1):
        victim.write_bytes(f"crash-{index}".encode())
        owner = reclaimer._garbage_owner(victim, purpose=f"queue conflict crash {index}")
        intent = reclaimer._prepared_intent(owner, created_at=NOW + timedelta(seconds=index))
        reclaimer._write_prepared_intent(intent)
        intents.append(intent)
    reclaimer._recovery_queue_path(1).unlink()
    original_conflict = reclaimer._ensure_recovery_queue_conflict_locked
    original_cursor = reclaimer._write_recovery_queue_cursor_locked

    def interrupt_conflict(sequence: int, *, reason: str) -> object:
        if crash_stage == "before_conflict":
            raise InterruptedError("crash before conflict publication")
        result = original_conflict(sequence, reason=reason)
        if crash_stage == "after_conflict":
            raise InterruptedError("crash after conflict publication")
        return result

    def interrupt_cursor(cursor: LabQuarantineQueueCursor) -> None:
        original_cursor(cursor)
        if crash_stage == "after_cursor" and cursor.last_sequence == 1:
            raise InterruptedError("crash after conflict cursor")

    monkeypatch.setattr(
        reclaimer,
        "_ensure_recovery_queue_conflict_locked",
        interrupt_conflict,
    )
    monkeypatch.setattr(
        reclaimer,
        "_write_recovery_queue_cursor_locked",
        interrupt_cursor,
    )
    with pytest.raises(InterruptedError, match="crash"):
        reclaimer.recover_active(max_entries=1)

    restarted = LabArtifactReclaimer(
        artifact_root=artifact_root,
        report_spool=LabReportSpool(reports_root),
    )
    first = restarted.recover_active(max_entries=1)
    second = restarted.recover_active(max_entries=1) if first.queue_conflicts else first
    assert second.reconciled == 1
    assert restarted._recovery_queue_conflict_path(1).is_file()

    restarted.repair_recovery_queue_conflict(
        sequence=1,
        intent=intents[0],
        phase="active",
    )
    for _ in range(8):
        restarted.recover_active(max_entries=1)
        if not any(victim.exists() for victim in victims):
            break
    assert not any(victim.exists() for victim in victims)
    assert restarted.quarantine_summary().bundle_count == 2


def test_recovery_queue_ambiguous_delivery_cannot_reassign_marker(
    tmp_path: Path,
) -> None:
    from rquant.lab_worker import LabArtifactConflictError, LabArtifactReclaimer

    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    victim = tmp_path / "artifacts" / "ambiguous-queue" / "result.bin"
    victim.parent.mkdir(parents=True)
    victim.write_bytes(b"ambiguous-business")
    owner = reclaimer._garbage_owner(victim, purpose="ambiguous queue fixture")
    intent = reclaimer._prepared_intent(owner)
    reclaimer._write_prepared_intent(intent)
    pending = reclaimer._recovery_queue_path(1)
    archived = reclaimer._recovery_queue_path(1, archived=True)
    archived.write_bytes(pending.read_bytes())
    marker = reclaimer._recovery_queue_enqueued_path(intent, "active")
    marker_bytes = marker.read_bytes()

    result = reclaimer.recover_active(max_entries=1)

    assert result.queue_conflicts == 1
    conflict = reclaimer._load_recovery_queue_conflict(reclaimer._recovery_queue_conflict_path(1))
    assert conflict.reason == "ambiguous_delivery"
    with pytest.raises(LabArtifactConflictError, match="ambiguous"):
        reclaimer.repair_recovery_queue_conflict(
            sequence=1,
            intent=intent,
            phase="active",
        )
    assert pending.is_file()
    assert archived.is_file()
    assert marker.read_bytes() == marker_bytes
    assert victim.read_bytes() == b"ambiguous-business"


@pytest.mark.parametrize("crash_stage", ["after_marker_archive", "after_requeue"])
def test_recovery_queue_repair_resumes_after_crash(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    crash_stage: str,
) -> None:
    from rquant.lab_worker import LabArtifactReclaimer

    artifact_root = tmp_path / "artifacts"
    reports_root = tmp_path / "reports"
    reclaimer = LabArtifactReclaimer(
        artifact_root=artifact_root,
        report_spool=LabReportSpool(reports_root),
    )
    victim = artifact_root / "queue-repair-crash" / "result.bin"
    victim.parent.mkdir(parents=True)
    victim.write_bytes(b"repair-crash-business")
    owner = reclaimer._garbage_owner(victim, purpose="queue repair crash fixture")
    intent = reclaimer._prepared_intent(owner)
    reclaimer._write_prepared_intent(intent)
    reclaimer._recovery_queue_path(1).unlink()
    assert reclaimer.recover_active(max_entries=1).queue_conflicts == 1
    original_enqueue = reclaimer._enqueue_recovery_intent

    def interrupt_requeue(*args: object, **kwargs: object) -> object:
        if crash_stage == "after_marker_archive":
            raise InterruptedError("crash after marker archive")
        entry = original_enqueue(*args, **kwargs)
        raise InterruptedError(f"crash after requeue {entry.sequence}")

    monkeypatch.setattr(reclaimer, "_enqueue_recovery_intent", interrupt_requeue)
    with pytest.raises(InterruptedError, match="crash after"):
        reclaimer.repair_recovery_queue_conflict(
            sequence=1,
            intent=intent,
            phase="active",
        )

    restarted = LabArtifactReclaimer(
        artifact_root=artifact_root,
        report_spool=LabReportSpool(reports_root),
    )
    repaired = restarted.repair_recovery_queue_conflict(
        sequence=1,
        intent=intent,
        phase="active",
    )
    assert repaired.new_sequence > 1
    for _ in range(4):
        restarted.recover_active(max_entries=1)
    assert not victim.exists()
    assert restarted.quarantine_summary().bundle_count == 1


def test_bounded_quarantine_recovery_migrates_legacy_intent_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.lab_worker import LabArtifactReclaimer, LabGarbagePreparedIntent

    artifact_root = tmp_path / "artifacts"
    reports = LabReportSpool(tmp_path / "reports")
    legacy = LabArtifactReclaimer(
        artifact_root=artifact_root,
        report_spool=reports,
    )
    legacy.garbage_queue_migration_complete_path.unlink()
    victim = artifact_root / "legacy-active" / "result.bin"
    victim.parent.mkdir(parents=True)
    victim.write_bytes(b"legacy-active")
    owner = legacy._garbage_owner(victim, purpose="legacy active fixture")
    intent = LabGarbagePreparedIntent(
        schema_version=1,
        source_relative_path=owner.original_relative_path,
        staging_relative_path=f".garbage-v1/staging/{owner.garbage_id.hex}",
        owner=owner,
    )
    legacy._prepared_intent_path(owner.garbage_id).write_text(
        intent.canonical_json(),
        encoding="utf-8",
    )

    restarted = LabArtifactReclaimer(
        artifact_root=artifact_root,
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    original_scan = restarted._recovery_intent_paths_locked

    def reject_ordinary_scan(_directory: Path) -> tuple[Path, ...]:
        raise AssertionError("ordinary recovery must not scan legacy authority")

    monkeypatch.setattr(restarted, "_recovery_intent_paths_locked", reject_ordinary_scan)
    before_migration = restarted.recover_active(max_entries=1)
    monkeypatch.setattr(restarted, "_recovery_intent_paths_locked", original_scan)

    assert before_migration.inspected == 0
    assert victim.exists()

    initialized = restarted.initialize_legacy_recovery_migration()
    migration = restarted.migrate_legacy_recovery_queue(max_entries=1)
    finalized = restarted.initialize_legacy_recovery_migration()
    result = restarted.recover_active(max_entries=1)
    health = restarted.recover_active(max_entries=1)

    assert initialized.indexed == 1
    assert migration.enqueued == 1
    assert finalized.complete
    assert result.inspected == 1
    assert result.reconciled == 1
    assert health.cold_metadata_checked == 1
    assert not victim.exists()
    assert tuple(restarted.garbage_active_intent_dir.iterdir()) == ()
    assert len(tuple(restarted.garbage_cold_intent_dir.iterdir())) == 1
    assert restarted.garbage_queue_migration_complete_path.is_file()


def test_old_migration_marker_does_not_hide_queue_index_migration(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.lab_worker import LabArtifactReclaimer, LabGarbagePreparedIntent

    artifact_root = tmp_path / "artifacts"
    reports_root = tmp_path / "reports"
    legacy = LabArtifactReclaimer(
        artifact_root=artifact_root,
        report_spool=LabReportSpool(reports_root),
    )
    cold_victim = artifact_root / "legacy-v2-cold" / "result.bin"
    cold_victim.parent.mkdir(parents=True)
    cold_victim.write_bytes(b"legacy-v2-cold")
    assert legacy.logical_quarantine_tree(cold_victim, purpose="legacy v2 cold fixture")

    for directory in (
        legacy.garbage_recovery_queue_pending_dir,
        legacy.garbage_recovery_queue_archive_dir,
        legacy.garbage_recovery_queue_enqueued_dir,
    ):
        for path in directory.iterdir():
            path.unlink()
    legacy.garbage_recovery_queue_sequence_path.unlink(missing_ok=True)
    legacy.garbage_recovery_queue_cursor_path.unlink(missing_ok=True)
    legacy.garbage_queue_migration_complete_path.unlink(missing_ok=True)
    old_queue_marker_identity = {"schema_version": 2, "state": "complete"}
    old_queue_marker_hash = hashlib.sha256(
        json.dumps(
            old_queue_marker_identity,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    legacy.garbage_queue_migration_legacy_complete_path.write_text(
        json.dumps(
            {**old_queue_marker_identity, "content_hash": old_queue_marker_hash},
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ),
        encoding="utf-8",
    )

    active_victim = artifact_root / "legacy-v2-active" / "result.bin"
    active_victim.parent.mkdir(parents=True)
    active_victim.write_bytes(b"legacy-v2-active")
    active_owner = legacy._garbage_owner(active_victim, purpose="legacy v2 active fixture")
    active_intent = LabGarbagePreparedIntent(
        source_relative_path=active_owner.original_relative_path,
        staging_relative_path=f".garbage-v1/staging/{active_owner.garbage_id.hex}",
        owner=active_owner,
        created_at=NOW,
    )
    legacy._prepared_intent_path(active_owner.garbage_id).write_text(
        active_intent.canonical_json(),
        encoding="utf-8",
    )
    legacy._intent_marker_path(
        legacy.garbage_active_intent_dir,
        active_owner.garbage_id,
    ).write_text(active_intent.canonical_json(), encoding="utf-8")
    old_marker_bytes = legacy.garbage_legacy_complete_path.read_bytes()
    old_queue_marker_bytes = legacy.garbage_queue_migration_legacy_complete_path.read_bytes()

    restarted = LabArtifactReclaimer(
        artifact_root=artifact_root,
        report_spool=LabReportSpool(reports_root),
    )
    original_scan = restarted._recovery_intent_paths_locked
    monkeypatch.setattr(
        restarted,
        "_recovery_intent_paths_locked",
        lambda _directory: pytest.fail("ordinary recovery scanned legacy intents"),
    )
    assert restarted.recover_active(max_entries=1).inspected == 0
    monkeypatch.setattr(restarted, "_recovery_intent_paths_locked", original_scan)

    initialized = restarted.initialize_legacy_recovery_migration()
    first = restarted.migrate_legacy_recovery_queue(max_entries=1)
    second = restarted.migrate_legacy_recovery_queue(max_entries=1)

    assert initialized.indexed == 2
    assert first.scanned == first.enqueued == 1
    assert second.scanned == second.enqueued == 1
    assert second.complete
    assert restarted.garbage_queue_migration_complete_path.is_file()
    assert restarted.garbage_legacy_complete_path.read_bytes() == old_marker_bytes
    assert (
        restarted.garbage_queue_migration_legacy_complete_path.read_bytes()
        == old_queue_marker_bytes
    )

    for _ in range(3):
        restarted.recover_active(max_entries=1)
    assert not active_victim.exists()
    assert not cold_victim.exists()
    assert len(tuple(restarted.garbage_cold_intent_dir.iterdir())) == 2


def test_legacy_queue_migration_consumes_ten_thousand_index_boundedly(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.lab_worker import (
        LabArtifactReclaimer,
        LabGarbageInventoryEntry,
        LabGarbageOwner,
        LabGarbagePreparedIntent,
    )

    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    reclaimer.garbage_queue_migration_complete_path.unlink(missing_ok=True)
    for index in range(1, 10_001):
        owner = LabGarbageOwner(
            purpose=f"legacy bounded migration {index}",
            original_relative_path=f"legacy-migration/result-{index:05d}.bin",
            payload_type="regular",
            inventory=(
                LabGarbageInventoryEntry(
                    relative_path=".",
                    file_type="regular",
                    device=1,
                    inode=index,
                    size=1,
                    sha256=f"{index:064x}",
                ),
            ),
        )
        intent = LabGarbagePreparedIntent(
            schema_version=1,
            source_relative_path=owner.original_relative_path,
            staging_relative_path=f".garbage-v1/staging/{owner.garbage_id.hex}",
            owner=owner,
        )
        payload = intent.canonical_json()
        reclaimer._prepared_intent_path(owner.garbage_id).write_text(payload, encoding="utf-8")
        reclaimer._intent_marker_path(
            reclaimer.garbage_active_intent_dir,
            owner.garbage_id,
        ).write_text(payload, encoding="utf-8")

    initialized = reclaimer.initialize_legacy_recovery_migration()
    intent_loads = 0
    index_parses = 0
    metadata_reads = 0
    enumerations = 0
    original_load = reclaimer._load_prepared_intent
    original_index_load = reclaimer._load_queue_migration_index_entry
    original_metadata_read = reclaimer._read_recovery_metadata
    original_scandir = os.scandir
    original_listdir = os.listdir

    def count_intent_load(path: Path) -> LabGarbagePreparedIntent:
        nonlocal intent_loads
        intent_loads += 1
        return original_load(path)

    def count_index_parse(cycle: object, index: int) -> object:
        nonlocal index_parses
        index_parses += 1
        return original_index_load(cycle, index)

    def count_metadata_read(path: Path, *, label: str) -> str:
        nonlocal metadata_reads
        metadata_reads += 1
        return original_metadata_read(path, label=label)

    def count_scandir(path: os.PathLike[str] | str) -> object:
        nonlocal enumerations
        if Path(path) in {
            reclaimer.garbage_intent_dir,
            reclaimer.garbage_active_intent_dir,
            reclaimer.garbage_cold_health_dir,
        }:
            enumerations += 1
        return original_scandir(path)

    def count_listdir(path: os.PathLike[str] | str) -> list[str]:
        nonlocal enumerations
        if Path(path) in {
            reclaimer.garbage_intent_dir,
            reclaimer.garbage_active_intent_dir,
            reclaimer.garbage_cold_health_dir,
        }:
            enumerations += 1
        return original_listdir(path)

    monkeypatch.setattr(reclaimer, "_load_prepared_intent", count_intent_load)
    monkeypatch.setattr(reclaimer, "_load_queue_migration_index_entry", count_index_parse)
    monkeypatch.setattr(reclaimer, "_read_recovery_metadata", count_metadata_read)
    monkeypatch.setattr(os, "scandir", count_scandir)
    monkeypatch.setattr(os, "listdir", count_listdir)

    first = reclaimer.migrate_legacy_recovery_queue(max_entries=1)
    second = reclaimer.migrate_legacy_recovery_queue(max_entries=1)

    assert initialized.indexed == 10_000
    assert first.scanned == first.enqueued == 1
    assert second.scanned == second.enqueued == 1
    assert intent_loads == 2
    assert index_parses == 6
    assert metadata_reads == 24
    assert enumerations == 0
    assert reclaimer._load_recovery_queue_cursor_locked().last_sequence == 0
    assert reclaimer._recovery_queue_path(1).is_file()
    assert reclaimer._recovery_queue_path(2).is_file()


def test_legacy_queue_migration_cycles_do_not_starve_new_lower_uuid(
    tmp_path: Path,
) -> None:
    from rquant.lab_worker import (
        LabArtifactReclaimer,
        LabGarbageInventoryEntry,
        LabGarbageOwner,
        LabGarbagePreparedIntent,
    )

    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    reclaimer.garbage_queue_migration_complete_path.unlink()
    fixtures = []
    for index in range(1, 4):
        owner = LabGarbageOwner(
            purpose=f"legacy cycle fairness {index}",
            original_relative_path=f"legacy-cycle/result-{index}.bin",
            payload_type="regular",
            inventory=(
                LabGarbageInventoryEntry(
                    relative_path=".",
                    file_type="regular",
                    device=1,
                    inode=index,
                    size=1,
                    sha256=f"{index:064x}",
                ),
            ),
        )
        intent = LabGarbagePreparedIntent(
            schema_version=1,
            source_relative_path=owner.original_relative_path,
            staging_relative_path=f".garbage-v1/staging/{owner.garbage_id.hex}",
            owner=owner,
        )
        fixtures.append((owner.garbage_id.hex, intent))
    fixtures.sort(key=lambda item: item[0])

    def publish_legacy(intent: LabGarbagePreparedIntent) -> None:
        payload = intent.canonical_json()
        reclaimer._prepared_intent_path(intent.owner.garbage_id).write_text(
            payload,
            encoding="utf-8",
        )
        reclaimer._intent_marker_path(
            reclaimer.garbage_active_intent_dir,
            intent.owner.garbage_id,
        ).write_text(payload, encoding="utf-8")

    publish_legacy(fixtures[1][1])
    publish_legacy(fixtures[2][1])
    assert reclaimer.initialize_legacy_recovery_migration().indexed == 2
    assert reclaimer.migrate_legacy_recovery_queue(max_entries=1).enqueued == 1

    publish_legacy(fixtures[0][1])
    drained = reclaimer.migrate_legacy_recovery_queue(max_entries=1)
    assert not drained.complete
    assert reclaimer.initialize_legacy_recovery_migration().indexed == 1
    final = reclaimer.migrate_legacy_recovery_queue(max_entries=1)

    assert final.complete
    for _name, intent in fixtures:
        assert reclaimer._recovery_queue_enqueued_path(intent, "active").is_file()


def test_legacy_queue_migration_cursor_resumes_after_restart(tmp_path: Path) -> None:
    from rquant.lab_worker import LabArtifactReclaimer, LabGarbagePreparedIntent

    artifact_root = tmp_path / "artifacts"
    reports_root = tmp_path / "reports"
    reclaimer = LabArtifactReclaimer(
        artifact_root=artifact_root,
        report_spool=LabReportSpool(reports_root),
    )
    reclaimer.garbage_queue_migration_complete_path.unlink()
    intents = []
    for index in range(2):
        victim = artifact_root / "legacy-restart" / f"result-{index}.bin"
        victim.parent.mkdir(parents=True, exist_ok=True)
        victim.write_bytes(f"legacy-restart-{index}".encode())
        owner = reclaimer._garbage_owner(victim, purpose=f"legacy restart {index}")
        intent = LabGarbagePreparedIntent(
            schema_version=1,
            source_relative_path=owner.original_relative_path,
            staging_relative_path=f".garbage-v1/staging/{owner.garbage_id.hex}",
            owner=owner,
        )
        payload = intent.canonical_json()
        reclaimer._prepared_intent_path(owner.garbage_id).write_text(payload, encoding="utf-8")
        reclaimer._intent_marker_path(
            reclaimer.garbage_active_intent_dir,
            owner.garbage_id,
        ).write_text(payload, encoding="utf-8")
        intents.append(intent)

    assert reclaimer.initialize_legacy_recovery_migration().indexed == 2
    assert reclaimer.migrate_legacy_recovery_queue(max_entries=1).enqueued == 1

    restarted = LabArtifactReclaimer(
        artifact_root=artifact_root,
        report_spool=LabReportSpool(reports_root),
    )
    second = restarted.migrate_legacy_recovery_queue(max_entries=1)

    assert second.scanned == second.enqueued == 1
    assert second.complete
    assert restarted._load_recovery_queue_sequence_locked().last_sequence == 2
    assert all(
        restarted._recovery_queue_enqueued_path(intent, "active").is_file() for intent in intents
    )


def test_legacy_queue_migration_rejects_canonical_duplicate_index_entry(
    tmp_path: Path,
) -> None:
    from rquant.lab_worker import (
        LabArtifactConflictError,
        LabArtifactReclaimer,
        LabGarbagePreparedIntent,
        LabQuarantineQueueMigrationIndexEntry,
    )

    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    reclaimer.garbage_queue_migration_complete_path.unlink()
    intents: list[LabGarbagePreparedIntent] = []
    for index in range(2):
        victim = reclaimer.artifact_root / "migration-index-integrity" / f"{index}.bin"
        victim.parent.mkdir(parents=True, exist_ok=True)
        victim.write_bytes(f"migration-index-integrity-{index}".encode())
        owner = reclaimer._garbage_owner(victim, purpose=f"index integrity {index}")
        intent = LabGarbagePreparedIntent(
            schema_version=1,
            source_relative_path=owner.original_relative_path,
            staging_relative_path=f".garbage-v1/staging/{owner.garbage_id.hex}",
            owner=owner,
        )
        payload = intent.canonical_json()
        reclaimer._prepared_intent_path(owner.garbage_id).write_text(payload, encoding="utf-8")
        reclaimer._intent_marker_path(
            reclaimer.garbage_active_intent_dir,
            owner.garbage_id,
        ).write_text(payload, encoding="utf-8")
        intents.append(intent)

    assert reclaimer.initialize_legacy_recovery_migration().indexed == 2
    cycle = reclaimer._load_active_queue_migration_cycle_locked()
    first = reclaimer._load_queue_migration_index_entry(cycle, 1)
    original_second = reclaimer._load_queue_migration_index_entry(cycle, 2)
    omitted = next(
        intent
        for intent in intents
        if original_second.file_name.startswith(intent.owner.garbage_id.hex)
    )
    replacement = LabQuarantineQueueMigrationIndexEntry(
        index=2,
        namespace=first.namespace,
        file_name=first.file_name,
        previous_chain_hash=first.chain_hash,
    )
    replacement_path = reclaimer._migration_index_path(cycle, 2)
    temporary = replacement_path.with_name("replacement.json")
    temporary.write_text(replacement.canonical_json(), encoding="utf-8")
    os.replace(temporary, replacement_path)

    assert reclaimer.migrate_legacy_recovery_queue(max_entries=1).enqueued == 1
    with pytest.raises(LabArtifactConflictError, match="migration index"):
        reclaimer.migrate_legacy_recovery_queue(max_entries=1)

    assert not reclaimer.garbage_queue_migration_complete_path.exists()
    assert not reclaimer._recovery_queue_enqueued_path(omitted, "active").exists()


def test_queue_migration_complete_marker_detects_post_observation_insertion(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.lab_worker import LabArtifactReclaimer, LabGarbagePreparedIntent

    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    reclaimer.garbage_queue_migration_complete_path.unlink()
    old_marker_bytes = reclaimer.garbage_legacy_complete_path.read_bytes()

    def publish_legacy(index: int) -> LabGarbagePreparedIntent:
        victim = reclaimer.artifact_root / "migration-complete-race" / f"{index}.bin"
        victim.parent.mkdir(parents=True, exist_ok=True)
        victim.write_bytes(f"migration-complete-race-{index}".encode())
        owner = reclaimer._garbage_owner(victim, purpose=f"complete race {index}")
        intent = LabGarbagePreparedIntent(
            schema_version=1,
            source_relative_path=owner.original_relative_path,
            staging_relative_path=f".garbage-v1/staging/{owner.garbage_id.hex}",
            owner=owner,
        )
        payload = intent.canonical_json()
        reclaimer._prepared_intent_path(owner.garbage_id).write_text(payload, encoding="utf-8")
        reclaimer._intent_marker_path(
            reclaimer.garbage_active_intent_dir,
            owner.garbage_id,
        ).write_text(payload, encoding="utf-8")
        return intent

    first = publish_legacy(1)
    assert reclaimer.initialize_legacy_recovery_migration().indexed == 1
    original_write_derived = reclaimer._write_derived_canonical_file
    inserted: list[LabGarbagePreparedIntent] = []

    def insert_during_marker_publish(target: Path, payload: str) -> None:
        if target == reclaimer.garbage_queue_migration_complete_path and not inserted:
            inserted.append(publish_legacy(2))
        original_write_derived(target, payload)

    monkeypatch.setattr(
        reclaimer,
        "_write_derived_canonical_file",
        insert_during_marker_publish,
    )
    raced = reclaimer.migrate_legacy_recovery_queue(max_entries=1)
    monkeypatch.setattr(
        reclaimer,
        "_write_derived_canonical_file",
        original_write_derived,
    )

    assert not raced.complete
    assert inserted
    assert not reclaimer.garbage_queue_migration_complete_path.exists()
    assert len(tuple(reclaimer.garbage_queue_migration_complete_archive_dir.iterdir())) == 1
    restarted = LabArtifactReclaimer(
        artifact_root=reclaimer.artifact_root,
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    assert restarted.initialize_legacy_recovery_migration().indexed == 1
    assert restarted.migrate_legacy_recovery_queue(max_entries=1).complete
    assert restarted._recovery_queue_enqueued_path(first, "active").is_file()
    assert restarted._recovery_queue_enqueued_path(inserted[0], "active").is_file()
    assert restarted.garbage_legacy_complete_path.read_bytes() == old_marker_bytes


@pytest.mark.parametrize(
    "tamper_case",
    [
        "canonical_first",
        "canonical_middle",
        "canonical_final",
        "duplicate",
        "reordered",
        "swapped",
        "wrong_previous",
    ],
)
def test_legacy_queue_migration_chain_rejects_index_tampering(
    tmp_path: Path,
    tamper_case: str,
) -> None:
    from rquant.lab_worker import (
        LabArtifactConflictError,
        LabArtifactReclaimer,
        LabGarbagePreparedIntent,
        LabQuarantineQueueMigrationIndexEntry,
    )

    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    reclaimer.garbage_queue_migration_complete_path.unlink()
    for index in range(3):
        victim = reclaimer.artifact_root / "migration-chain-tamper" / f"{index}.bin"
        victim.parent.mkdir(parents=True, exist_ok=True)
        victim.write_bytes(f"migration-chain-tamper-{index}".encode())
        owner = reclaimer._garbage_owner(victim, purpose=f"chain tamper {index}")
        intent = LabGarbagePreparedIntent(
            schema_version=1,
            source_relative_path=owner.original_relative_path,
            staging_relative_path=f".garbage-v1/staging/{owner.garbage_id.hex}",
            owner=owner,
        )
        payload = intent.canonical_json()
        reclaimer._prepared_intent_path(owner.garbage_id).write_text(payload, encoding="utf-8")
        reclaimer._intent_marker_path(
            reclaimer.garbage_active_intent_dir,
            owner.garbage_id,
        ).write_text(payload, encoding="utf-8")

    assert reclaimer.initialize_legacy_recovery_migration().indexed == 3
    cycle = reclaimer._load_active_queue_migration_cycle_locked()
    entries = [reclaimer._load_queue_migration_index_entry(cycle, index) for index in range(1, 4)]
    paths = [reclaimer._migration_index_path(cycle, index) for index in range(1, 4)]

    def replace(index: int, entry: LabQuarantineQueueMigrationIndexEntry) -> None:
        temporary = paths[index - 1].with_name(f"replacement-{tamper_case}.json")
        temporary.write_text(entry.canonical_json(), encoding="utf-8")
        os.replace(temporary, paths[index - 1])

    if tamper_case == "canonical_first":
        replace(
            1,
            LabQuarantineQueueMigrationIndexEntry(
                index=1,
                namespace=entries[1].namespace,
                file_name=entries[1].file_name,
                previous_chain_hash=entries[0].previous_chain_hash,
            ),
        )
    elif tamper_case == "canonical_middle":
        replace(
            2,
            LabQuarantineQueueMigrationIndexEntry(
                index=2,
                namespace=entries[0].namespace,
                file_name=entries[0].file_name,
                previous_chain_hash=entries[0].chain_hash,
            ),
        )
    elif tamper_case == "canonical_final":
        replace(
            3,
            LabQuarantineQueueMigrationIndexEntry(
                index=3,
                namespace=entries[0].namespace,
                file_name=entries[0].file_name,
                previous_chain_hash=entries[1].chain_hash,
            ),
        )
    elif tamper_case == "duplicate":
        replace(
            2,
            LabQuarantineQueueMigrationIndexEntry(
                index=2,
                namespace=entries[0].namespace,
                file_name=entries[0].file_name,
                previous_chain_hash=entries[0].chain_hash,
            ),
        )
    elif tamper_case == "reordered":
        replace(
            2,
            LabQuarantineQueueMigrationIndexEntry(
                index=2,
                namespace=entries[2].namespace,
                file_name=entries[2].file_name,
                previous_chain_hash=entries[0].chain_hash,
            ),
        )
    elif tamper_case == "swapped":
        first_raw = paths[0].read_bytes()
        second_raw = paths[1].read_bytes()
        first_temporary = paths[0].with_name("swapped-first.json")
        second_temporary = paths[1].with_name("swapped-second.json")
        first_temporary.write_bytes(second_raw)
        second_temporary.write_bytes(first_raw)
        os.replace(first_temporary, paths[0])
        os.replace(second_temporary, paths[1])
    else:
        replace(
            2,
            LabQuarantineQueueMigrationIndexEntry(
                index=2,
                namespace=entries[1].namespace,
                file_name=entries[1].file_name,
                previous_chain_hash=entries[0].previous_chain_hash,
            ),
        )

    with pytest.raises(LabArtifactConflictError, match="migration index"):
        for _ in range(3):
            reclaimer.migrate_legacy_recovery_queue(max_entries=1)
    assert not reclaimer.garbage_queue_migration_complete_path.exists()


def test_queue_migration_complete_marker_tracks_successive_cycles(tmp_path: Path) -> None:
    from rquant.lab_worker import (
        LabArtifactConflictError,
        LabArtifactReclaimer,
        LabGarbagePreparedIntent,
        LabQuarantineQueueMigrationComplete,
    )

    artifact_root = tmp_path / "artifacts"
    reports_root = tmp_path / "reports"
    reclaimer = LabArtifactReclaimer(
        artifact_root=artifact_root,
        report_spool=LabReportSpool(reports_root),
    )
    reclaimer.garbage_queue_migration_complete_path.unlink()
    completed_cycles: list[UUID] = []

    for index in range(3):
        victim = artifact_root / "successive-migration-cycles" / f"{index}.bin"
        victim.parent.mkdir(parents=True, exist_ok=True)
        victim.write_bytes(f"successive-migration-cycle-{index}".encode())
        owner = reclaimer._garbage_owner(victim, purpose=f"successive cycle {index}")
        intent = LabGarbagePreparedIntent(
            schema_version=1,
            source_relative_path=owner.original_relative_path,
            staging_relative_path=f".garbage-v1/staging/{owner.garbage_id.hex}",
            owner=owner,
        )
        payload = intent.canonical_json()
        reclaimer._prepared_intent_path(owner.garbage_id).write_text(payload, encoding="utf-8")
        reclaimer._intent_marker_path(
            reclaimer.garbage_active_intent_dir,
            owner.garbage_id,
        ).write_text(payload, encoding="utf-8")

        restarted = LabArtifactReclaimer(
            artifact_root=artifact_root,
            report_spool=LabReportSpool(reports_root),
        )
        initialized = restarted.initialize_legacy_recovery_migration()
        assert initialized.indexed == 1
        assert restarted.migrate_legacy_recovery_queue(max_entries=1).complete
        marker = LabQuarantineQueueMigrationComplete.model_validate_json(
            restarted.garbage_queue_migration_complete_path.read_text(encoding="utf-8")
        )
        cycle = restarted._load_active_queue_migration_cycle_locked()
        cursor = restarted._load_queue_migration_cursor(cycle)
        assert marker.cycle_id == cycle.cycle_id
        assert marker.index_hash == marker.final_chain_hash == cycle.index_hash
        assert marker.final_index == cursor.last_index == cycle.total_entries
        assert marker.directories == cycle.directories
        completed_cycles.append(marker.cycle_id)
        reclaimer = restarted

    assert len(set(completed_cycles)) == 3
    archived = tuple(reclaimer.garbage_queue_migration_complete_archive_dir.iterdir())
    assert len(archived) == 2

    replay = reclaimer.garbage_queue_migration_complete_path.with_name("replayed-complete.json")
    replay.write_bytes(archived[0].read_bytes())
    os.replace(replay, reclaimer.garbage_queue_migration_complete_path)
    with pytest.raises(LabArtifactConflictError, match="active cycle"):
        reclaimer.initialize_legacy_recovery_migration()


def test_damaged_cold_quarantine_warns_without_blocking_unrelated_claim(
    tmp_path: Path,
) -> None:
    claims = LabClaimSpool(tmp_path / "claims")
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims.publish(claim)
    registry = RecordingRegistry()
    worker = _worker(tmp_path, claims=claims, registry=registry)
    victim = tmp_path / "artifacts" / "cold-damage" / "result.bin"
    victim.parent.mkdir(parents=True)
    victim.write_bytes(b"retained-cold-result")
    assert worker.artifact_reclaimer.logical_quarantine_tree(
        victim,
        purpose="cold health fixture",
    )
    deferred = next(worker.artifact_reclaimer.garbage_deferred_dir.iterdir())
    (deferred / "unexpected.bin").write_bytes(b"foreign-metadata")

    with _raising_loguru_sink():
        result = worker.run_once()

    assert result.status == "succeeded"
    assert registry.executions == 1
    assert len(result.health_warnings) == 1
    assert result.health_warnings[0].category == "quarantine_reconcile_failed"
    assert result.health_warnings[0].error_type == "LabArtifactConflictError"
    assert (deferred / "unexpected.bin").read_bytes() == b"foreign-metadata"


def test_success_receipt_timeout_emits_structured_worker_warning(tmp_path: Path) -> None:
    from loguru import logger

    claims = LabClaimSpool(tmp_path / "claims")
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims.publish(claim)

    def delayed_success_receipt(
        report: LabWorkerReport,
        timeout_seconds: float,
        stop: object,
    ) -> LabReportReceipt:
        if isinstance(report.body, LabShardSucceeded):
            raise TimeoutError("scheduler receipt delayed")
        return _accept_report(report, timeout_seconds, stop)

    worker = _worker(
        tmp_path,
        claims=claims,
        receipt_waiter=delayed_success_receipt,
    )
    records: list[dict[str, object]] = []
    sink = logger.add(
        lambda message: records.append(dict(message.record["extra"])),
        level="WARNING",
    )
    try:
        with _raising_loguru_sink():
            result = worker.run_once()
    finally:
        logger.remove(sink)

    assert result.status == "awaiting_receipt"
    timeout_records = [
        record for record in records if record.get("failure") == "success_receipt_timeout"
    ]
    assert len(timeout_records) == 1
    assert timeout_records[0]["component"] == "lab_worker"
    assert timeout_records[0]["worker_id"] == "worker-a"
    assert timeout_records[0]["job_id"] == str(claim.job_id)
    assert timeout_records[0]["shard_id"] == str(claim.shard_id)
    assert timeout_records[0]["report_id"] == str(result.report_id)


def test_adapter_runtime_error_emits_structured_worker_failure(tmp_path: Path) -> None:
    from loguru import logger

    claims = LabClaimSpool(tmp_path / "claims")
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims.publish(claim)
    worker = _worker(
        tmp_path,
        claims=claims,
        registry=RecordingRegistry(failure=RuntimeError("adapter probe failed")),
    )
    records: list[dict[str, object]] = []
    sink = logger.add(
        lambda message: records.append(
            {
                "extra": dict(message.record["extra"]),
                "level": message.record["level"].name,
            }
        ),
        level="WARNING",
    )
    try:
        result = worker.run_once()
    finally:
        logger.remove(sink)

    assert result.status == "failed"
    failures = [
        record
        for record in records
        if record["extra"].get("failure") == "shard_execution_failed"  # type: ignore[union-attr]
    ]
    assert len(failures) == 1
    assert failures[0]["level"] == "ERROR"
    extra = failures[0]["extra"]
    assert extra["component"] == "lab_worker"  # type: ignore[index]
    assert extra["phase"] == "execute"  # type: ignore[index]
    assert extra["job_id"] == str(claim.job_id)  # type: ignore[index]
    assert extra["shard_id"] == str(claim.shard_id)  # type: ignore[index]
    assert extra["claim_generation"] == claim.claim_generation  # type: ignore[index]
    assert extra["error_type"] == "RuntimeError"  # type: ignore[index]


def test_worker_failure_logging_error_does_not_change_tick_result(tmp_path: Path) -> None:
    claims = LabClaimSpool(tmp_path / "claims")
    claims.publish(_claim(_nshape_compare_spec(hold_days=(1,))))
    worker = _worker(
        tmp_path,
        claims=claims,
        registry=RecordingRegistry(failure=RuntimeError("adapter failure")),
    )

    with _raising_loguru_sink():
        result = worker.run_once()

    assert result.status == "failed"


@pytest.mark.parametrize("mode", ["idle", "stopped"])
def test_idle_and_cooperative_stop_do_not_log_worker_failure(
    tmp_path: Path,
    mode: str,
) -> None:
    from loguru import logger

    worker = _worker(tmp_path)
    if mode == "stopped":
        worker.request_stop()
    failures: list[dict[str, object]] = []
    sink = logger.add(
        lambda message: failures.append(dict(message.record["extra"])),
        level="WARNING",
    )
    try:
        result = worker.run_once()
    finally:
        logger.remove(sink)

    assert result.status == mode
    assert not [record for record in failures if record.get("failure") == "shard_execution_failed"]


def test_worker_leaves_expired_claim_for_lease_recovery(tmp_path: Path) -> None:
    claims = LabClaimSpool(tmp_path / "claims")
    registry = RecordingRegistry()
    expired = _claim(_nshape_compare_spec()).model_copy(
        update={
            "claimed_at": NOW - timedelta(minutes=2),
            "lease_expires_at": NOW - timedelta(minutes=1),
        }
    )
    claims.publish(expired)
    worker = _worker(tmp_path, registry=registry, claims=claims)

    result = worker.run_once()

    assert result.status == "idle"
    assert registry.executions == 0
    assert [entry.claim for entry in claims.pending()] == [expired]


def test_worker_skips_superseded_claim_without_consuming_it(tmp_path: Path) -> None:
    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    registry = RecordingRegistry()
    stale = _claim(_nshape_compare_spec())
    fresh = _retry_claim(stale)
    claims.publish(stale)
    claims.publish(fresh)
    worker = _worker(
        tmp_path,
        registry=registry,
        claims=claims,
        reports=reports,
    )

    worker.run_once()

    assert claims.pending() == ()
    assert len(tuple(claims.quarantine_dir.glob("owned-entry-*.dead/evidence.json"))) == 1
    assert registry.executions == 1
    assert _reports(reports)[-1].claim_token == fresh.claim_token


def test_consumed_new_generation_prevents_old_claim_resurrection(tmp_path: Path) -> None:
    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    registry = RecordingRegistry()
    stale = _claim(_nshape_compare_spec(hold_days=(1,)))
    fresh = _retry_claim(stale).model_copy(update={"worker_id": "worker-b"})
    claims.publish(stale)
    claims.consume(claims.publish(fresh))

    result = _worker(
        tmp_path,
        worker_id="worker-a",
        registry=registry,
        claims=claims,
        reports=reports,
    ).run_once()

    assert result.status == "idle"
    assert registry.executions == 0
    assert reports.pending() == ()
    assert claims.pending() == ()
    assert len(tuple(claims.quarantine_dir.glob("owned-entry-*.dead/evidence.json"))) == 1
    assert LabClaimSpool(claims.root).current(stale.job_id, stale.shard_id).claim == fresh


def test_worker_never_executes_revoked_claim_after_cleanup_interruption(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    registry = RecordingRegistry()
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims.publish(claim)
    original_unlink_current = claims._unlink_current_locked
    monkeypatch.setattr(
        claims,
        "_unlink_current_locked",
        lambda _claim: (_ for _ in ()).throw(OSError("injected cleanup interruption")),
    )
    with pytest.raises(OSError, match="cleanup interruption"):
        claims.revoke(claim, reason="sqlite terminal")
    monkeypatch.setattr(claims, "_unlink_current_locked", original_unlink_current)

    result = _worker(
        tmp_path,
        registry=registry,
        claims=claims,
        reports=reports,
    ).run_once()

    assert result.status == "idle"
    assert registry.executions == 0
    assert reports.pending() == ()
    assert claims.pending() == ()


def test_worker_rechecks_admission_after_consume_before_open_or_execute(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    registry = RecordingRegistry()
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims.publish(claim)
    admission_attempted = threading.Event()
    release = threading.Event()
    original_admit = claims.admit_execution

    def pause_before_admission(exact_claim: LabShardClaim):
        admission_attempted.set()
        assert release.wait(2)
        return original_admit(exact_claim)

    monkeypatch.setattr(claims, "admit_execution", pause_before_admission)
    stores_opened = 0

    @contextmanager
    def counted_store() -> Iterator[object]:
        nonlocal stores_opened
        stores_opened += 1
        yield object()

    worker = _worker(
        tmp_path,
        claims=claims,
        reports=reports,
        registry=registry,
        exploratory_store_factory=counted_store,
    )
    results: list[object] = []
    thread = threading.Thread(target=lambda: results.append(worker.run_once()))
    thread.start()
    assert admission_attempted.wait(2)

    claims.revoke(claim, reason="scheduler terminalized consumed claim")
    release.set()
    thread.join(timeout=_observe(2))

    assert not thread.is_alive()
    assert results[0].status == "stopped"
    assert stores_opened == 0
    assert registry.executions == 0
    assert not worker.sealed_bundle_path(claim).exists()


def test_worker_admission_then_revoke_allows_compute_but_never_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    registry = RecordingRegistry()
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims.publish(claim)
    admitted = threading.Event()
    release = threading.Event()
    original_admit = claims.admit_execution

    def admit_then_pause(exact_claim: LabShardClaim):
        receipt = original_admit(exact_claim)
        admitted.set()
        assert release.wait(2)
        return receipt

    monkeypatch.setattr(claims, "admit_execution", admit_then_pause)
    worker = _worker(
        tmp_path,
        claims=claims,
        reports=reports,
        registry=registry,
        heartbeat_interval_seconds=0.01,
    )
    results: list[object] = []
    thread = threading.Thread(target=lambda: results.append(worker.run_once()))
    thread.start()
    assert admitted.wait(2)

    claims.revoke(claim, reason="scheduler revoked admitted execution")
    release.set()
    thread.join(timeout=_observe(2))

    assert not thread.is_alive()
    assert results[0].status == "failed"
    assert registry.executions == 1
    assert claims.execution_admission(claim.claim_token).admission.claim == claim
    assert claims.revocation(claim.claim_token).revocation.claim == claim
    assert not worker.sealed_bundle_path(claim).exists()
    assert not any(isinstance(report.body, LabShardSucceeded) for report in _reports(reports))


def test_revoke_during_execute_is_fenced_before_seal_and_success(
    tmp_path: Path,
) -> None:
    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims.publish(claim)
    spawn = multiprocessing.get_context("spawn")
    executing = spawn.Event()
    release = spawn.Event()

    registry = BlockingRegistry(executing=executing, release=release)
    worker = _worker(
        tmp_path,
        claims=claims,
        reports=reports,
        registry=registry,
        heartbeat_interval_seconds=0.01,
    )
    results: list[object] = []
    thread = threading.Thread(target=lambda: results.append(worker.run_once()))
    thread.start()
    entered_deadline = time.monotonic() + _child_startups(2)
    while not registry._closed_entered_path.exists() and time.monotonic() < entered_deadline:
        time.sleep(0.01)
    assert registry._closed_entered_path.exists()

    claims.revoke(claim, reason="scheduler revoked running attempt")
    registry._closed_release_path.write_text("release", encoding="ascii")
    thread.join(timeout=_observe(2))

    assert not thread.is_alive()
    assert results[0].status == "failed"
    assert registry.executions == 1
    assert not worker.sealed_bundle_path(claim).exists()
    assert not any(isinstance(report.body, LabShardSucceeded) for report in _reports(reports))


def test_worker_fails_closed_when_claim_high_water_marker_is_missing(tmp_path: Path) -> None:
    claims = LabClaimSpool(tmp_path / "claims")
    registry = RecordingRegistry()
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims.publish(claim)
    (claims.current_dir / f"{claim.job_id}.{claim.shard_id}.json").unlink()

    result = _worker(tmp_path, registry=registry, claims=claims).run_once()

    assert result.status == "idle"
    assert registry.executions == 0
    assert [entry.claim for entry in claims.pending()] == [claim]


def test_worker_heartbeats_during_long_shard_then_succeeds(tmp_path: Path) -> None:
    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    claim = _claim(_nshape_compare_spec())
    claims.publish(claim)
    worker = _worker(
        tmp_path,
        registry=RecordingRegistry(delay_seconds=0.06),
        claims=claims,
        reports=reports,
        heartbeat_interval_seconds=0.01,
    )

    worker.run_once()

    bodies = tuple(report.body for report in _reports(reports))
    assert any(isinstance(body, LabShardHeartbeat) for body in bodies)
    assert isinstance(bodies[-1], LabShardSucceeded)


def test_background_heartbeat_covers_candidate_serialization(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims.publish(claim)
    worker = _worker(
        tmp_path,
        claims=claims,
        reports=reports,
        heartbeat_interval_seconds=0.01,
    )
    original_write = worker._write_bundle

    def slow_write(*args: object, **kwargs: object):
        time.sleep(0.05)
        return original_write(*args, **kwargs)

    monkeypatch.setattr(worker, "_write_bundle", slow_write)

    result = worker.run_once()

    heartbeats = [
        report for report in _reports(reports) if isinstance(report.body, LabShardHeartbeat)
    ]
    assert result.status == "succeeded"
    assert len(heartbeats) >= 2  # periodic during candidate write, then synchronous final fence


def test_slow_candidate_loses_one_second_lease_to_new_generation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.lab_job_protocol import LabCommandEnvelope, LabCommandSpool, SubmitJobCommand
    from rquant.lab_jobs import LabJobStore
    from rquant.lab_scheduler import LabScheduler

    clock = [NOW]
    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    commands = LabCommandSpool(tmp_path / "commands")
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    commands.publish(
        LabCommandEnvelope(
            request_id=uuid4(),
            command=SubmitJobCommand(
                job_id=uuid4(),
                spec=_nshape_compare_spec(hold_days=(1,)),
                max_attempts=2,
            ),
        )
    )
    scheduler = LabScheduler(
        store=store,
        spool=commands,
        owner_id="scheduler-a",
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=5,
        report_spool=reports,
        claim_spool=claims,
        claim_worker_ids=("worker-a",),
        shard_lease_seconds=1,
        adapter_registry=default_strategy_job_adapter_registry(),
        clock=lambda: clock[0],
    )
    scheduler.run_once()
    original = claims.pending()[0].claim
    published: list[LabWorkerReport] = []
    original_publish = reports.publish

    def capture_publish(report: LabWorkerReport):
        published.append(report)
        return original_publish(report)

    monkeypatch.setattr(reports, "publish", capture_publish)
    worker = _worker(
        tmp_path,
        claims=claims,
        reports=reports,
        heartbeat_interval_seconds=0.05,
        lease_extension_seconds=1,
        receipt_timeout_seconds=0.5,
        receipt_waiter=None,
        clock=lambda: clock[0],
    )
    write_started = threading.Event()
    release_write = threading.Event()
    original_write = worker._write_bundle

    def slow_write(*args: object, **kwargs: object):
        write_started.set()
        assert release_write.wait(timeout=_observe(3))
        return original_write(*args, **kwargs)

    monkeypatch.setattr(worker, "_write_bundle", slow_write)
    outcomes = []
    thread = threading.Thread(target=lambda: outcomes.append(worker.run_once()))
    thread.start()
    write_timeout = time.monotonic() + _observe(3)
    while not write_started.is_set() and time.monotonic() < write_timeout:
        scheduler.run_once()
        time.sleep(0.01)
    assert write_started.is_set()
    time.sleep(1.05)
    clock[0] = NOW + timedelta(seconds=2)
    assert scheduler.lease is not None
    recovered_job_ids = store.recover_stale_shards(scheduler.lease, now=clock[0])
    assert recovered_job_ids == (original.job_id,)
    recovery = scheduler.run_once()
    replacement = claims.pending()[0].claim
    release_write.set()
    timeout_at = time.monotonic() + _observe(2)
    while thread.is_alive() and time.monotonic() < timeout_at:
        scheduler.run_once()
        time.sleep(0.01)
    thread.join(timeout=_observe(0.2))
    scheduler.release()

    assert not thread.is_alive()
    assert recovery.recovered == 0
    assert replacement.claim_generation == original.claim_generation + 1
    assert outcomes[0].status == "failed"
    assert not worker.sealed_bundle_path(original).exists()
    assert not any(
        isinstance(report.body, LabShardSucceeded) and report.claim_token == original.claim_token
        for report in published
    )


def test_final_fence_rejection_prevents_seal_and_success(tmp_path: Path) -> None:
    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims.publish(claim)

    def reject_fence(
        report: LabWorkerReport,
        _timeout_seconds: float,
        _stop: object,
    ) -> LabReportReceipt:
        return LabReportReceipt.from_report(
            report,
            status="rejected",
            reason="claim_generation_mismatch",
            accepted_at=NOW,
        )

    worker = _worker(
        tmp_path,
        claims=claims,
        reports=reports,
        receipt_waiter=reject_fence,
    )

    result = worker.run_once()

    assert result.status == "failed"
    assert not worker.sealed_bundle_path(claim).exists()
    assert not any(isinstance(report.body, LabShardSucceeded) for report in _reports(reports))


def test_heartbeat_publish_failure_returns_failed_without_sealing(tmp_path: Path) -> None:
    class FailFirstHeartbeatSpool(LabReportSpool):
        def __init__(self, root: Path) -> None:
            super().__init__(root)
            self.failed = False

        def publish(self, report: LabWorkerReport):
            if isinstance(report.body, LabShardHeartbeat) and not self.failed:
                self.failed = True
                raise OSError("injected heartbeat publish failure")
            return super().publish(report)

    claims = LabClaimSpool(tmp_path / "claims")
    reports = FailFirstHeartbeatSpool(tmp_path / "reports")
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims.publish(claim)
    worker = _worker(
        tmp_path,
        registry=RecordingRegistry(delay_seconds=0.06),
        claims=claims,
        reports=reports,
        heartbeat_interval_seconds=0.01,
    )

    result = worker.run_once()

    failure = _reported_failure(reports)
    assert result.status == "failed"
    assert failure.phase == "fence"
    assert failure.failure_kind == "fence"
    assert not worker.sealed_bundle_path(claim).exists()


def test_worker_reports_typed_failure_without_sealing(tmp_path: Path) -> None:
    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    claim = _claim(_nshape_compare_spec())
    claims.publish(claim)
    worker = _worker(
        tmp_path,
        registry=RecordingRegistry(failure=RuntimeError("fixture failed")),
        claims=claims,
        reports=reports,
    )

    result = worker.run_once()

    assert result.status == "failed"
    report = _reports(reports)[-1]
    assert isinstance(report.body, LabShardFailed)
    assert "fixture failed" in report.body.failure_json
    failure = LabWorkerFailure.model_validate_json(report.body.failure_json)
    assert failure.phase == "execute"
    assert failure.failure_kind == "execution"
    assert not worker.sealed_bundle_path(claim).exists()

    invalid_claims = LabClaimSpool(tmp_path / "invalid-claims")
    invalid_reports = LabReportSpool(tmp_path / "invalid-reports")
    invalid_definition = type(claim.definition).from_payload(
        shard_index=claim.definition.shard_index,
        adapter_id=claim.definition.adapter_id,
        adapter_version=claim.definition.adapter_version,
        plan_hash=claim.definition.plan_hash,
        payload_json="{}",
        work_plan=claim.definition.work_plan,
    )
    invalid_claim = LabShardClaim.model_validate(
        {**claim.model_dump(mode="python"), "definition": invalid_definition},
        strict=True,
    )
    invalid_claims.publish(invalid_claim)
    invalid_worker = _worker(
        tmp_path,
        claims=invalid_claims,
        reports=invalid_reports,
    )

    invalid_result = invalid_worker.run_once()

    invalid_failure = _reported_failure(invalid_reports)
    assert invalid_result.status == "failed"
    assert invalid_failure.phase == "claim"
    assert invalid_failure.failure_kind == "claim_validation"
    assert '"failure_kind":"claim_validation"' in _reports(invalid_reports)[-1].body.failure_json


def test_worker_deadline_before_execute_fails_without_running_shard(tmp_path: Path) -> None:
    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    registry = RecordingRegistry()
    spec = _nshape_compare_spec(hold_days=(1,)).model_copy(update={"deadline": NOW})
    claim = _claim(spec)
    claims.publish(claim)
    worker = _worker(
        tmp_path,
        registry=registry,
        claims=claims,
        reports=reports,
    )

    result = worker.run_once()

    failure = _reported_failure(reports)
    assert result.status == "failed"
    assert failure.phase == "deadline"
    assert failure.failure_kind == "deadline"
    assert registry.executions == 0
    assert not worker.sealed_bundle_path(claim).exists()


def test_worker_deadline_after_execute_prevents_fence_and_seal(tmp_path: Path) -> None:
    deadline_reached = multiprocessing.get_context("spawn").Value("b", False)
    spec = _nshape_compare_spec(hold_days=(1,)).model_copy(
        update={"deadline": NOW + _deadline_reachable_in(2)}
    )

    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    claim = _claim(spec)
    claims.publish(claim)
    registry = DeadlineRegistry(deadline_reached=deadline_reached)
    worker = _worker(
        tmp_path,
        registry=registry,
        claims=claims,
        reports=reports,
        clock=lambda: spec.deadline if deadline_reached.value else NOW,
    )
    worker.clock = lambda: spec.deadline if registry._closed_deadline_path.exists() else NOW

    result = worker.run_once()

    failure = _reported_failure(reports)
    assert result.status == "failed"
    assert failure.phase == "deadline"
    assert failure.failure_kind == "deadline"
    assert not worker.sealed_bundle_path(claim).exists()
    assert not any(isinstance(report.body, LabShardSucceeded) for report in _reports(reports))


def test_worker_deadline_during_bundle_write_prevents_atomic_seal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = [NOW]
    spec = _nshape_compare_spec(hold_days=(1,)).model_copy(
        update={"deadline": NOW + _deadline_reachable_in(2)}
    )
    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    claim = _claim(spec)
    claims.publish(claim)
    worker = _worker(
        tmp_path,
        claims=claims,
        reports=reports,
        clock=lambda: clock[0],
    )
    original_write = worker._write_bundle

    def write_across_deadline(*args: object, **kwargs: object):
        manifest = original_write(*args, **kwargs)
        clock[0] = spec.deadline
        return manifest

    monkeypatch.setattr(worker, "_write_bundle", write_across_deadline)

    result = worker.run_once()

    failure = _reported_failure(reports)
    assert result.status == "failed"
    assert failure.phase == "seal"
    assert failure.failure_kind == "seal"
    assert not worker.sealed_bundle_path(claim).exists()
    assert not any(isinstance(report.body, LabShardSucceeded) for report in _reports(reports))


def test_stop_triggered_at_atomic_rename_rolls_back_before_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.lab_worker as lab_worker

    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims.publish(claim)
    worker = _worker(tmp_path, claims=claims, reports=reports)
    sealed = worker.sealed_bundle_path(claim)
    original_rename = lab_worker.os.rename

    def rename_then_stop(source: object, target: object) -> None:
        original_rename(source, target)
        if Path(target) == sealed:
            worker.request_stop()

    monkeypatch.setattr(lab_worker.os, "rename", rename_then_stop)

    result = worker.run_once()

    assert result.status == "stopped"
    assert not sealed.exists()
    assert not any(isinstance(report.body, LabShardSucceeded) for report in _reports(reports))


def test_deadline_triggered_at_atomic_rename_rolls_back_before_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.lab_worker as lab_worker

    clock = [NOW]
    spec = _nshape_compare_spec(hold_days=(1,)).model_copy(
        update={"deadline": NOW + _deadline_reachable_in(2)}
    )
    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    claim = _claim(spec)
    claims.publish(claim)
    worker = _worker(
        tmp_path,
        claims=claims,
        reports=reports,
        clock=lambda: clock[0],
    )
    sealed = worker.sealed_bundle_path(claim)
    original_rename = lab_worker.os.rename

    def rename_then_expire(source: object, target: object) -> None:
        original_rename(source, target)
        if Path(target) == sealed:
            clock[0] = spec.deadline

    monkeypatch.setattr(lab_worker.os, "rename", rename_then_expire)

    result = worker.run_once()

    assert result.status == "failed"
    assert not sealed.exists()
    assert not any(isinstance(report.body, LabShardSucceeded) for report in _reports(reports))


def test_lease_expiry_at_atomic_rename_rolls_back_before_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.lab_worker as lab_worker

    clock = [NOW]
    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims.publish(claim)
    worker = _worker(
        tmp_path,
        claims=claims,
        reports=reports,
        lease_extension_seconds=1,
        clock=lambda: clock[0],
    )
    sealed = worker.sealed_bundle_path(claim)
    original_rename = lab_worker.os.rename

    def rename_then_expire_lease(source: object, target: object) -> None:
        original_rename(source, target)
        if Path(target) == sealed:
            clock[0] = NOW + timedelta(seconds=1)

    monkeypatch.setattr(lab_worker.os, "rename", rename_then_expire_lease)

    result = worker.run_once()

    assert result.status == "failed"
    assert not sealed.exists()
    assert not any(isinstance(report.body, LabShardSucceeded) for report in _reports(reports))


def test_high_water_change_at_atomic_rename_rolls_back_old_attempt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.lab_worker as lab_worker

    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims.publish(claim)
    worker = _worker(tmp_path, claims=claims, reports=reports)
    sealed = worker.sealed_bundle_path(claim)
    replacement = _retry_claim(claim)
    original_rename = lab_worker.os.rename

    def rename_then_replace_claim(source: object, target: object) -> None:
        original_rename(source, target)
        if Path(target) == sealed:
            claims.publish(replacement)

    monkeypatch.setattr(lab_worker.os, "rename", rename_then_replace_claim)

    result = worker.run_once()

    assert result.status == "failed"
    assert claims.current(claim.job_id, claim.shard_id).claim == replacement
    assert not sealed.exists()
    assert not any(isinstance(report.body, LabShardSucceeded) for report in _reports(reports))


def test_stop_before_execution_reports_worker_stopped(tmp_path: Path) -> None:
    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    registry = RecordingRegistry()
    claim = _claim(_nshape_compare_spec())
    claims.publish(claim)
    worker = _worker(
        tmp_path,
        registry=registry,
        claims=claims,
        reports=reports,
    )
    worker.request_stop()

    result = worker.run_once()

    assert result.status == "stopped"
    assert registry.executions == 0
    assert reports.pending() == ()
    assert [entry.claim for entry in claims.pending()] == [claim]


def test_stop_during_execution_reports_stopped_without_sealing(tmp_path: Path) -> None:
    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims.publish(claim)
    worker = _worker(
        tmp_path,
        registry=RecordingRegistry(delay_seconds=0.08),
        claims=claims,
        reports=reports,
        heartbeat_interval_seconds=0.01,
    )
    outcomes: list[object] = []
    thread = threading.Thread(target=lambda: outcomes.append(worker.run_once()))
    thread.start()
    time.sleep(0.02)
    worker.request_stop()
    thread.join(timeout=_observe(2))

    assert outcomes[0].status == "stopped"
    assert isinstance(_reports(reports)[-1].body, LabWorkerStopped)
    assert not worker.sealed_bundle_path(claim).exists()


def test_stop_during_execution_releases_resource_reservation(tmp_path: Path) -> None:
    from rquant.runtime_resource_admission import (
        RuntimeResourceAdmissionError,
        SQLiteResourceReservationStore,
    )

    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims.publish(claim)
    store = SQLiteResourceReservationStore(
        tmp_path / "resource-reservations.sqlite3",
        clock=lambda: NOW,
    )
    worker = _worker(
        tmp_path,
        registry=RecordingRegistry(delay_seconds=0.2),
        claims=claims,
        reports=reports,
        heartbeat_interval_seconds=0.01,
        resource_snapshot_provider=StaticResourceSnapshotProvider(_healthy_resource_snapshot()),
        admission_policy_provider=StaticAdmissionPolicyProvider(_permissive_admission_policy()),
        resource_reservation_store=store,
        require_resource_admission=True,
    )
    outcomes: list[object] = []
    thread = threading.Thread(target=lambda: outcomes.append(worker.run_once()))
    thread.start()
    deadline = time.monotonic() + _observe(2)
    active = ()
    while not active and time.monotonic() < deadline:
        try:
            active = store.active_leases()
        except RuntimeResourceAdmissionError as exc:
            assert "lock wait timeout" in str(exc)
        time.sleep(0.01)
    assert len(active) == 1

    worker.request_stop()
    thread.join(timeout=_observe(3))

    assert not thread.is_alive()
    assert outcomes[0].status == "stopped"
    assert store.active_leases() == ()
    assert not worker.sealed_bundle_path(claim).exists()


def test_job_deadline_terminates_child_and_releases_resource_reservation(
    tmp_path: Path,
) -> None:
    from rquant.runtime_resource_admission import SQLiteResourceReservationStore

    spec = _nshape_compare_spec(hold_days=(1,)).model_copy(
        update={"deadline": NOW + timedelta(milliseconds=50)}
    )
    claim = _short_claim_for_spec(spec)
    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    pid_path = tmp_path / "deadline-child.pid"
    claims.publish(claim)
    store = SQLiteResourceReservationStore(
        tmp_path / "resource-reservations.sqlite3",
        clock=lambda: NOW,
    )
    worker = _worker(
        tmp_path,
        registry=SlowPidRegistry(pid_path=pid_path, delay_seconds=0.2),
        claims=claims,
        reports=reports,
        resource_snapshot_provider=StaticResourceSnapshotProvider(_healthy_resource_snapshot()),
        admission_policy_provider=StaticAdmissionPolicyProvider(_permissive_admission_policy()),
        resource_reservation_store=store,
        lease_extension_seconds=5,
        require_resource_admission=True,
    )

    result = worker.run_once()

    assert result.status == "stopped"
    assert store.active_leases() == ()
    assert not worker.sealed_bundle_path(claim).exists()


def test_bundle_is_canonical_and_obsolete_attempt_is_reclaimed_across_retry(
    tmp_path: Path,
) -> None:
    from rquant.lab_worker import LabShardResultManifest

    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    registry = RecordingRegistry()
    claim = _claim(_nshape_compare_spec())
    claims.publish(claim)
    worker = _worker(
        tmp_path,
        registry=registry,
        claims=claims,
        reports=reports,
    )

    validated = worker._validate_closed_claim(claim)
    first_result = registry.execute_shard(validated, object())
    first = worker._seal_result(claim, first_result)
    sealed = worker.sealed_bundle_path(claim)
    manifest_path = sealed / "manifest.json"
    manifest = LabShardResultManifest.model_validate_json(manifest_path.read_text(encoding="utf-8"))
    assert first.manifest_hash == manifest.manifest_hash
    assert manifest.schema_version == CURRENT_RESULT_MANIFEST_SCHEMA_VERSION
    assert manifest.content_digest_algorithm == CURRENT_CONTENT_DIGEST_ALGORITHM
    assert manifest.worker_code_sha == "1" * 40
    assert manifest_path.read_text(encoding="utf-8") == manifest.canonical_json()
    assert (sealed / manifest.artifacts[0].file_name).is_file()
    retry = _retry_claim(claim)
    claims.publish(retry)
    claims.reconcile_current()
    assert not sealed.exists()
    second = worker.run_once()

    assert second.manifest_hash != first.manifest_hash
    assert manifest.claim_token == claim.claim_token
    assert manifest.claim_generation == claim.claim_generation
    assert manifest.scheduler_fencing_token == claim.scheduler_fencing_token
    assert worker.sealed_bundle_path(retry) != sealed
    assert worker.sealed_bundle_path(retry).is_dir()
    assert registry.executions == 2
    assert not tuple(
        path for path in (tmp_path / "artifacts" / ".tmp").rglob("*") if path.is_file()
    )


def test_result_manifest_preserves_legacy_v1_canonical_bytes() -> None:
    from rquant.lab_worker import LabShardResultManifest

    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    payload = {
        "schema_version": 1,
        "job_id": str(claim.job_id),
        "shard_id": str(claim.shard_id),
        "claim_token": str(claim.claim_token),
        "claim_generation": claim.claim_generation,
        "scheduler_fencing_token": claim.scheduler_fencing_token,
        "spec_hash": claim.spec_hash,
        "payload_hash": claim.payload_hash,
        "plan_hash": claim.plan_hash,
        "adapter_id": claim.definition.adapter_id,
        "adapter_version": claim.definition.adapter_version,
        "artifacts": [
            {
                "name": "trades",
                "file_name": "000-trades.parquet",
                "format": "parquet",
                "row_count": 1,
                "columns": ["hold_days", "ret_pct"],
                "file_size": 1,
                "file_sha256": "2" * 64,
                "content_sha256": "3" * 64,
            }
        ],
        "metrics": [],
    }
    raw = json.dumps(
        payload,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )

    manifest = LabShardResultManifest.model_validate_json(raw)

    assert manifest.canonical_json() == raw


@pytest.mark.parametrize(
    "updates",
    [
        {"schema_version": 2},
        {
            "schema_version": 1,
            "worker_code_sha": "1" * 40,
            "content_digest_algorithm": CURRENT_CONTENT_DIGEST_ALGORITHM,
        },
    ],
)
def test_result_manifest_rejects_missing_or_forged_digest_provenance(
    updates: dict[str, object],
) -> None:
    from rquant.lab_worker import LabShardResultManifest

    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    payload = {
        "schema_version": 1,
        "job_id": str(claim.job_id),
        "shard_id": str(claim.shard_id),
        "claim_token": str(claim.claim_token),
        "claim_generation": claim.claim_generation,
        "scheduler_fencing_token": claim.scheduler_fencing_token,
        "spec_hash": claim.spec_hash,
        "payload_hash": claim.payload_hash,
        "plan_hash": claim.plan_hash,
        "adapter_id": claim.definition.adapter_id,
        "adapter_version": claim.definition.adapter_version,
        "artifacts": [
            {
                "name": "trades",
                "file_name": "000-trades.parquet",
                "row_count": 1,
                "columns": ["value"],
                "file_size": 1,
                "file_sha256": "2" * 64,
                "content_sha256": "3" * 64,
            }
        ],
        **updates,
    }
    with pytest.raises(ValidationError, match="digest provenance"):
        LabShardResultManifest.model_validate(payload)


def test_same_attempt_conflicting_result_fails_closed(tmp_path: Path) -> None:
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    worker = _worker(tmp_path)
    validated = worker._validate_closed_claim(claim)

    def result(value: int) -> LabShardExecutionResult:
        return LabShardExecutionResult.from_validated(
            validated,
            tables=(LabShardTable(name="trades", frame=pd.DataFrame([{"value": value}])),),
        )

    first = worker._seal_result(claim, result(1))
    with pytest.raises(Exception, match="conflict"):
        worker._seal_result(claim, result(2))
    same = worker._seal_result(claim, result(1))

    assert same.manifest_hash == first.manifest_hash
    persisted = pd.read_parquet(worker.sealed_bundle_path(claim) / first.artifacts[0].file_name)
    assert persisted["value"].tolist() == [1]


def test_concurrent_same_attempt_conflicting_results_have_one_atomic_winner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.lab_worker as lab_worker

    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    workers = (_worker(tmp_path), _worker(tmp_path))
    validated = workers[0].adapter_registry.validate_claim(claim)
    sealed = workers[0].sealed_bundle_path(claim)
    rename_barrier = threading.Barrier(2)
    original_rename = lab_worker.os.rename

    def synchronized_rename(source: object, target: object) -> None:
        if Path(target) == sealed:
            rename_barrier.wait(timeout=_observe(1))
        original_rename(source, target)

    monkeypatch.setattr(lab_worker.os, "rename", synchronized_rename)
    outcomes: list[object] = []

    def seal(worker: object, value: int) -> None:
        result = LabShardExecutionResult.from_validated(
            validated,
            tables=(LabShardTable(name="trades", frame=pd.DataFrame([{"value": value}])),),
        )
        try:
            outcomes.append(worker._seal_result(claim, result))
        except Exception as exc:
            outcomes.append(exc)

    threads = (
        threading.Thread(target=seal, args=(workers[0], 1)),
        threading.Thread(target=seal, args=(workers[1], 2)),
    )
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=_observe(2))

    assert all(not thread.is_alive() for thread in threads)
    assert sum(isinstance(outcome, Exception) for outcome in outcomes) == 1
    assert any("conflict" in str(outcome) for outcome in outcomes if isinstance(outcome, Exception))
    persisted = pd.read_parquet(sealed / "000-trades.parquet")
    assert persisted["value"].tolist() in ([1], [2])


def test_bundle_validation_rejects_extra_unknown_file(tmp_path: Path) -> None:
    claims = LabClaimSpool(tmp_path / "claims")
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims.publish(claim)
    worker = _worker(tmp_path, claims=claims)
    assert worker.run_once().status == "succeeded"
    (worker.sealed_bundle_path(claim) / "extra.bin").write_bytes(b"unexpected")

    with pytest.raises(Exception, match="unexpected"):
        worker._validate_bundle(worker.sealed_bundle_path(claim), claim)


def test_new_generation_reclaims_only_obsolete_crash_temporary(tmp_path: Path) -> None:
    claims = LabClaimSpool(tmp_path / "claims")
    old = _claim(_nshape_compare_spec(hold_days=(1,)))
    new = _retry_claim(old)
    worker = _worker(tmp_path, claims=claims)
    obsolete = worker._temporary_bundle_path(old)
    obsolete.mkdir(parents=True)
    (obsolete / "partial.parquet").write_bytes(b"partial")
    current = worker._temporary_bundle_path(new)
    current.mkdir(parents=True)
    (current / "still-active").write_bytes(b"active")
    claims.publish(new)

    worker._reclaim_obsolete_temporaries(new)

    assert not obsolete.exists()
    assert (current / "still-active").read_bytes() == b"active"


def test_generation_three_recovers_crash_after_temporary_tree_isolation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    old = _claim(_nshape_compare_spec(hold_days=(1,)))
    generation_two = _retry_claim(old)
    generation_three = _retry_claim(generation_two)
    worker = _worker(tmp_path)
    obsolete = worker._temporary_bundle_path(old)
    nested = obsolete / "nested"
    nested.mkdir(parents=True)
    (obsolete / "manifest.partial").write_bytes(b"manifest")
    (nested / "artifact.partial").write_bytes(b"artifact")
    original_promote = worker.artifact_reclaimer._promote_garbage_bundle
    interrupted = False

    def crash_before_pending(source: Path, target: Path) -> None:
        nonlocal interrupted
        if source.parent == worker.artifact_reclaimer.garbage_staging_dir and not interrupted:
            interrupted = True
            raise InterruptedError("crash after temporary tree isolation")
        original_promote(source, target)

    monkeypatch.setattr(
        worker.artifact_reclaimer,
        "_promote_garbage_bundle",
        crash_before_pending,
    )
    with pytest.raises(InterruptedError, match="tree isolation"):
        worker._reclaim_obsolete_temporaries(generation_two)

    assert not obsolete.exists()
    staging = tuple(worker.artifact_reclaimer.garbage_staging_dir.iterdir())
    assert len(staging) == 1
    assert (staging[0] / "owner.json").is_file()
    assert (staging[0] / "payload" / "nested" / "artifact.partial").is_file()
    monkeypatch.setattr(
        worker.artifact_reclaimer,
        "_promote_garbage_bundle",
        original_promote,
    )

    restarted = _worker(tmp_path)
    restarted._reclaim_obsolete_temporaries(generation_three)
    restarted.artifact_reclaimer.collect_garbage()

    assert not obsolete.exists()
    assert tuple(restarted.artifact_reclaimer.garbage_staging_dir.iterdir()) == ()
    entries = restarted.artifact_reclaimer.quarantine_entries()
    assert len(entries) == 1
    assert entries[0].state == "deferred_gc"
    assert (entries[0].bundle_path / "payload" / "nested" / "artifact.partial").is_file()


def test_temporary_tree_quarantine_retains_complete_inventory_across_restarts(
    tmp_path: Path,
) -> None:
    old = _claim(_nshape_compare_spec(hold_days=(1,)))
    current = _retry_claim(old)
    worker = _worker(tmp_path)
    obsolete = worker._temporary_bundle_path(old)
    nested = obsolete / "nested"
    nested.mkdir(parents=True)
    first = obsolete / "first.partial"
    second = nested / "second.partial"
    first.write_bytes(b"first")
    second.write_bytes(b"second")
    worker._reclaim_obsolete_temporaries(current)
    for _ in range(3):
        restarted = _worker(tmp_path)
        restarted.artifact_reclaimer.collect_garbage()

    entry = restarted.artifact_reclaimer.quarantine_entries()[0]
    payload = entry.bundle_path / "payload"
    assert (payload / "first.partial").read_bytes() == b"first"
    assert (payload / "nested" / "second.partial").read_bytes() == b"second"
    assert entry.retained_bytes == len(b"first") + len(b"second")


@pytest.mark.parametrize("mutation", ["extra", "replace", "hardlink", "symlink"])
def test_temporary_tree_gc_rejects_mutated_owned_payload(
    tmp_path: Path,
    mutation: str,
) -> None:
    from rquant.lab_worker import LabArtifactConflictError

    old = _claim(_nshape_compare_spec(hold_days=(1,)))
    current = _retry_claim(old)
    worker = _worker(tmp_path)
    obsolete = worker._temporary_bundle_path(old)
    obsolete.mkdir(parents=True)
    original = obsolete / "partial.bin"
    original.write_bytes(b"original")
    worker._reclaim_obsolete_temporaries(current)
    deferred = tuple(worker.artifact_reclaimer.garbage_deferred_dir.iterdir())[0]
    payload = deferred / "payload"
    external = tmp_path / f"external-{mutation}"
    external.write_bytes(b"external")
    if mutation == "extra":
        (payload / "extra.bin").write_bytes(b"extra")
    elif mutation == "replace":
        os.replace(external, payload / "partial.bin")
    elif mutation == "hardlink":
        original_payload = payload / "partial.bin"
        external.unlink()
        os.link(original_payload, external)
    else:
        (payload / "partial.bin").unlink()
        (payload / "partial.bin").symlink_to(external)

    restarted = _worker(tmp_path)
    with pytest.raises(LabArtifactConflictError):
        restarted.artifact_reclaimer.collect_garbage()

    assert deferred.exists()
    if mutation in {"hardlink", "symlink"}:
        assert external.exists()


def test_current_attempt_reclaims_known_crash_candidate_directory(tmp_path: Path) -> None:
    claims = LabClaimSpool(tmp_path / "claims")
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    worker = _worker(tmp_path, claims=claims)
    abandoned = worker._temporary_bundle_path(claim) / uuid4().hex
    abandoned.mkdir(parents=True)
    (abandoned / "partial.parquet").write_bytes(b"partial")
    claims.publish(claim)

    worker._reclaim_obsolete_temporaries(claim)

    assert not abandoned.exists()
    assert worker._temporary_bundle_path(claim).is_dir()
    assert tuple(worker._temporary_bundle_path(claim).iterdir()) == ()
    assert worker.artifact_reclaimer.quarantine_summary().bundle_count == 1


def test_obsolete_temporary_symlink_is_rejected_without_following(tmp_path: Path) -> None:
    old = _claim(_nshape_compare_spec(hold_days=(1,)))
    new = _retry_claim(old)
    worker = _worker(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "keep").write_text("safe", encoding="utf-8")
    obsolete = worker._temporary_bundle_path(old)
    obsolete.parent.mkdir(parents=True)
    obsolete.symlink_to(outside, target_is_directory=True)

    with pytest.raises(Exception, match="symlink"):
        worker._reclaim_obsolete_temporaries(new)

    assert (outside / "keep").read_text(encoding="utf-8") == "safe"


def test_obsolete_temporary_hardlink_is_rejected_without_deleting_tree(
    tmp_path: Path,
) -> None:
    from rquant.lab_worker import LabArtifactConflictError

    old = _claim(_nshape_compare_spec(hold_days=(1,)))
    new = _retry_claim(old)
    worker = _worker(tmp_path)
    obsolete = worker._temporary_bundle_path(old)
    nested = obsolete / "nested"
    nested.mkdir(parents=True)
    partial = nested / "partial.parquet"
    partial.write_bytes(b"partial")
    external = tmp_path / "external-partial.parquet"
    os.link(partial, external)

    with pytest.raises(LabArtifactConflictError, match="hard link"):
        worker._reclaim_obsolete_temporaries(new)

    assert obsolete.is_dir()
    assert partial.read_bytes() == b"partial"
    assert external.read_bytes() == b"partial"
    assert partial.stat().st_nlink == 2


def test_obsolete_temporary_parent_symlink_is_rejected_without_following(
    tmp_path: Path,
) -> None:
    old = _claim(_nshape_compare_spec(hold_days=(1,)))
    new = _retry_claim(old)
    worker = _worker(tmp_path)
    outside = tmp_path / "outside-parent"
    outside.mkdir()
    temporary_base = tmp_path / "artifacts" / ".tmp"
    temporary_base.mkdir(parents=True)
    (temporary_base / str(old.job_id)).symlink_to(outside, target_is_directory=True)
    obsolete = worker._temporary_bundle_path(old)
    obsolete.mkdir(parents=True)
    (obsolete / "keep").write_text("safe", encoding="utf-8")

    with pytest.raises(Exception, match="symlink"):
        worker._reclaim_obsolete_temporaries(new)

    assert (obsolete / "keep").read_text(encoding="utf-8") == "safe"


def test_conflicting_sealed_bundle_fails_closed(tmp_path: Path) -> None:
    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    registry = RecordingRegistry()
    claim = _claim(_nshape_compare_spec())
    claims.publish(claim)
    worker = _worker(
        tmp_path,
        registry=registry,
        claims=claims,
        reports=reports,
    )
    validated = registry.validate_claim(claim)
    worker._seal_result(claim, registry.execute_shard(validated, object()))
    (worker.sealed_bundle_path(claim) / "manifest.json").write_text("{}", encoding="utf-8")

    result = worker.run_once()

    assert result.status == "failed"
    assert registry.executions == 2
    assert isinstance(_reports(reports)[-1].body, LabShardFailed)


class _MetadataStore:
    def __init__(self, identity: DatasetSnapshotIdentity) -> None:
        from rquant.data_quality import STAGE1_AUDIT_RULE_SET_VERSION

        eligibility_hash = "e" * 64
        as_of_time = datetime(2026, 2, 11, tzinfo=UTC)
        self.snapshot = SimpleNamespace(
            snapshot_id=identity.snapshot_id,
            status="ready",
            strategy_name="n_shape",
            as_of_time=as_of_time,
            code_commit="1" * 40,
            manifest_id="m" * 64,
            table_watermarks={
                "manifest_start_date": "2026-01-01",
                "manifest_end_date": "2026-02-10",
                "eligibility_resolution_hash": eligibility_hash,
            },
        )
        eligibility_artifact = SimpleNamespace(
            dataset_id="strategy_eligibility",
            table_name="strategy_eligibility",
            artifact_key=f"strategy_eligibility:{eligibility_hash}",
        )
        manifest = SimpleNamespace(
            strategy_name="n_shape",
            code_commit="1" * 40,
            as_of_time=as_of_time,
            start_date=date(2026, 1, 1),
            end_date=date(2026, 2, 10),
            eligibility_resolution_hash=eligibility_hash,
            eligibility_expected_dates=100,
            eligibility_complete_dates=100,
            artifacts=(eligibility_artifact,),
        )
        self.binding = SimpleNamespace(
            snapshot_id=identity.snapshot_id,
            binding_hash=identity.binding_hash,
            status="ready",
            manifest=manifest,
        )
        self.audit = SimpleNamespace(
            audit_run_id=identity.audit_run_id,
            status="completed",
            rule_set_version=STAGE1_AUDIT_RULE_SET_VERSION,
            range_start=date(2026, 1, 1),
            range_end=date(2026, 2, 10),
            as_of_date=date(2026, 2, 10),
            p0_count=0,
        )
        self.coverages = tuple(
            SimpleNamespace(
                snapshot_id=identity.snapshot_id,
                coverage_scope=scope,
                expected_count=100,
                available_count=100,
                coverage_ratio=1.0,
            )
            for scope in ("eligibility", "baseline", "entry", "exit")
        )

    def get_dataset_snapshot(self, snapshot_id: str):
        return self.snapshot if snapshot_id == self.snapshot.snapshot_id else None

    def get_dataset_snapshot_binding(self, snapshot_id: str):
        return self.binding if snapshot_id == self.binding.snapshot_id else None

    def get_data_audit_run(self, audit_run_id: str):
        return self.audit if audit_run_id == self.audit.audit_run_id else None

    def list_dataset_coverages(self, snapshot_id: str):
        return self.coverages if snapshot_id == self.snapshot.snapshot_id else ()

    def list_open_data_quality_issues(self, **_kwargs: object):
        return ()


class ObjectContext:
    def __init__(self, value: object) -> None:
        self.value = value

    def __enter__(self) -> object:
        return self.value

    def __exit__(self, *_args: object) -> None:
        return None


class MetadataStoreFactory:
    def __init__(self, store: object) -> None:
        self.store = store

    def __call__(self) -> ObjectContext:
        return ObjectContext(self.store)


class FakeExecutionSessionFactory:
    def __init__(self, *, expected_binding: object, expected_lake_root: Path) -> None:
        self.expected_binding = expected_binding
        self.expected_lake_root = expected_lake_root
        self.opened = multiprocessing.get_context("spawn").Value("i", 0)

    def __call__(self, binding: object, lake_root: Path) -> ObjectContext:
        if binding != self.expected_binding:
            raise AssertionError("research session received the wrong binding")
        if lake_root != self.expected_lake_root:
            raise AssertionError("research session received the wrong lake root")
        with self.opened.get_lock():
            self.opened.value += 1
        return ObjectContext(object())


class RecordingResearchStoreOpener:
    def __init__(self) -> None:
        self.requests = multiprocessing.get_context("spawn").Queue()

    @contextmanager
    def __call__(self, request: object, **_kwargs: object) -> Iterator[tuple[object, object]]:
        self.requests.put(request)
        yield object(), object()

    def close(self) -> None:
        self.requests.close()
        self.requests.join_thread()


def _formal_spec() -> ResearchRunSpec:
    identity = DatasetSnapshotIdentity(
        snapshot_id="a" * 64,
        binding_hash="b" * 64,
        audit_run_id="c" * 64,
    )
    return _nshape_compare_spec(hold_days=(1,)).model_copy(
        update={
            "dataset_snapshot": identity,
            "research_status": "comparable",
        }
    )


def test_formal_job_opens_verified_research_execution_session(
    tmp_path: Path,
) -> None:
    spec = _formal_spec()
    identity = spec.dataset_snapshot
    assert identity is not None
    metadata = _MetadataStore(identity)
    lake_root = tmp_path / "lake"
    execution_session_factory = FakeExecutionSessionFactory(
        expected_binding=metadata.binding,
        expected_lake_root=lake_root,
    )
    claims = LabClaimSpool(tmp_path / "claims")
    registry = RecordingRegistry()
    claims.publish(_claim(spec))
    worker = _worker(
        tmp_path,
        registry=registry,
        claims=claims,
        exploratory_store_factory=None,
        metadata_store_factory=MetadataStoreFactory(metadata),
        lake_root=lake_root,
        execution_session_factory=execution_session_factory,
    )

    result = worker.run_once()

    assert result.status == "succeeded"
    assert execution_session_factory._closed_opened_path.is_file()
    assert registry.executions == 1


def test_legacy_formal_spec_uses_canonical_snapshot_strategy(
    tmp_path: Path,
) -> None:
    spec = _formal_spec().model_copy(
        update={
            "parameters": _formal_spec().parameters.model_copy(
                update={"strategy_name": "NShapeCompare"}
            )
        }
    )
    identity = spec.dataset_snapshot
    assert identity is not None
    metadata = _MetadataStore(identity)
    research_store_opener = RecordingResearchStoreOpener()

    claims = LabClaimSpool(tmp_path / "claims")
    claims.publish(_claim(spec))
    worker = _worker(
        tmp_path,
        claims=claims,
        exploratory_store_factory=None,
        metadata_store_factory=MetadataStoreFactory(metadata),
        lake_root=tmp_path / "lake",
        research_store_opener=research_store_opener,
    )

    try:
        result = worker.run_once()
        from rquant.research_gate import ResearchGateRequest

        request = ResearchGateRequest.model_validate_json(
            research_store_opener._closed_request_path.read_text(encoding="utf-8")
        )
    finally:
        research_store_opener.close()

    assert result.status == "succeeded"
    assert request.strategy_name == "n_shape"


def test_formal_snapshot_identity_mismatch_fails_before_execution(tmp_path: Path) -> None:
    spec = _formal_spec()
    identity = spec.dataset_snapshot
    assert identity is not None
    metadata = _MetadataStore(identity)
    metadata.binding.binding_hash = "d" * 64

    claims = LabClaimSpool(tmp_path / "claims")
    registry = RecordingRegistry()
    claims.publish(_claim(spec))
    worker = _worker(
        tmp_path,
        registry=registry,
        claims=claims,
        exploratory_store_factory=None,
        metadata_store_factory=MetadataStoreFactory(metadata),
        lake_root=tmp_path / "lake",
    )

    result = worker.run_once()

    assert result.status == "failed"
    assert registry.executions == 0


def test_formal_runtime_clean_code_sha_must_match_spec(tmp_path: Path) -> None:
    spec = _formal_spec()
    identity = spec.dataset_snapshot
    assert identity is not None

    claims = LabClaimSpool(tmp_path / "claims")
    registry = RecordingRegistry()
    claims.publish(_claim(spec))
    worker = _worker(
        tmp_path,
        registry=registry,
        claims=claims,
        exploratory_store_factory=None,
        metadata_store_factory=MetadataStoreFactory(_MetadataStore(identity)),
        lake_root=tmp_path / "lake",
        verified_code_sha_provider=lambda: "f" * 40,
    )

    result = worker.run_once()

    assert result.status == "failed"
    assert registry.executions == 0


def test_formal_reuses_full_research_gate_evidence_checks(tmp_path: Path) -> None:
    spec = _formal_spec()
    identity = spec.dataset_snapshot
    assert identity is not None
    metadata = _MetadataStore(identity)
    metadata.snapshot.strategy_name = "wrong_strategy"
    metadata.audit.p0_count = 1

    claims = LabClaimSpool(tmp_path / "claims")
    registry = RecordingRegistry()
    claims.publish(_claim(spec))
    worker = _worker(
        tmp_path,
        registry=registry,
        claims=claims,
        exploratory_store_factory=None,
        metadata_store_factory=MetadataStoreFactory(metadata),
        lake_root=tmp_path / "lake",
    )

    result = worker.run_once()

    assert result.status == "failed"
    assert registry.executions == 0


def test_worker_module_has_no_control_db_network_or_notification_imports() -> None:
    import rquant.lab_worker as lab_worker
    import rquant.strategy_job_adapters as adapters

    source = (inspect.getsource(lab_worker) + inspect.getsource(adapters)).lower()
    forbidden = (
        "lab_jobs",
        "sqlite3",
        "duckdbstore",
        "tushare",
        "akshare",
        "ashare",
        "mootdx",
        "notifier",
        "rquant.notify",
    )

    assert not [name for name in forbidden if name in source]


def test_worker_execution_runtime_blocks_provider_and_notification_imports(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    claims.publish(_claim(_nshape_compare_spec()))
    original_import = builtins.__import__
    forbidden_prefixes = (
        "rquant.adapter",
        "rquant.notify",
        "tushare",
        "akshare",
        "ashare",
        "mootdx",
    )

    def guarded_import(
        name: str,
        globals: dict[str, object] | None = None,
        locals: dict[str, object] | None = None,
        fromlist: tuple[str, ...] = (),
        level: int = 0,
    ) -> object:
        if name.lower().startswith(forbidden_prefixes):
            raise AssertionError(f"forbidden worker import: {name}")
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", guarded_import)
    worker = _worker(tmp_path, claims=claims, reports=reports)

    result = worker.run_once()

    assert result.status == "succeeded"
    assert isinstance(_reports(reports)[-1].body, LabShardSucceeded)


def test_success_receipt_timeout_stays_pending_without_failed_report(
    tmp_path: Path,
) -> None:
    from rquant.lab_job_protocol import LabCommandEnvelope, LabCommandSpool, SubmitJobCommand
    from rquant.lab_jobs import JobStatus, LabJobReader, LabJobStore
    from rquant.lab_scheduler import LabScheduler

    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    commands = LabCommandSpool(tmp_path / "commands")
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    job_id = uuid4()
    commands.publish(
        LabCommandEnvelope(
            request_id=uuid4(),
            command=SubmitJobCommand(
                job_id=job_id,
                spec=_nshape_compare_spec(hold_days=(1,)),
                max_attempts=2,
            ),
        )
    )
    scheduler = LabScheduler(
        store=store,
        spool=commands,
        owner_id="scheduler-a",
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=5,
        report_spool=reports,
        claim_spool=claims,
        claim_worker_ids=("worker-a",),
        shard_lease_seconds=20,
        adapter_registry=default_strategy_job_adapter_registry(),
        clock=lambda: NOW,
    )
    scheduler.run_once()
    registry = RecordingRegistry()

    def delayed_success(
        report: LabWorkerReport,
        _timeout_seconds: float,
        _stop: object,
    ) -> LabReportReceipt:
        if isinstance(report.body, LabShardSucceeded):
            raise TimeoutError("delayed success receipt")
        return _accept_report(report, _timeout_seconds, _stop)

    worker = _worker(
        tmp_path,
        registry=registry,
        claims=claims,
        reports=reports,
        receipt_waiter=delayed_success,
    )

    first = worker.run_once()
    bodies = tuple(report.body for report in _reports(reports))

    assert first.status == "awaiting_receipt"
    assert first.report_id is not None
    assert first.manifest_hash is not None
    assert sum(isinstance(body, LabShardSucceeded) for body in bodies) == 1
    assert not any(isinstance(body, LabShardFailed) for body in bodies)

    scheduler.run_once()
    worker.receipt_waiter = worker._wait_for_receipt
    second = worker.run_once()
    job = LabJobReader(store.path).get_job(job_id)
    scheduler.release()

    assert second.status == "succeeded"
    assert second.report_id == first.report_id
    assert registry.executions == 1
    assert job is not None and job.status is JobStatus.RUNNING
    assert job.result_state.value == "ready"


def test_success_receipt_transport_error_is_unknown_without_failed_report(
    tmp_path: Path,
) -> None:
    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims.publish(claim)

    def unavailable_receipt(
        report: LabWorkerReport,
        _timeout_seconds: float,
        _stop: object,
    ) -> LabReportReceipt:
        if isinstance(report.body, LabShardSucceeded):
            raise OSError("receipt channel unavailable")
        return _accept_report(report, _timeout_seconds, _stop)

    worker = _worker(
        tmp_path,
        claims=claims,
        reports=reports,
        receipt_waiter=unavailable_receipt,
    )

    result = worker.run_once()
    bodies = tuple(report.body for report in _reports(reports))

    assert result.status == "unknown"
    assert result.report_id is not None
    assert result.manifest_hash is not None
    assert sum(isinstance(body, LabShardSucceeded) for body in bodies) == 1
    assert not any(isinstance(body, LabShardFailed) for body in bodies)


def test_success_publish_failure_retries_same_report_without_reexecution(
    tmp_path: Path,
) -> None:
    class FailFirstSuccessSpool(LabReportSpool):
        def __init__(self, root: Path) -> None:
            super().__init__(root)
            self.failed = False

        def publish(self, report: LabWorkerReport):
            if isinstance(report.body, LabShardSucceeded) and not self.failed:
                self.failed = True
                raise OSError("injected success publish failure")
            return super().publish(report)

    claims = LabClaimSpool(tmp_path / "claims")
    reports = FailFirstSuccessSpool(tmp_path / "reports")
    registry = RecordingRegistry()
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims.publish(claim)
    worker = _worker(
        tmp_path,
        registry=registry,
        claims=claims,
        reports=reports,
    )

    first = worker.run_once()
    second = worker.run_once()
    bodies = tuple(report.body for report in _reports(reports))

    assert first.status == "unknown"
    assert second.status == "succeeded"
    assert first.report_id == second.report_id
    assert registry.executions == 1
    assert sum(isinstance(body, LabShardSucceeded) for body in bodies) == 1
    assert not any(isinstance(body, LabShardFailed) for body in bodies)


def test_worker_success_report_uses_monotonic_duration_and_claim_work_plan(
    tmp_path: Path,
) -> None:
    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    assert claim.definition.work_plan is not None
    claims.publish(claim)
    monotonic_values = iter((100.0, 102.5))
    worker = _worker(
        tmp_path,
        claims=claims,
        reports=reports,
        monotonic_clock=lambda: next(monotonic_values),
    )

    result = worker.run_once()
    success = next(
        report.body for report in _reports(reports) if isinstance(report.body, LabShardSucceeded)
    )

    assert result.status == "succeeded"
    assert success.telemetry is not None
    assert success.telemetry.phase == claim.definition.work_plan.phase
    assert success.telemetry.work_unit_name == claim.definition.work_plan.work_unit_name
    assert success.telemetry.work_units == claim.definition.work_plan.work_units
    assert success.telemetry.static_duration_ms == claim.definition.work_plan.static_duration_ms
    assert success.telemetry.duration_ms == 2_500
    assert success.telemetry.throughput_units_per_second == pytest.approx(
        claim.definition.work_plan.work_units / 2.5
    )


def test_worker_executes_frozen_p13_claim_and_reports_success_without_telemetry(
    tmp_path: Path,
) -> None:
    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    claim = _p13_frozen_claim()
    claims.publish(claim)
    worker = _worker(
        tmp_path,
        worker_id=claim.worker_id,
        claims=claims,
        reports=reports,
    )

    result = worker.run_once()
    success = next(
        report.body for report in _reports(reports) if isinstance(report.body, LabShardSucceeded)
    )

    assert result.status == "succeeded"
    assert success.telemetry is None


def test_stop_after_success_publish_keeps_single_reported_terminal(
    tmp_path: Path,
) -> None:
    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims.publish(claim)
    worker = None

    def stop_after_success(
        report: LabWorkerReport,
        _timeout_seconds: float,
        _stop: object,
    ) -> LabReportReceipt:
        if isinstance(report.body, LabShardSucceeded):
            assert worker is not None
            worker.request_stop()
            raise InterruptedError("stop after success publish")
        return _accept_report(report, _timeout_seconds, _stop)

    worker = _worker(
        tmp_path,
        claims=claims,
        reports=reports,
        receipt_waiter=stop_after_success,
    )

    result = worker.run_once()
    bodies = tuple(report.body for report in _reports(reports))

    assert result.status == "reported"
    assert sum(isinstance(body, LabShardSucceeded) for body in bodies) == 1
    assert not any(isinstance(body, LabShardFailed | LabWorkerStopped) for body in bodies)


@pytest.mark.parametrize(
    ("phase", "expected"),
    [
        (
            "before",
            {"failed": 0, "sealed": False, "status": "stopped", "stopped": 1, "succeeded": 0},
        ),
        (
            "after",
            {"failed": 0, "sealed": True, "status": "reported", "stopped": 0, "succeeded": 1},
        ),
    ],
)
def test_real_sigterm_never_deadlocks_success_publication_boundary(
    tmp_path: Path,
    phase: str,
    expected: dict[str, object],
) -> None:
    root = tmp_path / phase
    root.mkdir(mode=0o700)

    completed = _run_worker_child("_sigterm_publication_child", root, phase)

    assert completed.returncode == 0, completed.stderr
    assert json.loads((root / "result.json").read_text(encoding="utf-8")) == expected


def test_rejected_success_receipt_returns_failed_without_second_terminal_report(
    tmp_path: Path,
) -> None:
    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims.publish(claim)

    def reject_success(
        report: LabWorkerReport,
        _timeout_seconds: float,
        _stop: object,
    ) -> LabReportReceipt:
        return LabReportReceipt.from_report(
            report,
            status="rejected" if isinstance(report.body, LabShardSucceeded) else "accepted",
            reason="stale_success" if isinstance(report.body, LabShardSucceeded) else "accepted",
            accepted_at=NOW,
        )

    worker = _worker(
        tmp_path,
        claims=claims,
        reports=reports,
        receipt_waiter=reject_success,
    )

    result = worker.run_once()
    bodies = tuple(report.body for report in _reports(reports))

    assert result.status == "failed"
    assert sum(isinstance(body, LabShardSucceeded) for body in bodies) == 1
    assert not any(isinstance(body, LabShardFailed) for body in bodies)
    assert not worker.sealed_bundle_path(claim).exists()


def test_pending_success_rejects_old_attempt_receipt_then_accepts_current_receipt(
    tmp_path: Path,
) -> None:
    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    generation_one = _claim(_nshape_compare_spec(hold_days=(1,)))
    generation_two = _retry_claim(generation_one)
    claims.publish(generation_one)
    claims.publish(generation_two)

    def stale_success_receipt(
        report: LabWorkerReport,
        timeout_seconds: float,
        stop: object,
    ) -> LabReportReceipt:
        accepted = _accept_report(report, timeout_seconds, stop)
        if isinstance(report.body, LabShardSucceeded):
            return accepted.model_copy(
                update={
                    "claim_token": generation_one.claim_token,
                    "claim_generation": generation_one.claim_generation,
                    "scheduler_fencing_token": generation_one.scheduler_fencing_token,
                }
            )
        return accepted

    worker = _worker(
        tmp_path,
        claims=claims,
        reports=reports,
        receipt_waiter=stale_success_receipt,
    )

    stale = worker.run_once()
    worker.receipt_waiter = _accept_report
    converged = worker.run_once()

    assert stale.status == "unknown"
    assert stale.report_id is not None
    assert worker.sealed_bundle_path(generation_two).is_dir()
    assert converged.status == "succeeded"
    assert converged.report_id == stale.report_id


def test_worker_waits_for_real_scheduler_receipts_before_completion(tmp_path: Path) -> None:
    from rquant.lab_job_protocol import LabCommandEnvelope, LabCommandSpool, SubmitJobCommand
    from rquant.lab_jobs import JobStatus, LabJobReader, LabJobStore
    from rquant.lab_scheduler import LabScheduler

    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    commands = LabCommandSpool(tmp_path / "commands")
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    job_id = uuid4()
    commands.publish(
        LabCommandEnvelope(
            request_id=uuid4(),
            command=SubmitJobCommand(
                job_id=job_id,
                spec=_nshape_compare_spec(hold_days=(1,)),
                max_attempts=2,
            ),
        )
    )
    scheduler = LabScheduler(
        store=store,
        spool=commands,
        owner_id="scheduler-a",
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=5,
        report_spool=reports,
        claim_spool=claims,
        claim_worker_ids=("worker-a",),
        shard_lease_seconds=20,
        adapter_registry=default_strategy_job_adapter_registry(),
        clock=lambda: NOW,
    )
    scheduler.run_once()
    claim = claims.pending()[0].claim
    worker = _worker(
        tmp_path,
        claims=claims,
        reports=reports,
        # The receipts this case waits for are produced by the scheduler it
        # drives by hand below, for _observe(2). The default 0.2s receipt wait
        # is ten times shorter than that loop, so on a host where one
        # `scheduler.run_once()` costs more than a few milliseconds the worker
        # gave up first and reported 'awaiting_receipt'. The wait has to
        # outlast the window the case is willing to feed it.
        receipt_timeout_seconds=_observe(2),
        receipt_waiter=None,
    )
    outcomes = []
    thread = threading.Thread(target=lambda: outcomes.append(worker.run_once()))

    thread.start()
    timeout_at = time.monotonic() + _observe(2)
    while thread.is_alive() and time.monotonic() < timeout_at:
        scheduler.run_once()
        time.sleep(0.01)
    thread.join(timeout=_observe(0.2))
    scheduler.release()

    assert not thread.is_alive()
    assert outcomes[0].status == "succeeded"
    assert worker.sealed_bundle_path(claim).is_dir()
    job = LabJobReader(store.path).get_job(job_id)
    assert job is not None
    assert job.status is JobStatus.RUNNING
    assert job.result_state.value == "ready"
    receipts = tuple(reports.load_receipt(path) for path in sorted(reports.ack_dir.glob("*.json")))
    assert len(receipts) == 2
    assert all(receipt.status == "accepted" for receipt in receipts)


def _pinned_ack_publisher(
    reports: LabReportSpool,
    entry: object,
    receipt: LabReportReceipt,
    *,
    target_name: str,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[threading.Thread, threading.Event, threading.Event, list[BaseException]]:
    """Park a real ack publish inside its own link-to-unlink window.

    `_publish_no_clobber` makes the target name visible with `link(temporary,
    target)` and drops `temporary` only in its `finally`, so between those two
    syscalls the published inode carries two links while the publisher still holds
    the spool lock. Pinning the publisher there turns that window from a
    microsecond-wide timing accident into a state the case fully controls: for as
    long as `release` is unset the target provably has `st_nlink == 2`, so any link
    count a reader records is evidence about locking rather than a timing sample.
    """
    window_open = threading.Event()
    release = threading.Event()
    errors: list[BaseException] = []
    real_link = os.link

    def pinned_link(source: object, target: object, **kwargs: object) -> object:
        result = real_link(source, target, **kwargs)
        if target == target_name:
            window_open.set()
            # Bounded only so a regression cannot wedge the session; the case
            # always sets `release` itself on both paths.
            release.wait(_observe(10))
        return result

    monkeypatch.setattr(os, "link", pinned_link)

    def publish() -> None:
        try:
            reports.ack(entry, receipt)
        except BaseException as exc:  # noqa: BLE001 - reported to the case
            errors.append(exc)

    return threading.Thread(target=publish), window_open, release, errors


def _witness_evidence_lock(
    reports: LabReportSpool,
    progressed: threading.Event,
    monkeypatch: pytest.MonkeyPatch,
) -> list[str]:
    """Announce every attempt to take the spool lock, blocking or not.

    This is what lets the case release the pinned publisher at exactly the right
    moment without waiting on a clock. A reader that asks for the lock announces
    itself here - whether it then parks on `flock` or is refused and comes back a
    poll interval later - and a reader that never asks announces nothing and
    instead finishes early, which its own thread wrapper reports. Both outcomes
    set `progressed`, so the case advances on whichever actually happened.
    """
    attempts: list[str] = []
    real_evidence_lock = reports.evidence_lock
    real_try_evidence_lock = reports.try_evidence_lock

    @contextmanager
    def witnessed_evidence_lock() -> Iterator[None]:
        attempts.append("requested")
        progressed.set()
        with real_evidence_lock():
            yield

    @contextmanager
    def witnessed_try_evidence_lock() -> Iterator[bool]:
        attempts.append("requested")
        progressed.set()
        with real_try_evidence_lock() as acquired:
            yield acquired

    monkeypatch.setattr(reports, "evidence_lock", witnessed_evidence_lock)
    monkeypatch.setattr(reports, "try_evidence_lock", witnessed_try_evidence_lock)
    return attempts


def test_receipt_wait_never_reads_inside_the_publish_window(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`_wait_for_receipt` must not read a receipt that still carries two links.

    The `st_nlink == 1` rule is a real defence against an external hard link and
    stays exactly as it is; what changes is that the reader now takes the same lock
    the publisher already holds, so it can only ever observe the settled inode.
    """
    from loguru import logger

    from rquant.lab_worker import LabStopSignal

    reports = LabReportSpool(tmp_path / "reports")
    worker = _worker(tmp_path, reports=reports)
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    report = LabWorkerReport.from_claim(
        claim,
        report_id=uuid4(),
        reported_at=NOW,
        body=LabShardHeartbeat(lease_extension_seconds=30),
    )
    entry = reports.publish(report)
    receipt = LabReportReceipt.from_report(
        report,
        status="accepted",
        reason="accepted",
        accepted_at=NOW,
    )
    receipt_name = f"{report.report_id}.json"
    receipt_path = reports.ack_dir / receipt_name

    # Record the link count of every read this reader actually accepted, and the
    # reason of any read it rejected, so a regression names the cause instead of
    # leaving only a tick status behind.
    observed: list[object] = []
    original_read = reports._read_regular_child

    def record_read(path: Path, parent: Path, **kwargs: object) -> object:
        try:
            result = original_read(path, parent, **kwargs)
        except InvalidCommandEnvelopeError as exc:
            if Path(path).name == receipt_name:
                observed.append(f"rejected: {exc}")
            raise
        if Path(path).name == receipt_name:
            observed.append(result[2].st_nlink)
        return result

    monkeypatch.setattr(reports, "_read_regular_child", record_read)

    publisher, window_open, release, publish_errors = _pinned_ack_publisher(
        reports,
        entry,
        receipt,
        target_name=receipt_name,
        monkeypatch=monkeypatch,
    )
    messages: list[str] = []
    sink = logger.add(lambda message: messages.append(message.record["message"]), level="WARNING")
    progressed = threading.Event()
    lock_attempts = _witness_evidence_lock(reports, progressed, monkeypatch)
    waited: list[LabReportReceipt] = []
    wait_errors: list[BaseException] = []

    def wait() -> None:
        try:
            waited.append(
                worker._wait_for_receipt(
                    report,
                    worker._receipt_wait_timeout_seconds(),
                    LabStopSignal(),
                )
            )
        except BaseException as exc:  # noqa: BLE001 - reported to the case
            wait_errors.append(exc)
        finally:
            progressed.set()

    reader = threading.Thread(target=wait)
    try:
        publisher.start()
        assert window_open.wait(_observe(2))
        # The window is genuinely open: the name is visible and the publisher's own
        # temporary is still the inode's second link.
        assert os.stat(receipt_path).st_nlink == 2
        reader.start()
        # Hold the publisher in the window until the reader has really acted on it -
        # either it asked for the lock and is now parked, or it read without one and
        # is already finished. Releasing on a clock instead would let a reader that
        # skips the lock miss the window and pass by luck.
        assert progressed.wait(_observe(2)), (
            "reader neither asked for the spool lock nor finished: it is parked "
            f"somewhere else while the window is open, observed={observed}"
        )
        release.set()
        reader.join(timeout=_observe(2))
        publisher.join(timeout=_observe(2))
    finally:
        release.set()
        logger.remove(sink)

    assert not reader.is_alive()
    assert not publisher.is_alive()
    assert publish_errors == []
    assert wait_errors == []
    # The reader asked for the lock the publisher was holding ...
    assert lock_attempts != [], "reader read the receipt without taking the spool lock"
    # ... so it can only have read after the temporary link was gone.
    assert observed == [1]
    assert waited[0] == receipt
    assert [message for message in messages if "external hard link" in message] == []


# Codex's reproduction holds the evidence lock for 0.200s against a 0.010s receipt
# budget. Keep that shape: a bounded, self-releasing hold makes the regression fail
# fast in both directions instead of parking the session on the defect it is naming.
_LOCK_HOLD_SECONDS = 0.2
_RECEIPT_BUDGET_SECONDS = 0.01


@contextmanager
def _evidence_lock_held_in_thread(reports: LabReportSpool) -> Iterator[float]:
    """Hold the spool from another thread, covering the in-process half."""
    held = threading.Event()
    failures: list[BaseException] = []
    hold_seconds = _observe(_LOCK_HOLD_SECONDS)

    def hold() -> None:
        try:
            with reports.evidence_lock():
                held.set()
                time.sleep(hold_seconds)
        except BaseException as exc:  # noqa: BLE001 - reported to the case
            failures.append(exc)
            held.set()

    holder = threading.Thread(target=hold)
    holder.start()
    try:
        assert held.wait(_observe(2))
        assert failures == []
        yield hold_seconds
    finally:
        holder.join(timeout=_observe(10))
        assert not holder.is_alive()
    assert failures == []


@contextmanager
def _evidence_lock_held_in_subprocess(reports: LabReportSpool, tmp_path: Path) -> Iterator[float]:
    """Hold the spool's flock from a real other process, covering the cross-process half.

    The in-process thread lock is free here, so this pins the flock specifically:
    the attempt has to be refused by `LOCK_EX | LOCK_NB` rather than by the thread
    lock, which is the half a same-process holder can never exercise.
    """
    ready = tmp_path / "flock-held"
    hold_seconds = _observe(_LOCK_HOLD_SECONDS)
    program = (
        "import fcntl, pathlib, sys, time\n"
        "lock = open(sys.argv[1], 'r+')\n"
        "fcntl.flock(lock, fcntl.LOCK_EX)\n"
        "pathlib.Path(sys.argv[2]).write_text('held')\n"
        "time.sleep(float(sys.argv[3]))\n"
    )
    holder = subprocess.Popen(
        [sys.executable, "-c", program, str(reports._lock_path), str(ready), str(hold_seconds)]
    )
    try:
        deadline = time.monotonic() + _observe(5)
        while not ready.exists() and time.monotonic() < deadline:
            if holder.poll() is not None:
                raise AssertionError("flock holder exited before taking the lock")
            time.sleep(0.005)
        assert ready.exists(), "flock holder never reported taking the lock"
        yield hold_seconds
    finally:
        holder.wait(timeout=_observe(10))


def _receipt_wait_worker(tmp_path: Path, reports: LabReportSpool) -> tuple[object, object]:
    worker = _worker(tmp_path, reports=reports)
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    report = LabWorkerReport.from_claim(
        claim,
        report_id=uuid4(),
        reported_at=NOW,
        body=LabShardHeartbeat(lease_extension_seconds=30),
    )
    return worker, report


@pytest.mark.parametrize(
    "contention",
    ["thread", "subprocess"],
    ids=["in-process thread lock", "cross-process flock"],
)
def test_receipt_wait_honours_its_budget_while_the_evidence_lock_is_held(
    tmp_path: Path,
    contention: str,
) -> None:
    """A holder of the evidence lock must not become this call's deadline.

    The lock is shared with reclaim, migration and scan work that can hold it far
    longer than a receipt wait is allowed to take, so parking on it would let an
    unrelated holder set the deadline. The wait has to give up on its own budget
    while the holder is still holding.
    """
    from rquant.lab_worker import LabStopSignal

    reports = LabReportSpool(tmp_path / "reports")
    worker, report = _receipt_wait_worker(tmp_path, reports)
    holder = (
        _evidence_lock_held_in_thread(reports)
        if contention == "thread"
        else _evidence_lock_held_in_subprocess(reports, tmp_path)
    )
    budget = _observe(_RECEIPT_BUDGET_SECONDS)

    with holder as hold_seconds:
        started = time.monotonic()
        with pytest.raises(TimeoutError):
            worker._wait_for_receipt(report, budget, LabStopSignal())
        elapsed = time.monotonic() - started

    # Returned while the lock was still held, and inside budget plus the one poll
    # interval the loop is allowed to sleep before rechecking.
    assert elapsed < hold_seconds, f"waited for the lock holder: {elapsed:.3f}s"
    assert elapsed < budget + _observe(0.05) + _observe(0.05)


@pytest.mark.parametrize(
    "contention",
    ["thread", "subprocess"],
    ids=["in-process thread lock", "cross-process flock"],
)
def test_receipt_wait_stops_while_the_evidence_lock_is_held(
    tmp_path: Path,
    contention: str,
) -> None:
    """A stop request is answered while someone else holds the lock."""
    from rquant.lab_worker import LabStopSignal

    reports = LabReportSpool(tmp_path / "reports")
    worker, report = _receipt_wait_worker(tmp_path, reports)
    stop = LabStopSignal()
    stop.request()
    holder = (
        _evidence_lock_held_in_thread(reports)
        if contention == "thread"
        else _evidence_lock_held_in_subprocess(reports, tmp_path)
    )

    with holder as hold_seconds:
        started = time.monotonic()
        with pytest.raises(InterruptedError):
            worker._wait_for_receipt(report, _observe(30), stop)
        elapsed = time.monotonic() - started

    assert elapsed < hold_seconds, f"waited for the lock holder: {elapsed:.3f}s"
    assert elapsed < _observe(0.05) + _observe(0.05)


def test_receipt_wait_reads_the_receipt_once_the_evidence_lock_is_free(
    tmp_path: Path,
) -> None:
    """Giving up early must not cost correctness once the lock is available."""
    from rquant.lab_worker import LabStopSignal

    reports = LabReportSpool(tmp_path / "reports")
    worker, report = _receipt_wait_worker(tmp_path, reports)
    entry = reports.publish(report)
    receipt = LabReportReceipt.from_report(
        report,
        status="accepted",
        reason="accepted",
        accepted_at=NOW,
    )
    reports.ack(entry, receipt)

    with _evidence_lock_held_in_thread(reports), pytest.raises(TimeoutError):
        worker._wait_for_receipt(report, _observe(_RECEIPT_BUDGET_SECONDS), LabStopSignal())

    assert worker._wait_for_receipt(report, _observe(2), LabStopSignal()) == receipt


def _thread_lock_free_for_another_thread(reports: LabReportSpool) -> bool:
    """Probe the spool's thread half from a thread that cannot already own it.

    The probe has to run off the main thread: `_thread_lock` is an `RLock`, so a
    main-thread `acquire(blocking=False)` succeeds by reentrancy even while this
    very frame holds it, which would make the check vacuously true.
    """
    outcome: list[bool] = []

    def probe() -> None:
        taken = reports._thread_lock.acquire(blocking=False)
        outcome.append(taken)
        if taken:
            reports._thread_lock.release()

    prober = threading.Thread(target=probe)
    prober.start()
    prober.join(_observe(5))
    assert not prober.is_alive(), "thread-half probe never finished"
    assert len(outcome) == 1
    return outcome[0]


def _open_descriptor_count() -> int:
    """Count this process's open descriptors.

    `os.listdir` opens one descriptor of its own for the directory it is reading,
    and closes it before returning, so the same transient appears in every sample
    and cancels out between them.
    """
    return len(os.listdir("/dev/fd"))


def test_refused_try_evidence_lock_holds_nothing_inside_the_false_body(
    tmp_path: Path,
) -> None:
    """A refused attempt must hand back a body that owns nothing.

    `_exclusive_lock` promises the half it took is "released before yielding
    False". A `@contextmanager` generator suspends at its `yield`, so releasing in
    a `finally` releases only after the caller has left the `with` block - during
    the whole False body the refused attempt still held the thread lock and both
    descriptors, and a caller that reacts to `acquired is False` by doing other
    spool work would deadlock against itself or leak.
    """
    reports = LabReportSpool(tmp_path / "reports")

    with _evidence_lock_held_in_subprocess(reports, tmp_path):
        descriptors_before = _open_descriptor_count()
        with reports.try_evidence_lock() as acquired:
            assert acquired is False
            assert _thread_lock_free_for_another_thread(reports), (
                "refused attempt still holds the thread lock inside the False body"
            )
            assert _open_descriptor_count() == descriptors_before, (
                "refused attempt still holds descriptors inside the False body"
            )
            assert reports._active_lock_descriptor is None
            assert reports._active_lock_parent_descriptor is None
        assert _open_descriptor_count() == descriptors_before


def test_refused_try_evidence_lock_leaves_the_spool_reusable(
    tmp_path: Path,
) -> None:
    """Repeated refusals must not accumulate, and must not poison the spool.

    Every refusal releases both halves on its own, so twenty of them in a row cost
    nothing, and once the other holder is gone the same spool takes the lock again
    - both the non-blocking and the blocking way.
    """
    reports = LabReportSpool(tmp_path / "reports")
    # Sampled before the holder exists so the holder's own bookkeeping cannot
    # show up as a leak once it is reaped.
    descriptors_at_rest = _open_descriptor_count()

    with _evidence_lock_held_in_subprocess(reports, tmp_path):
        descriptors_before = _open_descriptor_count()
        for attempt in range(20):
            with reports.try_evidence_lock() as acquired:
                assert acquired is False, f"lock holder released early on attempt {attempt}"
                assert _thread_lock_free_for_another_thread(reports), (
                    f"attempt {attempt} still holds the thread lock"
                )
            assert _open_descriptor_count() == descriptors_before, (
                f"attempt {attempt} leaked a descriptor"
            )
            assert _thread_lock_free_for_another_thread(reports)

    with reports.try_evidence_lock() as acquired:
        assert acquired is True
    with reports.evidence_lock():
        pass

    assert _open_descriptor_count() == descriptors_at_rest
    assert reports._active_lock_descriptor is None
    assert reports._active_lock_parent_descriptor is None
    assert reports._active_lock_identity is None
    assert reports._active_root_descriptor is None
    assert _thread_lock_free_for_another_thread(reports)


@pytest.mark.parametrize("exhausted", ["stop", "budget"])
def test_receipt_wait_never_tries_the_lock_once_stop_or_budget_is_gone(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    exhausted: str,
) -> None:
    """Stop and budget are answered before the lock is ever reached for.

    Ordering is the whole fix for RQ-RB-P1-02: the evidence lock is shared with
    reclaim, migration and scan work, so a wait that reaches for it before reading
    its own stop flag and remaining budget hands its deadline to an unrelated
    holder. A stub that both counts and raises pins the order in either direction.
    """
    import rquant.lab_worker as lab_worker
    from rquant.lab_worker import LabStopSignal

    reports = LabReportSpool(tmp_path / "reports")
    worker, report = _receipt_wait_worker(tmp_path, reports)
    attempts: list[str] = []

    def refuse_the_attempt() -> object:
        attempts.append(exhausted)
        raise AssertionError("try_evidence_lock must not be attempted")

    monkeypatch.setattr(reports, "try_evidence_lock", refuse_the_attempt)

    stop = LabStopSignal()
    if exhausted == "stop":
        stop.request()
        expected: type[BaseException] = InterruptedError
    else:
        # The first reading sets the deadline; every later one is already past it,
        # so the budget is spent before the first pass reaches the lock.
        readings = iter((0,))

        def spent_clock() -> int:
            return next(readings, 10**12)

        monkeypatch.setattr(lab_worker, "_monotonic_microseconds", spent_clock)
        expected = TimeoutError

    with pytest.raises(expected):
        worker._wait_for_receipt(report, 30.0, stop)

    assert attempts == []


def test_preflight_evidence_scan_never_reads_inside_the_publish_window(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The preflight evidence scan takes the same lock as its locked twin.

    `_preflight` used to snapshot `ack_dir` with an unlocked glob and read the
    receipts outside the spool lock, so it could reject a receipt the publisher was
    still linking into place and fail a reclaim that had nothing wrong with it.
    """
    from loguru import logger

    from rquant.lab_worker import LabArtifactReclaimer

    reports = LabReportSpool(tmp_path / "reports")
    old_claim, current_claim, _sealed, manifest = _sealed_obsolete_attempt(tmp_path, reports)
    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=reports,
    )
    heartbeat = LabWorkerReport.from_claim(
        old_claim,
        report_id=uuid4(),
        reported_at=NOW,
        body=LabShardHeartbeat(lease_extension_seconds=30),
    )
    entry = reports.publish(heartbeat)
    receipt = LabReportReceipt.from_report(
        heartbeat,
        status="rejected",
        reason="claim_generation_mismatch",
        accepted_at=NOW,
    )
    receipt_name = f"{heartbeat.report_id}.json"
    receipt_path = reports.ack_dir / receipt_name

    observed: list[object] = []
    original_read = reports._read_regular_child

    def record_read(path: Path, parent: Path, **kwargs: object) -> object:
        try:
            result = original_read(path, parent, **kwargs)
        except InvalidCommandEnvelopeError as exc:
            if Path(path).name == receipt_name:
                observed.append(f"rejected: {exc}")
            raise
        if Path(path).name == receipt_name:
            observed.append(result[2].st_nlink)
        return result

    monkeypatch.setattr(reports, "_read_regular_child", record_read)

    publisher, window_open, release, publish_errors = _pinned_ack_publisher(
        reports,
        entry,
        receipt,
        target_name=receipt_name,
        monkeypatch=monkeypatch,
    )
    messages: list[str] = []
    sink = logger.add(lambda message: messages.append(message.record["message"]), level="WARNING")
    progressed = threading.Event()
    lock_attempts = _witness_evidence_lock(reports, progressed, monkeypatch)
    scan_errors: list[BaseException] = []

    def scan() -> None:
        try:
            reclaimer._assert_no_terminal_success_evidence(old_claim, manifest, current_claim)
        except BaseException as exc:  # noqa: BLE001 - reported to the case
            scan_errors.append(exc)
        finally:
            progressed.set()

    scanner = threading.Thread(target=scan)
    try:
        publisher.start()
        assert window_open.wait(_observe(2))
        assert os.stat(receipt_path).st_nlink == 2
        scanner.start()
        assert progressed.wait(_observe(2)), (
            "preflight scan neither asked for the spool lock nor finished: an "
            "unlocked snapshot blocks in pending() instead of holding one lock "
            f"across discovery and read, observed={observed}"
        )
        release.set()
        scanner.join(timeout=_observe(2))
        publisher.join(timeout=_observe(2))
    finally:
        release.set()
        logger.remove(sink)

    assert not scanner.is_alive()
    assert not publisher.is_alive()
    assert publish_errors == []
    assert scan_errors == []
    assert lock_attempts != [], "preflight scan read receipts without taking the spool lock"
    assert observed == [1]
    assert [message for message in messages if "external hard link" in message] == []


def test_crash_without_report_is_reclaimed_by_existing_lease_recovery(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.lab_job_protocol import LabCommandEnvelope, LabCommandSpool, SubmitJobCommand
    from rquant.lab_jobs import LabJobStore
    from rquant.lab_scheduler import LabScheduler

    class WorkerCrash(BaseException):
        pass

    clock = [NOW]
    claims = LabClaimSpool(tmp_path / "claims")
    reports = LabReportSpool(tmp_path / "reports")
    commands = LabCommandSpool(tmp_path / "commands")
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    spec = _nshape_compare_spec(hold_days=(1,))
    commands.publish(
        LabCommandEnvelope(
            request_id=uuid4(),
            command=SubmitJobCommand(job_id=uuid4(), spec=spec, max_attempts=2),
        )
    )
    scheduler = LabScheduler(
        store=store,
        spool=commands,
        owner_id="scheduler-a",
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=5,
        report_spool=reports,
        claim_spool=claims,
        claim_worker_ids=("worker-a",),
        shard_lease_seconds=20,
        adapter_registry=default_strategy_job_adapter_registry(),
        clock=lambda: clock[0],
    )
    scheduler.run_once()
    original = claims.pending()[0].claim
    worker = _worker(
        tmp_path,
        registry=RecordingRegistry(),
        claims=claims,
        reports=reports,
    )
    monkeypatch.setattr(
        worker,
        "_execute_shard_isolated",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(WorkerCrash()),
    )

    with pytest.raises(WorkerCrash):
        worker.run_once()

    assert claims.pending() == ()
    assert reports.pending() == ()
    clock[0] = NOW + timedelta(seconds=21)
    assert scheduler.lease is not None
    recovered_job_ids = store.recover_stale_shards(scheduler.lease, now=clock[0])
    assert recovered_job_ids == (original.job_id,)
    result = scheduler.run_once()
    recovered = claims.pending()[0].claim

    assert result.recovered == 0
    assert recovered.shard_id == original.shard_id
    assert recovered.claim_generation == original.claim_generation + 1
    assert recovered.claim_token != original.claim_token


def test_hard_crash_after_rename_is_reclaimed_before_generation_two_runs(
    tmp_path: Path,
) -> None:
    from rquant.lab_job_protocol import LabCommandEnvelope, LabCommandSpool, SubmitJobCommand
    from rquant.lab_jobs import JobStatus, LabJobReader, LabJobStore
    from rquant.lab_scheduler import LabScheduler
    from rquant.lab_worker import LabArtifactReclaimer

    clock = [NOW]
    artifact_root = tmp_path / "artifacts"
    reports = LabReportSpool(tmp_path / "reports")
    reclaimer = LabArtifactReclaimer(
        artifact_root=artifact_root,
        report_spool=reports,
    )
    claims = LabClaimSpool(
        tmp_path / "claims",
        claim_advance_hook=reclaimer.reclaim,
    )
    commands = LabCommandSpool(tmp_path / "commands")
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    job_id = uuid4()
    commands.publish(
        LabCommandEnvelope(
            request_id=uuid4(),
            command=SubmitJobCommand(
                job_id=job_id,
                spec=_nshape_compare_spec(hold_days=(1,)),
                max_attempts=2,
            ),
        )
    )
    scheduler = LabScheduler(
        store=store,
        spool=commands,
        owner_id="scheduler-a",
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=5,
        report_spool=reports,
        claim_spool=claims,
        claim_worker_ids=("worker-a",),
        shard_lease_seconds=20,
        adapter_registry=default_strategy_job_adapter_registry(),
        clock=lambda: clock[0],
    )
    scheduler.run_once()
    generation_one = claims.pending()[0].claim
    crashed = _run_worker_child("_crash_after_atomic_rename_child", tmp_path)
    sealed_one = reclaimer.sealed_bundle_path(generation_one)

    assert crashed.returncode == 77, crashed.stderr
    assert sealed_one.is_dir()

    clock[0] = NOW + timedelta(seconds=21)
    assert scheduler.lease is not None
    recovered_job_ids = store.recover_stale_shards(scheduler.lease, now=clock[0])
    assert recovered_job_ids == (job_id,)
    recovery = scheduler.run_once()
    generation_two = claims.pending()[0].claim

    assert recovery.recovered == 0
    assert generation_two.claim_generation == generation_one.claim_generation + 1
    assert not sealed_one.exists()

    worker = _worker(
        tmp_path,
        claims=claims,
        reports=reports,
        # Same reason as the sibling case above: the receipts come from the
        # scheduler this case drives for _observe(3), so the worker's own
        # receipt wait cannot be the 0.2s default.
        receipt_timeout_seconds=_observe(3),
        receipt_waiter=None,
        clock=lambda: clock[0],
    )
    outcomes = []
    thread = threading.Thread(target=lambda: outcomes.append(worker.run_once()))
    thread.start()
    timeout_at = time.monotonic() + _observe(3)
    while thread.is_alive() and time.monotonic() < timeout_at:
        scheduler.run_once()
        time.sleep(0.01)
    thread.join(timeout=_observe(0.2))
    job = LabJobReader(store.path).get_job(job_id)
    scheduler.release()

    assert not thread.is_alive()
    assert outcomes[0].status == "succeeded"
    assert worker.sealed_bundle_path(generation_two).is_dir()
    assert job is not None and job.status is JobStatus.RUNNING
    assert job.result_state.value == "ready"


def test_stale_pending_success_does_not_block_obsolete_sealed_reclamation(
    tmp_path: Path,
) -> None:
    from rquant.lab_worker import LabArtifactReclaimer

    reports = LabReportSpool(tmp_path / "reports")
    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=reports,
    )
    claims = LabClaimSpool(
        tmp_path / "claims",
        claim_advance_hook=reclaimer.reclaim,
    )
    generation_one = _claim(_nshape_compare_spec(hold_days=(1,)))
    claims.publish(generation_one)
    worker = _worker(tmp_path, claims=claims, reports=reports)
    validated = worker._validate_closed_claim(generation_one)
    result = RecordingRegistry().execute_shard(validated, object())
    manifest = worker._seal_result(generation_one, result)
    success = LabWorkerReport.from_claim(
        generation_one,
        report_id=uuid4(),
        reported_at=NOW,
        body=LabShardSucceeded.current(
            result_manifest_hash=manifest.manifest_hash,
            worker_code_sha="1" * 40,
        ),
    )
    reports.publish(success)

    generation_two = _retry_claim(generation_one)
    claims.publish(generation_two)
    outcomes = claims.reconcile_current()

    assert claims.current(generation_one.job_id, generation_one.shard_id).claim == generation_two
    assert outcomes[0].status == "reconciled"
    assert not worker.sealed_bundle_path(generation_one).exists()
    assert reports.pending()[0].report == success


def test_rejected_success_receipt_allows_obsolete_sealed_reclamation(
    tmp_path: Path,
) -> None:
    from rquant.lab_worker import LabArtifactReclaimer

    reports = LabReportSpool(tmp_path / "reports")
    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=reports,
    )
    generation_one = _claim(_nshape_compare_spec(hold_days=(1,)))
    worker = _worker(tmp_path, reports=reports)
    validated = worker._validate_closed_claim(generation_one)
    result = RecordingRegistry().execute_shard(validated, object())
    manifest = worker._seal_result(generation_one, result)
    success = LabWorkerReport.from_claim(
        generation_one,
        report_id=uuid4(),
        reported_at=NOW,
        body=LabShardSucceeded(result_manifest_hash=manifest.manifest_hash),
    )
    entry = reports.publish(success)
    reports.ack(
        entry,
        LabReportReceipt.from_report(
            success,
            status="rejected",
            reason="claim_generation_mismatch",
            accepted_at=NOW,
        ),
    )

    reclaimer.reclaim(_retry_claim(generation_one))

    assert not worker.sealed_bundle_path(generation_one).exists()


def test_reclaimer_preserves_current_attempt_and_rejects_unsafe_entries(
    tmp_path: Path,
) -> None:
    from rquant.lab_worker import LabArtifactConflictError, LabArtifactReclaimer

    reports = LabReportSpool(tmp_path / "reports")
    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=reports,
    )
    current = _retry_claim(_claim(_nshape_compare_spec(hold_days=(1,))))
    worker = _worker(tmp_path, reports=reports)
    validated = worker._validate_closed_claim(current)
    result = RecordingRegistry().execute_shard(validated, object())
    worker._seal_result(current, result)
    current_path = worker.sealed_bundle_path(current)

    reclaimer.reclaim(current)
    assert current_path.is_dir()

    attempts_root = current_path.parent
    unknown = attempts_root / "unknown-attempt"
    unknown.mkdir()
    with pytest.raises(LabArtifactConflictError, match="invalid temporary attempt"):
        reclaimer.reclaim(current)
    assert unknown.is_dir()
    unknown.rmdir()

    future = _retry_claim(current)
    future_path = reclaimer.sealed_bundle_path(future)
    future_path.mkdir()
    with pytest.raises(LabArtifactConflictError, match="future sealed attempt"):
        reclaimer.reclaim(current)
    assert future_path.is_dir()
    future_path.rmdir()

    outside = tmp_path / "outside-sealed"
    outside.mkdir()
    (outside / "keep").write_text("safe", encoding="utf-8")
    obsolete = current.model_copy(
        update={
            "claim_generation": current.claim_generation - 1,
            "claim_token": uuid4(),
        }
    )
    obsolete_path = reclaimer.sealed_bundle_path(obsolete)
    obsolete_path.symlink_to(outside, target_is_directory=True)
    with pytest.raises(LabArtifactConflictError, match="symlink"):
        reclaimer.reclaim(current)
    assert (outside / "keep").read_text(encoding="utf-8") == "safe"


def test_unread_accepted_success_receipt_preserves_terminal_artifact(
    tmp_path: Path,
) -> None:
    from rquant.lab_job_protocol import LabCommandEnvelope, LabCommandSpool, SubmitJobCommand
    from rquant.lab_jobs import JobStatus, LabJobReader, LabJobStore
    from rquant.lab_scheduler import LabScheduler
    from rquant.lab_worker import LabArtifactReclaimer

    clock = [NOW]
    reports = LabReportSpool(tmp_path / "reports")
    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=reports,
    )
    claims = LabClaimSpool(
        tmp_path / "claims",
        claim_advance_hook=reclaimer.reclaim,
    )
    commands = LabCommandSpool(tmp_path / "commands")
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    job_id = uuid4()
    commands.publish(
        LabCommandEnvelope(
            request_id=uuid4(),
            command=SubmitJobCommand(
                job_id=job_id,
                spec=_nshape_compare_spec(hold_days=(1,)),
                max_attempts=2,
            ),
        )
    )
    scheduler = LabScheduler(
        store=store,
        spool=commands,
        owner_id="scheduler-a",
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=5,
        report_spool=reports,
        claim_spool=claims,
        claim_worker_ids=("worker-a",),
        shard_lease_seconds=20,
        adapter_registry=default_strategy_job_adapter_registry(),
        clock=lambda: clock[0],
    )
    scheduler.run_once()
    generation_one = claims.pending()[0].claim

    def delay_success_receipt(
        report: LabWorkerReport,
        timeout_seconds: float,
        stop: object,
    ) -> LabReportReceipt:
        if isinstance(report.body, LabShardSucceeded):
            raise TimeoutError("leave accepted receipt unread")
        return _accept_report(report, timeout_seconds, stop)

    worker = _worker(
        tmp_path,
        claims=claims,
        reports=reports,
        receipt_waiter=delay_success_receipt,
        clock=lambda: clock[0],
    )
    pending = worker.run_once()
    scheduler.run_once()
    job = LabJobReader(store.path).get_job(job_id)

    assert pending.status == "awaiting_receipt"
    assert job is not None and job.status is JobStatus.RUNNING
    assert job.result_state.value == "ready"
    assert reports.ack_dir.joinpath(f"{pending.report_id}.json").is_file()
    generation_two = _retry_claim(generation_one)
    claims.publish(generation_two)
    outcomes = claims.reconcile_current()

    assert claims.current(generation_one.job_id, generation_one.shard_id).claim == generation_two
    assert outcomes[0].status == "failed"
    assert "accepted success" in outcomes[0].error
    assert worker.sealed_bundle_path(generation_one).is_dir()
    scheduler.release()


def _sealed_obsolete_attempt(
    tmp_path: Path,
    reports: LabReportSpool,
) -> tuple[LabShardClaim, LabShardClaim, Path, object]:
    old_claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    current_claim = _retry_claim(old_claim)
    worker = _worker(
        tmp_path,
        claims=LabClaimSpool(tmp_path / "worker-claims"),
        reports=reports,
    )
    validated = worker._validate_closed_claim(old_claim)
    result = RecordingRegistry().execute_shard(validated, object())
    manifest = worker._seal_result(old_claim, result)
    return old_claim, current_claim, worker.sealed_bundle_path(old_claim), manifest


def test_pending_success_published_after_preflight_scan_is_stale_and_reclaimed(
    tmp_path: Path,
) -> None:
    from rquant.lab_worker import LabArtifactReclaimer

    reports = LabReportSpool(tmp_path / "reports")
    old_claim, current_claim, sealed, manifest = _sealed_obsolete_attempt(
        tmp_path,
        reports,
    )
    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=reports,
    )
    scanned = threading.Event()
    release = threading.Event()
    original_scan = reclaimer._assert_no_terminal_success_evidence

    def pause_after_scan(
        claim: LabShardClaim,
        candidate_manifest: object,
        durable_claim: LabShardClaim,
    ) -> None:
        original_scan(claim, candidate_manifest, durable_claim)
        scanned.set()
        assert release.wait(2)

    reclaimer._assert_no_terminal_success_evidence = pause_after_scan  # type: ignore[method-assign]
    errors: list[BaseException] = []

    def reclaim() -> None:
        try:
            reclaimer.reclaim(current_claim)
        except BaseException as exc:
            errors.append(exc)

    thread = threading.Thread(target=reclaim)
    thread.start()
    assert scanned.wait(2)
    success = LabWorkerReport.from_claim(
        old_claim,
        report_id=uuid4(),
        reported_at=NOW,
        body=LabShardSucceeded(result_manifest_hash=manifest.manifest_hash),
    )
    reports.publish(success)
    release.set()
    thread.join(timeout=_observe(2))

    assert not thread.is_alive()
    assert errors == []
    assert not sealed.exists()
    assert reports.pending()[0].report == success


def test_accepted_receipt_published_after_preflight_scan_preserves_attempt(
    tmp_path: Path,
) -> None:
    from rquant.lab_worker import LabArtifactConflictError, LabArtifactReclaimer

    reports = LabReportSpool(tmp_path / "reports")
    old_claim, current_claim, sealed, manifest = _sealed_obsolete_attempt(
        tmp_path,
        reports,
    )
    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=reports,
    )
    scanned = threading.Event()
    release = threading.Event()
    original_scan = reclaimer._assert_no_terminal_success_evidence

    def pause_after_scan(
        claim: LabShardClaim,
        candidate_manifest: object,
        durable_claim: LabShardClaim,
    ) -> None:
        original_scan(claim, candidate_manifest, durable_claim)
        scanned.set()
        assert release.wait(2)

    reclaimer._assert_no_terminal_success_evidence = pause_after_scan  # type: ignore[method-assign]
    errors: list[BaseException] = []
    thread = threading.Thread(
        target=lambda: _capture_reclaimer_error(reclaimer, current_claim, errors)
    )
    thread.start()
    assert scanned.wait(2)
    success = LabWorkerReport.from_claim(
        old_claim,
        report_id=uuid4(),
        reported_at=NOW,
        body=LabShardSucceeded(result_manifest_hash=manifest.manifest_hash),
    )
    entry = reports.publish(success)
    reports.ack(
        entry,
        LabReportReceipt.from_report(
            success,
            status="accepted",
            reason="accepted before claim advance",
            accepted_at=NOW,
        ),
    )
    release.set()
    thread.join(timeout=_observe(2))

    assert not thread.is_alive()
    assert len(errors) == 1
    assert isinstance(errors[0], LabArtifactConflictError)
    assert sealed.is_dir()


def _capture_reclaimer_error(
    reclaimer: object,
    claim: LabShardClaim,
    errors: list[BaseException],
) -> None:
    try:
        reclaimer.reclaim(claim)
    except BaseException as exc:
        errors.append(exc)


def test_claim_high_water_is_durable_before_reclaimer_hook_runs(tmp_path: Path) -> None:
    claims = LabClaimSpool(tmp_path / "claims")
    old_claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    current_claim = _retry_claim(old_claim)
    claims.publish(old_claim)
    observed: list[LabShardClaim] = []

    def observe_marker(claim: LabShardClaim) -> None:
        observed.append(claims.current(claim.job_id, claim.shard_id).claim)

    claims.set_claim_advance_hook(observe_marker)
    claims.publish(current_claim)

    assert observed == []
    outcomes = claims.reconcile_current()

    assert observed == [current_claim]
    assert outcomes[0].status == "reconciled"


def test_report_publish_uses_cross_process_evidence_lock(tmp_path: Path) -> None:
    reports = LabReportSpool(tmp_path / "reports")
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    report = LabWorkerReport.from_claim(
        claim,
        report_id=uuid4(),
        reported_at=NOW,
        body=LabShardSucceeded(result_manifest_hash="a" * 64),
    )
    source = (
        "from tests.unit.test_lab_worker import _publish_report_child; "
        "_publish_report_child(*__import__('sys').argv[1:])"
    )
    environment = os.environ.copy()
    repo_root = Path(__file__).parents[2]
    environment["PYTHONPATH"] = os.pathsep.join((str(repo_root / "src"), str(repo_root)))

    with reports.evidence_lock():
        process = subprocess.Popen(
            [sys.executable, "-c", source, str(tmp_path), report.model_dump_json()],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=environment,
        )
        time.sleep(0.1)
        assert process.poll() is None
        assert reports.pending_locked() == ()

    stdout, stderr = process.communicate(timeout=2)

    assert process.returncode == 0, (stdout, stderr)
    assert tuple(entry.report for entry in reports.pending()) == (report,)


def test_two_reclaimers_are_idempotent_for_same_obsolete_attempt(
    tmp_path: Path,
) -> None:
    from rquant.lab_worker import LabArtifactReclaimer

    reports = LabReportSpool(tmp_path / "reports")
    _old_claim, current_claim, sealed, _manifest = _sealed_obsolete_attempt(
        tmp_path,
        reports,
    )
    reclaimers = tuple(
        LabArtifactReclaimer(
            artifact_root=tmp_path / "artifacts",
            report_spool=LabReportSpool(tmp_path / "reports"),
        )
        for _ in range(2)
    )
    barrier = threading.Barrier(2)
    for reclaimer in reclaimers:
        original_scan = reclaimer._assert_no_terminal_success_evidence

        def synchronized_scan(
            claim: LabShardClaim,
            manifest: object,
            durable_claim: LabShardClaim,
            *,
            scan=original_scan,
        ) -> None:
            scan(claim, manifest, durable_claim)
            barrier.wait(timeout=_observe(2))

        reclaimer._assert_no_terminal_success_evidence = synchronized_scan  # type: ignore[method-assign]
    errors: list[BaseException] = []
    threads = tuple(
        threading.Thread(
            target=_capture_reclaimer_error,
            args=(reclaimer, current_claim, errors),
        )
        for reclaimer in reclaimers
    )
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=_observe(3))

    assert all(not thread.is_alive() for thread in threads)
    assert errors == []
    assert not sealed.exists()


def test_reclaimer_recovers_verified_tombstone_after_process_crash(
    tmp_path: Path,
) -> None:
    from rquant.lab_worker import LabArtifactReclaimer

    reports = LabReportSpool(tmp_path / "reports")
    _old_claim, current_claim, sealed, _manifest = _sealed_obsolete_attempt(
        tmp_path,
        reports,
    )
    completed = _run_worker_child(
        "_crash_reclaimer_after_tombstone_rename_child",
        tmp_path,
        current_claim.model_dump_json(),
    )

    assert completed.returncode == 79, completed.stderr
    assert not sealed.exists()
    tombstones = tuple(sealed.parent.glob(".reclaim-*"))
    assert len(tombstones) == 1

    restarted = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    restarted.reclaim(current_claim)

    assert tuple(sealed.parent.iterdir()) == ()


def test_reclaimer_recovers_prepared_ledger_after_crash_before_rename(
    tmp_path: Path,
) -> None:
    from rquant.lab_worker import LabArtifactReclaimer

    reports = LabReportSpool(tmp_path / "reports")
    _old_claim, current_claim, sealed, _manifest = _sealed_obsolete_attempt(
        tmp_path,
        reports,
    )
    completed = _run_worker_child(
        "_crash_reclaimer_after_prepared_ledger_child",
        tmp_path,
        current_claim.model_dump_json(),
    )

    assert completed.returncode == 81, completed.stderr
    assert sealed.is_dir()
    assert len(tuple((tmp_path / "artifacts" / ".reclaim-ledger").rglob("*.json"))) == 1

    restarted = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    restarted.reclaim(current_claim)

    assert not sealed.exists()
    ledgers = tuple((tmp_path / "artifacts" / ".reclaim-ledger").rglob("*.json"))
    assert len(ledgers) == 1
    assert '"state":"deferred_gc"' in ledgers[0].read_text(encoding="utf-8")
    assert restarted.quarantine_summary().bundle_count == 1


def test_newer_high_water_can_finish_verified_older_reclaim_ledger(
    tmp_path: Path,
) -> None:
    from rquant.lab_worker import LabArtifactReclaimer

    reports = LabReportSpool(tmp_path / "reports")
    _old_claim, generation_two, sealed, _manifest = _sealed_obsolete_attempt(
        tmp_path,
        reports,
    )
    completed = _run_worker_child(
        "_crash_reclaimer_after_tombstone_rename_child",
        tmp_path,
        generation_two.model_dump_json(),
    )
    assert completed.returncode == 79, completed.stderr
    generation_three = _retry_claim(generation_two)
    restarted = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )

    restarted.reclaim(generation_three)

    assert not sealed.exists()
    assert tuple(sealed.parent.glob(".reclaim-*")) == ()
    ledgers = tuple((tmp_path / "artifacts" / ".reclaim-ledger").rglob("*.json"))
    assert len(ledgers) == 1
    assert '"state":"deferred_gc"' in ledgers[0].read_text(encoding="utf-8")


def test_accepted_success_protects_tombstone_during_restart(tmp_path: Path) -> None:
    from rquant.lab_worker import LabArtifactConflictError, LabArtifactReclaimer

    reports = LabReportSpool(tmp_path / "reports")
    old_claim, current_claim, sealed, manifest = _sealed_obsolete_attempt(
        tmp_path,
        reports,
    )
    completed = _run_worker_child(
        "_crash_reclaimer_after_tombstone_rename_child",
        tmp_path,
        current_claim.model_dump_json(),
    )
    assert completed.returncode == 79, completed.stderr
    tombstone = tuple(sealed.parent.glob(".reclaim-*"))[0]
    success = LabWorkerReport.from_claim(
        old_claim,
        report_id=uuid4(),
        reported_at=NOW,
        body=LabShardSucceeded(result_manifest_hash=manifest.manifest_hash),
    )
    entry = reports.publish(success)
    reports.ack(
        entry,
        LabReportReceipt.from_report(
            success,
            status="accepted",
            reason="accepted before restart",
            accepted_at=NOW,
        ),
    )
    restarted = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )

    with pytest.raises(LabArtifactConflictError, match="accepted success"):
        restarted.reclaim(current_claim)

    assert tombstone.is_dir()


def test_source_and_same_identity_tombstone_are_preserved_as_conflict(
    tmp_path: Path,
) -> None:
    from rquant.lab_worker import LabArtifactConflictError, LabArtifactReclaimer

    reports = LabReportSpool(tmp_path / "reports")
    old_claim, current_claim, sealed, manifest = _sealed_obsolete_attempt(
        tmp_path,
        reports,
    )
    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=reports,
    )
    tombstone = sealed.parent / reclaimer._tombstone_name(old_claim, manifest)
    shutil.copytree(sealed, tombstone)

    with pytest.raises(LabArtifactConflictError, match="source and tombstone"):
        reclaimer.reclaim(current_claim)

    assert sealed.is_dir()
    assert tombstone.is_dir()


def test_unknown_reclaim_tombstone_remains_fail_closed(tmp_path: Path) -> None:
    from rquant.lab_worker import LabArtifactConflictError, LabArtifactReclaimer

    reports = LabReportSpool(tmp_path / "reports")
    _old_claim, current_claim, sealed, _manifest = _sealed_obsolete_attempt(
        tmp_path,
        reports,
    )
    unknown = sealed.parent / ".reclaim-v1-unknown"
    unknown.mkdir()
    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=reports,
    )

    with pytest.raises(LabArtifactConflictError):
        reclaimer.reclaim(current_claim)

    assert unknown.is_dir()
    assert sealed.is_dir()


@pytest.mark.parametrize("file_name", ["manifest.json", "artifact"])
def test_reclaimer_rejects_hardlinked_bundle_file_without_deleting(
    tmp_path: Path,
    file_name: str,
) -> None:
    from rquant.lab_worker import LabArtifactConflictError, LabArtifactReclaimer

    reports = LabReportSpool(tmp_path / "reports")
    _old_claim, current_claim, sealed, _manifest = _sealed_obsolete_attempt(
        tmp_path,
        reports,
    )
    target = (
        sealed / "manifest.json" if file_name == "manifest.json" else next(sealed.glob("*.parquet"))
    )
    external = tmp_path / f"external-{target.name}"
    os.link(target, external)
    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=reports,
    )

    with pytest.raises(LabArtifactConflictError, match="hard link"):
        reclaimer.reclaim(current_claim)

    assert sealed.is_dir()
    assert target.is_file()
    assert external.is_file()
    assert target.stat().st_nlink == 2


def test_seal_rejects_hardlink_created_at_atomic_rename_without_deleting(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.lab_worker as lab_worker_module
    from rquant.lab_worker import LabArtifactConflictError

    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    worker = _worker(tmp_path)
    validated = worker._validate_closed_claim(claim)
    result = RecordingRegistry().execute_shard(validated, object())
    sealed = worker.sealed_bundle_path(claim)
    external = tmp_path / "external-rename-artifact.parquet"
    original_rename = lab_worker_module.os.rename

    def hardlink_at_rename(source: Path, target: Path) -> None:
        original_rename(source, target)
        if Path(target) == sealed:
            os.link(next(sealed.glob("*.parquet")), external)

    monkeypatch.setattr(lab_worker_module.os, "rename", hardlink_at_rename)

    with pytest.raises(LabArtifactConflictError, match="hard link"):
        worker._seal_result(claim, result)

    assert sealed.is_dir()
    assert external.is_file()


def test_reclaimer_rejects_hardlinked_ledger_without_deleting_tombstone(
    tmp_path: Path,
) -> None:
    from rquant.lab_worker import LabArtifactConflictError, LabArtifactReclaimer

    reports = LabReportSpool(tmp_path / "reports")
    _old_claim, current_claim, sealed, _manifest = _sealed_obsolete_attempt(
        tmp_path,
        reports,
    )
    completed = _run_worker_child(
        "_crash_reclaimer_after_tombstone_rename_child",
        tmp_path,
        current_claim.model_dump_json(),
    )
    assert completed.returncode == 79, completed.stderr
    tombstone = tuple(sealed.parent.glob(".reclaim-*"))[0]
    ledger = tuple((tmp_path / "artifacts" / ".reclaim-ledger").rglob("*.json"))[0]
    external = tmp_path / "external-ledger.json"
    os.link(ledger, external)
    restarted = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )

    with pytest.raises(LabArtifactConflictError, match="hard link"):
        restarted.reclaim(current_claim)

    assert tombstone.is_dir()
    assert ledger.is_file()
    assert external.is_file()


def test_inventory_replacement_after_validation_is_restored_and_not_deleted(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.lab_worker import LabArtifactConflictError, LabArtifactReclaimer

    reports = LabReportSpool(tmp_path / "reports")
    _old_claim, current_claim, sealed, _manifest = _sealed_obsolete_attempt(
        tmp_path,
        reports,
    )
    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=reports,
    )
    original_inventory = reclaimer._inventory_entry
    validated_in_tombstone = 0
    saved_original = tmp_path / "saved-original-manifest.json"

    def replace_after_validation(path: Path, *, relative_path: str):
        nonlocal validated_in_tombstone
        observed = original_inventory(path, relative_path=relative_path)
        if path.parent.name.startswith(".reclaim-v1-") and relative_path == "manifest.json":
            validated_in_tombstone += 1
            if validated_in_tombstone == 2:
                os.replace(path, saved_original)
                path.write_bytes(b"replacement-must-survive")
        return observed

    monkeypatch.setattr(reclaimer, "_inventory_entry", replace_after_validation)

    with pytest.raises(LabArtifactConflictError, match="owner inventory"):
        reclaimer.reclaim(current_claim)

    tombstone = tuple(sealed.parent.glob(".reclaim-v1-*"))[0]
    assert (tombstone / "manifest.json").read_bytes() == b"replacement-must-survive"
    assert saved_original.is_file()


def test_ledger_replacement_after_validation_is_restored_and_not_deleted(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.lab_worker import LabArtifactConflictError, LabArtifactReclaimer

    reports = LabReportSpool(tmp_path / "reports")
    _old_claim, current_claim, sealed, _manifest = _sealed_obsolete_attempt(
        tmp_path,
        reports,
    )
    completed = _run_worker_child(
        "_crash_reclaimer_after_tombstone_rename_child",
        tmp_path,
        current_claim.model_dump_json(),
    )
    assert completed.returncode == 79, completed.stderr
    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=reports,
    )
    ledger = tuple((tmp_path / "artifacts" / ".reclaim-ledger").rglob("*.json"))[0]
    saved_original = tmp_path / "saved-original-ledger.json"
    original_load = reclaimer._load_ledger
    replaced = False

    def replace_after_load(path: Path):
        nonlocal replaced
        loaded = original_load(path)
        if path == ledger and not replaced:
            replaced = True
            os.replace(path, saved_original)
            path.write_bytes(b"replacement-ledger-must-survive")
        return loaded

    monkeypatch.setattr(reclaimer, "_load_ledger", replace_after_load)

    with pytest.raises(LabArtifactConflictError, match="changed before deletion"):
        reclaimer._remove_ledger(ledger)

    assert ledger.read_bytes() == b"replacement-ledger-must-survive"
    assert saved_original.is_file()
    assert tuple(sealed.parent.glob(".reclaim-v1-*"))


def test_reclaimer_retains_complete_inventory_in_deferred_quarantine(tmp_path: Path) -> None:
    from rquant.lab_worker import LabArtifactReclaimer

    reports = LabReportSpool(tmp_path / "reports")
    _old_claim, current_claim, sealed, _manifest = _sealed_obsolete_attempt(
        tmp_path,
        reports,
    )
    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=reports,
    )
    reclaimer.reclaim(current_claim)

    entry = reclaimer.quarantine_entries()[0]
    payload = entry.bundle_path / "payload"
    assert not sealed.exists()
    assert tuple(sealed.parent.glob(".reclaim-*")) == ()
    assert (payload / "manifest.json").is_file()
    assert len(tuple(payload.glob("*.parquet"))) == 1
    assert entry.state == "deferred_gc"
    assert entry.retained_bytes > 0


@pytest.mark.parametrize("mutation", ["unknown", "replace"])
def test_deferred_reclaim_rejects_unknown_or_replaced_file(
    tmp_path: Path,
    mutation: str,
) -> None:
    from rquant.lab_worker import LabArtifactConflictError, LabArtifactReclaimer

    reports = LabReportSpool(tmp_path / "reports")
    _old_claim, current_claim, sealed, _manifest = _sealed_obsolete_attempt(
        tmp_path,
        reports,
    )
    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=reports,
    )
    reclaimer.reclaim(current_claim)
    deferred = reclaimer.quarantine_entries()[0].bundle_path / "payload"
    if mutation == "unknown":
        (deferred / "intruder").write_text("unexpected", encoding="utf-8")
    else:
        remaining = deferred / "manifest.json"
        replacement = deferred / ".replacement"
        replacement.write_bytes(remaining.read_bytes())
        os.replace(replacement, remaining)
    restarted = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )

    with pytest.raises(LabArtifactConflictError):
        restarted.reclaim(current_claim)

    assert deferred.is_dir()


def test_accepted_success_protects_deferred_quarantine_bytes(tmp_path: Path) -> None:
    from rquant.lab_worker import LabArtifactConflictError, LabArtifactReclaimer

    reports = LabReportSpool(tmp_path / "reports")
    old_claim, current_claim, sealed, manifest = _sealed_obsolete_attempt(
        tmp_path,
        reports,
    )
    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=reports,
    )
    reclaimer.reclaim(current_claim)
    deferred = reclaimer.quarantine_entries()[0].bundle_path
    success = LabWorkerReport.from_claim(
        old_claim,
        report_id=uuid4(),
        reported_at=NOW,
        body=LabShardSucceeded(result_manifest_hash=manifest.manifest_hash),
    )
    entry = reports.publish(success)
    reports.ack(
        entry,
        LabReportReceipt.from_report(
            success,
            status="accepted",
            reason="accepted before partial restart",
            accepted_at=NOW,
        ),
    )

    with pytest.raises(LabArtifactConflictError, match="accepted success"):
        LabArtifactReclaimer(
            artifact_root=tmp_path / "artifacts",
            report_spool=LabReportSpool(tmp_path / "reports"),
        ).reclaim(current_claim)

    assert deferred.is_dir()
    assert (deferred / "payload" / "manifest.json").is_file()


def test_reclaimer_cleans_only_recognized_single_link_ledger_temporaries(
    tmp_path: Path,
) -> None:
    from rquant.lab_worker import LabArtifactReclaimer

    reports = LabReportSpool(tmp_path / "reports")
    current = _retry_claim(_claim(_nshape_compare_spec(hold_days=(1,))))
    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=reports,
    )
    ledger_dir = reclaimer._ledger_dir(current)
    ledger_dir.mkdir(parents=True)
    temporary = ledger_dir / f".reclaim-ledger-tmp-v1-{uuid4().hex}.tmp"
    temporary.write_bytes(b"x" * 32)

    reclaimer.reclaim(current)
    reclaimer.reclaim(current)

    assert not temporary.exists()


@pytest.mark.parametrize("kind", ["unknown", "symlink", "hardlink"])
def test_reclaimer_rejects_unsafe_ledger_temporary(
    tmp_path: Path,
    kind: str,
) -> None:
    from rquant.lab_worker import LabArtifactConflictError, LabArtifactReclaimer

    reports = LabReportSpool(tmp_path / "reports")
    current = _retry_claim(_claim(_nshape_compare_spec(hold_days=(1,))))
    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=reports,
    )
    ledger_dir = reclaimer._ledger_dir(current)
    ledger_dir.mkdir(parents=True)
    external = tmp_path / "external-ledger-temp"
    external.write_bytes(b"preserve")
    if kind == "unknown":
        temporary = ledger_dir / ".unknown.tmp"
        temporary.write_bytes(b"unknown")
    else:
        temporary = ledger_dir / f".reclaim-ledger-tmp-v1-{uuid4().hex}.tmp"
        if kind == "symlink":
            temporary.symlink_to(external)
        else:
            os.link(external, temporary)

    with pytest.raises(LabArtifactConflictError):
        reclaimer.reclaim(current)

    assert os.path.lexists(temporary)
    assert external.read_bytes() == b"preserve"


def test_ledger_temporary_replacement_after_identity_check_survives(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.lab_worker import LabArtifactConflictError, LabArtifactReclaimer

    current = _retry_claim(_claim(_nshape_compare_spec(hold_days=(1,))))
    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    ledger_dir = reclaimer._ledger_dir(current)
    ledger_dir.mkdir(parents=True)
    temporary = ledger_dir / f".reclaim-ledger-tmp-v1-{uuid4().hex}.tmp"
    temporary.write_bytes(b"original")
    saved_original = tmp_path / "saved-ledger-temporary"
    original_identity = reclaimer._regular_file_identity
    replaced = False

    def replace_after_identity(path: Path, *, label: str):
        nonlocal replaced
        identity = original_identity(path, label=label)
        if path == temporary and not replaced:
            replaced = True
            os.replace(path, saved_original)
            path.write_bytes(b"replacement-temporary-must-survive")
        return identity

    monkeypatch.setattr(reclaimer, "_regular_file_identity", replace_after_identity)

    with pytest.raises(LabArtifactConflictError, match="changed before deletion"):
        reclaimer._cleanup_ledger_temporaries(ledger_dir)

    assert temporary.read_bytes() == b"replacement-temporary-must-survive"
    assert saved_original.is_file()


def test_logical_quarantine_retains_bytes_and_reports_deferred_gc(
    tmp_path: Path,
) -> None:
    from rquant.lab_worker import LabArtifactReclaimer

    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    victim = tmp_path / "artifacts" / "logical-delete" / "victim.bin"
    victim.parent.mkdir(parents=True)
    victim.write_bytes(b"retained-until-p7")
    expected = reclaimer._regular_file_identity(victim, label="deferred gc fixture")

    assert reclaimer._safe_remove_regular_child(
        victim,
        expected=expected,
        label="deferred gc fixture",
    )
    reclaimer.collect_garbage()

    entries = reclaimer.quarantine_entries()
    summary = reclaimer.quarantine_summary()
    assert not victim.exists()
    assert len(entries) == 1
    assert entries[0].state == "deferred_gc"
    assert (entries[0].bundle_path / "payload").read_bytes() == b"retained-until-p7"
    assert summary.bundle_count == 1
    assert summary.retained_bytes == len(b"retained-until-p7")


def test_quarantine_summary_uses_verified_ledgers_without_rehashing_payload(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.lab_worker import LabArtifactReclaimer

    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    victim = tmp_path / "artifacts" / "summary" / "retained.bin"
    victim.parent.mkdir(parents=True)
    victim.write_bytes(b"ledger-sized-payload")
    assert reclaimer.logical_quarantine_tree(victim, purpose="summary fixture")

    def forbid_payload_hash(_path: Path) -> tuple[object, ...]:
        raise AssertionError("summary must not rehash immutable deferred payload")

    monkeypatch.setattr(reclaimer, "_garbage_inventory", forbid_payload_hash)

    first = reclaimer.quarantine_summary()
    second = reclaimer.quarantine_summary()

    assert first == second
    assert first.bundle_count == 1
    assert first.retained_bytes == len(b"ledger-sized-payload")


def test_p13_reclaim_critical_paths_have_no_physical_delete_calls() -> None:
    from rquant.lab_worker import LabArtifactReclaimer, LabWorker

    source = "\n".join(
        inspect.getsource(method)
        for method in (
            LabWorker._cleanup_temporary,
            LabWorker._rollback_sealed,
            LabArtifactReclaimer._cleanup_ledger_temporaries,
            LabArtifactReclaimer._delete_isolated_tombstone,
            LabArtifactReclaimer._safe_remove_regular_child,
        )
    )

    assert ".unlink(" not in source
    assert ".rmdir(" not in source
    assert "rmtree(" not in source


def test_owner_only_staging_resumes_source_isolation_after_restart(tmp_path: Path) -> None:
    from rquant.lab_worker import LabArtifactReclaimer

    reports = LabReportSpool(tmp_path / "reports")
    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=reports,
    )
    victim = tmp_path / "artifacts" / "owner-only" / "victim.bin"
    victim.parent.mkdir(parents=True)
    victim.write_bytes(b"owner-before-payload")
    owner = reclaimer._garbage_owner(victim, purpose="owner-only crash fixture")
    staging = reclaimer.garbage_staging_dir / owner.garbage_id.hex
    staging.mkdir(mode=0o700)
    reclaimer._write_garbage_owner(staging, owner)

    restarted = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    restarted.collect_garbage()

    entry = restarted.quarantine_entries()[0]
    assert not victim.exists()
    assert entry.state == "deferred_gc"
    assert (entry.bundle_path / "payload").read_bytes() == b"owner-before-payload"


@pytest.mark.parametrize(
    "derived_state",
    ["missing", "empty_staging", "global_owner_only", "both_owners_without_ledger"],
)
def test_prepared_intent_rebuilds_incomplete_derived_state(
    tmp_path: Path,
    derived_state: str,
) -> None:
    from rquant.lab_worker import LabArtifactReclaimer

    reports = LabReportSpool(tmp_path / "reports")
    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=reports,
    )
    victim = tmp_path / "artifacts" / "prepared-intent" / "victim.bin"
    victim.parent.mkdir(parents=True)
    victim.write_bytes(f"intent-{derived_state}".encode())
    owner = reclaimer._garbage_owner(victim, purpose="prepared intent recovery fixture")
    intent = reclaimer._prepared_intent(owner)
    intent_path = reclaimer._write_prepared_intent(intent)
    staging = reclaimer.garbage_staging_dir / owner.garbage_id.hex
    global_owner = reclaimer.garbage_owner_dir / f"{owner.garbage_id.hex}.json"
    if derived_state in {"empty_staging", "both_owners_without_ledger"}:
        staging.mkdir(mode=0o700)
    if derived_state in {"global_owner_only", "both_owners_without_ledger"}:
        global_owner.write_text(owner.canonical_json(), encoding="utf-8")
    if derived_state == "both_owners_without_ledger":
        (staging / "owner.json").write_text(owner.canonical_json(), encoding="utf-8")

    restarted = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    restarted.collect_garbage()

    entry = restarted.quarantine_entries()[0]
    assert intent_path.is_file()
    assert not victim.exists()
    assert entry.state == "deferred_gc"
    assert (entry.bundle_path / "payload").read_bytes() == f"intent-{derived_state}".encode()
    assert tuple(restarted.garbage_staging_dir.iterdir()) == ()
    assert len(tuple(restarted.garbage_ledger_dir.glob(f"{owner.garbage_id.hex}-*.json"))) == 3


def test_prepared_intent_publish_is_no_clobber_and_idempotent(tmp_path: Path) -> None:
    from rquant.lab_worker import LabArtifactConflictError, LabArtifactReclaimer

    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    victim = tmp_path / "artifacts" / "intent-publish" / "victim.bin"
    victim.parent.mkdir(parents=True)
    victim.write_bytes(b"authoritative-intent")
    owner = reclaimer._garbage_owner(victim, purpose="intent no-clobber fixture")
    intent = reclaimer._prepared_intent(owner)

    first = reclaimer._write_prepared_intent(intent)
    second = reclaimer._write_prepared_intent(intent)
    assert first == second
    assert first.read_text(encoding="utf-8") == intent.canonical_json()
    assert intent.state == "prepared"
    assert intent.source_relative_path == owner.original_relative_path
    assert intent.staging_relative_path == f".garbage-v1/staging/{owner.garbage_id.hex}"
    assert intent.owner.inventory == owner.inventory
    assert len(intent.intent_hash) == 64

    replacement = first.with_suffix(".replacement")
    replacement.write_text("foreign-intent", encoding="utf-8")
    os.replace(replacement, first)
    with pytest.raises(LabArtifactConflictError):
        reclaimer._write_prepared_intent(intent)
    assert first.read_text(encoding="utf-8") == "foreign-intent"


def test_partial_prepared_intent_temporary_is_isolated_and_does_not_block(
    tmp_path: Path,
) -> None:
    from rquant.lab_worker import LabArtifactReclaimer

    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    victim = tmp_path / "artifacts" / "partial-intent" / "victim.bin"
    victim.parent.mkdir(parents=True)
    victim.write_bytes(b"source-survives-partial-intent")
    owner = reclaimer._garbage_owner(victim, purpose="partial intent fixture")
    temporary = reclaimer.garbage_intent_temp_dir / (
        f".prepared-intent-tmp-v1-{owner.garbage_id.hex}-{uuid4().hex}.tmp"
    )
    temporary.write_bytes(b"{" + b"x" * 31)

    reclaimer.collect_garbage()
    assert not temporary.exists()
    assert len(tuple(reclaimer.garbage_intent_orphan_dir.iterdir())) == 1
    assert victim.is_file()

    assert reclaimer.logical_quarantine_tree(victim, purpose="partial intent fixture")
    assert reclaimer.quarantine_summary().bundle_count == 1


@pytest.mark.parametrize("publish_boundary", ["before_link", "after_link"])
def test_complete_prepared_intent_temporary_recovers_atomic_publish_boundary(
    tmp_path: Path,
    publish_boundary: str,
) -> None:
    from rquant.lab_worker import LabArtifactReclaimer

    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    victim = tmp_path / "artifacts" / "intent-link-crash" / "victim.bin"
    victim.parent.mkdir(parents=True)
    victim.write_bytes(f"intent-{publish_boundary}".encode())
    owner = reclaimer._garbage_owner(victim, purpose="intent link crash fixture")
    intent = reclaimer._prepared_intent(owner)
    temporary = reclaimer.garbage_intent_temp_dir / (
        f".prepared-intent-tmp-v1-{owner.garbage_id.hex}-{uuid4().hex}.tmp"
    )
    temporary.write_text(intent.canonical_json(), encoding="utf-8")
    target = reclaimer._prepared_intent_path(owner.garbage_id)
    if publish_boundary == "after_link":
        os.link(temporary, target)
        assert temporary.lstat().st_nlink == 2

    restarted = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    restarted.collect_garbage()

    assert not temporary.exists()
    assert target.lstat().st_nlink == 1
    assert restarted._load_prepared_intent(target) == intent
    assert restarted.quarantine_summary().bundle_count == 1


@pytest.mark.parametrize(
    "mutation",
    ["different_global_owner", "bundle_owner_symlink", "global_owner_hardlink", "staging_extra"],
)
def test_prepared_intent_rejects_conflicting_derived_state(
    tmp_path: Path,
    mutation: str,
) -> None:
    from rquant.lab_worker import LabArtifactConflictError, LabArtifactReclaimer

    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    victim = tmp_path / "artifacts" / "derived-conflict" / "victim.bin"
    victim.parent.mkdir(parents=True)
    victim.write_bytes(b"derived-state-source")
    owner = reclaimer._garbage_owner(victim, purpose="derived conflict fixture")
    intent = reclaimer._prepared_intent(owner)
    reclaimer._write_prepared_intent(intent)
    staging = reclaimer.garbage_staging_dir / owner.garbage_id.hex
    staging.mkdir(mode=0o700)
    global_owner = reclaimer.garbage_owner_dir / f"{owner.garbage_id.hex}.json"
    bundle_owner = staging / "owner.json"
    if mutation == "different_global_owner":
        global_owner.write_text("different", encoding="utf-8")
    elif mutation == "bundle_owner_symlink":
        external = tmp_path / "external-owner.json"
        external.write_text(owner.canonical_json(), encoding="utf-8")
        bundle_owner.symlink_to(external)
    elif mutation == "global_owner_hardlink":
        global_owner.write_text(owner.canonical_json(), encoding="utf-8")
        os.link(global_owner, tmp_path / "external-owner-hardlink.json")
    else:
        (staging / "unexpected.bin").write_bytes(b"foreign")

    with pytest.raises(LabArtifactConflictError):
        reclaimer.collect_garbage()

    assert victim.read_bytes() == b"derived-state-source"


@pytest.mark.parametrize("legacy_state", ["global_owner", "both_owners"])
def test_legacy_partial_staging_reconstructs_unique_prepared_intent(
    tmp_path: Path,
    legacy_state: str,
) -> None:
    from rquant.lab_worker import LabArtifactReclaimer

    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    victim = tmp_path / "artifacts" / "legacy-partial.bin"
    victim.write_bytes(f"legacy-{legacy_state}".encode())
    owner = reclaimer._garbage_owner(victim, purpose="legacy prepared fixture")
    global_owner = reclaimer.garbage_owner_dir / f"{owner.garbage_id.hex}.json"
    global_owner.write_text(owner.canonical_json(), encoding="utf-8")
    if legacy_state == "both_owners":
        staging = reclaimer.garbage_staging_dir / owner.garbage_id.hex
        staging.mkdir(mode=0o700)
        (staging / "owner.json").write_text(owner.canonical_json(), encoding="utf-8")

    restarted = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    restarted.collect_garbage()

    entries = restarted.quarantine_entries()
    assert len(entries) == 1
    assert entries[0].state == "deferred_gc"
    assert not victim.exists()
    assert len(tuple(restarted.garbage_intent_dir.glob("*.json"))) == 1


@pytest.mark.parametrize("business_shape", ["unique_file", "multiple_files", "directory"])
def test_recovers_legacy_empty_staging_without_intent(
    tmp_path: Path,
    business_shape: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.lab_worker as lab_worker_module
    from rquant.lab_worker import LabArtifactReclaimer

    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    live_result = tmp_path / "artifacts" / "live-result.bin"
    live_result.write_bytes(b"live-result-must-not-move")
    business_files = [live_result]
    if business_shape == "multiple_files":
        second = tmp_path / "artifacts" / "second-result.bin"
        second.write_bytes(b"second-result-must-not-move")
        business_files.append(second)
    elif business_shape == "directory":
        nested = tmp_path / "artifacts" / "business-run" / "nested" / "result.bin"
        nested.parent.mkdir(parents=True)
        nested.write_bytes(b"nested-result-must-not-move")
        business_files.append(nested)
    identities = {
        path: (path.lstat().st_dev, path.lstat().st_ino, path.read_bytes())
        for path in business_files
    }
    legacy_id = uuid4().hex
    staging = reclaimer.garbage_staging_dir / legacy_id
    staging.mkdir(mode=0o700)

    def reject_business_tree_scan(*args, **kwargs):
        raise AssertionError("legacy empty staging must not scan the artifact business tree")

    monkeypatch.setattr(lab_worker_module.os, "walk", reject_business_tree_scan)

    reclaimer.collect_garbage()

    assert not staging.exists()
    for path, identity in identities.items():
        assert path.is_file()
        assert (path.lstat().st_dev, path.lstat().st_ino, path.read_bytes()) == identity
    assert tuple(reclaimer.garbage_intent_dir.iterdir()) == ()
    assert tuple(reclaimer.garbage_deferred_dir.iterdir()) == ()
    orphans = tuple(reclaimer.garbage_intent_orphan_dir.glob(f"legacy-empty-staging-{legacy_id}-*"))
    assert len(orphans) == 1
    orphan = orphans[0]
    assert tuple(orphan.iterdir()) == ()
    ledgers = tuple(reclaimer.garbage_orphan_metadata_dir.glob(f"{orphan.name}.json"))
    assert len(ledgers) == 1
    metadata = json.loads(ledgers[0].read_text(encoding="utf-8"))
    assert metadata["reason"] == "no_proven_source"
    assert metadata["original_staging_relative_path"] == f".garbage-v1/staging/{legacy_id}"
    assert not hasattr(reclaimer, "_legacy_unique_active_source")


def test_legacy_empty_staging_rename_replacement_restores_business_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.lab_worker as lab_worker_module
    from rquant.lab_worker import LabArtifactConflictError, LabArtifactReclaimer

    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    legacy_id = uuid4().hex
    staging = reclaimer.garbage_staging_dir / legacy_id
    staging.mkdir(mode=0o700)
    expected_empty = (staging.lstat().st_dev, staging.lstat().st_ino)
    preserved_empty = tmp_path / "preserved-empty-staging"
    original_rename = lab_worker_module.os.rename
    replacement_identity: tuple[int, int] | None = None

    def replace_at_orphan_rename(source: Path, target: Path) -> None:
        nonlocal replacement_identity
        if Path(source) == staging and Path(target).parent == reclaimer.garbage_intent_orphan_dir:
            original_rename(source, preserved_empty)
            staging.mkdir(mode=0o700)
            (staging / "result.bin").write_bytes(b"business-result-must-survive")
            observed = staging.lstat()
            replacement_identity = (observed.st_dev, observed.st_ino)
        original_rename(source, target)

    monkeypatch.setattr(lab_worker_module.os, "rename", replace_at_orphan_rename)

    with pytest.raises(LabArtifactConflictError, match="changed during orphan isolation"):
        reclaimer.collect_garbage()

    assert replacement_identity is not None
    assert (staging.lstat().st_dev, staging.lstat().st_ino) == replacement_identity
    assert (staging / "result.bin").read_bytes() == b"business-result-must-survive"
    assert (preserved_empty.lstat().st_dev, preserved_empty.lstat().st_ino) == expected_empty
    assert tuple(reclaimer.garbage_intent_orphan_dir.rglob("orphan.json")) == ()
    assert tuple(reclaimer.garbage_orphan_metadata_dir.iterdir()) == ()


def test_legacy_empty_staging_normal_orphan_binds_original_inode(tmp_path: Path) -> None:
    from rquant.lab_worker import LabArtifactReclaimer

    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    legacy_id = uuid4().hex
    staging = reclaimer.garbage_staging_dir / legacy_id
    staging.mkdir(mode=0o700)
    before = staging.lstat()

    reclaimer.collect_garbage()

    orphans = tuple(reclaimer.garbage_intent_orphan_dir.glob(f"legacy-empty-staging-{legacy_id}-*"))
    assert len(orphans) == 1
    orphan = orphans[0]
    after = orphan.lstat()
    assert (after.st_dev, after.st_ino, after.st_nlink) == (
        before.st_dev,
        before.st_ino,
        before.st_nlink,
    )
    assert tuple(orphan.iterdir()) == ()
    ledger = reclaimer.garbage_orphan_metadata_dir / f"{orphan.name}.json"
    metadata = json.loads(ledger.read_text(encoding="utf-8"))
    assert metadata["reason"] == "no_proven_source"
    assert metadata["orphan_relative_path"].endswith(orphan.name)
    assert metadata["expected_device"] == before.st_dev
    assert metadata["expected_inode"] == before.st_ino
    assert metadata["expected_file_type"] == "directory"
    assert metadata["expected_nlink"] == before.st_nlink
    assert metadata["expected_empty"] is True


def test_external_orphan_identity_rejects_empty_replacement_after_entry_lstat(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.lab_worker import LabArtifactConflictError, LabArtifactReclaimer

    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    legacy_id = uuid4().hex
    staging = reclaimer.garbage_staging_dir / legacy_id
    staging.mkdir(mode=0o700)
    reclaimer.collect_garbage()
    orphan = next(reclaimer.garbage_intent_orphan_dir.glob(f"legacy-empty-staging-{legacy_id}-*"))
    ledger = reclaimer.garbage_orphan_metadata_dir / f"{orphan.name}.json"
    metadata = reclaimer._load_external_orphan_metadata(ledger)
    original_orphan_identity = (orphan.lstat().st_dev, orphan.lstat().st_ino)
    original_ledger = (ledger.lstat().st_dev, ledger.lstat().st_ino, ledger.read_bytes())
    preserved_orphan = tmp_path / "preserved-entry-orphan"
    original_lstat = Path.lstat
    replaced = False
    replacement_identity: tuple[int, int] | None = None

    def replace_after_entry_lstat(path: Path) -> os.stat_result:
        nonlocal replaced, replacement_identity
        observed = original_lstat(path)
        if path == orphan and not replaced:
            replaced = True
            os.rename(orphan, preserved_orphan)
            orphan.mkdir(mode=0o700)
            replacement = original_lstat(orphan)
            replacement_identity = (replacement.st_dev, replacement.st_ino)
        return observed

    monkeypatch.setattr(Path, "lstat", replace_after_entry_lstat)

    with pytest.raises(LabArtifactConflictError, match="orphan identity conflicts"):
        reclaimer._assert_external_orphan_identity(orphan, metadata)

    assert replaced
    assert replacement_identity is not None
    assert (original_lstat(preserved_orphan).st_dev, original_lstat(preserved_orphan).st_ino) == (
        original_orphan_identity
    )
    assert (original_lstat(orphan).st_dev, original_lstat(orphan).st_ino) == replacement_identity
    assert tuple(preserved_orphan.iterdir()) == ()
    assert tuple(orphan.iterdir()) == ()
    assert (original_lstat(ledger).st_dev, original_lstat(ledger).st_ino, ledger.read_bytes()) == (
        original_ledger
    )


@pytest.mark.parametrize(
    ("a_payload", "b_payload", "expects_conflict"),
    [
        (None, None, False),
        (None, b"b-must-not-affect-a", False),
        (b"a-must-be-observed", None, True),
    ],
)
def test_external_orphan_identity_fd_enumeration_resists_path_aba(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    a_payload: bytes | None,
    b_payload: bytes | None,
    expects_conflict: bool,
) -> None:
    import rquant.lab_worker as lab_worker_module
    from rquant.lab_worker import LabArtifactConflictError, LabArtifactReclaimer

    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    legacy_id = uuid4().hex
    staging = reclaimer.garbage_staging_dir / legacy_id
    staging.mkdir(mode=0o700)
    reclaimer.collect_garbage()
    orphan = next(reclaimer.garbage_intent_orphan_dir.glob(f"legacy-empty-staging-{legacy_id}-*"))
    ledger = reclaimer.garbage_orphan_metadata_dir / f"{orphan.name}.json"
    metadata = reclaimer._load_external_orphan_metadata(ledger)
    if a_payload is not None:
        (orphan / "a.bin").write_bytes(a_payload)
        observed_a = orphan.lstat()
        metadata = reclaimer._legacy_empty_staging_orphan_metadata(
            metadata.staging_id,
            metadata.orphan_token,
            reclaimer._directory_identity(observed_a),
        )
        ledger.write_text(metadata.canonical_json(), encoding="utf-8")
    replacement = tmp_path / "aba-replacement"
    replacement.mkdir(mode=0o700)
    if b_payload is not None:
        (replacement / "b.bin").write_bytes(b_payload)
    parked_a = tmp_path / "aba-parked-a"
    original_listdir = lab_worker_module.os.listdir
    original_a = (orphan.lstat().st_dev, orphan.lstat().st_ino)
    original_b = (replacement.lstat().st_dev, replacement.lstat().st_ino)
    original_ledger = (ledger.lstat().st_dev, ledger.lstat().st_ino, ledger.read_bytes())
    swapped = False

    def swap_around_fd_enumeration(path: int | str | bytes | os.PathLike[str]) -> list[str]:
        nonlocal swapped
        if isinstance(path, int) and not swapped:
            swapped = True
            os.rename(orphan, parked_a)
            os.rename(replacement, orphan)
            try:
                return original_listdir(path)
            finally:
                os.rename(orphan, replacement)
                os.rename(parked_a, orphan)
        return original_listdir(path)

    monkeypatch.setattr(lab_worker_module.os, "listdir", swap_around_fd_enumeration)

    if expects_conflict:
        with pytest.raises(LabArtifactConflictError, match="orphan identity conflicts"):
            reclaimer._assert_external_orphan_identity(orphan, metadata)
    else:
        reclaimer._assert_external_orphan_identity(orphan, metadata)

    assert swapped
    assert (orphan.lstat().st_dev, orphan.lstat().st_ino) == original_a
    assert (replacement.lstat().st_dev, replacement.lstat().st_ino) == original_b
    assert ({child.name: child.read_bytes() for child in orphan.iterdir()}) == (
        {"a.bin": a_payload} if a_payload is not None else {}
    )
    assert ({child.name: child.read_bytes() for child in replacement.iterdir()}) == (
        {"b.bin": b_payload} if b_payload is not None else {}
    )
    assert (ledger.lstat().st_dev, ledger.lstat().st_ino, ledger.read_bytes()) == original_ledger


def test_external_orphan_metadata_entry_replacement_never_writes_business_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.lab_worker import LabArtifactConflictError, LabArtifactReclaimer

    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    legacy_id = uuid4().hex
    staging = reclaimer.garbage_staging_dir / legacy_id
    staging.mkdir(mode=0o700)
    preserved_orphan = tmp_path / "preserved-validated-orphan"
    original_metadata = reclaimer._ensure_legacy_empty_staging_orphan_metadata

    def replace_at_external_metadata_entry(
        orphan: Path,
        staging_id: UUID,
        orphan_token: UUID | None,
        expected_identity: tuple[int, int, int, int],
    ) -> None:
        os.rename(orphan, preserved_orphan)
        orphan.mkdir(mode=0o700)
        (orphan / "business.bin").write_bytes(b"business-directory-must-stay-pristine")
        original_metadata(orphan, staging_id, orphan_token, expected_identity)

    monkeypatch.setattr(
        reclaimer,
        "_ensure_legacy_empty_staging_orphan_metadata",
        replace_at_external_metadata_entry,
    )

    with pytest.raises(LabArtifactConflictError, match="orphan identity conflicts"):
        reclaimer.collect_garbage()

    orphans = tuple(reclaimer.garbage_intent_orphan_dir.glob(f"legacy-empty-staging-{legacy_id}-*"))
    assert len(orphans) == 1
    assert {child.name for child in orphans[0].iterdir()} == {"business.bin"}
    assert (orphans[0] / "business.bin").read_bytes() == b"business-directory-must-stay-pristine"
    assert tuple(reclaimer.garbage_orphan_metadata_dir.iterdir()) == ()
    assert tuple(orphans[0].glob("orphan.json")) == ()
    assert preserved_orphan.is_dir()


def test_external_orphan_metadata_is_authoritative_after_orphan_replacement(
    tmp_path: Path,
) -> None:
    from rquant.lab_worker import LabArtifactConflictError, LabArtifactReclaimer

    reports = LabReportSpool(tmp_path / "reports")
    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=reports,
    )
    legacy_id = uuid4().hex
    staging = reclaimer.garbage_staging_dir / legacy_id
    staging.mkdir(mode=0o700)
    reclaimer.collect_garbage()
    orphan = next(reclaimer.garbage_intent_orphan_dir.glob(f"legacy-empty-staging-{legacy_id}-*"))
    ledger = reclaimer.garbage_orphan_metadata_dir / f"{orphan.name}.json"
    expected_ledger = ledger.read_bytes()
    preserved_orphan = tmp_path / "preserved-ledger-orphan"
    os.rename(orphan, preserved_orphan)
    orphan.mkdir(mode=0o700)
    (orphan / "business.bin").write_bytes(b"replacement-must-not-be-claimed")

    restarted = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=reports,
    )
    with pytest.raises(LabArtifactConflictError, match="orphan identity conflicts"):
        restarted.collect_garbage()

    assert ledger.read_bytes() == expected_ledger
    assert {child.name for child in orphan.iterdir()} == {"business.bin"}
    assert (orphan / "business.bin").read_bytes() == b"replacement-must-not-be-claimed"
    assert tuple(orphan.glob("orphan.json")) == ()
    assert tuple(preserved_orphan.iterdir()) == ()


def test_legacy_internal_orphan_metadata_is_read_only_compatible(tmp_path: Path) -> None:
    from rquant.lab_worker import LabArtifactReclaimer

    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    staging_id = uuid4()
    orphan = reclaimer.garbage_intent_orphan_dir / f"legacy-empty-staging-{staging_id.hex}"
    orphan.mkdir(mode=0o700)
    metadata = reclaimer._legacy_empty_staging_orphan_metadata(staging_id, None)
    marker = orphan / "orphan.json"
    marker.write_text(metadata.canonical_json(), encoding="utf-8")
    before = marker.lstat()
    before_bytes = marker.read_bytes()

    reclaimer.collect_garbage()

    after = marker.lstat()
    assert (after.st_dev, after.st_ino, after.st_nlink) == (
        before.st_dev,
        before.st_ino,
        before.st_nlink,
    )
    assert marker.read_bytes() == before_bytes
    assert {child.name for child in orphan.iterdir()} == {"orphan.json"}
    assert tuple(reclaimer.garbage_orphan_metadata_dir.iterdir()) == ()


def test_legacy_empty_staging_rename_occupation_preserves_both_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.lab_worker as lab_worker_module
    from rquant.lab_worker import LabArtifactConflictError, LabArtifactReclaimer

    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    legacy_id = uuid4().hex
    staging = reclaimer.garbage_staging_dir / legacy_id
    staging.mkdir(mode=0o700)
    original_rename = lab_worker_module.os.rename

    def occupy_after_orphan_rename(source: Path, target: Path) -> None:
        original_rename(source, target)
        if Path(source) == staging and Path(target).parent == reclaimer.garbage_intent_orphan_dir:
            staging.mkdir(mode=0o700)
            (staging / "concurrent.bin").write_bytes(b"concurrent-source-must-survive")

    monkeypatch.setattr(lab_worker_module.os, "rename", occupy_after_orphan_rename)

    with pytest.raises(LabArtifactConflictError, match="changed during orphan isolation"):
        reclaimer.collect_garbage()

    assert (staging / "concurrent.bin").read_bytes() == b"concurrent-source-must-survive"
    orphans = tuple(reclaimer.garbage_intent_orphan_dir.glob(f"legacy-empty-staging-{legacy_id}-*"))
    assert len(orphans) == 1
    assert tuple(orphans[0].iterdir()) == ()
    assert tuple(reclaimer.garbage_intent_orphan_dir.rglob("orphan.json")) == ()
    assert tuple(reclaimer.garbage_orphan_metadata_dir.iterdir()) == ()


@pytest.mark.parametrize("failure", ["missing", "both", "identity"])
def test_owner_only_staging_fails_closed_on_source_conflict(
    tmp_path: Path,
    failure: str,
) -> None:
    from rquant.lab_worker import LabArtifactConflictError, LabArtifactReclaimer

    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    victim = tmp_path / "artifacts" / "owner-conflict" / "victim.bin"
    victim.parent.mkdir(parents=True)
    victim.write_bytes(b"expected-source")
    owner = reclaimer._garbage_owner(victim, purpose="owner-only conflict fixture")
    staging = reclaimer.garbage_staging_dir / owner.garbage_id.hex
    staging.mkdir(mode=0o700)
    reclaimer._write_garbage_owner(staging, owner)
    if failure == "missing":
        victim.unlink()
    elif failure == "both":
        (staging / "payload").write_bytes(victim.read_bytes())
    else:
        replacement = victim.with_suffix(".replacement")
        replacement.write_bytes(b"different-source")
        os.replace(replacement, victim)

    with pytest.raises(LabArtifactConflictError):
        reclaimer.collect_garbage()

    assert staging.is_dir()
    if failure != "missing":
        assert victim.is_file()


def test_sealed_rollback_crash_resumes_as_deferred_gc(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    worker = _worker(tmp_path)
    validated = worker._validate_closed_claim(claim)
    prepared = worker._prepare_result(
        claim,
        RecordingRegistry().execute_shard(validated, object()),
    )
    bundle = worker._publish_candidate(
        claim,
        prepared,
        deadline=None,
        effective_expiry=None,
        validate_concurrent_race=True,
    )
    original_promote = worker.artifact_reclaimer._promote_garbage_bundle
    interrupted = False

    def interrupt_after_rollback_isolation(source: Path, target: Path) -> None:
        nonlocal interrupted
        if source.parent == worker.artifact_reclaimer.garbage_staging_dir and not interrupted:
            interrupted = True
            raise InterruptedError("crash after sealed rollback isolation")
        original_promote(source, target)

    monkeypatch.setattr(
        worker.artifact_reclaimer,
        "_promote_garbage_bundle",
        interrupt_after_rollback_isolation,
    )
    with pytest.raises(InterruptedError, match="sealed rollback isolation"):
        worker._rollback_sealed(claim, bundle)

    assert not bundle.path.exists()
    assert len(tuple(worker.artifact_reclaimer.garbage_staging_dir.iterdir())) == 1
    monkeypatch.setattr(
        worker.artifact_reclaimer,
        "_promote_garbage_bundle",
        original_promote,
    )
    restarted = _worker(tmp_path)
    restarted.artifact_reclaimer.collect_garbage()
    restarted.artifact_reclaimer.reclaim(_retry_claim(claim))

    entries = restarted.artifact_reclaimer.quarantine_entries()
    assert any("sealed rollback" in entry.owner.purpose for entry in entries)
    assert all(entry.state == "deferred_gc" for entry in entries)


def test_worker_runtime_drift_at_atomic_publish_preserves_prepared_candidate(
    tmp_path: Path,
) -> None:
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    worker = _worker(tmp_path)
    validated = worker._validate_closed_claim(claim)
    prepared = worker._prepare_result(
        claim,
        RecordingRegistry().execute_shard(validated, object()),
    )
    assert prepared.temporary is not None
    calls = 0

    def runtime_guard() -> str:
        nonlocal calls
        calls += 1
        if calls >= 2:
            raise LabDaemonConfigurationError("runtime checkout drifted")
        return "1" * 40

    worker.verified_code_sha_provider = runtime_guard

    with pytest.raises(LabDaemonConfigurationError, match="drifted"):
        worker._publish_candidate(
            claim,
            prepared,
            deadline=None,
            effective_expiry=None,
            validate_concurrent_race=True,
        )

    assert prepared.temporary.is_dir()
    assert not worker.sealed_bundle_path(claim).exists()


def test_worker_runtime_drift_before_staging_creation_leaves_no_bundle(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    drifted = False

    def runtime_guard() -> str:
        if drifted:
            raise LabDaemonConfigurationError("runtime drifted before staging creation")
        return "1" * 40

    worker = _worker(tmp_path, verified_code_sha_provider=runtime_guard)
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    validated = worker._validate_closed_claim(claim)
    result = RecordingRegistry().execute_shard(validated, object())

    def drift_after_parent_ready(_temporary: Path) -> None:
        nonlocal drifted
        drifted = True

    monkeypatch.setattr(
        worker,
        "_before_result_staging_creation",
        drift_after_parent_ready,
        raising=False,
    )

    with pytest.raises(LabDaemonConfigurationError, match="staging creation"):
        worker._prepare_result(claim, result)

    assert tuple((worker.artifact_root / ".tmp").rglob("manifest.json")) == ()


def test_worker_runtime_drift_before_parquet_publication_leaves_incomplete_staging(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    drifted = False

    def runtime_guard() -> str:
        if drifted:
            raise LabDaemonConfigurationError("runtime drifted before parquet publication")
        return "1" * 40

    worker = _worker(tmp_path, verified_code_sha_provider=runtime_guard)
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    validated = worker._validate_closed_claim(claim)
    result = RecordingRegistry().execute_shard(validated, object())

    def drift_after_parquet_fsync(_temporary: Path, _parquet_temp: Path) -> None:
        nonlocal drifted
        drifted = True

    monkeypatch.setattr(
        worker,
        "_after_result_parquet_temp_fsync",
        drift_after_parquet_fsync,
        raising=False,
    )

    with pytest.raises(LabDaemonConfigurationError, match="parquet publication"):
        worker._prepare_result(claim, result)

    assert tuple((worker.artifact_root / ".tmp").rglob("manifest.json")) == ()
    assert tuple((worker.artifact_root / ".tmp").rglob("*.parquet")) == ()


def test_worker_runtime_drift_before_manifest_completion_leaves_no_complete_bundle(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    drifted = False

    def runtime_guard() -> str:
        if drifted:
            raise LabDaemonConfigurationError("runtime drifted before manifest completion")
        return "1" * 40

    worker = _worker(tmp_path, verified_code_sha_provider=runtime_guard)
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    validated = worker._validate_closed_claim(claim)
    result = RecordingRegistry().execute_shard(validated, object())

    def drift_after_manifest_fsync(_temporary: Path, _manifest_temp: Path) -> None:
        nonlocal drifted
        drifted = True

    monkeypatch.setattr(
        worker,
        "_after_result_manifest_temp_fsync",
        drift_after_manifest_fsync,
        raising=False,
    )

    with pytest.raises(LabDaemonConfigurationError, match="manifest completion"):
        worker._prepare_result(claim, result)

    assert tuple((worker.artifact_root / ".tmp").rglob("manifest.json")) == ()


def test_sealed_rollback_hard_crash_resumes_in_new_process(tmp_path: Path) -> None:
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    worker = _worker(tmp_path)
    validated = worker._validate_closed_claim(claim)
    worker._seal_result(
        claim,
        RecordingRegistry().execute_shard(validated, object()),
    )
    sealed = worker.sealed_bundle_path(claim)

    crashed = _run_worker_child(
        "_crash_sealed_rollback_after_payload_isolation_child",
        tmp_path,
        claim.model_dump_json(),
    )

    assert crashed.returncode == 83, crashed.stderr
    assert not sealed.exists()
    assert len(tuple(worker.artifact_reclaimer.garbage_staging_dir.iterdir())) == 1

    restarted = _worker(tmp_path)
    restarted.artifact_reclaimer.collect_garbage()
    restarted.artifact_reclaimer.reclaim(_retry_claim(claim))

    entries = restarted.artifact_reclaimer.quarantine_entries()
    assert len(entries) == 1
    assert entries[0].state == "deferred_gc"
    assert "sealed rollback" in entries[0].owner.purpose


@pytest.mark.parametrize(
    "crash_phase",
    ["intent", "staging", "global_owner", "bundle_owner", "prepared_ledger"],
)
def test_sealed_rollback_recovers_every_prepared_intent_boundary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    crash_phase: str,
) -> None:
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    worker = _worker(tmp_path)
    validated = worker._validate_closed_claim(claim)
    prepared = worker._prepare_result(
        claim,
        RecordingRegistry().execute_shard(validated, object()),
    )
    bundle = worker._publish_candidate(
        claim,
        prepared,
        deadline=None,
        effective_expiry=None,
        validate_concurrent_race=True,
    )
    reclaimer = worker.artifact_reclaimer
    method_by_phase = {
        "intent": "_write_prepared_intent",
        "staging": "_ensure_garbage_staging",
        "global_owner": "_ensure_global_garbage_owner",
        "bundle_owner": "_ensure_bundle_garbage_owner",
        "prepared_ledger": "_write_garbage_ledger",
    }
    method_name = method_by_phase[crash_phase]
    original = getattr(reclaimer, method_name)
    interrupted = False

    def interrupt_after_phase(*args, **kwargs):
        nonlocal interrupted
        result = original(*args, **kwargs)
        state = kwargs.get("state")
        if len(args) > 1:
            state = args[1]
        should_interrupt = crash_phase != "prepared_ledger" or state == "prepared"
        if should_interrupt and not interrupted:
            interrupted = True
            raise InterruptedError(f"crash after {crash_phase}")
        return result

    monkeypatch.setattr(reclaimer, method_name, interrupt_after_phase)
    with pytest.raises(InterruptedError, match=crash_phase):
        worker._rollback_sealed(claim, bundle)
    monkeypatch.setattr(reclaimer, method_name, original)

    restarted = _worker(tmp_path)
    restarted.artifact_reclaimer.collect_garbage()
    restarted.artifact_reclaimer.reclaim(_retry_claim(claim))
    first = restarted.artifact_reclaimer.quarantine_summary()
    restarted.artifact_reclaimer.collect_garbage()
    second = restarted.artifact_reclaimer.quarantine_summary()

    assert not bundle.path.exists()
    assert first == second
    assert first.bundle_count == 1
    assert first.retained_bytes > 0


@pytest.mark.parametrize(
    "crash_phase",
    ["intent", "staging", "global_owner", "bundle_owner", "prepared_ledger"],
)
def test_sealed_rollback_hard_exit_recovers_every_prepared_intent_boundary(
    tmp_path: Path,
    crash_phase: str,
) -> None:
    claim = _claim(_nshape_compare_spec(hold_days=(1,)))
    worker = _worker(tmp_path)
    validated = worker._validate_closed_claim(claim)
    worker._seal_result(
        claim,
        RecordingRegistry().execute_shard(validated, object()),
    )

    crashed = _run_worker_child(
        "_crash_sealed_rollback_after_prepared_phase_child",
        tmp_path,
        claim.model_dump_json(),
        crash_phase,
    )

    assert crashed.returncode == 85, crashed.stderr
    restarted = _worker(tmp_path)
    restarted.artifact_reclaimer.collect_garbage()
    restarted.artifact_reclaimer.reclaim(_retry_claim(claim))
    summary = restarted.artifact_reclaimer.quarantine_summary()
    assert summary.bundle_count == 1
    assert summary.retained_bytes > 0


def test_quarantine_preserves_payload_replaced_after_final_validation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.lab_worker import LabArtifactConflictError, LabArtifactReclaimer

    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    victim = tmp_path / "artifacts" / "logical-delete" / "victim.bin"
    victim.parent.mkdir(parents=True)
    victim.write_bytes(b"validated-original")
    expected = reclaimer._regular_file_identity(victim, label="replacement fixture")
    reclaimer._safe_remove_regular_child(
        victim,
        expected=expected,
        label="replacement fixture",
    )
    saved_original = tmp_path / "saved-garbage-original.bin"
    original_validate = reclaimer._validate_garbage_bundle
    replaced = False

    def replace_after_validation(bundle: Path):
        nonlocal replaced
        owner = original_validate(bundle)
        if bundle.parent == reclaimer.garbage_deferred_dir and not replaced:
            replaced = True
            payload = bundle / "payload"
            os.replace(payload, saved_original)
            payload.write_bytes(b"external-replacement-must-survive")
        return owner

    monkeypatch.setattr(reclaimer, "_validate_garbage_bundle", replace_after_validation)

    reclaimer.collect_garbage()
    with pytest.raises(LabArtifactConflictError):
        reclaimer.collect_garbage()

    preserved = tuple(reclaimer.garbage_deferred_dir.rglob("payload"))
    assert len(preserved) == 1
    assert preserved[0].read_bytes() == b"external-replacement-must-survive"
    assert saved_original.read_bytes() == b"validated-original"


def test_quarantine_preserves_bundle_replaced_after_final_validation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.lab_worker import LabArtifactConflictError, LabArtifactReclaimer

    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    victim = tmp_path / "artifacts" / "logical-delete" / "victim.bin"
    victim.parent.mkdir(parents=True)
    victim.write_bytes(b"validated-original")
    expected = reclaimer._regular_file_identity(victim, label="owned replacement fixture")
    reclaimer._safe_remove_regular_child(
        victim,
        expected=expected,
        label="owned replacement fixture",
    )
    saved_original = tmp_path / "saved-owned-bundle"
    original_validate = reclaimer._validate_garbage_bundle
    replaced = False

    def replace_after_validation(bundle: Path):
        nonlocal replaced
        owner = original_validate(bundle)
        if bundle.parent == reclaimer.garbage_deferred_dir and not replaced:
            replaced = True
            os.rename(bundle, saved_original)
            bundle.mkdir()
            (bundle / "foreign.bin").write_bytes(b"foreign-must-survive")
        return owner

    monkeypatch.setattr(reclaimer, "_validate_garbage_bundle", replace_after_validation)

    reclaimer.collect_garbage()
    with pytest.raises(LabArtifactConflictError):
        reclaimer.collect_garbage()

    deferred = tuple(reclaimer.garbage_deferred_dir.iterdir())
    assert len(deferred) == 1
    assert (deferred[0] / "foreign.bin").read_bytes() == b"foreign-must-survive"
    assert (saved_original / "payload").read_bytes() == b"validated-original"


def test_deferred_bundle_remains_enumerable_across_restarts(tmp_path: Path) -> None:
    from rquant.lab_worker import LabArtifactReclaimer

    reports = LabReportSpool(tmp_path / "reports")
    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=reports,
    )
    victim = tmp_path / "artifacts" / "logical-delete" / "victim.bin"
    victim.parent.mkdir(parents=True)
    victim.write_bytes(b"restart-owned-payload")
    expected = reclaimer._regular_file_identity(victim, label="restart fixture")
    reclaimer._safe_remove_regular_child(
        victim,
        expected=expected,
        label="restart fixture",
    )
    for _ in range(3):
        restarted = LabArtifactReclaimer(
            artifact_root=tmp_path / "artifacts",
            report_spool=LabReportSpool(tmp_path / "reports"),
        )
        restarted.collect_garbage()

    entry = restarted.quarantine_entries()[0]
    assert entry.state == "deferred_gc"
    assert (entry.bundle_path / "payload").read_bytes() == b"restart-owned-payload"


def test_quarantine_rejects_unknown_deferred_bundle(tmp_path: Path) -> None:
    from rquant.lab_worker import LabArtifactConflictError, LabArtifactReclaimer

    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    unknown = reclaimer.garbage_deferred_dir / uuid4().hex
    unknown.mkdir()

    with pytest.raises(LabArtifactConflictError):
        reclaimer.collect_garbage()

    assert unknown.is_dir()


def test_quarantine_fails_closed_when_payload_and_bundle_owner_disappear(
    tmp_path: Path,
) -> None:
    from rquant.lab_worker import LabArtifactConflictError, LabArtifactReclaimer

    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    victim = tmp_path / "artifacts" / "logical-delete" / "victim.bin"
    victim.parent.mkdir(parents=True)
    victim.write_bytes(b"final-cleanup-crash")
    identity = reclaimer._regular_file_identity(victim, label="final cleanup fixture")
    reclaimer._safe_remove_regular_child(
        victim,
        expected=identity,
        label="final cleanup fixture",
    )
    deferred = tuple(reclaimer.garbage_deferred_dir.iterdir())[0]
    (deferred / "payload").unlink()
    (deferred / "owner.json").unlink()

    restarted = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=LabReportSpool(tmp_path / "reports"),
    )
    with pytest.raises(LabArtifactConflictError):
        restarted.collect_garbage()

    assert deferred.is_dir()
    assert tuple(restarted.garbage_ledger_dir.iterdir())


def test_reclaimer_does_not_isolate_directory_replaced_at_rename(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.lab_worker as lab_worker_module
    from rquant.lab_worker import LabArtifactConflictError, LabArtifactReclaimer

    reports = LabReportSpool(tmp_path / "reports")
    _old_claim, current_claim, sealed, _manifest = _sealed_obsolete_attempt(
        tmp_path,
        reports,
    )
    saved = sealed.parent / "saved-original"
    original_rename = lab_worker_module.os.rename

    def replace_at_rename(source: Path, target: Path) -> None:
        if Path(source) == sealed:
            original_rename(source, saved)
            sealed.mkdir()
            (sealed / "victim.txt").write_text("preserve", encoding="utf-8")
        original_rename(source, target)

    monkeypatch.setattr(lab_worker_module.os, "rename", replace_at_rename)
    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=reports,
    )

    with pytest.raises(LabArtifactConflictError, match="replaced during isolation"):
        reclaimer.reclaim(current_claim)

    assert (sealed / "victim.txt").read_text(encoding="utf-8") == "preserve"
    assert saved.is_dir()
    assert tuple(sealed.parent.glob(".reclaim-*")) == ()


def test_stale_success_rejection_retries_failed_reconciliation(
    tmp_path: Path,
) -> None:
    from rquant.lab_job_protocol import LabCommandEnvelope, LabCommandSpool, SubmitJobCommand
    from rquant.lab_jobs import LabJobStore
    from rquant.lab_scheduler import LabScheduler
    from rquant.lab_worker import LabArtifactReclaimer

    clock = [NOW]
    reports = LabReportSpool(tmp_path / "reports")
    reclaimer = LabArtifactReclaimer(
        artifact_root=tmp_path / "artifacts",
        report_spool=reports,
    )
    claims = LabClaimSpool(
        tmp_path / "claims",
        claim_advance_hook=reclaimer.reclaim,
    )
    commands = LabCommandSpool(tmp_path / "commands")
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    job_id = uuid4()
    commands.publish(
        LabCommandEnvelope(
            request_id=uuid4(),
            command=SubmitJobCommand(
                job_id=job_id,
                spec=_nshape_compare_spec(hold_days=(1,)),
                max_attempts=2,
            ),
        )
    )
    scheduler = LabScheduler(
        store=store,
        spool=commands,
        owner_id="scheduler-a",
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=5,
        report_spool=reports,
        claim_spool=claims,
        claim_worker_ids=("worker-a",),
        shard_lease_seconds=20,
        adapter_registry=default_strategy_job_adapter_registry(),
        clock=lambda: clock[0],
    )
    scheduler.run_once()
    old_claim = claims.pending()[0].claim
    sealing_worker = _worker(
        tmp_path,
        claims=LabClaimSpool(tmp_path / "sealing-claims"),
        reports=reports,
    )
    validated = sealing_worker._validate_closed_claim(old_claim)
    result = RecordingRegistry().execute_shard(validated, object())
    manifest = sealing_worker._seal_result(old_claim, result)
    sealed = sealing_worker.sealed_bundle_path(old_claim)
    failed_once = False

    def flaky_reclaim(claim: LabShardClaim) -> None:
        nonlocal failed_once
        assert claims.current(claim.job_id, claim.shard_id).claim == claim
        if claim.claim_generation > old_claim.claim_generation and not failed_once:
            failed_once = True
            raise RuntimeError("injected first reconciliation failure")
        reclaimer.reclaim(claim)

    claims.set_claim_advance_hook(flaky_reclaim)
    clock[0] = NOW + timedelta(seconds=21)
    with _raising_loguru_sink():
        recovery = scheduler.run_once()
    fresh_claim = claims.current(old_claim.job_id, old_claim.shard_id).claim
    assert recovery.claim_reconcile_failures == 1
    assert sealed.is_dir()
    success = LabWorkerReport.from_claim(
        old_claim,
        report_id=uuid4(),
        reported_at=clock[0],
        body=LabShardSucceeded(result_manifest_hash=manifest.manifest_hash),
    )
    reports.publish(success)
    assert fresh_claim.claim_generation == old_claim.claim_generation + 1
    processed = scheduler.run_once()
    receipt = reports.load_receipt(reports.ack_dir / f"{success.report_id}.json")
    scheduler.release()

    assert processed.reports_rejected == 1
    assert processed.claims_reconciled == 1
    assert processed.claim_reconcile_failures == 0
    assert receipt.status == "rejected"
    assert not sealed.exists()


def test_scheduler_retires_accepted_success_from_hot_claim_authority(tmp_path: Path) -> None:
    from rquant.lab_job_protocol import LabCommandEnvelope, LabCommandSpool, SubmitJobCommand
    from rquant.lab_jobs import LabJobStore
    from rquant.lab_scheduler import LabScheduler

    reports = LabReportSpool(tmp_path / "reports")
    claims = LabClaimSpool(tmp_path / "claims")
    commands = LabCommandSpool(tmp_path / "commands")
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    job_id = uuid4()
    commands.publish(
        LabCommandEnvelope(
            request_id=uuid4(),
            command=SubmitJobCommand(
                job_id=job_id,
                spec=_nshape_compare_spec(hold_days=(1,)),
                max_attempts=2,
            ),
        )
    )
    scheduler = LabScheduler(
        store=store,
        spool=commands,
        owner_id="scheduler-a",
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=5,
        report_spool=reports,
        claim_spool=claims,
        claim_worker_ids=("worker-a",),
        shard_lease_seconds=20,
        adapter_registry=default_strategy_job_adapter_registry(),
        clock=lambda: NOW,
    )
    scheduler.run_once()
    claim = claims.consume(claims.pending()[0])
    assert claim.definition.work_plan is not None
    success = LabWorkerReport.from_claim(
        claim,
        report_id=uuid4(),
        reported_at=NOW,
        body=LabShardSucceeded.current(
            result_manifest_hash="a" * 64,
            worker_code_sha="1" * 40,
            telemetry=LabShardTelemetry.from_work_plan(
                claim.definition.work_plan,
                monotonic_started=10,
                monotonic_finished=11,
            ),
        ),
    )
    reports.publish(success)

    result = scheduler.run_once()

    assert result.reports_accepted == 1
    with pytest.raises(InvalidCommandEnvelopeError):
        claims.current(claim.job_id, claim.shard_id)
    retired = claims.retired_high_water(claim.job_id, claim.shard_id)
    assert retired.claim == claim
    assert retired.outcome == "accepted"


def test_scheduler_authority_fairly_retires_orphan_current_across_restart(
    tmp_path: Path,
) -> None:
    from rquant.lab_job_protocol import LabCommandEnvelope, LabCommandSpool, SubmitJobCommand
    from rquant.lab_jobs import LabJobStore
    from rquant.lab_scheduler import LabScheduler

    claims = LabClaimSpool(tmp_path / "claims")
    commands = LabCommandSpool(tmp_path / "commands")
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    spec = _nshape_compare_spec(hold_days=(1,))
    for _ in range(2):
        commands.publish(
            LabCommandEnvelope(
                request_id=uuid4(),
                command=SubmitJobCommand(job_id=uuid4(), spec=spec, max_attempts=2),
            )
        )

    def scheduler(owner_id: str) -> LabScheduler:
        return LabScheduler(
            store=store,
            spool=commands,
            owner_id=owner_id,
            lease_seconds=60,
            heartbeat_seconds=10,
            poll_interval_ms=5,
            claim_spool=claims,
            claim_worker_ids=("worker-a", "worker-b"),
            shard_lease_seconds=20,
            max_claim_authority_per_tick=2,
            adapter_registry=default_strategy_job_adapter_registry(),
            clock=lambda: NOW,
        )

    first = scheduler("scheduler-a")
    first.run_once()
    orphan = _claim(spec)
    claims.consume(claims.publish(orphan))
    first.run_once()
    first.release()

    restarted = scheduler("scheduler-b")
    for _ in range(8):
        restarted.run_once()
        try:
            retired = claims.retired_high_water(orphan.job_id, orphan.shard_id)
        except InvalidCommandEnvelopeError:
            continue
        assert retired.claim == orphan
        assert retired.outcome == "revoked"
        break
    else:
        pytest.fail("orphan current claim was starved by persistent pending deliveries")

    with pytest.raises((LabClaimRevokedError, LabClaimSupersededError)):
        claims.admit_execution(orphan)
    assert len(claims.pending()) >= 2


def test_scheduler_retries_revocation_retirement_from_hot_marker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.lab_job_protocol import LabCommandEnvelope, LabCommandSpool, SubmitJobCommand
    from rquant.lab_jobs import LabJobStore
    from rquant.lab_scheduler import LabScheduler
    from rquant.lab_shard_protocol import LabRetiredClaimAuthority

    clock = [NOW]
    claims = LabClaimSpool(tmp_path / "claims")
    commands = LabCommandSpool(tmp_path / "commands")
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    commands.publish(
        LabCommandEnvelope(
            request_id=uuid4(),
            command=SubmitJobCommand(
                job_id=uuid4(),
                spec=_nshape_compare_spec(hold_days=(1,)),
                max_attempts=1,
            ),
        )
    )
    scheduler = LabScheduler(
        store=store,
        spool=commands,
        owner_id="scheduler-a",
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=5,
        claim_spool=claims,
        claim_worker_ids=("worker-a",),
        shard_lease_seconds=20,
        adapter_registry=default_strategy_job_adapter_registry(),
        clock=lambda: clock[0],
    )
    scheduler.run_once()
    claim = claims.consume(claims.pending()[0])
    original_retire = claims.retire
    failed_once = False

    def fail_first_retire(
        stale: LabShardClaim,
        *,
        outcome: Literal["accepted", "revoked"],
        reason: str,
    ) -> LabRetiredClaimAuthority:
        nonlocal failed_once
        if outcome == "revoked" and not failed_once:
            failed_once = True
            raise OSError("injected cold archive outage")
        return original_retire(stale, outcome=outcome, reason=reason)

    monkeypatch.setattr(claims, "retire", fail_first_retire)
    clock[0] = NOW + timedelta(seconds=21)
    interrupted = scheduler.run_once()

    assert interrupted.claim_revoke_failures == 1
    assert claims.hot_delivery_batch(limit=8).claims == (claim,)

    recovered = scheduler.run_once()

    assert recovered.claim_revoke_failures == 0
    assert recovered.claims_retired == 1
    assert claims.hot_delivery_batch(limit=8).claims == ()
    assert claims.retired_high_water(claim.job_id, claim.shard_id).outcome == "revoked"


def test_scheduler_tick_history_work_is_active_plus_bounded(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.lab_job_protocol import LabCommandEnvelope, LabCommandSpool, SubmitJobCommand
    from rquant.lab_jobs import LabJobStore
    from rquant.lab_scheduler import LabScheduler
    from rquant.lab_shard_protocol import LabClaimDeliveryReceipt, LabConsumedClaim

    reports = LabReportSpool(tmp_path / "reports")
    claims = LabClaimSpool(tmp_path / "claims")
    commands = LabCommandSpool(tmp_path / "commands")
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    for _ in range(3):
        commands.publish(
            LabCommandEnvelope(
                request_id=uuid4(),
                command=SubmitJobCommand(
                    job_id=uuid4(),
                    spec=_nshape_compare_spec(hold_days=(1,)),
                    max_attempts=2,
                ),
            )
        )
    scheduler = LabScheduler(
        store=store,
        spool=commands,
        owner_id="scheduler-a",
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=5,
        report_spool=reports,
        claim_spool=claims,
        claim_worker_ids=("worker-a", "worker-b", "worker-c"),
        shard_lease_seconds=20,
        adapter_registry=default_strategy_job_adapter_registry(),
        clock=lambda: NOW,
    )
    scheduler.run_once()
    active = tuple(claims.consume(entry) for entry in claims.pending())
    assert len(active) == 3
    for index in range(5_000):
        token = UUID(int=index + 10_000)
        (claims.ack_dir / f"{token}.json").write_bytes(b"{}")
        report_id = UUID(int=index + 20_000)
        (reports.ack_dir / f"{report_id}.json").write_bytes(b"{}")

    consumed_parses = 0
    report_parses = 0
    original_consumed = claims._load_consumed_locked
    original_receipt = reports.load_receipt

    def count_consumed(token: UUID) -> LabConsumedClaim:
        nonlocal consumed_parses
        consumed_parses += 1
        path = claims.ack_dir / f"{token}.json"
        if path.read_bytes() == b"{}":
            return LabConsumedClaim(
                path=path,
                receipt=LabClaimDeliveryReceipt(claim=active[0]),
            )
        return original_consumed(token)

    def count_report(path: Path) -> LabReportReceipt:
        nonlocal report_parses
        report_parses += 1
        return original_receipt(path)

    monkeypatch.setattr(claims, "_load_consumed_locked", count_consumed)
    monkeypatch.setattr(reports, "load_receipt", count_report)

    result = scheduler.run_once()

    assert result.claim_revoke_failures == 0
    assert consumed_parses <= 12
    assert report_parses == 0
