"""WP9 design v4 probe suite (stdlib only, macOS + Linux).

Every probe prints ``PROBE <name> PASS|FAIL <details>`` plus a ``DATA <name>
<json>`` line.  Exit status is non-zero when any probe fails.

Run: ``python run_all.py``  (no third-party deps, no pytest, no rquant import)
"""

import errno
import inspect
import json
import multiprocessing.util
import os
import platform
import select
import signal
import socket
import sqlite3
import statistics
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
HELPER_MIN = os.path.join(HERE, "helper_min.py")
HELPER_SIGTERM = os.path.join(HERE, "helper_sigterm_ignore.py")
HELPER_MP_HANG = os.path.join(HERE, "helper_mp_hang.py")
HELPER_SQLITE = os.path.join(HERE, "helper_sqlite_hold.py")
HELPER_HEARTBEAT = os.path.join(HERE, "helper_heartbeat.py")
HELPER_PARENT = os.path.join(HERE, "helper_parent.py")
HELPER_PRODSHAPE = os.path.join(HERE, "helper_prodshape.py")
HELPER_PARENT_V2 = os.path.join(HERE, "helper_parent_v2.py")

BUSY_MS = 400
RESULTS = []


def _errname(code):
    if code == 0:
        return "OK"
    return errno.errorcode.get(code, code)


def record(name, ok, details, data=None):
    RESULTS.append((name, ok))
    print(f"PROBE {name} {'PASS' if ok else 'FAIL'} {details}", flush=True)
    if data is not None:
        print(f"DATA {name} {json.dumps(data, sort_keys=True)}", flush=True)


def pct(values, q):
    ordered = sorted(values)
    if not ordered:
        return float("nan")
    index = min(len(ordered) - 1, int(round((len(ordered) - 1) * q)))
    return ordered[index]


def summarise(values):
    return {
        "n": len(values),
        "p50_ms": round(statistics.median(values) * 1000, 3),
        "p95_ms": round(pct(values, 0.95) * 1000, 3),
        "min_ms": round(min(values) * 1000, 3),
        "max_ms": round(max(values) * 1000, 3),
    }


def send_frame(fd, frame):
    os.write(fd, (json.dumps(frame, separators=(",", ":")) + "\n").encode("utf-8"))


class LineReader:
    def __init__(self, fd):
        self.fd = fd
        self.buf = b""

    def poll(self, timeout):
        """Return dict, 'EOF', or None on timeout."""
        deadline = time.monotonic() + timeout
        while True:
            if b"\n" in self.buf:
                line, _, self.buf = self.buf.partition(b"\n")
                return json.loads(line)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            readable, _, _ = select.select([self.fd], [], [], remaining)
            if not readable:
                return None
            chunk = os.read(self.fd, 65536)
            if not chunk:
                return "EOF"
            self.buf += chunk


def start_min_helper(misbehave="none", env=None):
    ctrl_r, ctrl_w = os.pipe()
    status_r, status_w = os.pipe()
    stop_r, stop_w = os.pipe()
    for fd in (ctrl_r, status_w, stop_r):
        os.set_inheritable(fd, True)
    started = time.monotonic()
    child = subprocess.Popen(
        [sys.executable, "-I", HELPER_MIN, str(ctrl_r), str(status_w), str(stop_r), misbehave],
        pass_fds=(ctrl_r, status_w, stop_r),
        start_new_session=True,
        close_fds=True,
        env={"PATH": "/usr/bin:/bin"} if env is None else env,
    )
    os.close(ctrl_r)
    os.close(status_w)
    os.close(stop_r)
    return child, ctrl_w, LineReader(status_r), status_r, stop_w, started


def stop_min_helper(child, ctrl_w, status_r, stop_w):
    for fd in (ctrl_w, stop_w, status_r):
        try:
            os.close(fd)
        except OSError:
            pass
    try:
        child.wait(timeout=5)
    except subprocess.TimeoutExpired:
        child.kill()
        child.wait(timeout=5)


# --------------------------------------------------------------------------
# (v) multiprocessing._exit_function hang vs Popen
# --------------------------------------------------------------------------
def probe_mp_exit_function_hang():
    source = inspect.getsource(multiprocessing.util._exit_function)
    joins_without_timeout = ".join()" in source
    outcomes = {}
    for mode, expect_hang in (("mp-daemon", True), ("mp-nondaemon", True), ("popen", False)):
        # Output goes to a FILE, never a pipe: a pipe would be held open by the
        # grandchild and ``communicate`` would block on it, which would confound
        # "the parent cannot exit" with "the probe cannot read EOF".
        out_path = os.path.join(tempfile.gettempdir(), f"wp9v4-mphang-{mode}-{os.getpid()}.out")
        started = time.monotonic()
        hung = False
        rc = None
        with open(out_path, "w+", encoding="utf-8") as sink:
            try:
                completed = subprocess.run(
                    [sys.executable, HELPER_MP_HANG, mode],
                    stdout=sink,
                    stderr=subprocess.STDOUT,
                    timeout=15,
                )
                rc = completed.returncode
            except subprocess.TimeoutExpired:
                hung = True
            sink.flush()
            sink.seek(0)
            stdout = sink.read().strip()
        try:
            os.unlink(out_path)
        except OSError:
            pass
        elapsed = time.monotonic() - started
        pid = None
        for token in (stdout or "").split():
            if token.startswith("child_pid="):
                pid = int(token.split("=", 1)[1])
        outcomes[mode] = {
            "hung_at_15s": hung,
            "returncode": rc,
            "elapsed_s": round(elapsed, 3),
            "expected_hang": expect_hang,
            "child_pid": pid,
            "child_confirmed_started": bool(pid),
        }
        if pid:
            try:
                os.kill(pid, signal.SIGKILL)
            except OSError:
                pass
    ok = (
        joins_without_timeout
        and outcomes["mp-daemon"]["hung_at_15s"]
        and outcomes["mp-nondaemon"]["hung_at_15s"]
        and not outcomes["popen"]["hung_at_15s"]
        and outcomes["popen"]["returncode"] == 0
    )
    record(
        "mp_exit_function_hang",
        ok,
        "multiprocessing atexit join never returns for a SIGTERM-ignoring child "
        "(daemon and non-daemon alike); the same child under Popen lets the parent exit",
        {
            "exit_function_has_untimed_join": joins_without_timeout,
            "exit_function_source_tail": source.strip().splitlines()[-8:],
            "modes": outcomes,
        },
    )


# --------------------------------------------------------------------------
# Popen helper start->ready cost (stdlib-only entry)
# --------------------------------------------------------------------------
def probe_popen_startup_cost(n=8):
    samples = []
    ack = []
    for _ in range(n):
        child, ctrl_w, reader, status_r, stop_w, started = start_min_helper()
        send_frame(ctrl_w, {"type": "config", "db": "/dev/null", "busy_timeout_ms": BUSY_MS})
        frame = reader.poll(20.0)
        samples.append(time.monotonic() - started)
        assert isinstance(frame, dict) and frame["type"] == "ready", frame
        t0 = time.monotonic()
        send_frame(ctrl_w, {"type": "session-start", "token": "t1"})
        got = reader.poll(20.0)
        ack.append(time.monotonic() - t0)
        assert isinstance(got, dict) and got["type"] == "session-ack", got
        stop_min_helper(child, ctrl_w, status_r, stop_w)
    record(
        "popen_startup_cost",
        True,
        f"start->ready p50={summarise(samples)['p50_ms']}ms, session-ack p50={summarise(ack)['p50_ms']}ms",
        {"start_to_ready": summarise(samples), "session_ack_roundtrip": summarise(ack)},
    )


# --------------------------------------------------------------------------
# SIGKILL -> wait() latency, and the bounded escalation chain
# --------------------------------------------------------------------------
def probe_popen_escalation_chain(n=6):
    kill_latency = []
    for _ in range(n):
        child, ctrl_w, reader, status_r, stop_w, _ = start_min_helper()
        send_frame(ctrl_w, {"type": "config"})
        reader.poll(20.0)
        child.kill()
        t0 = time.monotonic()
        child.wait(timeout=10)
        kill_latency.append(time.monotonic() - t0)
        for fd in (ctrl_w, stop_w, status_r):
            try:
                os.close(fd)
            except OSError:
                pass

    def escalate(honor):
        ready_r, ready_w = os.pipe()
        os.set_inheritable(ready_w, True)
        child = subprocess.Popen(
            [sys.executable, "-I", HELPER_SIGTERM, str(ready_w)] + (["honor"] if honor else []),
            pass_fds=(ready_w,),
            start_new_session=True,
            close_fds=True,
            env={"PATH": "/usr/bin:/bin"},
        )
        os.close(ready_w)
        os.read(ready_r, 64)
        os.close(ready_r)
        stages = {}
        t_all = time.monotonic()
        t0 = time.monotonic()
        try:
            child.wait(timeout=0.30)
            stages["t1_expired"] = False
        except subprocess.TimeoutExpired:
            stages["t1_expired"] = True
        stages["t1_s"] = round(time.monotonic() - t0, 4)
        t0 = time.monotonic()
        child.terminate()
        try:
            child.wait(timeout=0.25)
            stages["t2_expired"] = False
        except subprocess.TimeoutExpired:
            stages["t2_expired"] = True
        stages["t2_s"] = round(time.monotonic() - t0, 4)
        if stages["t2_expired"]:
            t0 = time.monotonic()
            child.kill()
            child.wait(timeout=5.0)
            stages["t3_s"] = round(time.monotonic() - t0, 4)
        stages["total_s"] = round(time.monotonic() - t_all, 4)
        stages["returncode"] = child.returncode
        return stages

    stubborn = escalate(honor=False)
    obedient = escalate(honor=True)
    ok = (
        stubborn["t2_expired"]
        and stubborn["returncode"] == -signal.SIGKILL
        and stubborn["total_s"] < 0.30 + 0.25 + 5.0 + 0.5
        and not obedient["t2_expired"]
        and obedient["returncode"] == -signal.SIGTERM
    )
    record(
        "popen_escalation_chain",
        ok,
        f"stubborn total={stubborn['total_s']}s rc={stubborn['returncode']}; "
        f"obedient total={obedient['total_s']}s rc={obedient['returncode']}; "
        f"SIGKILL->wait p50={summarise(kill_latency)['p50_ms']}ms",
        {
            "sigkill_to_wait": summarise(kill_latency),
            "sigterm_ignored": stubborn,
            "sigterm_honored": obedient,
        },
    )


# --------------------------------------------------------------------------
# Session handshake: the four interleavings, over newline-delimited JSON frames
# --------------------------------------------------------------------------
def probe_session_handshake():
    results = {}

    # A: normal ack
    child, ctrl_w, reader, status_r, stop_w, _ = start_min_helper()
    send_frame(ctrl_w, {"type": "config"})
    reader.poll(20.0)
    t0 = time.monotonic()
    send_frame(ctrl_w, {"type": "session-start", "token": "A"})
    frame = reader.poll(5.0)
    results["A_normal_ack"] = {
        "frame": frame,
        "elapsed_ms": round((time.monotonic() - t0) * 1000, 3),
    }
    stop_min_helper(child, ctrl_w, status_r, stop_w)

    # B: child already dead -> write raises BrokenPipeError
    child, ctrl_w, reader, status_r, stop_w, _ = start_min_helper()
    send_frame(ctrl_w, {"type": "config"})
    reader.poll(20.0)
    child.kill()
    child.wait(timeout=5)
    t0 = time.monotonic()
    try:
        send_frame(ctrl_w, {"type": "session-start", "token": "B"})
        results["B_write_after_death"] = {"error": None}
    except BaseException as exc:  # noqa: BLE001 - the exact type is the datum
        results["B_write_after_death"] = {
            "error": type(exc).__name__,
            "errno": getattr(exc, "errno", None),
            "elapsed_ms": round((time.monotonic() - t0) * 1000, 3),
        }
    for fd in (ctrl_w, stop_w, status_r):
        try:
            os.close(fd)
        except OSError:
            pass

    # C: write lands in the buffer, child then exits -> reader sees EOF
    child, ctrl_w, reader, status_r, stop_w, _ = start_min_helper(misbehave="exit-on-session")
    send_frame(ctrl_w, {"type": "config"})
    reader.poll(20.0)
    t0 = time.monotonic()
    send_frame(ctrl_w, {"type": "session-start", "token": "C"})
    frame = reader.poll(5.0)
    results["C_exit_after_buffered_send"] = {
        "frame": frame,
        "elapsed_ms": round((time.monotonic() - t0) * 1000, 3),
    }
    stop_min_helper(child, ctrl_w, status_r, stop_w)

    # D: child takes the frame and never answers -> bounded poll times out
    child, ctrl_w, reader, status_r, stop_w, _ = start_min_helper(misbehave="silent-on-session")
    send_frame(ctrl_w, {"type": "config"})
    reader.poll(20.0)
    t0 = time.monotonic()
    send_frame(ctrl_w, {"type": "session-start", "token": "D"})
    frame = reader.poll(0.5)
    results["D_silent"] = {
        "frame": frame,
        "elapsed_ms": round((time.monotonic() - t0) * 1000, 3),
    }
    stop_min_helper(child, ctrl_w, status_r, stop_w)

    ok = (
        isinstance(results["A_normal_ack"]["frame"], dict)
        and results["A_normal_ack"]["frame"]["type"] == "session-ack"
        and results["B_write_after_death"]["error"] == "BrokenPipeError"
        and results["C_exit_after_buffered_send"]["frame"] == "EOF"
        and results["D_silent"]["frame"] is None
    )
    record(
        "session_handshake_four_interleavings",
        ok,
        "A=ack B=BrokenPipeError C=EOF D=poll-timeout",
        results,
    )


# --------------------------------------------------------------------------
# Non-blocking Lock as the single-session gate (no thread is created)
# --------------------------------------------------------------------------
def probe_nonblocking_lock_gate(rounds=200):
    """Two threads race for one saga's gate.  The winner must keep the lock
    until *both* threads have made their attempt - releasing early would let the
    loser win too and would measure nothing.  (The first version of this probe
    released after 1ms and duly reported bogus 'double entries' on the 4 vCPU
    Linux runner; the bug was in the probe, not in ``Lock``.)"""
    import threading

    violations = 0
    both_attempted = 0
    for _ in range(rounds):
        lock = threading.Lock()
        winners = []
        start_gate = threading.Barrier(2)
        attempted = threading.Barrier(2)

        def runner():
            start_gate.wait(timeout=5)
            acquired = lock.acquire(blocking=False)
            if acquired:
                winners.append(threading.get_ident())
            attempted.wait(timeout=5)
            if acquired:
                lock.release()

        threads = [threading.Thread(target=runner) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)
        both_attempted += 1
        if len(winners) != 1:
            violations += 1
    ok = violations == 0
    record(
        "nonblocking_lock_gate",
        ok,
        f"{rounds} two-thread races with the winner holding the lock across both "
        f"attempts: exactly one winner every time (violations={violations})",
        {"rounds": rounds, "violations": violations, "rounds_completed": both_attempted},
    )


# --------------------------------------------------------------------------
# Helper lifecycle: one PID across two sessions, then an explicit bounded close
# --------------------------------------------------------------------------
def probe_helper_lifecycle():
    child, ctrl_w, reader, status_r, stop_w, _ = start_min_helper()
    send_frame(ctrl_w, {"type": "config", "busy_timeout_ms": BUSY_MS})
    ready = reader.poll(30.0)
    assert isinstance(ready, dict), ready
    pid = child.pid
    try:
        pgid = os.getpgid(pid)
    except OSError:
        pgid = None

    sessions = []
    for token in ("s1", "s2"):
        send_frame(ctrl_w, {"type": "session-start", "token": token})
        ack = reader.poll(10.0)
        send_frame(ctrl_w, {"type": "session-end", "token": token})
        end = reader.poll(10.0)
        sessions.append(
            {
                "token": token,
                "ack": ack,
                "end": end,
                "pid": child.pid,
                "poll_between_sessions": child.poll(),
            }
        )

    # non-blocking sweep on a live child: poll() must return None at once
    t0 = time.monotonic()
    live_poll = child.poll()
    live_poll_ms = round((time.monotonic() - t0) * 1000, 4)

    # explicit, idempotent, bounded close(): drop every parent-held write end
    t0 = time.monotonic()
    for fd in (ctrl_w, stop_w):
        os.close(fd)
    child.wait(timeout=5.0)
    close_seconds = round(time.monotonic() - t0, 4)
    returncode = child.returncode
    second_close = child.poll()  # idempotent: same answer, no exception
    os.close(status_r)

    try:
        os.kill(pid, 0)
        kill_zero = "no-error"
    except ProcessLookupError:
        kill_zero = "ESRCH"
    except PermissionError:
        kill_zero = "EPERM"

    ok = (
        sessions[0]["pid"] == sessions[1]["pid"] == pid
        and sessions[0]["poll_between_sessions"] is None
        and sessions[1]["poll_between_sessions"] is None
        and live_poll is None
        and returncode == 0
        and second_close == 0
        and close_seconds < 1.0
        and pgid == pid
        and isinstance(sessions[0]["ack"], dict)
        and isinstance(sessions[1]["end"], dict)
    )
    record(
        "helper_lifecycle_two_sessions_then_close",
        ok,
        f"pid {pid} served both sessions (poll() None throughout), start_new_session gave "
        f"pgid=={pgid}, explicit close reaped it in {close_seconds}s with returncode "
        f"{returncode}; poll() repeats the same answer",
        {
            "pid": pid,
            "pgid": pgid,
            "sessions": sessions,
            "live_poll_returns": live_poll,
            "live_poll_cost_ms": live_poll_ms,
            "close_seconds": close_seconds,
            "returncode": returncode,
            "poll_after_reap": second_close,
            "os_kill_zero_after_reap": kill_zero,
        },
    )


# --------------------------------------------------------------------------
# SQLite: write lock held across the durable tail; SIGKILL releases it
# --------------------------------------------------------------------------
def make_outbox(db):
    connection = sqlite3.connect(db, isolation_level=None)
    connection.execute("PRAGMA journal_mode = WAL")
    connection.execute("PRAGMA synchronous = FULL")
    connection.execute(
        "CREATE TABLE IF NOT EXISTS outbox ("
        "operation_id TEXT PRIMARY KEY, status TEXT, executor_owner_token TEXT, "
        "executor_generation INTEGER, executor_heartbeat_at TEXT, "
        "executor_lease_expires_at TEXT)"
    )
    connection.execute("DELETE FROM outbox")
    connection.execute(
        "INSERT INTO outbox VALUES (?, 'pending', ?, 1, 'BASELINE', 'BASELINE')",
        ("wp9-probe-op", "wp9-probe-owner"),
    )
    connection.close()


def read_outbox(db):
    connection = sqlite3.connect(db, isolation_level=None)
    connection.row_factory = sqlite3.Row
    try:
        row = connection.execute(
            "SELECT executor_heartbeat_at, executor_lease_expires_at FROM outbox"
        ).fetchone()
        return (row[0], row[1])
    finally:
        connection.close()


def try_begin_immediate(db, busy_ms):
    connection = sqlite3.connect(db, timeout=busy_ms / 1_000, isolation_level=None)
    try:
        connection.execute(f"PRAGMA busy_timeout = {busy_ms}")
        t0 = time.monotonic()
        try:
            connection.execute("BEGIN IMMEDIATE")
        except sqlite3.OperationalError as exc:
            return {
                "ok": False,
                "elapsed_ms": round((time.monotonic() - t0) * 1000, 2),
                "errorcode": getattr(exc, "sqlite_errorcode", None),
                "type": type(exc).__name__,
            }
        connection.execute("ROLLBACK")
        return {"ok": True, "elapsed_ms": round((time.monotonic() - t0) * 1000, 2)}
    finally:
        connection.close()


def probe_sqlite_takeover(root):
    data = {}
    ok = True
    for mode, expect_committed, expect_lock_held in (
        ("after-update", False, True),
        ("after-commit", True, False),
    ):
        db = os.path.join(root, f"takeover-{mode}.sqlite3")
        for suffix in ("", "-wal", "-shm"):
            try:
                os.unlink(db + suffix)
            except OSError:
                pass
        make_outbox(db)
        ready_r, ready_w = os.pipe()
        os.set_inheritable(ready_w, True)
        child = subprocess.Popen(
            [sys.executable, "-I", HELPER_SQLITE, db, str(ready_w), str(BUSY_MS), mode],
            pass_fds=(ready_w,),
            start_new_session=True,
            close_fds=True,
            env={"PATH": "/usr/bin:/bin"},
        )
        os.close(ready_w)
        assert os.read(ready_r, 64).startswith(b"held"), "child failed to take the lock"
        os.close(ready_r)

        contended = try_begin_immediate(db, BUSY_MS)
        child.kill()
        child.wait(timeout=5)
        after_kill = try_begin_immediate(db, BUSY_MS)
        row = read_outbox(db)
        connection = sqlite3.connect(db)
        integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
        connection.close()
        committed = row == ("CHILD-WROTE", "CHILD-WROTE")
        case_ok = (
            (
                (contended["ok"] is False and contended["errorcode"] == 5)
                if expect_lock_held
                else contended["ok"] is True
            )
            and after_kill["ok"] is True
            and committed is expect_committed
            and integrity == "ok"
        )
        ok = ok and case_ok
        data[mode] = {
            "contended_while_child_alive": contended,
            "after_sigkill": after_kill,
            "row": list(row),
            "committed": committed,
            "expected_committed": expect_committed,
            "expected_write_lock_held": expect_lock_held,
            "integrity_check": integrity,
        }
    record(
        "sqlite_takeover_and_sigkill_release",
        ok,
        "a cross-process takeover attempt is SQLITE_BUSY(5) for the whole busy_timeout "
        "while the child sits between UPDATE and commit, and succeeds within a "
        "millisecond after SIGKILL; commit releases the write lock, so a child stuck in "
        "close() blocks nobody; never a half-write",
        data,
    )


# --------------------------------------------------------------------------
# (i) three-layer parent death -> EOF -> helper exits -> lock released
# --------------------------------------------------------------------------
def external_fd_view(pid):
    """Best-effort, OS-native view of another process's fds."""
    proc = f"/proc/{pid}/fd"
    if os.path.isdir(proc):
        rows = []
        for entry in sorted(os.listdir(proc), key=int):
            try:
                target = os.readlink(os.path.join(proc, entry))
                flags = None
                ino = None
                with open(f"/proc/{pid}/fdinfo/{entry}", encoding="utf-8") as handle:
                    for line in handle:
                        if line.startswith("flags:"):
                            flags = int(line.split()[1], 8)
                        elif line.startswith("ino:"):
                            ino = int(line.split()[1])
            except OSError:
                continue
            rows.append(
                {
                    "fd": int(entry),
                    "target": target,
                    "accmode": {os.O_RDONLY: "r", os.O_WRONLY: "w", os.O_RDWR: "rw"}.get(
                        None if flags is None else flags & os.O_ACCMODE, "?"
                    ),
                    "ino": ino,
                }
            )
        return {"source": "/proc", "rows": rows}
    try:
        out = subprocess.run(
            ["lsof", "-p", str(pid)], capture_output=True, text=True, timeout=20
        ).stdout
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {"source": "lsof-unavailable", "error": str(exc)}
    rows = [line for line in out.splitlines() if "PIPE" in line.upper() or "FD" in line[:40]]
    return {"source": "lsof", "rows": rows[:40]}


def probe_parent_death_eof(root):
    db = os.path.join(root, "parent-death.sqlite3")
    for suffix in ("", "-wal", "-shm"):
        try:
            os.unlink(db + suffix)
        except OSError:
            pass
    make_outbox(db)
    interval = 0.2

    report_r, report_w = os.pipe()
    live_r, live_w = os.pipe()
    for fd in (report_w, live_w):
        os.set_inheritable(fd, True)
    parent = subprocess.Popen(
        [
            sys.executable,
            "-I",
            HELPER_PARENT,
            db,
            "wp9-probe-op",
            str(interval),
            str(report_w),
            str(live_w),
            HELPER_HEARTBEAT,
            str(BUSY_MS),
        ],
        pass_fds=(report_w, live_w),
        start_new_session=True,
        close_fds=True,
        env={"PATH": "/usr/bin:/bin"},
    )
    os.close(report_w)
    os.close(live_w)  # the helper is now the sole holder of the liveness pipe

    reader = LineReader(report_r)
    report = reader.poll(30.0)
    assert isinstance(report, dict), f"no report from helper-parent: {report!r}"
    helper_pid = report["ready"]["pid"]
    stop_ino = report["ready"]["stop_ino"]
    self_fds = report["ready"]["fds"]
    same_pipe = [row for row in self_fds if row["ino"] == stop_ino]
    helper_holds_only_read_end = len(same_pipe) == 1 and same_pipe[0]["accmode"] == "r"

    # let a couple of heartbeats land
    deadline = time.monotonic() + 10.0
    ticked = False
    while time.monotonic() < deadline:
        if read_outbox(db)[0] != "BASELINE":
            ticked = True
            break
        time.sleep(0.05)

    external = external_fd_view(helper_pid)
    external_stop_rows = [
        row
        for row in external.get("rows", [])
        if isinstance(row, dict) and row.get("ino") == stop_ino
    ]
    external_only_read_end = (
        external["source"] != "/proc"
        or (len(external_stop_rows) == 1 and external_stop_rows[0]["accmode"] == "r")
    )

    os.kill(parent.pid, signal.SIGKILL)
    killed_at = time.monotonic()
    parent.wait(timeout=10)

    # pid-reuse-free liveness: the helper is the only holder of live_w
    readable, _, _ = select.select([live_r], [], [], 15.0)
    helper_exit_seconds = time.monotonic() - killed_at
    eof = bool(readable) and os.read(live_r, 4096) == b""
    os.close(live_r)

    lock_probe = try_begin_immediate(db, BUSY_MS)
    before = read_outbox(db)
    time.sleep(3 * interval)
    after = read_outbox(db)

    connection = sqlite3.connect(db)
    integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
    connection.close()

    try:
        os.kill(helper_pid, 0)
        kill_zero = "no-error"
    except ProcessLookupError:
        kill_zero = "ESRCH"
    except PermissionError:
        kill_zero = "EPERM"
    except OSError as exc:
        kill_zero = _errname(exc.errno)

    ok = (
        helper_holds_only_read_end
        and external_only_read_end
        and ticked
        and eof
        and helper_exit_seconds < 5.0
        and lock_probe["ok"]
        and before == after
        and integrity == "ok"
    )
    record(
        "parent_death_eof",
        ok,
        f"helper exited {helper_exit_seconds:.3f}s after the parent was SIGKILLed "
        f"(EOF on the liveness pipe), write lock free in {lock_probe['elapsed_ms']}ms, "
        f"no tick in {3 * interval:.1f}s afterwards",
        {
            "helper_pid": helper_pid,
            "stop_pipe_ino": stop_ino,
            "helper_self_fds_on_stop_pipe": same_pipe,
            "helper_holds_only_read_end": helper_holds_only_read_end,
            "external_fd_view_source": external["source"],
            "external_stop_pipe_rows": external_stop_rows,
            "external_fd_rows": external.get("rows", [])[:40],
            "heartbeat_ticked_before_kill": ticked,
            "helper_exit_seconds": round(helper_exit_seconds, 4),
            "eof_on_liveness_pipe": eof,
            "write_lock_after_death": lock_probe,
            "row_before": list(before),
            "row_after": list(after),
            "quiet_window_s": round(3 * interval, 3),
            "integrity_check": integrity,
            "os_kill_zero_after_exit": kill_zero,
        },
    )


# --------------------------------------------------------------------------
# V4-M2: process-exit watch that needs NO extra fd on the production helper
# --------------------------------------------------------------------------
def process_create_time(pid):
    """A pid-reuse-proof identity token for `pid`, or None.

    Linux: field 22 (starttime) of /proc/<pid>/stat, parsed after the last ')'
    so a comm containing spaces or parens cannot shift the fields.
    macOS: `ps -o lstart=` (second resolution), which is stdlib-reachable via
    subprocess; there is no pure-Python sysctl in the stdlib.
    """
    stat_path = f"/proc/{pid}/stat"
    if os.path.exists(stat_path):
        try:
            with open(stat_path, encoding="utf-8") as handle:
                raw = handle.read()
        except OSError:
            return None
        tail = raw[raw.rindex(")") + 2 :].split()
        return f"starttime:{tail[19]}"          # field 22 == index 19 after (pid, comm, state)
    try:
        out = subprocess.run(
            ["ps", "-o", "lstart=", "-p", str(pid)],
            capture_output=True, text=True, timeout=20,
        ).stdout.strip()
    except (OSError, subprocess.TimeoutExpired):
        return None
    return f"lstart:{out}" if out else None


def open_exit_watch(pid):
    """Return (kind, waiter) where waiter.wait(timeout) -> True once pid exits.

    Neither mechanism needs the process to be our child, and neither needs an
    extra fd handed to the watched process - which is the whole point (V4-M2).
    """
    if hasattr(os, "pidfd_open"):
        fd = os.pidfd_open(pid)                 # raises ProcessLookupError if already gone

        class _PidfdWaiter:
            def wait(self, timeout):
                readable, _, _ = select.select([fd], [], [], timeout)
                return bool(readable)

            def close(self):
                os.close(fd)

        return "pidfd", _PidfdWaiter()
    if hasattr(select, "kqueue"):
        queue = select.kqueue()
        event = select.kevent(
            pid,
            filter=select.KQ_FILTER_PROC,
            flags=select.KQ_EV_ADD | select.KQ_EV_ENABLE,
            fflags=select.KQ_NOTE_EXIT,
        )
        queue.control([event], 0, 0)            # raises ProcessLookupError if already gone

        class _KqueueWaiter:
            def wait(self, timeout):
                return bool(queue.control(None, 1, timeout))

            def close(self):
                queue.close()

        return "kqueue", _KqueueWaiter()
    raise RuntimeError("no exit-watch mechanism on this platform")


def probe_parent_death_exit_watch(root):
    """T9 as V4-M2 rewrites it: the helper keeps the frozen three-fd shape and
    the supervisor learns of its exit through pidfd / kqueue, identified by
    (pid, create_time)."""
    db = os.path.join(root, "parent-death-v2.sqlite3")
    for suffix in ("", "-wal", "-shm"):
        try:
            os.unlink(db + suffix)
        except OSError:
            pass
    make_outbox(db)
    interval = 0.2

    report_r, report_w = os.pipe()
    os.set_inheritable(report_w, True)
    parent = subprocess.Popen(
        [
            sys.executable, "-I", HELPER_PARENT_V2, db, "wp9-probe-op", str(interval),
            str(report_w), HELPER_PRODSHAPE, str(BUSY_MS),
        ],
        pass_fds=(report_w,),
        start_new_session=True,
        close_fds=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env={"PATH": "/usr/bin:/bin"},
    )
    os.close(report_w)
    report = LineReader(report_r).poll(30.0)
    assert isinstance(report, dict), f"no report from helper-parent: {report!r}"
    helper_pid = report["ready"]["pid"]
    ready_frame_keys = sorted(report["ready"])

    created = process_create_time(helper_pid)
    kind, waiter = open_exit_watch(helper_pid)

    # a watch on a pid that cannot exist must fail loudly, not silently pass
    bogus_rejected = None
    try:
        open_exit_watch(2 ** 22 - 1)
        bogus_rejected = "no-error"
    except ProcessLookupError:
        bogus_rejected = "ProcessLookupError"
    except (OSError, RuntimeError) as exc:
        bogus_rejected = type(exc).__name__

    deadline = time.monotonic() + 10.0
    ticked = False
    while time.monotonic() < deadline:
        if read_outbox(db)[0] != "BASELINE":
            ticked = True
            break
        time.sleep(0.05)

    external = external_fd_view(helper_pid)
    linux_fd_rows = [row for row in external.get("rows", []) if isinstance(row, dict)]
    read_only_pipes = [row for row in linux_fd_rows if row.get("accmode") == "r" and
                       str(row.get("target", "")).startswith("pipe:")]
    created_before_kill = process_create_time(helper_pid)

    os.kill(parent.pid, signal.SIGKILL)
    killed_at = time.monotonic()
    parent.wait(timeout=10)

    fired = waiter.wait(15.0)
    helper_exit_seconds = time.monotonic() - killed_at
    waiter.close()
    os.close(report_r)

    lock_probe = try_begin_immediate(db, BUSY_MS)
    before = read_outbox(db)
    time.sleep(3 * interval)
    after = read_outbox(db)
    connection = sqlite3.connect(db)
    integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
    connection.close()

    ok = (
        created is not None
        and created == created_before_kill
        and bogus_rejected in {"ProcessLookupError", "OSError", "PermissionError"}
        and ticked
        and fired
        and helper_exit_seconds < 5.0
        and lock_probe["ok"]
        and before == after
        and integrity == "ok"
        and ready_frame_keys == ["pid", "protocol", "t"]     # production frame shape, no fd list
    )
    record(
        "parent_death_exit_watch",
        ok,
        f"three-fd production-shaped helper; exit observed via {kind} in "
        f"{helper_exit_seconds:.4f}s after the parent was SIGKILLed; identity "
        f"{created!r} unchanged; write lock free in {lock_probe['elapsed_ms']}ms; "
        f"no tick in {3 * interval:.1f}s afterwards",
        {
            "watch_kind": kind,
            "helper_pid": helper_pid,
            "create_time": created,
            "create_time_before_kill": created_before_kill,
            "bogus_pid_watch": bogus_rejected,
            "ready_frame_keys": ready_frame_keys,
            "heartbeat_ticked_before_kill": ticked,
            "helper_exit_seconds": round(helper_exit_seconds, 4),
            "exit_watch_fired": fired,
            "write_lock_after_death": lock_probe,
            "row_before": list(before),
            "row_after": list(after),
            "quiet_window_s": round(3 * interval, 3),
            "integrity_check": integrity,
            "external_fd_view_source": external["source"],
            "external_read_only_pipe_fds": read_only_pipes,
            "external_fd_rows": linux_fd_rows or external.get("rows", [])[:40],
        },
    )


# --------------------------------------------------------------------------
# (ii) AF_UNIX non-blocking connect errno set
# --------------------------------------------------------------------------
def probe_afunix_connect(root):
    path = os.path.join(root, "probe.sock")
    try:
        os.unlink(path)
    except OSError:
        pass
    data = {}

    missing = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    missing.setblocking(False)
    data["no_listener"] = {"errno": _errname(missing.connect_ex(path))}
    missing.close()

    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    listener.bind(path)
    listener.listen(1)

    clients = []
    attempts = []
    for index in range(12):
        client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        client.setblocking(False)
        code = client.connect_ex(path)
        writable = bool(select.select([], [client], [], 0.05)[1])
        attempts.append(
            {"index": index, "errno": _errname(code), "select_writable": writable}
        )
        clients.append(client)
    data["backlog_fill"] = attempts
    data["distinct_backlog_errnos"] = sorted({row["errno"] for row in attempts})

    connected = clients[0]
    data["reconnect_on_connected"] = {"errno": _errname(connected.connect_ex(path))}
    # retrying connect() on a socket that got EAGAIN is the documented AF_UNIX move
    retries = None
    blocked = [row["index"] for row in attempts if row["errno"] in {"EAGAIN", "EWOULDBLOCK"}]
    if blocked:
        victim = clients[blocked[0]]
        server_side = []
        deadline = time.monotonic() + 2.0
        retries = {"attempts": 0, "final": None}
        while time.monotonic() < deadline:
            code = victim.connect_ex(path)
            retries["attempts"] += 1
            retries["final"] = _errname(code)
            if code in (0, errno.EISCONN):
                break
            try:
                server_side.append(listener.accept()[0])
            except OSError:
                pass
            time.sleep(0.005)
        for sock in server_side:
            sock.close()
    data["retry_connect_after_eagain"] = retries

    for client in clients:
        client.close()
    listener.close()
    os.unlink(path)

    ok = data["no_listener"]["errno"] in {"ENOENT", "ECONNREFUSED"}
    record(
        "afunix_nonblocking_connect",
        ok,
        f"no-listener={data['no_listener']['errno']}, backlog errnos="
        f"{data['distinct_backlog_errnos']}, reconnect-on-connected="
        f"{data['reconnect_on_connected']['errno']}",
        data,
    )


# --------------------------------------------------------------------------
# (iii) AF_UNIX non-blocking send when the peer receive buffer is full
# --------------------------------------------------------------------------
def probe_afunix_send():
    left, right = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    left.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 2048)
    right.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 2048)
    left.setblocking(False)
    chunk = b"x" * 4096
    total = 0
    sends = []
    outcome = None
    for _ in range(4096):
        try:
            sent = left.send(chunk)
        except BlockingIOError as exc:
            outcome = {"kind": "BlockingIOError", "errno": _errname(exc.errno)}
            break
        except OSError as exc:
            outcome = {"kind": type(exc).__name__, "errno": _errname(exc.errno)}
            break
        if sent == 0:
            outcome = {"kind": "returned-zero"}
            break
        total += sent
        sends.append(sent)
    else:
        outcome = {"kind": "never-blocked"}
    data = {
        "sndbuf": left.getsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF),
        "rcvbuf": right.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF),
        "bytes_before_block": total,
        "send_calls": len(sends),
        "last_send_partial": bool(sends) and sends[-1] != len(chunk),
        "outcome": outcome,
    }
    left.close()
    right.close()
    ok = outcome["kind"] in {"BlockingIOError"} and total > 0
    record(
        "afunix_nonblocking_send_full_peer",
        ok,
        f"{total} bytes accepted in {len(sends)} send() calls, then {outcome['kind']}"
        f" ({outcome.get('errno')}); send() never returned 0",
        data,
    )


# --------------------------------------------------------------------------
# (vii) `python -I -m` under an empty environment
# --------------------------------------------------------------------------
def probe_isolated_module_env():
    script = (
        "import json,sys,os,threading;"
        "print(json.dumps({"
        "'isolated': bool(sys.flags.isolated),"
        "'no_user_site': bool(sys.flags.no_user_site),"
        "'safe_path': bool(getattr(sys.flags,'safe_path',0)),"
        "'in_venv': sys.prefix != sys.base_prefix,"
        "'prefix': sys.prefix,"
        "'env_keys': sorted(os.environ),"
        "'threads': [t.name for t in threading.enumerate()],"
        "'path_head': sys.path[:2]}))"
    )
    empty = subprocess.run(
        [sys.executable, "-I", "-c", script], capture_output=True, text=True, env={}, timeout=60
    )
    module = subprocess.run(
        [sys.executable, "-I", "-m", "json.tool"],
        input='{"a":1}',
        capture_output=True,
        text=True,
        env={},
        timeout=60,
    )
    payload = json.loads(empty.stdout) if empty.returncode == 0 else {"stderr": empty.stderr}
    observed = payload.get("env_keys", [])
    # ``env={}`` is not literally empty: libc / CoreFoundation add locale keys.
    # What matters for the design is that no PYTHON* / rquant key survives.
    leaked = [
        key
        for key in observed
        if key.startswith("PYTHON") or key.startswith("RQUANT") or key in {"HOME", "PATH", "TMPDIR"}
    ]
    ok = (
        empty.returncode == 0
        and module.returncode == 0
        and not leaked
        and payload.get("isolated") is True
        and payload.get("threads") == ["MainThread"]
    )
    record(
        "isolated_module_empty_env",
        ok,
        f"python -I under env={{}} runs (rc={empty.returncode}); -I -m json.tool rc="
        f"{module.returncode}; in_venv={payload.get('in_venv')}; surviving env keys="
        f"{observed}; leaked={leaked}",
        {
            "minus_I_c": payload,
            "minus_I_m_returncode": module.returncode,
            "surviving_env_keys": observed,
            "leaked_sensitive_keys": leaked,
        },
    )


# --------------------------------------------------------------------------
# Config travels over the control pipe, never argv (and never the environment)
# --------------------------------------------------------------------------
def probe_no_argv_or_env_leak():
    secret = "WP9-PROBE-OWNER-TOKEN-8f2c1d"
    child, ctrl_w, reader, status_r, stop_w, _ = start_min_helper()
    send_frame(
        ctrl_w,
        {
            "type": "config",
            "db": "/tmp/x.sqlite3",
            "saga_id": "saga-1",
            "operation_id": "op-1",
            "owner_token": secret,
            "owner_generation": 1,
            "lease_seconds": 30.0,
            "interval_seconds": 10.0,
            "busy_timeout_ms": BUSY_MS,
        },
    )
    ready = reader.poll(20.0)
    assert isinstance(ready, dict), ready
    views = {}
    try:
        views["ps"] = subprocess.run(
            ["ps", "-o", "args=", "-p", str(child.pid)],
            capture_output=True,
            text=True,
            timeout=20,
        ).stdout.strip()
    except (OSError, subprocess.TimeoutExpired) as exc:
        views["ps"] = f"unavailable: {exc}"
    for name, path in (
        ("cmdline", f"/proc/{child.pid}/cmdline"),
        ("environ", f"/proc/{child.pid}/environ"),
    ):
        try:
            with open(path, "rb") as handle:
                views[name] = handle.read().decode("utf-8", "replace").replace("\x00", " ")
        except OSError:
            views[name] = "unavailable (no /proc)"
    stop_min_helper(child, ctrl_w, status_r, stop_w)
    leaked_in = [name for name, text in views.items() if secret in text]
    ok = not leaked_in and ready.get("config_keys") and "owner_token" in ready["config_keys"]
    record(
        "no_argv_or_env_leak",
        ok,
        "the owner token reached the helper over the control pipe and appears in "
        f"none of {sorted(views)}",
        {"views": views, "leaked_in": leaked_in, "config_keys_seen_by_child": ready.get("config_keys")},
    )


# --------------------------------------------------------------------------
# Direct CI-cost measurement: one helper per saga instance, 67 saga instances
# --------------------------------------------------------------------------
def probe_ci_increment_model(instances=67, invokes_per_instance=6):
    started = time.monotonic()
    per_instance = []
    for _ in range(instances):
        t0 = time.monotonic()
        child, ctrl_w, reader, status_r, stop_w, _ = start_min_helper()
        send_frame(ctrl_w, {"type": "config", "busy_timeout_ms": BUSY_MS})
        assert isinstance(reader.poll(30.0), dict)
        for index in range(invokes_per_instance):
            send_frame(ctrl_w, {"type": "session-start", "token": f"s{index}"})
            assert isinstance(reader.poll(30.0), dict)
            send_frame(ctrl_w, {"type": "session-end", "token": f"s{index}"})
            assert isinstance(reader.poll(30.0), dict)
        stop_min_helper(child, ctrl_w, status_r, stop_w)
        per_instance.append(time.monotonic() - t0)
    total = time.monotonic() - started
    record(
        "ci_increment_model",
        True,
        f"{instances} helpers x {invokes_per_instance} sessions = {total:.2f}s wall "
        f"(per instance p50 {summarise(per_instance)['p50_ms']}ms)",
        {
            "instances": instances,
            "invokes_per_instance": invokes_per_instance,
            "total_seconds": round(total, 3),
            "per_instance": summarise(per_instance),
        },
    )


def main():
    print(
        "WP9-V4-PROBE-SUITE "
        + json.dumps(
            {
                "python": platform.python_version(),
                "implementation": platform.python_implementation(),
                "platform": platform.platform(),
                "machine": platform.machine(),
                "sqlite": sqlite3.sqlite_version,
                "executable": sys.executable,
                "in_venv": sys.prefix != sys.base_prefix,
                "cpu_count": os.cpu_count(),
            },
            sort_keys=True,
        ),
        flush=True,
    )
    with tempfile.TemporaryDirectory(prefix="wp9v4-") as root:
        probe_mp_exit_function_hang()
        probe_popen_startup_cost()
        probe_popen_escalation_chain()
        probe_session_handshake()
        probe_nonblocking_lock_gate()
        probe_helper_lifecycle()
        probe_sqlite_takeover(root)
        probe_parent_death_eof(root)
        probe_parent_death_exit_watch(root)
        probe_afunix_connect(root)
        probe_afunix_send()
        probe_isolated_module_env()
        probe_no_argv_or_env_leak()
        probe_ci_increment_model()
    failed = [name for name, ok in RESULTS if not ok]
    print(f"SUMMARY total={len(RESULTS)} failed={len(failed)} {failed}", flush=True)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
