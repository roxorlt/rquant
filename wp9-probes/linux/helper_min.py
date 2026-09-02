"""WP9 v4 probe helper: the stdlib-only shape the design proposes for the
heartbeat helper.  Started as ``python -I helper_min.py CTRL_R STATUS_W STOP_R
[MISBEHAVE]``.

Protocol: newline-delimited JSON frames.  Reads one ``config`` frame from
CTRL_R, answers ``ready`` on STATUS_W, then serves ``session-start`` /
``session-end`` and dies on EOF of STOP_R (or CTRL_R).  No argv secrets.
"""

import json
import os
import select
import signal
import sys

BUF = b""


def send(fd, frame):
    os.write(fd, (json.dumps(frame, separators=(",", ":")) + "\n").encode("utf-8"))


def read_frame(fd):
    """Return a dict, or None on EOF."""
    global BUF
    while b"\n" not in BUF:
        chunk = os.read(fd, 65536)
        if not chunk:
            return None
        BUF += chunk
    line, _, BUF = BUF.partition(b"\n")
    return json.loads(line)


def main():
    ctrl_r = int(sys.argv[1])
    status_w = int(sys.argv[2])
    stop_r = int(sys.argv[3])
    misbehave = sys.argv[4] if len(sys.argv) > 4 else "none"
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    config = read_frame(ctrl_r)
    if config is None:
        return 0
    send(status_w, {"type": "ready", "pid": os.getpid(), "config_keys": sorted(config)})

    watch = [ctrl_r] + ([stop_r] if stop_r >= 0 else [])
    session = None
    ticks = 0
    while True:
        ready, _, _ = select.select(watch, [], [], 1.0)
        if stop_r >= 0 and stop_r in ready:
            if os.read(stop_r, 4096) == b"":
                return 0
        if ctrl_r in ready:
            frame = read_frame(ctrl_r)
            if frame is None:
                return 0
            kind = frame.get("type")
            if kind == "session-start":
                if misbehave == "exit-on-session":
                    return 0
                if misbehave == "silent-on-session":
                    session = frame.get("token")
                    continue
                session = frame.get("token")
                ticks = 0
                send(status_w, {"type": "session-ack", "token": session})
            elif kind == "session-end":
                send(
                    status_w,
                    {
                        "type": "end-ack",
                        "token": frame.get("token"),
                        "ticks": ticks,
                        "last_stage": "idle",
                        "last_outcome": "ok" if session == frame.get("token") else "unknown-token",
                    },
                )
                session = None
            elif kind == "shutdown":
                return 0


if __name__ == "__main__":
    sys.exit(main())
