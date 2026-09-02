"""WP9 v4.1 probe helper: the middle layer of the T9 three-layer test, using the
**production** helper shape (three fds, no liveness pipe).

    python -I helper_parent_v2.py DB OP_ID INTERVAL REPORT_W HELPER BUSY_MS
"""

import json
import os
import subprocess
import sys
import time


def main():
    db, operation_id, interval, report_w, helper, busy_ms = sys.argv[1:7]
    report_w = int(report_w)

    ctrl_r, ctrl_w = os.pipe()
    status_r, status_w = os.pipe()
    stop_r, stop_w = os.pipe()
    for fd in (ctrl_r, status_w, stop_r):
        os.set_inheritable(fd, True)

    child = subprocess.Popen(
        [
            sys.executable, "-I", helper,
            "--control-fd", str(ctrl_r),
            "--status-fd", str(status_w),
            "--stop-fd", str(stop_r),
        ],
        pass_fds=(ctrl_r, status_w, stop_r),
        start_new_session=True,
        close_fds=True,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env={"PATH": "/usr/bin:/bin", "LC_ALL": "C"},
    )
    os.close(ctrl_r)
    os.close(status_w)
    os.close(stop_r)

    os.write(
        ctrl_w,
        (
            json.dumps(
                {
                    "t": "config", "protocol": 1, "db": db,
                    "operation_id": operation_id, "busy_timeout_ms": int(busy_ms),
                    "interval_seconds": float(interval),
                }
            )
            + "\n"
        ).encode("utf-8"),
    )
    buf = b""
    while b"\n" not in buf:
        chunk = os.read(status_r, 65536)
        if not chunk:
            raise SystemExit("helper died before ready")
        buf += chunk
    ready = json.loads(buf.split(b"\n", 1)[0])

    os.write(
        report_w,
        (json.dumps({"helper_parent_pid": os.getpid(), "ready": ready}) + "\n").encode("utf-8"),
    )
    deadline = time.monotonic() + 120.0
    while time.monotonic() < deadline:
        time.sleep(0.05)
    return 0


if __name__ == "__main__":
    sys.exit(main())
