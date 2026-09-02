"""WP9 v4 probe helper: the middle layer of the P1-05 three-layer parent-death
test.  It plays the role of the saga process: it owns the stop pipe's write end
and blocks forever, so the supervisor can SIGKILL it without killing pytest.

``python -I helper_parent.py DB OP_ID INTERVAL REPORT_W LIVE_W HELPER BUSY_MS``
"""

import json
import os
import subprocess
import sys
import time


def main():
    db = sys.argv[1]
    operation_id = sys.argv[2]
    interval = sys.argv[3]
    report_w = int(sys.argv[4])
    live_w = int(sys.argv[5])
    helper = sys.argv[6]
    busy_ms = sys.argv[7]

    stop_r, stop_w = os.pipe()
    status_r, status_w = os.pipe()
    for fd in (stop_r, status_w, live_w):
        os.set_inheritable(fd, True)

    child = subprocess.Popen(
        [
            sys.executable,
            "-I",
            helper,
            db,
            operation_id,
            interval,
            str(status_w),
            str(stop_r),
            str(live_w),
            busy_ms,
        ],
        pass_fds=(status_w, stop_r, live_w),
        start_new_session=True,
        close_fds=True,
        env={"PATH": "/usr/bin:/bin"},
    )
    # Only the helper may hold these, or the supervisor's EOF proofs are void.
    os.close(stop_r)
    os.close(status_w)
    os.close(live_w)

    buf = b""
    while b"\n" not in buf:
        chunk = os.read(status_r, 65536)
        if not chunk:
            raise SystemExit("helper died before ready")
        buf += chunk
    ready = json.loads(buf.split(b"\n", 1)[0])

    os.write(
        report_w,
        (
            json.dumps(
                {
                    "helper_parent_pid": os.getpid(),
                    "stop_w_fd": stop_w,
                    "ready": ready,
                }
            )
            + "\n"
        ).encode("utf-8"),
    )
    deadline = time.monotonic() + 120.0
    while time.monotonic() < deadline:
        time.sleep(0.05)
    return 0


if __name__ == "__main__":
    sys.exit(main())
