"""WP9 v4 probe helper: a child that ignores SIGTERM, used to measure the
``wait(T1) -> terminate -> wait(T2) -> kill -> wait(T3)`` escalation chain and
to show that ``multiprocessing``'s atexit join never returns for such a child.

``python -I helper_sigterm_ignore.py READY_W [HONOR_SIGTERM]``
"""

import os
import signal
import sys
import time


def main():
    ready_w = int(sys.argv[1])
    honor = len(sys.argv) > 2 and sys.argv[2] == "honor"
    if not honor:
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    os.write(ready_w, b"ready\n")
    # Bounded so a probe crash cannot leave the machine littered.
    deadline = time.monotonic() + 120.0
    while time.monotonic() < deadline:
        time.sleep(0.05)
    return 0


if __name__ == "__main__":
    sys.exit(main())
