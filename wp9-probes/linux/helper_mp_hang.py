"""WP9 v4 probe helper for RQ-WP9-DESIGN-P1-01.

Mode ``mp-daemon`` / ``mp-nondaemon``: start a ``multiprocessing.Process``
(spawn context) that ignores SIGTERM, **wait until it confirms the handler is
installed**, then return from ``main`` normally.  CPython's
``multiprocessing.util._exit_function`` terminates daemon children and then
joins every ``active_children()`` entry with **no timeout**, so the interpreter
never exits.

Mode ``popen``: the same child shape started with ``subprocess.Popen``
(``start_new_session=True``, output to DEVNULL).  Popen registers nothing with
``multiprocessing``'s atexit, so the parent exits at once.

``python helper_mp_hang.py <mode>``   (no -I: spawn needs the real sys.path)
"""

import multiprocessing
import os
import signal
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))


def stubborn_child(ready):
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    ready.set()
    deadline = time.monotonic() + 120.0
    while time.monotonic() < deadline:
        time.sleep(0.05)


def main():
    mode = sys.argv[1]
    if mode.startswith("mp-"):
        ctx = multiprocessing.get_context("spawn")
        ready = ctx.Event()
        process = ctx.Process(
            target=stubborn_child, args=(ready,), daemon=(mode == "mp-daemon")
        )
        process.start()
        if not ready.wait(30.0):
            raise SystemExit("child never confirmed its SIGTERM handler")
        print(f"MODE {mode} child_pid={process.pid} returning-from-main", flush=True)
        return 0
    if mode == "popen":
        read_fd, write_fd = os.pipe()
        os.set_inheritable(write_fd, True)
        child = subprocess.Popen(
            [
                sys.executable,
                "-I",
                os.path.join(HERE, "helper_sigterm_ignore.py"),
                str(write_fd),
            ],
            pass_fds=(write_fd,),
            start_new_session=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            close_fds=True,
        )
        os.close(write_fd)
        os.read(read_fd, 64)
        os.close(read_fd)
        print(f"MODE popen child_pid={child.pid} returning-from-main", flush=True)
        return 0
    raise SystemExit(f"unknown mode {mode!r}")


if __name__ == "__main__":
    sys.exit(main())
