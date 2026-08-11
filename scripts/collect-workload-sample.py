#!/home/lighthouse/rquant/.venv/bin/python
"""Append one read-only systemd/cgroup workload sample."""

from __future__ import annotations

from rquant.workload_evidence import main

if __name__ == "__main__":
    raise SystemExit(
        main(
            [
                "sample",
                "--output",
                "/var/lib/rquant/workload-isolation/samples.jsonl",
            ]
        )
    )
