#!/usr/bin/env bash
# Explicit cloud operator step: derive a hash-bound summary from append-only samples.

set -Eeuo pipefail

readonly PATH=/usr/sbin:/usr/bin:/sbin:/bin
export PATH
unset CDPATH PYTHONHOME PYTHONPATH

exec /home/lighthouse/rquant/.venv/bin/python -I -m rquant.workload_evidence summarize \
    --input /var/lib/rquant/workload-isolation/samples.jsonl \
    --output /var/lib/rquant/workload-isolation/high-water.json
