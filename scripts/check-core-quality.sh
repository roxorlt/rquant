#!/usr/bin/env bash
set -euo pipefail

PYTHON_BIN="${RQUANT_PYTHON:-.venv/bin/python}"

"${PYTHON_BIN}" -m ruff check \
  src/rquant/research_manifest.py \
  src/rquant/dashboard/strategy_lab_runs.py \
  src/rquant/dashboard/strategy_lab_worker.py \
  src/rquant/minute_replay.py \
  src/rquant/paper.py \
  src/rquant/monitor.py \
  src/rquant/growth_board_surge_strategy.py \
  tests/unit/test_research_manifest.py \
  tests/unit/test_strategy_lab_runs.py \
  tests/unit/test_minute_replay.py \
  tests/unit/test_paper.py \
  tests/unit/test_monitor.py \
  tests/unit/test_growth_board_surge_strategy.py
