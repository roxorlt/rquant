"""Compatibility Streamlit entrypoint for the durable Strategy Lab Job Center."""

from __future__ import annotations

from rquant.dashboard.lab.app import run_strategy_lab_app

if __name__ == "__main__":
    run_strategy_lab_app()
