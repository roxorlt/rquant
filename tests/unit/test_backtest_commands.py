from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest

from rquant.portfolio_backtest_commands import SubmitPortfolioBacktest
from tests.unit.test_backtest_platform import config


def test_pb02_original_page_control_freezes_and_restores_portfolio_effect(tmp_path: Path) -> None:
    from rquant.page_control import PageControlConsumer, PageControlOutbox, PageControlService

    now = datetime(2026, 10, 5, tzinfo=UTC)
    command = SubmitPortfolioBacktest(
        command_id=str(uuid4()), requested_at=now, actor_id="researcher", config=config()
    )
    calls: list[str] = []

    class Backend:
        completed = False

        def freeze(self, cmd):
            calls.append("freeze")
            return {"original": cmd.config.config_hash}

        def submit(self, cmd, marker):
            assert marker == {"original": command.config.config_hash}
            calls.append("submit")
            self.completed = True
            raise KeyboardInterrupt("after original task publication")

        def recover(self, cmd, marker):
            assert marker == {"original": command.config.config_hash}
            calls.append("recover")
            return (
                {"result": "submitted", "original": marker["original"]} if self.completed else None
            )

    backend = Backend()
    outbox = PageControlOutbox(tmp_path / "page-control.sqlite3")
    consumer = PageControlConsumer(
        outbox=outbox,
        data_dir=tmp_path / "data",
        log_dir=tmp_path / "logs",
        portfolio_backend=backend,
        clock=lambda: now,
        lease_seconds=1,
    )
    service = PageControlService(outbox=outbox, consumer=consumer)
    with pytest.raises(KeyboardInterrupt):
        service.submit(command)
    restarted = PageControlService(
        outbox=outbox,
        consumer=PageControlConsumer(
            outbox=outbox,
            data_dir=tmp_path / "data",
            log_dir=tmp_path / "logs",
            portfolio_backend=backend,
            clock=lambda: now + timedelta(seconds=2),
            lease_seconds=1,
        ),
    )
    receipt = restarted.submit(command)
    assert receipt.status.value == "succeeded"
    assert receipt.result["original"] == command.config.config_hash
    assert calls == ["freeze", "submit", "recover"]
    assert restarted.submit(command) == receipt
    changed = command.model_copy(update={"actor_id": "someone"})
    with pytest.raises(ValueError, match="conflict|different"):
        restarted.submit(changed)
