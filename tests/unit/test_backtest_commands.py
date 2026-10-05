from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from rquant.portfolio_backtest_commands import ExportPortfolioBacktestZip, SubmitPortfolioBacktest
from rquant.runtime_contracts import canonical_sha256
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


@pytest.mark.parametrize("kind", ["submit", "zip"])
def test_pb_final_01_unavailable_backend_keeps_original_effect_recoverable(
    tmp_path: Path, kind: str
) -> None:
    from rquant.page_control import PageControlConsumer, PageControlOutbox, PageControlService

    now = datetime(2026, 10, 5, tzinfo=UTC)
    command = (
        SubmitPortfolioBacktest(
            command_id=str(UUID(int=1)), requested_at=now, actor_id="researcher", config=config()
        )
        if kind == "submit"
        else ExportPortfolioBacktestZip(
            command_id=str(UUID(int=2)),
            requested_at=now,
            actor_id="researcher",
            job_id=UUID(int=3),
            result_hash="1" * 64,
        )
    )
    original_hash = canonical_sha256(command)
    marker = {"command_hash": original_hash, "kind": kind}
    expected_result = {"result": "submitted" if kind == "submit" else "exported", **marker}
    calls: list[str] = []

    class Backend:
        completed = False

        def freeze(self, original):
            assert canonical_sha256(original) == original_hash
            calls.append("freeze")
            return marker

        def submit(self, original, frozen_marker):
            assert canonical_sha256(original) == original_hash and frozen_marker == marker
            calls.append("publish")
            self.completed = True
            raise KeyboardInterrupt("original effect completed; receipt not recorded")

        def recover(self, original, frozen_marker):
            assert canonical_sha256(original) == original_hash and frozen_marker == marker
            calls.append("recover")
            return expected_result if self.completed else None

    backend = Backend()
    outbox = PageControlOutbox(tmp_path / "journal.sqlite3")

    def service(at: datetime, configured_backend: Backend | None) -> PageControlService:
        return PageControlService(
            outbox=outbox,
            consumer=PageControlConsumer(
                outbox=outbox,
                data_dir=tmp_path / "data",
                log_dir=tmp_path / "logs",
                portfolio_backend=configured_backend,
                clock=lambda: at,
                lease_seconds=1,
            ),
        )

    with pytest.raises(KeyboardInterrupt):
        service(now, backend).submit(command)
    started = outbox.effect(command.command_id)
    assert started is not None and started.status.value == "started" and started.result == marker
    unavailable = service(now + timedelta(seconds=2), None).submit(command)
    assert unavailable.status.value == "pending"
    assert unavailable.result is None and unavailable.completed_at is None
    retained = outbox.effect(command.command_id)
    assert retained is not None
    assert retained.model_dump(exclude={"owner_id", "claim_token"}) == started.model_dump(
        exclude={"owner_id", "claim_token"}
    )
    restored = service(now + timedelta(seconds=4), backend)
    receipt = restored.submit(command)
    assert receipt.status.value == "succeeded" and receipt.result == expected_result
    assert calls == ["freeze", "publish", "recover"]
    assert restored.submit(command) == receipt
    assert calls == ["freeze", "publish", "recover"]
    for altered in (
        command.model_copy(update={"actor_id": "another-researcher"}),
        command.model_copy(update={"requested_at": now + timedelta(seconds=1)}),
    ):
        with pytest.raises(ValueError, match="conflict|different"):
            restored.submit(altered)


def test_pb_final_01_missing_backend_before_freeze_cannot_publish(tmp_path: Path) -> None:
    from rquant.page_control import PageControlConsumer, PageControlOutbox, PageControlService

    now = datetime(2026, 10, 5, tzinfo=UTC)
    command = SubmitPortfolioBacktest(
        command_id=str(uuid4()), requested_at=now, actor_id="researcher", config=config()
    )
    outbox = PageControlOutbox(tmp_path / "journal.sqlite3")
    service = PageControlService(
        outbox=outbox,
        consumer=PageControlConsumer(
            outbox=outbox, data_dir=tmp_path / "data", log_dir=tmp_path / "logs", clock=lambda: now
        ),
    )
    assert service.submit(command).status.value == "failed"
    effect = outbox.effect(command.command_id)
    assert effect is not None and effect.result is None
