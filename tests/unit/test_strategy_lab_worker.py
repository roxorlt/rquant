"""Strategy Lab 后台任务 runner 测试。"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

CST = timezone(timedelta(hours=8))


def _dead_pid() -> int:
    """拿一个确定已退出的 pid（wait 已回收，kill 0 必然 ProcessLookupError）。"""
    proc = subprocess.Popen(["/bin/sleep", "0"])
    proc.wait()
    return proc.pid


def _fake_run(title: str = "假优化") -> Any:
    from rquant.dashboard.strategy_lab_runs import build_strategy_lab_run

    return build_strategy_lab_run(
        run_type="n_shape_optimize",
        title=title,
        params={"开始日期": "2026-06-01"},
        metrics={"策略排行数": 1},
        tables={"策略排行": pd.DataFrame({"candidate_id": ["a"], "robust_score": [1.0]})},
    )


class TestRunStatusFile:
    def test_write_and_read_roundtrip(self, tmp_path: Path) -> None:
        from rquant.dashboard.strategy_lab_worker import (
            LabRunStatus,
            read_run_status,
            write_run_status,
        )

        status = LabRunStatus(
            run_id="20260701-090000-n_shape_optimize-bg-abc123",
            run_type="n_shape_optimize",
            state="running",
            pid=12345,
            started_at=datetime(2026, 7, 1, 9, 0, tzinfo=CST),
        )
        path = write_run_status(status, base_dir=tmp_path)
        assert path.name == f"{status.run_id}.status.json"
        assert path.parent == tmp_path / "strategy_lab_runs"

        loaded = read_run_status(status.run_id, base_dir=tmp_path)
        assert loaded is not None
        assert loaded.run_id == status.run_id
        assert loaded.state == "running"
        assert loaded.pid == 12345

    def test_read_missing_returns_none(self, tmp_path: Path) -> None:
        from rquant.dashboard.strategy_lab_worker import read_run_status

        assert read_run_status("no-such-run", base_dir=tmp_path) is None

    def test_list_marks_dead_running_as_error(self, tmp_path: Path) -> None:
        from rquant.dashboard.strategy_lab_worker import (
            LabRunStatus,
            list_run_statuses,
            write_run_status,
        )

        write_run_status(
            LabRunStatus(
                run_id="run-dead",
                run_type="n_shape_optimize",
                state="running",
                pid=_dead_pid(),
            ),
            base_dir=tmp_path,
        )
        write_run_status(
            LabRunStatus(run_id="run-done", run_type="n_shape_optimize", state="done"),
            base_dir=tmp_path,
        )

        statuses = {item["run_id"]: item for item in list_run_statuses(base_dir=tmp_path)}
        assert statuses["run-dead"]["state"] == "error"
        assert "进程已不存在" in statuses["run-dead"]["error"]
        assert statuses["run-done"]["state"] == "done"

    def test_list_empty_dir(self, tmp_path: Path) -> None:
        from rquant.dashboard.strategy_lab_worker import list_run_statuses

        assert list_run_statuses(base_dir=tmp_path) == []

    def test_write_is_atomic_no_tmp_leftover(self, tmp_path: Path) -> None:
        """B1: 写状态走临时文件 + os.replace，目录里不残留 .tmp。"""
        from rquant.dashboard.strategy_lab_worker import LabRunStatus, write_run_status

        path = write_run_status(
            LabRunStatus(run_id="run-atomic", run_type="n_shape_optimize", state="running"),
            base_dir=tmp_path,
        )
        assert path.exists()
        assert list(path.parent.glob("*.tmp")) == []

    def test_read_corrupted_status_returns_none(self, tmp_path: Path) -> None:
        """B1 读侧容错：半截 JSON 不崩，按不存在处理。"""
        from rquant.dashboard.strategy_lab_worker import read_run_status

        runs_dir = tmp_path / "strategy_lab_runs"
        runs_dir.mkdir(parents=True)
        (runs_dir / "run-broken.status.json").write_text('{"run_id": "run-bro', encoding="utf-8")
        assert read_run_status("run-broken", base_dir=tmp_path) is None

    def test_list_skips_corrupted_status_file(self, tmp_path: Path) -> None:
        """B1 读侧容错：损坏的状态文件跳过，不影响其他条目。"""
        from rquant.dashboard.strategy_lab_worker import (
            LabRunStatus,
            list_run_statuses,
            write_run_status,
        )

        write_run_status(
            LabRunStatus(run_id="run-good", run_type="n_shape_optimize", state="done"),
            base_dir=tmp_path,
        )
        runs_dir = tmp_path / "strategy_lab_runs"
        (runs_dir / "run-broken.status.json").write_text("{ 半截", encoding="utf-8")

        statuses = list_run_statuses(base_dir=tmp_path)
        assert [item["run_id"] for item in statuses] == ["run-good"]

    def test_pid_alive_false_for_zombie(self) -> None:
        """B3: 僵尸进程（已退出未回收）不算存活。"""
        from rquant.dashboard.strategy_lab_worker import _pid_alive

        proc = subprocess.Popen(["/bin/sleep", "0"])
        try:
            deadline = time.monotonic() + 5.0
            became_zombie = False
            while time.monotonic() < deadline:
                stat = subprocess.run(
                    ["ps", "-p", str(proc.pid), "-o", "stat="],
                    capture_output=True,
                    text=True,
                ).stdout.strip()
                if stat.startswith("Z"):
                    became_zombie = True
                    break
                time.sleep(0.05)
            if not became_zombie:
                pytest.skip("子进程未进入僵尸状态，环境可能自动回收")
            assert _pid_alive(proc.pid) is False
        finally:
            proc.wait()


class TestCancelBackgroundRun:
    def test_cancel_missing_run_returns_false(self, tmp_path: Path) -> None:
        from rquant.dashboard.strategy_lab_worker import cancel_background_run

        assert cancel_background_run("no-such-run", base_dir=tmp_path) is False

    def test_cancel_non_running_returns_false(self, tmp_path: Path) -> None:
        from rquant.dashboard.strategy_lab_worker import (
            LabRunStatus,
            cancel_background_run,
            write_run_status,
        )

        write_run_status(
            LabRunStatus(run_id="run-done", run_type="n_shape_optimize", state="done"),
            base_dir=tmp_path,
        )
        assert cancel_background_run("run-done", base_dir=tmp_path) is False

    def test_cancel_tolerates_missing_process(self, tmp_path: Path) -> None:
        from rquant.dashboard.strategy_lab_worker import (
            LabRunStatus,
            cancel_background_run,
            read_run_status,
            write_run_status,
        )

        write_run_status(
            LabRunStatus(
                run_id="run-ghost",
                run_type="n_shape_optimize",
                state="running",
                pid=_dead_pid(),
            ),
            base_dir=tmp_path,
        )
        assert cancel_background_run("run-ghost", base_dir=tmp_path) is True
        status = read_run_status("run-ghost", base_dir=tmp_path)
        assert status is not None
        assert status.state == "cancelled"
        assert status.finished_at is not None

    def test_cancel_skips_signal_when_pgid_mismatch(self, tmp_path: Path) -> None:
        """B2: pid 被复用（pgid != pid）时不发信号，只标记取消，不误杀无关进程。"""
        from rquant.dashboard import strategy_lab_worker as worker

        # 不带 start_new_session 派生：子进程 pgid 继承自 pytest，必然 != 自身 pid
        proc = subprocess.Popen(["/bin/sleep", "30"])
        try:
            if os.getpgid(proc.pid) == proc.pid:
                pytest.skip("子进程意外成为进程组组长，无法构造 pgid 不匹配场景")
            worker.write_run_status(
                worker.LabRunStatus(
                    run_id="run-reused",
                    run_type="n_shape_optimize",
                    state="running",
                    pid=proc.pid,
                ),
                base_dir=tmp_path,
            )
            assert worker.cancel_background_run("run-reused", base_dir=tmp_path) is True
            # 未发信号：进程仍然活着
            assert proc.poll() is None
            status = worker.read_run_status("run-reused", base_dir=tmp_path)
            assert status is not None
            assert status.state == "cancelled"
        finally:
            proc.terminate()
            proc.wait()

    def test_cancel_does_not_overwrite_done_written_during_signal(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """B4: cancel 与子进程写 done 竞争时，done 不被 cancelled 覆盖。"""
        from rquant.dashboard import strategy_lab_worker as worker

        worker.write_run_status(
            worker.LabRunStatus(
                run_id="run-race",
                run_type="n_shape_optimize",
                state="running",
                pid=424242,
            ),
            base_dir=tmp_path,
        )
        monkeypatch.setattr(worker.os, "getpgid", lambda pid: pid)

        def fake_killpg(pgid: int, sig: int) -> None:
            # 模拟子进程在信号到达前恰好写完 done
            worker.write_run_status(
                worker.LabRunStatus(
                    run_id="run-race",
                    run_type="n_shape_optimize",
                    state="done",
                    pid=424242,
                    saved_run_id="saved-1",
                ),
                base_dir=tmp_path,
            )

        monkeypatch.setattr(worker.os, "killpg", fake_killpg)
        assert worker.cancel_background_run("run-race", base_dir=tmp_path) is False
        status = worker.read_run_status("run-race", base_dir=tmp_path)
        assert status is not None
        assert status.state == "done"
        assert status.saved_run_id == "saved-1"


class TestExecuteSpec:
    def test_formal_gate_runs_before_builder(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from rquant.dashboard import strategy_lab_worker as worker

        builder_called = False

        def fake_builder(params: dict[str, Any]) -> Any:
            nonlocal builder_called
            builder_called = True
            return _fake_run()

        def block(*_: object, **__: object) -> None:
            raise PermissionError("正式研究门未通过")

        monkeypatch.setitem(worker._SPEC_BUILDERS, "n_shape_optimize", fake_builder)
        monkeypatch.setattr(worker, "_enforce_spec_research_gate", block)

        with pytest.raises(PermissionError, match="正式研究门未通过"):
            worker.execute_spec(
                {
                    "run_type": "n_shape_optimize",
                    "run_id": "run-gate-blocked",
                    "params": {"research_gate": {"mode": "formal"}},
                },
                base_dir=tmp_path,
            )

        assert builder_called is False
        status = worker.read_run_status("run-gate-blocked", base_dir=tmp_path)
        assert status is not None and status.state == "error"

    def test_success_saves_run_and_marks_done(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from rquant.dashboard import strategy_lab_worker as worker

        calls: list[dict[str, Any]] = []

        def fake_builder(params: dict[str, Any]) -> Any:
            calls.append(params)
            return _fake_run()

        monkeypatch.setitem(worker._SPEC_BUILDERS, "n_shape_optimize", fake_builder)
        spec = {
            "run_type": "n_shape_optimize",
            "run_id": "run-ok",
            "params": {"start_date": "2026-06-01", "end_date": "2026-06-20"},
        }
        run_id = worker.execute_spec(spec, base_dir=tmp_path)

        assert run_id == "run-ok"
        assert calls == [{"start_date": "2026-06-01", "end_date": "2026-06-20"}]
        status = worker.read_run_status("run-ok", base_dir=tmp_path)
        assert status is not None
        assert status.state == "done"
        assert status.saved_run_id is not None
        # 研究记录照常走 save_strategy_lab_run 落盘
        saved_json = tmp_path / "strategy_lab_runs" / f"{status.saved_run_id}.json"
        assert saved_json.exists()

    def test_builder_error_writes_error_status_and_raises(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from rquant.dashboard import strategy_lab_worker as worker

        def broken_builder(params: dict[str, Any]) -> Any:
            raise RuntimeError("计算炸了")

        monkeypatch.setitem(worker._SPEC_BUILDERS, "n_shape_optimize", broken_builder)
        spec = {"run_type": "n_shape_optimize", "run_id": "run-bad", "params": {}}
        with pytest.raises(RuntimeError, match="计算炸了"):
            worker.execute_spec(spec, base_dir=tmp_path)

        status = worker.read_run_status("run-bad", base_dir=tmp_path)
        assert status is not None
        assert status.state == "error"
        assert "RuntimeError" in (status.error or "")
        assert status.finished_at is not None

    def test_unknown_run_type_raises_and_writes_error(self, tmp_path: Path) -> None:
        from rquant.dashboard import strategy_lab_worker as worker

        spec = {"run_type": "no_such_type", "run_id": "run-unknown"}
        with pytest.raises(ValueError, match="no_such_type"):
            worker.execute_spec(spec, base_dir=tmp_path)

        status = worker.read_run_status("run-unknown", base_dir=tmp_path)
        assert status is not None
        assert status.state == "error"

    def test_base_dir_from_spec(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from rquant.dashboard import strategy_lab_worker as worker

        monkeypatch.setitem(
            worker._SPEC_BUILDERS, "n_shape_optimize", lambda params: _fake_run()
        )
        spec = {
            "run_type": "n_shape_optimize",
            "run_id": "run-specdir",
            "base_dir": str(tmp_path),
            "params": {},
        }
        worker.execute_spec(spec)
        status = worker.read_run_status("run-specdir", base_dir=tmp_path)
        assert status is not None
        assert status.state == "done"


class TestLaunchBackgroundRun:
    def test_launch_writes_spec_and_running_status(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from rquant.dashboard import strategy_lab_worker as worker

        popen_calls: list[dict[str, Any]] = []

        class FakeProc:
            pid = 54321

        def fake_popen(cmd: list[str], **kwargs: Any) -> FakeProc:
            popen_calls.append({"cmd": cmd, **kwargs})
            return FakeProc()

        monkeypatch.setattr(worker.subprocess, "Popen", fake_popen)
        spec = {"run_type": "n_shape_optimize", "params": {"start_date": "2026-06-01"}}
        run_id = worker.launch_background_run(spec, base_dir=tmp_path)

        assert "n_shape_optimize" in run_id
        status = worker.read_run_status(run_id, base_dir=tmp_path)
        assert status is not None
        assert status.state == "running"
        assert status.pid == 54321

        spec_path = tmp_path / "strategy_lab_runs" / f"{run_id}.spec.json"
        assert spec_path.exists()
        payload = json.loads(spec_path.read_text(encoding="utf-8"))
        assert payload["run_id"] == run_id
        assert payload["base_dir"] == str(tmp_path)
        assert payload["params"] == {"start_date": "2026-06-01"}

        assert len(popen_calls) == 1
        call = popen_calls[0]
        assert call["cmd"][0] == sys.executable
        assert call["cmd"][1:4] == ["-m", "rquant.cli", "lab-run"]
        assert call["cmd"][4:6] == ["--spec", str(spec_path)]
        assert call["start_new_session"] is True
        src_dir = str(Path(worker.__file__).resolve().parents[2])
        assert src_dir in call["env"]["PYTHONPATH"]

    def test_launch_unknown_run_type_raises(self, tmp_path: Path) -> None:
        from rquant.dashboard.strategy_lab_worker import launch_background_run

        with pytest.raises(ValueError, match="no_such_type"):
            launch_background_run({"run_type": "no_such_type"}, base_dir=tmp_path)


class TestLabRunCli:
    def test_parser_accepts_lab_run(self) -> None:
        from rquant.cli import build_parser

        args = build_parser().parse_args(["lab-run", "--spec", "/tmp/spec.json"])
        assert args.command == "lab-run"
        assert args.spec == "/tmp/spec.json"

    def test_cmd_lab_run_success(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import argparse

        from rquant.cli import cmd_lab_run
        from rquant.dashboard import strategy_lab_worker as worker

        monkeypatch.setitem(
            worker._SPEC_BUILDERS, "n_shape_optimize", lambda params: _fake_run()
        )
        spec_path = tmp_path / "spec.json"
        spec_path.write_text(
            json.dumps({
                "run_type": "n_shape_optimize",
                "run_id": "run-cli-ok",
                "base_dir": str(tmp_path),
                "params": {},
            }),
            encoding="utf-8",
        )
        code = cmd_lab_run(argparse.Namespace(spec=str(spec_path)))
        assert code == 0
        status = worker.read_run_status("run-cli-ok", base_dir=tmp_path)
        assert status is not None
        assert status.state == "done"

    def test_cmd_lab_run_failure_exits_nonzero(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import argparse

        from rquant.cli import cmd_lab_run
        from rquant.dashboard import strategy_lab_worker as worker

        def broken_builder(params: dict[str, Any]) -> Any:
            raise RuntimeError("boom")

        monkeypatch.setitem(worker._SPEC_BUILDERS, "n_shape_optimize", broken_builder)
        spec_path = tmp_path / "spec.json"
        spec_path.write_text(
            json.dumps({
                "run_type": "n_shape_optimize",
                "run_id": "run-cli-bad",
                "base_dir": str(tmp_path),
                "params": {},
            }),
            encoding="utf-8",
        )
        code = cmd_lab_run(argparse.Namespace(spec=str(spec_path)))
        assert code == 1
        status = worker.read_run_status("run-cli-bad", base_dir=tmp_path)
        assert status is not None
        assert status.state == "error"

    def test_cmd_lab_run_corrupted_spec_writes_error_status(self, tmp_path: Path) -> None:
        """B5: spec 损坏时按 <run_id>.spec.json 约定提取 run_id 写 error status。"""
        import argparse

        from rquant.cli import cmd_lab_run
        from rquant.dashboard import strategy_lab_worker as worker

        runs_dir = tmp_path / "strategy_lab_runs"
        runs_dir.mkdir(parents=True)
        spec_path = runs_dir / "run-corrupt.spec.json"
        spec_path.write_text("{ 这不是合法 JSON", encoding="utf-8")

        code = cmd_lab_run(argparse.Namespace(spec=str(spec_path)))
        assert code == 1
        status = worker.read_run_status("run-corrupt", base_dir=tmp_path)
        assert status is not None
        assert status.state == "error"
        assert "spec" in (status.error or "")
        assert status.finished_at is not None

    def test_main_does_not_wrap_lab_run(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """B5: lab-run 在 main() 豁免列表里，异常不再推 PushDeer 运维告警。"""
        from unittest.mock import patch

        from rquant.cli import main

        def boom(_args: Any) -> int:
            raise ValueError("inner")

        monkeypatch.setattr("rquant.cli.cmd_lab_run", boom)
        with (
            patch("sys.argv", ["rquant", "lab-run", "--spec", "/tmp/x.json"]),
            patch("rquant.notify.notify") as mock_notify,
        ):
            with pytest.raises(ValueError):
                main()
            mock_notify.assert_not_called()
