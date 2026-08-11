"""Strategy Lab 后台任务 runner。

UI 侧 launch_background_run 写 spec → 派生独立进程跑 `rquant lab-run --spec`，
状态通过 data/strategy_lab_runs/<run_id>.status.json 跟踪，结果照常走
save_strategy_lab_run 落盘，历史记录 tab 可见。
"""

from __future__ import annotations

import json
import os
import signal
import stat
import subprocess
import sys
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Literal

from loguru import logger
from pydantic import BaseModel

from rquant.dashboard.strategy_lab_runs import (
    StrategyLabSavedRun,
    build_strategy_lab_run,
    save_strategy_lab_run,
    strategy_lab_runs_dir,
)

CST = timezone(timedelta(hours=8))

LabRunState = Literal["running", "done", "error", "cancelled"]


class LabRunStatus(BaseModel):
    """一个后台任务的状态文件内容。"""

    run_id: str
    run_type: str
    state: LabRunState
    pid: int | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    error: str | None = None
    saved_run_id: str | None = None


def _now() -> datetime:
    return datetime.now(CST)


def _generate_run_id(run_type: str) -> str:
    stamp = _now().strftime("%Y%m%d-%H%M%S")
    return f"{stamp}-{run_type}-bg-{uuid.uuid4().hex[:6]}"


def _status_path(run_id: str, base_dir: Path | None = None) -> Path:
    return strategy_lab_runs_dir(base_dir) / f"{run_id}.status.json"


def write_run_status(status: LabRunStatus, *, base_dir: Path | None = None) -> Path:
    path = _status_path(status.run_id, base_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    # UI 侧随时在轮询读取：先写同目录临时文件再原子替换，避免读到半截 JSON
    tmp_path = path.with_name(path.name + ".tmp")
    tmp_path.write_text(
        json.dumps(status.model_dump(mode="json"), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    os.replace(tmp_path, path)
    return path


def read_run_status(run_id: str, *, base_dir: Path | None = None) -> LabRunStatus | None:
    path = _status_path(run_id, base_dir)
    if not path.exists():
        return None
    try:
        return LabRunStatus.model_validate(json.loads(path.read_text(encoding="utf-8")))
    except (json.JSONDecodeError, OSError, ValueError) as e:
        logger.debug(f"状态文件解析失败，按不存在处理: {path} ({e})")
        return None


def _is_zombie(pid: int) -> bool:
    """macOS 无 /proc，用 ps 查 stat 首字母：'Z' 是崩溃后未被回收的僵尸进程。"""
    try:
        result = subprocess.run(
            ["ps", "-p", str(pid), "-o", "stat="],
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.SubprocessError):
        return False  # ps 本身跑不了时退回 kill(0) 的判断，宁可误报存活
    stat = result.stdout.strip()
    if result.returncode != 0 or not stat:
        return True  # kill(0) 成功后进程刚退出，ps 已查不到
    return stat.startswith("Z")


def _pid_alive(pid: int | None) -> bool:
    if not pid or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    # kill(0) 对僵尸进程也会成功：崩溃的后台任务若没被 wait 回收会永久"运行中"
    return not _is_zombie(pid)


def _build_n_shape_optimize_run(params: dict[str, Any]) -> StrategyLabSavedRun:
    from rquant.research_gate import (
        ResearchGateRequest,
        build_gate_research_manifest,
        open_gated_research_store,
    )
    from rquant.strategy_optimizer import run_strategy_optimization

    start_date = str(params["start_date"])
    end_date = str(params["end_date"])
    preset_name = str(params.get("preset_name") or "n-shape-combined")
    entry_modes = [str(v) for v in params.get("entry_modes") or ["first_break"]]
    profile_variants = [str(v) for v in params.get("profile_variants") or ["baseline"]]
    hold_options = [int(v) for v in params.get("hold_options") or [1, 2, 3]]
    topn_options = [int(v) for v in params.get("topn_options") or [1, 2, 3]]
    score_profile_names = [str(v) for v in params.get("score_profile_names") or ["v1"]]
    walk_forward_folds = int(params.get("walk_forward_folds") or 0)
    validation_ratio = float(params.get("validation_ratio") or 0.3)
    min_trades = int(params.get("min_trades") or 5)

    gate_payload = params.get("research_gate")
    snapshot_value = params.get("_legacy_research_snapshot")
    if not isinstance(snapshot_value, str):
        raise PermissionError("legacy lab-run requires an explicit immutable research snapshot")
    snapshot_path = Path(snapshot_value)
    manifest = None
    if gate_payload is not None:
        gate_request = ResearchGateRequest.model_validate(gate_payload)
        if gate_request.mode != "exploratory":
            raise PermissionError("legacy lab-run cannot execute formal research; use lab-worker")
        with open_gated_research_store(
            gate_request,
            metadata_store_factory=lambda: _open_legacy_immutable_research_snapshot(snapshot_path),
        ) as (store, gate_decision):
            result = run_strategy_optimization(
                store,
                start_date=start_date,
                end_date=end_date,
                entry_modes=entry_modes,  # type: ignore[arg-type]
                profile_variants=profile_variants,  # type: ignore[arg-type]
                max_hold_days_options=hold_options,
                top_n_options=topn_options,
                score_profile_names=score_profile_names,
                walk_forward_folds=walk_forward_folds,
                preset_name=preset_name,
                validation_ratio=validation_ratio,
                min_trades=min_trades,
            )
        manifest = build_gate_research_manifest(
            gate_request,
            gate_decision,
        )
    else:
        with _open_legacy_immutable_research_snapshot(snapshot_path) as store:
            result = run_strategy_optimization(
                store,
                start_date=start_date,
                end_date=end_date,
                entry_modes=entry_modes,  # type: ignore[arg-type]
                profile_variants=profile_variants,  # type: ignore[arg-type]
                max_hold_days_options=hold_options,
                top_n_options=topn_options,
                score_profile_names=score_profile_names,
                walk_forward_folds=walk_forward_folds,
                preset_name=preset_name,
                validation_ratio=validation_ratio,
                min_trades=min_trades,
            )
    return build_strategy_lab_run(
        run_type="n_shape_optimize",
        title=f"N字自动优化 {start_date} 至 {end_date}（后台）",
        params={
            "候选池": preset_name,
            "开始日期": start_date,
            "结束日期": end_date,
            "入场模式": entry_modes,
            "风控版本": profile_variants,
            "持有期集合": hold_options,
            "特征topN": topn_options,
            "评分画像": score_profile_names,
            "验证区间占比": validation_ratio,
            "最少交易数": min_trades,
            "Walk-forward折数": walk_forward_folds,
            "运行方式": "后台任务",
        },
        metrics={
            "策略排行数": len(result.rankings),
            "交易样本数": len(result.trades),
            "topN排行数": len(result.topn_rankings),
            "Walk-forward排行数": len(result.walk_forward_rankings),
        },
        tables={
            "策略排行": result.rankings,
            "自动优化交易样本": result.trades,
            "特征topN排行": result.topn_rankings,
            "特征topN入选样本": result.topn_trades,
            "Walk-forward排行": result.walk_forward_rankings,
            "Walk-forward入选样本": result.walk_forward_trades,
        },
        manifest=manifest,
    )


# run_type → 结果构建函数。新增后台任务类型时在这里注册即可
_SPEC_BUILDERS: dict[str, Callable[[dict[str, Any]], StrategyLabSavedRun]] = {
    "n_shape_optimize": _build_n_shape_optimize_run,
}


def _enforce_spec_research_gate(run_type: str, params: dict[str, Any]) -> None:
    payload = params.get("research_gate")
    if payload is None:
        return
    from rquant.research_gate import (
        ResearchGateRequest,
        evaluate_store_research_gate,
        research_gate_metadata_ready,
    )
    request = ResearchGateRequest.model_validate(payload)
    expected_strategy = {"n_shape_optimize": "n_shape"}.get(run_type)
    if expected_strategy is None or request.strategy_name != expected_strategy:
        raise PermissionError("后台任务的正式研究门策略标识不匹配")
    snapshot_value = params.get("_legacy_research_snapshot")
    if not isinstance(snapshot_value, str):
        raise PermissionError("legacy lab-run requires an explicit immutable research snapshot")
    if request.mode != "exploratory":
        raise PermissionError("legacy lab-run cannot execute formal research; use lab-worker")
    with _open_legacy_immutable_research_snapshot(Path(snapshot_value)) as store:
        decision = evaluate_store_research_gate(store, request)
    if request.mode == "formal" and not research_gate_metadata_ready(decision):
        reasons = "; ".join(failure.message for failure in decision.failures)
        raise PermissionError(f"正式研究门未通过: {reasons}")


@contextmanager
def _open_legacy_immutable_research_snapshot(path: Path) -> Iterator[object]:
    """Open the explicit local snapshot once and reject replacement during a legacy run."""
    from rquant.storage.duckdb import DuckDBStore

    if not path.is_absolute():
        raise PermissionError("immutable research snapshot must be an absolute path")
    resolved = Path(os.path.abspath(path))
    try:
        before = os.lstat(resolved)
    except FileNotFoundError as exc:
        raise PermissionError("immutable research snapshot does not exist") from exc
    if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
        raise PermissionError("immutable research snapshot must be a regular non-symlink file")
    identity = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
    with DuckDBStore(resolved, read_only=True) as store:
        yield store
    after = os.lstat(resolved)
    if (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns) != identity:
        raise PermissionError("immutable research snapshot rotated during legacy lab-run")


def execute_spec(spec: dict[str, Any], *, base_dir: Path | None = None) -> str:
    """按 spec 执行一次后台任务：计算 → save_strategy_lab_run → 更新状态。

    异常时把 error 写进 status 文件后再抛出，让调用方（lab-run CLI）exit 1。
    """
    if base_dir is None and spec.get("base_dir"):
        base_dir = Path(str(spec["base_dir"]))
    run_type = str(spec.get("run_type") or "")
    run_id = str(spec.get("run_id") or _generate_run_id(run_type or "unknown"))
    builder = _SPEC_BUILDERS.get(run_type)
    if builder is None:
        message = f"不支持的后台任务类型: {run_type!r}"
        write_run_status(
            LabRunStatus(
                run_id=run_id,
                run_type=run_type,
                state="error",
                pid=os.getpid(),
                started_at=_now(),
                finished_at=_now(),
                error=message,
            ),
            base_dir=base_dir,
        )
        raise ValueError(message)

    status = LabRunStatus(
        run_id=run_id,
        run_type=run_type,
        state="running",
        pid=os.getpid(),
        started_at=_now(),
    )
    write_run_status(status, base_dir=base_dir)
    logger.info(f"后台任务开始: {run_id} ({run_type})")
    try:
        params = dict(spec.get("params") or {})
        if spec.get("research_snapshot_mismatch"):
            raise PermissionError("immutable research snapshot mismatch between CLI and spec")
        snapshot_value = spec.get("research_snapshot")
        if not isinstance(snapshot_value, str) or not snapshot_value:
            raise PermissionError("legacy lab-run requires an explicit immutable research snapshot")
        if not Path(snapshot_value).is_absolute():
            raise PermissionError("immutable research snapshot must be an absolute path")
        params["_legacy_research_snapshot"] = snapshot_value
        _enforce_spec_research_gate(run_type, params)
        run = builder(params)
        saved = save_strategy_lab_run(run, base_dir=base_dir)
    except Exception as e:
        write_run_status(
            status.model_copy(
                update={
                    "state": "error",
                    "finished_at": _now(),
                    "error": f"{type(e).__name__}: {e}",
                }
            ),
            base_dir=base_dir,
        )
        logger.exception(f"后台任务失败: {run_id}")
        raise
    write_run_status(
        status.model_copy(
            update={"state": "done", "finished_at": _now(), "saved_run_id": saved.run_id}
        ),
        base_dir=base_dir,
    )
    logger.info(f"后台任务完成: {run_id} → 研究记录 {saved.run_id}")
    return run_id


def launch_background_run(spec: dict[str, Any], *, base_dir: Path | None = None) -> str:
    """spec 落盘后派生独立进程执行，立即返回 run_id（不等待完成）。"""
    run_type = str(spec.get("run_type") or "")
    if run_type not in _SPEC_BUILDERS:
        raise ValueError(f"不支持的后台任务类型: {run_type!r}")
    run_id = _generate_run_id(run_type)
    out_dir = strategy_lab_runs_dir(base_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    spec_payload: dict[str, Any] = {**spec, "run_id": run_id}
    if base_dir is not None:
        spec_payload["base_dir"] = str(base_dir)
    spec_path = out_dir / f"{run_id}.spec.json"
    spec_path.write_text(
        json.dumps(spec_payload, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )

    # 子进程要能 import rquant：把 src 目录塞进 PYTHONPATH（streamlit 进程自身
    # 也是靠 PYTHONPATH 起的，但 env 继承不保证，显式写死最稳）
    src_dir = Path(__file__).resolve().parents[2]
    env = dict(os.environ)
    existing = env.get("PYTHONPATH")
    env["PYTHONPATH"] = f"{src_dir}{os.pathsep}{existing}" if existing else str(src_dir)

    log_path = out_dir / f"{run_id}.log"
    with log_path.open("ab") as log_file:
        proc = subprocess.Popen(
            [sys.executable, "-m", "rquant.cli", "lab-run", "--spec", str(spec_path)],
            stdout=log_file,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            start_new_session=True,
            env=env,
        )
    write_run_status(
        LabRunStatus(
            run_id=run_id,
            run_type=run_type,
            state="running",
            pid=proc.pid,
            started_at=_now(),
        ),
        base_dir=base_dir,
    )
    logger.info(f"后台任务已派生: {run_id} pid={proc.pid} spec={spec_path}")
    return run_id


def list_run_statuses(*, base_dir: Path | None = None) -> list[dict[str, Any]]:
    """列出全部后台任务状态（新→旧）。running 但进程已死的标注为 error。"""
    out_dir = strategy_lab_runs_dir(base_dir)
    if not out_dir.exists():
        return []
    statuses: list[dict[str, Any]] = []
    for path in sorted(out_dir.glob("*.status.json"), reverse=True):
        try:
            status = LabRunStatus.model_validate(json.loads(path.read_text(encoding="utf-8")))
        except (json.JSONDecodeError, OSError, ValueError) as e:
            logger.debug(f"状态文件解析失败，跳过: {path} ({e})")
            continue
        item = status.model_dump(mode="json")
        if status.state == "running" and not _pid_alive(status.pid):
            # 只标注不落盘：避免与刚写完 done 状态的子进程竞争覆盖
            item["state"] = "error"
            item["error"] = "进程已不存在（可能被系统终止或机器重启）"
        statuses.append(item)
    return statuses


def cancel_background_run(run_id: str, *, base_dir: Path | None = None) -> bool:
    """SIGTERM 整个进程组并把状态标记为 cancelled。进程不存在时容错。"""
    status = read_run_status(run_id, base_dir=base_dir)
    if status is None or status.state != "running":
        return False
    if status.pid:
        try:
            pgid = os.getpgid(status.pid)
        except (ProcessLookupError, PermissionError, OSError) as e:
            logger.warning(f"后台任务 {run_id} 进程 {status.pid} 无法终止（{e}），直接标记取消")
        else:
            # start_new_session 派生的子进程 pgid == 自身 pid；不相等说明该 pid
            # 已被无关进程复用，killpg 会误杀别人的进程组，只标记状态不发信号
            if pgid != status.pid:
                logger.warning(
                    f"后台任务 {run_id} 进程 {status.pid} 的进程组 {pgid} 与 pid 不符"
                    "（疑似 pid 复用），跳过信号只标记取消"
                )
            else:
                try:
                    os.killpg(pgid, signal.SIGTERM)
                except (ProcessLookupError, PermissionError, OSError) as e:
                    logger.warning(
                        f"后台任务 {run_id} 进程 {status.pid} 无法终止（{e}），直接标记取消"
                    )
    # 发信号后重读状态：子进程可能恰好已写完 done/error，
    # 不能用 cancelled 覆盖掉 saved_run_id / error 信息
    latest = read_run_status(run_id, base_dir=base_dir)
    if latest is not None and latest.state in ("done", "error"):
        logger.info(f"后台任务 {run_id} 在取消前已结束（{latest.state}），保留原状态不覆盖")
        return False
    write_run_status(
        (latest or status).model_copy(update={"state": "cancelled", "finished_at": _now()}),
        base_dir=base_dir,
    )
    logger.info(f"后台任务已取消: {run_id}")
    return True
