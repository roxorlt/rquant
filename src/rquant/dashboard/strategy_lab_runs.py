"""Strategy Lab 研究记录的本地保存与 Markdown 导出。"""

from __future__ import annotations

import json
import math
import re
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field

from rquant.config import settings

CST = timezone(timedelta(hours=8))
RUN_TYPE_LABELS: dict[str, str] = {
    "n_shape_compare": "N字收益对比",
    "n_shape_optimize": "N字自动优化",
    "auction_gap": "集合竞价跳空",
    "growth_board_surge": "科创/创业放量",
}


class StrategyLabRunTable(BaseModel):
    """一个保存到研究记录里的表格。"""

    name: str
    total_rows: int = Field(ge=0)
    rows: list[dict[str, Any]]
    truncated: bool = False


class StrategyLabSavedRun(BaseModel):
    """一条 Strategy Lab 研究记录。"""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    run_id: str
    run_type: str
    title: str
    created_at: datetime
    params: dict[str, Any]
    metrics: dict[str, Any]
    tables: list[StrategyLabRunTable]
    markdown: str
    json_path: Path | None = None
    markdown_path: Path | None = None


def strategy_lab_runs_dir(base_dir: Path | None = None) -> Path:
    return (base_dir or settings.data_dir) / "strategy_lab_runs"


def _slug(value: str) -> str:
    normalized = re.sub(r"[^0-9A-Za-z\u4e00-\u9fff_-]+", "-", value).strip("-")
    return normalized[:40] or "strategy-lab-run"


def _run_id(run_type: str, title: str, created_at: datetime) -> str:
    stamp = created_at.strftime("%Y%m%d-%H%M%S")
    return f"{stamp}-{run_type}-{_slug(title)}"


def _json_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, datetime | date | time):
        return value.isoformat()
    if isinstance(value, pd.Timedelta):
        return str(value)
    if hasattr(value, "item"):
        return _json_value(value.item())
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return round(value, 6)
    if isinstance(value, dict):
        return {str(k): _json_value(v) for k, v in value.items()}
    if isinstance(value, list | tuple):
        return [_json_value(item) for item in value]
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return value


def _records(df: pd.DataFrame, *, limit: int) -> list[dict[str, Any]]:
    if df.empty:
        return []
    return [
        {str(col): _json_value(row[col]) for col in df.columns}
        for _, row in df.head(limit).iterrows()
    ]


def _markdown_value(value: Any) -> str:
    clean = _json_value(value)
    if clean is None:
        return ""
    if isinstance(clean, float):
        return f"{clean:.4f}".rstrip("0").rstrip(".")
    text = str(clean)
    return text.replace("|", "\\|").replace("\n", "<br>")


def _markdown_table(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "_无数据_"
    columns = list(rows[0].keys())
    out = [
        "| " + " | ".join(columns) + " |",
        "| " + " | ".join("---" for _ in columns) + " |",
    ]
    for row in rows:
        out.append("| " + " | ".join(_markdown_value(row.get(col)) for col in columns) + " |")
    return "\n".join(out)


def render_strategy_lab_markdown(run: StrategyLabSavedRun) -> str:
    """把研究记录渲染成可复制给 agent 继续讨论的 Markdown。"""
    lines = [
        f"# {run.title}",
        "",
        f"- 记录ID：`{run.run_id}`",
        f"- 类型：{RUN_TYPE_LABELS.get(run.run_type, run.run_type)}",
        f"- 生成时间：{run.created_at.astimezone(CST).strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "## 参数",
        "",
        _markdown_table([run.params]),
        "",
        "## 核心指标",
        "",
        _markdown_table([run.metrics]),
        "",
    ]
    for table in run.tables:
        lines.extend([
            f"## {table.name}",
            "",
            f"- 总行数：{table.total_rows}",
            f"- 当前导出：{len(table.rows)} 行",
        ])
        if table.truncated:
            lines.append("- 注意：表格已截断，完整复盘请读取同名 JSON。")
        lines.extend(["", _markdown_table(table.rows), ""])
    return "\n".join(lines).strip() + "\n"


def build_strategy_lab_run(
    *,
    run_type: str,
    title: str,
    params: dict[str, Any],
    metrics: dict[str, Any],
    tables: dict[str, pd.DataFrame],
    max_rows_per_table: int = 500,
) -> StrategyLabSavedRun:
    created_at = datetime.now(CST)
    saved_tables: list[StrategyLabRunTable] = []
    for name, df in tables.items():
        total_rows = len(df)
        saved_tables.append(
            StrategyLabRunTable(
                name=name,
                total_rows=total_rows,
                rows=_records(df, limit=max_rows_per_table),
                truncated=total_rows > max_rows_per_table,
            )
        )
    run = StrategyLabSavedRun(
        run_id=_run_id(run_type, title, created_at),
        run_type=run_type,
        title=title,
        created_at=created_at,
        params=_json_value(params),
        metrics=_json_value(metrics),
        tables=saved_tables,
        markdown="",
    )
    run.markdown = render_strategy_lab_markdown(run)
    return run


def save_strategy_lab_run(
    run: StrategyLabSavedRun,
    *,
    base_dir: Path | None = None,
) -> StrategyLabSavedRun:
    out_dir = strategy_lab_runs_dir(base_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / f"{run.run_id}.json"
    markdown_path = out_dir / f"{run.run_id}.md"
    payload = run.model_copy(
        update={"json_path": json_path, "markdown_path": markdown_path}
    )
    json_path.write_text(
        json.dumps(
            payload.model_dump(mode="json"),
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    markdown_path.write_text(run.markdown, encoding="utf-8")
    return payload


def load_strategy_lab_run(
    run_id: str,
    *,
    base_dir: Path | None = None,
) -> StrategyLabSavedRun:
    out_dir = strategy_lab_runs_dir(base_dir)
    json_path = out_dir / f"{run_id}.json"
    payload = json.loads(json_path.read_text(encoding="utf-8"))
    run = StrategyLabSavedRun.model_validate(payload)
    markdown_path = out_dir / f"{run_id}.md"
    if markdown_path.exists():
        run.markdown = markdown_path.read_text(encoding="utf-8")
    run.json_path = json_path
    run.markdown_path = markdown_path
    return run


def list_strategy_lab_runs(
    *,
    base_dir: Path | None = None,
    run_type: str | None = None,
    limit: int = 50,
) -> list[StrategyLabSavedRun]:
    out_dir = strategy_lab_runs_dir(base_dir)
    if not out_dir.exists():
        return []
    runs: list[StrategyLabSavedRun] = []
    for path in sorted(out_dir.glob("*.json"), reverse=True):
        try:
            run = load_strategy_lab_run(path.stem, base_dir=base_dir)
        except (json.JSONDecodeError, OSError, ValueError):
            continue
        if run_type is not None and run.run_type != run_type:
            continue
        runs.append(run)
        if len(runs) >= limit:
            break
    return runs
