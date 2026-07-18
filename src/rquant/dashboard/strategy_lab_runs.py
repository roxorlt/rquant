"""Strategy Lab 研究记录的本地保存与 Markdown 导出。"""

from __future__ import annotations

import hashlib
import json
import math
import re
import uuid
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field

from rquant.config import settings
from rquant.research_manifest import (
    RESEARCH_STATUS_LABELS,
    ResearchManifest,
    legacy_exploratory_manifest,
    new_exploratory_manifest,
)

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
    manifest: ResearchManifest
    markdown: str
    json_path: Path | None = None
    markdown_path: Path | None = None


def strategy_lab_runs_dir(base_dir: Path | None = None) -> Path:
    return (base_dir or settings.data_dir) / "strategy_lab_runs"


def _slug(value: str) -> str:
    normalized = re.sub(r"[^0-9A-Za-z\u4e00-\u9fff_-]+", "-", value).strip("-")
    return normalized[:40] or "strategy-lab-run"


def _run_id(run_type: str, title: str, created_at: datetime) -> str:
    stamp = created_at.strftime("%Y%m%d-%H%M%S-%f")
    nonce = uuid.uuid4().hex[:8]
    return f"{stamp}-{nonce}-{run_type}-{_slug(title)}"


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


def _canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _hash_json_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, datetime | date | time):
        return value.isoformat()
    if isinstance(value, pd.Timedelta):
        return str(value)
    if hasattr(value, "item"):
        return _hash_json_value(value.item())
    if isinstance(value, float):
        return None if math.isnan(value) or math.isinf(value) else value
    if isinstance(value, dict):
        return {str(key): _hash_json_value(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_hash_json_value(item) for item in value]
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return value


def _strategy_spec_hash(run_type: str, params: dict[str, Any]) -> str:
    payload = {
        "run_type": run_type,
        "params": _hash_json_value(params),
    }
    return hashlib.sha256(_canonical_json_bytes(payload)).hexdigest()


def _result_hash(
    metrics: dict[str, Any],
    tables: dict[str, pd.DataFrame],
) -> str:
    digest = hashlib.sha256()
    digest.update(
        _canonical_json_bytes({"metrics": _hash_json_value(metrics)})
    )
    for name in sorted(tables):
        df = tables[name]
        digest.update(
            _canonical_json_bytes({
                "table": name,
                "columns": [str(column) for column in df.columns],
                "dtypes": [str(dtype) for dtype in df.dtypes],
                "row_count": len(df),
            })
        )
        for row in df.itertuples(index=False, name=None):
            digest.update(_canonical_json_bytes(_hash_json_value(row)))
    return digest.hexdigest()


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


def _research_manifest_markdown_lines(manifest: ResearchManifest) -> list[str]:
    coverage = "未知"
    if manifest.coverage_ratio is not None:
        coverage = f"{manifest.coverage_ratio:.2%}"
        if (
            manifest.coverage_numerator is not None
            and manifest.coverage_denominator is not None
        ):
            coverage += (
                f"（{manifest.coverage_numerator:,}/"
                f"{manifest.coverage_denominator:,}）"
            )
    data_range = "未知"
    if manifest.data_start_date is not None and manifest.data_end_date is not None:
        data_range = f"{manifest.data_start_date} 至 {manifest.data_end_date}"
    lines = [
        "## 研究可信度",
        "",
        f"- 状态：{RESEARCH_STATUS_LABELS[manifest.research_status]}",
        f"- 原因：{manifest.status_reason}",
        f"- 代码提交：`{manifest.code_commit or '未知'}`",
        f"- 数据快照：`{manifest.dataset_snapshot_id or '未知'}`",
        f"- 执行数据绑定：`{manifest.dataset_binding_hash or '未知'}`",
        f"- 策略参数哈希：`{manifest.strategy_spec_hash or '未知'}`",
        f"- 完整结果哈希：`{manifest.result_hash or '未知'}`",
        f"- 数据覆盖：{coverage}",
        f"- 数据区间：{data_range}",
        f"- 资格全集：{manifest.universe_definition or '未知'}",
        f"- 执行模型：`{manifest.execution_model_version or '未知'}`",
        f"- 成本模型：`{manifest.cost_model_version or '未知'}`",
        f"- 严格样本外成交：{manifest.out_of_sample_trades or 0}",
        f"- 前瞻验证：{manifest.forward_validation_days or 0} 日 / "
        f"{manifest.forward_filled_trades or 0} 笔成交",
    ]
    if manifest.missing_evidence:
        lines.append(f"- 缺失证据：{', '.join(manifest.missing_evidence)}")
    lines.extend(f"- 注意：{warning}" for warning in manifest.warnings)
    return lines


def _inject_research_manifest_markdown(
    markdown: str,
    manifest: ResearchManifest,
) -> str:
    """给旧 Markdown 注入可信度段落，同时保留原有手工备注与大表。"""
    original = markdown.strip()
    if "## 研究可信度" in original:
        return original + "\n"
    section = "\n".join(_research_manifest_markdown_lines(manifest))
    if not original:
        return section + "\n"
    first_line, separator, remainder = original.partition("\n")
    if first_line.startswith("# "):
        parts = [first_line, "", section]
        if separator and remainder.strip():
            parts.extend(["", remainder.lstrip()])
        return "\n".join(parts).strip() + "\n"
    return f"{section}\n\n{original}\n"


def render_strategy_lab_markdown(run: StrategyLabSavedRun) -> str:
    """把研究记录渲染成可复制给 agent 继续讨论的 Markdown。"""
    lines = [
        f"# {run.title}",
        "",
        f"- 记录ID：`{run.run_id}`",
        f"- 类型：{RUN_TYPE_LABELS.get(run.run_type, run.run_type)}",
        f"- 生成时间：{run.created_at.astimezone(CST).strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        *_research_manifest_markdown_lines(run.manifest),
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
    manifest: ResearchManifest | None = None,
) -> StrategyLabSavedRun:
    created_at = datetime.now(CST)
    normalized_params = _json_value(params)
    normalized_metrics = _json_value(metrics)
    base_manifest = manifest or new_exploratory_manifest(run_type)
    enriched_manifest = ResearchManifest.model_validate({
        **base_manifest.model_dump(exclude={"missing_evidence"}),
        "schema_version": 2,
        "strategy_spec_hash": _strategy_spec_hash(run_type, params),
        "result_hash": _result_hash(metrics, tables),
    })
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
        params=normalized_params,
        metrics=normalized_metrics,
        tables=saved_tables,
        manifest=enriched_manifest,
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
    json_created = False
    try:
        with json_path.open("x", encoding="utf-8") as handle:
            handle.write(
                json.dumps(
                    payload.model_dump(mode="json"),
                    ensure_ascii=False,
                    indent=2,
                )
            )
        json_created = True
        with markdown_path.open("x", encoding="utf-8") as handle:
            handle.write(run.markdown)
    except BaseException:
        if json_created and not markdown_path.exists():
            json_path.unlink(missing_ok=True)
        raise
    return payload


def load_strategy_lab_run(
    run_id: str,
    *,
    base_dir: Path | None = None,
) -> StrategyLabSavedRun:
    out_dir = strategy_lab_runs_dir(base_dir)
    json_path = out_dir / f"{run_id}.json"
    payload = json.loads(json_path.read_text(encoding="utf-8"))
    is_legacy = "manifest" not in payload
    if is_legacy:
        payload["manifest"] = legacy_exploratory_manifest(
            str(payload.get("run_type") or "unknown")
        ).model_dump(mode="json")
    run = StrategyLabSavedRun.model_validate(payload)
    markdown_path = out_dir / f"{run_id}.md"
    if is_legacy:
        original_markdown = (
            markdown_path.read_text(encoding="utf-8")
            if markdown_path.exists()
            else run.markdown
        )
        run.markdown = _inject_research_manifest_markdown(
            original_markdown,
            run.manifest,
        )
    elif markdown_path.exists():
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
