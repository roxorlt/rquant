"""Week 7.5 C-Canvas：多画布持久化。

每个 canvas 是 pool 池的子集视图：
- pool 是全局公共资源（builtin + user_presets）
- canvas 引用 pool 而非拥有 pool（pool 不属于 canvas）
- 同一 pool 可被多个 canvas 引用

存储：`data/canvases/<base_name>.json`（同 user_presets 同源约定）

特殊：**默认画布**（virtual, 名为 __default__）
- data/canvases/ 为空 → 自动呈现含全部 PRESET_SCREENS 的默认 canvas
- 不可删（保底，确保至少有一个 canvas）
- 不存盘（动态生成 from PRESET_SCREENS）
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from rquant.config import settings


DEFAULT_NAME = "__default__"
DEFAULT_DISPLAY_NAME = "默认（所有 pool）"


@dataclass
class CanvasMeta:
    """Canvas 元数据（轻量，list_canvases 用）。"""

    name: str
    display_name: str
    description: str
    pool_count: int
    is_default: bool = False


@dataclass
class Canvas:
    """完整 canvas 实例。"""

    name: str
    description: str
    pool_refs: list[str] = field(default_factory=list)
    created_at: str = ""
    updated_at: str = ""
    source: str = "canvas_new"
    is_default: bool = False

    @property
    def display_name(self) -> str:
        return DEFAULT_DISPLAY_NAME if self.is_default else self.name


def canvas_dir() -> Path:
    return Path(settings.data_dir) / "canvases"


def canvas_path(name: str) -> Path:
    return canvas_dir() / f"{name}.json"


def list_canvases() -> list[CanvasMeta]:
    """返回所有 canvas 元数据（按 name 排序，默认 canvas 总在最前）。

    - data/canvases/ 不存在 / 为空 → 只返回 [default]
    - 否则：default + 文件系统中所有 *.json（按 name 排序）
    """
    # 默认 canvas 总是返回（动态生成）
    # 局部 import 避开循环 (canvas_files ← presets)
    from rquant.presets import PRESET_SCREENS

    default_meta = CanvasMeta(
        name=DEFAULT_NAME,
        display_name=DEFAULT_DISPLAY_NAME,
        description="自动呈现全部 pool（builtin + user）",
        pool_count=len(PRESET_SCREENS),
        is_default=True,
    )
    metas: list[CanvasMeta] = [default_meta]

    cdir = canvas_dir()
    if cdir.exists() and cdir.is_dir():
        for path in sorted(cdir.glob("*.json")):
            try:
                raw = json.loads(path.read_text(encoding="utf-8"))
                metas.append(CanvasMeta(
                    name=raw["name"],
                    display_name=raw["name"],
                    description=raw.get("description", ""),
                    pool_count=len(raw.get("pool_refs", [])),
                ))
            except Exception:
                continue  # 损坏文件跳过
    return metas


def load_canvas(name: str) -> Canvas:
    """加载 canvas。name == __default__ → 动态生成含全部 pool 的默认 canvas。

    其他不存在的 name → 抛 FileNotFoundError。
    """
    if name == DEFAULT_NAME:
        from rquant.presets import PRESET_SCREENS
        return Canvas(
            name=DEFAULT_NAME,
            description="自动呈现全部 pool（builtin + user）。新建画布后可独立筛选。",
            pool_refs=list(PRESET_SCREENS.keys()),
            is_default=True,
        )

    path = canvas_path(name)
    if not path.exists():
        raise FileNotFoundError(f"canvas {name} 不存在：{path}")
    raw = json.loads(path.read_text(encoding="utf-8"))
    return Canvas(
        name=raw["name"],
        description=raw.get("description", ""),
        pool_refs=raw.get("pool_refs", []),
        created_at=raw.get("created_at", ""),
        updated_at=raw.get("updated_at", ""),
        source=raw.get("source", "canvas_new"),
        is_default=False,
    )


def save_canvas(
    name: str,
    *,
    description: str,
    pool_refs: list[str],
    source: str = "canvas_new",
) -> Path:
    """写盘 canvas。覆盖式（同名直接覆盖）。"""
    if name == DEFAULT_NAME:
        raise ValueError(f"name 不能是保留名 {DEFAULT_NAME}")
    path = canvas_path(name)
    path.parent.mkdir(parents=True, exist_ok=True)
    existing_raw = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
    payload = {
        "name": name,
        "description": description,
        "pool_refs": pool_refs,
        "created_at": existing_raw.get("created_at") or datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "updated_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": source,
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def delete_canvas(name: str) -> bool:
    """物理删 canvas 文件。默认 canvas 不可删。"""
    if name == DEFAULT_NAME:
        raise ValueError(f"默认 canvas {DEFAULT_NAME} 不可删")
    path = canvas_path(name)
    if not path.exists():
        return False
    path.unlink()
    return True


def add_pool_to_canvas(canvas_name: str, pool_name: str) -> bool:
    """把 pool 加到 canvas.pool_refs（如果还没加）。

    返回 True 表示真加了；False 表示已存在。
    canvas_name == __default__ 时不写文件（默认 canvas 是动态的，不需要存）。
    """
    if canvas_name == DEFAULT_NAME:
        return False  # 默认 canvas 自动 include 所有 pool，无需 add
    canvas = load_canvas(canvas_name)
    if pool_name in canvas.pool_refs:
        return False
    canvas.pool_refs.append(pool_name)
    save_canvas(
        canvas_name,
        description=canvas.description,
        pool_refs=canvas.pool_refs,
        source="canvas_edit",
    )
    return True


def remove_pool_from_canvas(canvas_name: str, pool_name: str) -> bool:
    """从 canvas.pool_refs 移除 pool（不删 pool 本身）。"""
    if canvas_name == DEFAULT_NAME:
        return False
    canvas = load_canvas(canvas_name)
    if pool_name not in canvas.pool_refs:
        return False
    canvas.pool_refs.remove(pool_name)
    save_canvas(
        canvas_name,
        description=canvas.description,
        pool_refs=canvas.pool_refs,
        source="canvas_edit",
    )
    return True


def filter_pool_refs(canvas: Canvas) -> list[str]:
    """过滤掉 canvas.pool_refs 中不在当前 PRESET_SCREENS 的 pool（pool 被删了）。"""
    from rquant.presets import PRESET_SCREENS
    return [p for p in canvas.pool_refs if p in PRESET_SCREENS]


def canvas_membership_of(pool_name: str) -> list[str]:
    """返回引用了 pool_name 的 canvas 名列表（含虚拟默认 canvas，因为它自动 include 全部）。

    用法：在 pool 详情显示"该 pool 出现在哪些 canvas 中"。
    """
    members: list[str] = [DEFAULT_NAME]  # 默认 canvas 永远 include 全部 pool
    cdir = canvas_dir()
    if cdir.exists() and cdir.is_dir():
        for path in sorted(cdir.glob("*.json")):
            try:
                raw = json.loads(path.read_text(encoding="utf-8"))
                if pool_name in raw.get("pool_refs", []):
                    members.append(raw["name"])
            except Exception:
                continue
    return members


def set_canvas_pool_refs(canvas_name: str, pool_refs: list[str]) -> bool:
    """C-Canvas-2: 直接 override canvas.pool_refs（用户在「管理 pool」弹窗多选后调用）。

    默认 canvas 不可 override（自动 include 所有）。
    """
    if canvas_name == DEFAULT_NAME:
        return False
    canvas = load_canvas(canvas_name)
    save_canvas(
        canvas_name,
        description=canvas.description,
        pool_refs=pool_refs,
        source="canvas_edit",
    )
    return True
