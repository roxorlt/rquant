# Week 7 NL 选股 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 用户用一句中文描述筛选意图（如"MA5 上穿 MA20 + 量比 > 2 + 流通市值 < 100 亿"），dashboard 调 DeepSeek-V4-Flash 解析为结构化 ScreenPlan，分层卡片预览，确认后执行 `screen()` 出结果，可一键保存为 user preset 接入 daily pipeline。

**Architecture:** 新增 `src/rquant/llm/` 包装 LLM 调用层，Pydantic Settings 读 `DEEPSEEK_API_KEY`，OpenAI Python SDK（DeepSeek 兼容）调单一 `build_screen` tool，rule name 在 schema 中以 enum 强约束。规则注册表 `RuleSpec` 用 Pydantic args model（类 LangChain/MCP 范式），每条积木的 args schema 注入 system prompt 供 LLM 参考。Streamlit 用原生 `st.container(border=True)` 渲染分层卡片，Stage Cards 形态为 Week 7.5 真画布预留数据结构。

**Tech Stack:** Python 3.12 + uv，OpenAI SDK 1.0+（DeepSeek base_url），Pydantic v2，Streamlit 1.30+，loguru，pytest。

**参考文档：** `docs/plans/2026-04-30-week7-nl-screen-design.md`（设计决策、UI mockup、数据流、错误恢复策略详见此文）。

**工作目录：** 所有命令在 worktree 根目录执行：`/Users/roxor/brain/30-projects/rQuant/.worktrees/feat-week7-nl-screen`。`uv run` 会自动激活 venv。`.env` / `data/` / `logs/` 已 symlink 共用 main 的资源。

---

## File Structure

**新增文件：**

| 文件 | 责任 |
|---|---|
| `src/rquant/llm/__init__.py` | 包入口，导出 `DeepSeekClient`, `ScreenPlan`, `REGISTRY` |
| `src/rquant/llm/schemas.py` | `RuleCall`, `Stage`, `ScreenPlan` Pydantic 模型 |
| `src/rquant/llm/registry.py` | `RuleSpec` + 25 条 args model + `REGISTRY` + `get_rule_spec()` |
| `src/rquant/llm/schema_export.py` | `to_openai_tools()` 生成 OpenAI Tool Calls schema |
| `src/rquant/llm/dispatch.py` | `screen_plan_to_rules()` 把 ScreenPlan 翻译成 `list[Rule]` |
| `src/rquant/llm/prompts.py` | `build_system_prompt()` + few-shot examples |
| `src/rquant/llm/client.py` | `DeepSeekClient` + `nl_to_screen_plan()` + jsonl logger |
| `tests/unit/llm/__init__.py` | 测试包 init |
| `tests/unit/llm/test_schemas.py` | ScreenPlan / Stage / RuleCall 校验 |
| `tests/unit/llm/test_registry.py` | RuleSpec 实例化、args_model 校验 |
| `tests/unit/llm/test_registry_complete.py` | REGISTRY 与 `screen.__all__` 一一对应 |
| `tests/unit/llm/test_schema_export.py` | `to_openai_tools()` 输出符合 OpenAI 规范 |
| `tests/unit/llm/test_dispatch.py` | ScreenPlan → screen() 端到端 |
| `tests/unit/llm/test_client.py` | DeepSeekClient mock 测试 |
| `tests/unit/llm/test_user_presets.py` | `load_user_presets()` 从 JSON 加载 |

**修改文件：**

| 文件 | 改动 |
|---|---|
| `pyproject.toml` | + `openai>=1.0` 依赖 |
| `src/rquant/config.py` | + `deepseek_api_key`, `deepseek_base_url`, `deepseek_model` 字段 |
| `src/rquant/presets.py` | + `load_user_presets()` 启动时合并 user/ preset |
| `src/rquant/dashboard/app.py` | + 新 tab `🤖 NL 选股` |
| `.env.example` | 已有 DEEPSEEK_* 占位（scaffold 阶段已加） |

---

## Task 1: ScreenPlan / Stage / RuleCall Pydantic 模型

**Files:**
- Create: `src/rquant/llm/__init__.py`
- Create: `src/rquant/llm/schemas.py`
- Create: `tests/unit/llm/__init__.py`
- Create: `tests/unit/llm/test_schemas.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/unit/llm/test_schemas.py
"""ScreenPlan / Stage / RuleCall Pydantic 模型测试。"""

import pytest
from pydantic import ValidationError

from rquant.llm.schemas import RuleCall, ScreenPlan, Stage


class TestRuleCall:
    def test_minimal(self) -> None:
        rc = RuleCall(name="not_st")
        assert rc.name == "not_st"
        assert rc.args == {}

    def test_with_args(self) -> None:
        rc = RuleCall(name="circ_mv_lt", args={"threshold_yi": 100})
        assert rc.args["threshold_yi"] == 100

    def test_name_required(self) -> None:
        with pytest.raises(ValidationError):
            RuleCall()  # type: ignore[call-arg]


class TestStage:
    def test_minimal(self) -> None:
        s = Stage(label="基础过滤", rules=[])
        assert s.label == "基础过滤"
        assert s.rules == []

    def test_with_rules(self) -> None:
        s = Stage(
            label="形态",
            rules=[RuleCall(name="first_limit_up", args={"offset": 1})],
        )
        assert len(s.rules) == 1


class TestScreenPlan:
    def test_minimal(self) -> None:
        plan = ScreenPlan(trade_date="2026-04-30", stages=[])
        assert plan.trade_date == "2026-04-30"
        assert plan.stages == []
        assert plan.include_columns == []
        assert plan.rationale == ""

    def test_full(self) -> None:
        plan = ScreenPlan(
            trade_date="2026-04-30",
            stages=[
                Stage(label="过滤", rules=[RuleCall(name="not_st")]),
                Stage(label="形态", rules=[
                    RuleCall(name="first_limit_up", args={"offset": 1}),
                ]),
            ],
            rationale="测试",
        )
        assert len(plan.stages) == 2

    def test_flatten_rules(self) -> None:
        plan = ScreenPlan(
            trade_date="2026-04-30",
            stages=[
                Stage(label="A", rules=[RuleCall(name="r1"), RuleCall(name="r2")]),
                Stage(label="B", rules=[RuleCall(name="r3")]),
            ],
        )
        flat = plan.flatten_rules()
        assert [rc.name for rc in flat] == ["r1", "r2", "r3"]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/roxor/brain/30-projects/rQuant/.worktrees/feat-week7-nl-screen && uv run pytest tests/unit/llm/test_schemas.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'rquant.llm'`

- [ ] **Step 3: Create the module + minimal schemas**

```python
# src/rquant/llm/__init__.py
"""LLM 集成层：NL → ScreenPlan → screen()。"""

from rquant.llm.schemas import RuleCall, ScreenPlan, Stage

__all__ = ["RuleCall", "Stage", "ScreenPlan"]
```

```python
# src/rquant/llm/schemas.py
"""LLM 解析产出的结构化数据模型。"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class RuleCall(BaseModel):
    """单条积木调用。name 必须存在于 REGISTRY，args 由各 RuleSpec.args_model 校验。"""

    name: str = Field(..., min_length=1)
    args: dict[str, Any] = Field(default_factory=dict)


class Stage(BaseModel):
    """筛选 pipeline 一层：用户友好分组（基础过滤 / 形态 / 时机 等）。
    内部规则 AND 合取，跨 stage 也是 AND——分组只为 UI 渲染和认知组织。
    """

    label: str = Field(..., min_length=1)
    rules: list[RuleCall] = Field(default_factory=list)


class ScreenPlan(BaseModel):
    """LLM 解析的完整选股方案。

    Week 7.5 升级到真画布时此结构兼容扩展（stages → DAG nodes）。
    """

    trade_date: str = Field(..., min_length=10, max_length=10,
                            description="YYYY-MM-DD")
    stages: list[Stage] = Field(default_factory=list)
    include_columns: list[str] = Field(default_factory=list)
    rationale: str = Field(default="")

    def flatten_rules(self) -> list[RuleCall]:
        """跨 stage 合并所有规则；语义上等价于 list of rules（AND 合取）。"""
        return [rc for s in self.stages for rc in s.rules]
```

```python
# tests/unit/llm/__init__.py
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/llm/test_schemas.py -v`
Expected: 7 passed

- [ ] **Step 5: Commit**

```bash
git add src/rquant/llm/__init__.py src/rquant/llm/schemas.py \
        tests/unit/llm/__init__.py tests/unit/llm/test_schemas.py
git commit -m "feat(llm): add ScreenPlan/Stage/RuleCall Pydantic schemas"
```

---

## Task 2: RuleSpec dataclass + Args models + 前 5 条规则注册

**Files:**
- Create: `src/rquant/llm/registry.py`
- Create: `tests/unit/llm/test_registry.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/unit/llm/test_registry.py
"""RuleSpec + REGISTRY 测试。"""

import pytest
from pydantic import BaseModel, ValidationError

from rquant.llm.registry import (
    REGISTRY,
    REGISTRY_BY_NAME,
    RuleSpec,
    get_rule_spec,
)


class TestRegistryStructure:
    def test_registry_not_empty(self) -> None:
        assert len(REGISTRY) >= 5  # 第一轮至少 5 条

    def test_registry_by_name_lookup(self) -> None:
        assert "not_st" in REGISTRY_BY_NAME
        assert isinstance(REGISTRY_BY_NAME["not_st"], RuleSpec)

    def test_get_rule_spec_known(self) -> None:
        spec = get_rule_spec("not_st")
        assert spec.name == "not_st"
        assert spec.category == "filter"

    def test_get_rule_spec_unknown_raises(self) -> None:
        with pytest.raises(ValueError, match="Unknown rule"):
            get_rule_spec("nonexistent_rule")

    def test_registry_names_unique(self) -> None:
        names = [s.name for s in REGISTRY]
        assert len(names) == len(set(names))


class TestArgsModelValidation:
    def test_no_args_rule(self) -> None:
        spec = get_rule_spec("not_st")
        # _NoArgs 接受空 dict
        validated = spec.args_model.model_validate({})
        assert validated.model_dump() == {}

    def test_no_args_rule_rejects_extra(self) -> None:
        spec = get_rule_spec("not_st")
        with pytest.raises(ValidationError):
            spec.args_model.model_validate({"foo": "bar"})

    def test_circ_mv_lt_args(self) -> None:
        spec = get_rule_spec("circ_mv_lt")
        validated = spec.args_model.model_validate({"threshold_yi": 100})
        assert validated.threshold_yi == 100  # type: ignore[attr-defined]

    def test_circ_mv_lt_rejects_negative(self) -> None:
        spec = get_rule_spec("circ_mv_lt")
        with pytest.raises(ValidationError):
            spec.args_model.model_validate({"threshold_yi": -10})


class TestRuleSpecCallable:
    def test_each_rule_instantiates_with_default_args(self) -> None:
        """每条 RuleSpec.fn(**args_model().model_dump()) 应能产出 callable Rule。"""
        for spec in REGISTRY:
            try:
                args = spec.args_model.model_construct().model_dump(exclude_unset=True)
                # Required-only fields can't be defaulted; skip those rules
                if any(f.is_required() for f in spec.args_model.model_fields.values()):
                    continue
                rule = spec.fn(**args)
                assert callable(rule), f"{spec.name} produced non-callable"
            except Exception as e:
                pytest.fail(f"{spec.name} failed: {e}")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/llm/test_registry.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'rquant.llm.registry'`

- [ ] **Step 3: Create registry.py with RuleSpec + 5 rules**

```python
# src/rquant/llm/registry.py
"""LLM-facing 规则注册表：每条积木 → RuleSpec（描述、参数 schema、示例、分类）。

向 LLM 暴露的语义层与 screen.rules 实现层解耦。
新加积木时必须在此注册（test_registry_complete.py 会校验对应关系）。
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from rquant.screen.rules import (
    above_ma,
    between,
    board_in,
    circ_mv_lt,
    consecutive_ups_gte,
    cross_above,
    cross_below,
    first_limit_up,
    gt,
    gte,
    has_lower_shadow,
    has_prior_limit_up,
    limit_down,
    limit_up,
    lt,
    lte,
    no_consec_ups_in_window,
    no_limit_down_in_window,
    not_bj,
    not_limit_up,
    not_st,
    not_yiziban,
    rsi_overbought,
    rsi_oversold,
    volume_ratio_gte,
    yiziban,
)

# ── Args models ───────────────────────────────────────────────────────────────


class _NoArgs(BaseModel):
    """无参积木的占位 args model。"""

    model_config = ConfigDict(extra="forbid")


class OffsetArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")
    offset: int = Field(0, ge=0, le=30,
                        description="距 T 日的偏移；0=今天, 1=昨天, 2=前天")


class CircMvLtArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")
    threshold_yi: float = Field(..., gt=0, le=10000,
                                description="流通市值阈值（亿元）")
    offset: int = Field(0, ge=0, le=30)


# ── RuleSpec ──────────────────────────────────────────────────────────────────

Category = Literal["filter", "state", "indicator", "shape", "aggregate", "compare"]


@dataclass(frozen=True)
class RuleSpec:
    """LLM-facing 规则描述。"""

    name: str
    description: str
    args_model: type[BaseModel]
    examples: list[str]
    category: Category
    fn: Callable


REGISTRY: list[RuleSpec] = [
    RuleSpec(
        name="not_st",
        description="排除 ST / *ST / SST 标的",
        args_model=_NoArgs,
        examples=["排除 ST", "不要 ST 和 *ST"],
        category="filter",
        fn=not_st,
    ),
    RuleSpec(
        name="not_bj",
        description="排除北交所标的",
        args_model=_NoArgs,
        examples=["不要北交所", "排除北交所"],
        category="filter",
        fn=not_bj,
    ),
    RuleSpec(
        name="circ_mv_lt",
        description="流通市值 < threshold_yi 亿元",
        args_model=CircMvLtArgs,
        examples=["流通市值 < 100 亿", "小盘 50 亿以下", "微盘 30 亿"],
        category="filter",
        fn=circ_mv_lt,
    ),
    RuleSpec(
        name="first_limit_up",
        description="某日首板（今涨停且昨未涨停）",
        args_model=OffsetArgs,
        examples=["昨日首板", "T-1 首板", "前天首板（offset=2）"],
        category="state",
        fn=first_limit_up,
    ),
    RuleSpec(
        name="not_limit_up",
        description="某日未涨停",
        args_model=OffsetArgs,
        examples=["今日未涨停（offset=0）", "昨日未涨停"],
        category="state",
        fn=not_limit_up,
    ),
]


REGISTRY_BY_NAME: dict[str, RuleSpec] = {spec.name: spec for spec in REGISTRY}


def get_rule_spec(name: str) -> RuleSpec:
    """按 name 取 RuleSpec；未注册抛 ValueError。"""
    if name not in REGISTRY_BY_NAME:
        raise ValueError(f"Unknown rule: {name!r}")
    return REGISTRY_BY_NAME[name]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/llm/test_registry.py -v`
Expected: all pass (≥9 tests)

- [ ] **Step 5: Commit**

```bash
git add src/rquant/llm/registry.py tests/unit/llm/test_registry.py
git commit -m "feat(llm): add RuleSpec + first 5 registry entries"
```

---

## Task 3: 完成剩余 20 条规则注册 + 完整性校验测试

**Files:**
- Modify: `src/rquant/llm/registry.py`（追加 args models + 注册项）
- Create: `tests/unit/llm/test_registry_complete.py`

- [ ] **Step 1: Write the failing completeness test**

```python
# tests/unit/llm/test_registry_complete.py
"""校验 REGISTRY 与 screen.__all__ 中可调用积木一一对应。
未注册的积木 = LLM 看不到 = 用户用不到，必须被发现并修复。
"""

import rquant.screen as screen_pkg
from rquant.llm.registry import REGISTRY_BY_NAME

# 跳过非积木导出（基础设施 / 类型）
NON_RULE_EXPORTS = {"screen", "load_universe", "AggregateRequest"}


def _all_rule_names_in_screen() -> set[str]:
    """从 screen.__all__ 取所有公开的积木名。"""
    return set(screen_pkg.__all__) - NON_RULE_EXPORTS


def test_every_screen_rule_is_registered() -> None:
    screen_rules = _all_rule_names_in_screen()
    registered = set(REGISTRY_BY_NAME.keys())
    missing = screen_rules - registered
    assert not missing, f"未注册的积木：{sorted(missing)}"


def test_every_registered_rule_exists_in_screen() -> None:
    screen_rules = _all_rule_names_in_screen()
    registered = set(REGISTRY_BY_NAME.keys())
    extra = registered - screen_rules
    assert not extra, f"REGISTRY 中存在 screen 未导出的规则：{sorted(extra)}"
```

- [ ] **Step 2: Run completeness test to verify it fails**

Run: `uv run pytest tests/unit/llm/test_registry_complete.py -v`
Expected: FAIL with `未注册的积木：['above_ma', 'between', 'board_in', ...]`

- [ ] **Step 3: 在 registry.py 追加剩余 20 条**

在 `src/rquant/llm/registry.py` 中：
1. 在 `# ── Args models ──` 段落追加以下 args models（紧跟现有的 `CircMvLtArgs` 之后）：

```python
class BoardInArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")
    boards: list[Literal["main", "gem", "star", "bj"]] = Field(
        ..., min_length=1,
        description="允许的板块白名单：main=主板, gem=创业板, star=科创板, bj=北交所",
    )


class ConsecutiveUpsGteArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")
    n: int = Field(..., ge=1, le=20, description="连板数下限（含）")
    offset: int = Field(0, ge=0, le=30)


class HasLowerShadowArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")
    min_ratio: float = Field(1.5, gt=0, le=20,
                             description="下影线/实体 比下限")
    min_amplitude: float = Field(0.02, ge=0, le=0.30,
                                 description="日振幅下限（小数，0.02=2%）")
    offset: int = Field(0, ge=0, le=30)


class NoConsecUpsInWindowArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")
    threshold: int = Field(3, ge=1, le=20,
                           description="若窗口内最高连板数 ≥ threshold 则被排除")
    window: int = Field(8, ge=1, le=120, description="回看窗口（交易日）")


class NoLimitDownInWindowArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")
    window: int = Field(30, ge=1, le=250)


class HasPriorLimitUpArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")
    window: int = Field(120, ge=1, le=500)
    exclude_offset: int = Field(1, ge=0, le=30,
                                description="排除某偏移日，避免规则与 first_limit_up 冲突")


class CompareArgs(BaseModel):
    """gt / lt / gte / lte 通用 args。
    operand 形如 'CLOSE[0]' / 'MA5[1]' / 'CIRC_MV[0]'，或字面量数字。
    """
    model_config = ConfigDict(extra="forbid")
    left: str | float = Field(..., description="左操作数（字段名或数字）")
    right: str | float = Field(..., description="右操作数（字段名或数字）")


class BetweenArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")
    field: str = Field(..., description="字段名，如 'CLOSE[0]', 'PCT_CHG[0]'")
    low: float
    high: float


class CrossArgs(BaseModel):
    """cross_above / cross_below 通用 args。"""
    model_config = ConfigDict(extra="forbid")
    fast: str = Field(..., description="快线名，如 'MA5'")
    slow: str = Field(..., description="慢线名，如 'MA20'")
    offset: int = Field(0, ge=0, le=30)


class AboveMaArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")
    period: int = Field(..., ge=2, le=250, description="均线周期（5/10/20/60 常见）")
    offset: int = Field(0, ge=0, le=30)


class RsiArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")
    period: int = Field(14, ge=2, le=60)
    threshold: float = Field(..., ge=0, le=100,
                             description="超卖一般 ≤30，超买一般 ≥70")
    offset: int = Field(0, ge=0, le=30)


class VolumeRatioArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")
    n: float = Field(..., gt=0, le=20,
                     description="量比下限（n=2 表示当日量 ≥ 前 window 日均量×2）")
    offset: int = Field(0, ge=0, le=30)
    window: int = Field(5, ge=1, le=60)
```

2. 在 `REGISTRY: list[RuleSpec] = [` 中追加以下 entry（接在现有 `not_limit_up` 后、闭合 `]` 前）：

```python
    # ── filter (board) ──
    RuleSpec(
        name="board_in",
        description="板块白名单（main=主板/gem=创业板/star=科创板/bj=北交所）",
        args_model=BoardInArgs,
        examples=["只看主板", "主板+创业板", "排除科创板和北交所等价于 boards=['main','gem']"],
        category="filter",
        fn=board_in,
    ),
    # ── state (limit-up family) ──
    RuleSpec(
        name="limit_up",
        description="某日涨停",
        args_model=OffsetArgs,
        examples=["今日涨停", "昨日涨停（offset=1）"],
        category="state",
        fn=limit_up,
    ),
    RuleSpec(
        name="limit_down",
        description="某日跌停",
        args_model=OffsetArgs,
        examples=["今日跌停"],
        category="state",
        fn=limit_down,
    ),
    RuleSpec(
        name="yiziban",
        description="某日一字板",
        args_model=OffsetArgs,
        examples=["今日一字板"],
        category="state",
        fn=yiziban,
    ),
    RuleSpec(
        name="not_yiziban",
        description="某日非一字板",
        args_model=OffsetArgs,
        examples=["昨日不是一字板（offset=1）"],
        category="state",
        fn=not_yiziban,
    ),
    RuleSpec(
        name="consecutive_ups_gte",
        description="某日连板数 ≥ n",
        args_model=ConsecutiveUpsGteArgs,
        examples=["昨日 3 连板（n=3, offset=1）", "今日 2 连板"],
        category="state",
        fn=consecutive_ups_gte,
    ),
    # ── shape ──
    RuleSpec(
        name="has_lower_shadow",
        description="下影线达标：下影/实体 ≥ min_ratio 且振幅 ≥ min_amplitude",
        args_model=HasLowerShadowArgs,
        examples=["有明显下影（min_ratio=1.5）", "强势下影（min_ratio=2, min_amplitude=0.03）"],
        category="shape",
        fn=has_lower_shadow,
    ),
    # ── compare（通用比较）──
    RuleSpec(
        name="gt",
        description="left > right；操作数可以是字段名（'CLOSE[0]'）或数字常数",
        args_model=CompareArgs,
        examples=["今最高 > 昨收（left='HIGH[0]', right='CLOSE[1]'）", "PCT_CHG > 5（left='PCT_CHG[0]', right=5）"],
        category="compare",
        fn=gt,
    ),
    RuleSpec(
        name="lt",
        description="left < right",
        args_model=CompareArgs,
        examples=["今实体顶 < 昨实体顶（'BODY_UPPER[0]', 'BODY_UPPER[1]'）"],
        category="compare",
        fn=lt,
    ),
    RuleSpec(
        name="gte",
        description="left >= right",
        args_model=CompareArgs,
        examples=[],
        category="compare",
        fn=gte,
    ),
    RuleSpec(
        name="lte",
        description="left <= right",
        args_model=CompareArgs,
        examples=[],
        category="compare",
        fn=lte,
    ),
    RuleSpec(
        name="between",
        description="字段值在 [low, high] 闭区间",
        args_model=BetweenArgs,
        examples=["PCT_CHG 在 -2~5 之间（field='PCT_CHG[0]', low=-2, high=5）"],
        category="compare",
        fn=between,
    ),
    # ── indicator ──
    RuleSpec(
        name="cross_above",
        description="fast 均线在 offset 日上穿 slow 均线",
        args_model=CrossArgs,
        examples=["MA5 上穿 MA20（fast='MA5', slow='MA20'）", "今日 MA10 上穿 MA60"],
        category="indicator",
        fn=cross_above,
    ),
    RuleSpec(
        name="cross_below",
        description="fast 均线在 offset 日下穿 slow 均线",
        args_model=CrossArgs,
        examples=["MA5 下穿 MA20"],
        category="indicator",
        fn=cross_below,
    ),
    RuleSpec(
        name="above_ma",
        description="CLOSE 在 offset 日高于 MA{period}",
        args_model=AboveMaArgs,
        examples=["今收高于 MA20（period=20）", "昨收高于 MA60"],
        category="indicator",
        fn=above_ma,
    ),
    RuleSpec(
        name="rsi_oversold",
        description="RSI{period} 低于 threshold（默认 30）",
        args_model=RsiArgs,
        examples=["RSI14 超卖（period=14, threshold=30）", "RSI 低于 25"],
        category="indicator",
        fn=rsi_oversold,
    ),
    RuleSpec(
        name="rsi_overbought",
        description="RSI{period} 高于 threshold（默认 70）",
        args_model=RsiArgs,
        examples=["RSI14 超买（period=14, threshold=70）"],
        category="indicator",
        fn=rsi_overbought,
    ),
    RuleSpec(
        name="volume_ratio_gte",
        description="某日成交量 ≥ n × 前 window 日成交量均值",
        args_model=VolumeRatioArgs,
        examples=["量比 ≥ 2（n=2）", "放量超 3 倍 5 日均量（n=3, window=5）"],
        category="indicator",
        fn=volume_ratio_gte,
    ),
    # ── aggregate ──
    RuleSpec(
        name="no_consec_ups_in_window",
        description="近 window 日内最高连板数 < threshold（避开高位连板股）",
        args_model=NoConsecUpsInWindowArgs,
        examples=["近 8 日无 3 连板及以上（threshold=3, window=8）"],
        category="aggregate",
        fn=no_consec_ups_in_window,
    ),
    RuleSpec(
        name="no_limit_down_in_window",
        description="近 window 日无跌停",
        args_model=NoLimitDownInWindowArgs,
        examples=["近 30 日无跌停（window=30）"],
        category="aggregate",
        fn=no_limit_down_in_window,
    ),
    RuleSpec(
        name="has_prior_limit_up",
        description="近 window 日（排除 T-exclude_offset 日）至少 1 次涨停（验证活跃度）",
        args_model=HasPriorLimitUpArgs,
        examples=["近 120 日有过涨停（window=120, exclude_offset=1）"],
        category="aggregate",
        fn=has_prior_limit_up,
    ),
```

- [ ] **Step 4: Run completeness test to verify it passes**

Run: `uv run pytest tests/unit/llm/test_registry_complete.py tests/unit/llm/test_registry.py -v`
Expected: all pass; REGISTRY 现在 25 条

- [ ] **Step 5: Commit**

```bash
git add src/rquant/llm/registry.py tests/unit/llm/test_registry_complete.py
git commit -m "feat(llm): register all 25 rules + completeness check"
```

---

## Task 4: `to_openai_tools()` Schema Export

**Files:**
- Create: `src/rquant/llm/schema_export.py`
- Create: `tests/unit/llm/test_schema_export.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/unit/llm/test_schema_export.py
"""to_openai_tools() 输出格式校验。"""

from rquant.llm.registry import REGISTRY
from rquant.llm.schema_export import build_rule_catalog_md, to_openai_tools


class TestOpenAITools:
    def test_returns_single_tool(self) -> None:
        tools = to_openai_tools()
        assert len(tools) == 1

    def test_tool_has_function_type(self) -> None:
        tool = to_openai_tools()[0]
        assert tool["type"] == "function"
        assert tool["function"]["name"] == "build_screen"

    def test_rule_name_is_enum(self) -> None:
        tool = to_openai_tools()[0]
        params = tool["function"]["parameters"]
        rule_name_schema = (
            params["properties"]["stages"]["items"]["properties"]["rules"]
            ["items"]["properties"]["name"]
        )
        assert rule_name_schema["type"] == "string"
        enum_names = set(rule_name_schema["enum"])
        registry_names = {s.name for s in REGISTRY}
        assert enum_names == registry_names

    def test_required_fields(self) -> None:
        tool = to_openai_tools()[0]
        params = tool["function"]["parameters"]
        assert "trade_date" in params["required"]
        assert "stages" in params["required"]


class TestRuleCatalog:
    def test_catalog_lists_all_rules(self) -> None:
        md = build_rule_catalog_md()
        for spec in REGISTRY:
            assert spec.name in md, f"{spec.name} not in catalog"

    def test_catalog_includes_args_schema(self) -> None:
        md = build_rule_catalog_md()
        # CircMvLtArgs.threshold_yi 必须出现
        assert "threshold_yi" in md

    def test_catalog_groups_by_category(self) -> None:
        md = build_rule_catalog_md()
        for cat in ("filter", "state", "shape", "indicator", "aggregate", "compare"):
            assert cat.upper() in md or cat in md.lower()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/llm/test_schema_export.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'rquant.llm.schema_export'`

- [ ] **Step 3: Implement schema_export.py**

```python
# src/rquant/llm/schema_export.py
"""把 REGISTRY 转成 OpenAI Tool Calls schema + LLM 可读的规则目录 markdown。"""

from __future__ import annotations

import json

from rquant.llm.registry import REGISTRY, RuleSpec

# build_screen 是 LLM 唯一可调的工具；rule.name 是 enum 强约束，
# rule.args 是 free-form dict（详细 schema 通过 system prompt 中的目录传给 LLM）。
_BUILD_SCREEN_DESCRIPTION = (
    "构建 A 股选股方案。stages 是有序分层（基础过滤→形态→时机→指标），"
    "每个 stage 含 label（中文）+ rules。所有 rule 跨 stage AND 合取。"
    "rule.name 必须是预定义积木，rule.args 须严格匹配该积木的参数 schema（见 system 提示中的目录）。"
)


def to_openai_tools() -> list[dict]:
    """生成 OpenAI Tool Calls 兼容的 tools list。仅一个 build_screen tool。"""
    rule_names = [spec.name for spec in REGISTRY]

    schema = {
        "type": "object",
        "properties": {
            "trade_date": {
                "type": "string",
                "description": "YYYY-MM-DD；缺失时由调用方填默认值（最新交易日）",
            },
            "stages": {
                "type": "array",
                "minItems": 1,
                "items": {
                    "type": "object",
                    "properties": {
                        "label": {"type": "string", "minLength": 1,
                                  "description": "中文分组名（基础过滤 / 形态 / 时机 等）"},
                        "rules": {
                            "type": "array",
                            "minItems": 1,
                            "items": {
                                "type": "object",
                                "properties": {
                                    "name": {
                                        "type": "string",
                                        "enum": rule_names,
                                        "description": "积木名；必须是 enum 中的一个",
                                    },
                                    "args": {
                                        "type": "object",
                                        "description": "积木参数；按系统提示中的 args schema 填",
                                    },
                                },
                                "required": ["name"],
                                "additionalProperties": False,
                            },
                        },
                    },
                    "required": ["label", "rules"],
                    "additionalProperties": False,
                },
            },
            "include_columns": {
                "type": "array",
                "items": {"type": "string"},
                "description": "结果中额外展示的字段（如 'CIRC_MV[0]', 'CONSECUTIVE_LIMIT_UPS[1]'）",
            },
            "rationale": {
                "type": "string",
                "description": "用一两句话说明你为什么这样组合规则",
            },
        },
        "required": ["trade_date", "stages"],
        "additionalProperties": False,
    }

    return [{
        "type": "function",
        "function": {
            "name": "build_screen",
            "description": _BUILD_SCREEN_DESCRIPTION,
            "parameters": schema,
        },
    }]


def build_rule_catalog_md() -> str:
    """生成给 LLM 看的规则目录 markdown，按 category 分组，含每条 args schema。"""
    by_cat: dict[str, list[RuleSpec]] = {}
    for spec in REGISTRY:
        by_cat.setdefault(spec.category, []).append(spec)

    cat_order = ["filter", "state", "shape", "indicator", "aggregate", "compare"]
    cat_titles = {
        "filter": "FILTER（板块/属性过滤）",
        "state": "STATE（涨跌停状态）",
        "shape": "SHAPE（K线形态）",
        "indicator": "INDICATOR（技术指标）",
        "aggregate": "AGGREGATE（历史窗口聚合）",
        "compare": "COMPARE（通用比较，操作数为字段名或数字）",
    }

    out: list[str] = []
    for cat in cat_order:
        if cat not in by_cat:
            continue
        out.append(f"\n## {cat_titles[cat]}\n")
        for spec in by_cat[cat]:
            args_schema = spec.args_model.model_json_schema()
            args_brief = _summarize_args(args_schema)
            out.append(f"- **{spec.name}**({args_brief}) — {spec.description}")
            if spec.examples:
                out.append(f"  · 示例：{' | '.join(spec.examples)}")
            # 完整 args schema（紧凑 JSON，供 LLM 校验参数类型/范围）
            out.append(f"  · args schema: `{json.dumps(args_schema, ensure_ascii=False, separators=(',', ':'))}`")
    return "\n".join(out)


def _summarize_args(schema: dict) -> str:
    """从 JSON schema 抽出参数签名简写，如 'offset: int=0'。"""
    props = schema.get("properties", {})
    if not props:
        return ""
    parts: list[str] = []
    for k, v in props.items():
        t = v.get("type", "any")
        if "default" in v:
            parts.append(f"{k}:{t}={v['default']}")
        else:
            parts.append(f"{k}:{t}")
    return ", ".join(parts)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/llm/test_schema_export.py -v`
Expected: 7 passed

- [ ] **Step 5: Commit**

```bash
git add src/rquant/llm/schema_export.py tests/unit/llm/test_schema_export.py
git commit -m "feat(llm): export REGISTRY as OpenAI tool schema + rule catalog md"
```

---

## Task 5: Dispatch — ScreenPlan → list[Rule] + 端到端 screen()

**Files:**
- Create: `src/rquant/llm/dispatch.py`
- Create: `tests/unit/llm/test_dispatch.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/unit/llm/test_dispatch.py
"""ScreenPlan → screen() 端到端测试（不调 LLM）。"""

import pytest
from pydantic import ValidationError

from rquant.llm.dispatch import build_rules, screen_with_plan
from rquant.llm.schemas import RuleCall, ScreenPlan, Stage


class TestBuildRules:
    def test_empty_plan(self) -> None:
        plan = ScreenPlan(trade_date="2026-04-30", stages=[])
        rules = build_rules(plan)
        assert rules == []

    def test_single_no_args_rule(self) -> None:
        plan = ScreenPlan(
            trade_date="2026-04-30",
            stages=[Stage(label="过滤", rules=[RuleCall(name="not_st")])],
        )
        rules = build_rules(plan)
        assert len(rules) == 1
        assert callable(rules[0])

    def test_rule_with_args(self) -> None:
        plan = ScreenPlan(
            trade_date="2026-04-30",
            stages=[Stage(label="过滤", rules=[
                RuleCall(name="circ_mv_lt", args={"threshold_yi": 100}),
            ])],
        )
        rules = build_rules(plan)
        assert len(rules) == 1

    def test_unknown_rule_raises(self) -> None:
        plan = ScreenPlan(
            trade_date="2026-04-30",
            stages=[Stage(label="x", rules=[RuleCall(name="bogus_rule")])],
        )
        with pytest.raises(ValueError, match="Unknown rule"):
            build_rules(plan)

    def test_invalid_args_raises(self) -> None:
        plan = ScreenPlan(
            trade_date="2026-04-30",
            stages=[Stage(label="x", rules=[
                RuleCall(name="circ_mv_lt", args={"threshold_yi": -10}),
            ])],
        )
        with pytest.raises(ValidationError):
            build_rules(plan)

    def test_multiple_stages_flatten(self) -> None:
        plan = ScreenPlan(
            trade_date="2026-04-30",
            stages=[
                Stage(label="A", rules=[RuleCall(name="not_st"), RuleCall(name="not_bj")]),
                Stage(label="B", rules=[RuleCall(name="first_limit_up", args={"offset": 1})]),
            ],
        )
        rules = build_rules(plan)
        assert len(rules) == 3


class TestScreenWithPlanIntegration:
    """使用 conftest 提供的真 DuckDB fixture 跑一次 end-to-end。"""

    def test_runs_against_db(self, populated_store):  # type: ignore[no-untyped-def]
        plan = ScreenPlan(
            trade_date="2026-04-29",
            stages=[Stage(label="过滤", rules=[RuleCall(name="not_st")])],
        )
        result = screen_with_plan(plan, store=populated_store)
        # 不强求行数，只保证流程跑通
        assert "ts_code" in result.columns
```

- [ ] **Step 2: Confirm conftest 提供 `populated_store` fixture**

Run: `grep -l populated_store tests/conftest.py tests/unit/conftest.py 2>/dev/null`
Expected: 至少一个 conftest 文件命中。如果没有，跳过最后一个集成测试用例（`@pytest.mark.skip` 或改为 mock）。

```bash
grep -rn "def populated_store\|@pytest.fixture" tests/conftest.py tests/unit/ 2>&1 | head -20
```

如果 fixture 名称不同，调整 `test_runs_against_db` 用现有 fixture（如 `duckdb_store` / `store_with_data` 等）。

- [ ] **Step 3: Run tests to verify they fail (无 dispatch.py)**

Run: `uv run pytest tests/unit/llm/test_dispatch.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'rquant.llm.dispatch'`

- [ ] **Step 4: Implement dispatch.py**

```python
# src/rquant/llm/dispatch.py
"""ScreenPlan → list[Rule] → screen() 桥。"""

from __future__ import annotations

import pandas as pd

from rquant.llm.registry import get_rule_spec
from rquant.llm.schemas import ScreenPlan
from rquant.screen.core import screen
from rquant.screen.rules import Rule
from rquant.storage.duckdb import DuckDBStore


def build_rules(plan: ScreenPlan) -> list[Rule]:
    """将 plan.stages 平铺并按 name 查 RuleSpec → 用 args_model 校验 → 实例化 Rule。"""
    rules: list[Rule] = []
    for rc in plan.flatten_rules():
        spec = get_rule_spec(rc.name)  # 不存在 → ValueError
        validated = spec.args_model.model_validate(rc.args)  # 不合法 → ValidationError
        kwargs = validated.model_dump(exclude_unset=False)
        rules.append(spec.fn(**kwargs))
    return rules


def screen_with_plan(
    plan: ScreenPlan,
    *,
    store: DuckDBStore | None = None,
) -> pd.DataFrame:
    """端到端：ScreenPlan → 实例化规则 → screen() 出表。"""
    rules = build_rules(plan)
    return screen(
        trade_date=plan.trade_date,
        rules=rules,
        include_columns=plan.include_columns or None,
        store=store,
    )
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/unit/llm/test_dispatch.py -v`
Expected: 6 passed (or 5 + 1 skipped if fixture name differs)

- [ ] **Step 6: Commit**

```bash
git add src/rquant/llm/dispatch.py tests/unit/llm/test_dispatch.py
git commit -m "feat(llm): dispatch ScreenPlan to screen() with arg validation"
```

---

## Task 6: User Presets Loader（B 路径）

**Files:**
- Modify: `src/rquant/presets.py`（追加 `load_user_presets()`）
- Create: `tests/unit/llm/test_user_presets.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/unit/llm/test_user_presets.py
"""user_presets/*.json → ScreenPreset 加载测试。"""

import json
from pathlib import Path

import pytest

from rquant.presets import load_user_presets


class TestLoadUserPresets:
    def test_empty_dir_returns_empty(self, tmp_path: Path) -> None:
        result = load_user_presets(tmp_path)
        assert result == {}

    def test_nonexistent_dir_returns_empty(self, tmp_path: Path) -> None:
        result = load_user_presets(tmp_path / "not_there")
        assert result == {}

    def test_load_single_preset(self, tmp_path: Path) -> None:
        (tmp_path / "突破新高.json").write_text(json.dumps({
            "name": "突破新高",
            "description": "今日突破 60 日新高",
            "rules": [
                {"name": "not_st", "args": {}},
                {"name": "above_ma", "args": {"period": 60, "offset": 0}},
            ],
            "include_columns": ["CIRC_MV[0]"],
            "created_at": "2026-04-30T15:00:00",
            "source": "nl_input",
        }, ensure_ascii=False))
        result = load_user_presets(tmp_path)
        assert "user/突破新高" in result
        preset = result["user/突破新高"]
        assert preset.name == "user/突破新高"
        assert len(preset.rules) == 2

    def test_load_skips_invalid_json(self, tmp_path: Path) -> None:
        (tmp_path / "bad.json").write_text("not valid json")
        result = load_user_presets(tmp_path)
        assert result == {}  # 不抛，记 warning 后跳过

    def test_load_skips_unknown_rule(self, tmp_path: Path) -> None:
        (tmp_path / "p.json").write_text(json.dumps({
            "name": "p",
            "description": "x",
            "rules": [{"name": "bogus_rule", "args": {}}],
            "created_at": "2026-04-30T00:00:00",
            "source": "nl_input",
        }))
        result = load_user_presets(tmp_path)
        assert result == {}  # 不抛，跳过此 preset
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/llm/test_user_presets.py -v`
Expected: FAIL with `ImportError: cannot import name 'load_user_presets'`

- [ ] **Step 3: Add `load_user_presets()` to presets.py**

在 `src/rquant/presets.py` 末尾追加：

```python
import json as _json
from pathlib import Path as _Path

from loguru import logger as _logger


def load_user_presets(directory: _Path) -> dict[str, ScreenPreset]:
    """从 directory 下的 *.json 加载用户保存的 preset。

    JSON 结构：
        {
          "name": "<base_name>",
          "description": "<NL query>",
          "rules": [{"name": "not_st", "args": {}}, ...],
          "include_columns": [...],
          "created_at": "...",
          "source": "nl_input"
        }

    解析失败的文件跳过（记 warning），不影响其他 preset。

    返回 dict 的 key 是 "user/{base_name}"，前缀强制隔离与代码内置 preset。
    """
    result: dict[str, ScreenPreset] = {}
    if not directory.exists() or not directory.is_dir():
        return result

    # 局部 import 避免循环（registry 依赖 screen.rules）
    from rquant.llm.dispatch import build_rules
    from rquant.llm.schemas import RuleCall, ScreenPlan, Stage

    for path in sorted(directory.glob("*.json")):
        try:
            data = _json.loads(path.read_text(encoding="utf-8"))
            base_name = data["name"]
            full_name = f"user/{base_name}"

            # 把 rules（flat list）包成单 stage，复用 dispatch.build_rules 校验
            plan = ScreenPlan(
                trade_date="1900-01-01",  # placeholder，preset 落库时与日期无关
                stages=[Stage(label="loaded", rules=[
                    RuleCall(name=r["name"], args=r.get("args", {})) for r in data["rules"]
                ])],
                include_columns=data.get("include_columns", []),
            )
            rules = build_rules(plan)

            result[full_name] = ScreenPreset(
                name=full_name,
                description=data.get("description", ""),
                rules=rules,
                include_columns=data.get("include_columns", []),
            )
        except Exception as e:
            _logger.warning(f"加载 user preset 失败 {path.name}: {e}")
            continue

    return result
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/llm/test_user_presets.py -v`
Expected: 5 passed

- [ ] **Step 5: 启动时合并 user presets 到 PRESET_SCREENS**

在 `src/rquant/presets.py` 末尾追加：

```python
from rquant.config import settings as _settings

# 启动时自动 merge user_presets 目录下所有 JSON
_user_presets_dir = _Path(_settings.data_dir) / "user_presets"
PRESET_SCREENS.update(load_user_presets(_user_presets_dir))
```

- [ ] **Step 6: 跑全套 presets 测试，确保不破已有行为**

Run: `uv run pytest tests/unit/test_presets.py tests/unit/llm/test_user_presets.py -v`
Expected: all pass; 既有测试不受影响（user_presets 目录不存在时不影响 PRESET_SCREENS）

- [ ] **Step 7: Commit**

```bash
git add src/rquant/presets.py tests/unit/llm/test_user_presets.py
git commit -m "feat(presets): load data/user_presets/*.json with user/ prefix"
```

---

## Task 7: 添加 openai 依赖 + config 字段

**Files:**
- Modify: `pyproject.toml`
- Modify: `src/rquant/config.py`

- [ ] **Step 1: Add openai dep**

```bash
uv add 'openai>=1.0'
```

Expected: `pyproject.toml` 多一行 `"openai>=1.0"`，`uv.lock` 同步更新。

- [ ] **Step 2: Add config fields**

修改 `src/rquant/config.py`，在 `notify_heartbeat: bool = True` 之后追加：

```python
    # ===== LLM (Week 7) =====
    deepseek_api_key: str = Field(default="")
    deepseek_base_url: str = Field(default="https://api.deepseek.com")
    deepseek_model: str = Field(default="deepseek-v4-flash")

    @property
    def deepseek_enabled(self) -> bool:
        return bool(self.deepseek_api_key)
```

- [ ] **Step 3: Verify settings load**

Run: `uv run python -c "from rquant.config import settings; print('enabled=', settings.deepseek_enabled, 'model=', settings.deepseek_model)"`
Expected: `enabled= True model= deepseek-v4-flash`（worktree 的 .env 通过 symlink 共用 main 的 key）

- [ ] **Step 4: Commit**

```bash
git add pyproject.toml uv.lock src/rquant/config.py
git commit -m "chore(deps): add openai SDK + DeepSeek config fields"
```

---

## Task 8: System Prompt + Few-Shot Examples

**Files:**
- Create: `src/rquant/llm/prompts.py`
- Create: `tests/unit/llm/test_prompts.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/unit/llm/test_prompts.py
"""System prompt 渲染测试。"""

from rquant.llm.prompts import FEW_SHOT_EXAMPLES, build_system_prompt


class TestSystemPrompt:
    def test_includes_rule_catalog(self) -> None:
        prompt = build_system_prompt()
        assert "not_st" in prompt
        assert "circ_mv_lt" in prompt
        assert "cross_above" in prompt

    def test_includes_args_schema(self) -> None:
        prompt = build_system_prompt()
        # CircMvLtArgs.threshold_yi 出现
        assert "threshold_yi" in prompt

    def test_mentions_offset_semantics(self) -> None:
        prompt = build_system_prompt()
        assert "0=今天" in prompt or "offset" in prompt.lower()


class TestFewShot:
    def test_at_least_4_examples(self) -> None:
        assert len(FEW_SHOT_EXAMPLES) >= 4

    def test_each_example_has_user_and_assistant(self) -> None:
        for ex in FEW_SHOT_EXAMPLES:
            assert "user" in ex
            assert "assistant_tool_call" in ex
            assert ex["assistant_tool_call"]["name"] == "build_screen"

    def test_examples_use_registered_rules(self) -> None:
        from rquant.llm.registry import REGISTRY_BY_NAME
        for ex in FEW_SHOT_EXAMPLES:
            args = ex["assistant_tool_call"]["arguments"]
            for stage in args["stages"]:
                for rule in stage["rules"]:
                    assert rule["name"] in REGISTRY_BY_NAME
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/llm/test_prompts.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'rquant.llm.prompts'`

- [ ] **Step 3: Implement prompts.py**

```python
# src/rquant/llm/prompts.py
"""DeepSeek system prompt + few-shot examples。"""

from __future__ import annotations

from typing import Any

from rquant.llm.schema_export import build_rule_catalog_md

_SYSTEM_TEMPLATE = """你是 A 股选股助手。用户用中文描述选股意图，你调用 build_screen 工具构建结构化方案。

# 工作流

1. 解析用户意图：识别其中的过滤条件（市值 / 板块 / 涨停状态 / 技术指标 / K线形态 / 历史窗口聚合）
2. 选择对应积木，按以下推荐顺序分组到 stages：
   - 第 1 层「基础过滤」：not_st / not_bj / circ_mv_lt 等普适过滤
   - 第 2 层「涨停状态」：first_limit_up / not_limit_up / consecutive_ups_gte / yiziban 等
   - 第 3 层「形态」：has_lower_shadow / gt 比较等
   - 第 4 层「技术指标」：cross_above / above_ma / rsi_oversold / volume_ratio_gte 等
   - 第 5 层「历史窗口」：no_consec_ups_in_window / no_limit_down_in_window / has_prior_limit_up
3. 调用 build_screen，stages 有序，每个 stage 含 label（中文）+ rules
4. 在 rationale 中用 1-2 句说明你的组合思路

# 重要规则

- **必须只用积木目录中列出的 name**，不要发明
- **args 必须严格匹配每个积木的参数 schema**（类型、范围、默认值）
- **offset 含义统一**：0=今天（trade_date 当日），1=昨天（T-1），2=前天（T-2），以此类推
- **circ_mv_lt 阈值单位是亿元**（threshold_yi=100 表示 100 亿）
- **如果用户描述含糊或缺关键信息**，不要瞎猜——返回纯文本请用户澄清，不调用 tool
- **通常应包含 not_st**（除非用户明确说"包括 ST"）
- **trade_date 由调用方传入**：除非用户明确指定日期，否则填空字符串 ""，调用方会替换为最新交易日

# 积木目录

{rule_catalog}
"""


def build_system_prompt() -> str:
    """渲染完整 system prompt。"""
    return _SYSTEM_TEMPLATE.format(rule_catalog=build_rule_catalog_md())


# Few-shot examples：每条覆盖一类常见 query
FEW_SHOT_EXAMPLES: list[dict[str, Any]] = [
    {
        "user": "找昨天首板、流通市值 100 亿以下、今天没涨停的票",
        "assistant_tool_call": {
            "name": "build_screen",
            "arguments": {
                "trade_date": "",
                "stages": [
                    {"label": "基础过滤", "rules": [
                        {"name": "not_st", "args": {}},
                        {"name": "not_bj", "args": {}},
                        {"name": "circ_mv_lt", "args": {"threshold_yi": 100}},
                    ]},
                    {"label": "涨停状态", "rules": [
                        {"name": "first_limit_up", "args": {"offset": 1}},
                        {"name": "not_limit_up", "args": {"offset": 0}},
                    ]},
                ],
                "rationale": "先过滤 ST/北交所/大盘股，再用昨日首板+今未涨停定位次新机会。",
            },
        },
    },
    {
        "user": "今天 MA5 上穿 MA20，量比超过 2，主板创业板都行",
        "assistant_tool_call": {
            "name": "build_screen",
            "arguments": {
                "trade_date": "",
                "stages": [
                    {"label": "基础过滤", "rules": [
                        {"name": "not_st", "args": {}},
                        {"name": "board_in", "args": {"boards": ["main", "gem"]}},
                    ]},
                    {"label": "技术指标", "rules": [
                        {"name": "cross_above", "args": {"fast": "MA5", "slow": "MA20", "offset": 0}},
                        {"name": "volume_ratio_gte", "args": {"n": 2.0, "offset": 0, "window": 5}},
                    ]},
                ],
                "rationale": "金叉 + 放量是经典短线信号，主板创业板覆盖大多数活跃标的。",
            },
        },
    },
    {
        "user": "RSI14 超卖、近 30 天没跌停过、不要北交所",
        "assistant_tool_call": {
            "name": "build_screen",
            "arguments": {
                "trade_date": "",
                "stages": [
                    {"label": "基础过滤", "rules": [
                        {"name": "not_st", "args": {}},
                        {"name": "not_bj", "args": {}},
                    ]},
                    {"label": "技术指标", "rules": [
                        {"name": "rsi_oversold", "args": {"period": 14, "threshold": 30, "offset": 0}},
                    ]},
                    {"label": "历史窗口", "rules": [
                        {"name": "no_limit_down_in_window", "args": {"window": 30}},
                    ]},
                ],
                "rationale": "RSI 超卖找潜在反弹候选，30 天无跌停过滤掉趋势恶化的标的。",
            },
        },
    },
    {
        "user": "昨日首板、有下影线、近 8 天没出现 3 连板",
        "assistant_tool_call": {
            "name": "build_screen",
            "arguments": {
                "trade_date": "",
                "stages": [
                    {"label": "基础过滤", "rules": [
                        {"name": "not_st", "args": {}},
                        {"name": "not_bj", "args": {}},
                    ]},
                    {"label": "涨停状态", "rules": [
                        {"name": "first_limit_up", "args": {"offset": 1}},
                        {"name": "not_yiziban", "args": {"offset": 1}},
                    ]},
                    {"label": "形态", "rules": [
                        {"name": "has_lower_shadow", "args": {"min_ratio": 1.5, "min_amplitude": 0.02, "offset": 0}},
                    ]},
                    {"label": "历史窗口", "rules": [
                        {"name": "no_consec_ups_in_window", "args": {"threshold": 3, "window": 8}},
                    ]},
                ],
                "rationale": "昨首板非一字（保证可买入）+ 今日下影（说明承接好），近 8 天无 3 连板避开过热标的。",
            },
        },
    },
]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/llm/test_prompts.py -v`
Expected: 6 passed

- [ ] **Step 5: Commit**

```bash
git add src/rquant/llm/prompts.py tests/unit/llm/test_prompts.py
git commit -m "feat(llm): add system prompt + 4 few-shot examples"
```

---

## Task 9: DeepSeek Client + nl_to_screen_plan

**Files:**
- Create: `src/rquant/llm/client.py`
- Create: `tests/unit/llm/test_client.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/unit/llm/test_client.py
"""DeepSeekClient 单元测试（OpenAI SDK mock）。"""

import json
from unittest.mock import MagicMock

import pytest

from rquant.llm.client import DeepSeekClient, LLMClarificationNeeded
from rquant.llm.schemas import ScreenPlan


def _make_fake_openai_with_tool_call(arguments: dict) -> MagicMock:
    """构造一个返回 build_screen tool_call 的 mock OpenAI client。"""
    fake = MagicMock()
    fake_tool_call = MagicMock()
    fake_tool_call.function.name = "build_screen"
    fake_tool_call.function.arguments = json.dumps(arguments)
    fake.chat.completions.create.return_value = MagicMock(
        choices=[MagicMock(message=MagicMock(
            tool_calls=[fake_tool_call], content=None
        ))],
        usage=MagicMock(prompt_tokens=100, completion_tokens=50),
    )
    return fake


def _make_fake_openai_with_text(text: str) -> MagicMock:
    """构造一个只返回文字（无 tool_call）的 mock client（澄清场景）。"""
    fake = MagicMock()
    fake.chat.completions.create.return_value = MagicMock(
        choices=[MagicMock(message=MagicMock(tool_calls=None, content=text))],
        usage=MagicMock(prompt_tokens=100, completion_tokens=20),
    )
    return fake


class TestNlToScreenPlan:
    def test_basic_tool_call(self) -> None:
        fake = _make_fake_openai_with_tool_call({
            "trade_date": "2026-04-30",
            "stages": [{"label": "x", "rules": [{"name": "not_st", "args": {}}]}],
            "rationale": "test",
        })
        client = DeepSeekClient(api_key="fake", openai_client=fake)
        plan = client.nl_to_screen_plan("test query", today="2026-04-30")
        assert isinstance(plan, ScreenPlan)
        assert plan.trade_date == "2026-04-30"
        assert plan.rationale == "test"

    def test_empty_trade_date_filled_with_today(self) -> None:
        """LLM 返回 trade_date='' 时，client 用 today 填充。"""
        fake = _make_fake_openai_with_tool_call({
            "trade_date": "",
            "stages": [{"label": "x", "rules": [{"name": "not_st", "args": {}}]}],
        })
        client = DeepSeekClient(api_key="fake", openai_client=fake)
        plan = client.nl_to_screen_plan("test", today="2026-04-30")
        assert plan.trade_date == "2026-04-30"

    def test_clarification_response_raises(self) -> None:
        """LLM 返回纯文本 → 抛 LLMClarificationNeeded。"""
        fake = _make_fake_openai_with_text("请问你想筛选哪个板块？")
        client = DeepSeekClient(api_key="fake", openai_client=fake)
        with pytest.raises(LLMClarificationNeeded) as exc:
            client.nl_to_screen_plan("不清楚的需求", today="2026-04-30")
        assert "板块" in str(exc.value)

    def test_no_api_key_raises(self) -> None:
        with pytest.raises(ValueError, match="API key"):
            DeepSeekClient(api_key="")

    def test_writes_jsonl_log(self, tmp_path) -> None:  # type: ignore[no-untyped-def]
        fake = _make_fake_openai_with_tool_call({
            "trade_date": "",
            "stages": [{"label": "x", "rules": [{"name": "not_st", "args": {}}]}],
        })
        log_path = tmp_path / "nl_queries.jsonl"
        client = DeepSeekClient(api_key="fake", openai_client=fake, log_path=log_path)
        client.nl_to_screen_plan("test query", today="2026-04-30")
        assert log_path.exists()
        line = log_path.read_text().strip().splitlines()[-1]
        log = json.loads(line)
        assert log["query"] == "test query"
        assert log["tokens_in"] == 100
        assert log["tokens_out"] == 50
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/llm/test_client.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'rquant.llm.client'`

- [ ] **Step 3: Implement client.py**

```python
# src/rquant/llm/client.py
"""DeepSeek-V4-Flash 客户端：NL → ScreenPlan，OpenAI-compatible。"""

from __future__ import annotations

import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from loguru import logger
from openai import OpenAI
from pydantic import ValidationError

from rquant.llm.prompts import FEW_SHOT_EXAMPLES, build_system_prompt
from rquant.llm.schema_export import to_openai_tools
from rquant.llm.schemas import ScreenPlan


class LLMClarificationNeeded(Exception):
    """LLM 没调 tool，返回了澄清问题。message 即 LLM 原文。"""


class LLMError(Exception):
    """LLM 调用或返回解析失败。"""


class DeepSeekClient:
    """DeepSeek API 包装。线程安全（内部 OpenAI client 是线程安全的）。"""

    def __init__(
        self,
        api_key: str,
        *,
        base_url: str = "https://api.deepseek.com",
        model: str = "deepseek-v4-flash",
        openai_client: OpenAI | None = None,
        log_path: Path | None = None,
        max_retries: int = 3,
    ) -> None:
        if not api_key and openai_client is None:
            raise ValueError("DeepSeek API key required")
        self._client = openai_client or OpenAI(api_key=api_key, base_url=base_url)
        self._model = model
        self._log_path = log_path
        self._max_retries = max_retries

    def nl_to_screen_plan(self, query: str, *, today: str) -> ScreenPlan:
        """解析中文 query 为 ScreenPlan。

        Args:
            query: 中文选股意图
            today: YYYY-MM-DD，用于填充 LLM 留空的 trade_date

        Raises:
            LLMClarificationNeeded: LLM 返回纯文本而非 tool_call（请用户澄清）
            LLMError: API 失败或返回不可解析
            ValidationError: tool_call 参数不通过 Pydantic 校验
        """
        messages = self._build_messages(query)
        tools = to_openai_tools()

        t0 = time.monotonic()
        last_err: Exception | None = None
        for attempt in range(1, self._max_retries + 1):
            try:
                resp = self._client.chat.completions.create(
                    model=self._model,
                    messages=messages,
                    tools=tools,
                    tool_choice="auto",  # 允许 LLM 不调 tool（澄清场景）
                    temperature=0.0,
                )
                break
            except Exception as e:
                last_err = e
                logger.warning(f"DeepSeek attempt {attempt}/{self._max_retries} failed: {e}")
                if attempt < self._max_retries:
                    time.sleep(2 ** (attempt - 1))  # 1s / 2s / 4s
        else:
            raise LLMError(f"DeepSeek 调用失败（{self._max_retries} 次重试均失败）: {last_err}")

        latency_ms = int((time.monotonic() - t0) * 1000)
        msg = resp.choices[0].message
        usage = getattr(resp, "usage", None)
        tokens_in = getattr(usage, "prompt_tokens", 0) if usage else 0
        tokens_out = getattr(usage, "completion_tokens", 0) if usage else 0

        # 澄清场景：LLM 没调 tool
        if not msg.tool_calls:
            text = msg.content or ""
            self._log(query, None, tokens_in, tokens_out, latency_ms, error="clarification")
            raise LLMClarificationNeeded(text)

        tc = msg.tool_calls[0]
        if tc.function.name != "build_screen":
            raise LLMError(f"LLM 调用了未知工具: {tc.function.name}")

        try:
            args = json.loads(tc.function.arguments)
        except json.JSONDecodeError as e:
            raise LLMError(f"tool_call 参数不是合法 JSON: {e}") from e

        # 填充空 trade_date
        if not args.get("trade_date"):
            args["trade_date"] = today

        try:
            plan = ScreenPlan.model_validate(args)
        except ValidationError as e:
            self._log(query, args, tokens_in, tokens_out, latency_ms, error=str(e))
            raise

        self._log(query, plan.model_dump(), tokens_in, tokens_out, latency_ms)
        return plan

    def _build_messages(self, query: str) -> list[dict[str, Any]]:
        """system + few-shot + user。"""
        messages: list[dict[str, Any]] = [{"role": "system", "content": build_system_prompt()}]
        for ex in FEW_SHOT_EXAMPLES:
            messages.append({"role": "user", "content": ex["user"]})
            messages.append({
                "role": "assistant",
                "content": None,
                "tool_calls": [{
                    "id": f"fs_{hash(ex['user']) & 0xffff:04x}",
                    "type": "function",
                    "function": {
                        "name": ex["assistant_tool_call"]["name"],
                        "arguments": json.dumps(
                            ex["assistant_tool_call"]["arguments"],
                            ensure_ascii=False,
                        ),
                    },
                }],
            })
            # tool message confirming the few-shot tool execution
            messages.append({
                "role": "tool",
                "tool_call_id": f"fs_{hash(ex['user']) & 0xffff:04x}",
                "content": "ok",
            })
        messages.append({"role": "user", "content": query})
        return messages

    def _log(
        self,
        query: str,
        plan: dict | None,
        tokens_in: int,
        tokens_out: int,
        latency_ms: int,
        *,
        error: str | None = None,
    ) -> None:
        if self._log_path is None:
            return
        self._log_path.parent.mkdir(parents=True, exist_ok=True)
        record = {
            "ts": datetime.now().isoformat(timespec="seconds"),
            "query": query,
            "plan": plan,
            "tokens_in": tokens_in,
            "tokens_out": tokens_out,
            "latency_ms": latency_ms,
            "model": self._model,
            "error": error,
        }
        with self._log_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
```

也在 `src/rquant/llm/__init__.py` 导出新增：

```python
# src/rquant/llm/__init__.py
"""LLM 集成层：NL → ScreenPlan → screen()。"""

from rquant.llm.client import DeepSeekClient, LLMClarificationNeeded, LLMError
from rquant.llm.dispatch import build_rules, screen_with_plan
from rquant.llm.registry import REGISTRY, REGISTRY_BY_NAME, RuleSpec, get_rule_spec
from rquant.llm.schemas import RuleCall, ScreenPlan, Stage

__all__ = [
    "DeepSeekClient", "LLMClarificationNeeded", "LLMError",
    "RuleCall", "Stage", "ScreenPlan",
    "REGISTRY", "REGISTRY_BY_NAME", "RuleSpec", "get_rule_spec",
    "build_rules", "screen_with_plan",
]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/llm/test_client.py -v`
Expected: 5 passed

- [ ] **Step 5: Commit**

```bash
git add src/rquant/llm/client.py src/rquant/llm/__init__.py tests/unit/llm/test_client.py
git commit -m "feat(llm): DeepSeekClient with retry/log/clarification handling"
```

---

## Task 10: 真实 API Smoke Test（手动跑 + 文档化）

**Files:**
- Create: `scripts/llm_smoke.py`

- [ ] **Step 1: Create smoke script**

```python
#!/usr/bin/env python
# scripts/llm_smoke.py
"""手动 smoke test：用真实 API 跑几条 query，看 LLM 是否能稳定产出合理 plan。

不在 CI 中跑（费 API、不稳定）。开发本机或部署后人肉验证。

用法：
    uv run python scripts/llm_smoke.py
"""

from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path

from rquant.config import settings
from rquant.llm.client import DeepSeekClient, LLMClarificationNeeded


def main() -> int:
    if not settings.deepseek_enabled:
        print("ERROR: DEEPSEEK_API_KEY not set in .env")
        return 1

    client = DeepSeekClient(
        api_key=settings.deepseek_api_key,
        base_url=settings.deepseek_base_url,
        model=settings.deepseek_model,
        log_path=Path(settings.log_dir) / "nl_queries.jsonl",
    )

    queries = [
        "找昨天首板、流通市值 100 亿以下、今天没涨停的票",
        "今天 MA5 上穿 MA20，量比超过 2，主板创业板都行",
        "RSI14 超卖、近 30 天没跌停过、不要北交所",
        "昨日首板、有下影线、近 8 天没出现 3 连板",
        "今天涨幅大于 7% 的小票",  # 模糊问题，看 LLM 怎么处理
    ]
    today = date.today().isoformat()

    for i, q in enumerate(queries, 1):
        print(f"\n{'='*60}\n[{i}/{len(queries)}] Query: {q}\n{'-'*60}")
        try:
            plan = client.nl_to_screen_plan(q, today=today)
            print(f"trade_date: {plan.trade_date}")
            print(f"rationale:  {plan.rationale}")
            print(f"stages:")
            for s in plan.stages:
                print(f"  · {s.label}")
                for r in s.rules:
                    print(f"      - {r.name}({r.args})")
        except LLMClarificationNeeded as e:
            print(f"⚠️  LLM 请求澄清: {e}")
        except Exception as e:
            print(f"❌ 失败: {type(e).__name__}: {e}")

    print(f"\n日志写入：{Path(settings.log_dir) / 'nl_queries.jsonl'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 2: Run smoke script with real API**

Run: `uv run python scripts/llm_smoke.py 2>&1 | tee /tmp/llm_smoke.log`
Expected: 5 queries 全部产出合理 plan（前 4 条），第 5 条要么产出 plan 要么请求澄清。
**手动检查每条 plan 是否合理**：rule name 是否正确、args 是否合理、stages 分组是否清晰。

- [ ] **Step 3: 如发现 system prompt 不稳定，迭代调整**

如果某些 query 产出错误（如选错积木、参数过激），编辑 `src/rquant/llm/prompts.py` 增强提示，再次跑 smoke 直到稳定。每次调整后 commit：

```bash
git add src/rquant/llm/prompts.py
git commit -m "feat(llm): tune system prompt based on smoke test feedback"
```

- [ ] **Step 4: Commit smoke script**

```bash
git add scripts/llm_smoke.py
git commit -m "test(llm): add manual smoke test script for real API"
```

---

## Task 11: Streamlit NL 选股 Tab — 骨架（输入 + 解析 + JSON 预览）

**Files:**
- Modify: `src/rquant/dashboard/app.py`

- [ ] **Step 1: 找到 dashboard 现有 tab 结构**

Run: `grep -n "st.tabs\|tab1\|sidebar" src/rquant/dashboard/app.py | head -20`

记下 tabs 定义所在行号。

- [ ] **Step 2: 加入 "🤖 NL 选股" tab**

在 `src/rquant/dashboard/app.py` 中找到现有的 `st.tabs([...])` 调用，把新 tab 名 `"🤖 NL 选股"` 加到列表末尾。例如：

如果现在是：
```python
tab_overview, tab_pool1, tab_pool2 = st.tabs(["概览", "Pool 1", "Pool 2"])
```

改为：
```python
tab_overview, tab_pool1, tab_pool2, tab_nl = st.tabs(
    ["概览", "Pool 1", "Pool 2", "🤖 NL 选股"]
)
```

- [ ] **Step 3: 在 app.py 顶部 import 新增模块**

```python
# 加在现有 imports 中
from rquant.config import settings
from rquant.llm.client import DeepSeekClient, LLMClarificationNeeded, LLMError
from rquant.llm.schemas import ScreenPlan
```

- [ ] **Step 4: 在文件末尾加入 NL tab 渲染逻辑**

```python
# ── 🤖 NL 选股 tab ──────────────────────────────────────────────────────────

with tab_nl:
    st.header("🤖 自然语言选股")

    if not settings.deepseek_enabled:
        st.error("未配置 `DEEPSEEK_API_KEY`，本 tab 不可用。请在 .env 中填入 DeepSeek API key。")
        st.stop()

    if "nl_plan" not in st.session_state:
        st.session_state.nl_plan = None
    if "nl_history" not in st.session_state:
        st.session_state.nl_history = []  # list[str]
    if "nl_result_df" not in st.session_state:
        st.session_state.nl_result_df = None

    query = st.text_input(
        "输入选股需求",
        placeholder="例：MA5 上穿 MA20 + 量比 > 2 + 流通市值 < 100 亿，昨天首板",
        key="nl_query_input",
    )

    col_run, col_clear = st.columns([1, 1])
    with col_run:
        parse_clicked = st.button("🔍 解析", type="primary", use_container_width=True)
    with col_clear:
        if st.button("🗑 清空", use_container_width=True):
            st.session_state.nl_plan = None
            st.session_state.nl_result_df = None
            st.rerun()

    if parse_clicked and query.strip():
        from datetime import date as _date
        today = _date.today().isoformat()
        client = DeepSeekClient(
            api_key=settings.deepseek_api_key,
            base_url=settings.deepseek_base_url,
            model=settings.deepseek_model,
            log_path=Path(settings.log_dir) / "nl_queries.jsonl",
        )
        with st.spinner("正在解析..."):
            try:
                plan = client.nl_to_screen_plan(query, today=today)
                st.session_state.nl_plan = plan
                st.session_state.nl_history.insert(0, query)
                st.session_state.nl_history = st.session_state.nl_history[:5]
            except LLMClarificationNeeded as e:
                st.warning(f"💬 LLM 需要澄清：{e}")
                st.session_state.nl_plan = None
            except LLMError as e:
                st.error(f"❌ LLM 调用失败：{e}")
                st.session_state.nl_plan = None

    plan: ScreenPlan | None = st.session_state.nl_plan
    if plan:
        st.success(f"✅ 解析成功 · trade_date={plan.trade_date}")
        if plan.rationale:
            st.info(f"💭 {plan.rationale}")

        # 暂时用 st.json 预览；下个 task 改为 Stage Cards
        st.json(plan.model_dump())

    # 侧边栏：历史 query
    with st.sidebar:
        st.subheader("📜 NL 查询历史")
        if not st.session_state.nl_history:
            st.caption("（暂无）")
        else:
            for h in st.session_state.nl_history:
                if st.button(h[:30] + ("..." if len(h) > 30 else ""), key=f"nl_hist_{hash(h)}"):
                    st.session_state["nl_query_input"] = h
                    st.rerun()
```

确保 `from pathlib import Path` 在文件 imports 中（可能已有）。

- [ ] **Step 5: 启动 dashboard 验证渲染**

Run: `uv run streamlit run src/rquant/dashboard/app.py --server.headless=true --server.port=8502 &`
然后浏览器开 http://localhost:8502，切到 "🤖 NL 选股" tab：
- 输入框可见
- 输入示例 query → 点 "解析" → spinner → 显示 trade_date / rationale / JSON
- 点 "🗑 清空" 清空
- 侧边栏显示历史 query

如果跑不起来，检查 import / Streamlit 版本（≥1.30）。完成后停掉服务：
```bash
pkill -f "streamlit run" 2>/dev/null
```

- [ ] **Step 6: Commit**

```bash
git add src/rquant/dashboard/app.py
git commit -m "feat(dashboard): NL 选股 tab scaffold (input + parse + json preview)"
```

---

## Task 12: Stage Cards 渲染（替代 JSON 预览）

**Files:**
- Modify: `src/rquant/dashboard/app.py`（替换 st.json 调用）

- [ ] **Step 1: 把 plan 显示从 st.json 改为 Stage Cards**

在 `tab_nl` 块中，找到 `st.json(plan.model_dump())` 那行，替换为以下渲染逻辑：

```python
        # Stage Cards 渲染
        for i, stage in enumerate(plan.stages):
            with st.container(border=True):
                col_label, col_count = st.columns([4, 1])
                with col_label:
                    st.markdown(f"#### 🔹 第 {i+1} 层 · **{stage.label}**")
                with col_count:
                    st.caption(f"{len(stage.rules)} 条规则")

                for j, rule in enumerate(stage.rules):
                    args_str = ", ".join(f"{k}={v!r}" for k, v in rule.args.items())
                    if args_str:
                        st.markdown(f"- ✓ `{rule.name}({args_str})`")
                    else:
                        st.markdown(f"- ✓ `{rule.name}()`")

            # 卡片之间画个箭头（最后一层不画）
            if i < len(plan.stages) - 1:
                st.markdown(
                    "<div style='text-align:center;color:#888;font-size:24px;margin:-8px 0;'>↓</div>",
                    unsafe_allow_html=True,
                )

        # 底部：plan 完整 JSON 折叠区，方便用户复制 / debug
        with st.expander("📄 查看完整 plan JSON"):
            st.json(plan.model_dump())
```

- [ ] **Step 2: 重启 dashboard 验证**

Run: `uv run streamlit run src/rquant/dashboard/app.py --server.headless=true --server.port=8502 &`
浏览器看 NL tab，输入 query 解析后：
- 每个 stage 显示为带边框卡片
- 卡片头有"第 N 层 · 标签"
- 规则列在卡片内
- 卡片之间有 ↓ 箭头
- 底部"完整 plan JSON" 折叠区可展开

完成后停服务 `pkill -f "streamlit run"`。

- [ ] **Step 3: Commit**

```bash
git add src/rquant/dashboard/app.py
git commit -m "feat(dashboard): render plan as Stage Cards with arrows"
```

---

## Task 13: Stage Cards 编辑能力（add/del rule、add/del stage）

**Files:**
- Modify: `src/rquant/dashboard/app.py`

- [ ] **Step 1: Plan 改为 dict 形态存 session state**

把 plan 用 `model_dump()` 存为 dict 而非 ScreenPlan 对象。原因：Streamlit 在 button 回调中 mutate 嵌套 dict 后 rerun 会保留改动；用 Pydantic 对象则需要每次重建。

修改 session_state 初始化（替换 `nl_plan` 那行）：

```python
if "nl_plan_dict" not in st.session_state:
    st.session_state.nl_plan_dict = None
```

解析成功的赋值改为：
```python
st.session_state.nl_plan_dict = plan.model_dump()
```

并把后面 `plan: ScreenPlan | None = st.session_state.nl_plan` 整段替换为：
```python
plan_dict = st.session_state.nl_plan_dict
if plan_dict:
    st.success(f"✅ 解析成功 · trade_date={plan_dict['trade_date']}")
    if plan_dict.get("rationale"):
        st.info(f"💭 {plan_dict['rationale']}")
```

（即把 plan.X 访问改为 plan_dict["X"] dict 访问）

- [ ] **Step 2: 渲染卡片时支持删除规则 / 加规则 / 删除 stage**

把 Stage Cards 渲染段替换为：

```python
        plan_dict = st.session_state.nl_plan_dict
        # 顶部加新 stage 按钮
        if st.button("➕ 在末尾加新一层", key="nl_add_stage"):
            plan_dict["stages"].append({"label": "新分层", "rules": []})
            st.rerun()

        for i, stage in enumerate(plan_dict["stages"]):
            with st.container(border=True):
                col_label, col_del_stage = st.columns([6, 1])
                with col_label:
                    new_label = st.text_input(
                        f"layer-{i}",
                        value=stage["label"],
                        key=f"nl_stage_label_{i}",
                        label_visibility="collapsed",
                    )
                    if new_label != stage["label"]:
                        plan_dict["stages"][i]["label"] = new_label
                with col_del_stage:
                    if st.button("🗑", key=f"nl_del_stage_{i}", help="删除整层"):
                        plan_dict["stages"].pop(i)
                        st.rerun()

                # 列规则，每条带删除按钮
                for j, rule in enumerate(stage["rules"]):
                    args_str = ", ".join(f"{k}={v!r}" for k, v in rule["args"].items())
                    label = f"`{rule['name']}({args_str})`" if args_str else f"`{rule['name']}()`"
                    col_r, col_del_r = st.columns([10, 1])
                    with col_r:
                        st.markdown(f"- ✓ {label}")
                    with col_del_r:
                        if st.button("✕", key=f"nl_del_rule_{i}_{j}", help="删除该规则"):
                            plan_dict["stages"][i]["rules"].pop(j)
                            st.rerun()

                # 加规则下拉
                with st.expander("➕ 加规则到本层"):
                    from rquant.llm.registry import REGISTRY
                    rule_options = {f"{r.name} — {r.description}": r.name for r in REGISTRY}
                    chosen = st.selectbox(
                        "选择积木",
                        options=list(rule_options.keys()),
                        key=f"nl_add_rule_select_{i}",
                    )
                    args_json = st.text_area(
                        "args（JSON）",
                        value="{}",
                        key=f"nl_add_rule_args_{i}",
                        height=60,
                    )
                    if st.button("加入", key=f"nl_add_rule_btn_{i}"):
                        try:
                            args = json.loads(args_json) if args_json.strip() else {}
                            rule_name = rule_options[chosen]
                            # 用 RuleSpec.args_model 校验一遍
                            from rquant.llm.registry import get_rule_spec
                            get_rule_spec(rule_name).args_model.model_validate(args)
                            plan_dict["stages"][i]["rules"].append(
                                {"name": rule_name, "args": args}
                            )
                            st.rerun()
                        except Exception as e:
                            st.error(f"args 校验失败：{e}")

            if i < len(plan_dict["stages"]) - 1:
                st.markdown(
                    "<div style='text-align:center;color:#888;font-size:24px;margin:-8px 0;'>↓</div>",
                    unsafe_allow_html=True,
                )

        with st.expander("📄 查看完整 plan JSON"):
            st.json(plan_dict)
```

确保 app.py 顶部 import：
```python
import json
```

- [ ] **Step 2.5: import json 已在文件顶部**

Run: `head -20 src/rquant/dashboard/app.py | grep "^import json"`
如果没有，加 `import json` 到顶部 imports。

- [ ] **Step 3: 重启 dashboard 验证**

Run: `uv run streamlit run src/rquant/dashboard/app.py --server.headless=true --server.port=8502 &`
NL tab → 解析 query → 验证：
- 每个 stage 的 label 可以现场改名
- 每条规则旁的 ✕ 删除该规则
- 每个 stage 末尾"加规则"展开后可选积木 + 填 args + 加入
- 卡片右上角 🗑 删除整层
- 底部"加新一层"加空 stage

完成后停服务。

- [ ] **Step 4: Commit**

```bash
git add src/rquant/dashboard/app.py
git commit -m "feat(dashboard): editable Stage Cards (add/del rule + add/del stage)"
```

---

## Task 14: 运行按钮 → screen() → 结果表格

**Files:**
- Modify: `src/rquant/dashboard/app.py`

- [ ] **Step 1: 加运行按钮 + 渲染结果**

在 plan_dict 渲染段最后（"完整 plan JSON" 折叠区之后），加：

```python
        st.divider()
        col_run, col_date = st.columns([1, 2])
        with col_date:
            from datetime import date as _date
            run_date = st.date_input(
                "trade_date",
                value=_date.fromisoformat(plan_dict["trade_date"]),
                key="nl_run_date",
            )
            plan_dict["trade_date"] = run_date.isoformat()
        with col_run:
            if st.button("🚀 运行", type="primary", use_container_width=True):
                from rquant.llm.dispatch import screen_with_plan
                from rquant.storage.duckdb import DuckDBStore
                try:
                    plan = ScreenPlan.model_validate(plan_dict)
                    store = DuckDBStore(settings.duckdb_path)
                    df = screen_with_plan(plan, store=store)
                    st.session_state.nl_result_df = df
                except ValidationError as e:
                    st.error(f"plan 校验失败：{e}")
                except Exception as e:
                    st.error(f"运行失败：{type(e).__name__}: {e}")

        # 结果展示
        result_df = st.session_state.nl_result_df
        if result_df is not None:
            st.markdown(f"### 📊 命中 **{len(result_df)}** 只")
            if len(result_df) == 0:
                st.warning("无标的命中。检查规则参数是否过严，或调整 trade_date。")
            else:
                st.dataframe(result_df, use_container_width=True, hide_index=True)
```

确保顶部 import 有：
```python
from pydantic import ValidationError
```

- [ ] **Step 2: 重启 dashboard，端到端跑一次**

Run: `uv run streamlit run src/rquant/dashboard/app.py --server.headless=true --server.port=8502 &`
NL tab → 输入 "找昨天首板、流通市值 100 亿以下、今天没涨停的票" → 解析 → 运行：
- 应该看到结果表格（行数视当日数据而定）
- trade_date 改为往前推一周再跑，看历史结果

完成后停服务。

- [ ] **Step 3: Commit**

```bash
git add src/rquant/dashboard/app.py
git commit -m "feat(dashboard): run NL plan via screen() and display results"
```

---

## Task 15: 保存为 user preset 按钮

**Files:**
- Modify: `src/rquant/dashboard/app.py`

- [ ] **Step 1: 加保存按钮**

在结果表格之后（`st.dataframe(...)` 那行下面），加：

```python
                st.divider()
                col_save_input, col_save_btn = st.columns([3, 1])
                with col_save_input:
                    save_name = st.text_input(
                        "保存为 preset 名（自动加 `user/` 前缀）",
                        key="nl_save_name",
                        placeholder="例：突破新高放量",
                    )
                with col_save_btn:
                    st.markdown("<br>", unsafe_allow_html=True)  # 对齐
                    if st.button("💾 保存", use_container_width=True, disabled=not save_name.strip()):
                        from datetime import datetime as _dt
                        import re as _re

                        # 校验文件名安全
                        safe_name = _re.sub(r"[^\w一-鿿_-]", "", save_name.strip())
                        if not safe_name:
                            st.error("名字含非法字符")
                        else:
                            user_presets_dir = Path(settings.data_dir) / "user_presets"
                            user_presets_dir.mkdir(parents=True, exist_ok=True)
                            preset_path = user_presets_dir / f"{safe_name}.json"
                            if preset_path.exists():
                                st.error(f"⚠️ 已有同名 preset：{safe_name}.json")
                            else:
                                preset_path.write_text(json.dumps({
                                    "name": safe_name,
                                    "description": st.session_state.nl_history[0] if st.session_state.nl_history else "",
                                    "rules": [
                                        rc for s in plan_dict["stages"] for rc in s["rules"]
                                    ],
                                    "include_columns": plan_dict.get("include_columns", []),
                                    "created_at": _dt.now().isoformat(timespec="seconds"),
                                    "source": "nl_input",
                                }, ensure_ascii=False, indent=2), encoding="utf-8")
                                st.success(f"✅ 已保存到 {preset_path}")
```

- [ ] **Step 2: 重启 dashboard 验证保存**

Run: `uv run streamlit run src/rquant/dashboard/app.py --server.headless=true --server.port=8502 &`
NL tab → 跑一个 query → 输入"测试 preset" → 点保存 → 应看到成功提示。
检查文件：`ls -la data/user_presets/`，应有 `测试 preset.json`。

完成后停服务，删测试文件：
```bash
rm "data/user_presets/测试 preset.json"
```

- [ ] **Step 3: 重启 dashboard 验证 preset 加载到 PRESET_SCREENS**

Run: `uv run python -c "from rquant.presets import PRESET_SCREENS; print([k for k in PRESET_SCREENS.keys()])"`
Expected: 输出包含原 `n-shape-pool1`, `n-shape-pool2`，**不包含**已删的"测试 preset"。

再造一个永久测试：
```bash
mkdir -p data/user_presets
cat > data/user_presets/test-roundtrip.json << 'EOF'
{
  "name": "test-roundtrip",
  "description": "test",
  "rules": [{"name": "not_st", "args": {}}],
  "include_columns": [],
  "created_at": "2026-04-30T00:00:00",
  "source": "nl_input"
}
EOF
uv run python -c "from rquant.presets import PRESET_SCREENS; print('user/test-roundtrip' in PRESET_SCREENS)"
```
Expected: `True`

清理：
```bash
rm data/user_presets/test-roundtrip.json
```

- [ ] **Step 4: Commit**

```bash
git add src/rquant/dashboard/app.py
git commit -m "feat(dashboard): save NL plan as user preset (auto user/ prefix)"
```

---

## Task 16: 错误恢复与边界情况

**Files:**
- Modify: `src/rquant/llm/dispatch.py`（命中 0 只时的诊断信息）
- Modify: `src/rquant/dashboard/app.py`（per-rule hit count 显示）

- [ ] **Step 1: dispatch.py 加 per-rule hit count**

在 `src/rquant/llm/dispatch.py` 末尾追加：

```python
def screen_with_plan_diagnostic(
    plan: ScreenPlan,
    *,
    store: DuckDBStore | None = None,
) -> tuple[pd.DataFrame, list[tuple[str, int]]]:
    """运行 plan 同时记录每条 rule 单独的命中数（用于诊断）。

    返回 (final_result_df, [(rule_repr, hit_count_after_this_rule), ...]).
    若 final_result_df 为空，调用方可看每条 rule 的命中数判断哪条 rule 筛空了。
    """
    rules = build_rules(plan)
    if not rules:
        return screen(plan.trade_date, [], store=store), []

    # 第一次先用全量规则跑出最终结果
    final = screen(plan.trade_date, rules, store=store)

    # 诊断：累加规则数从 1 到 N，看每加一条命中变化
    diagnostics: list[tuple[str, int]] = []
    rule_calls = plan.flatten_rules()
    for n in range(1, len(rules) + 1):
        partial = screen(plan.trade_date, rules[:n], store=store)
        rc = rule_calls[n - 1]
        args_str = ", ".join(f"{k}={v!r}" for k, v in rc.args.items())
        rule_repr = f"{rc.name}({args_str})" if args_str else f"{rc.name}()"
        diagnostics.append((rule_repr, len(partial)))

    return final, diagnostics
```

- [ ] **Step 2: dashboard 显示诊断（仅命中 0 只时）**

修改 NL tab 的运行逻辑，使用 `screen_with_plan_diagnostic`：

把这段：
```python
    df = screen_with_plan(plan, store=store)
    st.session_state.nl_result_df = df
```

替换为：
```python
    from rquant.llm.dispatch import screen_with_plan_diagnostic
    df, diag = screen_with_plan_diagnostic(plan, store=store)
    st.session_state.nl_result_df = df
    st.session_state.nl_diagnostics = diag
```

并在 "无标的命中" warning 那段后追加：
```python
            if len(result_df) == 0:
                st.warning("无标的命中。检查规则参数是否过严，或调整 trade_date。")
                diag = st.session_state.get("nl_diagnostics", [])
                if diag:
                    st.markdown("**逐条规则累加命中数（看哪条筛空）：**")
                    diag_df = pd.DataFrame(diag, columns=["规则", "累加命中"])
                    st.dataframe(diag_df, use_container_width=True, hide_index=True)
```

确保 `import pandas as pd` 在 app.py 顶部。

- [ ] **Step 3: 测试空结果场景**

Run: dashboard → 输入一个明显不可能命中的需求，例：
"找今天 RSI14 低于 5、近 30 日 5 连板涨停 8 次以上的票"
应看到：
- 命中 0 只
- 诊断表显示哪条规则把命中筛空（例如 rsi_oversold 加进去后从 1000 → 0）

- [ ] **Step 4: Commit**

```bash
git add src/rquant/llm/dispatch.py src/rquant/dashboard/app.py
git commit -m "feat(llm): per-rule hit count diagnostic for empty results"
```

---

## Task 17: CHANGELOG / TODO / 手动验收 / 合并 main

**Files:**
- Modify: `CHANGELOG.md`
- Modify: `TODO.md`

- [ ] **Step 1: 跑全套测试**

Run: `uv run pytest --tb=short -q 2>&1 | tail -20`
Expected: 全绿，新增 ~30 个测试（schemas / registry / registry_complete / schema_export / dispatch / user_presets / prompts / client）。

如有失败：定位修复，再跑。

- [ ] **Step 2: 手动 dashboard 验收**

Run: `uv run streamlit run src/rquant/dashboard/app.py`
浏览器开 `http://localhost:8501`，NL tab 走完整流程：

- [ ] 输入 "找昨天首板、流通市值 100 亿以下、今天没涨停的票" → 解析成功
- [ ] Stage Cards 正确显示分层
- [ ] 编辑 stage label / 删除规则 / 加规则可用
- [ ] 加新一层可用
- [ ] 运行 → 看到表格（命中数视当日数据）
- [ ] 改 trade_date 重跑能拿不同日期结果
- [ ] 保存为 preset → `data/user_presets/<name>.json` 落盘
- [ ] 故意造空命中场景 → 看到诊断表
- [ ] 输入模糊需求（如"今天好的票"）→ LLM 请求澄清，UI 显示 warning

- [ ] **Step 3: 更新 CHANGELOG**

在 `CHANGELOG.md` 的 `## [Unreleased]` 下加：

```markdown
## [v0.11.0] — 2026-04-30 — Week 7：自然语言选股（NL → 积木）

dashboard 加 "🤖 NL 选股" tab：用户用一句中文描述筛选意图，DeepSeek-V4-Flash
解析为结构化 ScreenPlan（按 stage 分层），可视化卡片预览/编辑后跑 screen()
出表格，可一键保存为 user preset 接入 daily pipeline。

### Added

- `src/rquant/llm/`：完整 LLM 集成模块
  - `schemas.py`：ScreenPlan / Stage / RuleCall Pydantic 模型
  - `registry.py`：25 条积木 RuleSpec 注册表（Pydantic args model + examples + category）
  - `schema_export.py`：to_openai_tools() 生成 Tool Calls schema + 规则目录 markdown
  - `dispatch.py`：ScreenPlan → list[Rule] → screen()，含 per-rule 诊断
  - `prompts.py`：system prompt + 4 条 few-shot examples
  - `client.py`：DeepSeekClient（OpenAI SDK + DeepSeek base_url），retry + jsonl 日志
- `data/user_presets/*.json`：NL 输入保存的 preset，启动时合并到 PRESET_SCREENS（user/ 前缀）
- `dashboard/app.py`：新 tab "🤖 NL 选股"，Stage Cards UI（编辑 / 加 / 删 / 运行 / 保存）
- `scripts/llm_smoke.py`：手动 smoke test 脚本
- `.env.example`：DEEPSEEK_API_KEY / DEEPSEEK_BASE_URL / DEEPSEEK_MODEL
- 30+ 单元测试覆盖 schema / registry / dispatch / client mock / user_presets / prompts

### Changed

- `pyproject.toml`：+ openai>=1.0
- `src/rquant/config.py`：+ deepseek_api_key / deepseek_base_url / deepseek_model 字段
- `src/rquant/presets.py`：+ load_user_presets() loader，启动自动 merge

### Why

`screen/rules.py` 已有 25+ 积木但每次想跑新组合必须翻函数列表写 Python。Week 7
让用户用中文描述意图直接触达积木组合。Stage Cards 形态为 Week 7.5 真画布
预留数据结构（每 stage 直接映射成节点）。
```

- [ ] **Step 4: 更新 TODO.md**

把 Week 7 项标记完成，把 Week 7.5 加上：

```markdown
## MVP 收尾（剩 Week 8）

- [x] **Week 7**：Streamlit UI 自然语言输入（v0.11.0）
- [ ] **Week 8**：通达信选股公式支持（解析器 → MyTT/积木）

## Week 7.5（NL 选股下游优化）

- [ ] 真节点画布 UI 升级：streamlit-flow 集成，Stage 升级 Node，DAG 编辑
  - 触发条件：v0.11.0 上线后用户实际使用反馈，确认 Stage Cards 不足
- [ ] LLM-driven 画布操作：用户在画布上"问 LLM"添加节点 / 修改节点
- [ ] preset 保存为子图模板，多 preset DAG 关系可视化
```

- [ ] **Step 5: Commit + push 到远端 + （可选）打 tag**

```bash
git add CHANGELOG.md TODO.md
git commit -m "docs: update CHANGELOG + TODO for v0.11.0 (Week 7 NL screen)"
git push -u origin feat/week7-nl-screen
```

- [ ] **Step 6: 等用户确认后合并到 main**

按 CLAUDE.md "合 main 的硬规则"：
1. ✅ 本地实际运行核心路径跑通（Step 2 已做）
2. ✅ 更新 CHANGELOG.md（Step 3 已做）
3. ✅ 测试通过（Step 1 已做）
4. ✅ 准备打 tag v0.11.0（merge 后做）
5. ✅ README/docs 同步（如需要）

**用户操作**：
```bash
cd /Users/roxor/brain/30-projects/rQuant   # 回到 main 工作树
git checkout main
git merge feat/week7-nl-screen
git tag -a v0.11.0 -m "Week 7: NL 选股（DeepSeek + Stage Cards）"
git push origin main --tags
```

合并后清理 worktree：
```bash
git worktree remove .worktrees/feat-week7-nl-screen
git branch -d feat/week7-nl-screen
```

---

## Self-Review Checklist

- [x] 每个新增文件都有任务覆盖（schemas / registry / schema_export / dispatch / prompts / client / user_presets / dashboard）
- [x] 25 条积木全部注册（Task 3 完整列出）
- [x] LLM 错误场景全覆盖（API 重试 / 澄清请求 / args 校验 / 空命中诊断）
- [x] B 路径完整闭环（保存 → loader → daily pipeline 可用）
- [x] 类型/方法签名一致（`build_rules`, `screen_with_plan`, `screen_with_plan_diagnostic`, `nl_to_screen_plan`, `to_openai_tools`, `build_rule_catalog_md` 在 plan 中前后一致）
- [x] CHANGELOG / TODO 同步纪律按项目要求
- [x] 每个 task 有 commit 步骤，最大 task 也在 1 小时工作量内
