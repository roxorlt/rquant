# Week 7: 自然语言选股（NL → 积木） — 设计文档

**日期**：2026-04-30
**状态**：已确认，待实施
**前置版本**：v0.10.0（Upload HTTP API + dashboard 框架已就绪）

---

## 背景与目标

`screen/rules.py` 已有 25+ 积木函数（`first_limit_up`, `circ_mv_lt`, `cross_above`...），但每次想跑一个新组合必须翻函数列表 + 手写 Python。

Week 7 让用户用一句中文描述出筛选意图（如"MA5 上穿 MA20 + 量比 > 2 + 流通市值 < 100 亿"），LLM 解析后调出对应积木组合，预览确认后执行，结果显示在 dashboard。

**现阶段范围（A 路径为主，B 派生）**：

- **A. 即兴查询** — NL 输入 → 分层卡片预览 → 跑一次 → 看结果。主流程。
- **B. 保存为 preset** — A 结果满意时一键钉成持久化 preset，下次 17:00 daily pipeline 自动跑。一个按钮 + ~30 行代码的事。

**UI 形态：Stage Cards（分层卡片）**

LLM 解析后产出的规则按 `stage`（基础过滤 / 形态 / 时机 / ...）分组，dashboard 渲染成垂直堆叠的卡片，卡片之间用箭头/分隔线视觉化 pipeline 流向。底层数据结构是 `list[Stage]`（每个 Stage 内含 `list[RuleCall]`），所有 stage 内规则 AND 合取。

为什么不直接上真画布（react-flow / streamlit-flow）：(1) Streamlit 节点画布生态尚不成熟 (2) 当前 screen() 底层是 AND 合取，DAG 表达力暂时用不上 (3) Week 7 时间预算紧。**Week 7.5（独立立项）** 升级到真画布，到时已经有真实使用反馈知道哪些交互是必要的。

**不在本期范围**：

- C 路径（参数微调）/ D 路径（自动保存）
- 真节点画布编辑（Week 7.5 独立做）
- Monitor 弹窗 UX 网页化（独立做）
- 通达信选股公式解析（Week 8）

---

## 技术决策

### LLM 选型 — DeepSeek-V4-Flash

`deepseek-v4-flash`，1M 上下文，384K 输出，**支持 Tool Calls + JSON Output**。

- 价格：缓存命中 ¥0.02 / 未命中 ¥1 / 输出 ¥2（每百万 tokens），可忽略不计
- OpenAI-compatible，base_url = `https://api.deepseek.com`
- 国内云直连无障碍，本地开发同模型同代码
- 未来想换 Claude/GPT，改 base_url 即可（OpenAI Python SDK 同时兼容）

环境变量 `DEEPSEEK_API_KEY`，加入 `.env.example` + `config.py` 读取。

### 结构化输出策略 — 单一 `build_screen` tool + 规则数组

LLM 调用一个统一 tool：

```json
{
  "name": "build_screen",
  "arguments": {
    "trade_date": "2026-04-30",
    "stages": [
      {
        "label": "基础过滤",
        "rules": [
          {"name": "not_st", "args": {}},
          {"name": "not_bj", "args": {}},
          {"name": "circ_mv_lt", "args": {"threshold_yi": 100}}
        ]
      },
      {
        "label": "涨停状态",
        "rules": [
          {"name": "first_limit_up", "args": {"offset": 1}},
          {"name": "not_limit_up", "args": {"offset": 0}}
        ]
      },
      {
        "label": "技术指标",
        "rules": [
          {"name": "cross_above", "args": {"fast": "MA5", "slow": "MA20"}},
          {"name": "volume_ratio_gte", "args": {"n": 2.0}}
        ]
      }
    ],
    "include_columns": [],
    "rationale": "..."
  }
}
```

- `name` 字段是 enum，所有 25 条积木名枚举进 schema，LLM 不能瞎拼
- `stages` 是有序列表，每个 stage 有用户友好 label（基础过滤 / 形态 / 时机 等）
- 所有 stage 内规则 AND 合取（语义上等价于 flat list，但分组便于 UI 渲染和用户编辑）
- 一次返回完整 plan，不靠多次 tool call 拼接
- 输出是结构化 JSON，安全风险为 0（只能调到注册过的积木）

**为什么是 stages 而不是 rules + category 字段**：stages 是**有序**的（label 是 LLM 命名而非 enum），未来升级到 Week 7.5 真画布时每个 stage 直接映射成一个节点，零迁移成本；如果用 `rules + category`，category 是 enum 限定不灵活，且渲染逻辑要做"按 category 分组排序"。

### 规则注册表 — Pydantic-based RuleSpec（类 LangChain / MCP 范式）

新模块 `src/rquant/llm/registry.py`，每条规则一个 `RuleSpec`：

```python
class FirstLimitUpArgs(BaseModel):
    offset: int = Field(0, ge=0, le=30,
        description="距 T 日的偏移，0=今天, 1=昨天, 2=前天")

class RuleSpec(BaseModel):
    name: str
    description: str            # 中文一句话
    args_model: type[BaseModel] # Pydantic 强约束
    examples: list[str]         # NL 示例 ["昨天首板", "T-2 首板"]
    category: Literal["filter", "state", "indicator", "shape", "aggregate"]
    fn: Callable                # 实际积木 factory

REGISTRY: list[RuleSpec] = [...]  # 25+ 条
```

**Qlib 借鉴范围**：命名风格 + docstring 写法 + RD-Agent 的 prompt 模式。**不引入 pyqlib 依赖**——抽象层不匹配（Qlib ops 是因子表达式，我们是布尔筛选规则），且依赖太重（PyTorch + 500MB）。

### Schema 转换

```python
def to_openai_tools() -> list[dict]:
    """REGISTRY → OpenAI / DeepSeek tool spec list（含 name enum + args schema）"""

def to_mcp_tools() -> list[dict]:
    """同上，MCP 格式 — 未来扩展用，先不实现"""
```

参考实现：[langchain-ai/langchain `convert_to_openai_function`](https://github.com/langchain-ai/langchain/blob/master/libs/core/langchain_core/utils/function_calling.py)。

---

## 模块结构

```
src/rquant/
├── llm/                       ← 新模块
│   ├── __init__.py
│   ├── client.py              ← DeepSeekClient + nl_to_screen_plan()
│   ├── registry.py            ← REGISTRY: list[RuleSpec] + to_openai_tools()
│   ├── schemas.py             ← ScreenPlan, RuleCall Pydantic models
│   └── prompts.py             ← system prompt + few-shot examples
├── screen/
│   └── ...                    ← 不动
├── presets.py                 ← + load_user_presets() 扫 data/user_presets/
└── dashboard/
    └── app.py                 ← + 新 tab "🤖 NL 选股"
```

---

## 数据流

```
用户输入 NL query
    ↓
DeepSeekClient.nl_to_screen_plan(query, today=2026-04-30)
    ├─ system prompt + tools=to_openai_tools() + user query
    └─ DeepSeek API (tool_choice=required)
    ↓
ScreenPlan { trade_date, stages: [Stage(label, rules: [RuleCall])], rationale }
    ↓
[ Streamlit UI Stage Cards 预览，每张卡片可编辑/折叠/删除/加规则 ]
    ↓ (用户点 "运行")
flatten: stages → list[RuleCall] → registry dispatch → fn(**args) → list[Rule]
    ↓
screen(trade_date, rules, ...) → DataFrame
    ↓
Streamlit 显示表格 + [保存为 preset] 按钮
    ↓ (可选, 用户点 "保存为 preset")
data/user_presets/{name}.json   # 落库时也保留 stages 结构（B 提示信息）
```

---

## Streamlit UI 设计 — Stage Cards

`dashboard/app.py` 新增 tab `🤖 NL 选股`：

```
┌─ st.text_input：NL 查询 ─────────────────────────────┐
│ MA5 上穿 MA20 + 量比 > 2 + 流通市值 < 100 亿，昨天首板 │
└──────────────────────────────────────────────────────┘
        [ 解析 ] (调 LLM)

💭 LLM rationale: "我把市值过滤放第一层，确保候选池规模合适..."

┌── 🔹 第 1 层 · 基础过滤 ──────────────┐  [✏️ 编辑] [🗑]
│  ✓ 排除 ST                            │
│  ✓ 排除北交所                         │
│  ✓ 流通市值 < 100 亿                  │   [+ 加规则]
└──────────────────────────────────────┘
                 ↓
┌── 🔹 第 2 层 · 涨停状态 ──────────────┐  [✏️ 编辑] [🗑]
│  ✓ 昨日首板（offset=1）               │
│  ✓ 今日未涨停（offset=0）             │   [+ 加规则]
└──────────────────────────────────────┘
                 ↓
┌── 🔹 第 3 层 · 技术指标 ──────────────┐  [✏️ 编辑] [🗑]
│  ✓ MA5 上穿 MA20                      │
│  ✓ 量比 ≥ 2                           │   [+ 加规则]
└──────────────────────────────────────┘

[+ 加新一层]

[ 🚀 运行 ]   [ 🔄 改写需求 ]

═══════════════════════════════════════
📊 命中 N 只 ｜ 总 stages: 3 ｜ 总 rules: 7
   <DataFrame 表格>

[ 💾 保存为 preset → ] 输入框：preset 名
```

**实现要点**：

- 每张 Stage Card 用 `st.container(border=True)` 渲染，container 内 `st.expander` 折叠/展开规则，每条规则一行带删除按钮
- 卡片之间用居中的 `↓` Markdown 字符做视觉箭头，CSS 调一下颜色字号
- "✏️ 编辑" 弹 `st.dialog`（Streamlit 1.30+）展开 Pydantic 表单：根据该规则的 `args_model` 渲染输入控件，提交后更新 `st.session_state.plan.stages[i].rules[j].args`
- "[+ 加规则]" 弹 dialog 选择规则名（registry 的 `select` 框，按 category 分组），选完跳到参数表单
- "[+ 加新一层]" 在 plan.stages 末尾追加空 Stage，用户输入 label
- 所有编辑都改 `st.session_state.plan`，不重新调 LLM；改完点 "🚀 运行" 才跑 screen()

**侧边栏**：

- 历史最近 5 条 query（`st.session_state` 存），点击重新解析
- 当前 plan 的 JSON 视图（`st.json` 折叠，方便看底层结构 / 一键复制）
- trade_date 选择器（默认最新交易日，从 DuckDB 查）

**最小依赖**：纯 Streamlit 1.30+ 原生组件，不引入 streamlit-flow / react-flow。CSS 用 `st.markdown` + `<style>` 微调（rQuant 现有 dashboard 已经这么做）。

---

## B 路径：保存为 preset

按钮触发 → 弹"preset 名字" → 写 JSON：

`data/user_presets/{name}.json`：

```json
{
  "name": "突破新高放量",
  "description": "[原始 NL query]",
  "rules": [{"name": "...", "args": {...}}, ...],
  "include_columns": ["..."],
  "created_at": "2026-04-30T15:00:00",
  "source": "nl_input"
}
```

`presets.py` 加 loader：

```python
def load_user_presets() -> dict[str, ScreenPreset]:
    """启动时扫 data/user_presets/*.json，命名前缀 user/。"""
    # user/突破新高放量 → ScreenPreset(...)

# 启动时合并
PRESET_SCREENS.update(load_user_presets())
```

daily pipeline 自动会拿到 `user/` 前缀的所有 preset，下次 17:00 跑就带上。

---

## 错误恢复

| 情形 | 处理 |
|---|---|
| API 超时 / 503 | retry 3 次（指数退避，1s/2s/4s），仍败则 UI 红字 |
| LLM 返回纯文本（澄清请求）| 直接显示文本，不进规则预览阶段 |
| Tool call `name` 不在 registry | 防御代码：跳过该规则 + UI warning（enum 应已挡住） |
| 命中 0 只 | 显示"命中 0 只" + 列出每条规则单独命中数（哪条筛空一目了然） |
| 参数 Pydantic 校验失败 | UI 显示错误参数 + 期望范围，让用户改 JSON 重跑 |
| API key 缺失 | 启动时检查，dashboard 显示"未配置 DEEPSEEK_API_KEY，本 tab 不可用" |

---

## Logging

`logs/nl_queries.jsonl` —— 每次查询 append 一行：

```json
{"ts": "2026-04-30T15:00:00", "query": "...", "plan": {...},
 "hit_count": 12, "latency_ms": 1230, "tokens_in": 850, "tokens_out": 220}
```

后续调 system prompt / 加 few-shot examples 时是金矿。

---

## 测试

新增 `tests/llm/`：

- `test_registry.py` — 每个 `RuleSpec.fn(**args_model().model_dump())` 能 instantiate；Pydantic schema 合法
- `test_registry_complete.py` — `REGISTRY` 跟 `screen.__all__` 一一对应（漏注册红灯）
- `test_schema_export.py` — `to_openai_tools()` 输出符合 OpenAI tool schema spec
- `test_dispatch.py` — 给定固定 `ScreenPlan` → 端到端跑通 `screen()` 出表
- `test_user_presets.py` — write 一个 user preset JSON → loader 能正确解析为 `ScreenPreset`

**不测 LLM 本身**（flaky + 费 key + 不稳定）；测的是 parsing/dispatch/execution 这条结构化路径。LLM 调用层在 unit test 里 mock 掉。

可选：`tests/llm/test_e2e_smoke.py` 用真 API key 跑一次 smoke test，标 `@pytest.mark.slow` + `@pytest.mark.requires_api_key`，CI 跳过本地手动跑。

---

## 依赖变更

`pyproject.toml` 新增：

```toml
openai = ">=1.0"     # DeepSeek OpenAI-compatible
# pydantic 已在 deps
# streamlit 已在 deps
```

`.env.example` 新增：

```
DEEPSEEK_API_KEY=sk-...
DEEPSEEK_BASE_URL=https://api.deepseek.com  # 可选 override，默认值
DEEPSEEK_MODEL=deepseek-v4-flash             # 可选 override
```

---

## 实施步骤（建议分支 `feat/week7-nl-screen`）

1. **registry 骨架**：`src/rquant/llm/{schemas,registry}.py`，先把 25 条 `RuleSpec` 写完，配套 unit test
2. **schema export**：`to_openai_tools()` + 校验输出符合 OpenAI 规范
3. **DeepSeek client**：`client.py` + `prompts.py`（system prompt + 4-5 条 few-shot），单元测试 mock API
4. **dispatch + 端到端**：`ScreenPlan → screen()` 跑通，命令行能验证
5. **Streamlit UI**：`dashboard/app.py` 加 tab，先打通 query → 预览 → 运行 → 表格
6. **保存 preset**：按钮 + JSON 写入 + `load_user_presets()` loader
7. **错误恢复 + logging**：retry / fallback / jsonl 日志
8. **手动验收**：本地跑 5-10 个真实 query，调 system prompt 直到稳定
9. **更新 CHANGELOG + TODO + 合并 main**

---

## 验收标准

- [ ] 给定 5 个常见 query（涵盖涨停状态、均线、量比、市值、形态），LLM 全部正确映射到积木组合
- [ ] UI 能预览规则、可编辑 JSON、可重新解析
- [ ] 命中结果能保存为 user preset，重启 dashboard 后 daily pipeline 能跑该 preset
- [ ] 没配 API key 时 tab 友好降级（不崩）
- [ ] 全部新增测试通过，原有 297 个测试不受影响

---

## 后续（不在本期）

### Week 7.5：升级到真画布形态（C 路径）

**触发条件**：Week 7 上线后，若使用中发现 Stage Cards 的局限——比如想做"同一基础过滤分两条支路对比不同时机判断"、"多 preset DAG 可视化"、"实验快照对比"——就值得做真画布。

**实施要点**：

- 引入 `streamlit-flow` 或 `streamlit-agraph`，先 spike 验证 Streamlit 集成稳定性
- 每个 Stage 升级为 Node，stage 之间的 `↓` 升级为可拖拽连接的 edge
- 支持新节点类型：分支（OR 节点）、对比（diff 节点）、可保存的子图模板
- 与 NL 输入整合：LLM 可以"在画布上添加节点 / 修改节点"，不只是初始生成
- ScreenPlan 数据结构兼容：当前的 `list[Stage]` 是退化形式的 DAG（线性 chain），升级到完整 DAG 时 schema 自然扩展，不破坏 user_presets/ 已落库的数据

**依赖前置工作**：Week 7 的 stages 结构必须设计成易扩展为 DAG（而非 flat list），这样 Week 7.5 不用大改数据模型。

### 其他后续

- Monitor 弹窗 UX 网页化（Pool 2 退出确认页）— 跟 NL 选股不耦合
- Week 8 通达信选股公式解析（与 NL 输入并存的另一种入口）
- 把 registry 包成 MCP server，让 Claude Desktop 直接调 → "今天有哪些 N 形态股票" 一句话直达
