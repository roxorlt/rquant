"""rQuant NL 选股 — 节点画布版（Week 7.5 B + C.1）。

启动方式（本地开发）：
    streamlit run src/rquant/dashboard/nl_canvas.py --server.port 8503 \\
        --server.address 0.0.0.0 --server.headless true

跟现有 nl_screen.py（Stage Cards 形态，8502）并存。C 阶段全部完成后再决定
是否替代 nl_screen.py。

B 阶段：read-only 画布 + per-rule diagnostic 漏斗 + 命中标的预览。
C.1 阶段：user/ 前缀 pool 的规则 CRUD（inline edit args / × 删除 /
  + 加规则 模板路径 / 保存 / 撤销）；builtin pool 仍只读（rules 是闭包，
  反查不出 RuleCall args；fork-to-user 留到 C.X）。
"""

from __future__ import annotations

import pandas as pd
import streamlit as st
from streamlit_flow import streamlit_flow
from streamlit_flow.elements import StreamlitFlowEdge, StreamlitFlowNode
from streamlit_flow.layouts import LayeredLayout
from streamlit_flow.state import StreamlitFlowState

from rquant.config import settings
from rquant.dashboard.canvas_diagnostic import diagnose_preset, latest_trade_date
from rquant.dashboard.canvas_persistence import (
    base_name_of,
    is_user_pool,
    load_user_pool_raw,
    load_user_pool_rule_calls,
    save_user_pool,
)
from rquant.dashboard.canvas_rule_editor import (
    render_args_form,
    rule_spec_options,
)
from rquant.llm.schemas import RuleCall
from rquant.presets import PRESET_SCREENS
from rquant.storage.duckdb import DuckDBStore


st.set_page_config(
    page_title="rQuant NL 画布",
    page_icon="🧩",
    layout="wide",
    initial_sidebar_state="collapsed",
)


@st.cache_resource
def _shared_store() -> DuckDBStore:
    """整个 session 共享一个 read-only DuckDBStore，避免每次回调重开。"""
    return DuckDBStore(settings.duckdb_path, read_only=True)


@st.cache_data(ttl=300, show_spinner=False)
def _cached_diagnose(preset_name: str, trade_date: str) -> tuple[pd.DataFrame, list[tuple[str, int]]]:
    """跨 rerun 缓存单个 pool 的 diagnostic + 命中表（5min TTL）。

    参数都是 hashable 字符串；store 通过 _shared_store() 在内部取，不进 cache key。
    """
    return diagnose_preset(preset_name, trade_date, store=_shared_store())


def _build_initial_state() -> StreamlitFlowState:
    """PRESET_SCREENS → 节点 + edge。LayeredLayout 自动横向叠层。"""
    presets = list(PRESET_SCREENS.values())
    names = {p.name for p in presets}
    referenced = {p.depends_on for p in presets if p.depends_on in names}

    nodes: list[StreamlitFlowNode] = []
    for p in presets:
        is_input = p.depends_on is None
        is_output = p.name not in referenced
        if is_input and is_output:
            node_type = "default"
        elif is_input:
            node_type = "input"
        elif is_output:
            node_type = "output"
        else:
            node_type = "default"

        lines = [f"### {p.name}", "", p.description, "", f"`{len(p.rules)}` 条规则"]
        if p.depends_on:
            lines.append(f"← `{p.depends_on}` (offset {p.offset_days}d)")
        nodes.append(
            StreamlitFlowNode(
                id=p.name,
                pos=(0, 0),
                data={"content": "\n".join(lines)},
                node_type=node_type,
                source_position="right",
                target_position="left",
                draggable=False,
                deletable=False,
            )
        )

    edges = [
        StreamlitFlowEdge(
            id=f"{p.depends_on}->{p.name}",
            source=p.depends_on,
            target=p.name,
            animated=True,
            label=f"+{p.offset_days}d",
            marker_end={"type": "arrowclosed"},
        )
        for p in presets
        if p.depends_on and p.depends_on in names
    ]

    return StreamlitFlowState(nodes=nodes, edges=edges)


st.markdown("## 🧩 NL 选股画布")
trade_date = latest_trade_date(_shared_store())
st.caption(
    f"trade_date = `{trade_date}`（DuckDB 最新交易日）· "
    f"{len(PRESET_SCREENS)} 个 pool（builtin + `data/user_presets/*.json`）"
)

# 必须 stash 在 session_state，否则 streamlit_flow 库会 infinite re-render
if "canvas_state" not in st.session_state:
    st.session_state.canvas_state = _build_initial_state()

left, right = st.columns([0.6, 0.4], gap="medium")

with left:
    st.session_state.canvas_state = streamlit_flow(
        key="nl_canvas",
        state=st.session_state.canvas_state,
        layout=LayeredLayout(direction="right", node_node_spacing=80, node_layer_spacing=180),
        height=560,
        fit_view=True,
        show_controls=True,
        show_minimap=False,
        allow_new_edges=False,
        enable_pane_menu=False,
        enable_node_menu=False,
        enable_edge_menu=False,
        animate_new_edges=False,
        get_node_on_click=True,
        get_edge_on_click=False,
        pan_on_drag=True,
        allow_zoom=True,
        min_zoom=0.3,
        hide_watermark=True,
        style={"border": "1px solid #ddd", "borderRadius": "8px"},
    )


def _render_diagnostic_funnel(diagnostics: list[tuple[str, int]]) -> None:
    """累加漏斗：每条规则一行 `name  保留数  % of 初始`。"""
    if not diagnostics:
        st.info("无规则")
        return
    initial = diagnostics[0][1] or 1  # 防除零
    rows = []
    for name, count in diagnostics:
        pct = count / initial if initial else 0
        rows.append({"规则": name, "保留": count, "% of 初始": f"{pct:.1%}"})
    st.dataframe(
        pd.DataFrame(rows),
        hide_index=True,
        column_config={"保留": st.column_config.NumberColumn(format="%d")},
        use_container_width=True,
    )


def _pending_key(selected_id: str) -> str:
    return f"pending_rules__{selected_id}"


def _initial_pending(selected_id: str) -> list[RuleCall]:
    """C.1：从 user_presets/<base>.json 装载初始 RuleCall 列表到 pending。"""
    base = base_name_of(selected_id)
    return load_user_pool_rule_calls(base)


def _ensure_pending_loaded(selected_id: str) -> list[RuleCall]:
    key = _pending_key(selected_id)
    if key not in st.session_state:
        st.session_state[key] = _initial_pending(selected_id)
    return st.session_state[key]


def _render_user_pool_crud(selected_id: str, preset) -> None:
    """C.1 CRUD UI：仅对 user/ 前缀生效。"""
    base = base_name_of(selected_id)
    raw = load_user_pool_raw(base) or {}
    description = raw.get("description", preset.description)
    include_columns = raw.get("include_columns", preset.include_columns)

    pending: list[RuleCall] = _ensure_pending_loaded(selected_id)
    initial: list[RuleCall] = _initial_pending(selected_id)
    dirty = [rc.model_dump() for rc in pending] != [rc.model_dump() for rc in initial]

    # —— Pending banner
    if dirty:
        c1, c2, c3 = st.columns([0.5, 0.25, 0.25])
        c1.warning(f"⚠ {abs(len(pending) - len(initial))} 处改动未保存")
        if c2.button("↩ 撤销", key=f"undo_{selected_id}", use_container_width=True):
            st.session_state[_pending_key(selected_id)] = initial
            st.rerun()
        if c3.button("💾 保存", key=f"save_{selected_id}", type="primary", use_container_width=True):
            try:
                save_user_pool(
                    base,
                    description=description,
                    rule_calls=pending,
                    include_columns=include_columns,
                )
                # 清 cache 让 diagnostic / PRESET_SCREENS 下次重载
                _cached_diagnose.clear()
                st.success(f"✓ 已保存到 user_presets/{base}.json（重启 streamlit 让 PRESET_SCREENS 重载）")
            except Exception as e:
                st.error(f"保存失败：{e}")

    st.markdown("**规则列表**")
    if not pending:
        st.caption("空 pool —— 用下面的 `+ 加规则` 加第一条")

    # —— 每条规则一行
    for i, rc in enumerate(pending):
        with st.expander(f"`{i + 1}` · `{rc.name}` `{rc.args}`", expanded=False):
            new_args = render_args_form(
                rc.name,
                rc.args,
                key_prefix=f"{selected_id}_rule{i}",
            )
            cols = st.columns([0.7, 0.3])
            if cols[0].button("应用参数", key=f"apply_{selected_id}_{i}"):
                pending[i] = RuleCall(name=rc.name, args=new_args or {})
                st.rerun()
            if cols[1].button("× 删除", key=f"del_{selected_id}_{i}"):
                pending.pop(i)
                st.rerun()

    # —— + 加规则（模板路径）
    st.markdown("**+ 加规则（模板）**")
    options = rule_spec_options()
    labels = [label for _, label in options]
    chosen_idx = st.selectbox(
        "选规则",
        range(len(options)),
        format_func=lambda i: labels[i],
        key=f"add_rule_select_{selected_id}",
    )
    chosen_name = options[chosen_idx][0]
    st.caption("默认参数；加完后展开规则行调参数")
    if st.button("➕ 加到 pending", key=f"add_btn_{selected_id}"):
        pending.append(RuleCall(name=chosen_name, args={}))
        st.rerun()


with right:
    selected_id = st.session_state.canvas_state.selected_id
    if not selected_id:
        st.info("👈 点击左侧节点查看 pool 详情 / diagnostic / 命中标的")
    elif selected_id not in PRESET_SCREENS:
        st.warning(f"未知节点 `{selected_id}`")
    else:
        preset = PRESET_SCREENS[selected_id]
        st.markdown(f"### `{preset.name}`")
        st.caption(preset.description)

        meta_lines = []
        if preset.depends_on:
            meta_lines.append(
                f"**依赖**：`{preset.depends_on}` + offset `{preset.offset_days}d`"
            )
        else:
            meta_lines.append("**输入**：全市场")
        meta_lines.append(f"**规则数**：{len(preset.rules)}")
        st.markdown("\n\n".join(meta_lines))

        # —— C.1: user/ pool 编辑 UI；builtin 显示提示
        if is_user_pool(selected_id):
            with st.container(border=True):
                _render_user_pool_crud(selected_id, preset)
        else:
            st.info(
                "💡 builtin pool 不可编辑（rules 是闭包，参数不可反查）。"
                "要编辑请用 8502 端口 NL Screen 创建 user/ 副本，或等 fork-to-user 功能上线。"
            )

        with st.spinner("跑 diagnostic + 命中表…"):
            try:
                hits_df, diagnostics = _cached_diagnose(selected_id, trade_date)
            except Exception as e:
                st.error(f"diagnostic 失败：{e}")
                hits_df, diagnostics = pd.DataFrame(), []

        # —— 漏斗
        st.markdown(f"**diagnostic 漏斗** (最终 `{len(hits_df)}` 只)")
        _render_diagnostic_funnel(diagnostics)

        # —— 命中表
        st.markdown(f"**命中标的** · `{len(hits_df)}` 只")
        if hits_df.empty:
            st.caption("无命中（可能是当日数据不全 / 父预设为空）")
        else:
            st.dataframe(hits_df, hide_index=True, use_container_width=True, height=240)
