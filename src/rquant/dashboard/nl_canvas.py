"""rQuant 画布 — Pool 节点图 + 规则 CRUD（Week 7.5 B + C.1 + C.1.1 UI polish）。

启动方式（本地开发）：
    streamlit run src/rquant/dashboard/nl_canvas.py --server.port 8503 \\
        --server.address 0.0.0.0 --server.headless true

阶段总览：
  - B：read-only 画布 + per-rule diagnostic 漏斗 + 命中标的预览
  - C.1：user/ 前缀 pool 的规则 CRUD（inline edit args / 删除 / 加规则 / 持久化）
  - C.1.1：UI polish — CSS 隐 chrome / 节点拖动 / 紧凑规则行 / popover 加规则 / sidebar 工具栏

builtin pool 仍只读（rules 是闭包，args 反查不出；fork-to-user 推到后续）。
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
    page_title="rQuant 画布",
    page_icon="🧩",
    layout="wide",
    initial_sidebar_state="collapsed",
)


# ── CSS：让画布"全屏铺底"——压缩 streamlit 默认 padding，提高内容利用率 ──
st.markdown(
    """
    <style>
      /* 主容器 padding 收紧 */
      .block-container { padding-top: 1rem !important; padding-bottom: 1rem !important; }
      /* 顶部工具条 (Deploy 按钮等) 还在但更紧凑 */
      header[data-testid="stHeader"] { height: 2.2rem; background: transparent; }
      /* 把 streamlit-flow iframe 边框 / 圆角统一 */
      iframe[src*="streamlit_flow"] { border-radius: 8px; }
      /* 详情列：长内容时单独滚动 */
      [data-testid="stColumn"]:nth-child(2) { max-height: 90vh; overflow-y: auto; padding-right: 0.5rem; }
      /* 规则行紧凑：去掉 caption / divider 的上下大间距 */
      .rule-row { padding: 0.25rem 0; border-bottom: 1px solid #eee; }
    </style>
    """,
    unsafe_allow_html=True,
)


# ── 共享 store / diagnostic 缓存 ──

@st.cache_resource
def _shared_store() -> DuckDBStore:
    return DuckDBStore(settings.duckdb_path, read_only=True)


@st.cache_data(ttl=300, show_spinner=False)
def _cached_diagnose(preset_name: str, trade_date: str) -> tuple[pd.DataFrame, list[tuple[str, int]]]:
    return diagnose_preset(preset_name, trade_date, store=_shared_store())


def _build_initial_state() -> StreamlitFlowState:
    """PRESET_SCREENS → 节点 + edge。"""
    presets = list(PRESET_SCREENS.values())
    names = {p.name for p in presets}
    referenced = {p.depends_on for p in presets if p.depends_on in names}

    nodes: list[StreamlitFlowNode] = []
    for p in presets:
        is_input_pool = p.depends_on is None
        is_terminal = p.name not in referenced
        if is_input_pool and is_terminal:
            node_type = "default"
        elif is_input_pool:
            node_type = "input"
        elif is_terminal:
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
                draggable=True,      # C.1.1：允许拖动
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


# ── Sidebar：工具栏 ──

trade_date = latest_trade_date(_shared_store())

with st.sidebar:
    st.markdown("### 🧩 rQuant 画布")
    st.caption(f"trade_date `{trade_date}`")
    st.caption(f"{len(PRESET_SCREENS)} 个 pool")
    st.divider()
    if st.button("🔄 重置画布布局", use_container_width=True, help="重新自动叠层布局"):
        st.session_state.pop("canvas_state", None)
        st.rerun()
    if st.button("🗑 清诊断缓存", use_container_width=True, help="下次点节点会重跑 SQL"):
        _cached_diagnose.clear()
        st.toast("已清缓存")
    st.divider()
    st.caption("**Pool 列表**")
    for name in sorted(PRESET_SCREENS.keys()):
        prefix = "🟦" if is_user_pool(name) else "⚪"
        st.markdown(f"{prefix} `{name}`")


# ── 主区：画布 + 详情，6:4 列 ──

if "canvas_state" not in st.session_state:
    st.session_state.canvas_state = _build_initial_state()

# active_pool_id 单独存 session_state，跟 streamlit-flow 内部 selected_id 解耦。
# 修复 C.1.2 bug：
#   1. popover 内选 selectbox / 点按钮后，rerun 时 streamlit-flow 返回的 state.selected_id
#      在前端可能因 popover 占位 / "空白点击"判定 被设回 None
#   2. 拖动节点完成时 react-flow 也会把 selected_id 设为 None
#   只有在新 selected_id 是合法 pool 名时才更新 active，None / 未知值保留原值。
if "active_pool_id" not in st.session_state:
    st.session_state.active_pool_id = None

left, right = st.columns([0.6, 0.4], gap="medium")

with left:
    new_canvas_state = streamlit_flow(
        key="nl_canvas",
        state=st.session_state.canvas_state,
        layout=LayeredLayout(direction="right", node_node_spacing=80, node_layer_spacing=180),
        height=720,
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
        style={"border": "1px solid #e6e6e6", "borderRadius": "8px"},
    )

# 同步：画布 state 是 react-flow 自己管的（含拖动后的节点位置 / zoom 等），需要保留。
# active_pool_id 仅在新 selected_id 是合法 pool 名时更新；None / 未知值不覆盖。
st.session_state.canvas_state = new_canvas_state
_new_sel = new_canvas_state.selected_id
if _new_sel and _new_sel in PRESET_SCREENS:
    st.session_state.active_pool_id = _new_sel


# ── 右侧详情 ──

def _render_diagnostic_funnel(diagnostics: list[tuple[str, int]]) -> None:
    if not diagnostics:
        st.caption("无规则")
        return
    initial = diagnostics[0][1] or 1
    rows = [
        {"规则": name, "保留": count, "%": f"{(count / initial):.1%}" if initial else "—"}
        for name, count in diagnostics
    ]
    st.dataframe(
        pd.DataFrame(rows),
        hide_index=True,
        column_config={"保留": st.column_config.NumberColumn(format="%d")},
        use_container_width=True,
    )


def _pending_key(selected_id: str) -> str:
    return f"pending_rules__{selected_id}"


def _initial_pending(selected_id: str) -> list[RuleCall]:
    return load_user_pool_rule_calls(base_name_of(selected_id))


def _ensure_pending_loaded(selected_id: str) -> list[RuleCall]:
    key = _pending_key(selected_id)
    if key not in st.session_state:
        st.session_state[key] = _initial_pending(selected_id)
    return st.session_state[key]


def _is_dirty(pending: list[RuleCall], initial: list[RuleCall]) -> bool:
    return [rc.model_dump() for rc in pending] != [rc.model_dump() for rc in initial]


def _render_dirty_banner(selected_id: str, pending: list[RuleCall], initial: list[RuleCall]) -> None:
    base = base_name_of(selected_id)
    raw = load_user_pool_raw(base) or {}
    description = raw.get("description", "")
    include_columns = raw.get("include_columns", [])

    cols = st.columns([0.45, 0.25, 0.30])
    cols[0].markdown(f"⚠ **未保存改动**（{len(pending)} 条规则 vs 磁盘 {len(initial)} 条）")
    if cols[1].button("↩ 撤销", key=f"undo_{selected_id}", use_container_width=True):
        st.session_state[_pending_key(selected_id)] = initial
        st.rerun()
    if cols[2].button(
        "💾 保存", key=f"save_{selected_id}", type="primary", use_container_width=True
    ):
        try:
            save_user_pool(
                base,
                description=description,
                rule_calls=pending,
                include_columns=include_columns,
            )
            _cached_diagnose.clear()
            st.toast(f"✓ 已保存到 user_presets/{base}.json")
        except Exception as e:
            st.error(f"保存失败：{e}")


def _render_compact_rule_row(selected_id: str, idx: int, rc: RuleCall, pending: list[RuleCall]) -> None:
    """一条规则一行（紧凑无 expander）：`N · name`  |  args inline  |  [✓][×]"""
    container = st.container(border=False)
    with container:
        st.markdown(f"<div class='rule-row'></div>", unsafe_allow_html=True)
        # 第一行：序号 + 规则名 + args 表单（横向铺开）
        header = st.columns([0.05, 0.40, 0.45, 0.05, 0.05])
        header[0].markdown(f"**{idx + 1}**")
        header[1].markdown(f"`{rc.name}`")
        with header[2]:
            new_args = render_args_form(
                rc.name,
                rc.args,
                key_prefix=f"{selected_id}_rule{idx}",
            )
        # 检测 args 变化 → 自动更新 pending（无需"应用"按钮）
        if new_args is not None and new_args != rc.args:
            pending[idx] = RuleCall(name=rc.name, args=new_args)
        if header[3].button("📋", key=f"dup_{selected_id}_{idx}", help="复制此规则"):
            pending.insert(idx + 1, RuleCall(name=rc.name, args=dict(rc.args)))
            st.rerun()
        if header[4].button("✕", key=f"del_{selected_id}_{idx}", help="删除此规则"):
            pending.pop(idx)
            st.rerun()


def _render_add_rule_popover(selected_id: str, pending: list[RuleCall]) -> None:
    """+ 加规则 用 popover，弹小窗选 RuleSpec + 一键加"""
    with st.popover("➕ 加规则", use_container_width=True):
        st.caption("选一条积木加到 pending（默认参数，加完上方可改）")
        options = rule_spec_options()
        chosen_idx = st.selectbox(
            "规则",
            range(len(options)),
            format_func=lambda i: options[i][1],
            key=f"add_rule_select_{selected_id}",
            label_visibility="collapsed",
        )
        if st.button("加入", key=f"add_btn_{selected_id}", type="primary", use_container_width=True):
            pending.append(RuleCall(name=options[chosen_idx][0], args={}))
            st.rerun()


def _render_user_pool_crud(selected_id: str) -> None:
    pending: list[RuleCall] = _ensure_pending_loaded(selected_id)
    initial: list[RuleCall] = _initial_pending(selected_id)

    if _is_dirty(pending, initial):
        _render_dirty_banner(selected_id, pending, initial)
    else:
        st.caption("💡 改任意参数后会出现「保存」按钮 · 改完点保存写盘")

    if not pending:
        st.caption("空 pool — 用下方 ➕ 加规则 开始")

    for i, rc in enumerate(pending):
        _render_compact_rule_row(selected_id, i, rc, pending)

    _render_add_rule_popover(selected_id, pending)


with right:
    # 用持久化的 active_pool_id 而不是 streamlit-flow 的 selected_id；
    # selected_id 在拖动 / popover 等场景会被 react-flow 内部清成 None
    selected_id = st.session_state.active_pool_id
    if not selected_id:
        st.markdown("### 🖱 点击左侧节点")
        st.caption("查看 pool 详情 / diagnostic 漏斗 / 命中标的 / 编辑规则")
    elif selected_id not in PRESET_SCREENS:
        st.warning(f"未知节点 `{selected_id}`")
    else:
        preset = PRESET_SCREENS[selected_id]
        # —— 标题 + meta 一行
        st.markdown(f"### `{preset.name}`")
        meta = []
        if preset.depends_on:
            meta.append(f"依赖 `{preset.depends_on}`（offset {preset.offset_days}d）")
        else:
            meta.append("输入 全市场")
        meta.append(f"{len(preset.rules)} 条规则")
        st.caption(" · ".join(meta))
        if preset.description:
            st.caption(preset.description)

        st.divider()

        # —— 用户 pool：CRUD UI；builtin：read-only 提示
        if is_user_pool(selected_id):
            _render_user_pool_crud(selected_id)
        else:
            st.info("📖 builtin pool 只读 · fork-to-user 功能在后续 PR")

        st.divider()

        # —— diagnostic 漏斗 + 命中表（折叠到 expander 节省视高）
        with st.spinner("跑 diagnostic + 命中表…"):
            try:
                hits_df, diagnostics = _cached_diagnose(selected_id, trade_date)
            except Exception as e:
                st.error(f"diagnostic 失败：{e}")
                hits_df, diagnostics = pd.DataFrame(), []

        with st.expander(f"📊 diagnostic 漏斗（最终 {len(hits_df)} 只）", expanded=True):
            _render_diagnostic_funnel(diagnostics)

        with st.expander(f"📋 命中标的（{len(hits_df)} 只）", expanded=False):
            if hits_df.empty:
                st.caption("无命中（当日数据不全 / 父预设为空）")
            else:
                st.dataframe(hits_df, hide_index=True, use_container_width=True, height=240)
