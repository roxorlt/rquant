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
from rquant.dashboard.canvas_files import (
    DEFAULT_NAME as CANVAS_DEFAULT_NAME,
    add_pool_to_canvas,
    canvas_membership_of,
    delete_canvas,
    filter_pool_refs,
    list_canvases,
    load_canvas,
    save_canvas,
    set_canvas_pool_refs,
)
from rquant.dashboard.canvas_nl_edit import diff_rule_calls, nl_edit_pool
from rquant.dashboard.canvas_persistence import (
    base_name_of,
    delete_user_pool,
    fork_builtin_to_user,
    is_user_pool,
    load_user_pool_raw,
    load_user_pool_rule_calls,
    save_user_pool,
)
from rquant.dashboard.canvas_rule_editor import (
    render_args_form,
    rule_spec_options,
)
from rquant.llm.client import LLMClarificationNeeded, LLMError
from rquant.llm.schemas import RuleCall
from rquant.presets import PRESET_SCREENS
from rquant.storage.duckdb import open_readonly_store


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


# ── Lazy DB 连接（避免 daily 17:00 拿不到 write lock）──
#
# 历史教训：旧版用 @st.cache_resource 缓存 DuckDBStore 实例 → 一旦用户访问 canvas
# 触发 cache 建立，conn 永久持锁，daily 17:00 拿 exclusive write lock 失败 fatal。
# 2026-05-14 17:00 真实事故见 PR fix-canvas-lock-leak。
#
# 现在改 lazy：每次 cache miss 才 `with DuckDBStore(read_only=True) as store:` 开关，
# 函数返回后 conn 自动 close。@st.cache_data 仍缓存 diagnostic 结果，所以多次 click
# 同一 pool 不会重连 DB。dashboard / nl-screen 跟 daily 之前共存就是这种模式。


@st.cache_data(ttl=300, show_spinner=False)
def _cached_diagnose(preset_name: str, trade_date: str) -> tuple[pd.DataFrame, list[tuple[str, int]]]:
    with open_readonly_store() as store:
        return diagnose_preset(preset_name, trade_date, store=store)


def _read_latest_trade_date() -> str:
    """每次 streamlit rerun 调用，conn 用完就 close。"""
    with open_readonly_store() as store:
        return latest_trade_date(store)


def _build_initial_state(pool_refs: list[str] | None = None) -> StreamlitFlowState:
    """根据 canvas 的 pool_refs 过滤 PRESET_SCREENS → 节点 + edge。

    pool_refs=None 时回退到全部 pool（兼容老逻辑）。
    """
    if pool_refs is None:
        presets = list(PRESET_SCREENS.values())
    else:
        presets = [PRESET_SCREENS[n] for n in pool_refs if n in PRESET_SCREENS]
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

trade_date = _read_latest_trade_date()

with st.sidebar:
    st.markdown("### 🧩 rQuant 画布")
    st.caption(f"trade_date `{trade_date}`")

    # —— C-Canvas-1: 多画布切换 ——
    metas = list_canvases()
    if "active_canvas" not in st.session_state:
        st.session_state.active_canvas = CANVAS_DEFAULT_NAME
    canvas_options = [m.name for m in metas]
    canvas_display = {m.name: m.display_name for m in metas}
    cur_idx = (
        canvas_options.index(st.session_state.active_canvas)
        if st.session_state.active_canvas in canvas_options
        else 0
    )
    chosen = st.selectbox(
        "当前画布",
        canvas_options,
        index=cur_idx,
        format_func=lambda n: canvas_display.get(n, n),
        key="canvas_picker",
    )
    if chosen != st.session_state.active_canvas:
        st.session_state.active_canvas = chosen
        st.session_state.pop("canvas_state", None)
        st.session_state["_skip_next_selected_sync"] = True
        st.rerun()

    # —— 当前画布详情 + CRUD
    try:
        current_canvas = load_canvas(st.session_state.active_canvas)
    except FileNotFoundError:
        # 用户删了 canvas 但 session 还指向它 → fallback
        st.session_state.active_canvas = CANVAS_DEFAULT_NAME
        current_canvas = load_canvas(CANVAS_DEFAULT_NAME)

    valid_pool_refs = filter_pool_refs(current_canvas)
    st.caption(f"📦 {len(valid_pool_refs)} 个 pool" + ("（含 builtin + user）" if current_canvas.is_default else ""))
    if current_canvas.description:
        st.caption(current_canvas.description)

    # 复制 / 删除 canvas（默认 canvas 不可改）
    if not current_canvas.is_default:
        cols = st.columns(2)
        if cols[0].button("📋 复制", key="canvas_dup_btn", use_container_width=True, help="复制为新 canvas"):
            st.session_state["_show_canvas_dup"] = True
        if cols[1].button("🗑 删除", key="canvas_del_btn", use_container_width=True, help="删 canvas 文件，不删 pool"):
            st.session_state["_show_canvas_del"] = True

        if st.session_state.get("_show_canvas_dup"):
            new_name = st.text_input("新 canvas 名", key="canvas_dup_name", placeholder=f"{current_canvas.name}-copy")
            if st.button("✓ 确认复制", key="canvas_dup_ok", type="primary", use_container_width=True):
                if not new_name.strip():
                    st.warning("名字不能空")
                else:
                    try:
                        save_canvas(
                            new_name.strip(),
                            description=current_canvas.description,
                            pool_refs=list(current_canvas.pool_refs),
                            source="canvas_dup",
                        )
                        st.session_state.active_canvas = new_name.strip()
                        st.session_state.pop("canvas_state", None)
                        st.session_state.pop("_show_canvas_dup", None)
                        st.session_state["_skip_next_selected_sync"] = True
                        st.toast(f"✓ 复制为 {new_name.strip()}")
                        st.rerun()
                    except Exception as e:
                        st.error(f"复制失败：{e}")

        if st.session_state.get("_show_canvas_del"):
            st.error(f"⚠ 确认删 canvas `{current_canvas.name}`？只删文件，不删 pool。")
            cols = st.columns(2)
            if cols[0].button("取消", key="canvas_del_cancel", use_container_width=True):
                st.session_state.pop("_show_canvas_del", None)
                st.rerun()
            if cols[1].button("✓ 确认删", key="canvas_del_ok", type="primary", use_container_width=True):
                try:
                    delete_canvas(current_canvas.name)
                    st.session_state.active_canvas = CANVAS_DEFAULT_NAME
                    st.session_state.pop("canvas_state", None)
                    st.session_state.pop("_show_canvas_del", None)
                    st.session_state["_skip_next_selected_sync"] = True
                    st.toast(f"✓ 删 canvas {current_canvas.name}")
                    st.rerun()
                except Exception as e:
                    st.error(f"删除失败：{e}")

        # —— C-Canvas-2: 管理 pool 成员（仅非默认 canvas）
        with st.expander("⚙ 管理 pool 成员"):
            all_pool_names = sorted(PRESET_SCREENS.keys())
            cur_pool_refs = filter_pool_refs(current_canvas)
            chosen_pools = st.multiselect(
                "勾选要包含的 pool",
                all_pool_names,
                default=cur_pool_refs,
                key=f"canvas_pools_multi__{current_canvas.name}",
                label_visibility="collapsed",
            )
            if st.button("✓ 应用", key="canvas_pools_apply", type="primary", use_container_width=True):
                try:
                    set_canvas_pool_refs(current_canvas.name, chosen_pools)
                    st.session_state.pop("canvas_state", None)
                    st.session_state["_skip_next_selected_sync"] = True
                    st.toast(f"✓ 已更新 {current_canvas.name} 含 {len(chosen_pools)} 个 pool")
                    st.rerun()
                except Exception as e:
                    st.error(f"应用失败：{e}")

    # —— 新建空 canvas
    with st.expander("➕ 新建空 canvas"):
        new_canvas_name = st.text_input("name", key="new_canvas_name", placeholder="my-strategy")
        new_canvas_desc = st.text_input("description", key="new_canvas_desc", placeholder="一句话描述")
        if st.button("创建", key="new_canvas_btn", type="primary", use_container_width=True):
            nm = new_canvas_name.strip()
            if not nm:
                st.warning("name 不能空")
            elif nm == CANVAS_DEFAULT_NAME:
                st.warning(f"{CANVAS_DEFAULT_NAME} 是保留名")
            elif nm in canvas_options:
                st.warning(f"canvas {nm} 已存在")
            else:
                try:
                    save_canvas(
                        nm,
                        description=new_canvas_desc or f"canvas {nm}",
                        pool_refs=[],
                        source="canvas_new",
                    )
                    st.session_state.active_canvas = nm
                    st.session_state.pop("canvas_state", None)
                    st.session_state["_skip_next_selected_sync"] = True
                    st.toast(f"✓ 新建 canvas {nm}")
                    st.rerun()
                except Exception as e:
                    st.error(f"新建失败：{e}")

    st.divider()

    # —— 新建 user pool（自动加到 active canvas）
    with st.expander("➕ 新建空 user pool"):
        new_base = st.text_input(
            "name (base，不含 user/ 前缀)",
            value="",
            key="new_pool_base",
            placeholder="my-screen-2026",
        )
        new_desc = st.text_input(
            "description",
            value="",
            key="new_pool_desc",
            placeholder="一句话描述这个 pool",
        )
        if st.button("创建", key="new_pool_btn", type="primary", use_container_width=True):
            base = new_base.strip()
            if not base:
                st.warning("name 不能空")
            elif f"user/{base}" in PRESET_SCREENS:
                st.warning(f"user/{base} 已存在")
            else:
                try:
                    save_user_pool(
                        base,
                        description=new_desc or f"user pool {base}",
                        rule_calls=[],
                        include_columns=[],
                        source="canvas_new",
                    )
                    from rquant.presets import load_user_presets
                    from pathlib import Path as _P
                    PRESET_SCREENS.update(
                        load_user_presets(_P(settings.data_dir) / "user_presets")
                    )
                    # 自动加到当前 canvas（默认 canvas 自动 include 所有 pool，跳过）
                    new_full = f"user/{base}"
                    if not current_canvas.is_default:
                        add_pool_to_canvas(current_canvas.name, new_full)
                    st.session_state.pop("canvas_state", None)
                    st.session_state.active_pool_id = new_full
                    st.session_state["_skip_next_selected_sync"] = True
                    st.toast(f"✓ 新建 user/{base}")
                    st.rerun()
                except Exception as e:
                    st.error(f"新建失败：{e}")

    if st.button("🔄 重置画布布局", use_container_width=True, help="重新自动叠层布局"):
        st.session_state.pop("canvas_state", None)
        st.rerun()
    if st.button("🗑 清诊断缓存", use_container_width=True, help="下次点节点会重跑 SQL"):
        _cached_diagnose.clear()
        st.toast("已清缓存")
    st.divider()
    st.caption("**当前 canvas 含 pool**")
    for name in valid_pool_refs:
        prefix = "🟦" if is_user_pool(name) else "⚪"
        st.markdown(f"{prefix} `{name}`")


# ── 主区：画布 + 详情，6:4 列 ──

if "canvas_state" not in st.session_state:
    st.session_state.canvas_state = _build_initial_state(pool_refs=valid_pool_refs)

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
# 一次性 _skip_next_selected_sync：fork 等动作后，前端 react-flow 可能还记得旧
# selected_id，需要跳过一次同步保留主动设置的 active_pool_id。
st.session_state.canvas_state = new_canvas_state
if st.session_state.pop("_skip_next_selected_sync", False):
    pass
else:
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


def _normalize_rule_call(rc: RuleCall) -> dict:
    """通过 args_model 校验+dump 归一化 args 类型（如 int 150 → float 150.0 for
    threshold_yi）。规避 widget 写回类型变化误判 dirty。spec 未注册时 fallback 到 raw。"""
    from rquant.llm.registry import get_rule_spec

    try:
        spec = get_rule_spec(rc.name)
        validated = spec.args_model.model_validate(rc.args)
        return {"name": rc.name, "args": validated.model_dump()}
    except Exception:
        return {"name": rc.name, "args": rc.args}


def _is_dirty(pending: list[RuleCall], initial: list[RuleCall]) -> bool:
    return [_normalize_rule_call(rc) for rc in pending] != [
        _normalize_rule_call(rc) for rc in initial
    ]


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


def _nl_parse_callback(selected_id: str, today: str) -> None:
    """st.button on_click 回调：button click 那次 rerun **之前**同步执行。
    用这个模式而不是 if st.button(): 是因为后者在 input → button 同 rerun 时
    可能返回 False（streamlit 1.57 widget 嵌套场景观察到）。"""
    nl_input_key = f"nl_input__{selected_id}"
    pkey = f"nl_proposed__{selected_id}"
    qkey = f"nl_query__{selected_id}"
    ekey = f"nl_error__{selected_id}"
    ckey = f"nl_clarify__{selected_id}"

    # 清旧 transient 错误 / clarify
    st.session_state.pop(ekey, None)
    st.session_state.pop(ckey, None)

    query = (st.session_state.get(nl_input_key, "") or "").strip()
    if not query:
        st.session_state[ekey] = "请先输入指令"
        return
    pending: list[RuleCall] = st.session_state.get(_pending_key(selected_id), [])
    try:
        proposed = nl_edit_pool(query, pending, today=today)
        st.session_state[pkey] = proposed
        st.session_state[qkey] = query
    except LLMClarificationNeeded as e:
        st.session_state[ckey] = str(e)
    except LLMError as e:
        st.session_state[ekey] = f"LLM 调用失败：{e}"
    except Exception as e:
        st.session_state[ekey] = f"解析失败：{e}"


def _nl_apply_callback(selected_id: str) -> None:
    pkey = f"nl_proposed__{selected_id}"
    qkey = f"nl_query__{selected_id}"
    proposed = st.session_state.get(pkey)
    if proposed is None:
        return
    st.session_state[_pending_key(selected_id)] = proposed
    st.session_state.pop(pkey, None)
    st.session_state.pop(qkey, None)


def _nl_clear_callback(selected_id: str) -> None:
    for k in (
        f"nl_proposed__{selected_id}",
        f"nl_query__{selected_id}",
        f"nl_error__{selected_id}",
        f"nl_clarify__{selected_id}",
    ):
        st.session_state.pop(k, None)


def _render_nl_edit_section(selected_id: str, pending: list[RuleCall]) -> None:
    """C.2 NL 改 pool：DeepSeek 解析 → diff 预览 → 一键应用。

    用 st.button on_click 回调而不是 `if st.button(): ...` —— 后者在 input 改值
    紧接 button click 的场景下 button() 偶尔返回 False（widget rerun 时机问题）。
    """
    pkey = f"nl_proposed__{selected_id}"
    qkey = f"nl_query__{selected_id}"
    ekey = f"nl_error__{selected_id}"
    ckey = f"nl_clarify__{selected_id}"
    nl_input_key = f"nl_input__{selected_id}"

    if not settings.deepseek_enabled:
        st.warning("🧠 NL 改 pool：DEEPSEEK_API_KEY 未配置")
        return

    st.markdown("**🧠 NL 改 pool（DeepSeek）**")
    st.caption("用一句话描述你想怎么改，LLM 会基于当前规则产出修改建议供预览")
    st.text_input(
        "指令",
        key=nl_input_key,
        placeholder="例：加 first_limit_up offset=1；删 circ_mv_lt",
        label_visibility="collapsed",
    )
    cols = st.columns([0.75, 0.25])
    cols[0].button(
        "📤 解析",
        key=f"nl_parse__{selected_id}",
        type="primary",
        use_container_width=True,
        on_click=_nl_parse_callback,
        args=(selected_id, trade_date),
    )
    cols[1].button(
        "✕",
        key=f"nl_clear__{selected_id}",
        use_container_width=True,
        help="清提议",
        on_click=_nl_clear_callback,
        args=(selected_id,),
    )

    # 渲染回调留下的 transient 反馈
    if ekey in st.session_state:
        st.error(st.session_state[ekey])
    if ckey in st.session_state:
        st.info(f"💬 LLM 需要澄清：\n\n{st.session_state[ckey]}")

    # —— 显示 diff
    proposed = st.session_state.get(pkey)
    if proposed is None:
        return
    added, removed, unchanged = diff_rule_calls(pending, proposed)
    st.markdown(
        f"**diff 预览**：保留 {len(unchanged)} · ➕ 新增 {len(added)} · ✕ 删除 {len(removed)}"
    )
    for rc in added:
        st.markdown(f"<span style='color:#2e7d32'>➕ `{rc.name}` `{rc.args}`</span>",
                    unsafe_allow_html=True)
    for rc in removed:
        st.markdown(f"<span style='color:#c62828;text-decoration:line-through'>✕ `{rc.name}` `{rc.args}`</span>",
                    unsafe_allow_html=True)
    if not added and not removed:
        st.caption("LLM 提议跟当前规则一致，无需改动")

    st.button(
        "✓ 应用提议到 pending",
        key=f"nl_apply__{selected_id}",
        type="primary",
        use_container_width=True,
        disabled=(not added and not removed),
        on_click=_nl_apply_callback,
        args=(selected_id,),
    )


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

    # —— C.2: NL 改 pool（DeepSeek 解析 + diff 预览 + 一键应用）
    _render_nl_edit_section(selected_id, pending)

    # —— C.3: 删除 user pool（二次确认）
    confirm_key = f"confirm_delete__{selected_id}"
    if st.session_state.get(confirm_key):
        st.error(f"⚠ 确认删除 `{selected_id}`？文件 `user_presets/{base_name_of(selected_id)}.json` 会被永久删除。")
        cols = st.columns(2)
        if cols[0].button("取消", key=f"cancel_del_{selected_id}", use_container_width=True):
            st.session_state[confirm_key] = False
            st.rerun()
        if cols[1].button(
            "确认删除", key=f"confirm_del_{selected_id}", type="primary", use_container_width=True
        ):
            base = base_name_of(selected_id)
            try:
                ok = delete_user_pool(base)
                if not ok:
                    st.warning(f"user/{base}.json 不存在")
                else:
                    PRESET_SCREENS.pop(selected_id, None)
                    st.session_state.pop(_pending_key(selected_id), None)
                    st.session_state.pop(confirm_key, None)
                    st.session_state.pop("canvas_state", None)
                    st.session_state.active_pool_id = None
                    st.session_state["_skip_next_selected_sync"] = True
                    _cached_diagnose.clear()
                    st.toast(f"✓ 删除 user/{base}")
                    st.rerun()
            except Exception as e:
                st.error(f"删除失败：{e}")
    else:
        if st.button(
            "🗑 删除此 user pool",
            key=f"del_pool_btn_{selected_id}",
            use_container_width=True,
            help="物理删除 user_presets/<base>.json 文件",
        ):
            st.session_state[confirm_key] = True
            st.rerun()


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

        # —— C-Canvas-2: 显示该 pool 出现在哪些 canvas
        memberships = canvas_membership_of(selected_id)
        if len(memberships) > 1:
            non_default = [m for m in memberships if m != CANVAS_DEFAULT_NAME]
            mship_str = "默认" + (" · " + " · ".join(non_default) if non_default else "")
            st.caption(f"📋 在 `{len(memberships)}` 个 canvas 中：{mship_str}")
        else:
            st.caption("📋 只在默认 canvas")

        st.divider()

        # —— 用户 pool：CRUD UI；builtin：read-only + Fork 按钮
        if is_user_pool(selected_id):
            _render_user_pool_crud(selected_id)
        else:
            st.info("📖 builtin pool 只读 · 点下方按钮 fork 一份到 user/ 即可编辑")
            cols = st.columns([0.65, 0.35])
            cols[0].caption(f"会创建 `user_presets/{selected_id}.json`，含当前所有规则副本")
            if cols[1].button(
                f"🍴 Fork as user/{selected_id}",
                key=f"fork_{selected_id}",
                use_container_width=True,
                type="primary",
            ):
                try:
                    path = fork_builtin_to_user(selected_id)
                    # runtime merge 新 user pool 到 PRESET_SCREENS（streamlit 单进程，安全）
                    from rquant.presets import load_user_presets
                    from pathlib import Path as _P
                    PRESET_SCREENS.update(
                        load_user_presets(_P(settings.data_dir) / "user_presets")
                    )
                    # 自动加到当前 canvas（默认 canvas 自动 include 所有 pool）
                    if not current_canvas.is_default:
                        add_pool_to_canvas(current_canvas.name, f"user/{selected_id}")
                    # 清画布 state 让 _build_initial_state 重新 include 新 pool
                    st.session_state.pop("canvas_state", None)
                    # 切到新 user pool（active_pool_id）
                    st.session_state.active_pool_id = f"user/{selected_id}"
                    # 一次性 flag：下一次 rerun 时跳过 streamlit_flow.selected_id 同步，
                    # 否则 react-flow 内部状态还记得是 builtin selected，会把 active 覆盖回去
                    st.session_state["_skip_next_selected_sync"] = True
                    _cached_diagnose.clear()
                    st.toast(f"✓ Fork OK：{path.name}")
                    st.rerun()
                except FileExistsError as e:
                    st.warning(str(e))
                except Exception as e:
                    st.error(f"fork 失败：{e}")

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
