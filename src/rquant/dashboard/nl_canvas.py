"""rQuant NL 选股 — 节点画布版（Week 7.5 A spike）。

启动方式（本地开发）：
    streamlit run src/rquant/dashboard/nl_canvas.py --server.port 8503 \\
        --server.address 0.0.0.0 --server.headless true

跟现有 nl_screen.py（Stage Cards 形态，8502）并存。spike 通过后会逐步替代。

A spike 目标（设计文档 §3.3）：
  - 验证 streamlit-flow 在 Streamlit 1.57 上稳定渲染
  - PRESET_SCREENS 全部 pool 转节点，depends_on 转 edge
  - 点击节点右侧面板显示该 pool 的规则名 + 描述
  - 通过 → B 阶段（接 per-rule diagnostic 漏斗 + 命中标的）
  - 不通过 → 退 streamlit-agraph
"""

from __future__ import annotations

import streamlit as st
from streamlit_flow import streamlit_flow
from streamlit_flow.elements import StreamlitFlowEdge, StreamlitFlowNode
from streamlit_flow.layouts import LayeredLayout
from streamlit_flow.state import StreamlitFlowState

from rquant.presets import PRESET_SCREENS


st.set_page_config(
    page_title="rQuant NL 画布",
    page_icon="🧩",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown("## 🧩 NL 选股画布 · A spike")
st.caption(
    "Week 7.5 A 阶段：验证 streamlit-flow 库可用。"
    "渲染 PRESET_SCREENS 中所有 pool + depends_on edge。"
    "B 阶段会接入 per-rule diagnostic 漏斗与命中标的预览。"
)


def _build_initial_state() -> StreamlitFlowState:
    """从 PRESET_SCREENS 派生初始节点 + edge。

    - 每个 ScreenPreset 一个节点，id = preset name
    - 节点 content = markdown（名字 + 描述）
    - depends_on != None → 一条 edge（source=depends_on, target=self）
    - 入度 0 的节点标 node_type='input'，无被依赖的标 'output'，其他 'default'

    所有节点起始 pos=(0,0)；交给 LayeredLayout 自动排版。
    """
    presets = list(PRESET_SCREENS.values())
    names = {p.name for p in presets}
    referenced: set[str] = set()
    for p in presets:
        if p.depends_on and p.depends_on in names:
            referenced.add(p.depends_on)

    nodes: list[StreamlitFlowNode] = []
    for p in presets:
        is_input = p.depends_on is None
        is_output = p.name not in referenced
        if is_input and is_output:
            node_type = "default"  # 孤立节点也走 default 视觉
        elif is_input:
            node_type = "input"
        elif is_output:
            node_type = "output"
        else:
            node_type = "default"

        content_lines = [f"### {p.name}", "", p.description, ""]
        content_lines.append(f"`{len(p.rules)}` 条规则")
        if p.depends_on:
            content_lines.append(f"← `{p.depends_on}`（offset {p.offset_days}d）")
        content = "\n".join(content_lines)

        nodes.append(
            StreamlitFlowNode(
                id=p.name,
                pos=(0, 0),
                data={"content": content},
                node_type=node_type,
                source_position="right",
                target_position="left",
                draggable=False,
                deletable=False,
            )
        )

    edges: list[StreamlitFlowEdge] = []
    for p in presets:
        if p.depends_on and p.depends_on in names:
            edges.append(
                StreamlitFlowEdge(
                    id=f"{p.depends_on}->{p.name}",
                    source=p.depends_on,
                    target=p.name,
                    animated=True,
                    label=f"+{p.offset_days}d",
                    marker_end={"type": "arrowclosed"},
                )
            )

    return StreamlitFlowState(nodes=nodes, edges=edges)


# 必须 stash 在 session_state，否则每次 rerun 重建会导致 infinite re-render（库的 README 强调）
if "canvas_state" not in st.session_state:
    st.session_state.canvas_state = _build_initial_state()


left, right = st.columns([0.65, 0.35], gap="medium")

with left:
    st.session_state.canvas_state = streamlit_flow(
        key="nl_canvas",
        state=st.session_state.canvas_state,
        layout=LayeredLayout(
            direction="right",
            node_node_spacing=80,
            node_layer_spacing=180,
        ),
        height=560,
        fit_view=True,
        show_controls=True,
        show_minimap=False,
        # 只读模式：不让创建/删除/编辑
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

with right:
    selected_id = st.session_state.canvas_state.selected_id
    if not selected_id:
        st.info("👈 点击左侧节点查看 pool 详情")
    elif selected_id not in PRESET_SCREENS:
        st.warning(f"未知节点 `{selected_id}`（spike 阶段不应该出现）")
    else:
        preset = PRESET_SCREENS[selected_id]
        st.markdown(f"### `{preset.name}`")
        st.caption(preset.description)

        meta = []
        if preset.depends_on:
            meta.append(f"**依赖**：`{preset.depends_on}` + offset `{preset.offset_days}d`")
        else:
            meta.append("**输入**：全市场")
        meta.append(f"**规则数**：{len(preset.rules)}")
        st.markdown("\n\n".join(meta))

        st.markdown("**规则列表**")
        for i, rule in enumerate(preset.rules, 1):
            # Rule 是闭包（screen.rules 用 `def factory(): def _rule(df): ...; return _rule` 模式），
            # __name__ 是 "_rule"，要从 __qualname__ 取外层 factory 名（如 "first_limit_up._rule" → "first_limit_up"）
            qualname = getattr(rule, "__qualname__", "")
            rule_name = qualname.split(".")[0] if qualname else getattr(rule, "__name__", "rule")
            st.markdown(f"`{i:>2}` · `{rule_name}`")

        if preset.include_columns:
            st.markdown("**额外字段**：" + " ".join(f"`{c}`" for c in preset.include_columns))

        # spike 阶段不接 diagnostic / 命中表（B 阶段才做）
        st.markdown("---")
        st.caption("ℹ️ B 阶段将在此显示 per-rule diagnostic 漏斗 + 命中标的列表")
