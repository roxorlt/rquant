"""rQuant NL 选股页面 — 独立于监控看板的 Streamlit 应用。

启动方式（本地开发）：
    streamlit run src/rquant/dashboard/nl_screen.py --server.port 8502 \\
        --server.address 0.0.0.0 --server.headless true

监控看板跑在 8501（meta 30s 自动刷新），本页跑在 8502（不刷新，避免编辑时打断）。
未来部署：两个 Streamlit 应用各自上 systemd timer + nginx 反代，可分别开启 auth：
监控看板仅自己访问，NL 选股可选择性对外开放。
"""

from __future__ import annotations

import json
import re
from datetime import date as _date, datetime as _dt
from pathlib import Path

import pandas as pd
import streamlit as st
from pydantic import ValidationError

from rquant.config import settings
from rquant.llm.client import DeepSeekClient, LLMClarificationNeeded, LLMError
from rquant.llm.dispatch import screen_with_plan_diagnostic
from rquant.llm.schemas import ScreenPlan
from rquant.storage.duckdb import DuckDBStore


st.set_page_config(
    page_title="rQuant NL 选股",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="auto",
)


# ── 主体渲染 ──

st.markdown("## 🤖 自然语言选股")

if not settings.deepseek_enabled:
    st.error(
        "未配置 `DEEPSEEK_API_KEY`，本页不可用。"
        "请在 `.env` 中填入 DeepSeek API key。"
    )
    st.stop()

# session state
if "nl_plan_dict" not in st.session_state:
    st.session_state.nl_plan_dict = None
if "nl_history" not in st.session_state:
    st.session_state.nl_history = []
if "nl_result_df" not in st.session_state:
    st.session_state.nl_result_df = None
if "nl_diagnostics" not in st.session_state:
    st.session_state.nl_diagnostics = []

nl_query = st.text_input(
    "输入选股需求（中文）",
    placeholder="例：MA5 上穿 MA20 + 量比 > 2 + 流通市值 < 100 亿，昨天首板",
    key="nl_query_input",
)

nl_col_run, nl_col_clear = st.columns([1, 1])
with nl_col_run:
    nl_parse_clicked = st.button(
        "🔍 解析", type="primary", use_container_width=True, key="nl_parse_btn"
    )
with nl_col_clear:
    if st.button("🗑 清空", use_container_width=True, key="nl_clear_btn"):
        st.session_state.nl_plan_dict = None
        st.session_state.nl_result_df = None
        st.rerun()

if nl_parse_clicked and nl_query.strip():
    today_iso = _date.today().isoformat()
    nl_client = DeepSeekClient(
        api_key=settings.deepseek_api_key,
        base_url=settings.deepseek_base_url,
        model=settings.deepseek_model,
        log_path=Path(settings.log_dir) / "nl_queries.jsonl",
    )
    with st.spinner("正在解析..."):
        try:
            nl_plan_obj = nl_client.nl_to_screen_plan(nl_query, today=today_iso)
            st.session_state.nl_plan_dict = nl_plan_obj.model_dump()
            st.session_state.nl_history.insert(0, nl_query)
            st.session_state.nl_history = st.session_state.nl_history[:5]
        except LLMClarificationNeeded as e:
            st.warning(f"💬 LLM 需要澄清：{e}")
            st.session_state.nl_plan_dict = None
        except LLMError as e:
            st.error(f"❌ LLM 调用失败：{e}")
            st.session_state.nl_plan_dict = None
        except Exception as e:
            st.error(f"❌ 未预期错误（{type(e).__name__}）：{e}")
            st.session_state.nl_plan_dict = None

nl_plan_dict = st.session_state.nl_plan_dict
if nl_plan_dict:
    st.success(f"✅ 解析成功 · trade_date={nl_plan_dict['trade_date']}")
    if nl_plan_dict.get("rationale"):
        st.info(f"💭 {nl_plan_dict['rationale']}")

    # 顶部：加新一层按钮
    if st.button("➕ 在末尾加新一层", key="nl_add_stage_btn"):
        nl_plan_dict["stages"].append({"label": "新分层", "rules": []})
        st.rerun()

    # Stage Cards 渲染（可编辑）
    for nl_i, nl_stage in enumerate(nl_plan_dict["stages"]):
        with st.container(border=True):
            nl_col_label, nl_col_del_stage = st.columns([6, 1])
            with nl_col_label:
                nl_new_label = st.text_input(
                    f"layer-{nl_i}",
                    value=nl_stage["label"],
                    key=f"nl_stage_label_{nl_i}",
                    label_visibility="collapsed",
                )
                if nl_new_label != nl_stage["label"]:
                    nl_plan_dict["stages"][nl_i]["label"] = nl_new_label
            with nl_col_del_stage:
                if st.button("🗑", key=f"nl_del_stage_{nl_i}", help="删除整层"):
                    nl_plan_dict["stages"].pop(nl_i)
                    st.rerun()

            # 列规则，每条带删除按钮
            for nl_j, nl_rule in enumerate(nl_stage["rules"]):
                nl_args_str = ", ".join(
                    f"{k}={v!r}" for k, v in nl_rule["args"].items()
                )
                nl_label = (
                    f"`{nl_rule['name']}({nl_args_str})`"
                    if nl_args_str
                    else f"`{nl_rule['name']}()`"
                )
                nl_col_r, nl_col_del_r = st.columns([10, 1])
                with nl_col_r:
                    st.markdown(f"- ✓ {nl_label}")
                with nl_col_del_r:
                    if st.button(
                        "✕", key=f"nl_del_rule_{nl_i}_{nl_j}",
                        help="删除该规则",
                    ):
                        nl_plan_dict["stages"][nl_i]["rules"].pop(nl_j)
                        st.rerun()

            # 加规则下拉
            with st.expander("➕ 加规则到本层"):
                from rquant.llm.registry import REGISTRY, get_rule_spec

                nl_rule_options = {
                    f"{spec.name} — {spec.description}": spec.name
                    for spec in REGISTRY
                }
                nl_chosen = st.selectbox(
                    "选择积木",
                    options=list(nl_rule_options.keys()),
                    key=f"nl_add_rule_select_{nl_i}",
                )
                nl_args_json = st.text_area(
                    "args（JSON）",
                    value="{}",
                    key=f"nl_add_rule_args_{nl_i}",
                    height=68,
                )
                if st.button("加入", key=f"nl_add_rule_btn_{nl_i}"):
                    try:
                        nl_args = (
                            json.loads(nl_args_json)
                            if nl_args_json.strip()
                            else {}
                        )
                        nl_rule_name = nl_rule_options[nl_chosen]
                        # 用 RuleSpec.args_model 校验一遍
                        get_rule_spec(nl_rule_name).args_model.model_validate(
                            nl_args
                        )
                        nl_plan_dict["stages"][nl_i]["rules"].append(
                            {"name": nl_rule_name, "args": nl_args}
                        )
                        st.rerun()
                    except Exception as nl_e:
                        st.error(f"args 校验失败：{nl_e}")

        # 箭头（最后一层不画）
        if nl_i < len(nl_plan_dict["stages"]) - 1:
            st.markdown(
                "<div style='text-align:center;color:#888;font-size:24px;"
                "margin:-8px 0;'>↓</div>",
                unsafe_allow_html=True,
            )

    with st.expander("📄 查看完整 plan JSON"):
        st.json(nl_plan_dict)

    st.divider()
    nl_col_run, nl_col_date = st.columns([1, 2])
    with nl_col_date:
        try:
            _default_date = _date.fromisoformat(nl_plan_dict["trade_date"])
        except (ValueError, KeyError):
            _default_date = _date.today()
        nl_run_date = st.date_input(
            "trade_date",
            value=_default_date,
            key="nl_run_date_input",
        )
        nl_plan_dict["trade_date"] = nl_run_date.isoformat()
    with nl_col_run:
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button(
            "🚀 运行", type="primary",
            use_container_width=True, key="nl_run_btn",
        ):
            try:
                nl_plan_validated = ScreenPlan.model_validate(nl_plan_dict)
                nl_store = DuckDBStore(settings.duckdb_path)
                nl_df, nl_diag = screen_with_plan_diagnostic(
                    nl_plan_validated, store=nl_store,
                )
                st.session_state.nl_result_df = nl_df
                st.session_state.nl_diagnostics = nl_diag
            except ValidationError as nl_e:
                st.error(f"plan 校验失败：{nl_e}")
                st.session_state.nl_result_df = None
            except Exception as nl_e:
                st.error(f"运行失败：{type(nl_e).__name__}: {nl_e}")
                st.session_state.nl_result_df = None

    nl_result_df = st.session_state.nl_result_df
    if nl_result_df is not None:
        st.markdown(f"### 📊 命中 **{len(nl_result_df)}** 只")
        if len(nl_result_df) == 0:
            st.warning(
                "无标的命中。检查规则参数是否过严，或调整 trade_date。"
            )
            nl_diag = st.session_state.get("nl_diagnostics", [])
            if nl_diag:
                st.markdown("**逐条规则累加命中数（看哪条筛空）：**")
                nl_diag_df = pd.DataFrame(
                    nl_diag, columns=["规则", "累加命中"]
                )
                st.dataframe(
                    nl_diag_df,
                    use_container_width=True,
                    hide_index=True,
                )
        else:
            st.dataframe(
                nl_result_df,
                use_container_width=True,
                hide_index=True,
            )

            st.divider()
            nl_col_save_input, nl_col_save_btn = st.columns([3, 1])
            with nl_col_save_input:
                nl_save_name = st.text_input(
                    "保存为 preset 名（自动加 `user/` 前缀）",
                    key="nl_save_name_input",
                    placeholder="例：突破新高放量",
                )
            with nl_col_save_btn:
                st.markdown("<br>", unsafe_allow_html=True)
                if st.button(
                    "💾 保存",
                    use_container_width=True,
                    disabled=not nl_save_name.strip(),
                    key="nl_save_btn",
                ):
                    # 文件名安全化：保留字母/数字/下划线/中文/横线
                    nl_safe_name = re.sub(
                        r"[^\w一-鿿_-]",
                        "",
                        nl_save_name.strip(),
                    )
                    if not nl_safe_name:
                        st.error("名字含非法字符或为空")
                    else:
                        nl_user_presets_dir = (
                            Path(settings.data_dir) / "user_presets"
                        )
                        nl_user_presets_dir.mkdir(parents=True, exist_ok=True)
                        nl_preset_path = (
                            nl_user_presets_dir / f"{nl_safe_name}.json"
                        )
                        if nl_preset_path.exists():
                            st.error(
                                f"⚠️ 已有同名 preset："
                                f"{nl_safe_name}.json"
                            )
                        else:
                            nl_payload = {
                                "name": nl_safe_name,
                                "description": (
                                    st.session_state.nl_history[0]
                                    if st.session_state.nl_history
                                    else ""
                                ),
                                "rules": [
                                    rc
                                    for s in nl_plan_dict["stages"]
                                    for rc in s["rules"]
                                ],
                                "include_columns": nl_plan_dict.get(
                                    "include_columns", []
                                ),
                                "created_at": _dt.now().isoformat(
                                    timespec="seconds"
                                ),
                                "source": "nl_input",
                            }
                            nl_preset_path.write_text(
                                json.dumps(
                                    nl_payload,
                                    ensure_ascii=False,
                                    indent=2,
                                ),
                                encoding="utf-8",
                            )
                            st.success(
                                f"✅ 已保存到 {nl_preset_path}"
                            )

# 侧边栏：NL 查询历史
with st.sidebar:
    st.subheader("📜 NL 查询历史")
    if not st.session_state.nl_history:
        st.caption("（暂无）")
    else:
        for h in st.session_state.nl_history:
            short = h[:30] + ("..." if len(h) > 30 else "")
            if st.button(short, key=f"nl_hist_{hash(h)}"):
                st.session_state["nl_query_input"] = h
                st.rerun()
