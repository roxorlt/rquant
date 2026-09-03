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
import secrets
from collections.abc import Callable
from datetime import UTC, datetime
from datetime import date as _date
from typing import Literal
from uuid import uuid4

import pandas as pd
import streamlit as st
from pydantic import JsonValue, ValidationError

from rquant.config import settings
from rquant.dashboard.serving_page_data import (
    bind_nl_screen_plan_session,
    load_nl_screen_page_session,
    read_nl_screen_page,
    reset_nl_screen_page_session,
)
from rquant.dashboard.serving_page_ui import render_serving_state_banner
from rquant.llm.client import DeepSeekClient, LLMClarificationNeeded, LLMError
from rquant.llm.dispatch import build_rules
from rquant.llm.schemas import RuleCall, ScreenPlan
from rquant.page_control import AppendNlQueryLog, PageControlClient, SaveNlPreset
from rquant.serving_paths import serving_root_from_env
from rquant.serving_read_models import nl_screen_query_digest

st.set_page_config(
    page_title="rQuant NL 选股",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="auto",
)

_page_serving = None


# ── 主体渲染 ──

st.markdown("## 🤖 自然语言选股")

if not settings.deepseek_enabled:
    st.error("未配置 `DEEPSEEK_API_KEY`，本页不可用。请在 `.env` 中填入 DeepSeek API key。")
    st.stop()

try:
    _page_control = PageControlClient()

    def _record_nl_query(
        query: str,
        *,
        plan: JsonValue | None,
        outcome: Literal["success", "clarification", "error"],
        error: str | None = None,
    ) -> None:
        try:
            _page_control.submit(
                AppendNlQueryLog(
                    command_id=uuid4().hex,
                    requested_at=datetime.now(UTC),
                    query=query,
                    plan=plan,
                    outcome=outcome,
                    error=error,
                )
            )
        except Exception as exc:
            st.warning(f"查询日志暂未写入 control service：{exc}")

    # session state
    if "nl_plan_dict" not in st.session_state:
        st.session_state.nl_plan_dict = None
    if "nl_history" not in st.session_state:
        st.session_state.nl_history = []
    if "nl_cursor_signing_key" not in st.session_state:
        reset_nl_screen_page_session(
            st.session_state,
            cursor_signing_key=secrets.token_bytes(32),
            plan_digest=None,
        )
    if "nl_result_df" not in st.session_state:
        st.session_state.nl_result_df = None
    if "nl_diagnostics" not in st.session_state:
        st.session_state.nl_diagnostics = []
    if "nl_next_cursor" not in st.session_state:
        st.session_state.nl_next_cursor = None
    if "nl_current_cursor" not in st.session_state:
        st.session_state.nl_current_cursor = None
    if "nl_start_cursor" not in st.session_state:
        st.session_state.nl_start_cursor = None
    if "nl_cursor_history" not in st.session_state:
        st.session_state.nl_cursor_history = []
    if "nl_page_error" not in st.session_state:
        st.session_state.nl_page_error = None
    if "nl_plan_digest" not in st.session_state:
        st.session_state.nl_plan_digest = None

    def _bind_nl_plan_state(plan: dict[str, object]) -> None:
        include_columns = tuple(str(column) for column in plan.get("include_columns", ()))
        plan_digest = nl_screen_query_digest(plan, include_columns)
        bind_nl_screen_plan_session(
            st.session_state,
            plan_digest=plan_digest,
            cursor_signing_key_factory=lambda: secrets.token_bytes(32),
        )

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
            reset_nl_screen_page_session(
                st.session_state,
                cursor_signing_key=secrets.token_bytes(32),
                plan_digest=None,
            )
            st.rerun()

    if nl_parse_clicked and nl_query.strip():
        today_iso = _date.today().isoformat()
        nl_client = DeepSeekClient(
            api_key=settings.deepseek_api_key,
            base_url=settings.deepseek_base_url,
            model=settings.deepseek_model,
            log_path=None,
        )
        with st.spinner("正在解析..."):
            try:
                nl_plan_obj = nl_client.nl_to_screen_plan(nl_query, today=today_iso)
                st.session_state.nl_plan_dict = nl_plan_obj.model_dump()
                st.session_state.nl_history.insert(0, nl_query)
                st.session_state.nl_history = st.session_state.nl_history[:5]
                _record_nl_query(
                    nl_query,
                    plan=nl_plan_obj.model_dump(mode="json"),
                    outcome="success",
                )
            except LLMClarificationNeeded as e:
                _record_nl_query(
                    nl_query,
                    plan=None,
                    outcome="clarification",
                    error=str(e),
                )
                st.warning(f"💬 LLM 需要澄清：{e}")
                st.session_state.nl_plan_dict = None
            except LLMError as e:
                _record_nl_query(
                    nl_query,
                    plan=None,
                    outcome="error",
                    error=str(e),
                )
                st.error(f"❌ LLM 调用失败：{e}")
                st.session_state.nl_plan_dict = None
            except Exception as e:
                _record_nl_query(
                    nl_query,
                    plan=None,
                    outcome="error",
                    error=f"{type(e).__name__}: {e}",
                )
                st.error(f"❌ 未预期错误（{type(e).__name__}）：{e}")
                st.session_state.nl_plan_dict = None

    nl_plan_dict = st.session_state.nl_plan_dict
    if nl_plan_dict:
        _bind_nl_plan_state(nl_plan_dict)

        def _nl_rules_and_labels(
            plan: ScreenPlan,
        ) -> tuple[tuple[Callable[[pd.DataFrame], pd.Series], ...], tuple[str, ...]]:
            labels: list[str] = []
            for call in plan.flatten_rules():
                argument_text = ", ".join(f"{key}={value!r}" for key, value in call.args.items())
                labels.append(
                    f"{call.name}({argument_text})" if argument_text else f"{call.name}()"
                )
            return tuple(build_rules(plan)), tuple(labels)

        def _load_nl_page(
            plan: ScreenPlan,
            *,
            cursor: str | None,
            navigation: Literal["replace", "next", "previous"],
        ) -> bool:
            rules, labels = _nl_rules_and_labels(plan)
            page_result = load_nl_screen_page_session(
                st.session_state,
                load_page=lambda: read_nl_screen_page(
                    serving_root_from_env(),
                    trade_date=plan.trade_date,
                    rules=rules,
                    rule_labels=labels,
                    normalized_plan=plan.model_dump(mode="json"),
                    include_columns=tuple(plan.include_columns),
                    page_size=100,
                    signing_key=st.session_state.nl_cursor_signing_key,
                    cursor=cursor,
                ),
                navigation=navigation,
            )
            if page_result is None:
                return False
            render_serving_state_banner(st, page_result, label="自然语言选股数据")
            return True

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
                    nl_args_str = ", ".join(f"{k}={v!r}" for k, v in nl_rule["args"].items())
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
                            "✕",
                            key=f"nl_del_rule_{nl_i}_{nl_j}",
                            help="删除该规则",
                        ):
                            nl_plan_dict["stages"][nl_i]["rules"].pop(nl_j)
                            st.rerun()

                # 加规则下拉
                with st.expander("➕ 加规则到本层"):
                    from rquant.llm.registry import REGISTRY, get_rule_spec

                    nl_rule_options = {
                        f"{spec.name} — {spec.description}": spec.name for spec in REGISTRY
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
                            nl_args = json.loads(nl_args_json) if nl_args_json.strip() else {}
                            nl_rule_name = nl_rule_options[nl_chosen]
                            # 用 RuleSpec.args_model 校验一遍
                            get_rule_spec(nl_rule_name).args_model.model_validate(nl_args)
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
            _bind_nl_plan_state(nl_plan_dict)
        with nl_col_run:
            st.markdown("<br>", unsafe_allow_html=True)
            if st.button(
                "🚀 运行",
                type="primary",
                use_container_width=True,
                key="nl_run_btn",
            ):
                try:
                    nl_plan_validated = ScreenPlan.model_validate(nl_plan_dict)
                    _load_nl_page(
                        nl_plan_validated,
                        cursor=None,
                        navigation="replace",
                    )
                except ValidationError as nl_e:
                    st.error(f"plan 校验失败：{nl_e}")
                    st.session_state.nl_result_df = None
                except Exception as nl_e:
                    st.error(f"运行失败：{type(nl_e).__name__}: {nl_e}")
                    st.session_state.nl_result_df = None

        if st.session_state.nl_page_error is not None:
            st.error(st.session_state.nl_page_error)

        nl_result_df = st.session_state.nl_result_df
        if nl_result_df is not None:
            st.markdown(f"### 📊 本页命中 **{len(nl_result_df)}** 只")
            if len(nl_result_df) == 0:
                st.warning("无标的命中。检查规则参数是否过严，或调整 trade_date。")
                nl_diag = st.session_state.get("nl_diagnostics", [])
                if nl_diag:
                    st.markdown("**逐条规则累加命中数（看哪条筛空）：**")
                    nl_diag_df = pd.DataFrame(nl_diag, columns=["规则", "累加命中"])
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

                nl_prev_col, nl_next_col = st.columns(2)
                with nl_prev_col:
                    previous_cursor = (
                        st.session_state.nl_cursor_history[-1]
                        if st.session_state.nl_cursor_history
                        else None
                    )
                    if st.button(
                        "上一页",
                        disabled=not st.session_state.nl_cursor_history,
                        use_container_width=True,
                        key="nl_previous_page_btn",
                    ):
                        loaded = _load_nl_page(
                            ScreenPlan.model_validate(nl_plan_dict),
                            cursor=previous_cursor,
                            navigation="previous",
                        )
                        if loaded:
                            st.rerun()
                with nl_next_col:
                    if st.button(
                        "下一页",
                        disabled=st.session_state.nl_next_cursor is None,
                        use_container_width=True,
                        key="nl_next_page_btn",
                    ):
                        loaded = _load_nl_page(
                            ScreenPlan.model_validate(nl_plan_dict),
                            cursor=st.session_state.nl_next_cursor,
                            navigation="next",
                        )
                        if loaded:
                            st.rerun()

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
                        try:
                            receipt = _page_control.submit(
                                SaveNlPreset(
                                    command_id=uuid4().hex,
                                    requested_at=datetime.now(UTC),
                                    name=nl_save_name.strip(),
                                    description=(
                                        st.session_state.nl_history[0]
                                        if st.session_state.nl_history
                                        else ""
                                    ),
                                    rule_calls=tuple(
                                        RuleCall.model_validate(rule)
                                        for stage in nl_plan_dict["stages"]
                                        for rule in stage["rules"]
                                    ),
                                    include_columns=tuple(nl_plan_dict.get("include_columns", [])),
                                )
                            )
                            if receipt.status != "succeeded":
                                raise RuntimeError(receipt.error or "control command failed")
                            st.success(f"✅ 已保存 preset：{nl_save_name.strip()}")
                        except Exception as exc:
                            st.error(f"保存失败：{exc}")

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

except Exception:
    raise
finally:
    if _page_serving is not None:
        _page_serving.close()
