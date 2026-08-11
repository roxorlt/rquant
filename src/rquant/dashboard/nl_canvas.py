"""Serving-only canvas page.

Canvas definitions are published into the current Serving generation.  This
page never opens ``data/canvases`` or ``data/user_presets``; mutations cross
the loopback PageControl outbox and are visible after the next publication.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime
from uuid import uuid4

import streamlit as st

from rquant.dashboard.serving_page_data import ServingPageRenderContext
from rquant.dashboard.serving_page_ui import render_serving_state_banner
from rquant.page_control import (
    PageControlClient,
    PageControlUnavailableError,
    SaveCanvas,
    SaveUserPool,
)

_page_serving: ServingPageRenderContext | None = None


def _submit(client: PageControlClient, command: SaveCanvas | SaveUserPool) -> None:
    receipt = client.submit(command)
    if receipt.status != "succeeded":
        raise RuntimeError(receipt.error or "page control command did not complete")


def run_canvas_app() -> None:
    global _page_serving
    st.set_page_config(page_title="rQuant 画布", page_icon=None, layout="wide")
    st.title("rQuant 画布")
    try:
        _page_serving = ServingPageRenderContext.open(
            os.environ.get("RQUANT_SERVING_ROOT", "data/serving")
        )
    except Exception as exc:
        st.error(f"画布 Serving 不可用：{type(exc).__name__}: {exc}")
        return

    try:
        definitions = _page_serving.canvas_definitions()
        render_serving_state_banner(st, definitions, label="画布定义")
        if definitions.value is None:
            return
        if definitions.value.empty:
            st.info("尚无已发布的自定义画布")
        else:
            definition_names = definitions.value["name"].astype(str).tolist()
            selected_definition = st.selectbox("已保存画布", definition_names)
            st.dataframe(
                definitions.value.loc[
                    definitions.value["name"] == selected_definition
                ],
                width="stretch",
                hide_index=True,
            )

        catalog = _page_serving.dataframe(
            """
            SELECT preset_name, min_date, max_date, candidate_count
            FROM screen_bounds ORDER BY preset_name ASC LIMIT 256
            """,
            max_rows=256,
            max_result_bytes=256 * 1024,
            required_projections=("screen_bounds", "canvas_diagnostic", "canvas_hit"),
        )
        render_serving_state_banner(st, catalog, label="画布目录")
        if catalog.value is None:
            return
        if catalog.value.empty:
            st.info("尚无已发布的候选池投影")
        else:
            options = catalog.value["preset_name"].astype(str).tolist()
            selected = st.selectbox("当前候选池", options)
            st.dataframe(catalog.value, width="stretch", hide_index=True)
            latest = _page_serving.canvas_latest_trade_date()
            render_serving_state_banner(st, latest, label="画布交易日")
            if latest.value is None:
                return
            refs = _page_serving.dataframe(
                """
                SELECT ts_code, row_json FROM canvas_hit
                WHERE preset_name = ? AND trade_date = ? ORDER BY ts_code ASC LIMIT 1000
                """,
                (selected, latest.value),
                max_rows=1000,
                max_result_bytes=1024 * 1024,
                required_projections=("canvas_hit",),
            )
            render_serving_state_banner(st, refs, label="候选池成员")
            st.dataframe(refs.value, width="stretch", hide_index=True)

        client = PageControlClient()
        with st.expander("新建画布", expanded=False):
            name = st.text_input("画布名称", key="canvas_name")
            description = st.text_input("描述", key="canvas_description")
            if st.button("创建画布", type="primary"):
                try:
                    _submit(client, SaveCanvas(
                        command_id=uuid4().hex,
                        requested_at=datetime.now(UTC),
                        name=name,
                        description=description,
                        pool_refs=(),
                        source="canvas_page",
                    ))
                    st.success("已提交，下一次 Serving 发布后可见")
                except PageControlUnavailableError as exc:
                    st.warning(f"PageControl 暂不可用，未写入：{exc}")
                except Exception as exc:
                    st.error(f"创建失败：{type(exc).__name__}: {exc}")

        with st.expander("新建自定义池", expanded=False):
            base_name = st.text_input("池名称", key="pool_name")
            description = st.text_input("池描述", key="pool_description")
            if st.button("创建空池", type="primary"):
                try:
                    _submit(client, SaveUserPool(
                        command_id=uuid4().hex,
                        requested_at=datetime.now(UTC),
                        base_name=base_name,
                        description=description,
                        rule_calls=(),
                        include_columns=(),
                        source="canvas_page",
                    ))
                    st.success("已提交，下一次 Serving 发布后可见")
                except PageControlUnavailableError as exc:
                    st.warning(f"PageControl 暂不可用，未写入：{exc}")
                except Exception as exc:
                    st.error(f"创建失败：{type(exc).__name__}: {exc}")
    finally:
        if _page_serving is not None:
            _page_serving.close()
        _page_serving = None


run_canvas_app()
