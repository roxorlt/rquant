"""rQuant Dashboard 入口 — 路由到监控看板和 NL 选股两个独立页面。

启动方式：
    streamlit run src/rquant/dashboard/app.py --server.port 8501 \\
        --server.address 0.0.0.0 --server.headless true
"""

from __future__ import annotations

import streamlit as st

st.set_page_config(
    page_title="rQuant",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="auto",
)

monitor_page = st.Page(
    "monitor.py",
    title="监控看板",
    icon="📊",
    default=True,
)
nl_screen_page = st.Page(
    "nl_screen.py",
    title="NL 选股",
    icon="🤖",
)

nav = st.navigation([monitor_page, nl_screen_page])
nav.run()
