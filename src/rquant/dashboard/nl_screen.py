"""Retired Streamlit page (kept as a stub so existing service units still start).

Its functions moved to the React app at /app/ (rquant.web). The only Streamlit page
still maintained is market_panorama.py (市场全景).
"""

from __future__ import annotations

import streamlit as st

st.set_page_config(page_title="rQuant", layout="centered")
st.info("这个页面已下线，请使用新版网页：/app/（市场全景仍在 market_panorama 页面）。")
