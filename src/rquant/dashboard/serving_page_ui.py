"""Explicit Serving health banners shared by Streamlit pages."""

from __future__ import annotations

from typing import Protocol

from rquant.dashboard.runtime_console_data import ServingFrameState


class _BannerTarget(Protocol):
    def warning(self, message: str) -> object: ...

    def error(self, message: str) -> object: ...


class ServingStateEvidence(Protocol):
    state: ServingFrameState
    detail: str


def render_serving_state_banner(
    target: _BannerTarget,
    evidence: ServingStateEvidence,
    *,
    label: str,
) -> None:
    detail = evidence.detail.strip() or "未提供状态详情"
    if evidence.state is ServingFrameState.STALE:
        target.warning(f"{label}已过期：{detail}")
    elif evidence.state is ServingFrameState.DEGRADED:
        target.warning(f"{label}处于降级状态：{detail}")
    elif evidence.state is ServingFrameState.UNAVAILABLE:
        target.error(f"{label}不可用：{detail}")


__all__ = ["ServingStateEvidence", "render_serving_state_banner"]
