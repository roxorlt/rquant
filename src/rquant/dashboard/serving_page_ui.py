"""Explicit Serving health banners shared by Streamlit pages."""

from __future__ import annotations

from typing import Protocol

from rquant.dashboard.serving_only_page_data import ServingFrameState


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


def render_serving_root_failure(
    target: _BannerTarget,
    *,
    root: str,
    error: BaseException,
) -> None:
    """Announce a Serving root that could not be opened at all.

    Without this the page renders every business block empty and looks like a
    quiet day of no data; the operator has to guess that the serving publisher
    is down.  Name the root, the exception and the service to check.
    """

    target.error(
        f"Serving 根不可读：{root}（{type(error).__name__}: {error}）。"
        "本页业务区块将保持空白——这是 serving 发布链路故障，不是当日没有数据；"
        "请检查 rquant-runtime-serving@ 服务与 RQUANT_SERVING_ROOT。"
    )


__all__ = [
    "ServingStateEvidence",
    "render_serving_root_failure",
    "render_serving_state_banner",
]
