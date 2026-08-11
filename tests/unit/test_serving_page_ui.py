from __future__ import annotations

from datetime import UTC, datetime

from rquant.dashboard.runtime_console_data import ServingFrameState
from rquant.dashboard.serving_page_data import ServingPageResult
from rquant.dashboard.serving_page_ui import render_serving_state_banner


class _Target:
    def __init__(self) -> None:
        self.messages: list[tuple[str, str]] = []

    def warning(self, message: str) -> None:
        self.messages.append(("warning", message))

    def error(self, message: str) -> None:
        self.messages.append(("error", message))


def _result(state: ServingFrameState) -> ServingPageResult[tuple[()]]:
    return ServingPageResult(
        state=state,
        detail=f"{state.value} watermark",
        generation_id="generation-1",
        generated_at=datetime(2026, 8, 3, tzinfo=UTC),
        value=(),
    )


def test_serving_banner_exposes_stale_degraded_and_unavailable_states() -> None:
    target = _Target()

    render_serving_state_banner(target, _result(ServingFrameState.READY), label="分钟数据")
    render_serving_state_banner(target, _result(ServingFrameState.STALE), label="分钟数据")
    render_serving_state_banner(target, _result(ServingFrameState.DEGRADED), label="分钟数据")
    render_serving_state_banner(target, _result(ServingFrameState.UNAVAILABLE), label="分钟数据")

    assert target.messages == [
        ("warning", "分钟数据已过期：stale watermark"),
        ("warning", "分钟数据处于降级状态：degraded watermark"),
        ("error", "分钟数据不可用：unavailable watermark"),
    ]
