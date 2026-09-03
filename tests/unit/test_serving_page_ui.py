from __future__ import annotations

from datetime import UTC, datetime

from rquant.dashboard.runtime_console_data import ServingFrameState
from rquant.dashboard.serving_page_data import ServingPageResult
from rquant.dashboard.serving_page_ui import (
    render_serving_root_failure,
    render_serving_state_banner,
)


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


def test_unreadable_serving_root_is_announced_as_a_pipeline_failure() -> None:
    target = _Target()

    render_serving_root_failure(
        target,
        root="/home/lighthouse/rquant/data/runtime/serving",
        error=FileNotFoundError("serving root is missing or is not a directory"),
    )

    assert target.messages == [
        (
            "error",
            "Serving 根不可读：/home/lighthouse/rquant/data/runtime/serving"
            "（FileNotFoundError: serving root is missing or is not a directory）。"
            "本页业务区块将保持空白——这是 serving 发布链路故障，不是当日没有数据；"
            "请检查 rquant-runtime-serving@ 服务与 RQUANT_SERVING_ROOT。",
        )
    ]
