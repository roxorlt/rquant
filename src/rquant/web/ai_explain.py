"""AI interpretation of a backtest, with every number checked (roadmap item 15).

The model sees only a fixed list of formatted facts and must quote numbers
exactly as given. Afterwards every number in its answer is matched against
those facts; anything that does not match is returned as ``unverified`` and the
page shows a warning instead of trusting it.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from typing import Any

from pydantic import BaseModel

Complete = Callable[[list[dict[str, str]]], str]

SYSTEM = (
    "你是量化研究助手。只根据用户给出的「事实」解读这次组合回测，用中文写 3–5 条要点，"
    "每条一句话：收益与风险、相对基准、过拟合风险、可改进方向。"
    "引用数字时必须与事实中的写法完全一致，不要自己计算或换算新数字，不要编造事实之外的信息。"
)

_NUMBER = re.compile(r"-?\d+(?:\.\d+)?%?")


class Explanation(BaseModel):
    text: str
    facts: dict[str, str]
    unverified: list[str]
    model: str


def _pct(v: float | None) -> str | None:
    return None if v is None else f"{v * 100:.2f}%"


def _num(v: float | None, digits: int = 2) -> str | None:
    return None if v is None else f"{v:.{digits}f}"


def build_facts(run: Any, perf: Any, overfit: Any) -> dict[str, str]:
    facts: dict[str, str | None] = {
        "回测名称": run.title,
        "区间": f"{run.start} 至 {run.end}",
        "持股数上限": str(run.max_positions),
        "调仓间隔（信号次数）": str(run.rebalance_every),
        "成交笔数": str(run.filled),
        "拒单笔数": str(run.rejected),
    }
    if perf is not None:
        facts |= {
            "交易日数": str(perf.days),
            "总收益": _pct(perf.total_return),
            "年化收益": _pct(perf.annualized_return),
            "年化波动": _pct(perf.annualized_volatility),
            "夏普": _num(perf.sharpe),
            "最大回撤": _pct(perf.max_drawdown),
        }
        if perf.benchmark is not None:
            facts |= {
                "基准": perf.benchmark.code,
                "基准收益": _pct(perf.benchmark.total_return),
                "超额收益": _pct(perf.benchmark.excess_return),
                "Beta": _num(perf.benchmark.beta),
            }
    if overfit is not None:
        facts |= {
            "PSR": _pct(overfit.psr),
            "DSR": _pct(overfit.dsr),
            "同预设回测次数": str(overfit.trials),
        }
    return {k: v for k, v in facts.items() if v is not None}


def _allowed_numbers(facts: dict[str, str]) -> set[str]:
    allowed: set[str] = set()
    for value in facts.values():
        for token in _NUMBER.findall(value):
            allowed.add(token)
            allowed.add(token.lstrip("-"))
    return allowed


def unverified_numbers(text: str, facts: dict[str, str]) -> list[str]:
    allowed = _allowed_numbers(facts) | {str(i) for i in range(1, 6)}  # list numbering
    return sorted({t for t in _NUMBER.findall(text) if t not in allowed
                   and t.lstrip("-") not in allowed})


def explain(facts: dict[str, str], complete: Complete, model: str) -> Explanation:
    user = "事实：\n" + "\n".join(f"- {k}：{v}" for k, v in facts.items())
    text = complete([{"role": "system", "content": SYSTEM}, {"role": "user", "content": user}])
    return Explanation(text=text.strip(), facts=facts,
                       unverified=unverified_numbers(text, facts), model=model)


def deepseek_complete() -> tuple[Complete, str] | None:
    """The configured DeepSeek chat completion, or None when no key is set."""
    from rquant.config import settings

    if not settings.deepseek_enabled:
        return None
    from openai import OpenAI

    client = OpenAI(api_key=settings.deepseek_api_key, base_url=settings.deepseek_base_url,
                    timeout=60)

    def complete(messages: list[dict[str, str]]) -> str:
        reply = client.chat.completions.create(model=settings.deepseek_model,
                                               messages=messages,  # type: ignore[arg-type]
                                               temperature=0.2, max_tokens=600)
        return reply.choices[0].message.content or ""

    return complete, settings.deepseek_model
