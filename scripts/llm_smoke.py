#!/usr/bin/env python
# scripts/llm_smoke.py
"""手动 smoke test：用真实 API 跑几条 query，看 LLM 是否能稳定产出合理 plan。

不在 CI 中跑（费 API、不稳定）。开发本机或部署后人肉验证。

用法：
    uv run python scripts/llm_smoke.py
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

from rquant.config import settings
from rquant.llm.client import DeepSeekClient, LLMClarificationNeeded


def main() -> int:
    if not settings.deepseek_enabled:
        print("ERROR: DEEPSEEK_API_KEY not set in .env")
        return 1

    client = DeepSeekClient(
        api_key=settings.deepseek_api_key,
        base_url=settings.deepseek_base_url,
        model=settings.deepseek_model,
        log_path=Path(settings.log_dir) / "nl_queries.jsonl",
    )

    queries = [
        # Q1: 测试首板 + 市值 + 涨停状态组合（few-shot 覆盖了昨日首板，这里测前日首板变体）
        "找两天前首板、流通市值 50 亿以下、昨天和今天都没涨停的票",
        # Q2: 测试技术指标（few-shot 用了 MA5/MA20，这里换均线参数）
        "今天 MA10 上穿 MA60，量比超过 3，只看科创板和创业板",
        # Q3: 测试窗口聚合 + 板块（few-shot 用了 RSI，这里换连板+板块）
        "近 20 天出现过涨停的股票、不要 ST 也不要北交所、市值 200 亿以下",
        # Q4: 测试形态 + 历史窗口（few-shot 有下影线，这里换连板窗口）
        "今天首板、近 5 天没出现 2 连板以上、排除北交所",
        # Q5: 模糊问题，看 LLM 怎么处理（要澄清还是合理猜测）
        "今天涨幅大于 7% 的小票",
    ]
    today = date.today().isoformat()

    for i, q in enumerate(queries, 1):
        print(f"\n{'='*60}\n[{i}/{len(queries)}] Query: {q}\n{'-'*60}")
        try:
            plan = client.nl_to_screen_plan(q, today=today)
            print(f"trade_date: {plan.trade_date}")
            print(f"rationale:  {plan.rationale}")
            print("stages:")
            for s in plan.stages:
                print(f"  · {s.label}")
                for r in s.rules:
                    print(f"      - {r.name}({r.args})")
        except LLMClarificationNeeded as e:
            print(f"⚠️  LLM 请求澄清: {e}")
        except Exception as e:
            print(f"❌ 失败: {type(e).__name__}: {e}")

    print(f"\n日志写入：{Path(settings.log_dir) / 'nl_queries.jsonl'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
