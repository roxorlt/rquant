"""DeepSeek system prompt + few-shot examples。"""

from __future__ import annotations

from typing import Any

from rquant.llm.schema_export import build_rule_catalog_md

_SYSTEM_TEMPLATE = """你是 A 股选股助手。用户用中文描述选股意图，你调用 build_screen 工具构建结构化方案。

# 工作流

1. 解析用户意图：识别其中的过滤条件（市值 / 板块 / 涨停状态 / 技术指标 / K线形态 / 历史窗口聚合）
2. 选择对应积木，按以下推荐顺序分组到 stages：
   - 第 1 层「基础过滤」：not_st / not_bj / circ_mv_lt 等普适过滤
   - 第 2 层「涨停状态」：first_limit_up / not_limit_up / consecutive_ups_gte / yiziban 等
   - 第 3 层「形态」：has_lower_shadow / gt 比较等
   - 第 4 层「技术指标」：cross_above / above_ma / rsi_oversold / volume_ratio_gte 等
   - 第 5 层「历史窗口」：no_consec_ups_in_window / no_limit_down_in_window / has_prior_limit_up
3. 调用 build_screen，stages 有序，每个 stage 含 label（中文）+ rules
4. 在 rationale 中用 1-2 句说明你的组合思路

# 重要规则

- **必须只用积木目录中列出的 name**，不要发明
- **args 必须严格匹配每个积木的参数 schema**（类型、范围、默认值）
- **offset 含义统一**：0=今天（trade_date 当日），1=昨天（T-1），2=前天（T-2），以此类推
- **circ_mv_lt 阈值单位是亿元**（threshold_yi=100 表示 100 亿）
- **如果用户描述含糊或缺关键信息**，不要瞎猜——返回纯文本请用户澄清，不调用 tool
- **通常应包含 not_st**（除非用户明确说"包括 ST"）
- **trade_date 由调用方传入**：除非用户明确指定日期，否则填空字符串 ""，调用方会替换为最新交易日

# 积木目录

{rule_catalog}
"""


def build_system_prompt() -> str:
    """渲染完整 system prompt。"""
    return _SYSTEM_TEMPLATE.format(rule_catalog=build_rule_catalog_md())


# Few-shot examples：每条覆盖一类常见 query
FEW_SHOT_EXAMPLES: list[dict[str, Any]] = [
    {
        "user": "找昨天首板、流通市值 100 亿以下、今天没涨停的票",
        "assistant_tool_call": {
            "name": "build_screen",
            "arguments": {
                "trade_date": "",
                "stages": [
                    {"label": "基础过滤", "rules": [
                        {"name": "not_st", "args": {}},
                        {"name": "not_bj", "args": {}},
                        {"name": "circ_mv_lt", "args": {"threshold_yi": 100}},
                    ]},
                    {"label": "涨停状态", "rules": [
                        {"name": "first_limit_up", "args": {"offset": 1}},
                        {"name": "not_limit_up", "args": {"offset": 0}},
                    ]},
                ],
                "rationale": "先过滤 ST/北交所/大盘股，再用昨日首板+今未涨停定位次新机会。",
            },
        },
    },
    {
        "user": "今天 MA5 上穿 MA20，量比超过 2，主板创业板都行",
        "assistant_tool_call": {
            "name": "build_screen",
            "arguments": {
                "trade_date": "",
                "stages": [
                    {"label": "基础过滤", "rules": [
                        {"name": "not_st", "args": {}},
                        {"name": "board_in", "args": {"boards": ["main", "gem"]}},
                    ]},
                    {"label": "技术指标", "rules": [
                        {"name": "cross_above", "args": {"fast": "MA5", "slow": "MA20", "offset": 0}},
                        {"name": "volume_ratio_gte", "args": {"n": 2.0, "offset": 0, "window": 5}},
                    ]},
                ],
                "rationale": "金叉 + 放量是经典短线信号，主板创业板覆盖大多数活跃标的。",
            },
        },
    },
    {
        "user": "RSI14 超卖、近 30 天没跌停过、不要北交所",
        "assistant_tool_call": {
            "name": "build_screen",
            "arguments": {
                "trade_date": "",
                "stages": [
                    {"label": "基础过滤", "rules": [
                        {"name": "not_st", "args": {}},
                        {"name": "not_bj", "args": {}},
                    ]},
                    {"label": "技术指标", "rules": [
                        {"name": "rsi_oversold", "args": {"period": 14, "threshold": 30, "offset": 0}},
                    ]},
                    {"label": "历史窗口", "rules": [
                        {"name": "no_limit_down_in_window", "args": {"window": 30}},
                    ]},
                ],
                "rationale": "RSI 超卖找潜在反弹候选，30 天无跌停过滤掉趋势恶化的标的。",
            },
        },
    },
    {
        "user": "昨日首板、有下影线、近 8 天没出现 3 连板",
        "assistant_tool_call": {
            "name": "build_screen",
            "arguments": {
                "trade_date": "",
                "stages": [
                    {"label": "基础过滤", "rules": [
                        {"name": "not_st", "args": {}},
                        {"name": "not_bj", "args": {}},
                    ]},
                    {"label": "涨停状态", "rules": [
                        {"name": "first_limit_up", "args": {"offset": 1}},
                        {"name": "not_yiziban", "args": {"offset": 1}},
                    ]},
                    {"label": "形态", "rules": [
                        {"name": "has_lower_shadow", "args": {"min_ratio": 1.5, "min_amplitude": 0.02, "offset": 0}},
                    ]},
                    {"label": "历史窗口", "rules": [
                        {"name": "no_consec_ups_in_window", "args": {"threshold": 3, "window": 8}},
                    ]},
                ],
                "rationale": "昨首板非一字（保证可买入）+ 今日下影（说明承接好），近 8 天无 3 连板避开过热标的。",
            },
        },
    },
]
