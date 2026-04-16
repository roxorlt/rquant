"""Week 3b 冒烟脚本：在本地 DuckDB 的真实数据上跑用户原始场景，肉眼检查结果。

运行前确保 data/warehouse/rquant.duckdb 已有足够覆盖的近 60 日数据。
"""

from __future__ import annotations

import sys

from rquant.screen import (
    first_limit_up,
    gt,
    not_bj,
    not_limit_up,
    not_st,
    screen,
)


def main(trade_date: str) -> None:
    result = screen(
        trade_date=trade_date,
        rules=[
            not_st(),
            not_bj(),
            first_limit_up(offset=1),
            not_limit_up(offset=0),
            gt("HIGH[0]", "CLOSE[1]"),
        ],
        include_columns=["MA20[0]", "CONSECUTIVE_LIMIT_UPS[1]"],
    )
    print(f"[{trade_date}] 命中 {len(result)} 只股票")
    print(result.to_string(index=False))


if __name__ == "__main__":
    date = sys.argv[1] if len(sys.argv) > 1 else "2026-04-15"
    main(date)
