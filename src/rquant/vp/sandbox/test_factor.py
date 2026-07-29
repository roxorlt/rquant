"""✍️ 沙盒因子单元测试（空壳，W2 脚手架）。

`factor.py` 里每个信号函数配一条用例：合成数据 + 手算期望值，断言到具体数字。

不被 `pytest` 默认收集——`pyproject.toml` 的 `testpaths = ["tests"]` 只扫 tests/。
沙盒测试要跑就显式指定路径：

    uv run pytest src/rquant/vp/sandbox/test_factor.py -q

引擎本身的测试在 `tests/unit/test_vp_engine.py`，属于 🔒 只读区，不在沙盒可写清单内。
"""

from __future__ import annotations
