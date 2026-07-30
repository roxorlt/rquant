"""三重锁相 VP 策略子包（规格：docs/vp-strategy/VP-STRATEGY-SPEC.md）。

两层结构，权限边界见 `README.md` 与规格 §7：

- `engine/` 🔒 回测底座，只读。纯函数 + Pydantic，无 IO、无 DuckDB、无外网。
- `sandbox/` ✍️ 研究沙盒，仅 7 个可写文件。

本子包尚未接入任何生产调用链。
"""

from __future__ import annotations
