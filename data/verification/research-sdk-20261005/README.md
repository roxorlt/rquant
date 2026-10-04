# SDK 本机验收

实现树：`/Users/roxor/brain/30-projects/rQuant/.worktrees/cdx-research-sdk`。实际 Codex 原生角色为 `/root/research_sdk_impl`（implementer），父任务 `/root`；基准为 `51d1dc5a0a3c009e05d13de184ea78723a66c429`，产品基准为 `09b11f4407ccd9703baa6981d25224cc71390516`。

## 结果

| 记录 | 实际结果 |
| --- | --- |
| `red.log.gz` | SDK 尚不存在时 3 failed，均在明确的缺入口断言失败 |
| `green-initial.log.gz` | 1 failed / 2 passed；失败是隔离 Python 的示例参数转交，保留原记录 |
| `green.log` | 修正启动命令后 3 passed，3.13s |
| `green-final.log` | 最终 smoke 3 passed，2.83s；无 skip/deselect |
| `example.log` / `verify.log` | 独立 Notebook 等价示例退出码 0，0.482s |
| `resources.json` | 成功、范围错误、坏产物和示例结束后 FD/Python/native 线程均 `4/1/14`，新增量 0 |
| Ruff | 直接 SDK、测试及示例 lint 通过，3 个文件 format check 通过 |

两份失败日志按原始字节 gzip 保留，未去除 pytest 生成的尾空格；解压校验值见 `raw-log-archives.json`。当前验收树也保留未压缩的原件。

8 个合成代码、3 日、18 个实际字段与原 reader 完全一致。以最新日 2026-07-03 起核验：当日缺市场记录，前推 1 日为 NULL/NaN，前推 2 日为有效 0/100。单位、缺因和原值保持；市场源表为 2 行，查询只在有界当前代码集合内展开。未知字段、已知但当前来源缺少的分钟字段、范围外日期/代码、错误来源摘要、重复字段/代码、501 代码及额外请求字段均拒绝。

完整和展示产物来自已有 `_research` 测试工厂及原 publisher，完整请求在 SDK 中调用同一 `assemble_factor_research_result`。保留源、结果及校验清单见 `demo/inputs.json`，不是独立行情重放。源文件 byte hash 在读后未变；临时 reader、坏产物 scratch 均为 0。所有验证进程已退出。

`/bin/ps` 被沙箱限制；原生线程通过 macOS 标准 `proc_pidinfo` 只读当前 PID 测得。线程退出的短暂尾部用最多 1 秒的收尾观察处理；验证未改平台安全控制。

## 实际命令

Python 3.13.12，复用只读环境；未安装依赖或修改该环境，未读取 `.env` 或主库。

```bash
cd /Users/roxor/brain/30-projects/rQuant/.worktrees/cdx-research-sdk
env RQUANT_DISABLE_DOTENV=1 TUSHARE_TOKEN_MAIN=00000000000000000000000000000000 \
  /Users/roxor/brain/30-projects/rQuant/.worktrees/cdx-factor-source-integration/.venv/bin/python -I -B - <<'PY'
import os
import sys
from pathlib import Path
root = Path('/Users/roxor/brain/30-projects/rQuant/.worktrees/cdx-research-sdk')
proof = root / 'data/verification/research-sdk-20261005'
config = proof / 'test-config'
os.environ.update(DATA_DIR=str(config / 'data'), DUCKDB_PATH=str(config / 'primary.duckdb'), PARQUET_DIR=str(config / 'parquet'), LOG_DIR=str(config / 'logs'))
sys.path.insert(0, str(root / 'src'))
sys.path.insert(1, str(root))
import pytest
raise SystemExit(pytest.main(['tests/unit/test_research_sdk.py', '-q', '-o', 'addopts=', '-p', 'no:cacheprovider', '--basetemp', str(proof / 'pytest-tmp')]))
PY
```

独立资源验收生成命令为 `python -I -B data/verification/research-sdk-20261005/verify.py`，使用上述解释器。`verify.py` 首次创建固定的 `demo`，存在时拒绝覆盖；可执行示例的完整只读命令保存于 `resources.json` 及 `docs/research-sdk.md`。

Ruff 命令为上述解释器加 `-I -B -m ruff check --no-cache src/rquant/research_sdk.py tests/unit/test_research_sdk.py docs/examples/research_sdk.py`，format check 将 `check` 改为 `format --check`。

没有扩大到全仓测试、SQL 查询服务、云端 Notebook、网络或生产。仅本机 Python 3.13.12 在此记录中得到验证。最终独立审查及合版由 root 处理；M2 整体仍为部分。
