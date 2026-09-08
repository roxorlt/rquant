# rQuant Tests

## 目录结构

```
tests/
├── unit/          # 单测（纯函数、内存 DB、mock 外部依赖）
├── integration/   # 集成测试（跨模块，可能较慢）
└── fixtures/      # 测试数据（小样本 parquet、mock 响应 JSON 等）
```

## 跑测试

```bash
# 默认：跑所有非 network 测试
uv run pytest

# 跑指定目录
uv run pytest tests/unit

# 跑网络测试（会真实调用 Tushare，消耗积分）
uv run pytest -m network

# 跑集成测试
uv run pytest -m integration

# 覆盖率
uv run pytest --cov=rquant --cov-report=term-missing
```

## Full Suite CI 分片

默认非网络、非 `linux_exact` 全集由
`tests/manifests/full-suite-v1/index.json` 固定为 **14137 cases / 55 skips**，并以
`0437aaba7ed17b06a95eddc44f8ea2d6a4e6fae869e76dff777d42054f78cc01` 绑定完整 nodeid
集合。五个 JSONL shard 必须并集精确等于该集合、彼此不重叠；不要手改 nodeid 或 digest。
分片数是 `scripts/full_suite_shards.py` 里的 `SHARD_COUNT`：改这个常量再重生成清单，
CI 矩阵 `shard: [0, 1, 2, 3, 4]` 要同步。

在改动测试选择面时，使用与 CI 相同的 dummy 配置重生成清单，再审查分配与 digest：

```bash
RQUANT_DISABLE_DOTENV=1 TUSHARE_TOKEN_MAIN=00000000000000000000000000000000 \
NOTIFY_ENABLED=false DATA_DIR=/private/tmp/rquant-ci/data \
DUCKDB_PATH=/private/tmp/rquant-ci/data/test.duckdb \
DUCKDB_READONLY_PATH=/private/tmp/rquant-ci/data/test_ro.duckdb \
PARQUET_DIR=/private/tmp/rquant-ci/parquet LOG_DIR=/private/tmp/rquant-ci/logs \
uv run python scripts/full_suite_shards.py generate \
  --manifest-dir tests/manifests/full-suite-v1
```

CI 先由 `core-preflight` 在 Python 3.11/3.12 运行 lint、smoke 与 SourceBroker 边界；
随后 `full-suite-shard` 在每个 Python 版本运行五片。runner 每次都会重新 collect 默认
selector、校验全量和 shard digest，并仅通过 pytest `@argsfile` 传入 nodeid。每版本的
`full-suite-contract` 下载全部 5 份 JUnit/selection evidence，缺任一 artifact、版本或
shard 混入、JUnit 失败/error、case/skip 偏移都会失败。

shard runner 与 contract aggregator 通过同一个 stdlib 环境准备入口创建 canonical 私有
根目录；默认 collect 会强制 dummy token、禁用 dotenv/通知并清除继承凭据，不读取仓库配置。

v1 manifest 的 selector 固定为空列表。index 与 JSONL 必须是无重复 key、无未知字段的
canonical JSON；nodeid 文件部分只能指向仓库内非符号链接的 `tests/**/*.py`。单 nodeid
上限 1,100,000 bytes、单 JSONL 行上限 1,100,032 bytes、manifest 总量上限 4 MiB。

## 跨版本 schema 快照

`tests/fixtures/runtime-schema-contracts/v0.33.1.json` 是生产第三代（producer_commit
`a0bbb4c`、21 条 channel）真实写下的 `schema-contracts.json`。仓库里其他 schema 转换用例的
两侧都由工作树里的同一份代码生成，改了载荷形状之后「前代」也跟着改，转换永远是绿的；安装器
比的却是新代码生成的 bundle 与**已装那一代**留下的这份文件。这份快照是全套用例里唯一一个
代码改不动的「前代」，`tests/unit/test_runtime_schema_release_snapshot.py` 与
`tests/unit/test_runtime_deployment_bundle.py::test_installer_accepts_the_generation_production_actually_published`
读它。

**快照由集成者在每个发布 tag 上刷新，节奏与上面的全集清单一致。** 刷新时放进来的必须是那一刻
**生产上实际装着**的那一代的 `schema-contracts.json`（从
`<runtime_root>/current/schema-contracts.json` 取），不是本地生成的 bundle。

用例红了只有两条合法出路，改快照本身不是其中之一：

1. 服务侧载荷又内嵌了会变的模型 —— 恢复冻结投影（这就是 #237）。
2. 真的要给那条 channel 升 `schema_version`，并且这次升版是走完整 rollout
   （PREPARE → DUAL_WRITE → …）的 —— 那么在那次发布里由集成者换上新一代的快照。

## 约定

- **不联网测试打 `@pytest.mark.network`**：默认被 `addopts = -m 'not network'` 跳过
- **集成测试打 `@pytest.mark.integration`**：跨模块、较慢的
- **数据类测试用 fixtures/**：不要每次联网拉真数据，用固定样本（5 只股票 × 100 天）
- **DB 测试用 `tmp_path` fixture**：pytest 自动提供临时目录，每条测试独立 DuckDB 文件
- **不测第三方库**：tushare / duckdb / pandas 的行为不归我们测
- **Mock 边界**：mock 外部 HTTP / 文件系统边界，不 mock 自己代码内部

## 分层覆盖目标（MVP 阶段）

| 模块 | 强度 | 说明 |
|---|---|---|
| adapter/ | 单测 + 集成测 | 上游 API 变更最常见的事故源 |
| indicator/（Week 2 加） | 单测必须 | 纯函数，bug 直接影响选股结果 |
| rule/（Week 3 加） | 单测必须 | 同上 |
| scheduler/（Week 4 加） | smoke test | 主要靠 APScheduler 自己 |
| notifier/（Week 6 加） | smoke test | 挂了不会死人 |
| UI（Week 7 加） | 不测 | 手动点 |
