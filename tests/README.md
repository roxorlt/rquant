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
