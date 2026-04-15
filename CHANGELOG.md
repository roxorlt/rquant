# Changelog

本项目遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/) 格式，版本号遵循 [SemVer](https://semver.org/lang/zh-CN/)。

## [Unreleased]

### Added
-

### Changed
-

### Deprecated
-

### Removed
-

### Fixed
-

### Security
-

---

## [v0.2.0] — 2026-04-16 — Week 2: 复权因子 + 技术指标

### Added
- 复权因子层
  - `schema.ADJ_FACTOR_DDL`：`adj_factor` 表
  - `TushareAdapter.adj_factor(ts_codes, start, end)`
  - `DuckDBStore.upsert_adj_factor` / `get_daily_qfq(ts_code, start, end)` / `count_adj_factor`
  - `get_daily_qfq` 以该股票最新 adj_factor 为参照计算前复权价，同时返回原始价和因子值便于核验
- 技术指标层（基于前复权价，避免分红除权造成指标跳变）
  - `rquant.indicator.compute_indicators(df)`：MA5/10/20/60 + RSI6/14 + MACD(12,26,9) + KDJ(9,3,3)
  - KDJ 用 A 股常用口径（α=1/3 指数平滑）手写实现
  - `schema.DAILY_INDICATOR_DDL`：`daily_indicator` 表（13 列指标）
  - `DuckDBStore.upsert_indicators` / `count_indicators`
- `ingest_daily.py` 扩展：拉完 daily+factor 后自动基于全量 qfq 重算指标并入库
- `status.py` 扩展：展示首日/最新日的原始价 vs 前复权价对比、最新技术指标（含多空/金死叉判断）
- 依赖：`ta==0.11.0`（放弃 pandas-ta 因其锁死旧版 numba 与 pandas 3 冲突）
- 测试：新增 15 个单测（adj_factor/qfq 5 个 + 指标 8 个 + indicator 落库 2 个），累计 23 个全部通过

### Infrastructure
- Week 1 代码推送到 GitHub private：<https://github.com/roxorlt/rquant>

### Verified
- 茅台 2025-12-18 前复权收盘 1407.04（除权日 2025-12-19 前一天）对上东方财富
- 茅台 2026-04-15 MA5 = 1454.43 对上东方财富日 K 均线

---

## [v0.1.0] — 2026-04-16 — Week 1: 数据接入 + DuckDB 存储

### Added
- 项目 scaffold：uv 包管理 + Python 3.12 + pyproject.toml + ruff/pytest 配置
- 配置层：`rquant.config.Settings`（Pydantic Settings 读 `.env`，校验 token 长度、自动创建目录）
- 日志层：`rquant.logging.setup_logging`（loguru stderr + 按日轮转到 `logs/`，保留 30 天）
- 数据模型：`rquant.models.DailyBar`（Pydantic，frozen）
- Tushare Adapter：`rquant.adapter.TushareAdapter`
  - `daily(ts_codes, start, end)` 拉日线 OHLCV，主 token 失败自动切备用
  - `stock_basic(list_status)` 拉股票基础信息
- DuckDB 存储：`rquant.storage.DuckDBStore`
  - 建表 DDL 集中在 `schema.py`（`daily_bar` + `stock_basic`）
  - `upsert_daily` / `upsert_stock_basic` 幂等写入
  - context manager 支持
- CLI：`scripts/ingest_daily.py` 一次性拉历史日线入库
- 测试：8 个单测（config 4 + DuckDB 4），`tests/README.md` 规范说明

### Infrastructure
- 项目初始化：README + CLAUDE.md + docs/（data-sources-matrix、references）
- CHANGELOG.md（Keep a Changelog 格式）+ .gitignore（Python + data/ + .env）
- .env.example 模板，`.env` 忽略提交
- git init + `v0.0.1` scaffold tag

---

## 版本计划（MVP 路径对应）

| 版本 | 里程碑 | 对应周 |
|------|-------|--------|
| v0.1.0 | 数据接入 + DuckDB 存储跑通 | Week 1 |
| v0.2.0 | 指标计算模块 | Week 2 |
| v0.3.0 | 筛选规则引擎 | Week 3 |
| v0.4.0 | APScheduler 调度 | Week 4 |
| v0.5.0 | 盘中 Ashare 轮询监控 | Week 5 |
| v0.6.0 | cc2im 告警通知 | Week 6 |
| v0.7.0 | Streamlit 最小 UI（MVP 完整） | Week 7 |
| v1.0.0 | 第一次真正日常可用 | MVP 稳定后 |

---

<!--
模板用法：
- 每次合 main 前，把 [Unreleased] 下的条目整理好
- 打 tag 时，把 [Unreleased] 换成 [v0.X.0] - YYYY-MM-DD
- 下方重建一个空的 [Unreleased]
- 分类用：Added / Changed / Deprecated / Removed / Fixed / Security
-->
