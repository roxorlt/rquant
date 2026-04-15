# rQuant Index
> Auto-maintained by LLM. Last updated: 2026-04-16

| File | Summary | Tags | Status |
|------|---------|------|--------|
| [[README]] | 项目总览：定位、六层架构图、7 周 MVP 路径、技术选型、下一步 | #quant #a-shares #personal-tool | week-1-done |
| [[CLAUDE]] | Claude 项目指令：Mac 约束、技术栈、MVP 路径纪律、版本控制与部署、边界守则 | #meta #claude | active |
| [[CHANGELOG]] | Keep a Changelog — v0.0.1 scaffold + v0.1.0 Week 1（数据接入 + DuckDB） | #meta #changelog | active |
| [[tests/README]] | 测试规范：目录结构、pytest marker、分层覆盖目标 | #meta #testing | active |
| [[docs/data-sources-matrix]] | 19 个 A 股数据源竞品矩阵（调研成果） | #research #data-source | archived |
| [[docs/references]] | 开源参考项目分层（第一档 myhhub/stock、qstock、daily_stock_analysis 等） | #reference #open-source | archived |

## 代码结构（不纳入 Obsidian 索引，git 管理）

```
src/rquant/      config / logging / adapter(tushare) / storage(duckdb) / models
scripts/         ingest_daily.py（数据采集 CLI）+ status.py（人读健康报告）
tests/           unit + integration + fixtures
```

## 版本里程碑

| Tag | 日期 | 内容 |
|-----|------|------|
| v0.0.1 | 2026-04-15 | 项目 scaffold + 数据源调研 |
| v0.1.0 | 2026-04-16 | **Week 1 完成**：Tushare + DuckDB 链路打通 |
| v0.2.0 | — | Week 2：指标计算 + 复权因子 |
