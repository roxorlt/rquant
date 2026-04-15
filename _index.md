# rQuant Index
> Auto-maintained by LLM. Last updated: 2026-04-16 · GitHub: https://github.com/roxorlt/rquant (private)

| File | Summary | Tags | Status |
|------|---------|------|--------|
| [[README]] | 项目总览：定位、六层架构图、7 周 MVP 路径、技术选型、下一步 | #quant #a-shares #personal-tool | week-2-done |
| [[CLAUDE]] | Claude 项目指令：Mac 约束、技术栈、MVP 路径纪律、版本控制与部署、边界守则 | #meta #claude | active |
| [[CHANGELOG]] | Keep a Changelog — v0.0.1 scaffold + v0.1.0 Week 1（数据接入 + DuckDB） | #meta #changelog | active |
| [[tests/README]] | 测试规范：目录结构、pytest marker、分层覆盖目标 | #meta #testing | active |
| [[docs/data-sources-matrix]] | 19 个 A 股数据源竞品矩阵（调研成果） | #research #data-source | archived |
| [[docs/references]] | 开源参考项目分层（第一档 myhhub/stock、qstock、daily_stock_analysis 等） | #reference #open-source | archived |

## 代码结构（不纳入 Obsidian 索引，git 管理）

```
src/rquant/      config / logging / adapter(tushare) / storage(duckdb) / indicator / models
scripts/         ingest_daily.py（数据+因子+指标一次性采集）+ status.py（人读健康报告）
tests/           unit + integration + fixtures（23 个单测）
```

## 版本里程碑

| Tag | 日期 | 内容 |
|-----|------|------|
| v0.0.1 | 2026-04-15 | 项目 scaffold + 数据源调研 |
| v0.1.0 | 2026-04-16 | **Week 1 完成**：Tushare + DuckDB 链路打通 |
| v0.2.0 | 2026-04-16 | **Week 2 完成**：复权因子（adj_factor）+ 前复权查询 + MA/RSI/MACD/KDJ 指标 |
| v0.3.0 | — | Week 3：筛选规则引擎 |
