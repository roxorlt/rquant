# 财务 PIT 表在云端热备合并中的归属

2026-09-29 集成分支的固定全集发现 `research_sync` 表分类比 DuckDB schema 少七张新表。本片只补这七张表的合并归属及直接回归；不运行生产同步、不修改真实主库或云端备份。

**分级：高风险。** 这些表保存首次观察、已提交导入批次、单调游标和基本面版本 head。误设成云端整表替换会抹去本地 PIT 证据；普通云端优先 merge 会让另一库的同键值覆盖本地首次观察或 head，污染历史筛选。

## 冻结权威与失败模型

- **权威**：`financial_observation`、`financial_import_batch`、`financial_import_cursor` 来自本地不可变财报归档与事务导入；`daily_basic_valuation_observation`、`daily_basic_valuation_batch` 保存本地首次观察顺序；`fundamental_daily_version`、`fundamental_daily_head` 由这些本地证据派生并以本地主库的事务 head 选定当前版本。当前云端快照没有这些表的权威身份与冲突合并协议。
- **决定**：上述七张表全部列入 `LOCAL_ONLY_TABLES`。热备合并不得从云端读取、替换或按同键覆盖它们；本地行和版本指针在同步前后保持原样。不能因云端暂时缺表而把它们放进 `MERGE_TABLES`：以后云端若出现同名表，也不能自动获得覆盖本地 PIT 的资格。
- **排除**：不改变旧生产表的 `REPLACE`/`MERGE` 分类，不迁移或修复生产财务库，不把未观察值补成已观察事实。以后若要跨主机复制财务 PIT，须先定义双方归档身份、观察顺序与 head 冲突规则，作为独立高风险范围审查。

## 验收

1. Schema 全表分类测试覆盖七张表，三个分类无交集；这七张精确属于 `LOCAL_ONLY_TABLES`，不进入 replace、merge 或研究表恢复默认集合。
2. 用临时本地 DuckDB 和合成云端备份验证同步前后七张表的内容与 head 不变，包括备份有同名冲突行的情况；若旧备份缺表，现有同步路径仍正常完成。测试只用合成数据，不访问生产库。
3. 直接相关同步、PIT 版本与只读筛选聚焦回归通过；独立终审只覆盖此分类与其直接依赖。
