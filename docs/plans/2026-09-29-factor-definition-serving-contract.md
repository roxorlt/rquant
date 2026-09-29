# 因子定义只读表：同代发布合同

把已核验的因子定义快照接入 Lab 所拥有的共享 Serving 表合同，供后续因子库 Web API 在单个固定数据代读取。本片止于发布输入、表合同和同代核验；不开放浏览器写入口，不创建生产仓库，也不声称因子页已经可用。

**任务分级：高风险。** 本片改动所有 Serving 数据代共用的物理表合同与 Lab 页面投影来源；半份表、动态接受换库或截断后的可信空态会误导后续研究操作。

## 冻结失败模型

- **资产与信任边界**：因子定义 SQLite 的不可变版本/命令回执与固定文件身份 → 已核验的 `FactorDefinitionServingSnapshot` → Lab authority 的两次一致读取 → Serving 单代 `factor_definition_state` 和 `factor_definition` 表。SQLite 是权威；Serving 仅是可丢弃的只读副本。因子 ID、表达式及目录路径均不授予文件或代码执行权限。
- **失败路径**：仓库缺失、被同路径替换、实例 ID 变化、schema/历史/回执损坏；第 513 条因子或表字节超限；第一次与第二次 authority 读取间变化；状态表存在而定义表缺失、行数/排序/摘要不符；发布后旧数据代与新表拼接；未配置仓库时把空表当作可信空库。
- **不变量**：仅当调用方**显式提供**仓库和预期 `FactorRegistryIdentity`，并通过原只读快照验证，才生成一对完整投影。预期身份不从当前文件动态重取；无配置保持两表均 `projection_not_published`。配置后任何缺失、换库、损坏或超限均拒绝整个本次来源发布，不悄悄返回上一次或空表。可信空库发布一行 `status=empty,count=0` 与零条定义；有库发布 `status=populated` 和所有当前 head，包括归档，按 `factor_id` 稳定排序；最多 512 条。状态和定义在同一 `available_at`、同一 Lab owner generation 中封存，状态行携带快照摘要；从完整两表重构并核对摘要、数量、唯一键与状态。两次 authority 读取若不同则拒绝，不发布半代。Web 后续只能借用一个固定 Serving generation 读这对表。
- **排除项**：此片不证明历史采集 PIT、因子检验结果或跟踪健康，不把表达式解释为 Python/SQL，不配置生产运行时路径，不修改 nginx/systemd/sudoers，不给 Web 或 React 添假数据/按钮，不迁移/停用 Streamlit。

## 精确合同

1. `PAGE_PROJECTION_CONTRACTS` 新增 `factor_definition_state`（恰一行，`status_key=current`, `status`, `definition_count`, `registry_instance_id`, `snapshot_sha256`）和 `factor_definition`（最多 512 行，`factor_id`, `version`, `content_sha256`, `name_zh`, `category`, `direction`, `expression`, `earliest_available_date`, `dependency_columns_json`, `max_history_window`, `archived`）。owner 均为 `lab_jobs`，列类型、排序键及字节上限固定；依赖列用规范 JSON 字符串，避免逗号拼接歧义。内部实例 ID、摘要、表达式由后续 Web 选择性放在详情或提示，不直接塞进页面正文。
2. 因子域单一转换函数接收已核验 `FactorDefinitionServingSnapshot`，生成这对 `ServingProjectionPayload`；反向核验从表重建领域快照，并确认可用时间、定义顺序、数量、摘要、归档标记。`LabPageProjectionSnapshot` 把两表作为全有或全无的可选组；不允许单表或重复表，完整组必须通过反向核验。
3. `DuckDBLabPageProjectionSource` 只接受成对的可选 `FactorDefinitionRegistry` 和固定预期身份，不自行初始化/修复仓库。配置时在每次调用中用明确 `observed_at` 投影；超出发布时刻、坏仓库或超限直接抛来源完整性错误。原有未配置路径与表集合保持可用；本片不把可选参数接入生产 manifest，生产启用及固定身份设置另行验收。
4. 发布链沿用现有 Lab authority 两次读取和 Serving 固定 generation 规则。对同一个 `observed_at`，仓库未变化时两次投影字节相等；两次之间保存或归档时，第二次不等且拒绝该次发布。不可用表不冒充可信空库。

## 可执行验收

- 用真实临时 SQLite 测空库、多个当前定义、归档与多版本；经 `DuckDBLabPageProjectionSource` 生成两表，并通过 `LabPageProjectionSnapshot`、`ServingReadModelInput` 到 Serving 物理表的聚焦路径核对同一 owner generation。
- 红测：缺一表、行数/摘要/排序/归档/依赖列篡改、513 项、字节超限、缺库/换库/旧 schema/坏命令回执、两次读取中途保存。每项必须整组拒绝或保持两表未发布，不产生可信空态。维持现有未配置 Lab 来源回归。
- 仅运行新增测试和直接受影响的 Serving/Lab 回归、Ruff、diff；独立 reviewer 先审此 SPEC，最终候选一次覆盖当前 diff 与验收，阻断修复上限按项目规则。

下一片再把固定仓库身份接入运行时配置和只读 Web API；React 显示、编辑/归档命令、Lab 检验与跟踪分别验收。
