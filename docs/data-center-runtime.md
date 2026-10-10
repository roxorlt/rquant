# 数据中心运行说明

## 当前状态

新页面使用 React。日线回补和财务采集沿用原任务、原数据函数和原提交回执。网页只提交操作，后台执行写入。

当前代码候选正在验收。隔离测试使用合成日历、假凭据和模拟供应商响应。它们不能证明真实接口权益、额度或生产安装。执行开关默认关闭。

## 页面操作

| 操作 | 用户需要做什么 | 后台实际执行 |
|---|---|---|
| 查看采集情况 | 打开数据集详情 | 核对原采集回执、固定副本和原审计报告；只确认记录中的范围 |
| 回补日线 | 生成计划，确认范围，再输入“日线回补” | 按封存的具体日期补整日空洞；保留正确既有行，拒绝冲突 |
| 采集财务 | 选择日期、报告期和股票，再输入“财务采集” | 调用原七接口，保存首次观察版本，导入原财务事实并装配每日六字段 |
| 暂停或继续 | 在当前任务上操作 | 保留原任务身份和已提交事实；在批次边界停止或继续 |
| 核对上次请求 | 请求结果不明确时点击核对 | 查询或重放同一操作身份，不另发一个任务 |
| 查看运行记录 | 查看最近运行记录 | 最多显示 20 条原任务状态和操作回执；最新记录在前 |

“所选范围已完成”要求原事实、派生结果、固定副本和新审计均通过。日线某天有记录，不代表全部股票或全部 24 类数据已采集。财务缺值保留“未知”。

财务计划使用成功审计报告中封存的股票清单。准备计划时，后台核对当前副本、原证明和完整副本元数据身份。主库可在同一文件上有后续提交；这不代表计划已读取主库的最新内容。主库或副本换代、元数据变动、原证明不符时，计划不会受理。

页面正文显示短状态、日期、数字和操作。来源摘要、内部编号和详细原因放在 Tip。Tip 支持鼠标悬停、键盘聚焦和手机点按。

## 启用前需要的材料

1. 确定安装的精确代码提交。保留对应校验值。
2. 为同一主库安装共同写入锁。日常采集、监控、手动写入、同步、备份、副本刷新和维护任务都须参与。
3. 核对主库、锁、原任务状态库和原额度账本的实际文件身份。路径须为绝对路径，不能是符号链接。
4. 取得当前供应商账号的接口权益、可用范围和额度证明。日线五接口与财务七接口分别核对；一个接口成功不能证明其他接口可用。
5. 配置完整、可信的 SSE 日历。缺日历或 SDK 返回零行不能推断休市。
6. 保存受信任运行配置和明确的允许用户清单。财务配置还须指定原财务归档目录。
7. 核对真实安装和资源，再单独授权生产写入与入口迁移。不要把测试配置当成安装证明。

任何材料缺失、过期、来源不明或身份不符时，执行入口保持关闭。每次实际 SDK 请求都重新核对当前政策、权益、范围、窗口和原额度账本。

## 配置入口

原 `Settings` 新增字段默认 `None` 或 `False`。未配置运行文件时，旧 CLI 保持原调用方式。

| 字段 | 作用 |
|---|---|
| `data_center_runtime_profile_path` | 指向受信任运行文件；用于 PageControl 工厂和显式日常采集 |
| `primary_writer_gate_path` | 让原写入者和副本刷新使用同一物理主库锁 |
| `data_center_execution_policy_path` | 供原运行装配配置引用；政策仍须绑定真实来源和身份 |
| `data_center_financial_archive_path` | 指定原不可变财务归档 |
| `data_center_backfill_execute_enabled` / `data_center_financial_collect_enabled` | 默认关闭；不能单靠布尔值证明执行条件满足 |

`Settings` 没有环境变量前缀。例如运行文件对应 `DATA_CENTER_RUNTIME_PROFILE_PATH`。运行文件、政策、安装证明和权益证明均不含网页可修改的路径。凭据继续由原配置层读取；不要写入页面或版本库。

运行文件由 `DataCenterRuntimeProfile` 校验，最多 64 KiB。必须包含这些字段：

```json
{
  "contract": "data-center-runtime-profile/v1",
  "policy_path": "/absolute/trusted/policy.json",
  "maintenance": {
    "replica_path": "/absolute/data/readonly.duckdb",
    "audit_state_path": "/absolute/state/audit.sqlite",
    "audit_directory": "/absolute/reports/audit",
    "collection_directory": "/absolute/reports/collection",
    "audit_null_fields": ["close"],
    "hash_timeout_seconds": 600
  },
  "plan_state_path": "/absolute/state/plans.sqlite",
  "plan_directory": "/absolute/reports/plans",
  "financial_archive_path": "/absolute/data/financial-archive",
  "allowed_owners": ["authorized-user"]
}
```

上例只说明字段，不能直接启用执行。政策必须绑定实际原账本、物理身份、供应商权益和安装证明。`daily_collection` 可选；配置时须使用原 `legacy_daily` 采集器、可信日历、固定运行身份及允许用户。`backfill_plan_assumptions` 可选；只影响原计划估算，不代表供应商扣额。

## 有限运行入口

使用已安装项目的 Python 3.11+ 环境。先从当前已确认任务取得精确执行编号和用户身份。每次命令只推进原任务的一轮。

```bash
rquant data-center-run \
  --profile /absolute/trusted/runtime-profile.json \
  --execution-id <exact-execution-id> \
  --owner <authorized-user> \
  --apply
```

该入口监督原子进程，随后核对进程已退出、主库能只读重开、共同写入锁能重新取得。工作日 17:50 后或周末可接新批次；次个工作日 08:20 停止接新批次，08:30 前释放锁。监督轮次最长 1,830 秒，原截止约束继续生效。

原审计队列可显式接入同一运行文件。一轮最多受理一个封存来源，并执行一个原审计任务：

```bash
python -m rquant.data_audit_report_runner \
  --state-path /absolute/state/audit.sqlite \
  --report-directory /absolute/reports/audit \
  --runtime-profile /absolute/trusted/runtime-profile.json \
  --bridge --once
```

持续原轮询使用 `--poll`，可选 `--poll-interval` 为 1–60 秒。不配置运行文件时，不启用采集桥接。原审计队列、冷却时间、租约和尝试上限不变。

原只读计划队列也可使用同一运行文件。状态库和计划目录须与运行文件完全相同：

```bash
python -m rquant.backfill_plan_runner \
  --state-path /absolute/state/plans.sqlite \
  --plan-directory /absolute/reports/plans \
  --runtime-profile /absolute/trusted/runtime-profile.json \
  --once
```

生成计划时，原临时硬链接会改变副本身份时间。显式配置的 worker 在原计划核验通过后，核对同源成功采集报告、原证明和完整副本摘要，再原子发布原副本元数据。报告、计划和来源水位不变。材料缺失或身份不同就拒绝恢复；计划摘要本身不能证明采集完成。旧入口不配置运行文件时保持原行为。

## 恢复与容量

- 原事实和本任务提交回执在同一个 DuckDB 事务内提交。未提交就回滚；提交响应丢失时先查询原回执。
- 已有耐久响应或原提交回执时可零 SDK 恢复。额度为零不影响这些材料的读取。未知响应或缺材料时不能换身份重发。
- 每个实际 SDK 请求最多六次原尝试。重启不能重置次数。恢复先核对原账本中的连续尝试和结果。
- 仅显式维护恢复可重排原失败审计任务。必须没有有效租约，并保持原任务、请求、证明和摘要。原六次上限与唯一活动任务约束继续生效。
- 固定副本最大 64 GiB。完整摘要按 1 MiB 分块计算，检查停止和截止。哈希不在 HTTP 或 SQLite 写事务内执行，也不复制大文件。
- 同时最多保留两个未完成 pin。失败或未知 pin 继续占容量；只有报告可读且原任务成功、无租约时才释放。
- 原派生函数每批最多处理 250 只股票。读取前检查原行数与材料预算，保留原指标、财务和 PIT 口径。
- 页面新增投影合计最多 512 KiB，包含最多 50 个任务和 20 条最近记录。旧资料仍可读，不会变成新的完成证明。

## 生产安装与回退

以下是仍需实际安装验收和单独生产授权的路径：

- `scripts/sync-readonly-replica.sh`
- `scripts/backup-snapshot.sh`
- `deploy/systemd/rquant-daily.service`
- `deploy/systemd/rquant-monitor.service`
- `deploy/systemd/rquant-backup.service`
- `deploy/systemd/rquant-replica-sync.service`

先准备精确提交、关闭的政策、参与者清单和 Linux 只读核验材料。unit 和公共锁接线须在真实主机核对后才启用。普通应用代码发布继续使用原受控部署器；不得绕过安装和生产写入边界。

回退时关闭新执行政策，停止接新任务。已有 claim 在边界释放。保留已提交事实、原归档和封存报告。不要删除生产事实、清空财务归档或整文件替换主库。

确认新功能、实际来源、资源和页面都可用后，再迁移对应入口并停用被替代的 Streamlit 单元。代码候选通过不等于已经切流或停服。

规格和实际验收材料位于 `data/verification/data-center-completion-20261006/`。未运行、跳过或被环境阻止的节点分别记录，不能记为通过。
