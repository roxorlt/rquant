# 任务与调度总览：受信状态来源

接续已合入的研究任务队列页面，完成 v2 C13.1 的定时任务、常驻服务和资源概况。原型最终还包含日志、立即运行及研究任务控制，分别由 C13.2–C13.4 接续；本片不展示无行为的操作按钮。当前 `runtime_health` 有常驻 role 的心跳，`lab_jobs` 有研究队列；`rquant-workload-sample` 只是未自动启用的候选采样器，且其摘要是 24 小时历史峰值，不能当作当前资源。仓库尚无 `unit_status` 或 `scheduled_units` 发布源。页面不能据代码里的 timer 文件猜测服务器运行状态。

**分级：高风险。** 定时任务状态会成为运维判断依据，且状态采集接触共享的运行时健康与 Serving 发布链。实现前冻结以下信任边界；生产 unit、权限、发布及切流仍需按项目规则单独授权。

## 资产、失败路径与不变量

- 资产：固定允许查看的 rQuant timer/service 集合、主机 systemd 的只读状态、既有运行时心跳、同一 Serving 数据代、网页操作员。边界为 `systemd → 独立有界只读采集 → ops_status 权威快照 → Serving → Web API → React`。浏览器和 Web 进程不执行 `systemctl`，不接收可指定任意 unit 的采集参数；采集进程不读取 journal、`.env` 或主库。
- 失败路径：任意 unit 名注入命令；`systemctl` 卡住或输出过大；timer 已禁用却显示“正常”；一次服务运行失败却被当前 timer `active` 掩盖；UTC/上海时间混用；一次采样缺失或过期却沿用旧绿色状态；资源占用把历史峰值冒充当前；不同 Serving 数据代拼接；状态采集失败拖垮既有 runtime-health 发布。
- 只从固定白名单以 argv 调用 `/usr/bin/systemctl show`，不用 shell；单 unit、总时间、输出字节和 unit 数都有上限。timer 只读 `LoadState/UnitFileState/ActiveState/SubState/LastTriggerUSec/NextElapseUSecRealtime`，service 只读 `LoadState/ActiveState/SubState/Result/InvocationID/ExecMainStatus/ExecMainStartTimestamp/ExecMainExitTimestamp`；另读固定 `/proc/sys/kernel/random/boot_id`。属性缺失时该项未知，不反推成功。动态模板实例只能来自受签安装清单，不按前缀扫描自动纳入。
- 独立的短生命周期采集每 60 秒最多生成一次原子 `ops_status` 快照，包含采样时刻、主机/boot ID、安装清单摘要与每条原始证据；超时、输出超界或源身份变化则本次不发布。`runtime_health` 的 10 秒必需心跳和发布路径不调用 systemctl、不读取此快照，也不因其失败受阻。Serving 扩充精确 owner 集合到第七个 `ops_status`，在 `runtime_builder_serving`、`ServingSnapshotAssembler`、生产 profile/authority 根及 `DEFAULT_OPTIONAL_SOURCE_DATASETS` 中一起登记为**可选**，其缺失、采集失败或已知损坏只返回不可用水位并记录安全事件，不沿用旧绿色；未知程序错误仍按原完整性错误处理。正式采集 timer、authority 路径和安装配置另行授权。
- `ops_status` 快照超过采样时刻 120 秒即过期；Web 不用页面刷新时刻延长 TTL。资源当前内存只取本次快照同主机同 boot 的 `MemoryCurrent` 与 `/proc/meminfo`，明确标“当前”，`MemoryPeak` 若展示须标“本次启动峰值”。现有 `workload_evidence` raw 链和 24 小时摘要均不用于本片当前卡片；旧候选采样器未启用不影响 `ops_status`，但 `ops_status` 本身未安装时资源与定时任务都显示不可用。CPU 没有可信相邻采样时不显示使用率，配额或 reservation 也不能冒充 CPU 已用；CPU 完整接入留在 C13.1 未完成项。
- `RuntimeServiceHealth` 负责常驻 role 状态，只呈现受签运行时安装清单中的 role 与确切实例，最多 32 个；缺安装清单时 role 列表不可用，不按心跳里偶然出现的任意 ID 扩展。timer 的 `inactive` 不一概解释为异常：以同一安装清单中的预期启用状态、交易/非交易时段和 `NextElapseUSecRealtime` 判定，刻意停用显示“未运行”，预期启用却缺失/禁用才显示注意或异常。休市、等待开盘和已收盘按 `web/AGENTS.md` 五态规则表达。
- API 只给中文显示名、上次/下次时间、耗时、结果、状态和必要原因；技术 unit 名留在提示或详情。新增 `GET /tasks/overview?cursor=` 在**一次 Serving borrow** 中读取 `ops_status`、`runtime_health` 与 `lab_jobs`，返回三个分区及研究任务一页，游标绑定这一代；保留旧 `/tasks/jobs` 供兼容但本页不分别调用三个 GET。换代导致游标 409 时整页清理旧状态并从首页重取，不拼接 A/B。React 在同页保留研究队列，增设定时任务和运行状态；桌面与 390px 均能读上次/下次及状态，加载、无来源、过期、失败有不同反馈。C13.2–C13.4 未实现前，不出现无真实回执的日志、立即运行和暂停按钮。

## 单元清单与执行归属

- 静态 timer↔service 白名单以同名 `.timer/.service` 成对：`artifact-retention`、`backup`、`daily-report`、`daily`、`kpl-snapshot`、`midday-report`、`monitor-watchdog`、`monitor`、`morning-pulse`、`pre-market-check`、`replica-sync`、`research-ingest`、`surge-watch`、`tushare-token-reminder`，均带 `rquant-` 前缀。两组模板 `rquant-runtime-daily-orchestrator@`、`rquant-runtime-recovery-rehearsal@` 只接纳受签安装清单中的确切实例。`rquant-workload-sample.timer` 是未自动启用的候选，不在默认状态表；未在安装清单的其他一次性 service 也不列入。清单固定版本、每项对应 service 与 `expected_enabled`、适用时段、中文名和资源组；安装时由受控部署生成并签封，采集与 Serving 核对同一摘要，不能由网页请求更改。清单未发布时整类显示不可用；不能只挑少数“看起来正常”的 unit。
- `LastTriggerUSec` 只证明 timer 曾触发，不证明相邻 service 成功。要把 `Result` 归属为该次 timer 运行，必须有同一 boot 的受信触发回执明确绑定 timer 名、触发时刻、service `InvocationID`、启动/退出时刻和结果；再要求 service 启动不早于触发且在 120 秒内、结束不早于启动。仅有 `TriggeredBy` 配置关系、时间接近或 service 最近一次成功均不足以证明归属；无回执、手动运行、跨 boot、运行中或字段冲突时“上次结果”固定未知，但 timer 自身的启用/下次触发仍可展示。回执来源需要生产 service/timer 配置时，先另行授权；本地可用合成回执测试，不以当前无回执的绿色状态冒充完整交付。

## 实施与验收

1. 先实现与既有健康周期完全独立的短生命周期 `ops_status` 采集器和受签安装清单校验。用可注入的只读系统命令适配器，在 Linux 合成回放中证明固定 unit、时区、禁用、成功、失败、运行中、缺字段、超时、超长输出、恶意 unit 输入、不同 boot 与无触发回执的行为。采集不得修改 systemd 状态，不得接进 10 秒 runtime-health 周期；未安装独立采集任务时来源明确不可用。
2. 在 Serving 的精确 owner 集合、装配器、可选源配置与本地 authority 中一次性登记 `ops_status`；实现已知缺失、失败、损坏时的可选源降级与安全事件，未知完整性错误仍失败。只读一次 Serving 的 `GET /tasks/overview?cursor=` 用 Pydantic → OpenAPI → TypeScript 合同返回定时任务、受签 role、当前资源和研究队列一页。固定小列表有上限；研究队列游标必须绑定代次，保留已有 `/tasks/jobs` 兼容语义。
3. 任务页按原型同页展示定时任务、研究任务队列和运行状态，状态与短文案按 `web/AGENTS.md`；无受信触发回执的结果显示未知，CPU 未接入时显示暂无可信数据。定时任务操作留待 C13.2–C13.4 接通后出现。验收覆盖同代读取、换代整页清理、时区、空态、过期、已知损坏、资源当前值与峰值区分、错误、键盘和 390px。只跑直接相关 Python/Web/浏览器聚焦回归，最终候选安排一次独立高风险审查。
4. 正式服务器的签封安装清单、采集调度、触发回执、systemd 属性、服务用户读取权限、采集开销、资源样本新鲜度和约 10 GB 副本共存情况必须在受控环境单独验证；涉及 `deploy/systemd/`、权限或 Serving 生产发布配置的变更先形成具体候选并取得单独授权。若仍缺真实触发回执、CPU 证据或正式采集安装，只能将 C13.1 相应项记为未完成，不能称任务总览上线。
