# 因子定义命令接入 PageControl

接续因子定义仓库 `795e30d5`，实现 v2 C3.1 的 `save_factor_definition` 与 `archive_factor` 的**本地命令桥**。本片不开放 Web 路由、不配置生产仓库路径、不创建生产数据库、不发布 Serving，也不宣称 React 保存按钮可用。后续 Web 入口负责认证并生成命令身份；PageControl 负责持久投递、执行及恢复；因子仓库独占定义版本事务。三者不直接借用策略定义或生产 DuckDB。

**分级：高风险。** PageControl 与独立 SQLite 仓库之间存在跨进程崩溃窗口，若把已提交的版本当成失败重试，可能生成重复版本或把有效命令永久标失败。本片仅改变上述两种因子命令的执行路径，不修改其他命令的语义。

## 冻结失败模型

- **资产与边界**：命令 ID 与不可变载荷、因子版本与归档 head、原始回执，以及 PageControl 的 effect/command 状态。信任边界是未来已认证 Web 提交者 → PageControl outbox/consumer → 因子仓库；本片的注入式 backend 不代表在线身份验证。
- **失败路径**：通用 loopback HTTP 把未经认证的因子写入放进 outbox；仓库提交后进程在 PageControl 记成功前退出；恢复时仓库暂不可读、文件被移走或换成空库；首次调用无 backend；同 ID 换载荷；并发新命令改变 head 后旧命令重放；仓库只读恢复时意外创建文件；坏回执被当成功；归档与保存同时争用同一 head。
- **必须保持**：通用解析器、HTTP `/v1/commands`、`PageControlService.submit` 和 `PageControlOutbox.enqueue` 都不能入队因子写命令；仅预留内部受信入口，未来须由已认证 Web 绑定 actor 身份后调用。同一命令只产生一个领域事务及同一份回执。PageControl 在首次领域写入前持久记录仓库身份栅栏；已开始命令的恢复必须在**同一已核验仓库**只读查精确命令及载荷，查到已提交结果则完成 effect，不凭当前 head 重做 CAS；只有同库确认无该命令才可首次执行。仓库缺失、换代或状态不可判定时保持待恢复，不能标成已失败或发第二次写；异载荷重放拒绝。已有 PageControl 命令路径保持原行为。因子定义的版本、历史与原回执完整性继续由仓库校验。
- **排除与阻断**：不开放未经认证的 Web 写入，不让 Serving/React 直写 SQLite，不迁移现有库或生产路径。没有显式 backend 或预初始化仓库时命令不能写入；仓库损坏时 fail closed。生产配置、发布和数据写入按项目单独授权处理。外部攻击者在同一 inode 内直接篡改 SQLite 字节或同时回滚 outbox 与仓库属于更广泛的主机完整性问题，不在本桥声称解决范围内。

## 合同和实现边界

1. `page_control.py` 增加 ownerless 有类型请求 `SaveFactorDefinition` / `ArchiveFactor`，字段为 `command_id`、`requested_at`、完整 `FactorDefinition` 或 `factor_id`、精确 `expected_head`；命令 ID 约束与仓库一致。与现有受信价格规则相同，持久命令是由受信入口写入认证 actor 的内部 owned 类型。通用 parser 和通用 service/outbox API 必须拒绝两种因子 kind，即使调用方直接构造命令对象；只保留内部受信提交方法和其显式 actor 参数，本片不挂到公开 HTTP。PageControl 不计算下一版本、不改定义内容、不持有因子仓库连接。
2. 因子仓库给每个**显式预初始化**的新库一个随机持久实例 ID，并提供可核验的仓库身份（实例 ID + 文件设备/inode + 固定绝对路径）；旧 schema 不自动迁移，已有本地候选由测试重建。PageControl 在任何因子领域写入前把该身份记录进 STARTED effect 的持久结果，且调用 backend 时传入期望身份。缺身份的 STARTED effect 只能在尚未调用 backend 的既定代码顺序下记录身份；已有身份与当前仓库不符时只能待恢复。仓库事务和只读查询均验证期望身份，路径丢失或换库不得悄悄创建新库或执行。
3. 因子仓库公开只读 `lookup_command(request, expected_identity)`：接受有类型保存或归档请求，以命令 ID、动作、规范请求摘要与因子 ID 查原回执；**在同一预初始化仓库中**命令不存在才返回 `None`。异载荷冲突、坏回执/历史、缺库、换代或旧 schema 抛明确异常。恢复不得调用 `save` / `archive`，不得创建文件。已提交命令即使 head 后续推进或归档，仍返回原回执。
4. 新 backend 只负责映射 PageControl 命令与领域请求，以及将领域回执稳定转成 JSON 结果；`submit` 走带身份校验的仓库 `save` / `archive`，`recover` 走只读 `lookup_command`。PageControl 的 `begin_effect`、`recover_started_effect`、`must_recover_before_failure` 与终结逻辑复用现有有回执 backend 模式：首次无 backend 可以明确失败；已开始或 backend 曾可执行时，恢复异常要保留待重试状态，不得把未知效果误报失败。
5. 服务构造器只接受显式注入的 `factor_definition_backend`，默认 `None`；本片不从环境变量猜生产路径，也不自动建库。Web/Serving 接线和部署配置作为后续独立切片。领域注册与命令分派保持各自内聚，避免把注册事务或身份判断塞进通用 consumer。

## 红测与完成条件

先用临时预初始化仓库复现保存 v1/v2、归档、旧命令原样重放和异载荷冲突。原始 HTTP POST、通用 parser、service.submit/outbox.enqueue 拒绝因子写；受信入口写入认证 actor 后成功入队。再注入「仓库提交后 PageControl 记录 effect 前崩溃」：重启消费同一命令须恢复同一回执且不增加版本；移走原库或换空库时不建新库、不二次提交，命令保持可重试；恢复原库后拿原回执且仅一笔事务。同一原库中确实没有命令时可继续首次执行。覆盖缺身份、只读查不建文件、旧 schema/坏回执 fail closed、两个命令争同一 head、首次无 backend，以及其他命令的一个聚焦回归。仅运行因子仓库、PageControl 新命令及直接受影响的服务构造测试、Ruff、diff；测试清单在集成分支统一重生。

先审此高风险 SPEC，再由原生 Codex implementer 编码；最终候选一次独立审查，阻断 finding 最多三轮定向修复。成功标准是本地命令桥有可复现的跨崩溃行为证据，且无生产或 Web 写入。
