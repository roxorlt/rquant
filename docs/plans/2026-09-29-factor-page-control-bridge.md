# 因子定义命令接入 PageControl

接续因子定义仓库 `795e30d5`，实现 v2 C3.1 的 `save_factor_definition` 与 `archive_factor` 的**本地命令桥**。本片不开放 Web 路由、不配置生产仓库路径、不创建生产数据库、不发布 Serving，也不宣称 React 保存按钮可用。后续 Web 入口负责认证并生成命令身份；PageControl 负责持久投递、执行及恢复；因子仓库独占定义版本事务。三者不直接借用策略定义或生产 DuckDB。

**分级：高风险。** PageControl 与独立 SQLite 仓库之间存在跨进程崩溃窗口，若把已提交的版本当成失败重试，可能生成重复版本或把有效命令永久标失败。本片仅改变上述两种因子命令的执行路径，不修改其他命令的语义。

## 冻结失败模型

- **资产与边界**：命令 ID 与不可变载荷、因子版本与归档 head、原始回执，以及 PageControl 的 effect/command 状态。信任边界是未来已认证 Web 提交者 → PageControl outbox/consumer → 因子仓库；本片的注入式 backend 不代表在线身份验证。
- **失败路径**：仓库提交后进程在 PageControl 记成功前退出；恢复时仓库暂不可读；首次调用无 backend；同 ID 换载荷；并发新命令改变 head 后旧命令重放；仓库只读恢复时意外创建文件；坏回执被当成功；归档与保存同时争用同一 head。
- **必须保持**：同一命令只产生一个领域事务及同一份回执；恢复先以只读方式查精确命令及载荷，查到已提交结果则完成 PageControl effect，不凭当前 head 重做 CAS；查不到才可首次执行；仓库状态不可判定时保持待恢复，不能标成已失败或发第二次写；异载荷重放拒绝。已有 PageControl 命令路径保持原行为。因子定义的版本、历史与原回执完整性继续由仓库校验。
- **排除与阻断**：不开放未经认证的 Web 写入，不让 Serving/React 直写 SQLite，不迁移现有库或生产路径。没有显式 backend 时命令不能写入；仓库损坏时 fail closed。生产配置、发布和数据写入按项目单独授权处理。

## 合同和实现边界

1. `page_control.py` 增加有类型命令 `SaveFactorDefinition` / `ArchiveFactor`，字段为 `command_id`、`requested_at`、完整 `FactorDefinition` 或 `factor_id`、精确 `expected_head`。仅把新命令加入 discriminated union 和消费者分派；命令 ID 约束与仓库一致。PageControl 不计算下一版本，不改定义内容，不持有因子仓库连接。
2. 因子仓库公开只读 `lookup_command(request)`：接受有类型保存或归档请求，以命令 ID、动作、规范请求摘要与因子 ID 查原回执；不存在返回 `None`，异载荷冲突、坏回执/历史或旧 schema 抛明确异常。恢复不得调用 `save` / `archive`，不得创建缺失文件。已提交命令即使 head 后续推进或归档，仍返回原回执。
3. 新 backend 只负责把 PageControl 命令映射为领域请求，以及将领域回执稳定转成 JSON 结果；`submit` 走仓库 `save` / `archive`，`recover` 走 `lookup_command`。PageControl 的 `begin_effect`、`recover_started_effect`、`must_recover_before_failure` 与终结逻辑复用现有外部有回执 backend 模式：首次无 backend 可以明确失败；已开始或 backend 曾可执行时，恢复异常要保留待重试状态，不得把未知效果误报失败。
4. 服务构造器只接受显式注入的 `factor_definition_backend`，默认 `None`；本片不从环境变量猜生产路径，也不自动建库。Web/Serving 接线和部署配置作为后续独立切片。领域注册与命令分派保持各自内聚，避免把注册事务或身份判断塞进通用 consumer。

## 红测与完成条件

先用临时路径复现保存 v1/v2、归档、旧命令原样重放和异载荷冲突。再注入「仓库提交后 PageControl 记录 effect 前崩溃」：重启消费同一命令须恢复同一回执且不增加版本；恢复路径异常时保持可重试，不能产生第二次写或永久失败。覆盖空仓库只读查不建文件、旧 schema/坏回执 fail closed、两个命令争同一 head、首次无 backend，以及其他命令的一个聚焦回归。仅运行因子仓库、PageControl 新命令及直接受影响的服务构造测试、Ruff、diff；测试清单在集成分支统一重生。

先审此高风险 SPEC，再由原生 Codex implementer 编码；最终候选一次独立审查，阻断 finding 最多三轮定向修复。成功标准是本地命令桥有可复现的跨崩溃行为证据，且无生产或 Web 写入。
