# React 平台：安装与回退准备

更新：2026-10-07 16:41，上海时间。

**已完成离线清单和线上只读核对。尚未安装、切换或停止旧服务。**

本清单用于交付项 R13。先准备可审阅的精确版本和回退步骤，再在会话中提出安装授权。安装、切换、正式推送和旧服务停用分别遵守项目 AGENTS.md 的授权范围。

## 1. 还缺哪些发布材料

| 材料 | 当前状态 |
|---|---|
| 已合入 main 的精确 tag、完整提交号和最终改动 | 未确定 |
| Python 3.11/3.12 必需 CI 和整体候选验收 | 等最终候选 |
| 同一版本的网页、API、运行时包和签名安装回执 | 等最终冻结 |
| 正式数据、来源证明、允许用户和有效期 | 未齐备 |
| 实际安装服务、目录权限和资源限制 | 需要准备精确差异并单独授权 |

任务基准 `7cd052123e9d1356b22f1b7a67115bf1fdf3da69` 和当前有未提交改动的工作树都不是可部署版本。

## 2. 线上现在是什么状态

10 月 7 日 16:38–16:41，通过已授权的 SSH 只读核对取得以下事实。

| 检查项 | 实际结果 |
|---|---|
| 当前 `/app/` | 仍指向 CC 临时网页 |
| 临时 API | PID `2783622`，用户和组均为 `1001`，占用 `127.0.0.1:8768` |
| PID 文件与端口 | 均对应上述进程 |
| 临时 API 的数据 | 仍读取 9 月旧回放目录；公开状态为 `stale`，即数据已过期 |
| 正式网页服务 | `rquant-web.service` 尚未安装 |
| 研究执行与调度服务 | `rquant-lab-worker.service`、`rquant-lab-scheduler.service` 尚未安装 |
| 手动通知测试服务 | `rquant-notify-test.service` 尚未安装 |
| 正式网页链接和数据指针 | `current`、`previous`、正式 Serving `current.json` 和 runtime `installation.json` 均缺失 |
| 旧看板 | `rquant-dashboard.service` 正在运行 |

旧网页链接、临时 API 启动参数、打开的数据文件和 API 返回的数据版本相互一致。这些资料可用于准备首次切换回退，不能证明正式平台已经可用。

本次还读到旧 `rquant-daily.service` 处于 failed，旧 `rquant-monitor.service` 处于 inactive。这里只记录状态；没有诊断、重启或修改它们。常驻服务状态可能变化，安装前须重新核对。

两次 SSH 读取均已正常退出。没有读取凭据或进程环境，没有创建远程文件或常驻进程。

## 3. 安装顺序与验收条件

1. **冻结候选。** 完成适用的合版、CI 和发行决定。若原 R07 发布规则适用，沿用其前序证据和合并提交门禁。
2. **记录现状。** 登记已安装提交、服务与定时器状态、PID、端口、用户、目录权限和资源限制。记录旧网页与恢复入口。查不到的项保持未确定。
3. **准备正式输入。** 包括完整交易日历、分钟快照、真实来源与签名回执、可信密钥目录、路由和恢复材料。测试资料不能代替正式来源。原日历生成器默认要求覆盖至 `2027-12-31`；合法降低期限时须记录实际期限和原因。
4. **核对 Linux 条件。** 验证实际 unit 和 timer 语法、用户隔离、目录与通信权限、可信代码执行，以及原资源门禁。未改部分可复用有效证据；新服务和新配置必须实测。原资源核验还要求同一次启动内至少 24 小时的真实高水位记录。
5. **安装所需部分。** 主代理先提出精确基础设施差异和回退方案，获单独授权后执行。沿用原安装接口；代码部署器不能代替基础设施授权。
6. **先准备新网页，再切换。** 正式数据、权限和 API 自检通过后，重新核对临时 PID 与 8768 端口。获得该切换授权后才停临时 API。原发布器先切代码、启动并验证 API，最后切网页。
7. **验收替代功能。** 核对数据日期、休市和未知状态、用户权限、原请求恢复、报告下载、关键页面和实际资源。正式通知、生产数据写入和旧 Streamlit 退役分别取得授权。缺少完整功能或来源证据时保留对应旧服务。

需要重启路线 A 或旧常驻服务时，保留原工作日 `09:15–15:10` 禁止重启规则。新的只读网页服务按原网页脚本处理。

### 各条链路需要什么

| 功能 | 必需条件 |
|---|---|
| 可信运行时 | 精确代码、解释器、实例、签名和安装证明；模板文件存在不算已安装 |
| 数据与业务 | 日历、分钟、竞价、候选、特征、策略、模拟盘和通知由原所有者提供；读者读受验副本或封存资料，写者沿用共同锁 |
| 发布与健康 | 原发布关系完整，正式 Serving 自检通过；额外健康观测默认关闭，核验真实主机、启动和时间资料后才开；未知不能补成正常 |
| 私有网页操作 | 实际用户与角色、Unix socket 用户权限、代理证明、CSRF 与同源检查、当前数据版本、原 UUID 恢复和功能开关 |
| 分钟研究 | 原 Lab 登记、准备、准入、队列、worker、封存与读取；完整参数和研究结果接口仍待最后冻结 |
| 静态网页 | 精确 tag、正式 API、最小静态权限，以及桌面和 390px 实际验收 |

网页 unit 本身没有配置完全部私有操作。最终配置须列出用户、角色、代理、socket、功能开关和分钟代码安装值。缺项保持不可用。

手动通知测试须有独立签名服务安装证明、无同名 timer 和竞争任务证明，以及原 StartUnit 归属回执。之后实际验证 10 分钟频控、恢复和通道尝试。通道接受请求不能算手机已送达。

## 4. 原发布命令草案

**以下发布命令均未执行。** 只读主机核对不等于执行这些命令。`pending` 须替换为已验收的精确值。

```bash
INSTALL_TARGET_TAG='pending'
INSTALL_TARGET_SHA='pending'
[[ "$INSTALL_TARGET_TAG" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]] || exit 2
[[ "$INSTALL_TARGET_SHA" =~ ^[0-9a-f]{40}$ ]] || exit 2
cd /home/lighthouse/rquant
export RQUANT_RUNTIME_PRODUCTION_INPUTS=/home/lighthouse/rquant/data/runtime-production-inputs.json
export RQUANT_RUNTIME_PROFILE_OUTPUT_DIR=/home/lighthouse/rquant/data/runtime-profiles
export RQUANT_RUNTIME_ROOT=/home/lighthouse/rquant/data/runtime
bash scripts/deploy-production.sh --target "$INSTALL_TARGET_SHA" --dry-run
# 审阅原 intent/change plan，实际发布仍走同一部署器
bash scripts/deploy-production.sh --target "$INSTALL_TARGET_SHA"
```

生产输入、profile 和 runtime 根必须精确匹配。上述代码发布入口没有授权生成正式输入或安装 root 基础设施。

```bash
# 已受验的精确 tag 中取出的原发布脚本；首次没有 current 时不能从 current 启动
bash /home/lighthouse/rquant-web/web-release.sh --target "$INSTALL_TARGET_TAG" --prepare --dry-run
bash /home/lighthouse/rquant-web/web-release.sh --target "$INSTALL_TARGET_TAG" --prepare
# 仅首次安装/旧临时 API 停止均已获准后
bash /home/lighthouse/rquant-web/releases/"$INSTALL_TARGET_TAG"/scripts/web-release.sh --target "$INSTALL_TARGET_TAG"
# unit 已受控安装后只读核验，不改变任何 unit 状态
bash /home/lighthouse/rquant/scripts/verify-workload-isolation.sh
env RQUANT_DISABLE_DOTENV=1 RQUANT_SERVING_ROOT=/home/lighthouse/rquant/data/runtime/serving \
  /home/lighthouse/rquant-web/current/.venv/bin/rquant web-serve --self-check
```

`--prepare` 会创建发行目录并设置静态权限，属于写入。网页脚本只接受精确 SemVer tag；生产部署器也接受完整提交号。首次安装前，还须从已验收 tag 取出并核对网页发布脚本。

## 5. 首次切换怎样回退

以下路径已于 16:41 重新核对。切换前仍须再核对一次。

- 旧网页：`/home/lighthouse/rquant-web-interim/0516efbfe1060b79217a8417291557fb690bd9db/web/dist`
- 旧 API 工作目录：上述路径去掉 `/web/dist`。
- 旧启动入口：该目录的 `.venv/bin/rquant web-serve --bind 127.0.0.1:8768`。
- 旧数据根：`/home/lighthouse/replay/runs/20260925T173059-13cb88/host/data/runtime/serving`。
- 旧 PID 文件：`/home/lighthouse/rquant/var/web-interim/api.pid`。
- 数据版本：`81a643a4444140dd6ab8791437e63befa49427b04721cfe1fafb509a0fe91bd4`。磁盘指针、进程打开文件和 API 返回值一致。

首次安装没有 `previous`，原网页脚本不能自动恢复临时 API。失败时按 [DEPLOY.md 首次回退步骤](../../DEPLOY.md) 恢复上述旧网页、旧数据根和启动入口，再验证端口与 API。具体恢复动作须随切换方案审阅；本次只读核对没有执行恢复。

旧数据根用于恢复旧服务，不作为新平台的正式数据。

### 其他恢复情况

| 情况 | 处理方式 |
|---|---|
| 普通常规部署未完成 | 读取已记录 intent，用其精确目标继续，或用其精确前版回退；首次 initialize 中断不能套常规 recover |
| 网页已有完整前版 | 沿用原 `--rollback`，然后核 API 与网页 |
| 可信代码需要撤回 | 原 `runtime-code rotate` 使用更高序号和外部 CAS，再预览、安装；不能降低序号 |
| 已完成代码发布需要撤回 | 建立 revert 发行版并向前发布；不能用旧 SHA 冒充快进目标 |

```bash
# 已持久化常规 intent 的精确值；首次 initialize 中断不能改走 recover
INSTALL_RECORDED_TARGET='pending'
INSTALL_RECORDED_PREVIOUS_SHA='pending'
# 两条是不同恢复决策，由 Root 选一条，不顺次运行
bash /home/lighthouse/rquant/scripts/deploy-production.sh --recover-generation --recovery-action resume --target "$INSTALL_RECORDED_TARGET"
bash /home/lighthouse/rquant/scripts/deploy-production.sh --recover-generation --recovery-action rollback --target "$INSTALL_RECORDED_PREVIOUS_SHA"

# 仅已有受验 previous 的网页回退
bash /home/lighthouse/rquant-web/current/scripts/web-release.sh --rollback --dry-run
bash /home/lighthouse/rquant-web/current/scripts/web-release.sh --rollback
bash /home/lighthouse/rquant-web/current/scripts/web-release.sh --status
```

恢复成功须有一致的标记、完成记录、提交与环境清单，原服务状态和正式网页证据。恢复不完整时保留失败资料，不补写成功记录。

## 6. 证据与剩余工作

原离线准备核对了发布脚本、安装接口、服务模板、恢复入口和 API07 冻结配置。三段 bash 草案只通过语法检查，没有执行安装功能。原件和 34 份直接来源保存在：

`data/verification/production-installation-readiness-20261007/health-preparation-01/`

本次两次实际只读主机结果分别保存在 `root-host-readonly-02/` 和 `root-host-readonly-03/`。文档修改前版本与检查结果保存在 `root-installation-update-04/`。三个目录都位于上述证据根。

**R13 的离线准备已完成；精确发行版、正式来源、安装授权和实际安装仍待完成。** 后续按同一清单填写，不另开一轮规划。
