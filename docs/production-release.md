# rQuant 受控自动发布

## 目标

日常代码发布由 Codex 完成 PR 合并、版本 tag、腾讯云部署和验证，用户不需要登录服务器。
自动化不是任意生产权限：部署器只能部署 `origin/main` 中的精确 SemVer tag 或完整 commit，
只能重启固定的 rQuant 服务，不能修改 systemd/nginx、写生产数据库或执行任意 sudo。

## 发布链路

1. PR 必须可合并，Python 3.11/3.12 CI 全绿。
2. Codex squash merge PR，删除远端功能分支。
3. 在合并 commit 创建并推送 annotated SemVer tag。
4. 腾讯云执行：

   ```bash
   ROOT=/home/lighthouse/rquant
   cd "${ROOT}"
   export RQUANT_RUNTIME_PRODUCTION_INPUTS="${ROOT}/data/runtime-production-inputs.json"
   export RQUANT_RUNTIME_PROFILE_OUTPUT_DIR="${ROOT}/data/runtime-profiles"
   export RQUANT_RUNTIME_ROOT="${ROOT}/data/runtime"
   bash scripts/deploy-production.sh --target v0.13.2
   ```

   这三个绝对路径必须由受控部署环境预置并保持稳定；Linux 缺任一项即在 bootstrap 前失败关闭。
   操作人员不传 Job Center 的 SQLite、command spool、artifact、Definition/Experiment/Catalog 路径，
   也不传 profile id 或 generation hash。

5. 纯标准库 bootstrap 在创建或取得 generation/handoff lock 前先只读核对 installation state 及其
   绑定的 prepared runtime sentinel。macOS installed 发布还会先只读解析已有 handoff record 并
   执行交易时间门禁；缺失/不符时零写失败，incomplete handoff 即使是 dry-run 也在窗口内直接
   返回 75，既不 fetch，也不改 `FETCH_HEAD`、refs 或 lock namespace。窗口外才有界 fetch 并解析精确 target，
   随后取得稳定 handoff lock，并在 A 仍 loaded 时持共享 generation lock 构建、封存、preflight B 的
   exact-SHA 不可变代码/环境候选，同时从 B 候选渲染并验证三份 generation-bound plist。只有 B
   候选完整可用后，才在第一次 `bootout` 前原子写入 typed prepared deployment intent，其中固定
   previous/target/ref、完整 change plan、当前 marker/environment generation、handoff operation 与
   installation identity；再停止原先 loaded 的三个 Lab launchd daemon，确认其 shared generation
   lock 已释放后取得独占锁；Linux 无此本地 launchd 步骤。B 预备失败时 A 保持原样。之后验证当前已提交代际，
   才导入项目
   deployer。bootstrap 与 deployer 的所有 Git 子命令都固定使用已验证的绝对
   `RQUANT_TRUSTED_GIT_PATH`，不读取 `PATH` 中的 `git`；所有只读核对显式设置
   `GIT_OPTIONAL_LOCKS=0`。部署器依次执行：tracked 工作区检查、target/main 归属与快进检查、
   diff 风险分类、接管 bootstrap 已验证的 prepared intent（不再 fetch、重算 diff 或重建 plan）、
   快照实际 active 的受影响服务及 timer、使旧 marker 失效、暂停原先 active 的相关 timer、
   `git merge --ff-only <exact-sha>`、用物理绑定的 uv 执行 frozen sync、发布并 rollout 精确 SHA 的
   production deployment profile。随后部署器调用 `rquant lab-runtime-prepare`：它从当前 profile 与
   install receipt 重读并逐项核对 code SHA、profile id、generation hash、runtime root 以及
   Lab Jobs/Definition/Experiment/Dataset/Catalog 四类 authority 路径，原子发布并安装
   `research/job-center-authority.json`。这一步完成后才运行第一次 preflight、按 intent 的精确集合
   重启服务、第二次 preflight、恢复原先 active 的 timer。最后由 target checkout 的隔离 stdlib
   bootstrap 重新加载 target authority。它在 operation id + commit 唯一命名的 staging 目录中
   直接构建 owner-only 不可变环境，把 console script 中精确指向 staging/source interpreter 的
   shebang 重绑到最终 generation 物理解释器，确认产物不再含 staging/source 路径后再冻结目录、
   生成全量内容 manifest 并原子切换环境 selector，然后
   发布绑定 operation id、target 和环境 manifest 的 marker。旧 coordinator 再把 intent 推进为
   `completed`，最后由 target authority 原子发布 commit record。daemon 只接受
   `marker + completed intent + commit record + selected environment manifest` 完整一致的代际；
   旧 coordinator 不能替新版本 marker schema 写标记。uv 等待以短轮询响应整体 deadline 或取消，
   超时/取消会终止完整进程组；manifest 序列化、哈希、写入/fsync、selector rename/目录 fsync
   与 GC manifest 扫描均在有界分块或持久化边界 checkpoint。目录 fsync 后发生的取消不会返回
   成功，已经完整落盘的 manifest/selector 则作为可验证的重放状态保留。每个 durable stage 同时写入 intent
   时间线和 JSONL 审计。结束时先释放独占锁，再只恢复原先 loaded 的 Lab daemon，并验证
   launchd health 和 shared lock；恢复失败会使发布返回非零。dry-run 只输出 handoff 计划并持
   shared lock，不 bootout daemon。
   遗留在任一 stage 的 `action=deploy` handoff 由 resume/rollback 以新的 superseding operation
   接管；旧 operation id、target/ref、profile、lifecycle 与 installation identity 必须和 deployment
   intent 完全一致。每次 `launchctl print` 都按 command timeout 与当前整体/readiness 剩余时间的
   较小值执行，剩余预算为零时不再发起命令。
   若进程在 typed prepared intent 已落盘、deployer 尚未接管时失败，cleanup 只在核对 prepared
   target、原始 handoff operation、labels 与 previous generation 后恢复旧 daemon，并把原 operation
   记为 `aborted`；它绝不写 completed proof。若崩溃发生在首个 handoff record 前，仅存的
   `.intent.prepared.json` 仍是正式恢复入口：显式 resume/rollback 会在 handoff 锁内幂等物化原始
   `deploy/planned` 根记录，再由 deployer 原子晋升 intent。所有 authority JSON 都拒绝重复键。
   接管前还必须确认旧 operation id 正是 intent 当前记录的 `handoff_operation_id`；bootstrap 在仍持
   handoff 锁且首个 bootout 之前验证 exact successor 与完整物理链，原子 rebind intent 并重读确认，
   deployer 只消费该持久 binding。相反 action 重试因此会形成 A→B→C，而不会把历史压成 A→C。
   完成 handoff 的 proof、operation record 与 stable record 若因
   崩溃只写入一部分，下次发布会在锁内验证全 binding 后幂等补齐；不一致 proof 一律阻断。显式
   resume/rollback 自身的 readiness 失败时，rollback 会沿已验证的 supersede 链停止 target daemon、
   恢复 previous generation 并重新验收旧 daemon。
   completed proof 还必须把 generation operation、environment generation 与 code SHA 逐项交叉绑定
   到 typed deployment intent、当前 marker、environment selector 和 commit record。bootstrap 与
   deployer 使用 release authority 中同一份 changed-files、service/timer、generation 与 stage-history
   policy；任何损坏、越权或自相矛盾的 intent 都会在首个 launchd mutation 之前失败关闭。
   supersede 链以 intent 永久保存的初始 handoff operation 为根；每次 rebound 的 previous id 必须
   与上一跳完全相等，物理 operation 链不得隐藏或遗漏 rebound 节点。proof、按 operation 命名的
   记录和 stable active 必须内容一致；provisional 新 active 仅可引用 supersede 链内经验证的旧
   completed proof，无合法链的 operation 错配会被 daemon 拒绝。
   completed handoff proof 已落盘但事务仍为 `awaiting_readiness`，或 intent 已 completed 但 commit
   record 尚未落盘时，显式 `resume` 会幂等继续 readiness finalizer/commit；同 SHA 或空 diff 若仍有
   incomplete handoff，则返回结构化 `recovery_required`，不会误报 `already_current`。
6. 更新依赖、preflight 或服务健康检查失败时，自动 `git reset --hard` 回 intent 记录的
   previous commit、恢复锁定依赖并按同一服务/timer 合同切回。只有旧 checkout、旧依赖、
   精确服务集合、第二次 preflight 与 timer 原状态全部恢复后，才由 previous checkout 的隔离
   authority 为 previous commit 构建或选择已验证的不可变环境，并完成相同三记录提交协议；
   回滚不完整时不会产生可接受代际，intent 保持可恢复。

## 自动拒绝

- target 是 `main`、`origin/main`、短 SHA 或包含 shell 字符，而不是 SemVer tag/完整 SHA。
- target 不属于 `origin/main`，或不是当前生产 commit 的快进后继。
- tracked 工作区存在未提交改动；`backup/` 等 untracked 文件不阻断。
- diff 包含 `deploy/systemd/`、`deploy/nginx/`、`deploy/frp/`、`deploy/sudoers/`。
- 工作日 09:15-15:10 的发布需要重启任何长驻服务。
- 另一个部署/交接进程已持有稳定 handoff lock；工作日 09:15-15:10 的 macOS Lab daemon
  bootout/bootstrap 交接一律拒绝。Lab daemon 的 shared generation lock 由受控交接在窗口外
  有界释放，不再要求人工停止 KeepAlive。
- generation marker、completed intent、commit record、环境 selector/manifest 任一缺失、格式错误，
  或与 Git SHA、`uv.lock`、包版本、Python ABI、不可变 venv/解释器/site-packages 内容不一致。
- `sudo -n`、依赖同步、preflight 或服务健康检查失败。

部署器没有 `--force` / `--emergency` 绕过参数。高风险基础设施和生产数据操作必须另开
受控变更，并取得用户明确授权。

## v0.29.0 发布门

`v0.29.0` 的版本元数据整理不表示已部署。创建 tag 或调用生产部署器前，隔离运行时还必须保留
以下验收证据：Linux CI 目标平台验证、云端 systemd 的原样解析与资源限制检查、以及新旧实时链路
在真实交易日的 shadow 对账。三项完成前，旧 `monitor`/`surge-watch` 链路继续保留，不能以本地
专项或全量归因结果替代生产验收。实施映射与退休门见
[工作负载解耦设计](architecture/2026-07-22-workload-isolation-design.md)。

## R07 Release A / Release B 两阶段部署门

从 `v0.30.0` 起，受控部署器在**任何** checkout、merge、`_stop_timers` 或 systemctl 之前，
先解析 installed 与 target 两侧的 `tests/fixtures/r07_differential_gate/policy-v1.json`
（从 Git object 读，不读工作树），再按下表决定放行还是拒绝。gate 本身跑在隔离子进程里用
release 解释器执行，因为部署器自己在 `-I -S` 下无法 import policy 模型；子进程的任何失败都是
拒绝加审计，不是降级。

| installed policy | target policy | 决定 |
|---|---|---|
| absent（pre-R07 checkout） | `disabled_for_bootstrap` | 允许一次，即 **Release A** 安装路径；audit 记 `r07_gate=bootstrap_disabled` 与精确 target commit/tree；不下载也不校验 evidence |
| absent | `enforced` | 拒绝：没有可声明的 predecessor |
| absent | absent | 拒绝：部署器存在即说明 R07 已进入链路，不得退回 pre-R07 |
| `disabled_for_bootstrap` | `disabled_for_bootstrap`（含同 commit） | 拒绝：装过 A 之后下一个目标只能是 B |
| `disabled_for_bootstrap` | absent | 拒绝 |
| `disabled_for_bootstrap` | `enforced` 且 predecessor 精确等于 installed 的 (commit, tree)，且 evidence 通过 | 允许，即 **Release B** |
| `disabled_for_bootstrap` | `enforced` 但 predecessor 不符 | 拒绝 |
| `enforced` | `enforced` 且 evidence 通过（前滚与回滚同此） | 允许 |
| `enforced` | `disabled_for_bootstrap` 或 absent | 拒绝 |

`_recover_locked` **不重跑上表**（Amended per Codex round-2 order 2026-08-25, ruling 8）。
自动 recovery 从不挑选 target：它只重放此前已被 R07 门接受并持久化的那一对精确 intent
（`resume` 走记录的 target、`rollback` 走记录的 previous），因此 audit 记
`r07_gate=recorded_intent`，并额外写入 recovery provenance——`recovery_action`、
`recovery_intent_operation_id`、`recovery_intent_stage`、`recovery_intent_target_ref`
以及该 intent 记录的 `previous_sha` / `target_sha`——让「跳过了决策表」这件事本身可审计。
任何不在记录里的 pair 一律拒绝。任何显式 `--target`（含回滚之后的 retry）仍走完整决策表。

**Release A（本阶段）**：policy 的 `deployment_mode=disabled_for_bootstrap`、
`bootstrap_predecessor=null`。它只把 R07 决策链路装进生产，不消费任何 CI 证据，因此
不需要 `RQUANT_GITHUB_EVIDENCE_TOKEN`。

**Release B（下一次部署）**：policy 必须是 `enforced`，且 `bootstrap_predecessor` 精确声明
Release A 的 commit 与 tree SHA。部署器会按 `evidence_channel` 声明的仓库、workflow、三个
job 与 artifact 内部路径，向 GitHub 取该 target commit 的 `r07-dr-gate/evidence-v1.json`，
用完整的私有 verifier 校验两次——写 cache 之前一次，读 cache 之后一次——然后才放行。证据缓存
固定在云服务器 82.156.0.68（lighthouse 用户）的 `/home/lighthouse/rquant/var/r07-dr-evidence`；
Linux 生产 profile 下部署器会要求这个目录精确等于 policy 声明的 `cache_path`。
服务器缺 token 或无网络时结果是 **blocked**，不是降级放行。

合版方式对这条链路是硬约束：R07 冻结 baseline 必须留在 `origin/main` 的祖先链上，所以带 R07
policy 的 PR **只能用 "Create a merge commit"**；squash 或 rebase 会让 main 上任何 commit 永远
过不了 ancestry 检查，Release B 也就永远拿不到证据。

## 一次性安装

首次需要恢复可用的受控 SSH，并由 root 安装最小 sudoers 白名单：

```bash
cd /home/lighthouse/rquant
sudo visudo -cf deploy/sudoers/rquant-production-deploy
sudo install -o root -g root -m 0440 \
  deploy/sudoers/rquant-production-deploy \
  /etc/sudoers.d/rquant-production-deploy
sudo visudo -cf /etc/sudoers.d/rquant-production-deploy
sudo -n -l /usr/bin/systemctl restart rquant-dashboard.service
sudo -n -l /usr/bin/systemctl stop rquant-monitor.timer
```

最后两条只检查白名单授权，不会重启服务或停止 timer。正式安装后，Codex 仅通过
`scripts/deploy-production.sh --target <exact-ref>` 部署。

P1.5d 首次安装 Lab launchd 前还需在主 checkout 建立自有物理 `.venv`。安装状态不从仓库中存在
plist 推断，必须按下面四个阶段显式完成。首先在目标 checkout 创建第一个 marker：

```bash
bash scripts/deploy-production.sh \
  --initialize-generation \
  --target <exact-semver-tag-or-full-sha>
```

该模式持有同一独占锁，要求 main/HEAD 精确等于 target、target 属于本地
`origin/main`、tracked checkout 干净，并逐字节验证目标 commit 的 `uv.lock` 与
`pyproject.toml`。随后运行物理 uv 的 `sync --frozen`、复验包版本/Python ABI/物理 venv，执行
target preflight，随后构建不可变环境并按 marker、completed sentinel、commit record 的顺序提交。
第一次执行会在任何依赖或 marker mutation 前创建并 fsync
一次性 `rquant.initialized.json` sentinel；中断只能以同一 target 续跑。sentinel 完成后，即使
删除 marker，`--initialize-generation` 也会拒绝重放，不能把初始化当作恢复开关。不得手写
marker/sentinel/commit/selector；任一步中断时 daemon 都会失败关闭。若中断发生在 sentinel 已
完成、commit record 尚未发布的窄窗口，重复同一 target 的 initialize 只允许核验并补齐 commit
record；完整初始化仍拒绝重放。

初始化事务与普通部署 intent 是两套 lifecycle。初始化中断后，唯一受支持的续跑命令是原样重复
同一精确 target 的初始化命令：

```bash
bash scripts/deploy-production.sh \
  --initialize-generation \
  --target <the-same-recorded-exact-target>
```

此时不得改用 `--recover-generation`；后者只恢复已经持久化 `rquant.intent.json` 的常规部署。
初始化 sentinel 已完成且 commit record 完整时，上述命令会明确拒绝重放，而不是重新祝福当前状态。

第二步，在 marker 已可验证但 launchd 尚未安装时，通过只读 stdlib wrapper 创建专用私有 runtime
根并迁移旧 Lab 状态。默认目标是 `DATA_DIR/lab-runtime`，根和目录均为 `0700`，文件为 `0600`；
共享的 `DATA_DIR` 可以保持现有 `0755`，命令不会 chmod 它。若旧 SQLite 仍存在
`-wal`/`-shm`/`-journal`，
迁移会失败关闭，必须先在旧服务停机状态完成 SQLite checkpoint，再重新运行：

```bash
ROOT=/Users/roxor/brain/30-projects/rQuant
LOCK=/Users/roxor/brain/30-projects/.rquant-deploy/rQuant.lock
export RQUANT_RUNTIME_ROOT="${ROOT}/data/runtime"
export LAB_RUNTIME_DIR="${RQUANT_RUNTIME_ROOT}/research"
export LAB_JOBS_PATH="${LAB_RUNTIME_DIR}/lab_jobs.sqlite3"
export LAB_JOB_COMMAND_DIR="${LAB_RUNTIME_DIR}/commands"
export LAB_FINAL_ARTIFACT_DIR="${LAB_RUNTIME_DIR}/final-artifacts"
"${ROOT}/.venv/bin/python" -I -S "${ROOT}/scripts/run-lab-daemon.py" \
  --expected-checkout-root "${ROOT}" \
  --trusted-git-path /usr/bin/git \
  --deployment-lock-path "${LOCK}" \
  -- "${ROOT}/.venv/bin/rquant" lab-runtime-prepare
```

`RQUANT_RUNTIME_ROOT` 是已经由 `runtime-deployment-profile --apply` 安装 current profile/receipt 的
受控根。wrapper 只从该环境绑定 `--runtime-deployment-root`；CLI 再从 current profile 解析
Definition Registry、Experiment Registry、Lab Jobs、command spool、final artifact、dataset 与
catalog authority，调用方不能覆盖。profile 缺失、过期、SHA/代际不匹配时零 Job/SQLite 写入；
candidate 发布或 current 安装失败会保留/恢复原 current，不留下可加载的半安装文件。

准备命令最后以原子 `0600` 的 `lab-runtime/.prepared.json` 固化稳定 runtime authority id、
checkout 路径、runtime 根身份、全部托管目录/文件和每个 legacy 迁移来源；执行时的 release
commit 仅作为 `prepared_by_commit` 审计信息，不参与后续 A→B daemon 准入。首次安装可以把尚未
创建的 `lab_jobs.sqlite3` 明确记录为 uninitialized，但只有 scheduler 能在持有私有 authority 锁时
原子创建数据库并把 inode 登记回 sentinel；worker/finalizer 在登记完成前失败关闭。数据库登记后
若被删除或替换，所有 daemon 都会拒绝启动。旧库在迁移后重新出现、sentinel 被篡改、路径身份
漂移或热 sidecar 出现同样会失败关闭，避免新旧 SQLite 静默分叉。

第三步，显式登记已准备的 runtime/readiness 根；这会生成稳定、owner-only 的 installation state，
后续 installed 模式发布必须验证它，不能仅靠 plist 文件存在：

```bash
bash scripts/deploy-production.sh \
  --register-lab-installation \
  --lab-runtime-root "${ROOT}/data/lab-runtime" \
  --lab-readiness-root "${ROOT}/data/lab-runtime/readiness" \
  --target <same-exact-semver-tag-or-full-sha>
```

第四步才由 P1.5d 的人工基础设施步骤调用已在 P1.5b 实现并测试的 generation-bound 安装器：

```bash
"<active-generation>/bin/rquant" lab-launchd-install \
  --expected-checkout-root "${ROOT}" \
  --trusted-git-path /usr/bin/git \
  --deployment-lock-path "${LOCK}" \
  --launch-agents-dir "${HOME}/Library/LaunchAgents" \
  --worker-id rquant-mac-primary
```

恢复/卸载仅允许针对本安装状态精确绑定且未被人工修改的 plist：

```bash
"<active-generation>/bin/rquant" lab-launchd-uninstall \
  --expected-checkout-root "${ROOT}" \
  --trusted-git-path /usr/bin/git \
  --deployment-lock-path "${LOCK}" \
  --launch-agents-dir "${HOME}/Library/LaunchAgents"
```

安装器会从 active immutable generation 内的模板原子 materialize 三份 `0600` plist，运行
`plistlib`/`plutil` 校验。它先以独立 installation/handoff transaction lock 和已登记 installation
identity 串行化，bootout 原 loaded label 并确认其 generation shared lock 已释放，之后才取得
generation exclusive lock。每个 plist 与 local/registered state 的原 inode 都通过同文件系统
quarantine rename 和 fsync journal 保护；任一写入、bootstrap、kickstart 或 bootout 失败都会恢复
原 bytes/inode、两份状态与精确 loaded 集合。未登记的同名 plist 即使内容合法、权限为 `0600` 也
视为 foreign file，绝不覆盖。quarantine、temp publish 和 rollback restore 都使用父目录 FD 下的
原子 no-clobber rename；目标或备份竞态出现 foreign occupant 时保留证据并失败关闭。复跑相同
generation 幂等，成功卸载则同时移除 local 与 registered
installation authority，重新安装前必须重新登记。P1.5b 只交付并测试了该能力，**尚未在本机安装或加载**；P1.5d 才执行上述命令并做
真实 launchd readiness/rollback 演练。初始化和登记模式不要求
launchd 已安装或 loaded；常规 `macos-lab + installed` 发布则反过来强制 installation state 与三个
label 都存在。installed dry-run 同样逐个只读核对三个 label 已 loaded 以及 installation、runtime、
prepared sentinel、plist 和 generation 前置条件，但不会 bootout。该区分避免首次安装陷入“必须
先停一个尚未安装的 daemon”的循环依赖。

## 中断恢复

常规部署在任何 mutation 前已原子写入
`/home/lighthouse/.rquant-deploy/rquant.intent.json`。硬中断后，正常 deploy 模式不会猜测当前
checkout；只能读取该 intent 并选择 resume 或 rollback。命令中的 target 只是对 intent 的再次
确认，不能覆盖 intent：

```bash
# 继续 intent 已记录的 target；可使用原始精确 tag 或 target full SHA
bash scripts/deploy-production.sh \
  --recover-generation --recovery-action resume \
  --target <recorded-target-tag-or-full-sha>

# 恢复 intent 已记录的 previous，必须使用 previous full SHA
bash scripts/deploy-production.sh \
  --recover-generation --recovery-action rollback \
  --target <recorded-previous-full-sha>
```

恢复模式不重新 fetch、不重新解析移动后的 `origin/main`，只接受 intent 内的 previous/target、
changed files、service plan、当时 active 的服务和 timer。resume 使用可信 Git 精确 fast-forward，
rollback 精确 hard reset；随后两者都重新执行 frozen sync、第一次 preflight、原计划服务切换、
第二次 preflight 和 timer 恢复，并由最终 checkout 自己的隔离 authority 完成不可变环境与三记录
提交。每次恢复先 fsync `recovery_started` 和审计，再使旧 marker/commit 失效；在此之前不得
stop/start timer、切换 checkout、同步依赖、运行 preflight 或重启服务。工作日
09:15-15:10 只要原计划包含服务切换，resume 与 rollback 都返回 75 延期，不允许借恢复绕过。
缺少 intent、operation id 不符、当前 HEAD 不在 previous/target 或 plan 漂移时一律拒绝。
sync、partial restart、post-preflight、timer 恢复、环境封存、marker/commit 发布中断后可原样重跑
同一动作；
不得改用新 ref，也不得删除 intent 后运行 initialize。

## 不可变环境保留与磁盘预算

每次构建新环境前会在 generation 独占锁内执行 GC。默认宽限期 7 天、发布完成后至少保留
2 GiB 可用空间，可通过 `RQUANT_RELEASE_GENERATION_GC_GRACE_SECONDS` 与
`RQUANT_RELEASE_GENERATION_MIN_FREE_BYTES` 调整。current 与精确持久化的
`previous_generation_id`、active intent 的 previous/target，以及 marker/commit 引用永不作为孤儿
删除；保留关系不使用 mtime 推断，因此时钟回拨或更新的 orphan 不会改变回滚代际。其他严格受控的旧完成/失败目录
才会解冻删除。扫描、删除数、回收字节、前后磁盘与保留集合写入 owner-only
`<lock-stem>.generation-gc.jsonl`。磁盘预算不足时发布在复制前失败，不会留下完整 staging。
统一引用收集器还会枚举每一份严格命名的 completed deployment intent archive；archive 必须是
descriptor-bound、非 symlink、canonical authority，任一未知文件名或损坏 archive 都让 GC 在删除前
失败关闭，不能只保留当前两个硬编码 intent 路径。

不可变 venv 由物理绑定的 uv 在新的 generation 目录内执行 `uv venv --relocatable` 与
`uv sync --frozen --active`，不复制现用的几百 MB 环境。`RQUANT_DEPLOY_UV` 可指定绝对路径；
留空时仅探测 `/opt/homebrew/bin/uv`、`/usr/local/bin/uv` 和 `~/.local/bin/uv`。Homebrew symlink
链会被解析到物理 target，并把路径、owner、mode、inode 与内容 hash 写入 generation manifest；
不使用 `PATH` 中的 uv。允许的 symlink 仅限 uv 的 `bin/python*` 与 `lib64` 结构：
Python 链必须最终绑定 marker 中已校验的 system interpreter，其他相对链接必须留在同一 generation；
任意额外链接、越界链接或解释器身份漂移均失败关闭。冻结前会重写精确指向 staging/source
`bin/python*` 的 console-script shebang，并扫描确认最终环境不再含临时或源 venv 路径。

## Linux 与 macOS 发布 profile

发布脚本按宿主显式选择 `linux-production` 或 `macos-lab`，profile 与平台不匹配时拒绝运行。
Linux profile 保持既有 systemd service/timer 计划；macOS profile 不运行任何 `systemctl`。
每次精确 SHA 变化都会先安装同 SHA 的 deployment profile，再重新发布同 SHA/profile generation
绑定的 Job Center current manifest；旧 SHA manifest 不能被新 scheduler 自动加载。prepare 失败时
部署事务在任何 target scheduler/daemon restart 前进入 rollback，previous checkout 会按 previous
profile 重新发布 previous manifest。若 previous manifest 已存在，失败恢复保持其原 bytes；首次安装
失败则 current 仍不存在，只有不可加载的 candidate 会被清理。
由于 Lab runtime guard 绑定精确 checkout SHA，macOS 上任何 commit 迁移都必须交接 scheduler、
worker、finalizer，而不按文件后缀猜测“这次改动大概无关”。交接在交易保护窗口外确认三个 label
均已 loaded，各执行一次 bootout，部署完成后各 bootstrap 一次，不用重启循环掩盖故障。

`deploy/launchd/*.plist` 属于受控基础设施，不进入普通代码发布：模板内容变化会像 systemd、nginx、
sudoers 一样 fail closed，并要求独立人工验收/安装。模板不变的普通 A→B 代码发布仍必须把 plist
中的 Python、code root、launcher 与 commit 从 A 原子重绑到 B；handoff 在 bootstrap B 前使用安装
事务 journal 重新物化三份 generation-bound plist，并同步 local/registered installation state，失败
则精确恢复 A 的文件 inode、状态和 loaded 集合，不要求人工再次调用 installer。基础设施模板安装或
更新后，必须重新运行
`--register-lab-installation` 持久化新的文件 hash 与 inode；对完全相同的安装重复登记保持原文件
inode/bytes 不变，避免使既有 completed proof 失效。若已经存在 deployment handoff authority，plist、
runtime root 或 installation identity 的变更会要求单独受控迁移，不允许普通 re-registration 覆盖；
尚未产生 deployment handoff 的首次安装基线可归档旧 descriptor 后更新。Linux profile 的 systemd
规则保持不变。

交接本身也有独立的 `0600` 持久事务记录：在第一次 bootout 前 fsync operation id、已解析验证的
exact target/ref、action、release profile、lifecycle、installation identity、原 loaded label、
已停止/已恢复集合与阶段。未完成 operation 不可换 target 重入。崩溃恢复以 launchctl 当前状态和
该记录共同判断，允许“只恢复了一部分”的合法中间态；仍 loaded 的 label 会重新停下，最终只恢复
原集合。generation marker 在 handoff
记录尚未 completed 时仅允许记录内某个 label 以 provisional 身份启动并发布 readiness，普通 daemon
验收仍失败关闭；三个 label 全部恢复并通过稳定窗口后，handoff 才 completed，并发布按 handoff
operation id 命名且绑定 marker operation、environment generation 与 code SHA 的不可变证明。下一次
发布可以更新活动 handoff 记录，但不能覆盖当前 generation 的 completed 证明。

每个 Lab daemon 在持有同一 generation shared lock 后，以 `0600` 原子文件发布 label、PID、
operation id、environment generation id、code SHA、启动时间和单调心跳。handoff 验收要求三个
label 独立匹配 launchctl PID 和新 marker，并在稳定窗口内由同一 PID 推进心跳；任一 label 缺失、
代际错误、重启抖动或 shared lock 未保持都会自动停止 target daemon、以独立恢复预算回滚到 intent
记录的 previous generation，再恢复并验收旧 daemon；completed 证明只在最终稳定验收后发布。
`RQUANT_DEPLOY_COMMAND_TIMEOUT_SECONDS` 限制单次 Git/uv/preflight/launchctl 子命令，
`RQUANT_DEPLOY_OVERALL_TIMEOUT_SECONDS` 限制整个发布、handoff、失败恢复和锁重取；所有路径继承
同一个绝对 deadline，预算耗尽后不再执行恢复副作用，只保留可继续的持久 authority。阻塞命令统一
由有界进程树收容器运行；root 在实际命令启动前通过闸门完成 PID identity 和内核 tracker 注册。
Darwin 使用 `EVFILT_PROC/NOTE_FORK/NOTE_EXIT`、XNU `PROC_PIDUNIQIDENTIFIERINFO` 的进程/出生父
唯一 ID，以及继承的 stdout/stderr 管道 identity；管道 identity 让中间父进程已经退出并发生 reparent
的后代仍可被同用户进程清单识别。macOS 自 10.5 起不支持 `NOTE_TRACK/NOTE_CHILD`，所以这里是针对
rQuant 可信发布命令的生命周期收容，不是允许执行对抗代码的安全沙箱：主动关闭全部继承管道、移除
token 再脱离的程序不在证明范围。Linux 使用 child subreaper、`/proc` start identity 与可用的 pidfd。
parent graph 和每次运行的唯一 token 是补充证据。在
SIGINT/SIGTERM/timeout/BaseException 时先停止生成源，再反复终止并复核原进程组、立即脱离的
`setsid` 后代与 cleanup 期间新 fork；任一 tracker、inventory、PID identity 或已发现后代存活检查无法
完成时失败关闭，不释放成功权威。连续信号由一次性 latch 合并，避免 cleanup 被嵌套信号异常打断。
signal authority 会一直持有到最终 inventory、必要后代回收、returncode/结果校验以及 Darwin 管道
anchor、tracker 和闸门资源全部关闭；原 handler 恢复、首信号 replay 与原 mask 恢复在同一个仲裁
操作内完成，unmask 边界的后续 handler 异常只能作为首信号的次要 cleanup 证据。
POSIX 标准 SIGINT/SIGTERM 在阻塞窗口内只形成 pending set：相同信号会合并，同时 pending 的不同
标准信号不保留真实到达顺序。仲裁器优先保留进入阻塞窗口前已经 latch 的首信号；若只能观察到
同时 pending 的集合，则采用确定性的 SIGINT、SIGTERM 顺序选取一个重放，其余仅作为已合并的后续
中断，不宣称还原真实先后。Darwin tracker 在线程退出后才关闭 kqueue，Linux subreaper 恢复失败
也会作为发布失败上报。

所有持久 release/deployment/installation/handoff/runtime/protocol JSON 共用同一 canonical UTF-8
编码：`ensure_ascii=false`、排序键、紧凑分隔符、禁止 NaN。release、deployment、installation、
handoff 和 runtime authority 文件的字节合约是末尾恰好一个 LF；单记录 Strategy Lab spool/model 文件
无尾随换行，JSONL 则每条记录一个 LF。reader 按各自文件类型比较 exact bytes，同时拒绝任意层重复键；
旧 ASCII 转义、pretty JSON 或错误换行不会被静默接受，需显式迁移。

以下非秘钥部署控制项可以放在 repo `.env`：`RQUANT_DEPLOY_UV`、单命令/整体 timeout、
generation GC 宽限期/最小剩余磁盘、`RQUANT_RELEASE_PROFILE`、`RQUANT_LAB_LIFECYCLE_MODE` 与
`LAB_TRUSTED_GIT_PATH`。stdlib bootstrap 只读取这份 allowlist，不 source/eval 文件，也不读取或
打印 Tushare、通知等秘钥；显式进程环境/命令参数优先于 `.env`。任何部署命名空间内的未知拼写、
缺少 `=`、非法值或重复控制项都失败关闭，普通非部署环境项被忽略。`.env` 必须是当前用户所有的
真实单链接 `0600` 文件，否则发布失败关闭。

## 预演与审计

预演会 fetch 和计算计划，但不 checkout、不更新依赖、不重启服务：

```bash
bash scripts/deploy-production.sh --target v0.13.2 --dry-run
```

退出码：`0` 成功/无需更新，`2` 策略拒绝，`75` 交易时段延期，`1` 部署或回滚失败。
审计记录位于 `/home/lighthouse/rquant/logs/production-deploy.jsonl`。
marker 位于 `/home/lighthouse/.rquant-deploy/rquant.complete.json`；活动事务、首次初始化 sentinel、
commit record 和环境 selector 分别位于同目录的 `rquant.intent.json`、`rquant.initialized.json`、
`rquant.commit.json`、`rquant.environment.json`。不可变 venv 位于 `rquant.venvs/<generation-id>`，
对应 manifest 为 `rquant.venv-<generation-id>.manifest.json`。控制记录均为 owner-only `0600` 原子
文件，环境根为 `0700`，已发布 generation 为只读/可执行 owner-only。marker 由最终 checkout
authority 以 `0600`
临时文件循环处理 short write，文件 `fsync` 后重新读取、解析并核对内容 hash，再原子 rename 和
目录 `fsync` 发布。它不是人工恢复开关；故障后只能运行上面的精确 initialize/resume/rollback
流程，而不是复制、修改或删除 JSON。每个 intent 的 immutable plan、stage history 和操作结果还会
写入 `logs/production-deploy.jsonl`；完成 intent 在下一次发布开始前按 operation id 归档。
macOS 还使用同一稳定私有根中的 `rquant.lab-install.json`、活动
`rquant.lab-handoff.json`、每个未完成 operation 的
`rquant.lab-handoff.<operation-id>.json` 与每次完成后的
`rquant.lab-handoff.<operation-id>.completed.json`，分别绑定显式安装状态、可恢复 launchd 交接和
当前 generation 的不可变交接证明；它们都不是人工补写的开关。

## 中断恢复决策

1. 先读取审计与 `rquant.intent.json`，确认 operation id、previous/target、stage 和服务/timer 计划；
   不从当前 `origin/main` 猜目标。
2. 交易保护窗口内只做只读诊断，任何包含服务重启的 resume/rollback 都等待 15:10 后。
3. 目标版本确认可继续时执行 recorded target 的 resume；需要撤回时执行 recorded previous 的
   rollback。两者都必须走 `scripts/deploy-production.sh`，不能手工 reset 后补 marker。
4. 成功标准是 intent=`completed`，commit record 精确绑定 marker、intent 和 selected environment
   manifest，marker commit/schema 与最终 checkout 一致，两次 preflight 通过，intent 中 active
   services 健康且 active timers 已恢复。Daily receipt signer 以
   `rquant-daily-receipt-signer.socket` 的 `active/waiting` 为健康信号；root signer service 在首次
   请求前 `inactive/dead` 是正常的 socket-activation 状态。任一项缺失都仍是未完成事务。

## 旧脚本边界

`scripts/deploy.sh` 保留给 systemd unit 等人工基础设施部署。它会执行交互式 sudo，且不具备
精确 target、交易时段保护和自动回滚，因此不得用于 Codex 无人值守发布。
