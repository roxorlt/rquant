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
   cd /home/lighthouse/rquant
   bash scripts/deploy-production.sh --target v0.13.2
   ```

5. 纯标准库 bootstrap 先取得稳定 generation 独占锁并验证当前已提交代际，之后才导入项目
   deployer。bootstrap 与 deployer 的所有 Git 子命令都固定使用已验证的绝对
   `RQUANT_TRUSTED_GIT_PATH`，不读取 `PATH` 中的 `git`。部署器依次执行：tracked 工作区检查、
   `git fetch`、target/main 归属与快进检查、diff 风险分类、快照实际 active 的受影响服务及
   timer、原子落盘 deployment intent、使旧 marker 失效、暂停原先 active 的相关 timer、
   `git merge --ff-only <exact-sha>`、`uv sync --frozen`、第一次 preflight、按 intent 的精确集合
   重启服务、第二次 preflight、恢复原先 active 的 timer。最后由 target checkout 的隔离 stdlib
   bootstrap 重新加载 target authority。它把同步完成的 `.venv` 复制到 operation id + commit
   唯一命名的 owner-only 不可变环境目录，生成全量内容 manifest 并原子切换环境 selector，然后
   发布绑定 operation id、target 和环境 manifest 的 marker。旧 coordinator 再把 intent 推进为
   `completed`，最后由 target authority 原子发布 commit record。daemon 只接受
   `marker + completed intent + commit record + selected environment manifest` 完整一致的代际；
   旧 coordinator 不能替新版本 marker schema 写标记。每个 durable stage 同时写入 intent
   时间线和 JSONL 审计。
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
- 另一个部署进程或 Lab daemon 已持有
  `/home/lighthouse/.rquant-deploy/rquant.lock`（本地主 checkout 对应
  `/Users/roxor/brain/30-projects/.rquant-deploy/rQuant.lock`）。
- generation marker、completed intent、commit record、环境 selector/manifest 任一缺失、格式错误，
  或与 Git SHA、`uv.lock`、包版本、Python ABI、不可变 venv/解释器/site-packages 内容不一致。
- `sudo -n`、依赖同步、preflight 或服务健康检查失败。

部署器没有 `--force` / `--emergency` 绕过参数。高风险基础设施和生产数据操作必须另开
受控变更，并取得用户明确授权。

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

P1.5d 首次安装 Lab launchd 前还需在主 checkout 建立自有物理 `.venv`。完成目标 checkout 后，
只能用下面的显式模式创建第一个 marker：

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

## 中断恢复决策

1. 先读取审计与 `rquant.intent.json`，确认 operation id、previous/target、stage 和服务/timer 计划；
   不从当前 `origin/main` 猜目标。
2. 交易保护窗口内只做只读诊断，任何包含服务重启的 resume/rollback 都等待 15:10 后。
3. 目标版本确认可继续时执行 recorded target 的 resume；需要撤回时执行 recorded previous 的
   rollback。两者都必须走 `scripts/deploy-production.sh`，不能手工 reset 后补 marker。
4. 成功标准是 intent=`completed`，commit record 精确绑定 marker、intent 和 selected environment
   manifest，marker commit/schema 与最终 checkout 一致，两次 preflight 通过，intent 中 active
   services 健康且 active timers 已恢复。任一项缺失都仍是未完成事务。

## 旧脚本边界

`scripts/deploy.sh` 保留给 systemd unit 等人工基础设施部署。它会执行交互式 sudo，且不具备
精确 target、交易时段保护和自动回滚，因此不得用于 Codex 无人值守发布。
