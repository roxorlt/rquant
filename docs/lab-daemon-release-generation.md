# Strategy Lab daemon 发布代际边界

## 启动链路

三个 Lab daemon 由 launchd 使用 active generation 自带的 Python 运行该 generation 内封存的
`release/scripts/run-lab-daemon.py`。wrapper、preflight、bootstrap、`src/rquant`、三份 plist 模板和
私有 `.env` 副本都来自 exact-SHA release payload；`WorkingDirectory`、launcher 与 `PYTHONPATH`
只指向该不可变 generation。mutable checkout 被修改、重命名或移走，不会改变已激活 daemon 的
代码权威。release payload 由 `git archive <exact-sha>` 的显式 allowlist 生成，不复制 generation
存储根本身，因此不会递归自包含；完整环境 manifest 会对代码和 venv 一并哈希，GC/rollback 继续
保留 marker、commit、intent 或 previous generation 引用的旧代际。

wrapper 只使用标准库，并且在创建
generation lock 目录或锁文件前，先以纯只读模式验证 prepared runtime sentinel。`DATA_DIR` 必须
显式且非空，其他 Lab 路径沿用 Settings 的逐项默认语义。`.env` 从已验证父目录 FD、sentinel 从
已验证 runtime-root dir FD 使用 `openat(O_NOFOLLOW)` 打开，再从同一文件 FD 读取并在结束时复核目录项
身份；缺失、被替换或无法安全解析的 sentinel/相关 `.env` 路径配置会立即失败，仓库、Git index
和部署锁命名空间均不发生变化。stdlib dotenv 只把精确小写 `export` 识别为关键字，混合大小写
且指向 Lab 路径的歧义配置失败关闭。通过该门禁后才完成物理 checkout、bootstrap virtualenv、可信 Git、console launcher
和 clean commit 校验，并取得该 checkout 唯一发布锁的共享锁。所有只读 Git 调用显式使用
`GIT_OPTIONAL_LOCKS=0`。wrapper、只读 preflight 和隔离 bootstrap 都会验证
crash-persistent 提交协议，再从环境 selector 解析已封存的不可变 venv，以该 generation 的 Python
执行 `-I -S` bootstrap，而不是执行 checkout 中可变 `.venv` 的 console script。

`scripts/bootstrap-lab-daemon.py` 不处理 `site`、`.pth`、`sitecustomize` 或 user site。它只把已验证
的项目 `src` 和 selected immutable venv 的单一 `site-packages` 代际加入 `sys.path`，再次运行只读
preflight，再导入 `rquant.cli`。共享发布锁 fd 会保留到 daemon 退出，runtime guard 每个副作用
边界同时复验 clean SHA、发布代际和锁 inode。

Python runtime authority 会从文件系统根开始逐层 `openat(O_NOFOLLOW)` 声明的 runtime path，保留
并复核每一级目录 FD 的 device/inode/type/mode/owner。sentinel 从最终 trusted runtime-root FD
读取，首次 SQLite 登记也只从同一 FD 打开并 `fstat` 数据库对象；路径中的 symlink、任一 ancestor
rename/replacement 或声明路径与物理路径漂移都会失败关闭。

## 发布互斥

主 checkout `/Users/roxor/brain/30-projects/rQuant` 的锁固定为：

```text
/Users/roxor/brain/30-projects/.rquant-deploy/rQuant.lock
```

daemon 持共享锁；`scripts/deploy-production.sh` 另持稳定的 sibling handoff lock。macOS 正式
发布会在交易保护窗口外记录当时 loaded 的三个 Lab label，逐个 `bootout`，有界等待 shared lock
前先持久化严格 typed 的 prepared deployment intent；释放后再取得 generation 独占锁并原子接管该
intent，deployer 不重新 fetch 或重算 plan。事务成功或已回滚后，部署器只 `bootstrap` 原先 loaded 的 label，
并验证 launchd health 与 shared lock 已重新取得；每个 `launchctl print` 的超时取 command timeout
与当前整体/readiness 剩余预算的较小值，预算耗尽立即失败。任一步超时都返回失败，不会无限等待。dry-run
仅以共享锁核对并输出 handoff 计划，不停止 daemon。`launchctl` 始终由当前用户执行，sudoers
不授予它。由此一次进程只能看到一个完整 Git 代际，且常驻 KeepAlive 不再永久阻塞部署。

若常规发布在任一 handoff stage 中断，resume/rollback 以新的 operation 显式记录被接管的旧 deploy
operation。接管只允许 `deploy -> resume/rollback`，并从不可变 deployment intent 精确复核旧 target/ref、
新恢复目标、release profile、lifecycle 与 installation identity；任一漂移都会在 launchd mutation 前
失败关闭。

若只写完 typed `.intent.prepared.json` 就中断，恢复不会要求人工补记录：bootstrap 在 handoff 锁内
严格解析 prepared intent，幂等生成其原始 `deploy/planned` 根 operation 后再执行显式 resume/rollback。
deployer 接管前的失败清理只恢复 previous daemon 并落 `aborted`，不会把旧代际伪装成新 target 的
completed handoff。intent 永久保存初始 handoff operation，所有 rebound 和 supersede 必须从该根
连续；authority JSON 对任意层重复键一律失败关闭。

每个 partial handoff record 在落盘前都由 release authority 校验：`planned` 不得声称已经停止或
恢复 label，`stopping` 只允许 stopped 子集且 restarted 为空，`stopped` 必须覆盖全部 label，
`restarting` 允许在早期 abort/recovery 中保存 stopped/restarted 子集，`aborted` 则要求全部 label
已恢复且不生成 completed proof。若 recovery operation B 已写入而 intent 仍绑定 A，重试只在
`B.supersedes_operation_id == A`、action edge 和 immutable target/install binding 均成立时继续，
bootstrap 会在仍持 handoff 锁、尚未执行任何 `launchctl` mutation 时原子追加并重读验证 A→B
rebound，之后 deployer 只消费这个已持久化 binding；其他 operation 错配失败关闭。若此时相反
action 再接管，则必须先收敛 A→B，再生成 B→C，不能把物理链压缩成 A→C。

handoff 完成状态按 `completed proof -> operation record -> stable active record` 顺序原子发布。若任一写
边界崩溃，下次启动先只读校验三份记录的 operation、target、label、profile、installation、supersede
链和 generation binding；完全一致才在 handoff 锁内补齐后两份记录。proof 缺字段、伪造 generation
或任一 binding 漂移都不会触发收敛。接管旧 operation 前还要求其 id 等于 deployment intent 当前
`handoff_operation_id`；successor 与完整物理链通过验证后，bootstrap 必须在 mutation 前完成 durable
rebind，deployer 不再拥有延迟写回的分裂权威。显式 resume/rollback 的 readiness 若失败，
自动 rollback 可以继续 supersede 当前 recovery operation，并复核每一跳 action 与 intent binding。
partial-stop operation 的 stopped labels 可以是声明 labels 的合法子集，但阶段必须匹配；最终 proof
会验证完整 supersede 链每一跳的 target/ref/profile/lifecycle/installation binding。completed proof
后若崩溃在 intent completion 或 commit record 边界，显式 resume 会幂等补齐，且不会重复 launchd
mutation。
其中 completed proof 的 `generation_operation_id`、`environment_generation_id` 与 `code_sha` 并非
仅做格式检查：它们必须分别匹配 typed deployment intent、当前 generation marker/environment
selector/commit record 的真实权威值。bootstrap 与生产 deployer 共同调用 release authority 中的
changed-files、service/timer、generation 与 stage-history 校验，因此损坏或越权 intent 会在任何
`launchctl bootout/bootstrap` 前失败关闭。
物理 supersede 链还必须与 intent 的完整 rebound 序列逐跳相等，不能省略或插入隐藏 operation。
provisional daemon 会读取链内每一份已有 completed proof，并要求它与同 operation record 内容完全
一致；旧 proof 与新 partial active 只有在该 proof 是合法 ancestor 时才可继续启动。

同目录的 `rQuant.complete.json` 不是单独完成凭证。daemon 必须同时核对 completed intent、
`rQuant.commit.json`、`rQuant.environment.json` 和环境 manifest；commit record 精确绑定 marker、
intent content hash、operation id、commit 与环境 generation。部署器使用物理绑定的 uv，在新的
staging generation 中执行 `uv venv --relocatable` 与 `uv sync --frozen --active` 重建环境，不复制
当前 `.venv`。它仅允许经验证的 `bin/python*` 与 `lib64` 链接：解释器必须绑定已验证的系统 Python，
其他相对链接不得逃出 generation；随后封存权限并记录每个文件的 hash/身份。selector 只在完整
manifest 可验后原子切换。manifest 和 selector 的文件 rename、文件/目录 fsync 都共享同一取消
checkpoint；目录 fsync 后到达的取消会返回失败，但保留已落盘、可由下一次重放验证的完整记录，
不会把未持久化状态报告为成功。marker 可以先于 intent completion
出现，但 commit record 只能在 intent=`completed` 后发布，因此任何中断代际都不会被 daemon
接受。回滚以相同协议选择 previous commit 的不可变 generation。

安装状态重复登记只有在物理 binding 完全相同时才是幂等操作，并保留原 descriptor inode；已有
deployment handoff 后若 plist/runtime/installation identity 改变，普通 registration 失败关闭，必须
走单独受控 installation authority 迁移，不能覆盖旧 completed proof。

Lab runtime prepared sentinel 绑定长期稳定的 runtime-root device/inode。scheduler 首次创建
`lab_jobs.sqlite3` 时，从同一个已验证 root dir FD 读取 sentinel、用 `openat(O_NOFOLLOW)` 创建数据库，
并在仍持有该 FD 与 prepared lock 时登记数据库 inode；worker/finalizer 不具备首次登记权限。root
ancestor 在验证后发生 rename/replacement 时，创建与登记都会失败关闭，replacement namespace 不会
收到数据库写入。

发布环境 GC 只在同一 generation 独占锁内运行。它保留当前 selector、marker、commit、active
intent 的 resume/rollback 目标，以及按私有 manifest 判定的紧邻上一代；只删除超过宽限期、
严格位于 generation root、无 symlink/hardlink 且不再被引用的完成或失败目录。只读树先受控解冻
再按 descriptor 删除。每次扫描记录到 `rQuant.generation-gc.jsonl`，并在构建前验证 generation
预算与 `RQUANT_RELEASE_GENERATION_MIN_FREE_BYTES`；不足时不创建 staging。uv 子进程以短轮询检查
整体 deadline/cancellation，取消后终止完整进程组；manifest 编码、哈希、写入、fsync 和 GC 对
retained/orphan 记录的读取也按有界块执行 checkpoint，因此不会等到单个长步骤结束才响应取消。

## P1.5d 安装要求

P1.5d 安装 launchd 前必须在主 checkout 重建自有、物理、非 symlink 的 `.venv`，并确保部署锁
目录为当前用户所有且 mode `0700`、锁文件 mode `0600`。随后运行
`deploy-production.sh --initialize-generation --target <exact-ref>`；该 stdlib-only 模式在同一
独占锁内核对精确 target/origin-main、tracked clean、锁文件 hash、包版本、ABI 和物理 venv，
执行 frozen sync 与 preflight 后才初始化 marker。初始化中断后必须原样重跑同一个
`--initialize-generation --target <the-same-recorded-exact-target>`；`--recover-generation` 仅用于
已经持久化常规 deployment intent 的发布，不得用于初始化恢复，详见
`docs/production-release.md`。

P1.5b 已实现并用临时目录/fake launchctl 验证 `rquant lab-launchd-install` 与
`rquant lab-launchd-uninstall`：安装器先取得与发布 handoff 共用的独立安装事务锁，只接受已登记
installation state 精确授权的文件身份；随后停止登记为 loaded 的 label、有界确认 daemon 共享锁释放，
才取得 generation 独占锁并验证 current marker/selector/manifest。每次安装或卸载先 fsync 一份
identity-bound transaction journal，再用同文件系统 rename 保存原 inode；plist、local state、registered
state 和原 loaded label 在任一边界失败后都按 journal 精确恢复。首次安装遇到任何未登记的同名文件
都会失败关闭且保持其 bytes/inode 不变；幂等复跑不替换相同 inode。卸载只有在全部精确 managed
label 确认 unload 后才移除文件和两份状态，任一 bootout 失败则完整恢复。
P1.5b **没有**向 `~/Library/LaunchAgents` 写文件，也没有执行真实 `launchctl bootstrap/kickstart`；
这些实际安装、健康观察和回滚演练只在 P1.5d 人工基础设施窗口进行。

隔离 worktree 可继续复用链接 `.venv` 运行测试，但正式 daemon 会在读取
配置或创建运行时目录前拒绝这种 runtime。

该锁约束所有受控部署。具有同一 UID 且绕过 deployer 直接改写 checkout 的进程不属于本地权限
边界；runtime guard 仍会检测漂移并停止，但不把普通可写 worktree描述成不可变文件系统。
