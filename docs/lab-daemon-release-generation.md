# Strategy Lab daemon 发布代际边界

## 启动链路

三个 Lab daemon 由 launchd 先运行 `scripts/run-lab-daemon.py`。wrapper 只使用标准库，并且在创建
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

## 发布互斥

主 checkout `/Users/roxor/brain/30-projects/rQuant` 的锁固定为：

```text
/Users/roxor/brain/30-projects/.rquant-deploy/rQuant.lock
```

daemon 持共享锁；`scripts/deploy-production.sh` 另持稳定的 sibling handoff lock。macOS 正式
发布会在交易保护窗口外记录当时 loaded 的三个 Lab label，逐个 `bootout`，有界等待 shared lock
释放后再取得 generation 独占锁。事务成功或已回滚后，部署器只 `bootstrap` 原先 loaded 的 label，
并验证 launchd health 与 shared lock 已重新取得；每个 `launchctl print` 的超时取 command timeout
与当前整体/readiness 剩余预算的较小值，预算耗尽立即失败。任一步超时都返回失败，不会无限等待。dry-run
仅以共享锁核对并输出 handoff 计划，不停止 daemon。`launchctl` 始终由当前用户执行，sudoers
不授予它。由此一次进程只能看到一个完整 Git 代际，且常驻 KeepAlive 不再永久阻塞部署。

若常规发布在任一 handoff stage 中断，resume/rollback 以新的 operation 显式记录被接管的旧 deploy
operation。接管只允许 `deploy -> resume/rollback`，并从不可变 deployment intent 精确复核旧 target/ref、
新恢复目标、release profile、lifecycle 与 installation identity；任一漂移都会在 launchd mutation 前
失败关闭。

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
`docs/production-release.md`。P1.5b 不安装 launchd，也不
修改现有主 checkout。隔离 worktree 可继续复用链接 `.venv` 运行测试，但正式 daemon 会在读取
配置或创建运行时目录前拒绝这种 runtime。

该锁约束所有受控部署。具有同一 UID 且绕过 deployer 直接改写 checkout 的进程不属于本地权限
边界；runtime guard 仍会检测漂移并停止，但不把普通可写 worktree描述成不可变文件系统。
