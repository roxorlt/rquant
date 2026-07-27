# Strategy Lab daemon 发布代际边界

## 启动链路

三个 Lab daemon 由 launchd 先运行 `scripts/run-lab-daemon.py`。wrapper 只使用标准库，完成
物理 checkout、virtualenv、可信 Git、console launcher 和 clean commit 校验，并取得该 checkout
唯一发布锁的共享锁。wrapper、只读 preflight 和隔离 bootstrap 都会验证 crash-persistent 完成
标记，再执行同一 virtualenv Python 的 `-I -S` bootstrap，而不是直接执行 console script。

`scripts/bootstrap-lab-daemon.py` 不处理 `site`、`.pth`、`sitecustomize` 或 user site。它只把已验证
的项目 `src` 和该物理 virtualenv 的单一 `site-packages` 代际加入 `sys.path`，再次运行只读
preflight，再导入 `rquant.cli`。共享发布锁 fd 会保留到 daemon 退出，runtime guard 每个副作用
边界同时复验 clean SHA、发布代际和锁 inode。

## 发布互斥

主 checkout `/Users/roxor/brain/30-projects/rQuant` 的锁固定为：

```text
/Users/roxor/brain/30-projects/.rquant-deploy/rQuant.lock
```

daemon 持共享锁；`scripts/deploy-production.sh` 的 Python deployer 持独占锁。部署已经开始时，新
daemon 启动失败；daemon 仍在运行时，部署失败关闭。由此一次进程只能看到一个完整 Git 代际，
不能在检查与 import 之间混入合法部署。

同目录的 `rQuant.complete.json` 是唯一完成凭证，schema v1 绑定精确 Git commit、`uv.lock` 与
`pyproject.toml` hash、包版本、Python 版本/ABI、物理 venv、`pyvenv.cfg`、解释器和
site-packages 身份。部署器在第一次 checkout/依赖持久变更前使旧 marker 失效；只有 target
checkout、`uv sync --frozen`、两次 preflight 和服务验证全部成功后，才以 `0600` 临时文件、
short-write 循环、文件 `fsync`、内容回读/hash 复验、原子 rename、目录 `fsync` 发布。硬中断会
留下缺失或 stale marker，daemon 因而失败关闭。回滚也必须完整验证旧代际后才能重发旧 marker。

## P1.5d 安装要求

P1.5d 安装 launchd 前必须在主 checkout 重建自有、物理、非 symlink 的 `.venv`，并确保部署锁
目录为当前用户所有且 mode `0700`、锁文件 mode `0600`。随后运行
`deploy-production.sh --initialize-generation --target <exact-ref>`；该 stdlib-only 模式在同一
独占锁内核对精确 target/origin-main、tracked clean、锁文件 hash、包版本、ABI 和物理 venv，
执行 frozen sync 与 preflight 后才初始化 marker。中断后只能以显式 `--recover-generation` 的
`resume` 或 `rollback` 动作恢复，详见 `docs/production-release.md`。P1.5b 不安装 launchd，也不
修改现有主 checkout。隔离 worktree 可继续复用链接 `.venv` 运行测试，但正式 daemon 会在读取
配置或创建运行时目录前拒绝这种 runtime。

该锁约束所有受控部署。具有同一 UID 且绕过 deployer 直接改写 checkout 的进程不属于本地权限
边界；runtime guard 仍会检测漂移并停止，但不把普通可写 worktree描述成不可变文件系统。
