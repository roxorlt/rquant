# 包 J 报告：live 策略链在真实沙箱与盘外空闲下可启动（#231、#232、#220）

**worktree**：`/Users/roxor/brain/30-projects/rQuant/.worktrees/ra-j-cc`
**分支**：`cc/20260908-live-chain-idle`，base `origin/main` = `a0bbb4c291797eb086fb2f2a9fc50a91cc264095`
**状态**：四个 commit 已在本地分支上，**未 push**；未重冻结 R07、未重生成运行时 manifest、未动
`deploy/systemd/`、`.env`、发布原语、stage、`runtime_authority*`、wrapper、`runtime_capabilities.py`。

---

## 1. 一句话结论

第三窗口那一夜四个 role 起不来，不是因为它们该拒绝，而是因为它们把「**别人还没启动**」和
「**这份文件坏了**」当成了同一件事。现在两者分开：已经在盘上的制品照旧全量校验、存在但不合法
照旧拒绝启动；**不存在**的制品改为在主循环里等，`last_error` 点名在等哪份文件，进程不退出，
`OnFailure` 不再触发。strategy 先建自己的 `runner.sqlite3`，router 先建自己的 bus 与 spool。

---

## 2. 改动清单

| commit | 内容 | 文件 |
|---|---|---|
| `2a418a3` | **改法 1+2**：feature spool 读者只读化 + 三处对端制品延迟打开 + 两处建序前移 | 新增 `src/rquant/runtime_peer_artifacts.py`；改 `src/rquant/feature_spool.py`、`src/rquant/runtime_builder_strategy.py`、`src/rquant/runtime_builder_signal.py`；新增 `tests/unit/test_runtime_peer_artifacts.py`、`tests/runtime_readonly_sandbox.py`；改三个 unit 测试文件与 `tests/integration/test_route_a_strategy_chain_e2e.py` |
| `898eb51` | **改法 3**：Linux e2e | 新增 `tests/integration/test_route_a_live_chain_idle_e2e.py` |
| `dfc18b6` | **改法 4 的补课**：第一轮变异存活暴露的三条性质 + 一个没保护到任何东西的沙箱 | 改三个 unit 测试文件 |
| （未提交）| `CHANGELOG.md` 的 `[Unreleased] / Fixed` 一条 + 本报告 | |

### 2.1 #231：feature spool 的读者不再碰生产者目录

`runtime_builder_strategy.py:262` 原来是 `FeatureBatchSpool(settings.feature_spool_root)`——
**写模式**。写模式的构造器要在生产者根里拿 `.feature-spool.lock`（`_initialize_source_identity`
→ `_exclusive_lock()`），游标默认也落在生产者根的 `cursors/` 下；strategy unit 的
`ReadWritePaths` 只有 `live/strategies/%i`，于是内核给了 `EROFS`。

改成：

```python
FeatureBatchSpool(
    settings.feature_spool_root,
    cursor_root=settings.runner_state_path.parent / "feature-cursors",
    read_only=True,
)
```

`read_only=True` 这条路径**本来就存在**（`tests/unit/test_feature_spool.py::
test_readonly_strategy_keeps_cursor_outside_feature_spool` 早就钉住了它的语义，与
`live_spool.py` 给 reference publisher 用的 `source_read_only` + `consumer_cursor_root`
是同一套形状），只是 builder 从来没用它。游标根**从 `runner_state_path` 的父目录派生**，
不新增 manifest 设置——新增设置要改画像生成器与 stage，那是本包边界之外。

同时在 `FeatureBatchSpool.__init__` 里加了一条**不变量**：只读消费者的 `cursor_root`
落在生产者根里面（含默认值）直接拒绝，报错点名路径。这样 #231 的形状在库一层就回不来，
不靠每个消费者自己记得。写者（feature）侧一个字没改，完整性校验一条没放宽。

### 2.2 #232 / #220：拥有者没启动 ≠ 制品坏了

新模块 `src/rquant/runtime_peer_artifacts.py`（约 110 行）：

- `DeferredPeerArtifact.probe()`——**构建 step 时**调用：路径上有东西就**当场打开、当场全量校验**，
  所以「存在但不合法」照旧在构造期拒绝启动，与改动前完全一致；
- `DeferredPeerArtifact.get()`——**step 里**调用：还没打开就再试一次，仍然不存在就抛
  `PeerArtifactUnavailableError`，消息里带 reader / 制品名 / 完整路径；
- 「不存在」的判据只有 `FileNotFoundError` 一种。其余任何 `OSError`（父段不是目录、符号链接环、
  沙箱不让穿过的目录）都算**在场**，交给打开器自己的校验去判——悬空符号链接不是「还没建」。

`PeerArtifactUnavailableError` 是 `ValueError` 子类，`run_service_loop` 的
`except Exception` 接住它、`record_failure` 把它记成 `last_error`（形如
`PeerArtifactUnavailableError: signal_router is waiting for the runner source its owner
creates: /home/lighthouse/rquant/data/runtime/live/strategies/svc-…/runner.sqlite3`），
状态 DEGRADED，**进程活着**，systemd 不重启、`OnFailure` 不推送。

三处对端制品改为延迟打开（都在 `_READONLY_PATH_SETTINGS[STRATEGY_LIVE]` / `[SIGNAL_ROUTER]`
里，即安装器早就声明「这不是我拥有的」的那些）：

| role | 制品 | 拥有者 | 探测路径 |
|---|---|---|---|
| `strategy_live` | feature spool | `feature_live` | `live/features/source-identity.json` |
| `strategy_live` | paper broker 台账 | `paper_broker` | `live/paper-brokers/<svc>/broker.sqlite3` |
| `strategy_live` | signal bus | `signal_router` | `live/signal-bus/signal_bus.sqlite3` |
| `signal_router` | 每个 runner 源 | `strategy_live` | `live/strategies/<svc>/runner.sqlite3` |

后两个用两个单方法代理（`_DeferredLifecycleFeatureSource`、`_DeferredRouteDrainAuthority`）
接进现有协议，`strategy_live_service.py` 一个字没改——完成签名那一组「要么全配、要么全不配」的
校验因此仍然成立，**不存在「缺 bus 就悄悄跳过完成回执」这条路**：盘外空闲时 route authority
根本不会被调用（`run_strategy_live_batch` 只在「收盘后 + 有 feature 收盘标记 + 当天是交易日」
那一支才读它），真到要读的时候缺文件仍然抛。

两处建序前移：

- `strategy_live`：`StrategyRunnerStore` 提到 builder 里**所有别人拥有的制品之前**。它是这个
  role 唯一拥有的制品，也是整条 live 平面的读者都在等的那份文件。
- `signal_router`：`settings.open_store()` / `SignalRouteSpool` / `SignalRouteCursorStore`
  提到 runner 源检查之前（#220 里 reviewer 的「第二选择」）。

---

## 3. 端到端证据

### 3.1 复现基线（改之前）

用与 systemd 同款的只读沙箱模拟，在真实 feature spool 上跑改动前的消费者，报的就是生产现场那一句：

```
OSError: [Errno 30] Read-only file system: '<root>/live/features/.feature-spool.lock'
```

沙箱只拦到这一条：`mkdir` 三个已存在目录没有被拦。**这是有意的、并且是主机证据要求的**——
真实内核对「已存在的目录」先答 `EEXIST`（`Path.mkdir(exist_ok=True)` 吞掉），再谈 `EROFS`，
否则那一夜的 strategy 根本走不到锁文件那一步。这条推理写在
`tests/runtime_readonly_sandbox.py` 的 `refuse()` 里。

### 3.2 沙箱怎么做的

`tests/runtime_readonly_sandbox.py`：

- `readonly_runtime(root, writable=[...])`：把 `root` 下除 `writable` 之外的**写系统调用**
  全部拒成 `OSError(EROFS, "Read-only file system", path)`——与 systemd
  `ProtectSystem=strict` + `ReadWritePaths` 的可观测结果逐字一致。拦截面覆盖
  `os.{mkdir,makedirs,rmdir,remove,unlink,chmod,chown,utime,truncate,mknod,mkfifo,rename,replace,symlink,link,open}`
  与 `io.open`/`builtins.open` 的写模式（`tempfile.mkstemp` 走 `os.open`，自动覆盖）。
  每一次拒绝都记进 `violations`，测试断言它是空的。
- `tree_state(root)`：目录树的（相对路径、mode、size、mtime_ns）快照。作为**终态证据**与
  syscall 拦截互相独立——即使有写绕过了 wrapper（例如通过 `dir_fd` 的写），终态比对也会发现。
- **不用 `chmod 0500` 做沙箱**：`FeatureBatchSpool` 自己会拒绝 mode 不是 0700 的生产者根，
  而主机上那个目录的 mode 就是 0700、拦住写的是 *mount* 不是 mode。chmod 会把被测对象的
  校验一起改掉，那是假的沙箱。

e2e 里每个 role 的 `writable` 清单是**从 `deploy/systemd/rquant-runtime-*@.service` 里
逐字读出来的 `ReadWritePaths=`**（去掉 systemd 的 `-` 前缀、代入 `%i`、把冻结的
`/home/lighthouse/rquant/data/runtime` 前缀换成本次 world 的 runtime root）。所以：

1. 这个 unit 文件改了，测试跟着改，不会各说各话；
2. 「四个 role 一次越界写都没有」这句话是被断言过的，等于**顺带把四份 `ReadWritePaths`
   清单本身验了一遍**。

### 3.3 主 e2e：`tests/integration/test_route_a_live_chain_idle_e2e.py`

world 沿用 `test_route_a_legacy_binding_e2e` 的：真实 `install_runtime_deployment_bundle`
legacy 根（真的 `current -> generations/<64hex>` 相对符号链接、真的 `deployment-profile.json`）、
真的 `runtime-authority-stage --legacy-runtime-root` 采集本代 manifest、真的发布进 root-owned
权威链、wrapper 自己的 `resolve_launch` 派生 argv **与子环境**（`mock.patch.dict(os.environ,
…, clear=True)`），notifier 的凭据按包 E 的夹具形状投递（0400、单链接、属主自己、
`CREDENTIALS_DIRECTORY` 进 `source_environment` 再由 wrapper 生成子环境）。

起始状态就是那一夜：

- `live/features` 只被 feature role 初始化过（`FeatureBatchSpool(root)` 一次），**零批次**；
- `signal_bus.sqlite3`、`spool/`、`broker.sqlite3`、三份 `runner.sqlite3` **全部不存在**；
- 时钟 = 明天 22:00 UTC = 后天 06:00 上海：**盘外**，且不是 bundle 日历开的那一天
  （日历只开 2026-08-03）。取「明天」而不是写死日期，是因为 router 会拒绝 mtime 在未来的
  冻结路由策略文件，而夹具文件都是跑测试时才写的。

五条用例，全绿：

| 用例 | 断言的事实 |
|---|---|
| `test_the_whole_live_chain_starts_idle_in_the_runbook_order` | runbook C-3 顺序 strategy×3 → router → broker → notifier，六个服务全部进主循环跑完一次迭代；三份 `runner.sqlite3` 在 router 启动**之前**就在；`bus` 与 `spool/source.json` 在 router 一步之后就在；`broker.sqlite3` 在 broker 构造后就在；feature spool 目录树**全程逐字节未变**；四个 role 的越界写 `violations == []` |
| `test_the_router_started_first_creates_the_bus_and_waits_by_name` | #220 的反向：router 先起也行——它建出 bus 与 spool，然后在主循环里等 runner，`last_error` 里有 `PeerArtifactUnavailableError` 和 `runner.sqlite3`；随后三个 strategy 起来全部成功 |
| `test_a_strategy_waits_by_name_while_the_feature_role_has_published_nothing` | feature role 完全没跑过时，strategy 仍然进主循环、仍然建出自己的 `runner.sqlite3`，只是迭代等在 feature spool 上（`last_error` 含 `feature spool`） |
| `test_a_corrupt_runner_database_still_stops_the_router` | 反向：runner 文件在场但不是数据库 ⇒ router **构造期拒绝**（抛出、不是 `PeerArtifactUnavailableError`） |
| `test_a_corrupt_route_spool_still_stops_the_paper_broker` | 反向：`spool/source.json` 被改坏 ⇒ broker 的迭代拒绝、`last_error` 含 `spool`、成功数 0；spool 目录整个被换成普通文件 ⇒ broker **构造期拒绝** |

**broker 与 notifier 的那一次迭代是「等」不是「成」**，两者等的都不是本包碰过的东西，报告如实记下：

- `paper_broker`：`PaperExecutionConstraintUnavailableError: current pointer is unavailable`
  ——`authorities/paper-execution` 的 current 指针由 `paper_constraint_publisher` 发布，
  这条空闲链上没有。**它已经进了主循环、也建出了自己的台账**，这正是 #220 之前不给它的东西。
- `notifier`：夹具里 `external/rquant_ro.duckdb` 是占位路径（画像输入夹具本来就没有这个文件），
  与 spool 无关。

两条都断言了「错误里没有 `route spool`、也不是 `PeerArtifactUnavailableError`」。

### 3.4 三个平台的数字

| 环境 | Python | 结果 |
|---|---|---|
| 本机 macOS（`.venv`） | 3.11.15 | **366 passed**（4:19） |
| Docker `python:3.11-slim`，非 root（uid 1000），`uv sync --frozen` | 3.11.16 | **366 passed**（4:49） |
| Docker `python:3.12-slim`，非 root（uid 1000），`uv sync --frozen` | 3.12.14 | **366 passed**（4:53） |

跑的是同一组 366 条：本包四个新增/改动的测试文件，加上定向回归
`test_runtime_builder_feature` / `test_feature_live_service` / `test_strategy_live_service` /
`test_strategy_runner` / `test_signal_router_runtime` / `test_runtime_service_main` /
`test_runtime_service_builtin` / `test_runtime_service_control` /
`test_builtin_strategy_paper_lifecycle_integration` / `test_runtime_builder_paper` /
`test_route_a_strategy_chain_e2e` / `test_isolated_runtime_pipeline`。

**「两版本」= CI 的 Python 3.11 / 3.12 矩阵**（沿用包 F 审查 S3 的口径）。

> 途中一次假失败值得记下来，免得下次再查一遍：第一次 Docker 3.11 跑出
> `ExecutableDependencyError: executable source is unavailable:
> tests.unit.test_runtime_builder_strategy:_replacement_runtime_dispatcher`，3.12 与 macOS
> 都不复现。原因是我的容器拷贝把 macOS 的 `__pycache__` 一起带了进去，`inspect.getsource`
> 撞上过期字节码；把 `__pycache__` 排除后同一条用例在 3.11 里通过，baseline
> （`a0bbb4c` 的 `git archive`）在同一容器里也通过。是搬运方式的问题，不是代码的问题。

---

## 4. 变异表（6 条，全部被杀）

每条变异只改 `src/`，跑同一组 117 条 unit（四个文件）+ 5 条 e2e，跑完 `git checkout -- src/`。
基线：**117 passed / 5 passed**。

| # | 变异 | 位置 | 被杀于 |
|---|---|---|---|
| **M1** | 读者的锁回到生产者目录：消费者改回 `FeatureBatchSpool(feature_spool_root)` 写模式 | `runtime_builder_strategy.py` | unit **4**（`writes_nothing_inside_the_producer_root`、`keeps_its_cursors_beside_its_runner_database`、`a_feature_spool_that_is_present_and_unsafe_still_fails_closed`、`a_strategy_that_refuses_to_start_still_leaves_its_runner_database`）＋ e2e **4**（整条链、router 先起、runner 损坏、spool 损坏） |
| **M2** | runner 延迟创建：`StrategyRunnerStore` 移回所有对端制品之后 | `runtime_builder_strategy.py` | unit **1**（`a_strategy_that_refuses_to_start_still_leaves_its_runner_database`） |
| **M3** | router 构造期失败关闭：`probe()`/`get()` 换成直接 `_open_artifact()` | `runtime_builder_signal.py` | unit **3**（`creates_its_own_artifacts_before_it_looks_for_a_runner`、`route_spool_source_document_is_published_before_the_wait`、`a_runner_database_that_appears_later_is_routed_from`）＋ e2e **1**（router 先起） |
| **M4** | **损坏文件被放行**：`probe()` 把打开器的异常吞成「还没建」 | `runtime_peer_artifacts.py` | unit **9**（含既有用例 `test_signal_router_manifest_authority_identity_failures_are_closed[overrides2-strategy spec]`）＋ e2e **1**（runner 损坏） |
| **M5** | spool 放行「只读消费者的游标根在生产者根里」这一不变量 | `feature_spool.py` | unit **1**（`a_readonly_consumer_may_not_keep_its_cursors_in_the_producer_root`） |
| **M6** | router 先查 runner 再建 bus（只改顺序） | `runtime_builder_signal.py` | unit **1**（`the_signal_bus_exists_even_when_a_runner_source_refuses`） |

**第一轮跑完 M2 / M4 / M5 / M6 全部存活**，四条各有各的原因，都记在
commit `dfc18b6` 里：

- M2、M6 存活是因为**延迟打开之后它们之间再没有会失败的东西**，前移这件事就没有可观测后果了。
  补的两条用例给了它可观测后果，而且补的正是 #232 / #220 真正要的那条性质——
  「一个自己起不来的 strategy 不许把 router 一起拖下水」「一份读不了的 runner 不许把 bus
  从另外两个 strategy 手里夺走」。
- M4 第一版变异写错了（改的是 `exists` 对非 `FileNotFoundError` 的 `OSError` 的判断，而所有
  损坏用例的文件都 `lstat` 得到），改成「打开器的拒绝被降级成等待」才是「损坏文件被放行」的
  正确形状，一改就被 10 条用例打红。
- M5 存活是因为 builder 本来就传了外置游标根，不变量没有任何用例直接打它。
- 另外发现 `test_the_feature_consumer_writes_nothing_inside_the_producer_root` 原来把
  `tmp_path` 整个声明成可写，而 `_manifest` 把 runner 库直接放在 `tmp_path` 下，**沙箱等于
  没开**；改成只保护生产者目录之后 M1 才被它打红。

---

## 5. runbook C-3 顺序的最终版

### 5.1 直接回答：不再需要「先起 strategy 一轮失败」

那一步的全部作用是「靠一次失败的副产品把 `runner.sqlite3` 留下来」，而且要在
`StartLimitIntervalSec=600s` / `StartLimitBurst=5` / `RestartSec=10s` 给的**约 50 秒**里完成
第二步，本质是竞态。现在：

- `strategy_live` **一启动就建** `runner.sqlite3`，不需要先失败一次；
- `signal_router` **一启动就建** bus 与 spool，缺 runner 时在主循环里等，不退出；
- 两个方向都不再互等，**任何顺序都能起来**。

所以 C-3 里「④ strategy（预期 failed，这是设计）」这一步连同「⑤ 两个预期受阻」的定性
一起删掉，C-4 的放行判据从「`rquant-runtime-strategy@` failed（设计）」改成
**active**。

### 5.2 建议顺序（唯一一条真依赖）

```
① rquant-runtime-health              （探路，不变）
② serving                            （不变）
③ feature_live                       ← 唯一的硬前置：只有它会写 live/features/source-identity.json
   探针 0： test -f $ROOT/live/features/source-identity.json
④ strategy_live × 3
   探针 1： [ "$(ls -1 $ROOT/live/strategies/*/runner.sqlite3 2>/dev/null | wc -l)" -eq 3 ]
⑤ signal_router
   探针 2： test -f $ROOT/live/signal-bus/spool/source.json
⑥ paper_broker → notifier
⑦ 其余 unit（顺序无关）
```

`ROOT=/home/lighthouse/rquant/data/runtime`。三条探针的产物在
`tests/integration/test_route_a_live_chain_idle_e2e.py` 里都是被断言过的真实文件。

**③ 是软依赖，不是硬依赖**：feature role 没起的时候 strategy 照样进主循环、照样把
`runner.sqlite3` 留下，只是每一轮 `last_error` 写着在等
`.../live/features/source-identity.json`（`test_a_strategy_waits_by_name_while_the_feature_role_has_published_nothing`
钉住了这一条）。把 feature 排在前面只是为了让 strategy 一上来就是 RUNNING 而不是 DEGRADED。

### 5.3 窗口里应当预期到的两个 DEGRADED

这两个都**不是**本包的缺口，但会出现在健康面板上，先说清楚免得当成回归：

1. `paper_broker`：在 `paper_constraint_publisher` 发布出
   `authorities/paper-execution` 的 current 指针之前，每一轮
   `PaperExecutionConstraintUnavailableError: current pointer is unavailable`。
   **进程是活的**。若整晚都不发布，那是下一条要开的 issue（本包 e2e 已经把这个状态钉成
   已知事实，不会被误读成 #220 复发）。
2. `notifier`：依赖 `serving/page-control` 与操作库的那一段；夹具里是占位路径，主机上取决于
   serving 面的状态。

---

## 6. 边界确认

- 四个 commit 全在本地分支 `cc/20260908-live-chain-idle` 上，**未 push**。
- 未动：`deploy/`（只**读**了三个 unit 文件的 `ReadWritePaths`）、`.env`、发布原语、stage、
  `runtime_authority*`、wrapper、`runtime_capabilities.py`（包 I 在改）。
- 未重冻结 R07、未重生成运行时 manifest。
- 没有 skip / xfail；没有放宽任何完整性校验——M4、M5 与两条反向 e2e 是证据。
- 本机为跑测试临时写过一份 `.env`（只有占位 token 与 worktree 内的路径，`.gitignore` 覆盖），
  交付前已删除。

---

## 7. 交给协调者的两件事（本包边界之外，但会让 CI 红）

按包 F 的先例（`60bc367`、`7cc7ab8` 都是合入后由协调者单独提交的），这两件事不在本包做：

### 7.1 CI 分片清单要重生成

本包净增 **28 条 case**，另有 **1 条 nodeid 改名**
（`test_route_a_strategy_chain_e2e.py::test_a_strategy_role_without_the_signal_bus_still_creates_its_runner_database`
→ `…::test_a_strategy_role_starts_before_the_router_and_leaves_its_runner_database`）。
明细：

| 文件 | 清单里 | 现在 | 增量 |
|---|---:|---:|---:|
| `tests/unit/test_runtime_peer_artifacts.py`（新） | 0 | 9 | +9 |
| `tests/integration/test_route_a_live_chain_idle_e2e.py`（新） | 0 | 5 | +5 |
| `tests/unit/test_runtime_builder_strategy.py` | 36 | 43 | +7 |
| `tests/unit/test_runtime_builder_signal.py` | 42 | 48 | +6 |
| `tests/unit/test_feature_spool.py` | 16 | 17 | +1 |
| `tests/integration/test_route_a_strategy_chain_e2e.py` | 6 | 6 | 0（1 条改名） |

`index.json` 的固定值 **14070 → 14098**。不重生成的话
`tests/unit/test_assert_full_suite_shards.py::
test_checked_in_manifest_matches_exact_collection_without_missing_or_duplicate_cases`
直接红，五个 shard job 与 contract job 一起倒。命令见 `tests/README.md`：

```bash
RQUANT_DISABLE_DOTENV=1 TUSHARE_TOKEN_MAIN=00000000000000000000000000000000 \
NOTIFY_ENABLED=false DATA_DIR=/private/tmp/rquant-ci/data \
DUCKDB_PATH=/private/tmp/rquant-ci/data/test.duckdb \
DUCKDB_READONLY_PATH=/private/tmp/rquant-ci/data/test_ro.duckdb \
PARQUET_DIR=/private/tmp/rquant-ci/parquet LOG_DIR=/private/tmp/rquant-ci/logs \
uv run python scripts/full_suite_shards.py generate \
  --manifest-dir tests/manifests/full-suite-v1
```

### 7.2 R07 差分门要重冻结

`policy-v1.json` 的 `source_file_snapshots` 里有 9 个文件，本包改了其中两个
（`src/rquant/runtime_builder_strategy.py`、`src/rquant/runtime_builder_signal.py`），
所以 `tests/unit/test_signal_family_differential_gate.py::
test_forbidden_definition_universe_is_static_source_only`（它对 HEAD 直接跑
`verify_top_level_source_closure`）会报 source digest / top-level declaration closure drift。
**本包边界写明「不重冻结」，所以没做**；生成器是 `scripts/r07_policy_regenerate.py`（带
`--check`）。新增的 `src/rquant/runtime_peer_artifacts.py` 不在那 9 个文件里，只有那两个
既有文件需要重新快照。
