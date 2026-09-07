# 包 J 独立审查（#231、#232、#220）

**审查对象**：worktree `/Users/roxor/brain/30-projects/rQuant/.worktrees/ra-j-cc`，分支
`cc/20260908-live-chain-idle`，base `a0bbb4c`（已核对 `git rev-parse origin/main` =
`a0bbb4c291797eb086fb2f2a9fc50a91cc264095`），四个 commit `2a418a3` → `898eb51` →
`dfc18b6` → `ea1400f`。
**审查标准**：`route-a-pkgJ-brief.md` 改法 1–4 与边界，加上 `gh issue view 231 232 220`。
**审查方式**：所有结论都由我自己在本机与 Docker 里重新跑出来，不引用实现报告的数字。
本次审查没有修改任何代码、没有 commit；临时试验文件跑完即删，最终 `git status --porcelain`
为空、`git diff HEAD` 为空。

---

## 1. 裁定

**有条件通过**：改法 1、2 的主体正确，改法 3 的 e2e 是真的（我在 Docker 非 root 里复跑通过，
并且用自造变异证明沙箱确实按各 unit 的 `ReadWritePaths` 判越界），改法 4 的六条变异我逐条复跑，
杀伤范围与报告一致。

**两条 must-fix 必须先解决再合入**：

1. 四类对端制品里有一类（`paper_broker` 台账）**根本没有调用 `probe()`**，于是「文件存在但
   不合法照旧拒绝启动」这条对它不成立，相对 base `a0bbb4c` 是一次**放松**；报告 §2.2 的表和
   CHANGELOG 都写成「三处/所有已经在盘上的制品当场打开、当场全量校验」，与代码不符。
2. e2e 的 world 只有**一代** bundle，不是报告与测试文件 docstring 里写的「两代」；简报改法 3
   要求的是两代。

两条都很小：第 1 条是补一次 `probe()` 加一条用例（或者把「这一类故意延后」写进代码与报告并用
用例钉住），第 2 条是装第二代（仓库里已有现成写法）或者把两处文字改对。

---

## 2. 逐条核读

### 2.1 改法 1（#231）：feature spool 的读者不再写生产者目录

| 简报要求 | 核读结果 |
|---|---|
| 消费者改 `read_only=True` | ✅ `src/rquant/runtime_builder_strategy.py:370-378` |
| 游标根 = `<runner_state_path>.parent / "feature-cursors"`，派生、不新增 manifest 设置 | ✅ 同上；`runner_state_path` 在生产画像里是 `live/strategies/<svc>/runner.sqlite3`（`runtime_production_profile.py:792-798`），所以游标根落在 `live/strategies/<svc>/feature-cursors`，正好在 strategy unit 的 `ReadWritePaths=…/live/strategies/%i` 里面。`RuntimeServiceManifest` 的 settings 没有新增字段 |
| `FeatureBatchSpool` 拒绝「只读消费者的游标根在生产者根内」 | ✅ `src/rquant/feature_spool.py:377-390`，默认值也拒 |
| 写者侧未动 | ✅ `git diff --name-only a0bbb4c..HEAD` 里没有 `runtime_builder_feature.py`；`feature_spool.py` 的改动只有新增的不变量与一个私有判定函数，`publish` / `publish_session_close_marker` / `_atomic_write` 一个字符没动 |
| 生产者目录逐字节不变 | ✅ e2e 用 `tree_state()` 在整条链跑完前后逐项比对（相对路径、mode、size、mtime_ns），我复跑通过 |

补充核读：只读消费者在生产者根里**再没有任何写操作**——`_initialize_source_identity` 的
只读分支不取锁（`feature_spool.py:581-587`），`commit_cursor` 取的是
`cursor_root/.cursor.lock`（`_cursor_lock_path` 在游标根外置时指向消费者自己的目录），
`_ensure_private_directories` 只在非只读时才 `mkdir` / `chmod` 生产者目录。

### 2.2 改法 2（#232 / #220）：拥有者没启动 ≠ 制品坏了

| 简报要求 | 核读结果 |
|---|---|
| `probe()` 构造期打开并校验，坏则拒 | ⚠️ 四类里三类成立，`paper_broker` 台账那一类**没有调用**（见 MF-1） |
| `get()` 主循环内等待、抛 `PeerArtifactUnavailableError` 点名文件 | ✅ `runtime_peer_artifacts.py:88-99`，消息含 reader、制品名、完整路径 |
| 进 `last_error`、不触发 `OnFailure` | ✅ `run_service_loop`（`runtime_service_control.py:518-527`）用 `except Exception` 接住，`record_failure` 记 `last_error`、状态 DEGRADED、进程不退出；e2e 里 `code == 0`。我自己跑了一次观测，router 先起时 `last_error` = `PeerArtifactUnavailableError: signal_router is waiting for the runner source its owner creates: …/runner.sqlite3` |
| `StrategyRunnerStore` 先于所有 peer 创建 | ✅ `runtime_builder_strategy.py:343-364` 在 feature spool、bus 之前；变异 M2 复跑被杀 |
| router 的 bus / spool / cursor 先于 runner 检查 | ✅ `runtime_builder_signal.py:572-583`；变异 M6 复跑被杀 |

### 2.3 改法 3：Linux e2e

`tests/integration/test_route_a_live_chain_idle_e2e.py`，5 条用例。我逐项核读并复跑：

| 要求 | 核读结果 |
|---|---|
| 真 stage / publish | ✅ `RouteAWorld.stage_and_publish()` 走 `world.stage(..., legacy_runtime_root=…)` 再 `world.publish(plan)`，发布进 root-owned 权威链 |
| 真两代 bundle | ❌ **只有一代**（见 MF-2） |
| wrapper 派生 argv 与子环境 | ✅ `_verify.resolve_launch(...)` 出 `module_argv` 与 `environment`，跑的时候 `mock.patch.dict(os.environ, …, clear=True)`；唯一改写的是 `--control-root`（包 A 冻结的那个 seam） |
| 各 role 用自己 unit 的 `ReadWritePaths` 做沙箱 | ✅ `read_write_paths()` 用正则从 `deploy/systemd/rquant-runtime-{strategy,signal-router,paper-broker,notifier}@.service` 读 `ReadWritePaths=`，去掉 `-` 前缀、代入 `%i`、把 `/home/lighthouse/rquant/data/runtime` 换成本次 runtime root。我对着四个 unit 文件逐条核过，条目一致 |
| 空 spool + 盘外时钟 | ✅ `idle_chain` 只跑一次 `FeatureBatchSpool(feature_root)`（零批次），并断言 bus / spool / 台账 / 三份 runner 都不存在；时钟取「明天 22:00 UTC」= 后天 06:00 上海，且 bundle 日历只开 2026-08-03 |
| 六个服务按 C-3 顺序全部进主循环 | ✅ 我自己驱动一遍：strategy×3 与 router 各 `total_successes=1`、`last_error=None`；broker 与 notifier 各跑完一次迭代（结果是失败，见 §5） |
| 四 role 零越界写 | ✅ 断言 `violations == []`；我用自造变异 S1 证明这条断言是有效的（把游标根挪到 `live/strategies/`，四条 e2e 立刻报 `[Errno 30] Read-only file system`） |
| 反向：坏 runner ⇒ router 拒 | ✅ 构造期抛 `ValueError`，且不是 `PeerArtifactUnavailableError` |
| 反向：坏 spool ⇒ broker 拒 | ✅ `source.json` 被改坏 ⇒ 迭代拒；spool 目录被换成普通文件 ⇒ 构造期拒 |

**关于「有一处夹具曾把 `tmp_path` 整个设为可写」**：已修，且没有同类残留。全仓库
`readonly_runtime(` 的调用点只有两处（`tests/unit/test_runtime_builder_strategy.py:1245`
只保护生产者根、不传 `writable`；e2e 只传该 role 自己 unit 的清单），我用 M1 复跑确认了
单测那一处现在真的会红。

### 2.4 改法 4：变异

我把六条变异逐条重新造、重新跑（每条只改 `src/`，跑完 `git checkout -- src/`），
基线是 **117 unit（四个文件）+ 5 e2e**，与报告一致。

| # | 变异 | 我复跑的结果 | 与报告一致 |
|---|---|---|---|
| M1 | 消费者改回写模式 `FeatureBatchSpool(feature_spool_root)` | 杀：unit 4 + e2e 4（e2e 报的正是主机那句 `.feature-spool.lock` EROFS） | ✅ 4+4 |
| M2 | `StrategyRunnerStore` 移回所有对端制品之后 | 杀：unit 1 | ✅ 1 |
| M3 | router 构造期直接开每一份 runner 源 | 杀：unit 3 + e2e 1 | ✅ 3+1 |
| M4 | `probe()` 把打开器的异常吞成「还没建」 | 杀：unit 9 + e2e 1 | ✅ 9+1 |
| M5 | 去掉「只读消费者游标根不得在生产者根内」不变量 | 杀：unit 1 | ✅ 1 |
| M6 | bus / spool / cursor 移回 runner 检查之后 | 杀：unit 1 | ✅ 1 |

自补两条：

| # | 变异 | 结果 |
|---|---|---|
| S1 | 游标根改成 `runner_state_path.parent.parent / "feature-cursors"`（仍在生产者根之外，但在 strategy unit 授权目录之外） | **杀**：13 条（unit 3 + e2e 4 报 EROFS，其余为连锁）。这条证明 e2e 沙箱判的是「unit 声明的那几条路径」，不是只判生产者根 |
| S2 | `DeferredPeerArtifact.exists` 把所有 `OSError` 都当成「还没建」 | **存活**：122 全绿（见 SF-1） |

### 2.5 边界与工程卫生

| 项 | 结果 |
|---|---|
| `git status` | ✅ 干净 |
| 改动面 | ✅ `git diff --name-only a0bbb4c..HEAD` 13 个文件，无 `deploy/`、`.env`、发布原语、stage、`runtime_authority*`、wrapper、`runtime_capabilities.py` |
| commit trailer | ✅ 四个 commit 都带 `Co-Authored-By` 与 `Claude-Session` |
| 未 push / 未重冻结 / 未重生成 manifest | ✅ |
| 没有 skip / xfail | ✅ 我在四个新增改动的测试文件里未见 skip/xfail |
| `ruff check`（改动文件） | ✅ All checks passed |
| `ruff format --check` | ⚠️ 见 N-2（CI 不跑，仅记录） |
| 与包 I（`ra-i-cc` `76f7015`）`merge-tree` | 仅 `CHANGELOG.md` 一处冲突，源码零冲突（见 §6） |

### 2.6 两版本数字（我自己跑的）

| 环境 | Python | 结果 |
|---|---|---|
| 本机 macOS `.venv` | 3.11.15 | **366 passed**（4:24） |
| Docker `python:3.11-slim`，uid 1000 非 root，`uv sync --frozen` | 3.11.16 | **366 passed**（5:08） |
| Docker `python:3.12-slim`，uid 1000 非 root，`uv sync --frozen` | 3.12.14 | **366 passed**（5:18） |

跑的是报告列的同一组 17 个文件。

> 我头两次 Docker 尝试失败过，原因是容器里 `TMPDIR` 父链是 0755 且没装 git，
> `runtime_authority` 的父链校验报 `deployment lock ancestor … is unsafe`。按 CI 自己的
> 私有根做法（`umask 077` + 各级 `chmod 700`）重建后全绿。这是环境搭建问题，不是代码问题，
> 记在这里是因为它就是记忆里那条「Linux CI 父链坑」的同一种表现。

---

## 3. must-fix

### MF-1 `paper_broker` 台账没有 `probe()`，「存在但不合法仍失败关闭」对它不成立

**事实**。`runtime_builder_strategy.py:346-363` 里，台账的 `DeferredPeerArtifact` 是直接写在
`StrategyRunnerStore(...)` 的参数里的，构造完就被 `_DeferredLifecycleFeatureSource` 包住，
**没有任何地方调用它的 `probe()`**——对比 feature spool（`:379` 有 `feature_spool.probe()`）
与 signal bus（`:434` 有 `deferred_bus.probe()`）。

**我自己造坏文件验过**（临时用例，跑完已删）：把 `broker.sqlite3` 写成
`b"this is not a paper broker ledger"`，再调用 `strategy_live_builder(...)(manifest)`，
**构造成功，不抛异常**。

**这是相对 base 的放松**。`a0bbb4c` 上台账是 `PaperBrokerLifecycleReader(path, account_id=…)`
急切构造，而这个构造器本身就做全量校验（`strategy_paper_lifecycle.py:108-160`：
`_validate_file()` 拒非普通文件与符号链接，然后连库检查九张表、`schema_version == 5`、
一大串必需列）。也就是说：**改动前**台账坏了 → 启动即拒；**改动后**启动通过，要等到某个
候选进入 ARMED/HOLDING、真的去解生命周期特征时才在
`strategy_runner.py:2428` 抛出来（该处无 try/except，异常会一路传到 step，
记成一次迭代失败）。整晚空闲的机器上，这个错误一次也不会出现。

**报告与 CHANGELOG 的说法与代码不符**：报告 §2.2 的表把台账列为「三处对端制品改为延迟打开」
之一，正文写「路径上有东西就当场打开、当场全量校验」；CHANGELOG 写「在构建 step 时把已经在盘上的
制品照旧打开、照旧全量校验（存在但不合法仍然拒绝启动，一条校验都没放宽）」。

**补 `probe()` 不会引入新的启动耦合，我验过**：把台账（连同 `-wal`/`-shm`）放在一个用
`readonly_runtime` 全部设为只读的目录里，`PaperBrokerLifecycleReader` 无论 broker 进程还活着
还是已经关闭都能打开成功，`violations == []`（一次写都没有）。所以「加 probe 会不会让
strategy 反过来卡在 broker 上」这个担心不成立——台账不存在时 `probe()` 本来就是空操作。

**建议改法（二选一）**：
- 把台账的 `DeferredPeerArtifact` 提出来命名、在 `StrategyRunnerStore` 之后调用一次
  `.probe()`，并补一条「台账在场但不是台账 ⇒ 构造期拒」的单测；或者
- 如果确实要对这一类故意延后（例如担心 broker 崩溃后 `-wal` 在、`-shm` 不在的边角情形），
  就在代码注释、报告和 CHANGELOG 里明确写成「四类里三类构造期校验，台账按使用点校验」，
  并补一条用例把「延后」这个选择本身钉住。

### MF-2 e2e 的 world 只有一代 bundle，不是「两代」

**事实**。`_route_a_world`（`test_route_a_legacy_binding_e2e.py:360-371`）只调用一次
`_production_bundle(...)`，并且带着 `schema_bootstrap_reason`（只有装进空根的第一代才允许带）。
我在夹具里打点数过：`<runtime_root>/generations/` 只有 1 个 64 hex 目录，
`<authority>/generations/` 也只有 1 个。仓库里真正装两代的写法在
`tests/integration/test_route_a_schema_rollout_sandbox_e2e.py:174-188`
（先装一代，再用 `schema_bootstrap_reason=None` 装第二代）和
`test_route_a_rollout_acknowledge_e2e.py:461-466`。

**文字与事实不符的位置**：`tests/integration/test_route_a_live_chain_idle_e2e.py` 第 19 行
docstring「a real two-generation bundle」，以及报告 §3.3。

**影响判断**：本包要证的性质（构造期建序、延迟打开、沙箱越界）与代际无关，所以我不认为一代
就让 e2e 失去价值；但简报改法 3 白纸黑字写的是「真实两代 bundle」，而且第三窗口的现场本来
就是第二代装在第一代之上——验收文件自己的 docstring 说错了代际，下一个窗口的人会被误导。

**建议改法（二选一）**：照 `test_route_a_schema_rollout_sandbox_e2e.py` 的写法在
`cold_chain` 里装第二代（约十行）；或者把 docstring 与报告改成事实（一代 legacy 根 + 一代
权威链），并由协调者确认降级可接受。

---

## 4. should-fix

### SF-1 `exists` 对 `OSError` 的分类没有任何用例钉住

自补变异 S2 把 `runtime_peer_artifacts.py` 的

```python
        except FileNotFoundError:
            return False
        except OSError:
            return True
```

改成 `except OSError: return False`，**122 条全绿**。也就是说模块 docstring 与 `exists`
docstring 里那条很关键的规则——「只有 `FileNotFoundError` 表示拥有者还没写；父段不是目录、
符号链接成环、沙箱不让穿过的目录都算在场，交给打开器判」——**没有任何测试保护**。

这条规则本身是对的，我单独验过：ELOOP（`a -> b -> a`）与 ENOTDIR（父段是普通文件）两种路径，
`probe()` 都会走到打开器并抛出打开器的异常，而不是当成「还没建」。既有的
`test_a_dangling_symlink_is_present_and_the_opener_judges_it` 打不到这个分支，因为悬空符号链接
`lstat` 是成功的。

**建议**：补一条五行用例（造一个符号链接环或把父段做成普通文件，断言 `probe()` 抛的是打开器
的异常而不是返回 `None`）。它守的正是「present but wrong 仍然失败关闭」这条本包的核心边界。

### SF-2 等待没有上限、没有退避，可观测性只有心跳

`run_service_loop` 没有连续失败阈值，失败只记不退。这是本包想要的（不再触发 `OnFailure`），
但代价是：一个整晚等不到对端的 role 在面板上只是 DEGRADED，**没有任何东西会主动告诉人**。
可用的信号是心跳里的 `last_error`（点名在等哪份文件）与 `consecutive_failures` 单调增长。

**建议**：C-3 的三条探针就是这个「超时」，runbook 里应当把它写成判据而不是提示；另外建议
协调者单开一条 issue，讨论「同一份对端制品等待超过 N 分钟」是否要有告警，否则本包把
「告警风暴」换成了「静默」。

### SF-3 router 在「路由策略缺失」这条拒绝之前就建出了自己的制品

`bus = settings.open_store()` / `SignalRouteSpool` / `SignalRouteCursorStore` 现在移到了
`if settings.routing_policy_path is None: raise ValueError("default signal router authority is
unavailable")` **之前**。`has_manifest_authority` 那道门仍在最前面，所以只影响这一条。
后果很轻（写的都是自己的目录，而且这些制品本来就是幂等创建），但一个注定要拒绝启动的 role
会先在盘上留下三样东西。

**建议**：要么把 `routing_policy_path` 的检查提到建制品之前，要么加一行注释说明这是有意的
（「即使本 role 起不来，也要把整条链在等的 bus 留下」——这其实与
`test_the_signal_bus_exists_even_when_a_runner_source_refuses` 的意图一致，写清楚即可）。

---

## 5. note

- **N-1 游标搬家会遗弃旧游标，但重放是安全的**。消费者游标从 `live/features/cursors/` 换到
  `live/strategies/<svc>/feature-cursors/`，旧位置的游标文件不会被读到。丢游标的后果是从
  sequence -1 重新走一遍，而 `strategy_live_service.py:161` 先调 `runner.replay_source_batch`，
  已处理过的批次会被识别成重放而不是重新产信号，所以不会重复发信号。第三窗口三个 strategy
  都死在构造期，理论上不可能留下游标；上线前在主机上 `ls
  /home/lighthouse/rquant/data/runtime/live/features/cursors` 确认为空即可。
- **N-2 `ruff format` 有新漂移**。`src/rquant/runtime_builder_strategy.py` 与
  `tests/unit/test_runtime_builder_strategy.py` 在 base 上是 format 干净的，现在
  `ruff format --check` 会要求重排（`tests/integration/test_route_a_strategy_chain_e2e.py`
  在 base 上就不干净）。CI 的 `scripts/check-core-quality.sh` 只 lint 一份固定的研究/交易
  文件清单，也不跑 `ruff format`，所以不影响 CI，只记录。
- **N-3 沙箱的覆盖范围**。`readonly_runtime` 只保护 runtime root 之下，而真实的
  `ProtectSystem=strict` 是整个文件系统只读；通过 `dir_fd` 的写也绕得过 wrapper。这两点
  helper 自己的 docstring 都写明了，并且用 `tree_state` 的终态比对兜住第二点。判断：这是
  合理的近似，不算缺陷。
- **N-4 `mkdir` 遇已存在目录不算越界，这条豁免是对的**。真实内核对已存在目录先答 `EEXIST`，
  `Path.mkdir(exist_ok=True)` 会吞掉；否则第三窗口的 strategy 根本走不到锁文件那一步。
  推理写在 `refuse()` 里，我核对过，成立。
- **N-5 notifier 那条 DEGRADED 的措辞**。报告写「依赖 `serving/page-control` 与操作库」。
  我实测到的 `last_error` 只有一条：`FileNotFoundError: [Errno 2] No such file or directory:
  '…/source/external/rquant_ro.duckdb'`。夹具占位路径这一点报告说对了，但「serving/page-control」
  是推断不是观测，写进 runbook 时应当降级成「取决于 serving 面状态」。

---

## 6. 第 1 / 2 条的判断

### 6.1 只读消费者不加锁读 spool，有没有撕裂或不一致的风险

**没有引入新风险，而且这不是本包才开始的。** 三条理由，都在代码里可核：

1. **读路径本来就不加锁**。`current()` / `list_after()` / `read_payload()` / `read_result()`
   在 `a0bbb4c` 上就一把锁都不取。消费者过去唯一会碰生产者那把锁的地方是
   `commit_cursor`——因为游标根默认落在生产者根内，`_cursor_lock_path` 就等于生产者的
   `.feature-spool.lock`——以及 `_initialize_source_identity`。这两处都不是「读批次时的互斥」。
2. **写者是原子的**。`_atomic_write` 是同目录 `mkstemp` → `fchmod(0o600)` → 写 → `fsync` →
   `os.replace` → 再 `fsync` 父目录；`publish` 全程持生产者独占锁，落盘顺序是
   **payload → manifest → session segment → current.json**。读者以 `current.json` 为闸门，
   `list_after` 只收 `sequence <= current.sequence` 的 manifest，多出来的新 manifest 被过滤掉；
   manifest 在盘上就意味着它的 payload 已经先落盘。所以读者看到的要么是旧状态、要么是新状态，
   不存在半条批次。
3. **读到的内容还要再自证一次**。`read_payload` 会重算 sha256 与「必须是 canonical JSON」，
   `list_after` 会检查序号连续、`current()` 会检查 `source_generation_id` 与构造时读到的
   身份一致。即便真出现撕裂，也是失败关闭而不是读出脏数据。

游标锁域从「生产者锁」缩到「消费者自己的 `.cursor.lock`」也不引入不一致：`commit_cursor` 校验
时读的那份 manifest 是**不可变**的（`publish` 对同一 sequence 写入不同内容会直接拒绝），
游标文件按 consumer_id 哈希命名、每个实例一份，同一目录内的并发仍由 `.cursor.lock` 串行。

**失败关闭没有被放宽**：只读构造仍然要求生产者根、`batches/`、`sessions/` 三个目录存在、
属主是自己、mode 严格等于 0700（`_ensure_private_directories` 只读分支不再 `chmod` 修复，
而是直接抛 `FeatureSpoolIntegrityError`），并且要求 `source-identity.json` 在场；
`publish` / `publish_session_close_marker` 对只读实例直接拒绝。M4、M5 与
`test_a_feature_spool_that_is_present_and_unsafe_still_fails_closed` 是证据，我都复跑过。

### 6.2 「存在但不合法仍失败关闭」是否对每一类 peer 都成立

我按四类逐一造坏文件验证：

| # | 制品 | 读者 | 构造期是否 `probe()` | 我造的坏文件 | 结果 |
|---|---|---|---|---|---|
| 1 | feature spool | `strategy_live` | ✅ | 生产者根 `chmod 0755` | 构造期抛 `FeatureSpoolIntegrityError: unsafe read-only …`，且此时 `runner.sqlite3` 已经留下 |
| 2 | signal bus | `strategy_live` | ✅ | bus 路径换成指向别处的符号链接 | 构造期抛 `ValueError`（`symlink`），不是 `PeerArtifactUnavailableError` |
| 3 | **paper broker 台账** | `strategy_live` | ❌ **没有** | 台账写成垃圾字节 | **构造期通过，不抛异常**（MF-1） |
| 4 | runner 源（每一份） | `signal_router` | ✅ | 写成非 sqlite 字节 / 换成符号链接 | 两种都在构造期抛 `ValueError`，且 bus 与 spool 已经先建好 |

另外，第 3 类延后之后的表现我也核了：台账不存在时 `get()` 抛的
`PeerArtifactUnavailableError` 会一路传到 step（`strategy_runner.py:2428` 的调用点没有
try/except），记成一次迭代失败，**不会**被降级成「特征缺失」悄悄放过。所以第 3 类不是
「校验消失了」，而是「校验从启动挪到了使用点」——只是这个使用点在盘外空闲时永远不会到达，
而且报告和 CHANGELOG 没有说这件事。

**等待有没有上限、退避与可观测性**：没有上限，没有额外退避（只有服务循环固定的
`interval_seconds`）。可观测的只有心跳：状态 DEGRADED、`last_error` 点名到具体文件的完整路径、
`consecutive_failures` 单调增长；退出码 0，systemd 不重启也不推告警。见 SF-2。

---

## 7. C-3 / C-4 的核对

**报告给的最终顺序我认为准确，并且我复跑验证了它的每一个前提。**

```
① rquant-runtime-health
② serving
③ feature_live        ← 软依赖
   探针 0：test -f $ROOT/live/features/source-identity.json
④ strategy_live × 3   ← 软依赖
   探针 1：[ "$(ls -1 $ROOT/live/strategies/*/runner.sqlite3 2>/dev/null | wc -l)" -eq 3 ]
⑤ signal_router       ← 软依赖
   探针 2：test -f $ROOT/live/signal-bus/spool/source.json
⑥ paper_broker → notifier
⑦ 其余 unit
ROOT=/home/lighthouse/rquant/data/runtime
```

- 三条探针的路径我对着生产画像生成器核过：`live/features`
  （`runtime_production_profile.py:963`）、`live/strategies/<svc-…>/runner.sqlite3`
  （`:792-798`）、`live/signal-bus/spool`（`:899`、`:1451`）——都对。
- **③ 是软依赖这条成立，而且比报告说得更明确**：我实测到，feature spool 已初始化但零批次时，
  strategy 的心跳是 `total_successes=1 / total_failures=0 / last_error=None`（RUNNING）；
  只有 feature role 从没跑过（连 `source-identity.json` 都没有）时才是
  `total_failures=1 / last_error` 含 `feature spool`（DEGRADED）。所以把 feature 排前面的
  作用确实只是「让 strategy 一上来就是 RUNNING」。
- **两个预期 DEGRADED 我逐字复现了**：
  - `paper_broker`：`PaperExecutionConstraintUnavailableError: current pointer is unavailable`
    ——准确。
  - `notifier`：`FileNotFoundError: [Errno 2] No such file or directory:
    '…/source/external/rquant_ro.duckdb'`——夹具占位路径这点准确；「serving/page-control」
    那半句是推断，见 N-5。
- **C-4 判据改成 active 是对的**：strategy 不再需要先失败一轮，`runner.sqlite3` 在构造期就建
  （M2 钉住），router 缺 runner 时在循环里等而不是退出（M3 钉住），两个方向都能起（e2e 的
  `test_the_router_started_first_creates_the_bus_and_waits_by_name` 钉住）。
- **DEPLOY.md 还没改，本包也不该改**。合入时要动的地方我找出来了：
  - `DEPLOY.md:613` 第 28 条整条（「第一轮 strategy 失败是这条链的一部分」+ 四步 + 互等叙述）
    要重写成上面的顺序与探针；
  - `DEPLOY.md:396` 那段服务状态口径里关于 `rquant-runtime-strategy@` 的措辞要落到「active /
    持续运行」；
  - `DEPLOY.md:1294` 出现过一次 `rquant-runtime-strategy@ failed` 的历史描述，属于当时的现场
    记录，改不改由协调者定，但不要与新判据混读。

---

## 8. 集成输入（给协调者）

1. **CI 分片清单要重生成**。我核对了 `tests/manifests/full-suite-v1/` 的实际数字，与报告一致：
   清单里 `test_runtime_builder_strategy.py` 36、`test_runtime_builder_signal.py` 42、
   `test_feature_spool.py` 16、`test_route_a_strategy_chain_e2e.py` 6，五个 shard 合计
   **14070**。本包之后这四个文件是 43 / 48 / 17 / 6，另加新文件
   `tests/unit/test_runtime_peer_artifacts.py` 9 与
   `tests/integration/test_route_a_live_chain_idle_e2e.py` 5，**净增 28，14070 → 14098**。
   另有一条改名：旧 nodeid
   `tests/integration/test_route_a_strategy_chain_e2e.py::test_a_strategy_role_without_the_signal_bus_still_creates_its_runner_database`
   现在躺在 `shard-2.jsonl` 第 19 行，必须由 `scripts/full_suite_shards.py generate` 重生成，
   不要手改 nodeid 或 digest。
2. **CHANGELOG 已在 `ea1400f` 里**，不需要另外补；但它写着「已经在盘上的制品照旧打开、照旧
   全量校验」，**必须跟 MF-1 一起改**，否则合入的是一条不准确的记录。
3. **DEPLOY.md / runbook 的 C-3、C-4 要点**：见 §7 的三个行号。
4. **R07 差分门必须重冻结**。我逐个比对了 `tests/fixtures/r07_differential_gate/policy-v1.json`
   里 9 份 `source_file_snapshots` 的 sha256：在 `a0bbb4c` 上 **9 份全部匹配**（门是绿的），
   在本分支 HEAD 上 **恰好两份漂移**——`src/rquant/runtime_builder_signal.py` 与
   `src/rquant/runtime_builder_strategy.py`，其余 7 份不变，新增的
   `src/rquant/runtime_peer_artifacts.py` 不在这 9 份里。实际后果比报告说的更大：
   `tests/unit/test_signal_family_differential_gate.py` 现在是 **7 failed + 8 errors**
   （不止报告点名的那一条，因为多条用例都要过
   `verifier(ROOT, _head(), policy.source_file_snapshots)`）。生成器是
   `scripts/r07_policy_regenerate.py`（带 `--check`）。
5. **与包 I 的合版**：`git merge-tree --write-tree HEAD 76f7015` 只有一处冲突，
   **`CHANGELOG.md`**（双方都往 `[Unreleased] / Fixed` 里加条目），源码零冲突——包 I 动的是
   `runtime_capabilities.py` 与凭据相关测试，与本包不相交。
6. **本包已知会红的 CI 项**（合入前必须由协调者补齐）：分片清单 digest、R07 差分门。
   除此之外我跑过的 366 条在 3.11 / 3.12 两版本都是绿的。
