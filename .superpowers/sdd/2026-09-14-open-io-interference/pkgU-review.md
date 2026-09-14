# 包 U 独立复核：开盘时段 I/O 干扰与停机推送（#268）

复核对象：worktree `/Users/roxor/brain/30-projects/rQuant/.worktrees/ra-u-cc`，分支
`cc/20260914-open-io-interference`，HEAD `9f472e2f`，base `4e76850`（v0.33.11），**9 条 commit**
（实施报告 §8 写的是 8 条，少算了报告自身那一条 `9f472e2f`）。

我没有改分支、没有 push、没有重冻结、没有重生成 manifest。所有探针、变异工作副本、
stage 目录、容器日志都在 `/Users/roxor/.claude/jobs/67487964/tmp/rev-u/`（0700），
变异跑在 `rev-u/mut/`（`git archive HEAD` 的独立副本，用 `PYTHONPATH` 覆盖 `.venv` 的
`rquant.pth`），worktree 全程 `git status --porcelain` 为空。

---

## 0. 结论

**有条件通过（conditional pass）**，2 条 must-fix、8 条 should-fix。

裁决 30 的第 1、2 条做到了，证据我自己复现了一遍；第 3 条不做的理由成立，但报告给出的
「一列都省不掉」这句结论是错的，**存在一个语义完全不变的收窄**（SF-7），值得协调者另开一包。
第 4 条边界守得住：`deploy/` 一字未动，已发布模型未动，无 skip/xfail。

四条 lane 的数字、12 条变异、收集增量 `missing=1 extra=46`、快照门 4 条，**逐项与报告一致**。
实施者记的那条 Linux 3.12 间歇失败我**也没有复现**（1 次全 lane + 10 轮整文件 + 40 轮单条带压）。

must-fix 两条都不是正确性缺陷，是「写在 DEPLOY.md 里让 owner 照着验收的东西，实际不会出现」
和「一个不该进 main 的文件进了 commit」，各一行的修法。

---

## 1. 我自己跑出来的数字

### 1.1 四条 lane（同一份 22 文件定向集，667 条）

镜像用包 L 配方自建：Dockerfile 与实施者的**逐字节相同**，构建出的 image id 也相同
（`a1ab2e1917af` / `d9d0c5fa76b7`）。stage 由 `git archive HEAD | tar -x` 生成，`:ro` 挂 `/work`，
容器内 `cp -a` 到 `/rq/src`，uid 1000，`TMPDIR=/rq/tmp`（0700），`/home/lighthouse` 属 1000，
**worktree 从未挂进容器**，配置走 `export`（worktree 里没有 `.env`），`PYTHONDONTWRITEBYTECODE=1`，
`pytest -q -p no:cacheprovider`。本机两条与容器两条各自并跑一条（与实施者相同的并发形状）。

| 环境 | Python / SQLite / DuckDB | 我的结果 | 耗时 | 报告 |
|---|---|---|---|---|
| 本机 macOS `.venv` | 3.11.15 / 3.50.4 / 1.5.2 | **667 passed** | 853.94 s | 667 passed ✓ |
| 本机 macOS `.venv312` | 3.12.13 / 3.50.4 / 1.5.2 | **667 passed, 1 warning** | 842.21 s | 667 passed ✓ |
| Docker `python:3.11-slim`, uid 1000 | 3.11.16 / 3.46.1 / 1.5.2 | **666 passed, 1 deselected** | 925.50 s | 666+1 ✓ |
| Docker `python:3.12-slim`, uid 1000 | 3.12.14 / 3.46.1 / 1.5.2 | **666 passed, 1 deselected, 1 warning** | 946.87 s | 首轮 1 failed，复跑两次绿 |

deselect 的是包 O / 包 Q 记过的 base 红
`tests/unit/test_runtime_builder_candidate.py::test_default_candidate_loader_rejects_change_while_reading[content]`。
那条 1 warning 是 3.12 的 `os.fork()` DeprecationWarning，两边一致。

**闸门**：`tests/unit/test_runtime_schema_release_snapshot.py` 在 HEAD 与 base 上各 **4 passed**
（HEAD 2.20 s / base 1.32 s）。

**lint**：`ruff check` 改动的 15 个 `.py`——**All checks passed!**

### 1.2 §6.2 那条间歇失败：我这边一次都没出现

`tests/unit/test_serving_page_projection_source.py::test_a_source_touched_during_the_copy_is_refused_rather_than_served`

| 我做了什么（全在 `python:3.12-slim` 容器里） | 结果 |
|---|---|
| 整条 lane 跑一遍（与 macOS 3.12 lane 并跑） | **666 passed, 1 deselected** |
| 整个文件连跑 10 轮 | **10 轮全 84 passed**（9.0–10.9 s/轮） |
| 只跑这一条 nodeid 连跑 40 轮，容器里同时开四个忙循环压着 | **40 轮 0 failed** |

**机理判断**：这条用例用 `copy_then_touch` 把同样的字节重写回去，靠
`_copy_identity`（`_file_identity` + `st_ctime_ns`，`serving_page_projection_source.py:198`）
发现「被碰过」。它依赖的是文件系统时间戳粒度，不依赖本包任何一行。
**本包确实到不了那条路径**，实施者的隔离论证我核过了：拒绝抛在
`_connect_through_copy()` 里（`serving_page_projection_source.py:482-484`），而登记连接那一行在
`_connect_generation()` **返回之后**（`:362`），`_release()` 的新块在 token 为 −1 时是空操作。
按「未复现的间歇失败，不归本包」记，我同意这个口径；集成者若在 CI 上再看到，按本节与报告
§6.2 的复跑记录一起判，再决定开不开 issue。**报告自己记下的操作失误（那一轮容器输出被
`tail -6` 掉了）是对的，下一个包在容器里跑 lane 请留全量输出。**

### 1.3 收集增量：我自己收了一遍 base 与 HEAD

同一台机器、同一个解释器、同一套 `export`，base 用 `git archive 4e76850` 的独立 stage：

```
base 14486 / 14505（19 deselected）     HEAD 14531 / 14550（19 deselected）     净 +45
missing = 1     extra = 46
```

消失的**唯一**一条：
`tests/integration/test_route_a_readside_io_cost_e2e.py::test_an_atomic_replacement_costs_exactly_one_more_read`
（被 `test_an_atomic_replacement_inside_the_floor_costs_no_further_read` 取代）。
逐文件增量与报告 §9 的表**逐格相同**（gate +14、control +6、read_interrupt +16、
本包 e2e +3、auction_universe +2、reference_slow +2、serving_page +2、包 Q e2e −1+1）。
集成者重生成 manifest 时的预期就是 **`missing=1 extra=46`**，不是「+45」。

---

## 2. 第 1 条（停机可中断）：证据我全部复现了

### 2.1 §1.1 那张表：复现，且修正一处口径（SF-3）

本机 macOS 3.11.15 / duckdb 1.5.2 / sqlite 3.50.4，信号在查询开始后 0.4 秒发出，各三遍
（脚本 `rev-u/probes/handler_timing.py`、`sqlite_timing.py`）：

| 引擎 | 查询/语句耗时 | Python 处理函数跑在第几秒 |
|---|---|---|
| duckdb 1.5.2 | 0.72 / 0.76 / 0.77 s | **0.41 / 0.41 / 0.41 s**——查询进行中 |
| CPython `sqlite3` | 4.67 / 4.66 / 4.74 s | **4.67 / 4.66 / 4.74 s**——语句结束那一刻 |

**结论与实施者一致**：DuckDB 查询期间处理函数会跑，SQLite 语句期间不跑。所以 09-14 的成因
不是「信号没送到」，而是没有任何地方叫引擎放弃——这条叙述是对的，CHANGELOG 与
commit `5d48fe68` 的改写也是对的。

**但有一处口径写错了（SF-3）**：`src/rquant/runtime_read_interrupt.py:35` 写
「`within about a second (measured: 0.4 s from the signal)`」。0.4 秒是**从查询开始算**的
（信号就是那时发的），**从信号算是 0.00–0.01 秒**。我实测
（`rev-u/probes/interrupt_probe.py`，信号在第 1.00 秒发出）：

```
DUCKDB: raised InterruptException after 1.00s  delta=0.00s  is_read_interrupt=True
SQLITE: raised OperationalError: interrupted after 1.01s  delta=0.01s  is_read_interrupt=True
IDLE-INTERRUPT: next query ran to completion in 0.37s -> [(28571429,)]
```

三条实测全部站得住：`interrupt()` 中断进行中的 DuckDB 查询、watcher 中断进行中的 SQLite 语句、
**空闲时 `interrupt()` 是空操作**（所以 `register()` 在停机后直接拒绝新读是对的设计）。
这处口径是本包**专门为了把事实说对**才返工的那段（commit `5d48fe68`），把这一句也改对。

### 2.2 四条读路径，我逐条发真信号验过

`rev-u/probes/paths_probe.py`，生产形状的处理函数（只置事件，不请求中断）+ `StopSignalWatcher`，
信号在第 1.00 秒发出：

| 路径 | 结果 |
|---|---|
| A. notifier 的 `_StableReadonlyDuckDB`（`serving_page_projection_source.py:362` 登记） | `InterruptException` @ **+0.01 s**，`is_read_interrupt=True`，`open_reads` 归零 |
| B. reference-slow 的 `_verified_database_read`（`reference_slow_source.py:472`） | `InterruptException` @ **+0.01 s**，同上 |
| C. notifier 的 PageControl SQLite 审计（`serving_page_projection_source.py:656-658`） | `OperationalError: interrupted` @ **+0.01 s**，同上 |
| D. 停机之后才开始的读 | `ReadInterruptedError`「a database read may not start after a stop has been requested」@ +0.01 s |

auction-universe 与 auction-gap 两条由变异 M7 / M5 从反面钉住（见 §5）。

**线程与 fd 无泄漏**：`StopSignalWatcher` 进出 300 轮（`rev-u/probes/leak_probe.py`），
fd 稳定在 11、线程稳定在 1、`open_reads` 恒 0、退出后 wakeup fd 归 −1。

**空闲时收到 SIGTERM 行为未变**：`run_service_loop` 的 `while not stop_event.is_set()` 与
`return control.stop(reason="loop completed")` 一行没动；新分支只在
`stop_event.is_set() and is_read_interrupt(error)` 同时成立时才走
（`runtime_service_control.py:971-981`）。两条反向用例把这条判断的两半都钉住了：
`test_an_interrupt_nobody_asked_for_is_still_a_failure`（没请求停机的中断仍记失败）与
`test_an_ordinary_failure_during_a_stop_is_still_recorded`（停机期间的普通失败仍记失败）。
这两条是本包测试里质量最高的部分。

### 2.3 覆盖清点：哪些读**没有**进注册表

简报要我逐条点清。读侧四个 role + serving/notifier 投影里，**进了注册表的**是五条
（四条 DuckDB + 一条 SQLite），**没进**的如下：

| 没覆盖的读 | 在哪 | 要紧吗 |
|---|---|---|
| `_connect_through_copy()` 的整文件字节拷贝 | `serving_page_projection_source.py:418`→`:503` | 在**登记之前**跑。Linux 生产走描述符分支，不暴露；macOS / 兜底分支暴露 |
| `_private_copy_of_generation()` 的整文件拷贝 | `reference_slow_source.py:453` | 同上，且另有 `monotonic_deadline` 与 limits 兜底 |
| lab_jobs role 的 `DuckDBStore(stable.generation_path, read_only=True)` | `serving_page_projection_source.py:1754` | 外层 `_StableReadonlyDuckDB` 进了注册表，**这个内层第二连接没有**，按候选数循环跑 research gate 查询 |
| `_ReadonlyPageControlAuditReader.snapshot()` 的绑定/校验与读后复验语句 | `:700-706`、`:733` | 都是极小的 `PRAGMA` / `SELECT 1 LIMIT 1`。另：`:751-757` 那个 `except (OSError, sqlite3.Error)` 构造 `PageProjectionSourceIntegrityError` **没有 `from exc`**，`is_read_interrupt` 的链式回溯到不了——目前无害（该连接根本没登记，不会被中断），但这类转换今后要带 `from` |
| `_validate_schema()` 在 `__init__` 里的那次连接 | `:637-648` | 极小 |

**还有一条口径要写给协调者**：#268 原文点名的是**七个** role
（paper-constraint、auction-match、daily-close、market-minute、reference-slow source + publisher、notifier），
裁决 30 第 1 条写的也是「**所有**打开 DuckDB/SQLite 做读的 role」。本包接上的是其中
**四个**（notifier、reference-slow source、auction_gap、auction-universe）。
我查过另外几个 role 的 step：它们卡住的是 spool / quota 的 SQLite 与文件 I/O，
**`interrupt()` 本来就够不着**（D 状态下的 `cp`、`os.read`、fsync 都不是可中断的语句）。
所以残留是真实的，但不是本包能修的；**请在报告里把这句话写出来**，别让读的人以为
09-14 那七个 role 现在都能秒停。owner 建议里的 `TimeoutStopSec ≥ 300` 正是为这半边留的兜底，
这一条我支持。

### 2.4 三条设计上的小洞（都是 SF / nit）

- **SF-2：`StopSignalWatcher` 必须有人先装 Python 处理函数，否则它什么都不做。**
  `set_wakeup_fd` 只有在该信号**装了 Python 处理函数**时，CPython 的 C 层处理函数才会写管道；
  否则 SIGTERM 走默认动作直接杀进程。我第一版探针就是这么死的（exit 143）。
  生产是对的（`runtime_service_main.py:778` 先装 handler，`:787` 再起 watcher），但
  `StopSignalWatcher` 的 docstring 只说「C 层处理函数立刻写」，没写这个前提，
  `__enter__` 也照样把 `active` 置 True（`:325`）。**今后任何别的调用者会静默失效**，
  而 `:789-792` 那条 warning 也不会响。把前提写进 docstring，或在 `__enter__` 里检查
  `signal.getsignal(signum)` 不是 `SIG_DFL`/`SIG_IGN`。
- **SF-4：watcher 里 `registry.request()` 排在 `on_stop()` 前面**（`runtime_read_interrupt.py:375-377`），
  而 `run_service_loop` 要求 `stop_event.is_set()` 才把中断读成停机。两行对调即可。
  实际触发概率极低（watcher 线程在这两句之间只有三条字节码，主线程要在这个缝里
  把异常一路抛到循环外），但没有理由留着。entrypoint 的处理函数顺序是对的
  （`runtime_service_main.py:764-771` 先 `stop_event.set()` 再 `request_read_interrupt()`）。
- **nit：登记之后、第一条语句之前的缝。** 一条连接登记后若停机到达，`interrupt()` 对空闲连接
  是空操作（§2.1 实测），**下一条语句会跑完**。notifier 一次读里有 `_require_tables` + 若干
  查询 + `minute_coverage` 聚合共用一条登记连接，最贵的那条排在最后，所以这个缝是
  Python 级的微秒量级。要彻底关掉，可以在 `request()` 之后让已登记连接的后续 `execute` 直接拒绝，
  或让 `request()` 保留 latch 后对新语句再补一次 `interrupt()`。

---

## 3. 第 2 条（换代读限频）：机制正确，默认值我基本认，但有一条会掩盖故障

### 3.1 时钟来源：确实是同一个对象，不是抄的字符串

`readside_replica_gate.py:34` `from rquant.runtime_market_session import MARKET_TIMEZONE`，
而 `runtime_market_session.py:31-32` 是 `MARKET_TIMEZONE = ZoneInfo("Asia/Shanghai")` /
`_SHANGHAI = MARKET_TIMEZONE`——`decide_market_session()`（`:284`，`may_fetch_market_minute`
就是它算出来的）用的 `_SHANGHAI` 与 gate 用的 `MARKET_TIMEZONE` 是**同一个对象**，
我用 `MARKET_TIMEZONE is _SHANGHAI` 验过为 True。裁决要求的「同一日历/时钟来源，import 不另抄」
满足。

`e0ebe6a0` 那条返工（`normalize_aware_utc` 之后再 `astimezone`）是对的且必要：
`normalize_aware_utc` 对 naive 直接 `raise ValueError`（`runtime_contracts.py:28-31`），
所以 gate 这条路不可能踩主机本地时区。**但 `suspends_reads_at` 本身仍然接受 naive**
并按主机时区回答（我实测 naive 09:30 在本机返回 True）——目前只有 gate 调它，
加一行 `normalize_aware_utc` 或断言会更稳，算 nit。

### 3.2 边界：我逐点打过（`rev-u/probes/window_probe.py`）

```
09:19:59.999999 本地 -> suspends=False
09:20:00.000000 本地 -> suspends=True
09:39:59.999999 本地 -> suspends=True
09:40:00.000000 本地 -> suspends=False        （半开区间 [09:20, 09:40)，与 __post_init__ 一致）
```
同样三个瞬时改用 UTC 表达，答案完全相同——主机时钟不参与。

**gate 层的连续行为**（notifier 画像 + 假时钟）：
```
09:10 冷启动      -> opened=True   skipped=False  loader 调用 1 次
09:15 新一代       -> opened=False  skipped=True   （15 min 间隔）
09:20 新一代       -> opened=False  skipped=True   （禁读时段）
09:39:59 新一代    -> opened=False  skipped=True
09:40:00           -> opened=True   skipped=False  loader 调用 2 次
```
**本地午夜翻日**：notifier 的 `key` 带 `cutoff.date()`（本地日期，
`serving_page_projection_source.py:1072`），23:59 与 00:01 是两个 key，
所以 00:01 会读（间隔只过了 2 分钟）——**换了问题一定重读**这条压过间隔，行为正确；
代价是每天本地 00:00 必有一次整表聚合，不在开盘窗口内，无害。
Asia/Shanghai 无 DST，无需考虑。

### 3.3 默认值：三条我认，一条的**理由**站不住（但结论无害）

我从源码核了三个 role 自己的窗口：
`reference_slow_source.py:140/1257/1342` 的 `> time(9, 25)` 上界、
`runtime_builder_candidate.py:71-72` 的 `09:26–09:30`、
`auction_universe_source.py:25-26` 的 `09:15–15:10` 保护窗口。

- **notifier 15 min + 09:20–09:40**：认。它是唯一全天每两秒跑、且那一次读是整表聚合的 role，
  #268 的量也全在它身上。
- **auction-universe 5 min 无窗口**：理由完全成立，而且比报告写的更强——
  `runtime_service_builtin.py:648-652` 在保护窗口里**根本不碰 gate 就 return 了**，
  禁读时段确实永远不触发。
- **auction_gap 4 min 无窗口**：认。它读的是前几个交易日的 `daily_bar` 成交量，开盘期间不变。
- **reference-slow「加禁读时段不是放慢是停掉」这句话过了**（SF-6 的一部分）。
  gate 的实现是 `_floor_blocks(now) **and** _reusable_across_generations(key, cutoff)`
  （`readside_replica_gate.py:499`），**没有答案的 role 永远不被拦**，而且
  **换了 key 一定重读**。reference-slow 在采集窗口里 key 是
  `("reference-slow-evidence", prior_trade_date, projection_as_of_date)`，
  窗口内不变；修订回看的五个日期各是新 key，也一定会读。
  所以给它加禁读时段的真实后果是「**窗口内第一次读拿到的那一代一直用到窗口结束**」，
  不是「停掉」。我用变异 RM4b 验了：**把 `DEFAULT_NO_READ_WINDOW` 加到
  `REFERENCE_SLOW_SOURCE_PROFILE` 上，reference-slow 的三个行为测试文件 + builtin 全绿**
  （唯一杀它的是 e2e 里那条断言常量的
  `test_the_floor_is_the_roles_own_window_and_the_notifier_carries_the_open_window`）。
  **结论（不给它窗口）我仍然支持**——只是理由应当写成「它本来就只在自己那 5 分钟里读，
  再加一层窗口不会少读几次，只会让第一代答案冻到 09:40」，而不是「会把它停掉」。

**另一句要写给协调者的口径**：按生产节拍算，`auction-universe` 的 5 min 间隔 ≈ 副本换代周期，
`auction_gap` 的 4 min 间隔 ≈ 它整个装配窗口长度——这两条在生产上**几乎是空操作**
（包 Q 的「变了才读」已经把它们压到每代一次）。**#268 能量到的收益全部在 notifier 身上。**
报告那张四行的画像表读起来像四个 role 各省了一笔，实际不是。这不影响装机，但影响
装机后拿什么指标判断「第 2 条起作用了」。

### 3.4 SF-1：间隔会把「副本不见了」也一起压住

`readside_replica_gate.py:499` 的分支在 `current is None`（`ReplicaGeneration.observe` 对
不存在 / 非普通文件 / 符号链接都返回 None）时**照样成立**。实测
（`rev-u/probes/missing_replica.py`）：

```
包 Q（无间隔）  : 抛 RuntimeError（loader 说「副本没了」），loader 被调用 2 次
包 U（15min 间隔）: opened=False  skipped_by_floor=True  value='answer1'  generation=None  loader 只调用 1 次
```

也就是说，**副本被删掉 / 变成非普通文件时，有缓存的 role 会把旧答案一直发到间隔结束，
心跳上报的是 `replica_skipped_by_floor=true`**——而 DEPLOY.md 恰恰告诉 owner
「notifier 上它经常是 `true`，这是预期而不是故障」。这个故障模式按设计不可见。

生产触发概率低（`sync-readonly-replica.sh` 用 `mv` 原子替换，名字始终在），
但 09-14 那种磁盘打满的场景下同步失败不是不可能。修法一行：

```python
if current is not None and self._floor_blocks(now) and self._reusable_across_generations(key, cutoff):
```

配一条用例（副本消失 ⇒ 仍然调 loader ⇒ 失败按失败记）。

### 3.5 心跳字段：**reference-slow 的成功路径根本没接**（MF-1）

报告 §2.1 写的是「`ReplicaRead.skipped_by_floor` → `iteration_skipped_by_floor()` →
**四个 builder 的 `_replica_cost()`** → `RuntimeStepResult` → 心跳文件模型」。
实际只有**三个**：

| role | 成功路径报 `replica_skipped_by_floor` | 失败路径（`_iteration_replica_floor`） |
|---|---|---|
| `notifier.admin.shadow.v1` | ✅ `runtime_builder_signal.py:1063-1069` | ✅ `:1207` |
| `candidate.auction_gap.v1` | ✅ `runtime_builder_candidate.py:536-540` | ✅ `:620` |
| `auction-universe.publisher.v1` | ✅ `runtime_service_builtin.py:628-633` | ✅ `:697` |
| `reference-slow.source.v1` | ❌ **`runtime_service_builtin.py:467-470` 只带两个字段** | ✅ `:479` |

reference-slow 没有 `_replica_cost()`，它的成功返回是
`result.model_copy(update={"replica_opened": opened, "replica_read_bytes": read_bytes})`。
后果：**它按 5 分钟间隔压住一代之后，心跳是
`replica_opened=false, replica_read_bytes=0, replica_skipped_by_floor=null`——
和「这一轮根本没问过 gate」以及「认出了同一代」完全分不开**，而这正是这个字段存在的理由。

而 DEPLOY.md 写给 owner 的验收句是「**四个**读侧 role 的心跳里出现新字段
`replica_skipped_by_floor`」。装机那天照着这句去看，reference-slow 上看不到。

我用两条变异证过这个洞是空的：
- **RM7（把缺的接线补上）= GREEN**：加上之后没有任何用例的行为改变——说明这条路一条用例都没有。
- **RM6（把 auction-universe 成功路径的这个字段删掉）= GREEN**：那个 role 的上报也没人钉。

修法：`runtime_service_builtin.py:467-470` 的 `update={...}` 补一个
`"replica_skipped_by_floor": replica_gate.iteration_skipped_by_floor()`，
再给 reference-slow 与 auction-universe 各补一条钉住它的用例。

### 3.6 陈旧度：没人算过最坏值，建议写进 DEPLOY

按发出去的画像算：notifier 09:19:59 读一次 → 15 分钟间隔在 09:34:59 到期，但禁读时段到
09:40:00 才放开 → **这一代答案要撑 20 分 1 秒**；加上它读的那一代副本本身最多已经 5 分钟旧，
**开盘期间页面上的 `minute_coverage.max_time` 最坏会落后约 25 分钟**。
如果再采纳 owner 建议里的「盘中副本同步 5 → 15 min」，最坏值变成**约 35 分钟**。
这正是用户最可能去看页面的时段。**这不是缺陷，是一个需要 owner 明确点头的取舍**，
请写进 DEPLOY.md 那一节。（我核过它不会破坏已发布投影的校验：
`serving_read_models.py:690-745` 只禁止「晚于 `available_at` 的证据」，旧答案不违反。）

---

## 4. 第 3 条：不做是对的；但「一列都省不掉」这句是错的（SF-7）

### 4.1 语义确实会变——我在自己的合成副本上复现了同样的数字

`rev-u/probes/coverage_narrowing.py`，400 代码 × 40 交易日 × 240 分钟 = 384 万行：

| | `rows_count` | `codes_count` | `trade_dates` | `min_time` |
|---|---|---|---|---|
| 现状（全量） | 3,840,000 | 400 | 40 | 2025-07-14 09:30 |
| 加当日下界 | **96,000** | 400 | **1** | **2025-08-22 09:30** |

与报告 §3.2 的表**逐格相同**。`MinuteCoverageProjectionRow` 是冻结投影，所以裁决 30 第 3 条
按字面做不了——**这个结论我完全支持**，`test_the_minute_coverage_projection_cannot_be_narrowed_to_the_current_trade_date`
这条用例也该留着。

跨代增量那条反驳我也核了：`storage/duckdb.py:2753-2760` 的 `upsert_minute_bars` 是
`INSERT OR REPLACE`，列清单里**没有** `created_at`，所以重写的行 `created_at` 会跳到缓存
cutoff 之后——缓存的前缀计数与增量会重复计一行，`COUNT(*)` 静默出错。**不做是对的。**

### 4.2 但存在一个**语义完全不变**的收窄，报告漏了

报告 §3.3 说「剩下的 43.8 MB 就是 `ts_code` + `trade_time` + `source` 三列，一列都省不掉」。
实际这条扫描碰的是**五列**。我用 `EXPLAIN ANALYZE` 看了扫描块里出现的列名
（`rev-u/probes/plan_probe.py`）：

```
[现状]         ['created_at', 'freq', 'source', 'trade_time', 'ts_code']
[去掉 created_at 谓词] ['freq', 'source', 'trade_time', 'ts_code']
```

**去掉 `created_at <= ?` 这个谓词，`created_at` 整列就不再被读**，而且三行发布值
（total 与两个 source 的 `rows_count` / `codes_count` / `trade_dates` / `min_time` / `max_time`）
**完全相同**（我逐行比对，`a == b` 为 True）。

这不是近似，是**条件恒等**：副本是一代一代整文件替换的，文件里每一行的 `created_at`
都早于文件自己的 `mtime`，所以只要 `cutoff >= generation.modified_at`，
`created_at <= cutoff` 就是恒真——而 **gate 已经在算这个条件了**
（`_reusable` 里的 `self._cutoff >= current.modified_at`，`readside_replica_gate.py:429`）。
做法是：读之前比一次 `cutoff` 与这一代的 `mtime`，成立就发不带 `created_at` 谓词的那条 SQL，
不成立（副本被打上未来时间戳这种异常）才带上。

**给协调者的建议**：这是裁决 30 第 3 条真正可做的那一半，值得另开一包量一遍
（用包 Q 的 `rchar` 方法，在 Linux 容器里跑 217 MB 合成副本）。五列里省掉一列的量级，
不是 5.4 倍，但它**一个已发布字段都不动**，不需要 schema rollout。
本包边界不允许改这里，**不作为本包的 must-fix**。

---

## 5. 变异表

变异跑在 `rev-u/mut/`（`git archive HEAD` 的独立副本），驱动脚本 `rev-u/mutate.py`，
`PYTHONDONTWRITEBYTECODE=1`，每条跑完无论结果都按字节还原并校验
（跑完后逐文件 sha256 与 worktree 比对，**全部一致**）。`rc != 0` = 变异被杀（RED）。

### 5.1 实施者那 12 条，我独立重跑：**12/12 结果一致**

| # | 改成什么 | 我的结果 | 杀它的那条 |
|---|---|---|---|
| M1 | watcher 听见信号但不叫引擎放弃 | **RED** | `test_a_signal_arriving_during_a_query_abandons_it_within_the_budget`：`the query ran to completion, so the signal never reached the engine` |
| M1b | entrypoint 处理函数只置事件（= #268 之前那个） | **GREEN（设计使然）** | 两条路对 DuckDB 故意冗余 |
| M1c | 两条路一起删 | **RED（240 s 超时）** | 09-14 那个缺陷本身 |
| M2 | 所有 role 的最短重读间隔归零 | **RED** | `assert 4 == 1` |
| M3 | 禁读时段永不生效 | **RED** | `TypeError: cannot unpack non-iterable NoneType`（见下，形态不好，我另补了 RM8） |
| M4 | `_minute_coverage` 真的加当日下界 | **RED** | `assert [] == ['all', 'tushare']` |
| M5 | 停机后仍允许开始新的读 | **RED** | `the read must not begin` |
| M6 | 被中断的一轮重新记成迭代失败 | **RED** | `'loop completed' != 'stop request...database read'` |
| M7 | auction 源把中断翻译成自己的完整性错误 | **RED** | `AuctionUniverseSourceError: daily snapshot q...` |
| M8 | 间隔生效但心跳不报 skip | **RED** | `[False, False, False] != [True, True, True]` |
| M9 | 跨代复用不比 `key` | **RED** | `assert 1 == 2` |
| M10 | 没有答案的 role 也被拦 | **RED** | `assert 0 == 1` |

**M1b 那条绿我同意**，理由与报告一致（两条路对 DuckDB 是故意冗余），而 M1c 把两条一起删
立刻红，所以两条都不是死代码。**报告把 M1b 和 M1c 一起报出来是正确的做法**，
只报 M1 + M1b 会让人以为其中一条多余。

**M3 的形态要记一笔**：那个变异把 `if window is None: return False` 改成 `is not None`，
于是 window 为 None 时落到 `start, end = window` 直接 `TypeError`——它是**崩掉**的，
不是「窗口不生效」被行为断言抓住。我补了形态正确的 RM8（在 `suspends_reads_at` 开头直接
`return False`），**也是 RED**，所以结论站得住，只是 M3 本身作为证据偏弱。

**M10 的清单里 `test_serving_page_projection_source.py` 不贡献**：我单独跑了 M10b
（同一变异只跑 serving 文件）= **GREEN**，杀它的全部来自 gate 文件。不是缺陷，是口径。

### 5.2 我自己加的 9 条

| # | 改成什么 | 结果 | 说明 |
|---|---|---|---|
| RM1 | **watcher 线程从不启动**（`active` 仍为 True） | **RED**（300 s 超时） | 线程是承重的，不是装饰 |
| RM2b | **notifier 的 DuckDB 读从不进注册表** | **RED** | `test_the_projection_read_is_abandonable_while_it_is_open`：`assert 0 == (0 + 1)` |
| RM3a | 间隔读**进程墙钟**而不是这个 role 注入的时钟 | **RED** | `test_a_generation_arriving_after_the_floor_is_read`：`assert 1 == 2` |
| RM3b | 间隔从**这一代的 mtime** 算，而不是从上次打开算 | **RED** | `test_a_clock_that_went_backwards_does_not_hold_an_answer_for_ever`：`assert 1 == 2` |
| RM4 | 把禁读时段加到 **reference-slow** 上 | **RED** | 但只被**断言常量**的那条 e2e 杀（见 RM4b） |
| RM4b | 同上，**只跑 reference-slow 的三个行为文件 + builtin** | **GREEN** | §3.3：「加窗口会停掉它」这句没有行为证据，实际后果是「第一代答案冻到 09:40」 |
| RM5 | **被间隔跳过的一代被当成读过的一代缓存** | **RED** | `test_the_no_read_window_holds_a_newer_generation_and_lets_go_at_its_end`：`assert False is True` |
| RM6 | **auction-universe 成功路径不再上报 skip** | **GREEN** | §3.5：那个 role 的上报没人钉 |
| RM7 | **给 reference-slow 成功路径补上 skip 上报** | **GREEN** | §3.5：这条接线根本不存在，补上也没人察觉 |
| RM8 | 禁读时段**形态正确地**失效 | **RED** | 补 M3 的证据 |

（RM2 第一轮跑在被我自己的还原 bug 污染过的树上，结果作废；RM2b 是在干净树上的重跑，
M4b / M10b 同理。还原 bug 是我的驱动脚本对「同一文件两处锚点」保存了两份快照，
实施者的 `mutate.py` 用的是单次读写，**没有这个问题**。）

---

## 6. 边界与工程卫生

`git diff --name-only 4e76850..HEAD` 共 23 条路径：

- ✅ **没有** `deploy/`（含 `systemd/` `nginx/` `frp/` `sudoers/`）、`.env*`、`.github/`、
  `tests/manifests/`、发布原语、stage、`runtime_authority*`、`runtime_exec_wrapper/`、
  `runtime_capabilities.py`、注册表 hash。
- ✅ `src/` 的 diff 里**没有任何一行** `schema_version`。
- ✅ 已发布模型未动：`RuntimeServiceHeartbeatProjection` 字段集不变
  （`test_the_replica_floor_is_a_file_field_and_reaches_no_published_payload` 钉住），
  `PAGE_PROJECTION_CONTRACTS` 不变，`MinuteCoverageProjectionRow` 不变。快照门 4 passed。
- ✅ 测试里**零条**新增 skip / xfail。
- ✅ 9 条 commit **全部**带 `Refs #268`、`Co-Authored-By: Claude Fable 5.1` 与 `Claude-Session:`。
- ✅ worktree 里没有 `.env`；`.venv312` 未入库；`git status` 干净。
- ✅ `MARKET_TIMEZONE` 是新增导出，`runtime_market_session` 没有 `__all__`，不构成 API 破坏；
  `_SHANGHAI` 别名保留，模块内三处用法未动。
- ❌ **MF-2：`.superpowers/sdd/2026-09-14-open-io-interference/pkgU-report.md` 进了 commit
  `9f472e2f`。** `origin/main` 上 `.superpowers/` 下**一个文件都没有**（`git ls-tree` 计数为 0），
  `.gitignore` 也没有忽略它；包 Q 的报告明确写过自己全程只是 `??`、没进任何 commit。
  合进去会在 main 里开这个先例。要么把这条 commit 摘掉（报告留在 worktree 里，与前面几包一致），
  要么由协调者明确认可这次约定变更——不要默认合。

**CHANGELOG / DEPLOY 用词**：两处都写到位了，尤其是
「成因不是『信号没送到』」这个改正（`3bafffec`）与新增的首个交易日判据
「09:25–09:40 生产 monitor 轮询无中断、`rquant-monitor-watchdog` 0 次超时」。
owner 的四条建议（副本同步 5→15 min、备份避开 09:20–09:40、`TimeoutStopSec ≥ 300`、vda 换 bfq）
都列在「本包不改 `deploy/`」之下，写法正确。两处要改：

- **SF-5**：DEPLOY 写「手工 `systemctl stop` 一个读侧 role ⇒ `stop_reason` 是
  `stop requested during a database read`」。装上第 2 条之后，notifier 绝大多数轮**根本不在读**
  （15 分钟才开一次库），手工停的时候**通常拿到的是 `stop_reason = "loop completed"`**。
  第 2 条把第 1 条的可观察特征变稀了。照现在这句去验收会误判。
  改成「几秒内停干净、`Result=success`、`status=stopped`；如果正好停在读里，
  `stop_reason` 会是 `stop requested during a database read`」。
- **MF-1 的连带**：「四个读侧 role 的心跳里出现新字段」这句在 reference-slow 上不成立（§3.5）。

---

## 7. 结论与清单

**有条件通过。** 第 1 条与第 2 条的机制是对的，证据我全部独立复现，测试写得扎实
（`test_an_interrupt_nobody_asked_for_is_still_a_failure` /
`test_an_ordinary_failure_during_a_stop_is_still_recorded` 这两条反向用例尤其好），
第 3 条不做的理由成立，边界干净，数字全对。

### must-fix（合入前）

- **MF-1** `runtime_service_builtin.py:467-470`：reference-slow 的成功路径没有上报
  `replica_skipped_by_floor`，与报告 §2.1「四个 builder」和 DEPLOY.md
  「四个读侧 role 的心跳里出现新字段」都不符。证据：RM7 GREEN（补上接线没人察觉）+ grep。
  补一行 + 一条用例；顺带按 RM6 GREEN 给 auction-universe 也补一条钉住它的用例。
- **MF-2** commit `9f472e2f`：包报告进了 git。`origin/main` 的 `.superpowers/` 是空的，
  包 Q 明确记过报告不入 commit。摘掉，或由协调者明确认可改约定。

### should-fix

- **SF-1** `readside_replica_gate.py:499`：间隔分支在 `current is None` 时也成立，
  **副本消失会被伪装成 `replica_skipped_by_floor=true`**（实测）。加 `current is not None`。
- **SF-2** `runtime_read_interrupt.py:259-327`：`StopSignalWatcher` 只有在该信号已装 Python
  处理函数时才起作用（我实测未装时进程直接被 SIGTERM 杀掉，exit 143），docstring 没写这个前提，
  `active` 照样置 True。写进 docstring 或在 `__enter__` 里检查。
- **SF-3** `runtime_read_interrupt.py:35`：「measured: 0.4 s from the signal」写错了，
  从信号算是 0.00–0.01 s（0.4 s 是从查询开始算）。这段正是本包为把事实说对才返工的地方。
- **SF-4** `runtime_read_interrupt.py:375-377`：`registry.request()` 与 `on_stop()` 两行对调，
  免得中断先于 `stop_event` 被循环看到。
- **SF-5** DEPLOY.md 的手工停机验收句：第 2 条使 notifier 大多数轮不在读，
  `stop_reason` 通常是 `loop completed`。改写。
- **SF-6** 报告 §2.1 / e2e 里「给 reference-slow 加禁读时段会**停掉**它」这句没有行为证据
  （RM4b GREEN）。真实后果是「窗口内第一代答案冻到 09:40」。结论仍然支持，把理由改对。
- **SF-7** 报告 §3.3「一列都省不掉」是错的：扫描碰**五列**，
  去掉 `created_at <= ?`（当 `cutoff >= 这一代的 mtime` 时恒真，gate 已经在算这个条件）
  会让 `created_at` 整列不被读，**发布值逐行相同**（我用 `EXPLAIN ANALYZE` + 值比对验过）。
  这是裁决 30 第 3 条真正能做的那一半，建议另开一包用包 Q 的 `rchar` 方法量一遍。
- **SF-8** `READ_INTERRUPTS` 是进程级 latch，只有 `test_runtime_read_interrupt.py` 有 autouse
  重置。把那条 fixture 提到 `tests/conftest.py`——否则今后任何一条 latch 了不重置的用例，
  会让同一个 worker 里后面所有 `register()` 抛 `ReadInterruptedError`。

### 写给协调者的口径（不必改代码）

1. #268 点名七个 role，本包接上四个；另外几个卡在 spool/文件 I/O 上，`interrupt()` 够不着。
   请在报告里写明这条残留，`TimeoutStopSec ≥ 300` 这条 owner 建议是为它留的。
2. 第 2 条能量到的收益全在 notifier；auction-universe 的 5 min 与 auction_gap 的 4 min
   在生产节拍上近似空操作。装机后判断「第 2 条起作用了」要看 notifier 的
   `replica_opened` 频次，不要指望另外三个。
3. notifier 页面 `minute_coverage` 在开盘期间最坏会旧约 **25 分钟**；若再把副本同步改成
   15 分钟，最坏约 **35 分钟**。这是取舍，请 owner 明确点头并写进 DEPLOY。
4. 报告 §8 的 commit 表少了一条（实际 9 条）。
5. 集成者重生成 manifest 的预期是 **`missing=1 extra=46`**（我独立收集确认）；
   回滚到 v0.33.11 或更早前，按 2026-09-10 那条整批挪心跳的步骤先把心跳挪开
   （新增文件字段 `replica_skipped_by_floor`）。

---

复核环境：macOS 26.3.0（Darwin 25.3.0）、Docker 28.3.2；
本机 `.venv` 3.11.15 / `.venv312` 3.12.13，duckdb 1.5.2、sqlite 3.50.4；
容器 `python:3.11-slim` 3.11.16 / `python:3.12-slim` 3.12.14，duckdb 1.5.2、sqlite 3.46.1。
探针与日志：`/Users/roxor/.claude/jobs/67487964/tmp/rev-u/`。
