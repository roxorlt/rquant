# 包 U 实施报告：开盘时段 I/O 干扰与停机推送（#268）

base `4e76850`（v0.33.11），分支 `cc/20260914-open-io-interference`，worktree
`/Users/roxor/brain/30-projects/rQuant/.worktrees/ra-u-cc`。**未 push、未打 tag、未重冻结
R07、未重生成 manifest。**

---

## 0. 一句话结论

裁决 30 的**第 1、2 条做了**，**第 3 条做不了并给了证据**（`minute_coverage` 发布的是**全量**
覆盖率摘要，加当日下界会把 `rows_count`、`trade_dates`、`min_time` 三个已发布字段全改掉；
它值 43,790,572 → 8,138,988 `rchar` 字节，但那是另一个答案，不是同一个答案的省法），
第 4 条按简报只写建议、`deploy/` 一字未动。

**过程中查出一件与简报假设不一致的硬事实，写在 §1.1**：Python 的信号处理函数**在 DuckDB
查询期间是会跑的**（实测 1.85 秒的查询里 0.40 秒就跑了），**在 SQLite 语句期间不会**
（20.5 秒的语句要到第 20.5 秒才跑）。所以 09-14 停不下来的原因不是「信号没送到」，
而是**没有任何地方叫引擎放弃**，而循环只在两轮之间看 `stop_event`。这改变了修法的叙述，
不改变修法本身：两条路径都去调 `interrupt()`。

---

## 1. 第 1 条：停机可中断（最高优先）

### 1.1 先纠正一条事实：信号处理函数到底跑没跑

简报与我自己第一版代码注释都写着「一个卡在 `DuckDBPyConnection.execute()` 里的 role
一条字节码都不执行，所以处理函数根本不跑」。**这条对 SQLite 成立，对 DuckDB 不成立。**
本机 macOS 3.11.15、钉住的 duckdb 1.5.2，信号在查询开始后 0.4 秒发出，各跑三遍：

| 引擎 | 查询耗时 | Python 处理函数跑在第几秒 |
|---|---|---|
| duckdb 1.5.2 | 1.89 / 1.85 / 1.81 s | **0.41 / 0.41 / 0.40 s**——查询进行中 |
| CPython `sqlite3` | 20.48 / 20.56 / 20.67 s | **20.48 / 20.56 / 20.67 s**——语句结束那一刻 |

脚本落在 `/Users/roxor/.claude/jobs/67487964/tmp/pkg-u/`（0700 私有目录，不入库）。

**所以 09-14 的真正成因**：处理函数跑了，它把 `stop_event` 置上了，
但 `run_service_loop` **只在两轮之间**看这个事件，而这一轮正卡在读里——
一个在十分钟扫描第 1 秒到达的停机信号，仍然要花掉十分钟。
`TimeoutStopSec=60` 在第 60 秒到期，`SIGKILL`，`Result=timeout`，`OnFailure` 推送一条。
**缺的是「叫引擎放弃」这个动作**，不是信号。

这条事实同时决定了代码里为什么有**两条**触发路径，而不是一条：

- **entrypoint 的信号处理函数**里加一行 `request_read_interrupt()`：DuckDB 查询期间它会跑，
  所以四个读副本的 role 靠这一行就能中断。
- **`StopSignalWatcher`（`signal.set_wakeup_fd` + 自己的线程）**：SQLite 语句期间处理函数
  不跑，长 SQLite 语句只能靠它中断；同时它也是 DuckDB 那条路的兜底——万一以后某个
  duckdb 版本不再让出 GIL 给待处理信号，这条仍然有效。

`set_wakeup_fd` 的字节是 **CPython 自己的 C 层处理函数**写的，在内核投递信号的那个线程里
**立刻**写，与主线程在不在跑字节码无关。watcher 线程用 `select` 读它。

### 1.2 改了什么

| 文件 | 改了什么 |
|---|---|
| `src/rquant/runtime_read_interrupt.py`（新增，377 行） | `ReadInterruptRegistry`（正在读的连接清单 + 一个开关）、`interruptible_read` 上下文、`StopSignalWatcher`（wakeup fd + 线程）、`is_read_interrupt`（认 `duckdb.InterruptException`、SQLite 的 `OperationalError: interrupted`、自己的 `ReadInterruptedError`，**并沿 `__cause__`/`__context__` 链走最多 8 层**）、`READ_INTERRUPT_STOP_REASON` |
| `src/rquant/runtime_service_main.py` | `request_stop` 加 `request_read_interrupt()`；handler 装上之后再开 `StopSignalWatcher`，装不上时打一条 warning 而不是静默降级 |
| `src/rquant/runtime_service_control.py` | `run_service_loop`：`stop_event` 已置且 `is_read_interrupt(error)` ⇒ **这一轮不记失败**，直接 `control.stop(reason=READ_INTERRUPT_STOP_REASON)` |
| `src/rquant/serving_page_projection_source.py` | `_StableReadonlyDuckDB` 在 `__enter__` 里登记连接、在 `_release()` 里注销；PageControl 审计那条**只读 SQLite** 也进 `interruptible_read` |
| `src/rquant/reference_slow_source.py` | `_verified_database_read` 的 `yield` 包进 `interruptible_read`；两处 `except duckdb.Error` 先放行中断 |
| `src/rquant/auction_universe_source.py` | `_query_codes` 同上 |
| `src/rquant/auction_gap_candidate_input.py` | `_query_daily_volume_rows` 同上 |

**为什么四处 `except duckdb.Error` 都要改**：`InterruptException` **是** `duckdb.Error`
的子类，不改的话运维自己发的 `systemctl stop` 会被这些 role 翻译成
`daily snapshot query failed` / `reference source database query failed` ——
一条它自己造成的「完整性失败」，会被记进失败计数、触发退避、留在 `stopped` 之前的最后一条心跳里。
`is_read_interrupt` 还会沿异常链走，所以本包没有逐条改到的路径也不会把中断误判成故障。

**空闲时 `interrupt()` 是空操作**（duckdb 1.5.2 实测：空闲调一次之后，下一条查询照常跑完）。
所以 `register()` 在停机已请求之后**直接拒绝**开始新的读（抛 `ReadInterruptedError`），
而不是让它跑完——跑完的那一个正是熬过 `TimeoutStopSec` 的那一个。

**被中断的一轮在 gate 里算「没读完」**：`ReplicaReadGate.read()` 原本就对抛异常的 loader
走 `forget()`，不缓存半个答案（包 Q 的 SF-7），本包不需要改这一条，e2e 也钉住了它。

### 1.3 e2e 证据

`tests/integration/test_route_a_open_io_interference_e2e.py::test_a_role_interrupted_mid_read_exits_in_time_with_code_zero`
跑在包 P 的世界里（两代真 bundle、真 stage + 发布的权威链、wrapper 自己派生的 argv 与子环境、
真的五分钟副本、主库被第二个 DuckDB 连接以写模式占着）：

- role 的连接被一个代理包住，代理在它**第一条真实查询之前**先跑一条
  `SELECT count(*) FROM range(400000000000) WHERE range % 7 = 0`——**这条查询不会自己结束**，
  所以「它退出了」只可能是中断到了；
- role 自己的读路径、它的 `duckdb.Error` 转换、gate、循环**一个字都没改**，只有查询被加长；
- 另一个线程等到 `READ_INTERRUPTS.open_reads` 非零才 `os.kill(os.getpid(), SIGTERM)`，
  并记下发信号的时刻；
- 断言：**`returned_at - sent_at < 5 s`**、`code == 0`、`ran < 4`（循环没有跑满）、
  心跳 `status=stopped`、`stop_reason == "stop requested during a database read"`、
  `total_failures == 0`、`last_error is None`。

单元侧还钉了两条独立的：`test_a_signal_arriving_during_a_query_abandons_it_within_the_budget`
（处理函数**只置事件、不请求中断**，所以能够中断的只剩 watcher）与
`test_only_the_watcher_can_abandon_a_long_sqlite_statement`（处理函数请求中断也没用，
因为它要到语句结束才跑——这一条是 watcher 存在的理由，写成用例）。

---

## 2. 第 2 条：换代读限频

### 2.1 改了什么

`ReplicaReadProfile`（`readside_replica_gate.py`）两个字段：

- `min_reread_interval`：两次**打开**之间的最短间隔；
- `no_read_window`：一段**按盘中时钟**算的禁读时段（半开区间 `[start, end)`），
  时钟用 `runtime_market_session.MARKET_TIMEZONE`——就是 `may_fetch_market_minute`
  用的那一个，**import 过来而不是另抄一份字符串**。

四个 role 的画像，以及为什么只有 notifier 带禁读时段：

| role | 最短重读间隔 | 禁读时段 | 为什么 |
|---|---|---|---|
| `notifier.admin.shadow.v1` | **15 min** | **09:20–09:40** | 唯一一个全天每两秒跑的，而且那一次读是 `minute_bar` 整表聚合（包 Q 量到 44,052,711 字节里 44,052,711 都是它） |
| `reference-slow.source.v1` | 5 min（= 它 09:20–09:25 的采集窗口） | 无 | 采集窗口**落在** 09:20–09:40 里面，加禁读时段不是放慢是停掉 |
| `candidate.auction_gap.v1` | 4 min（= 它 09:26–09:30 的装配窗口） | 无 | 同上；它读的是**前几个交易日**的 `daily_bar` 成交量，开盘期间不会变 |
| `auction-universe.publisher.v1` | 5 min（= 一代副本） | 无 | 它自己拒绝在 09:15–15:10 发布，禁读时段对它永远不触发 |

**「不 09:20–09:40 开任何读」这条要按 role 读，不能按字面读**：#268 原文同时要求
「reference-slow 只在它的采集窗口读」和「09:20–09:40 不开新读」，而那个采集窗口
（09:20–09:25）本身就在 09:20–09:40 里面——两条字面上打架。本包按**能工作的那个解释**做：
禁读时段只给全天跑的那一个 role，其余三个用等于自己窗口长度的间隔约束。

三条设计上的硬性质，各有用例：

1. **没有答案的 role 永远不被拦**。冷启动（含冷启动落在禁读时段里）必须读，
   否则这个 role 整段窗口没有任何可发布的东西，等于在原来那个故障上再造一个更长的。
   实现上是 `_floor_blocks(now) and _reusable_across_generations(key, cutoff)`——
   后者在没有缓存时为 False。
2. **换了问题一定重读**。`key` 是「这是不是同一个问题」的判据，跨代复用只在 `key` 相同时允许。
3. **时钟往回走不会永久扣住答案**。NTP 把刚开机的主机往回拨时 `elapsed < 0`，按「不拦」处理。

被扣住的那一代**不是悄悄忽略**：`ReplicaRead.skipped_by_floor` →
`ReplicaReadGate.iteration_skipped_by_floor()` → 四个 builder 的 `_replica_cost()` →
`RuntimeStepResult.replica_skipped_by_floor` → 心跳**文件模型**
`RuntimeServiceHeartbeat.replica_skipped_by_floor`。
**冻结的 `RuntimeServiceHeartbeatProjection` 一个字段都没加**，用例
`test_the_replica_floor_is_a_file_field_and_reaches_no_published_payload` 钉住。

失败轮走的是另一条路：`run_service_loop` 里新增 `_iteration_replica_floor(step)`，
形状与包 R 的 `_iteration_replica_cost` 一样——没有这个属性的 role 报 `None` 而不是编一个
`False`，探针自己抛异常也读成「说不上来」，绝不顶替正在记录的那个错误。

### 2.2 e2e 证据

`test_three_consecutive_replica_replacements_cost_one_read`：同一个包 P 的世界，
八轮里替换三次副本（第 2、4、6 轮之后各一次，断言 `replacements == [2, 4, 6]`，
所以「确实替换了三次」本身是被断言的，不是假设的），结果
**`counted_replica_reads["auction_gap"] == 1`**，八轮全部成功、无降级，
末轮心跳 `replica_opened is False` **且** `replica_skipped_by_floor is True`。

### 2.3 一条被本包**改掉**的包 Q 断言（集成者必看）

`tests/integration/test_route_a_readside_io_cost_e2e.py` 里
`test_an_atomic_replacement_costs_exactly_one_more_read` 断言的正是裁决 30 要改的行为，
本包把它替换成 `test_an_atomic_replacement_inside_the_floor_costs_no_further_read`
（`2` → `1`，并加断言 `replica_skipped_by_floor is True`：不是「没有新东西」，
是「有新东西而间隔把旧答案留下了」）。**这是 nodeid 的一删一增**，见 §6 收集增量。

---

## 3. 第 3 条：notifier 换代读减量——**做不到，给证据**

### 3.1 结论

`_minute_coverage` 发布的是 `minute_bar` 的**全量**覆盖率摘要：
一共多少行、多少个代码、多少个交易日、最早与最晚是什么时候。
**任何 `trade_time` 下界都会改掉其中三个已发布字段**，而已发布投影本包不得动。
包 Q §2.3 早已写过同一句话；本包把它量成了数字。

### 3.2 量了什么（包 Q 的方法：Linux 容器 `/proc/self/io` 的 `rchar` 增量）

合成副本与包 Q 完全同形（400 代码 × 600 交易日的日频四表，400 × 40 天 × 240 分钟的
`minute_bar` = 384 万行，成品 217,067,520 字节）。`python:3.11-slim`、duckdb 1.5.2、
每条各跑三轮、每轮一个全新连接（生产上读者就是开→读→关）：

| 查询 | 每轮 `rchar` 字节（三轮） | 计划里的 `SEQ_SCAN` |
|---|---|---|
| **现状**（全表，`WHERE freq='1min' AND trade_time <= ? AND created_at <= ?`） | 43,790,572 / 43,790,572 / 43,790,572 | 1 |
| **加当日下界**（`AND trade_time >= <当日 00:00>`） | **8,138,988 / 8,138,988 / 8,138,988** | 1 |

省下 **81.4%**（5.4 倍）。**但发布出来的行变了**：

| | `rows_count` | `codes_count` | `trade_dates` | `min_time` |
|---|---|---|---|---|
| 现状 | 3,840,000 | 400 | **40** | **2025-07-14 00:00** |
| 加当日下界 | **96,000** | 400 | **1** | **2025-08-22 00:00** |

四个字段里三个变了。所以这不是「同一个答案读得少」，是**另一个答案**。

一条口径要说清楚：8.1 MB 不是 43.8 MB 的 1/40。合成副本是按
「代码 × 交易日 × 分钟」的顺序插入的，每个 row group 都横跨全部 40 天，
zone map 只能剪掉一部分；生产上 `minute_bar` 是按时间落库的，剪枝会比这更好。
**能外推的是「语义变了」这个结论，不是 81.4% 这个比例。**

### 3.3 那还剩什么办法——没有等价的

- `COUNT(DISTINCT ts_code)` 与 `COUNT(DISTINCT CAST(trade_time AS DATE))` 各自强制整列读，
  `MIN`/`MAX(trade_time)` 本来就由 zone map 答（包 Q 量过：等于空开一次库）。
  剩下的 43.8 MB 就是 `ts_code` + `trade_time` + `source` 三列，一列都省不掉。
- 跨代增量（缓存「今天之前」那一半，只读今天）要求「`created_at <= C0` 的行在两代之间没动过」。
  `upsert_minute_bars` 走 `INSERT OR REPLACE`，盘中重取同一分钟会把那行的 `created_at`
  推到 `C0` 之后，于是它**同时**落在缓存的前缀和增量里，`COUNT(*)` 会多算。
  这不是「大概率没事」，是**可以静默给出错误数字**，不做。
- `approx_count_distinct` 之类改的是数值本身。

**所以 notifier 那一次读不是变小了，是变稀了**——15 分钟的间隔加 09:20–09:40 的禁读时段。
这条结论以用例的形式留在库里
（`test_the_minute_coverage_projection_cannot_be_narrowed_to_the_current_trade_date`），
免得下一个人重做一遍同样的实验。

---

## 4. 给 owner 的建议（第 4 条：`deploy/` 一字未动）

这四条都是 owner 的决策，本包没有改，装机与否互不影响：

| 建议 | 现值 | 建议值 | 理由 |
|---|---|---|---|
| 盘中副本同步频率 | 5 min | **15 min** | 每次替换是 10 GB 的 `cp`；读侧现在最快也只有 15 分钟重读一次，5 分钟的代已经没有消费者 |
| 备份时段 | 每 15 min（含 09:30） | **避开 09:20–09:40** | 09:30 那一次是 `cp` + `gzip` 10 GB，正好压在开盘 |
| `TimeoutStopSec` | 60 s | **≥ 300 s** | 代码侧已经让读可中断，这一条是兜底：中断链路万一不可用（wakeup fd 被别人占、引擎换版本），停机仍不该变成 `Result=timeout` 和一条推送 |
| vda 调度器 | `mq-deadline` | **`bfq`** | `rquant-live-runtime.slice` 上的 `IOWeight=` 在 `mq-deadline` 下不生效（2026-09-08 已记为已知限制），换成 `bfq` 才让包 Q §8 的权重建议有意义 |

前两条是本次事故里**最直接**的两笔 I/O；后两条是兜底与前提。四条都不需要改代码。

---

## 5. 变异（裁决 30 第 5 条要求 ≥ 4，做了 12 条）

脚本 `/Users/roxor/.claude/jobs/67487964/tmp/pkg-u/mutate.py`：逐条改源码 → 跑指定用例
（`-x`）→ **无论结果如何都还原**。`rc` 非 0 = 变异被杀（红）。

| # | 改成什么 | 跑了什么 | 结果 |
|---|---|---|---|
| M1 | watcher 听见信号但不叫引擎放弃（`self.registry.request()` 删掉） | 两条 watcher 用例 | **红**（`the query ran to completion, so the signal never reached the engine`，12 分 25 秒——那条查询是自己跑完的） |
| M1b | entrypoint 的处理函数只置事件、不请求中断（= #268 之前那个处理函数） | 停机 e2e | **绿——这是设计使然，见下** |
| M1c | **两条路一起删**（watcher 不中断 + 处理函数不中断） | 停机 e2e | **红**（240 秒超时：`the read is never abandoned`——09-14 那个缺陷本身） |
| M2 | 所有 role 的最短重读间隔归零 | gate 全量 + 两条 e2e | **红**（`assert 4 == 1`） |
| M3 | 禁读时段永不生效 | gate 全量 | **红** |
| M4 | **`_minute_coverage` 真的加上当日下界**（裁决 30 第 3 条，正着做一遍） | notifier 投影全量 | **红**（`assert [] == ['all', 'tushare']`） |
| M5 | 停机之后仍允许开始新的读 | 两条拒绝用例 | **红**（`the read must not begin`） |
| M6 | 被中断的一轮重新记成迭代失败 | 循环用例 + 停机 e2e | **红**（`'loop completed' != 'stop request...database read'`） |
| M7 | auction 源把中断重新翻译成自己的完整性错误 | 该用例 | **红**（`AuctionUniverseSourceError: daily snapshot q...`） |
| M8 | 间隔照常生效但心跳不报 `skipped_by_floor` | gate 全量 + 一条 e2e | **红**（`[False, False, False] != [True, True, True]`） |
| M9 | 跨代复用时不比 `key`（换了问题也给旧答案） | 该用例 | **红**（`assert 1 == 2`） |
| M10 | 没有答案的 role 也被间隔拦住 | gate 全量 + notifier 投影全量 | **红**（`assert 0 == 1`） |

**M1b 为什么是绿的，以及为什么这条绿是对的**：两条触发路径对 DuckDB 是**故意冗余**的
（§1.1）——删掉处理函数那一行，watcher 仍然中断得到，e2e 当然还绿。
**M1c 把两条一起删，e2e 立刻红**，所以两条都不是死代码，这条性质也不是巧合。
如果只报 M1 + M1b 而不报 M1c，读的人会以为其中一条是多余的。

**M1 这一条第一轮是绿的，我改了用例而不是改了结论**：初版的 watcher 用例里，
信号处理函数自己也调了 `request_read_interrupt()`，而 DuckDB 查询期间处理函数**会跑**
（§1.1 的实测），所以那条用例根本没有在测 watcher。现在处理函数只置事件，
另加一条 SQLite 用例——那里处理函数调了也没用，因为它要到语句结束才跑。改完 M1 就红了。
**这也是 §1.1 那张表的由来：它是被一条活下来的变异逼出来的，不是先想到的。**

---

## 6. 数字（两个 Python 版本 + Linux）

**定向 lane = 22 个文件**：本包改动的 12 个源文件对应的测试，加定向回归
`test_runtime_service_main` / `test_runtime_market_session` / `test_runtime_service_builtin` /
`test_auction_universe_runtime_publisher` / `test_reference_slow_runtime` /
`test_reference_slow_publisher` / `test_serving_page_isolation` /
`test_readside_replica_bindings` / `test_runtime_production_profile` /
`test_runtime_schema_release_snapshot`（闸门），以及三条 Route A e2e
（包 P 的 `test_route_a_readside_replica_e2e`、包 Q 的 `test_route_a_readside_io_cost_e2e`、
本包新增的 `test_route_a_open_io_interference_e2e`）。**共 667 条。**

全部跑在交付 HEAD 上（Linux 两条跑的是同一个 commit 的 `git archive` 副本）。
### 6.1 四条 lane

| 环境 | Python / SQLite / DuckDB | 结果 |
|---|---|---|
| 本机 macOS，`.venv` | 3.11.15 / 3.50.4 / 1.5.2 | **667 passed**（954.21 s，15 分 54 秒） |
| 本机 macOS，`.venv312` | 3.12.13 / 3.50.4 / 1.5.2 | **667 passed**（907.12 s；1 条 `os.fork()` DeprecationWarning）。并发复跑：**667 passed**（834.03 s） |
| Docker `python:3.11-slim`，非 root uid 1000 | 3.11.16 / 3.46.1 / 1.5.2 | **666 passed, 1 deselected**（1040.00 s，17 分 19 秒） |
| Docker `python:3.12-slim`，非 root uid 1000 | 3.12.14 / 3.46.1 / 1.5.2 | 首轮 **1 failed, 665 passed, 1 deselected**（见 §6.2）；复跑两次各 **666 passed, 1 deselected**（930.50 s / 935.53 s） |

**闸门**：`tests/unit/test_runtime_schema_release_snapshot.py` 单跑 **4 passed**（1.55 s），
与 base 相同——本包没有碰任何已发布模型字段，新增的心跳字段只在文件模型上。

`ruff check` 只查改动文件（全库约 980 条历史告警）：**All checks passed**。

### 6.2 Linux 3.12 首轮那一条失败：**没有复现出来，也不在本包的改动能到达的路径上**

```
FAILED tests/unit/test_serving_page_projection_source.py::
       test_a_source_touched_during_the_copy_is_refused_rather_than_served
```

**先说清楚一个缺口**：那一轮我把容器输出 `tail -6` 了，**失败的正文没有留下来**，
只剩摘要行。下一个包在容器里跑 lane 时应该留全量输出，别重复这个错。

**做了什么去定性**（五次机会，只出现过那一次）：

| 做了什么 | 结果 |
|---|---|
| 这一条单跑，Linux 3.12，**HEAD** | 1 passed |
| 这一条单跑，Linux 3.12，**base `4e76850`** | 1 passed |
| 整个文件跑，Linux 3.12，HEAD | 84 passed |
| 整个文件连跑 **60 轮**，容器里同时开四个忙循环压着 | **0 failures**；`/rq/tmp` 剩 43 G，不是磁盘满 |
| 整条 lane 复跑（单独跑） | 666 passed, 1 deselected |
| 整条 lane 复跑（**和 macOS 3.12 lane 同时跑，与首轮同样的并发条件**） | 666 passed, 1 deselected |

**为什么说本包到不了那条路径**：这个用例走的是 `_StableReadonlyDuckDB` 的**拷贝**分支，
拒绝是在 `_connect_through_copy()` 里 `_copy_identity(after) != _copy_identity(opened)`
抛出来的，而本包在这个类上只加了三处——
`__init__` 里 `self._interrupt_token = -1`、
`__enter__` 里 `_connect_generation()` **返回之后**登记连接、
`_release()` 开头在 token ≥ 0 时注销。
拒绝发生在 `_connect_generation()` **内部**，登记那一行根本没执行，
`_release()` 那个新块在 token 为 -1 时是空操作。
进程级的中断闩（`READ_INTERRUPTS`）也到不了这里：它只在 `register()` 里起作用，
而这一轮没有 `register()`；何况 lane 里发真 SIGTERM 的那条 e2e 排在这个文件**之后**。

**口径**：按「未复现的间歇失败」记，**不当作本包引入**，也**不当作已知 base 红**
（包 O / 包 Q 记过的那条 base 红是另一个 nodeid，已按简报 deselect）。
集成者在 CI 上看到同一条时，先按这一节的复跑记录判断，再决定要不要单独开 issue。

---

## 7. 边界自查

**一个字都没动的**：`deploy/`（含 `deploy/systemd/`、`deploy/nginx/`、`deploy/frp/`、
`deploy/sudoers/`）、`.env`（**worktree 里从头到尾没有 `.env` 文件**，
配置一律走 `export`，`ls .env` 报 no such file）、发布原语、stage、
`runtime_authority*`、`runtime_exec_wrapper/`、`runtime_capabilities.py`、
注册表 hash、任何 channel 的 `schema_version`、
`tests/manifests/full-suite-v1/`（**未重生成**）、R07 冻结基线（**未重冻结**）。

**已发布模型**：`RuntimeServiceHeartbeatProjection` 字段集**未变**
（`PUBLISHED_HEARTBEAT_FIELDS` 那条用例原样通过），`PAGE_PROJECTION_CONTRACTS` 未变，
`MinuteCoverageProjectionRow` 的字段与取值**未变**（§3 的结论就是「不能变」）。
新增的 `replica_skipped_by_floor` **只在心跳文件模型上**，与包 Q 的
`replica_opened` / `replica_read_bytes` 同一条规则，用例
`test_the_replica_floor_is_a_file_field_and_reaches_no_published_payload` 钉住。

**没有 skip / xfail**：本包一条都没有加。
容器里那条 `--deselect` 是包 O / 包 Q 记过的 base 红
（`test_default_candidate_loader_rejects_change_while_reading[content]`，
在这套容器配方上确定性红、与本包无关），按简报要求 deselect，**不是 skip 标记**。

**未 push、未打 tag、未创建 PR。** 临时文件全部在
`/Users/roxor/.claude/jobs/67487964/tmp/pkg-u/`（0700），不入库。
worktree 里留了一个 `.venv312`（uv 自带 `.gitignore`，`git status` 干净）。

**一条要交给集成者的**：本包**新增一个心跳文件字段**。回滚到 `v0.33.11` 或更早时，
按 2026-09-10 那一条写的整批挪心跳步骤先把心跳挪开——旧代码的心跳模型不认识这个字段。
DEPLOY.md 里已经写了这一条。

---

## 8. 提交

base `4e76850` 之后 **8 条**，全部带 trailer：

| # | commit | 内容 |
|---|---|---|
| 1 | `2aa026f4` | `fix(runtime)`：`runtime_read_interrupt` 模块本身（注册表、watcher、`is_read_interrupt`） |
| 2 | `cf75ea4c` | `fix(runtime)`：四条读路径登记连接、四处 `duckdb.Error` 放行中断、循环把被中断的读读成「这个循环结束了」、entrypoint 起 watcher |
| 3 | `f1275c65` | `fix(runtime)`：`ReplicaReadProfile` 与四个 role 的画像、`replica_skipped_by_floor` 一路到心跳文件模型 |
| 4 | `d122e31e` | `test(runtime)`：新 e2e 两条 + 各文件的单元用例 |
| 5 | `c7c25a4d` | `docs`：CHANGELOG `[Unreleased]/Fixed` 两段 + DEPLOY.md 待装条目 |
| 6 | `e0ebe6a0` | `fix(runtime)`：gate 的时钟过 `normalize_aware_utc`，禁读时段按盘中时钟而不是主机本地时钟算 |
| 7 | `5d48fe68` | `fix(runtime)`：把「处理函数到底跑没跑」的说法改对（两个引擎的实测表），并把 watcher 的用例改成真的隔离 watcher（M1 因此从绿变红） |
| 8 | `3bafffec` | `docs`：CHANGELOG 里同一处说法改对 |

第 6、7、8 三条是**自己复核出来的返工**，不是复审给的：第 6 条是读自己写的
`astimezone` 时发现的（naive datetime 会按主机本地时区解释，而生产主机恰好也是 CST，
这种差别正是靠「恰好一样」活下来的）；第 7、8 条是被活下来的变异 M1 逼出来的。

---

## 9. 收集增量（collected delta）

base `4e76850` 与交付 HEAD，同一台机器、同一套 `export` 配置，各跑一次全量收集：

| 文件 | base | HEAD | Δ |
|---|---|---|---|
| `tests/unit/test_runtime_read_interrupt.py`（新增） | 0 | 16 | **+16** |
| `tests/unit/test_readside_replica_gate.py` | 25 | 39 | **+14** |
| `tests/unit/test_runtime_service_control.py` | 39 | 45 | **+6** |
| `tests/integration/test_route_a_open_io_interference_e2e.py`（新增） | 0 | 3 | **+3** |
| `tests/unit/test_auction_universe_source.py` | 6 | 8 | **+2** |
| `tests/unit/test_reference_slow_source.py` | 32 | 34 | **+2** |
| `tests/unit/test_serving_page_projection_source.py` | 82 | 84 | **+2** |
| **合计** | **184** | **229** | **+45** |

全量：**14486 → 14531（19 deselected 不变）**，净 **+45**。

**但集成者看到的不是净值**：`validate_manifest` 是逐条 nodeid 比对，本包的形状是

```
missing=1  extra=46
```

消失的那一条只有一个，就是 §2.3 那一条：

| 消失的 nodeid | 钉的是什么 | 替换成 |
|---|---|---|
| `tests/integration/test_route_a_readside_io_cost_e2e.py::test_an_atomic_replacement_costs_exactly_one_more_read` | 副本被替换 ⇒ 恰好多读一次（包 Q 的规则，正是裁决 30 第 2 条要改的那条） | `test_an_atomic_replacement_inside_the_floor_costs_no_further_read` |

除它之外没有别的 nodeid 消失，也没有改名。
`tests/manifests/full-suite-v1/` **未重生成**（简报禁止）；集成者重生成时的预期是
**`missing=1 extra=46`**，不是「+45」。

---

## 10. 复核时值得先看的四处

1. **§1.1 那张表**——如果这一条不对，第 1 条的整个叙述都要改。它是被一条活下来的变异逼出来的，
   不是先想到的。
2. **`ReplicaReadProfile` 为什么只有 notifier 带禁读时段**（§2.1）。#268 原文那两条要求字面上
   打架，我按「能工作的那个解释」做了，这是一个判断，不是一条推导。
3. **§3 的结论**：`minute_coverage` 不能收窄。如果复核认为可以（例如愿意改已发布投影并走
   schema rollout），那是另一个包的事，本包的边界不允许。
4. **§6.2 那条没复现出来的失败**，以及「那一轮容器输出被 `tail` 掉了」这个操作失误。

## 11. 交接给 integrator 的三条（不带就是 CI 红）

1. `tests/manifests/full-suite-v1/` **未重生成**。重生成时的预期是 **`missing=1 extra=46`**，
   消失的那一条是 `test_an_atomic_replacement_costs_exactly_one_more_read`（§2.3、§9）。
2. **本包新增一个心跳文件字段 `replica_skipped_by_floor`**。回滚到 `v0.33.11` 或更早时，
   要按 2026-09-10 那一条写的整批挪心跳步骤先把心跳挪开。DEPLOY.md 已写。
3. 容器里跑 lane 要 deselect
   `tests/unit/test_runtime_builder_candidate.py::test_default_candidate_loader_rejects_change_while_reading[content]`
   （包 O / 包 Q 记过的 base 红，与本包无关），并且**留全量输出**（§6.2）。
