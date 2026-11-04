# L2 种子回执流控与接收-落盘解耦

- 卡片：`card-81b53759`（40 股种子回执与消费吞吐排查，80 股前置）
- 日期：2026-11-04
- 状态：**本机离线验证通过；未部署服务器，未改生产采集参数**
- 相关证据：[seed latency curve](../../../evidence/rquant-l2-scale40-C-seed-latency-2026-11-04.json)、
  [离线验证报告](../../../rquant-l2-seed-throughput-2026-11-04.md)

## 1. 问题的实测形态

2026-10-29 13:00 的 `scale40-C`（40 股 / 120 流）只完成 72/120 种子：请求 3–74（前 24 只）全部拿到回执，
75–122（后 16 只）15 秒内都没有回执。逐流回执还原后的关键事实：

| 事实 | 数值 |
| --- | --- |
| 种子等待随「本连接累计注册页数」增长 | ≤57 页约 0.1–1 s；64 页 1.85 s；69 页 3.67 s；72 页 14.06 s（模型值，实测末次 10.99 s） |
| 越过 15 s 守卫的请求 | 第 75 号（第 73 页）；此后 48 条请求全部超时 |
| 供应商是否拒订 | 否：120 条流稍后都收到推送（`streams.push=120`，`unknown_push_count=0`） |
| 本地资源 | 采集器 CPU 27 %、RSS 40 MB、写盘 0.46 MB/s、端到端处理延迟约 10 ms，非 CPU/内存/磁盘打满 |
| 本地上游写入节奏 | 全程约 131 批/秒的平台期，与 20 股窗口的 128 批/秒同量级，接近本机逐批 fsync 写盘上限 |

因此「供应商永久不回执 / 协议上限」不成立；成立的是**种子等待随单连接累计页数（以及该连接上的实时推送量）
增长**，而采集器原来的单线程“发一条、等 15 秒、再发下一条”把这笔延迟放大成 15 秒 × 48 条的硬失败。
回放层用同一份逐流曲线做了可复现的故障模型（见 §4），不是凭空假设。

## 2. 改造内容

### 2.1 分批建连（`connection_count`）

`FeedCollector` 现在可以开 N 条上游连接，按**连续股票块**分配页面（第 0–9 只 → 连接 0，10–19 → 连接 1，…）。
每条连接有自己的 `pageList`、request-id 空间和种子等待，所以“单连接累计页数”被限制在
`ceil(股数 / N) × 3`，供应商侧的每连接增长项不再随整档规模变化。

- 代码：`src/rquant/l2/feed.py:146`（`_Link`）、`src/rquant/l2/feed.py:823`（`_link_for`）、
  `src/rquant/l2/feed.py:954`（`_epoch` 多连接引导/轮询）。
- 单连接是默认值，旧路径逐字节保留；多连接只在显式参数下启用。

### 2.2 分批订阅与自适应发送节奏（`subscribe_batch_stocks`）

- 每发完 `3 × subscribe_batch_stocks` 条流（默认 5 只 = 15 条）设置一个批次屏障：若实测种子延迟
  EWMA 超过 `seed_latency_soft_seconds`（默认 2 s），先停顿并把节奏放慢，再开始下一批，
  避免继续往供应商的种子队列里加压。
- 发送间隔从固定 1 s 变成 `[1 s, seed_pace_max_seconds]` 自适应：延迟高就放慢、延迟回落就恢复 1 s；
  **下限仍是 1 秒**，不改变“完成到下一发送至少 1 秒”的既有约定。
- 代码：`src/rquant/l2/feed.py:835`（`_record_seed_latency`）、`:871`（`_note_issued` 批次屏障）、
  `:891`（`_issue_stream` 的节奏等待）。

### 2.3 延迟种子重试（`seed_retry_limit`）

种子 15 秒超时后不再直接终态化：把该流记入延迟队列，等全部首批请求发完、经过冷静期后，用新的
request-id 重发一次（最多 `seed_retry_limit` 次）。收到迟到响应仍按既有规则作为该页的 candidate push
记入缓存，不丢数据。

- 代码：`src/rquant/l2/feed.py:845`（超时记账）、`:859`（`_retry_deferred`）。
- 回执 schema 未改：重试用“`sent` + 新 request_id”表达，`state` 枚举不变，旧归档仍可读。

### 2.4 接收-解包-落盘异步解耦（`receive_pump`）

- 每连接一个 reader 线程：只做 FD 分帧，把帧放进两条有界队列——**响应/ack 优先道**与**行情普通道**；
  解码、关联、缓存提交、回执全部留在采集线程。
- 普通道到上限后 reader 停止读该 socket（TCP 背压），这是单写者下唯一诚实的背压方式；
  优先道保证种子响应不会排在几百上千条推送之后。
- 指标只放内存（不改 `FeedStatus` 落地 schema）：`collector.metrics` 暴露
  `frames_pending_at_end`、`pumps[].highwater`、`seed_wait_seconds`、`deferred_retries`、
  `late_seed_pushes` 等，供门禁/基准断言“零帧堆积”。
- 代码：`src/rquant/l2/feed.py:168`（`_FramePump`）、`:698`（`_read`）、`:484`（metrics）。
- 限制：reader 线程用墙钟，`receive_pump` 只适用于 `clock_mode="wall"` 的真实/墙钟回放；
  虚拟时钟回放继续用单线程路径。

### 2.5 采集器入口参数（`src/rquant/l2/__main__.py`）

新增 `--connections`、`--subscribe-batch-stocks`、`--seed-retry-limit`、`--receive-pump` 等参数，
默认全部保持旧行为；40/80 股档的建议组合写在下节命令里。

## 3. 复跑命令

离线回放门禁（新增两个场景，加上原有 7 个；预算 180 s）：

```bash
bash "/Users/roxor/Documents/ChatGPT/rea L2/.tools/replay-packs/run_l2_replay_gate.sh"
```

| 场景 | 配置 | 期望 |
| --- | --- | --- |
| `fault-seed-latency-40` | 40 股、单连接、校准种子延迟故障 | 72/120（24/40 只），复现 2026-10-29 |
| `fault-seed-latency-40-batched` | 同上 + 4 条连接 + 分批订阅/重试 | 120/120（40/40 只），`frames_pending_at_end=0` |

单独跑一个场景：

```bash
cd "/Users/roxor/Documents/ChatGPT/rea L2/.tools/review/rquant-replay"
PYTHONPATH=src ../testvenv/bin/python -m rquant.l2.replay_gate run \
  --pack "/Users/roxor/Documents/ChatGPT/rea L2/.tools/replay-packs/20261016-live-6stream" \
  --stocks 40 --fault seed-latency --connections 4 --flow-control --out /tmp/latency-40.json
```

真实盘中运行（下一交易日 40/80 股档）建议参数：

```bash
rquant l2 collect ... \
  --connections 4 \                 # 40 股档 4 条；80 股档 8 条（每连接 ≤10 只）
  --subscribe-batch-stocks 5 \
  --seed-retry-limit 1 \
  --receive-pump
```

注意 4/8 条并发会话是否被供应商接受，只能用真实窗口验证；离线回放能证明本地策略正确，
不能证明供应商的多会话行为。

## 4. 回放故障模型（`seed_latency_onset_pages` / `seed_latency_coefficient`）

`replay_source.FaultInjection` 新增按连接页数计算的种子延迟：

```text
wait_seconds = coefficient × max(0, pages − onset)²
```

`onset=57, coefficient=0.0625` 是 2026-10-29 逐流回执的校准值：72 页 14.1 s、73 页 16.0 s，
正好复现“第 73 页越过 15 秒守卫”的边界；模型只拟合了失败边界，不声称供应商内部机制。
种子响应按延迟放进回放计划堆，超时后的迟到响应仍会以 candidate push 形式到达（真实运行同形）。

- 代码：`src/rquant/l2/replay_source.py:392`（延迟函数）、`:407`（门禁时长估算）、
  `src/rquant/l2/replay_gate.py:256`（故障常量）、`:272`（两个新场景）。

## 5. 未做与限制

- **未部署、未改生产采集参数**：默认值仍是单连接、固定 1 秒、无重试、无 reader 线程，
  需要显式参数开启；生产是否启用由下一次真实窗口决定。
- 多连接会同时开 N 个供应商会话，供应商是否有会话数/同账号限制未验证。
- 回放故障模型是“按连接页数增长的等待”的经验拟合，不是对供应商实现的断言；若真实原因不同，
  自适应节奏与延迟重试仍提供兜底，但 120/120 需要在真实窗口确认。
- `receive_pump` 的“零帧堆积”只在墙钟模式验证；虚拟时钟回放不启用线程路径。
- 缓存提交仍为每批 4 次 fsync、每条市场帧 1 次状态 fsync 的旧口径，本轮没有改缓存持久化语义。
