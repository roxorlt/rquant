# L2 下游消费吞吐：HTTP 消费者「2 秒固定 sleep」根因与校准基准

**结论**：40 股档 2026-10-29 13:00 实盘里 HTTP 消费者只追到 69,640/157,051 批，根因不是产品 API 读不动
（实测 API 读取能力 486–668 批/秒），而是测试消费者 `.tools/review/scale40/consumer.py::http_loop` 在
**每轮 `GET limit=128` 之后无条件 `time.sleep(2)`**。该校准项只需改测试消费者，不动 `api.py`：离线基准里
事件驱动连续轮询的 HTTP 消费者以 2,694 批/秒（双消费者并发，单消费者更高）追平全部 78,600 批合成缓存，
是 40 股档生产速率 131.2 批/秒的 **20.5 倍**；旧实现在零延迟 API 下的理论上限也只有 **64 批/秒**（131.2 的 49%）。

## 1. 根因（证据来自只读的 40 股档日志）

旧循环结构（`.tools/review/scale40/consumer.py`）：

```python
cursor = data["next_sequence"]
out.write(...)
time.sleep(2)          # 无条件，即使 128 批只是 0.2 秒的读取量
```

从 `.tools/review/scale40-C-20261029/logs/http.jsonl` 的 550 个成功循环实测：

| 项目 | 实测值 | 口径 |
|---|---|---|
| 生产者（WS 参照，同一次运行） | **131.2 批/秒** | 157,051 批 / 1,198.7 秒 |
| HTTP 消费量 | **69,640 批** | head 157,051，缺口 87,411（**55.7%**） |
| 循环周期 | p50 **2.1917 秒**，p95 2.2634 秒 | 相邻 `t` 差值 |
| 其中请求耗时 | p50 **0.1917 秒**，p95 0.2634 秒 | 周期 − 固定 2 秒 |
| API 实际读取能力 | **668 批/秒**（p50）/ 486 批/秒（p95） | 128 / 请求耗时 |
| 旧实现上限 | **64 批/秒**（零延迟）/ 58.4 批/秒（实测延迟） | 128 / (2 + 请求耗时) |

即使 API 零延迟，`128 / 2 = 64 批/秒 < 131.2 批/秒`，所以 HTTP 消费者**在数学上不可能**追平 head；
实测 58.1 批/秒 × 1,200 秒 ≈ 69,640 批，与日志完全吻合（理论 64 × 1,198.7 ≈ 76,718，达成率 91%）。
瓶颈是消费端的固定 sleep，不是 API、不是缓存读取、不是 WS（同一次运行 WS 追平到 head=157,051）。

## 2. 校准做法（`src/rquant/l2/consumer_bench.py`）

事件驱动连续轮询，四条规则：

1. **游标落后 head 时不 sleep**：一页返回后立即下一个 `GET limit=128`；部分页（<128，含 12MB 页预算截断）
   也立即续读，绝不固定等待。
2. **只有「无进展」才短等**：空页/游标不动时等 0.05–0.2 秒；连续 64 次无进展抛 `ConsumerStalled`，
   不会空转死循环。
3. **到 head 后短等**：每 0.1 秒空轮询一次并校验游标不动（实测 10 次/1 秒、0 批、p50 1.84ms）。
4. **每页都验链**：逐批 `local_batch_sequence` 连续 + 全链 sha256
   （`sequence:response_sha256:row_data_sha256:source_sha256`），网络错误重试有上限且**不前进游标**，
   游标跳变/超 limit 直接报错。

## 3. 本机实跑数字（2026-11-04，MacBook arm64 / macOS 26.3 / Python 3.11.15 / 12 核）

合成缓存 78,600 批（正好是 40 股 600 秒 × 131 批/秒量级），169.1 MB，构建 16.06 秒；
`rquant.l2.api.create_app` 在 127.0.0.1 随机端口起服务：

| 指标 | 主 HTTP 消费者（双消费者并发） | WS 参照 |
|---|---|---|
| catch_up_seconds（0→78,600） | **29.18 秒** | 18.21 秒（0→79,386） |
| sustained_batches_per_second | **2,694 批/秒**（两消费者合计 ≈5,388） | 4,358.5 批/秒 |
| GET/消息 p50 / p95 / max | 40.66 / 73.65 / 112.20 ms | 23.01 / 54.12 / 89.21 ms |
| 请求数 / 重试 / 缺批 | 785 / 0 / 0 | 621 / 0 / 0 |

实时跟随（`--live-seconds 6 --live-rate 131`，用未改动的 `RawL2Cache.append` 写）：

- 生产者 130.78 批/秒写入 786 批，head 78,600 → 79,386；
- 两个 HTTP 消费者 6.01 秒内各跟随 786 批，`max_lag_batches=3`，到点后 `cursor == head`。

**链校验**：HTTP 主/副消费者与 WS 的 `chain_sha256` 均等于全量缓存链
`0fa9bf7458aa251bacbb9d1c5a873326aab3e7029fc6f360884e1ec9e8f9d0b8`（79,386 批），
`integrity.ok=true`（完整结果见 `evidence/rquant-l2-consumer-throughput-2026-11-03.json`）。

与 40 股档对比：

| 消费者 | 批/秒 | 相对 131.2 批/秒 |
|---|---|---|
| 旧实现（2s sleep）上限（零延迟 API） | 64.0 | 0.49× |
| 旧实现实测（40 股日志 550 周期） | 58.1 | 0.44× |
| 校准 HTTP（本机双消费者，每个） | 2,694 | 20.5× |
| 校准 HTTP（本机单消费者，2,000 批冒烟） | 7,697 | 58.7× |

注：本机 GET 只有 5.9–40.7ms，而生产同规模缓存 GET p50 约 0.19s，因此绝对吞吐数值不能照搬；结论
「生产 131.2 批/秒下校准消费者可持续追平」由「消费端不再固定 sleep + 本机 headroom 20 倍」共同支撑。

## 4. 复跑命令

```bash
# 一键基准（默认 78,600 批 + 6 秒实时跟随 + 双 HTTP + WS 参照）
# 注意：脚本固定把报告写到 evidence/rquant-l2-consumer-throughput-2026-11-03.json，
# 小规模自检请直接调模块（见下），避免覆盖本证据。
bash "/Users/roxor/Documents/ChatGPT/rea L2/.tools/replay-packs/run_l2_consumer_bench.sh"

# 小规模自检（2,000 批、单消费者、3 秒实时跟随、不跑 WS）
cd "/Users/roxor/Documents/ChatGPT/rea L2/.tools/review/rquant-replay"
PYTHONPATH=src ../testvenv/bin/python -m rquant.l2.consumer_bench \
  --batches 2000 --live-seconds 3 --http-consumers 1 --no-ws-reference

# 单元测试（<20 秒；7 passed in ~4s）
bash "/Users/roxor/Documents/ChatGPT/rea L2/.tools/review/run-l2-replay-tests.sh" \
  tests/unit/test_l2_consumer_bench.py -q
```

## 5. 限制（本机合成缓存的局限）

- 合成观测是**单一形态**（`vendor4027` 逐笔委托 seed，每批 1 行，2,151 B/批），不是真实 40 股多流混合；
  真实缓存 3,536 B/批（555 MB / 157,051 批），单页更重、GET 更慢。
- 本机单机回环，没有跨机网络、磁盘冷热差异，也没有生产里 API 与采集器写盘争抢；绝对批/秒不可直接换算生产。
- 页预算 12 MB 未被触发（单页 ≈270 KB），生产大页截断路径由 API 测试覆盖，本基准未复现。
- 大缓存默认走 `bulk` 直写既有磁盘布局（`RawL2Cache` 读取器抽样校验 + 全链校验），不是逐批 `append`；
  `writer` 模式只用于小缓存测试。真实 `feed.py` 的写入速率与消费者并发竞争未纳入。
- WS 上限 8 连接、多消费者并发与 12 MB 页预算的交互未在本次基准中穷举。

相关文件：`src/rquant/l2/consumer_bench.py`、`tests/unit/test_l2_consumer_bench.py`、
`.tools/replay-packs/run_l2_consumer_bench.sh`、
`evidence/rquant-l2-consumer-throughput-2026-11-03.json`。
