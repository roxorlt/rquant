# L2 确定性回放引擎与盘后离线门禁

本文说明 `rquant.l2.replay_source`（本地确定性 L2 回放源）与 `rquant.l2.replay_gate`
（一键离线基准门禁）：它们把 R1 录制的原始帧包变成可反复重放的行情源，驱动**未改动**的
`FeedCollector`，从而把原本要等下一个交易日的排查验证压缩成盘后几分钟的自动检查。

| 项 | 位置 |
| --- | --- |
| 回放源（门控/倍速/扩流/故障注入） | `src/rquant/l2/replay_source.py` |
| 原始帧重写（Snappy 字面量重压 + 重打标/改标题） | `src/rquant/l2/_replay_rewrite.py` |
| 一键离线门禁与 JSON 报告 | `src/rquant/l2/replay_gate.py` |
| 单测 | `tests/unit/test_l2_replay_source.py`、`test_l2_replay_rewrite.py`、`test_l2_replay_gate.py`、`test_l2_replay_adversarial.py` |
| 测试入口 | `.tools/review/run-l2-replay-tests.sh` |
| 一键门禁入口 | `.tools/replay-packs/run_l2_replay_gate.sh` |
| 独立保真校验脚本 | `.tools/replay-packs/verify_replay_engine.py` |
| 输入回放包 | `.tools/replay-packs/20261016-live-6stream/`（R1 交付） |
| 本卡结论与复现 | `rquant-l2-replay-r2-2026-11-03.md` |

## 1. 一句话用法

```bash
# 跑一轮完整离线门禁（40/80/100 档 + 三个故障场景 + 确定性复跑）
bash "/Users/roxor/Documents/ChatGPT/rea L2/.tools/replay-packs/run_l2_replay_gate.sh"

# 单场景
cd "/Users/roxor/Documents/ChatGPT/rea L2/.tools/review/rquant-replay"
PYTHONPATH=src ../testvenv/bin/python -m rquant.l2.replay_gate run \
  --pack "/Users/roxor/Documents/ChatGPT/rea L2/.tools/replay-packs/20261016-live-6stream" \
  --stocks 40 --fault seed-24of40 --out /tmp/r2-single.json
```

## 2. 回放包读取契约

回放源只用 R1 的读取接口，不解析段文件格式：

```python
from rquant.l2.replay_source import read_pack_index

index = read_pack_index(pack_dir)      # manifest + 全部 RX 记录
index.control                          # 控制帧 body（ack/会话元数据/心跳），按记录顺序
index.streams[10117]                   # RecordedStream：code/market/kind/side
index.streams[10117].seed_groups        # 每条流按请求分组的种子响应（多页回填会有多组）
index.streams[10117].pushes             # (相对首帧微秒偏移, body) 候选推送时间线
index.duration_us                       # 原始采样窗口
```

分类规则与 `feed._market` 保持一致：空 body 与 page==0 的帧是控制帧；`request_id==0xFFFFFFFF`
的帧是候选推送（按 page 路由）；其余按 `request_id` 收成种子组。页 ID 必须落在
`10117 + 10*stock_index + {0,1,2}` 上（0=vendor4013 逐笔、1=vendor4027 买委托、2=卖委托），
不符合该约定的包直接判为 `unsupported_pack`。

## 3. 请求门控回放（为什么不能按时序盲灌）

`FeedCollector` 每 1 秒发一条种子请求并等待对应 `request_id`。多流场景下若把录制的 RX 帧
按原时序直接灌入，第二条流的响应会早于它自己的请求到达，`_market` 立刻抛
`association_mismatch`（R1 冒烟测试踩过）。因此回放连接实现的是一个 **mock socket**：

- `sendall(wire)`：解析 TX 的 `request_id` 与目标 `page_id`，找到对应流；
  只有这一刻才把该流的种子响应放入就绪队列，并把录制帧的 `request_id` 重打标为本次实际请求号。
- `recv(size)`：按「就绪队列 → 计划堆（按虚拟时间到期）→ 空转超时」三级返回；
  空转时推进时钟 `timeout` 秒并抛 `TimeoutError`，与真实 socket 的空闲语义一致。
- 推送流在**该页首次被请求时**才开始播放，因此不会出现"未订阅就收到推送"的假关联。

## 4. 倍速与时钟

| 模式 | `speed` | 行为 |
| --- | --- | --- |
| 1x | `1.0` | 按录制微秒偏移投递（与真实采样节奏一致） |
| 10x | `10.0` | 偏移除以 10 |
| 全速 | `0.0` | 全部帧到期时间为现在，消费者要多快给多快 |

`clock_mode="virtual"`（门禁默认）用一个只在「有人 sleep」时才前进的虚拟时钟同时驱动回放
连接与 `FeedCollector`：1x/10x/全速三档的**墙钟成本相同**（都只花在解码+落盘上），而虚拟
时间线严格按倍速伸缩，因此既确定性又快。需要真按墙钟节奏跑时用 `clock_mode="wall"`。
`speed` 只作用于「回放投递节奏」，不改变采集器自身的 1 秒请求节流与 15 秒种子守卫。

## 5. 40/80/100 股扩流克隆

`FeedCollector` 的页 ID 是确定公式 `10117 + 10*stock_index + offset`，所以扩流无需改采集器：

- 第 0/1 只股票使用录制包里的两只真实标的；第 2 只起克隆录制包第 0 只的流。
- 克隆时按目标位置重写 `page_id`，按实际请求重写 `request_id`；若是 order 流
  （`command==2267` 的 vendor4027 表），表头标题里的 6 位代码也改写成目标代码
  （`600000-逐笔委托`），**表体行数据不动**。
- 标题改写需要解压重压：`_replay_rewrite` 只输出 Snappy literal 指令（不实现 copy），
  因此**改写后的 body 可能比录制 body 长**；回放层一律用 `wire_frame()` 按新长度重包 FD
  前缀，绝不复用旧前缀。
- 所有克隆流在 `source.published()` 里标注 `synthetic`，报告与卡面都把扩流数据记为
  合成克隆，**不能当作真实市场证据**。
- `ReplayConfig` 对 symbols **显式查重并拒绝重复代码**（`invalid_symbols`）：`normalize_symbols`
  本身会静默去重，若沿用会让"40 股门禁"实际只跑 39 只却报 40 股的种子率。

## 6. 信封族归一（10/16 存量包的兼容动作）

2026-10-19 之后采集器把所有行情请求升级为 2357 家族（`build_common_order_frame`），而
`feed._market` 只接受 `command==2357` 的行情响应。2026-10-16 及更早缓存里 order 响应仍是
2267 家族。为了让存量包可重放，回放层在 `adapt_envelope_family=True`（默认）时把
order 响应信封里的那 **4 字节命令字** 归一为 2357：

- 只改 `body[16:20]`，表体（Snappy 压缩表、标题、行数据）逐字节不动；
- 计数暴露在 `source.stats["family_restamped"]`，报告里单列；
- `adapt_envelope_family=False` 是严格模式：老包会如实在第 2 条流上
  `association_mismatch`——这也证明归一是必需的，而不是装饰。
- 现场 tap 录制的新包本身就是 2357，归一动作是 0 次。

## 7. 故障注入

```python
FaultInjection(
    withhold_seed_from_stock=24,       # 第 24 只起不投递种子响应 → 每条流 15s 守卫超时
    withhold_seed_codes=("300006",),   # 指定标的静默（单股种子超时）
    stall_codes=("300006",),           # 种子正常，之后该股不再有推送（静默断流，不关连接）
    withhold_bootstrap_ack=True,       # 首包 ack 扣留 → epochs 在 bootstrap 就 15s 超时
)
```

故障代码必须是本档 `symbols` 的子集、`withhold_seed_from_stock` 不得大于档位股票数，否则
构造回放源时直接 `invalid_fault`（拼错的代码不会静默变成"健康场景"）。

`withhold_seed_from_stock=24` + 40 股档精确复现 2026-10-29 `scale40-C` 的形态：

| 指标 | 2026-10-29 真实 40 股 | 本卡回放（虚拟时钟） |
| --- | --- | --- |
| `seed_queries` | 120 | 120 |
| 完成种子的流 | 72 | 72 |
| 拿到种子的股票 | 24/40 | 24/40 |
| 15s 无回执的流 | 48 | 48 |
| 运行结束状态 | `stopped` / `duration_limit` | `stopped` / `duration_limit` |

超时后这些页仍会被后续推送命中（真实运行也是这样），因此判定"是否种子成功"必须看订阅回执
里的 `seed_sequence is None`，**不能**看 `state`（会变成 `first_push_observed`）。48 条流
× 15 秒 = 720 虚拟秒在虚拟时钟下不花墙钟时间。

## 8. 一键离线门禁与报告

`replay_gate` 用独立临时 cache 目录逐场景驱动 `FeedCollector`，默认场景集：

| 场景 | 期望 |
| --- | --- |
| `healthy-40/80/100` | 全部流种子成功、批次数 == 计划种子数 + 计划推送数 |
| `fault-seed-24of40` | 72 种子 / 24 只股票 / 48 失败流，停止状态 `stopped`+`duration_limit` |
| `fault-single-silent` | 117 种子，仅 300006 三流未种子 |
| `fault-single-stall` | 120 种子全成功，推送帧数少掉该股全部推送，epoch 仍健康 |
| `fault-seed-latency-40` | 校准 2026-10-29 的「按连接页数增长的种子等待」，单连接精确回到 72/120（24/40 只） |
| `fault-seed-latency-40-batched` | 同一故障 + 4 条连接 + 分批订阅/延迟重试，恢复到 120/120（40/40 只）且 `frames_pending_at_end=0` |
| `determinism-40` | 同配置连跑两次，提交批次的 `(page_id, kind, code, side, delivery, response_sha256)` 序列哈希完全一致 |

报告 schema `rquant-l2-replay-gate-v1`，四个卡面指标的定义：

| 指标 | 定义 |
| --- | --- |
| `seed_first_packet_rate` | 订阅回执里 `seed_sequence` 非空的流数 / 流总数；同时给出 `stocks_seeded/stocks_total`、`seed_queries`、`seed_timeout_streams` |
| `consumption_latency_p99_us` | 每帧「回放连接 `recv` 返回」→「`cache.append` 落盘完成」的真实墙钟延迟的 P99（同时给 p50/p95/max/samples） |
| `disk_write_mb_per_second` | `collector.run()` 墙钟秒内写入 cache 目录的字节数换算 MB/s（1 MB = 1e6 字节，附 batches/cache_bytes） |
| `memory` | 采样线程每 0.25 秒用 `ps -o rss= -p <pid>` 取 RSS，给曲线、峰值与 p50 |

报告同时写入 `gate.wall_ms` 与 `gate.budget_ms`（默认 180000），把「盘后 3 分钟一轮」变成
可回归的数字而不是口号。

### 实测（2026-11-04 本机，9 场景含种子延迟流控；详见 [l2-seed-flow-control.md](l2-seed-flow-control.md)）

`bash .tools/replay-packs/run_l2_replay_gate.sh` → `gate=pass`，总墙钟 **63.1 s**（预算 180 s），
9/9 场景全部通过。两个新增场景在 10x 回放下的结果：

| 场景 | 连接 | 批次 | 种子/流 | 股票 | 种子首包率 | 种子等待 max | 结束待处理帧 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `fault-seed-latency-40` | 1 | 3567 | 72/120 | 24/40 | 0.600 | 14.06 s | 0 |
| `fault-seed-latency-40-batched` | 4 | 3572 | 120/120 | 40/40 | 1.000 | 0.00 s | 0 |

### 历史实测（2026-11-03 本机，7 场景基线）

`bash .tools/replay-packs/run_l2_replay_gate.sh` → `gate=pass`，总墙钟 **47.6 s**（预算 180 s），
7/7 场景、116/116 条 expectations 全部通过：

| 场景 | 墙钟 ms | 批次 | 种子/流 | 股票 | 15s 无回执流 | 种子首包率 | 消费延迟 p50/p99/max µs | 写盘 MB/s | 内存峰值 MB |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| healthy-40 | 5466 | 3572 | 120/120 | 40/40 | 0 | 1.000 | 945/10709/20167 | 4.13 | 46.3 |
| healthy-80 | 6779 | 3772 | 240/240 | 80/80 | 0 | 1.000 | 977/10969/32110 | 5.13 | 47.5 |
| healthy-100 | 7145 | 3872 | 300/300 | 100/100 | 0 | 1.000 | 944/11114/54410 | 5.72 | 48.7 |
| fault-seed-24of40 | 5130 | 3524 | 72/120 | 24/40 | 48 | 0.600 | 941/2319/18481 | 3.46 | 50.6 |
| fault-single-silent | 5529 | 3569 | 117/120 | 39/40 | 3 | 0.975 | 955/10738/19013 | 4.03 | 50.6 |
| fault-single-stall | 992 | 198 | 120/120 | 40/40 | 0 | 1.000 | 1237/15112/33929 | 12.29 | 50.6 |
| determinism-40 | 12781 | 3572 | 120/120 | 40/40 | 0 | 1.000 | 940/10744/17391 | 4.09 | 50.6 |

补充说明：

- 主场景四个卡面指标：**seed_first_packet_rate = 1.0**、**消费延迟 P99 = 10.709 ms**
  （p50 0.945 ms）、**写盘吞吐 = 4.13 MB/s**（cache 21.6 MB / 5.24 s）、**内存峰值 46.3 MB**
  （P50 43.2 MB，22 点曲线，采样间隔 0.25 s）。
- 消费延迟的 P99 尾巴是 fsync 抖动而非解码：p50 亚毫秒、个别批次撞到 16–67 ms 的落盘尖峰，
  所以门禁只断言结构与形态，不对延迟/吞吐设绝对阈值；吸收这些抖动的正是「结构性门禁」定位。
- determinism-40 两次独立临时目录连跑的批次序列 SHA256 完全相同
  （`bb53cb3262ccaab4…`），三次独立门禁运行该 SHA 也保持一致。
- 内存采样本机为 `libproc` 回退（受限沙箱拒绝 exec setuid 的 `/bin/ps`，报告
  `metrics.memory.method`/`fallback_reason` 如实记录）；普通用户 shell 下 method 为 `ps`。
- 写盘吞吐受本机负载影响会在 2–5 MB/s 区间浮动（本次 3.4–13.2 MB/s，stall 场景因批次数少
  所以比值高）；这是同一台机器上的实测值，不是容量规划承诺。`verify_replay_engine.py` 是独立的逐帧保真校验（父/子进程各跑一次
回放，比较录制 body 与落库 body、对照每页首个差异偏移、校验 order 表标题代码、断言两次运行
digest 一致），用于给门禁结果做交叉复核，不参与门禁判定：

```bash
cd "/Users/roxor/Documents/ChatGPT/rea L2/.tools/review/rquant-replay"
PYTHONPATH=src ../testvenv/bin/python \
  "/Users/roxor/Documents/ChatGPT/rea L2/.tools/replay-packs/verify_replay_engine.py" \
  --pack "/Users/roxor/Documents/ChatGPT/rea L2/.tools/replay-packs/20261016-live-6stream" \
  --stocks 40 --json-out /tmp/r2-verify-40.json
```

## 9. 已知限制

- 回放源只支持「每流 1 页」的包：缓存包只录制了种子首包（`backfill=0`）。若采集器配置
  `--trade-pages/--order-pages > 1`，第 2/3 页请求没有对应录制组，回放会如实让其 15s 超时。
- 缓存包的控制帧是 R1 合成的 ack（`frames.synthetic_control=1`）；真实 ack 家族（含 887/2615
  字节族）只在现场 tap 录制里，需要控制帧保真时请用现场 tap 包。
- 扩流克隆是"同一条流的复制"，只用于验证采集/落盘/下游消费的伸缩性，不代表供应商对 40/80/100
  只标的的真实回执行为；真实档位结论仍以盘后或盘中的现场运行证据为准。
- 回放源不发任何网络包、不读凭据：`FeedCollector` 的请求只被 mock 连接吞掉，`sendall`
  永远不会到 `122.9.43.156`。
- 本卡未部署、未修改 `feed.py`/`cache.py` 的行为；新增代码默认不参与生产采集路径。
