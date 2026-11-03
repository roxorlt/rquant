# L2 原始帧录制器（Raw Frame Tap）与回放包

本文说明 L2 接收层的零阻塞原始帧录制器、录制文件格式，以及从存量缓存构建确定性回放包的工具。目标是一次盘中采样、盘后多次重放，减少对交易时段的依赖。

## 1. 模块与开关

| 项 | 位置 |
| --- | --- |
| 录制器与格式 | `src/rquant/l2/frame_tap.py` |
| 回放包构建/校验 | `src/rquant/l2/replay_pack.py` |
| 接收层接入 | `src/rquant/l2/feed.py`（`_send` / `_receive` / `_epoch` / `run`） |
| 命令行接入 | `src/rquant/l2/__main__.py`（`--frame-tap-dir` 等） |
| 缓存只读迭代 | `src/rquant/l2/cache.py`（`RawL2Cache.iter_batches`） |
| 单测 | `tests/unit/test_l2_frame_tap.py`、`tests/unit/test_l2_replay_pack.py` |

默认关闭。`RQUANT_L2_FRAME_TAP_DIR` / `--frame-tap-dir` 都是**根目录**：每次进程启动会在其下
新建 `<UTC时间戳>-p<pid>` 子目录，便于长期常驻时按运行轮次分开归档。三种等价开启方式：

```bash
# 1) 环境变量（适合 systemd unit）
RQUANT_L2_FRAME_TAP=1 RQUANT_L2_FRAME_TAP_DIR=/data/rquant-frame-tap \
  python -m rquant.l2 --cache-root ... --bootstrap-file ... --order-template ... --trade-template ...

# 2) 只给目录也会开启
RQUANT_L2_FRAME_TAP_DIR=/data/rquant-frame-tap python -m rquant.l2 ...

# 3) 显式 CLI 参数（每个进程生成一个唯一子目录）
python -m rquant.l2 ... --frame-tap-dir /data/rquant-frame-tap
```

可选环境变量：`RQUANT_L2_FRAME_TAP_SEGMENT_BYTES`（默认 256 MiB）、
`RQUANT_L2_FRAME_TAP_QUEUE_BYTES`（默认 8 MiB）、`RQUANT_L2_FRAME_TAP_FSYNC_SECONDS`（默认 5 秒）。
配置非法时命令行直接报错；运行期写盘失败只记入统计与 manifest，不终止行情采集。

## 2. 录制格式 `rquant-l2-frame-tap-v1`

所有整数为小端。一个 segment 文件自包含 header、记录流与 footer。

```text
header (40 B)
  0..8    magic  "RQL2TAP1"
  8..10   version u16 = 1
  10..12  flags u16 = 0
  12..16  meta_len u32
  16..24  started_wall_us u64   Unix 纪元微秒
  24..32  started_mono_us u64   开机单调时钟微秒
  32..40  reserved
meta JSON（紧凑 UTF-8，含 format/created_at/pid/clock 等）

record
  kind        u8      1=收到的原始帧 2=发出的原始帧 3=事件 JSON
  delta_us    uvarint 距上一条记录的单调时钟微秒；第一条相对 header.started_mono_us
  payload_len uvarint
  payload     kind 1/2 为完整 FD 帧字节（4×0xfd + 8 位十六进制长度 + 0x00 + body）；
              kind 3 为事件 JSON

footer (48 B)
  magic "RQL2END1"
  record_count u64
  sha256(header + meta + records) 32 B
```

要点：

- **原始帧无损**：记录的 payload 是套接字上观察到的完整 FD 帧（13 字节前缀 + body），不重新编码、不丢控制帧；零长度心跳帧也保留。
- **微秒单调时钟**：每条记录带相对上一条的微秒差；跨 segment 的单调时间连续，读取方累计即可还原绝对微秒时间。
- **紧凑**：典型记录开销 = 1 字节 kind + 1–2 字节 delta + 2 字节长度 ≈ 4–5 字节/帧。
- **可校验**：footer 覆盖整段内容的 sha256，截断或篡改会报 `checksum_mismatch` / `truncated`。
- **滚动分段**：按 `segment_bytes` 轮转，文件命名 `segment-000001.tap`；目录内 `manifest.json` 汇总各段记录数、字节数与 sha256。

## 3. 接收层接入点

`FeedCollector` 新增可选参数 `frame_tap`，只在三处埋点，全部为 O(1) 入队操作：

| 位置 | 记录内容 |
| --- | --- |
| `_send()` | 种子/回填请求的完整 wire 帧（TX） |
| `_receive()` | 每个读出的帧 body（RX），含种子回执、ack、心跳与行情推送 |
| `_epoch()` | `epoch_start` / `epoch_end` 事件（epoch_id、symbols、状态、批次数） |

`run()` 启动录制线程，退出时 flush + fsync + 写 manifest；录制器启动失败时自动降级为不录制，行情采集继续。

## 4. 零阻塞语义

录制器只做三件事：校验、入有界队列、唤醒写线程。磁盘写入、分段轮转与 fsync 全部在单独的写线程中完成。

- 队列上限：`queue_bytes`（默认 8 MiB）+ `queue_records`（默认 65536），先到先限。
- 队列满时**丢弃新帧并计数**，在后续记录流中插入 `{"event":"gap","dropped_frames":N}`，绝不让接收循环等待磁盘。
- 写线程异常（磁盘满等）只写进 `stats` 与 manifest 的 `error` 字段，不影响采集与缓存落盘。

本机实测（`--sustained` 见 `docs` 引用的证据文件）：

| 场景 | 提交量 | 丢弃 | 提交延迟 p99 | 写盘 |
| --- | ---: | ---: | ---: | --- |
| 4,000 帧/秒持续 5 秒 | 20,000 | 0 | 20.3 µs | 全部写出（3.4 MB） |
| 极限突发 | 60,000 | 11,452（队列满） | 1.75 µs | 48,548 帧（8.3 MB） |
| 写线程卡死 1.5 秒 | 40,000 | 38,511（队列满） | 1.7 µs | 1,489 帧 |

结论：接收线程的提交耗时与磁盘状态无关；队列满表现为丢弃计数，不表现为延迟。

## 5. 从存量缓存构建回放包

缓存批次里已保存 `response_body_hex` 与 `response_sha256`，因此不需要等下一个交易日：

```bash
# 构建（输出目录必须不存在或为空）
PYTHONPATH=src python -m rquant.l2.replay_pack build \
  --cache-root /path/to/cache --out /path/to/pack \
  [--start 1] [--end 3382] [--segment-bytes 268435456] [--no-synthetic-ack]

# 校验（重新读段、校验 footer sha256、比对 manifest 计数）
PYTHONPATH=src python -m rquant.l2.replay_pack verify --pack /path/to/pack
```

构建过程逐批校验 `sha256(body) == response_sha256`，并重建精确的 FD 帧。产物：

```text
pack/
  manifest.json        # 帧数、段 sha256、symbol→page 映射、时间范围、读取契约
  segment-000001.tap   # 与现场录制完全相同的 tap 格式
  ...
```

manifest 关键字段：`frames.rx/market/synthetic_control`、`delivery.seed/backfill/candidate_push`、
`symbols`、`epoch_ids`、`timing.duration_us`（真实采样时长，回放倍速由下游决定）、
`reader.module/callable`。相同缓存重建两次，segment 字节与 sha256 完全一致。

已知差异：存量缓存只保存行情帧，**不保存控制帧**。因此缓存回放包会合成一条结构合法的种子回执 ack（`frames.synthetic_control = 1`，用于让接收端完成 bootstrap 握手），并在 manifest 中标注。需要控制帧逐字节保真时，使用同一天的现场 tap 录制。

## 6. 下游读取契约

```python
from rquant.l2.frame_tap import iter_records, read_manifest

manifest = read_manifest(pack_dir)
for segment in manifest["segments"]:
    for record in iter_records(pack_dir / segment["file"]):
        record.kind      # 1=RX 2=TX 3=event
        record.mono_us   # 微秒单调时间（跨段累计）
        record.payload   # kind 1/2 为完整 FD 帧字节；kind 3 为事件 JSON
```

回放引擎（R2）用它构造确定性数据源：RX 帧写回客户端套接字/注入接口，TX 帧可做请求门控
（请求-响应配对）或时序校验，`mono_us` 差值即原始节奏，倍速回放只需按比例缩放。

## 7. 已知边界

- 只录 L2 接收/发送帧，不含 HTTP 管理面与缓存状态文件。
- 现场 tap 的 `manifest.json` 在正常 close 时生成；进程被 SIGKILL 时最后一段可能缺 footer（读取方会报错，可通过 `verify` 定位）。
- 缓存回放包依赖缓存本身完整（`sequence_gap` 的缓存无法构建）。
- fsync 间隔内崩溃可能丢失最后几秒记录；该窗口由 `RQUANT_L2_FRAME_TAP_FSYNC_SECONDS` 控制。
