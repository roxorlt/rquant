# L2 冷存储与滚动保留（`rquant-l2-cold-v1`）

- 卡片：`card-3537c8dc`（每日缓存占用与存储方案，80/100 前置）
- 日期：2026-11-03
- 状态：**本机实现并实测；未部署服务器，未改生产缓存布局**（默认不启用，须走受控发布链路）
- 上游：R1 原始帧录制器（`card-6ec91ec7`，`feat/l2-raw-frame-tap`）；本模块在其之上，不依赖 frame tap。

## 1. 问题：写盘体积的三个来源

用 4 份真实缓存（2026-10-16 采集的 2 股 6 路、每份约 180 秒，共 9,895 批）实测：

| 指标 | 实测 |
| --- | --- |
| `batch.json` 逻辑体积 | 31.84 MB（平均 3,218 B/批） |
| 磁盘分配体积（`du` 口径） | 84.86 MB（平均 8,575 B/批） |
| 分配 / 逻辑 | **2.72x** |

| 来源 | 实测占比 | 说明 |
| --- | --- | --- |
| 解析后的 typed table JSON | 60.7%–68.8% | 每行重复保存 `raw_hex` + `value` + `style` 和属性名 |
| `response_body_hex` + `raw_table_hex` | 21%–24% | 同一份行情信息的另外两份无损表示，十六进制让字节数翻倍 |
| 元数据（epoch/binding/时间/序号/hash） | 约 15% | 其中 `response_sha256` 可由 body 重算 |
| **小文件开销（不在 JSON 里）** | 分配体积的 47.8% | 每批两个 inode：`batch.json` + 65 B 的 `sha256`，各自至少 4 KiB 块 |

推算到 40 股 20 分钟实跑（`.tools/review/scale40-C-20261029/results.json`：157,051 批、555,449,247 B、314,106 个文件）：分配 ≈ 157,051 × (4 KiB sha + 1.392 × 逻辑) = **1.32 GiB**，与卡片记录的 ~1.4 GB 一致（1.011x），折合 **3.96 GiB/小时**。

关键结论：**即使把 JSON 里的冗余表示全删掉，每批两个 4 KiB inode 仍要 8 KiB**；要真正省盘，必须让“每批两个小文件”变成“大文件里的一条记录”。

## 2. 方案：热缓存不动，历史前缀转冷存

```
盘中：FeedCollector → cache/batches/<20位序号>/batch.json + sha256   （现有布局，API 不变）
盘后：archive（现有 R2 归档，不动）
      └→ retention/compact：读已提交前缀 → cold/segments/*.l2c（大文件、列式、分块 zlib）
                              └→ 读回逐批校验通过 → 写 manifest（verified_at）
                                   └→ 写 cache/batches/.floor → 删除 ≤ floor 的批次目录
长期：cold/segments 按天数/体积滚动淘汰；R2 归档仍是唯一长期权威
```

- **热路径零改动**：采集器仍写现有 `batch.json`；冷存只处理“已提交、已归档”的旧前缀。
- **冷存只保存线上原始帧**（`response_body` 的二进制字节）+ 少量身份/时间字段；`raw_table_hex`、typed table、`response_sha256` 在读取时用现有 codec 重新推导。已用 9,895/9,895 批实测证明推导结果与落盘内容**逐字节一致**。
- **可回读**：`restore` 可把冷存还原成完全相同的 cache 目录树（`RawL2Cache` 能直接读），`iter_batches` 可直接流式喂给回放/重放引擎。

## 3. 格式 `rquant-l2-cold-v1`

段文件（`segments/l2cold-<首序号>-<末序号>-<sha前12位>.l2c`）：

| 区段 | 内容 |
| --- | --- |
| Header（4096 B） | magic `RQL2COL1`、u16 version、u16 flags、u32 meta_len、meta JSON（schema/codec/block_records/首末序号/记录数/epoch 列表/源 cache 路径） |
| Block × N | `u32 stored_len | u32 raw_len | u8 codec | u8/u16 reserved` + 压缩后的列式块 |
| Index | 每块 offset/stored_len/raw_len/codec/sha256/首序号/记录数 |
| Footer | `sha256(header+blocks+index)`（32 B）+ `u32 index_len` + magic `RQL2CEND` |

块内是列式的（每次 512 条，可配）：序号增量、接收时间增量（µs）、`cached_at` 相对接收时间的偏移、epoch 字典下标、binding 字典下标（kind/code/market/side/page）、request_id、source fd、delivery、body 长度，最后是拼接的 body 字节。字典按块去重，时间/序号用 varint，body 已由供应商 Snappy 压缩过，块级 zlib 主要压元数据列。

- codec：`none` / `zlib`（默认 level 6）/ `gzip`。部署 venv 未安装 zstd/lz4，故默认用标准库；块头留有 codec 编号，后续可加 zstd。
- 完整性：整段 sha256 + 每块 sha256 + 恢复时逐批比较源 `batch.json` 字节；任一处不符立即报 `segment_corrupt` / `payload_mismatch`。
- 丢弃策略：写失败/校验失败不会写入 manifest；manifest 里没有 `verified_at` 的段永远不会被自动删除。

## 4. 保留与清理规则

`RetentionPolicy`（`src/rquant/l2/retention.py`）默认值针对 29 GiB 余量：

| 参数 | 默认 | 作用 |
| --- | --- | --- |
| `reserve_free_bytes` | 6 GiB | 任何压缩/淘汰后必须保留的空闲空间（采集器自身还有 5 GiB 写入保护） |
| `hot_keep_hours` | 2 h | 最近这段热数据不压缩，留给实时 API/排查 |
| `hot_max_bytes` | 12 GiB | 热缓存超过该体积时提示需要压缩（压缩本身可在线做，删前缀需要采集器停写） |
| `cold_keep_days` | 30 天 | 冷段最少保留天数（更年轻的段不会被自动淘汰） |
| `cold_max_bytes` | 6 GiB | 冷段总量上限，超过时从最老的已验证段开始淘汰 |
| `archive_required` | 是 | 只有 R2 归档回执（`last-run.json`，`status∈{archived,unchanged}`）覆盖到的前缀才允许从热缓存删除 |

强制顺序（`run_retention` 一次跑完）：

1. **先压缩**：`floor+1 .. 热窗口边界` 的已提交批次写冷段；
2. **再校验**：读回冷段与源 `batch.json` 逐批比较，通过才写 manifest；
3. **再归档门禁**：`archived_through` 必须 ≥ 待删前缀；不足则只压缩不删除；
4. **再删前缀**：非阻塞独占 `.writer.lock`（采集器在跑就跳过删除），先原子写 `batches/.floor`，再删目录；
5. **最后淘汰冷段**：只淘汰“已验证且超过 `cold_keep_days`”的段，最老优先，且不得突破磁盘保留线。

`.floor` 是新增的只读标记（`batches/.floor`，JSON `{"schema":"rquant-l2-cache-floor-v1","pruned_through":N,...}`）：读路径把 ≤N 的序号视为“已主动退役”，不再报 `sequence_gap`；写入进程重启后从 `N+1` 继续编号。保留最新 1 批热数据，`.head` 硬链接永远不会悬空。

### 运行排期（建议）

| 时间 | 作业 | 说明 |
| --- | --- | --- |
| 17:20 | `rquant-l2-archive.timer`（现有） | 归档到 R2，写 `last-run.json` |
| 18:30 | `rquant-l2-retention.timer`（待部署） | `python -m rquant.l2.retention --cache-root ... --cold-root ... --archive-state-root ...`；一次压缩+校验+删前缀+淘汰冷段 |
| 09:00 | 同一命令（可选） | 盘前检查热缓存余量；采集器在跑时只压缩不删除 |

80/100 股若出现“单日峰值超过热缓存上限”，在两个交易时段之间（11:30–13:00）停采集器 → 跑一次 retention → 13:00 前重启，即可在盘中释放空间。是否需要这步由实测决定（见 §6 容量表）。

systemd 单元（仅示意，部署前必须按 AGENTS 规则在云端 `systemd-analyze calendar` 验证）：

```ini
[Service]
Type=oneshot
User=lighthouse
Environment=RQUANT_DISABLE_DOTENV=1 TZ=Asia/Shanghai
ExecStart=/home/lighthouse/rquant-l2-retention-rel/current/.venv/bin/python -m rquant.l2.retention \
  --cache-root /home/lighthouse/rquant-l2-state/cache \
  --cold-root /home/lighthouse/rquant-l2-state/cold \
  --archive-state-root /home/lighthouse/rquant-l2-archive-state
ReadWritePaths=/home/lighthouse/rquant-l2-state/cache /home/lighthouse/rquant-l2-state/cold
Nice=15
CPUQuota=20%
MemoryMax=512M
```

## 5. 用法

```sh
# 压缩（默认校验、默认 512 条/块、64 MiB/段）
python -m rquant.l2.cold_store compact \
  --cache-root /path/cache --cold-root /path/cold \
  [--after-sequence N] [--until-sequence M] [--block-records 512] \
  [--codec zlib|gzip|none] [--level 6] [--segment-bytes 67108864] [--no-verify]

# 校验（有源缓存时逐批字节比较）
python -m rquant.l2.cold_store verify --cold-root /path/cold [--cache-root /path/cache]

# 还原成 cache 布局（可直接被 RawL2Cache 读取；partial 段会写 .floor）
python -m rquant.l2.cold_store restore --cold-root /path/cold --destination /path/restored [--overwrite]

python -m rquant.l2.cold_store stats --cold-root /path/cold

# 滚动保留（先 plan 看会做什么，再执行；--dry-run 不落盘）
python -m rquant.l2.retention --cache-root /path/cache --cold-root /path/cold \
  --archive-state-root /path/archive-state [--dry-run]
```

Python API：`compact_cache`、`verify_segment`、`restore`、`iter_batches`、`read_batch`、`stats`、`load_manifest`（`rquant.l2.cold_store`）；`plan_retention`、`run_retention`、`RetentionPolicy`、`hot_usage`、`archive_coverage`（`rquant.l2.retention`）。

## 6. 实测结果（本机，2026-11-03）

证据：`evidence/rquant-l2-cold-store-bench-2026-11-03.json`、`evidence/rquant-l2-retention-drill-2026-11-03.json`、`evidence/rquant-l2-storage-volume-analysis-2026-11-03.json`、`evidence/rquant-l2-storage-capacity-2026-11-03.json`（均在主工作区）。

| 项 | 数值 |
| --- | --- |
| 输入 | 4 份真实缓存，9,895 批（500 行业务 + 20/640 行委托与成交，含种子/推送） |
| 源逻辑 / 分配 | 31.84 MB / 84.86 MB |
| 冷存 stored / 分配 | **318,129 B / 344,064 B** |
| 逻辑下降 | **99.00%**（100.1x） |
| 磁盘分配下降 | **99.59%**（246.6x） |
| 每批 | 3,218 B → **32.15 B** |
| 无损校验 | 9,895/9,895 批 `batch.json` **字节一致**；模型序列化一致；行数一致 |
| 吞吐 | 压缩 2,905 批/s，还原 3,402 批/s；段级校验 1.26 s |
| 保留演练（400 批真实缓存副本） | 压缩 399 批、删除 399 批、热缓存分配下降 99.47%、冷存 20,959 B、0 失配、第二次运行 no-op、删前缀后归档 `_source` 仍通过 |

验收线“同数据量落盘体积下降 ≥60%”：逻辑体积 −99.0%、磁盘分配 −99.6%，**远超要求**。需要说明：这是本机真实缓存上的离线压测，不是服务器实盘；40 股实跑缓存不在本机，容量表用同一批帧的构成模型外推（与 20 分钟实跑误差 1.1%，见下）。

## 7. 容量估算（40 / 80 / 100 股）

口径：40 股 20 分钟开盘实测 157,051 批 / 555.4 MB；冷存按实测 32.15 B/批 × 1.5 安全系数；热缓存按 157,051 批 × (4 KiB sha + 1.392 × 逻辑) 建模。

| 股数 | 20 分钟开盘实测（逻辑/分配） | 全天峰值线性（分配） | 全天活动加权（分配） | 冷存全天·峰值线性 | 冷存全天·加权 | 冷存 30 天·峰值线性 |
| --- | --- | --- | --- | --- | --- | --- |
| 40 | 529.7 / 1350.8 MiB | 15.83 GiB | 3.38 GiB | 86.7 MiB | 18.1 MiB | 2.54 GiB |
| 80 | 1059.4 / 2701.7 MiB | 31.66 GiB | 6.75 GiB | 173.4 MiB | 36.2 MiB | 5.08 GiB |
| 100 | 1324.3 / 3377.1 MiB | 39.58 GiB | 8.44 GiB | 216.7 MiB | 45.3 MiB | 6.35 GiB |

- “全天峰值线性”= 开盘 20 分钟速率 × 12（4 小时交易时段），是**上限**（卡片原口径）。
- “全天活动加权”= 开盘 30 分钟按实测速率 + 其余 3.5 小时按 2026-10-19 归档日（20 股、约 2 小时午后、175,149,916 B）推出的每股价速率。只有一个午后的真实样本，**参考值**，需用 40 股整日实跑替换。
- 冷存 30 天最坏（线性峰值）在 100 股时 6.35 GiB，正好是 `cold_max_bytes=6 GiB` 的边界，策略会按最老优先把冷存压到 6 GiB 以内（30 天保留会略缩短）。

门禁建议：

| 档位 | 建议 |
| --- | --- |
| 40 股 | 可长期跑；每晚 18:30 retention + 09:00 盘前检查即可；即使按峰值线性一天 15.8 GiB 也在 29 GiB 内 |
| 80 股 | 峰值线性 31.66 GiB **超过** 29 GiB 余量 → 必须先启用 18:30 夜间保留；若实测开盘速率与 40 股同倍增长，需加 11:30–13:00 午休窗口压缩/删前缀（或等热写路径瘦身，后续卡） |
| 100 股 | 峰值线性 39.58 GiB 明显超限；除夜间保留外，建议先做“热写路径瘦身/分段 append”（把每批 2 个 4 KiB inode 变成大文件追加）再上，否则单日峰值可能触发采集器 5 GiB 写入保护 |

## 8. 未做与限制

- **未部署**：代码在 worktree 分支 `feat/l2-cold-store`，未合 `main`、未打 tag、未 push、未重启任何服务；服务器缓存布局未改。
- **未在服务器实跑**：本机压测数据来自 10/16 的 2 股 6 路缓存；40/80/100 股整日容量需用真实整日缓存复核（重点：开盘 20 分钟速率是否随股数线性增长）。
- 压缩仅在**采集器不在写同一前缀**时安全；删除前缀（prune）要求采集器停写，retention 会用 `.writer.lock` 非阻塞检测并跳过。
- 冷存是本机中间层，不替代 R2 归档；R2 归档仍是长期权威，retention 的删除门禁依赖归档回执。
- 未安装 zstd/lz4：zlib-6 与 gzip-6 实测差距 <1%，默认 zlib；部署 venv 若以后加 `zstandard`，块头 codec 已预留。
- 随机读按块（默认 512 批 ≈ 16–32 KiB）解码，不做段内单条随机索引；回放/还原走顺序流。
- 冷段不自带“来源 epoch 时间范围”索引，`stats` 只汇总体积/记录/序号；跨天检索仍需 R2 catalog。

## 9. 测试

```sh
# 新模块（35 个）
/Users/roxor/Documents/ChatGPT/rea L2/.tools/review/run-l2-cold-store-tests.sh

# L2 全量回归（567 项；不含 test_l2_web/test_storage_pool2，本机 venv 缺 duckdb）
/Users/roxor/Documents/ChatGPT/rea L2/.tools/review/run-l2-cold-store-tests.sh \
  tests/unit/test_l2_archive.py tests/unit/test_l2_archive_deploy.py \
  tests/unit/test_l2_cache.py tests/unit/test_l2_codec.py tests/unit/test_l2_feed.py \
  tests/unit/test_l2_frame_tap.py tests/unit/test_l2_replay_pack.py \
  tests/unit/test_l2_subscription_api.py tests/unit/test_l2_subscriptions.py \
  tests/unit/test_l2_symbols.py tests/unit/test_l2_cold_store.py \
  tests/unit/test_l2_retention.py tests/unit/test_l2_cache_floor.py -q

# 压测 / 保留演练 / 容量表（主工作区）
PYTHONPATH=.tools/review/rquant-cold-store/src \
  .tools/review/testvenv/bin/python .tools/cold-store/bench_cold_store.py --help
```
