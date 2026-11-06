# L2 段式 append 日志写盘路径（设计评审稿）

- 卡片：`card-a1f7899e`「P1 段式 append 写盘日志」
- 分支：`.tools/review/rquant-segments`，`feat/l2-segment-log`（基于 main `8de81101`）
- 状态：**设计评审稿，尚未实现**；本文档不部署、不碰生产
- 上游证据：`evidence/rquant-l2-write-path-2026-11-06.json`、`evidence/rquant-l2-scale80-window-2026-11-06.json`

## 1. 问题与目标

2026-11-06 80 股真实窗口实测：开盘 328 秒只写入 41,071 批（125 批/s），4 条泵队列积压 179,272 帧、
单链路 65,536 帧队列塞满，采集器 CPU 只有 29%（I/O 等待）。由此反算供应商开盘流入 ≈ **671.8 批/s**。

服务器真实磁盘微基准（`evidence/write-path-2026-11-06/`）：

| 路径 | 批/s | ms/批 |
| --- | --- | --- |
| 现行：每批 2 文件 + 4 次 fsync + 每批 status | 128–145 | ~7 |
| 已发布 opt-in：分组提交 + status 节流（2.0–2.2x；基准直调 write_status，实盘未复现） | 250–290 | 3.4–3.7 |
| 关闭 fsync（CPU/JSON 上限） | ~990 | 1.0 |

结论：逐批目录结构的安全 fsync 下限是 3 次/批（batch.json、sha256 各自 fsync + pending 目录 fsync，
batches/ 目录 fsync 已按组摊掉），架构上限约 290 批/s，**无法覆盖开盘 670 批/s**。

**目标**：开盘期 ≥700 批/s 持续写入（含 JSON 编码与提交开销），不丢数据、崩溃可恢复、读取方无感。
**非目标**：不改变行情解码/关联语义，不改变 `status.json` / `subscription-status.json` schema，
不在本卡切换 80 股档位（档位切换与验收部署另有卡）。

## 2. 设计概览

新增一种**可选**的缓存写入后端 `segments`，与现有 `batches/<seq>/` 目录后端并存；由缓存根下的
`.layout` 标记文件决定后端，读取方（`RawL2Cache`）对两种后端透明。默认仍为现有目录后端，
本卡所有新行为 opt-in（`--write-mode segments` / `RawL2Cache(..., write_mode="segments")`）。

```
cache/
  .layout                 {"schema":"rquant-l2-cache-layout-v1","mode":"segments"}（目录后端无此文件）
  .writer.lock            不变
  segments/
    index.jsonl           每关闭一段追加一行 {file,first_seq,last_seq,records,bytes,closed_at,sha256}
    head.json             原子快照：{committed_through, active_segment, active_bytes}（提交点更新）
    seg-000000000001-000000064100.l2s    已关闭段（只追加，完成后只读）
    .active.l2s           当前写入段（未关闭；尾部可能含未提交记录）
  status.json             不变
  subscription-status.json 不变
```

写入：采集器 `cache.append()` 把批次序列化为与 `batch.json` 完全相同的 canonical JSON 字节，
追加进当前段文件；**每 N 批或每 T 毫秒写一条 commit 标记并 fsync 一次**（默认 N=256、T=200ms，
每段 60s 或 64MiB 轮转）。段关闭时写 footer（记录数 + 全段 sha256）并原子追加 index 行。

读取：`RawL2Cache._snapshot()` / `read_batches()` / `iter_batches()` 在 segment 后端下从
`index.jsonl` + `head.json` 计算 `committed_through`，按序号定位段并顺序/随机读记录；
未提交尾（commit 标记之后）对读者不可见；只读到已提交且连续的前缀。

## 3. 段文件格式 `rquant-l2-segmented-log-v1`

```
header (32B): magic "RQSEG001" | version u16 | reserved u16 | segment_index u64 |
              first_sequence u64 | created_at_ns u64
record:
  u32   total_len（含头，8 字节起）
  u8    kind: 1=payload, 2=commit, 3=footer
  u32   crc32（kind+body，zlib.crc32）
  body: kind=payload → 与 batch.json 相同的 canonical JSON（末位 "\n"）；
        kind=commit  → {committed_through u64, wall_ns u64, records u64} + 追加 4B 段内偏移表? 否
        kind=footer  → {records u64, first_sequence u64, last_sequence u64, sha256(全段 payload 区)}
```

- 记录级 crc32 只做**帧完整性**；行情内容完整性仍由 payload 内 `response_sha256` 保证（不变）。
- commit 标记的 `committed_through` 必须等于此前最后一条 payload 的序号；读者只信最后一个 commit。
- footer 的 sha256 覆盖 header 之后至 footer 之前全部字节，用于冷存/归档前的一次性校验（可选）。
- 最多丢一个 commit 间隔内未提交记录（崩溃时截断到最后一个合法 commit），不会产生"半条可见"。

## 4. 写入路径

`SegmentWriter`（新增 `src/rquant/l2/segment_log.py`）负责：

1. 启动：读 `.layout`/`head.json`/`index.jsonl`，校验已关闭段连续；打开/恢复 `.active.l2s`
   （截断到最后一个合法 commit；若文件尾有未提交 payload → 丢弃并 fsync 截断）。
2. `append(batch)`：`record = frame(kind=payload, canonical_json)` → 写入段文件缓冲（不 fsync）；
   每 256 批或 200ms 写 commit 记录并 `fsync(fd)`（**每提交周期 1 次 fsync**）；更新 `head.json`
   （原子替换，2 次左右/s 的小文件 fsync 可忽略，实际与 commit 同批）。
3. 轮转：段内字节 ≥64MiB 或时长 ≥60s 时写 footer、fsync、关闭、追加 index 行、开新段。
4. 关闭（正常结束/停止）：写 footer + 关闭 + index 行 + head 最终快照；不再写批次目录。

与现有目录后端共用：`.writer.lock`（单写者）、`status.json` 写入（`write_status` 不变，仍由
采集器按 `--status-flush-interval` 节流）、`subscription-status.json` 回执、`floor` 语义。

**兼容点确认**：缓存的 `read_batches` 在写者持锁时也读 `status.json` 的 `last_sequence` 作为下界；
segment 后端下 `status.json` 仍由 `write_status` 更新，且同一次写里 `_position()` 会先强制提交
（复用写盘 v1 的"状态不得声称未提交批次"规则）。

## 5. 读取路径（对上层完全透明）

`RawL2Cache` 增加内部后端分派，公开方法签名与返回模型不变：

| 方法 | 目录后端（现状） | segment 后端（新增） |
| --- | --- | --- |
| `_snapshot()` | 列 batches/ 目录 + `.head` 审计 | index+head 得 committed_through/last_received_at；逐段校验连续 |
| `read_batches(after_sequence, limit)` | 读 `batches/<seq>/batch.json` | 二分定位段 → 顺序解码 payload（内存缓存段索引与读指针） |
| `iter_batches(after_sequence, limit)` | 逐目录 | 逐段 streaming（一次读一段，可跨段） |
| `read_status/read_runtime_status/read_subscription_status` | 不变 | 不变 |
| `floor_sequence()` | 不变（`.floor` 标记放在 cache 根，不放进 batches/） | 同左（segment 后端也读 cache 根 `.floor`；冷存删除前缀后由 retention 写 floor） |

- 混合布局（目录 + segments 同时存在）视为非法，`CacheError("mixed_layout")`；迁移只走显式工具。
- **读路径复核修正（2026-11-06 实测 grep）**：`api.py`、`replay_pack.py` 经 `RawL2Cache` 读，零改动；
  但下列模块**直接拼 `batches/<seq>` 路径**，P2 必须改成 layout 感知或经统一读接口：
  `archive.py:216`（`cache._read_batch(cache.root/"batches"/...)`）、
  `cold_store.py:963/978/1324`（压缩源读取与 restore 目标写目录）、
  `retention.py:155/570/773`（前缀删除与热字节统计）。
  因此 P2 的验收改为：三处直接路径改为「经 `RawL2Cache` 统一读 + segment 后端实现」，或实现
  `SegmentMaterializer` 把已提交前缀物化成 `batches/` 目录供旧工具过渡（两者二选一，倾向前者）。
- 读取延迟：段内顺序读 + 每段 64MiB 上限，`read_batches` 随机访问按段二分；实测目标 p99 < 50ms
  （API 页最多 128 批）。

## 6. 崩溃恢复矩阵（必须逐项测试）

| 场景 | 期望 |
| --- | --- |
| 段内 payload 写一半断电 | 截断到最后合法 commit；未提交记录不可见；重启后继续写同段或新段 |
| commit 写到一半（frame 不完整） | 同上（按 crc/长度丢弃尾） |
| footer 缺失（段未关闭但已有 commit） | 段按"未关闭段"处理：读到最后一个 commit；下次启动继续追加 |
| footer 完整但 index 行未写 | 启动扫描发现段文件闭合，补写 index 行（幂等） |
| index.jsonl 尾部行不完整 | 丢弃坏行；以段文件实际内容为准重建 |
| head.json 落后于实际 commit | 以段文件扫描结果为准修复 head（不回退已提交） |
| 磁盘写满 | `CacheError("cache_io")`，采集器按现状中止；不产生半条可见记录 |
| 段文件被篡改（crc/序号不连续） | `invalid_batch`，拒绝继续写，保留现场 |

## 7. 性能目标与门禁

1. **服务器真实盘基准**（复用 `write_path_bench.py` 改造为 `segment_bench.py`）：
   - 700 批/s 持续注入 1200s 合成负载 → 队列不增长、`committed_through` 持续前进；
   - 目标 ≥700 批/s；对比目录后端 130/290 基线；
   - 记录每次 commit 的 fsync 间隔、段大小、读延迟 p50/p99。
2. **回放门禁**：扩展 `replay_gate` 增加 segment 后端场景（healthy-40/80、确定性、种子故障形状），
   要求与目录后端结果一致（种子率/批次序列/延迟），并验证写盘吞吐提升。
3. **真实窗口**：40 → 80 股分两步，先在 40 股窗口验证 segment 后端不丢帧、API/归档/冷存在线工作，
   再上 80 股开盘窗口（需另卡审批/部署）。
4. 崩溃恢复矩阵单测（10+ 用例）+ 篡改/截断对抗用例。
5. 既有 864 项 L2 测试不变（默认目录后端）；新增 segment 专项测试 ≥40 项。

## 8. 分阶段计划

| 阶段 | 内容 | 交付 |
| --- | --- | --- |
| P1（本卡前半） | 格式 + `SegmentWriter` + 崩溃恢复 + 单测 | 可写段、可恢复；基准脚本 |
| P2 | `RawL2Cache` 读路径透明化 + API/归档/冷存/回放回归 | 读取方零改动可用 |
| P3 | 回放门禁扩展 + 真实 40 股窗口 + 部署/验收材料 | 80 股前置就绪 |

## 9. 风险与回滚

- 段内 commit 间隔（默认 200ms）决定崩溃可见性丢失窗口（≤200ms 的推流），与"丢失整段"相比可接受；
  可通过 `--segment-commit-ms` 调整（50–1000ms）。
- 读取侧新增段索引缓存，需保证与写者并发下的可见性一致（只信 fsync 过的 commit；读时不锁 writer）。
- 回滚：新后端仅 opt-in；生产若已切 segment 且需回滚，提供 `tools/l2-export-segments` 把段导出为
  目录结构（冷存/归档仍保留 R2 权威），并停止采集器后切换 `.layout`；本卡不改生产部署路径。
- 冷存/保留仍是"盘后压缩 + 前缀删除"，segment 后端下前缀删除=删除整段（粒度 64MiB/60s），
  与现有 6GiB 磁盘保留线兼容；retention 的 `floor` 写入改为按段边界前滚。

## 10. 待评审确认的问题

1. commit 间隔取 200ms（丢失窗口）是否可接受？还是 500ms / 1s（更省 fsync，但 API 可见延迟更大）？
2. 段文件 64MiB / 60s 轮转是否合适（冷存压缩块大小 512 批/块，需 ≥512 批/段才能合并；80 股开盘 60s≈40k 批，远大于此）？
3. 是否保留"段内 payload 也用 canonical JSON"（便于复用 `RawL2Batch` 模型与冷存/回放），
   还是用更紧凑的二进制列式编码（更快但需新解码器）？本设计选 JSON，理由是零解码器改动、CPU 上限 990 批/s 足够。
4. `read_batches` 随机访问是否需要段间索引常驻内存（80 股一天约 1–2 万段，索引约 2MB，可行）？
5. 是否需要同时保留"写目录 + 写段"双写过渡（用于灰度）？建议不需要：单写者、格式由 `.layout` 决定，切换=停采集器。

---

## 11. 独立对抗复核响应（2026-11-06，复核后修订）

独立复核结论：**方向成立，但需修改后进入实现**。5 条高优问题全部接受，逐条修订如下；
同时修正本文档 §1/§9 的错误假设。

### H1 每批强制写 status（已确认是真问题，必须修）

复核指出：`feed.py` 的 `_FORCE_STATUS_KEYS` 含 `state`，而每个 candidate_push 帧的
`_update(changes)` 都带 `state`/`subscription_state` → `force=True` → **每批写 status**。
这解释了为什么 80 股实盘 125 批/s 与「每批 2 文件 + status」基准 130 批/s 吻合，也说明
写盘 v1 的 `--status-flush-interval` 在真实路径上几乎没生效（基准是直接调 `write_status` 测的）。

修订：
- `_update()` 先与当前 `self.status` 比较，**只保留真正变化的字段**；`_FORCE_STATUS_KEYS`
  的强制落盘改为「字段值确实变化」才触发。
- 该比较逻辑只在 `status_flush_interval > 0`（快速模式）启用，`=0` 保持旧行为字节级不变。
- 段式后端 `_position()` 只返回 `committed_through`，不隐式 flush；`write_status` 前置一次
  `commit()`（约 1 次/秒 + 状态迁移），最终快照前强制 commit，回执（receipt）不写未提交序号。
- 段式后端要求 `--status-flush-interval 1.0`（否则每批仍会多一次 status fsync，达不到目标）。

### H2 head.json 的「已提交边界」

修订：`head.json` 增加 `committed_offset`（活跃段内已提交字节边界，含最后一个 commit 记录）、
`committed_records`、`last_received_at`、`index_entries`。读者只在
`offset < committed_offset` 范围内解析 payload，不再依赖序号比较推断可见性。

### H3 段内稀疏偏移索引

修订：每段旁边维护 `.idx`（活跃段 `.active.idx`），每次 commit 追加一条
`<first_sequence u64><offset u64>`（该 commit 周期第一条 payload 的偏移），二进制定长追加，
不单独 fsync（可重建）；段关闭时 fsync。读者按 `bisect` 定位到最近 checkpoint 再顺序扫描
≤1 个 commit 周期（≤256 批），`read_batches` 128 批页不再扫整段。缺失/损坏的 `.idx` 由读者
回退扫描重建并记录告警。

### H4 读取方并非零改动（实测 7 处）

已接受并改写 §5：新增后端中立读原语 `committed_head()` / `read_payload_bytes(seq)` /
`read_one(seq)` / `iter_batches(...)`，逐处改造：

| 位置 | 现状 | 改法 |
| --- | --- | --- |
| `archive.py:214-216` | `cache._read_batch(cache.root/"batches"/...)` | 经 `read_one/read_payload_bytes`；锚点摘要用存储原始字节 |
| `cold_store.py:961-987`（compact/verify） | 直读 batch.json 字节 | 经 `read_payload_bytes`，保持逐字节比对 |
| `retention.py:152-207 / 566-640 / 773` | 直读 batches/ 做前缀删除/热字节统计 | layout 感知：段后端按段边界向下取整删除、按段字节统计 |
| `replay_gate.py:628-633` | `_committed_batches` 读 batches/ | 经 `committed_head()`；否则段模式下恒 0 会直接判红 |
| `consumer_bench.py:299` | 直读 batches/ | 同上（研具，低优先） |
| `api.py` / `web/l2.py` / `replay_pack.py` | 已干净 | 无需改 |

### H5 容量账（复核后结论改变：由压缩解决，不引入盘中 prune）

实测：80 股窗口 41,071 批 → 缓存 **187,620,751 B = 4,568 B/批**；磁盘最小可用 29.38 GB。
按 670 批/s 推算原始 JSON 热数据 ≈ **11.0 GB/h → 4 小时 ≈ 44 GB ≫ 24–29 GB**——
**不做压缩时 80 股全天必然撞 5 GiB 写入保护**，写路径再快也没用。

实测同一批数据的组成：`table` JSON 占 79.6%、`raw_table_hex` 7.7%、`response_body_hex` 7.3%。
zlib level-1 压缩整条 canonical JSON：**9,293 B → 2,120 B（23%）**，按平均 4,568 B/批折算
≈ **1,050 B/批** → 670 批/s ≈ 750 KB/s ≈ **2.5 GB/h → 4 小时 ≈ 10 GB**，可放进现有 24–29 GB。
CPU 估算：4.5 KB/批 zlib-1 约 0.1 ms → 670 批/s 约 6–7% 单核，可接受。

修订：**payload 记录改为 zlib 压缩的 canonical JSON**（header 的 reserved u16 用作 codec 字段，
0=raw、1=zlib；读端按 codec 解压）。因此本卡**不需要**盘中在线 prune（那是 100 股/长期存储的
独立问题，另卡），80 股的磁盘可行性由压缩解决。§9 中「与 6GiB 保留线兼容」按压缩后重算：
80 股 4h ≈ 10 GB，保留线 6 GiB 仍偏紧 → 建议把热保留线改为 8–10 GiB 或在盘后尽快压缩+删除，
具体在 P3 部署前用真实窗口复核。

### 其他修订（中/低）

- **已闭合段禁止续写**、重启发现多个未闭合段 → fail-closed（`invalid_segment`），不猜测。
- 写序钉死：append → commit 记录 → fsync(段) → index 行(可选) → fsync(段目录) → head 原子替换；
  任何一步失败都不得推进 `committed_through`。
- 格式尺寸更正：header 36 B、记录头 9 B；`total_len` 参与 crc（复核 M3）。
- 完整性升级：commit 记录增加**链式 sha256**（每 commit 记录累计摘要，段尾 footer 再覆盖全段），
  读者可逐 commit 校验；payload 仍保留整批 `response_sha256`（不变）。
- `.floor` 实际在 `batches/.floor`（`cache.py:96`）：段式后端改为缓存根 `.floor`，旧目录后端不变；
  prune 阈值**向下取整到段边界**，活跃段永不删除。
- `_snapshot` 不在每次请求解析全量 index：`RawL2Cache` 层按 mtime 缓存 index+head（P2）。
- 迁移：`.layout` 决定后端；旧读端不认识 `.layout` → 切换后旧 API 报 503，**先升级读端再切写端**；
  提供 `tools/l2-export-segments`（段 → batch 目录）用于回滚。
- 不复用冷存 `.l2c` 段格式（列式压缩、用途不同）。

### 复核提出的 5 个拍板点与建议

| 拍板点 | 建议 |
| --- | --- |
| status/receipt 允许滞后 200ms？ | 允许（验收要求 lag ≤5s；迁移仍强制落盘） |
| 是否盘中 prune？ | 本卡不做（压缩已解决 80 股容量）；100 股/长期另卡 |
| 完整性级别 crc32 vs 链式 sha256？ | 用链式 sha256（CPU 可忽略） |
| 导出工具是否进本卡？ | 进（回滚/迁移必需，小工具） |
| 是否复用冷存段格式？ | 不复用 |
