# 研究数据首次迁云操作手册

本文用于把 Mac 上的历史分钟、集合竞价、模拟盘和 Strategy Lab 记录迁到腾讯云，形成
“云端候选权威”。这是一次性 bootstrap，不是生产库部署命令。

## 先理解四个阶段

1. `snapshot`：让本地 DuckDB 落完 WAL，再复制成只读恢复快照；同时记录 Strategy Lab
   文件清单和内容哈希。之后所有导出都读这一个冻结代际，Lab 文件在快照后发生变化会拒绝
   `prepare`，不会把两个时点拼成一个迁移包。数据库正式改名前会先持久化 pending 证据；即使
   在数据库与 `snapshot.json` 之间崩溃，重跑也只能用原证据恢复，不能重新绑定新 Lab 文件。
2. `prepare`：从恢复快照导出按交易日分区的 Parquet、7 张辅助研究表和 Lab 文件；同时生成
   行数、日期、主键、金额/成交量汇总、固定样本和 SHA-256 证据。
3. `upload`：用 `rsync --archive --partial --checksum` 上传到快照专属 staging。网络中断后
   重跑只传缺失或不同的文件，staging 永远不作为正式数据目录。
4. `publish`：云端只做一次完整证据校验，逐文件原子发布，重建 `research.duckdb`；最后一步
   才写 `research-authority-candidate.json`。这一步不会打开、替换或写入生产
   `rquant.duckdb`。

## 迁移内容

| 内容 | 云端位置 | 说明 |
|---|---|---|
| `minute_bar` | `data/lake/minute/...` | 交易日 + 频率分区，不可变版本 Parquet |
| `auction_bar` | `data/lake/auction/...` | 交易日分区，不可变版本 Parquet |
| 7 张辅助表 | `data/lake/snapshots/...` | monitor 事件、分钟特征、模拟仓/事件、数据快照/覆盖率/质量问题；入场、离场、完成、创建、更新等事实时间任一越过结束日都会排除整行 |
| Lab 历史文件 | `data/research_artifacts/snapshot_id=.../` | JSON、Markdown、日志等原文件 |
| 研究目录 | `data/research.duckdb` | 由云端已验证 manifest 重建，不直接复制本地 catalog |

价量分布当前由分钟线按参数重算，没有独立存量表。生产日线、池子、筛选结果和通知状态不进入
迁移包，也不会覆盖云端生产表。

## 执行条件

- 迁移代码必须已合入、CI 全绿，并以精确 tag 部署到 Mac 当前 checkout 和腾讯云。
- 只在工作日 15:10 后或非交易日开始 `prepare`；本地 `rquant monitor` 和 Strategy Lab
  worker 必须已停止。
- Mac 空闲空间至少为本地主库大小的 2 倍再加 1GiB；云端空闲空间至少为迁移包大小的 2 倍
  再加 1GiB。脚本会在写入前再次检查，不满足就退出。
- Mac 不应在准备阶段休眠。建议接电并使用 `caffeinate`。
- 发布属于生产数据目录写入，首次实际执行前仍需单独确认；dry-run、校验和上传 staging 不会
  发布正式研究数据。

## 1. 确定日期与快照 ID

在 Mac 项目目录运行：

```bash
cd /Users/roxor/brain/30-projects/rQuant

.venv/bin/python - <<'PY'
import duckdb

path = "/Users/roxor/brain/30-projects/rQuant/data/rquant_ro.duckdb"
with duckdb.connect(path, read_only=True) as conn:
    for table, expression in (
        ("minute_bar", "CAST(trade_time AS DATE)"),
        ("auction_bar", "trade_date"),
    ):
        row = conn.execute(
            f"SELECT MIN({expression}), MAX({expression}), COUNT(*) FROM {table}"
        ).fetchone()
        print(table, row)
PY

export SNAPSHOT_ID="research-$(date -u +%Y%m%dT%H%M%SZ)-$(git rev-parse --short=8 HEAD)"
echo "${SNAPSHOT_ID}"
```

记下两张表共同需要覆盖的最早和最晚日期。快照 ID 后续三阶段必须完全一致。

## 2. 先做 dry-run

下面的日期只是格式示例，执行时替换成上一步真实范围：

```bash
bash scripts/migrate-research-to-cloud.sh \
  --phase all \
  --snapshot-id "${SNAPSHOT_ID}" \
  --start-date 2025-01-16 \
  --end-date 2026-07-16 \
  --dry-run
```

核对输出必须满足：

- 源库是 Mac 的 `data/rquant.duckdb`；
- 上传目标包含 `data/research-staging/${SNAPSHOT_ID}`；
- `prepare`/`upload` 会出现显式 `research-migration verify`；远端
  `research-migration publish ... --apply` 内部会在发布前再完整校验一次，不重复跑第二遍；
- publish 仅允许工作日 15:10 后或非交易日启动，并包含 `rquant-monitor.service`、远端实时
  空间门禁和 6 小时硬超时，不允许 09:14 启动后跨入交易窗口；
- 不出现把任何文件 rsync 到云端 `data/rquant.duckdb` 的命令。

## 3. 分阶段正式执行

### 3.1 准备恢复快照和迁移包

```bash
mkdir -p /Users/roxor/brain/30-projects/rQuant/logs

caffeinate -dimsu bash scripts/migrate-research-to-cloud.sh \
  --phase prepare \
  --snapshot-id "${SNAPSHOT_ID}" \
  --start-date 2025-01-16 \
  --end-date 2026-07-16 \
  2>&1 | tee "logs/research-migration-${SNAPSHOT_ID}-prepare.log"
```

失败时不要删除 `data/research_migration/`。修复首个错误后用相同参数重跑；恢复快照和完整迁移包
都会返回 `unchanged`，半成品临时目录不会被发布。

### 3.2 断点续传到云端 staging

```bash
caffeinate -dimsu bash scripts/migrate-research-to-cloud.sh \
  --phase upload \
  --snapshot-id "${SNAPSHOT_ID}" \
  2>&1 | tee "logs/research-migration-${SNAPSHOT_ID}-upload.log"
```

SSH 页面或网络断开不影响已完成文件。恢复网络后原命令重跑即可，不要手工移动 staging 文件。

### 3.3 记录生产库哈希并发布候选

发布前记录生产库哈希：

```bash
ssh lighthouse@82.156.0.68 \
  'sha256sum /home/lighthouse/rquant/data/rquant.duckdb' \
  | tee "logs/research-migration-${SNAPSHOT_ID}-production-before.sha256"
```

取得生产数据目录写入确认后执行：

```bash
bash scripts/migrate-research-to-cloud.sh \
  --phase publish \
  --snapshot-id "${SNAPSHOT_ID}" \
  2>&1 | tee "logs/research-migration-${SNAPSHOT_ID}-publish.log"
```

如果最后一步前断线或进程退出，使用相同命令重跑。发布状态文件只允许同一个快照续跑；云端已有
不属于本次迁移的 `research.duckdb` 或 minute/auction 分区时会失败关闭，不会擅自吸收到本次
候选。即使崩溃前已有部分 Parquet/manifest 落到 lake，它们也只是未完成 staging；任何消费者
都必须先验证候选标记、bundle hash 和 `catalog_sha256`，不得直接扫描 lake 判定权威代际。
当前版本只发布迁移工具，没有把 Dashboard、Lab 或回测 worker 改成 lake 消费者；后续接入
消费者时必须先实现候选验证入口，禁止直接用 `read_parquet('lake/**/*.parquet')` 通配读取。

## 4. 发布验收

```bash
ssh lighthouse@82.156.0.68 \
  'sha256sum /home/lighthouse/rquant/data/rquant.duckdb'

ssh lighthouse@82.156.0.68 \
  'cat /home/lighthouse/rquant/data/research-authority-candidate.json'

ssh lighthouse@82.156.0.68 \
  'cd /home/lighthouse/rquant && PYTHONPATH=src .venv/bin/rquant \
   research-migration verify \
   --bundle-path /home/lighthouse/rquant/data/research-staging/'"${SNAPSHOT_ID}"
```

必须同时满足：

1. 发布前后的生产 `rquant.duckdb` SHA-256 完全相同。
2. 候选标记的 `snapshot_id`、代码 commit、bundle hash 和 `catalog_sha256` 与本次一致；重复
   publish 会重新验证 catalog 文件哈希。
3. verify 返回 `status=verified`，样本匹配数等于样本数，重复主键为 0。
4. `research.duckdb` 中 minute/auction 的分区数与行数等于 bundle manifest。
5. 主库恢复快照、迁移包、上传日志和发布日志都仍保留。

## 5. 何时可以删除 Mac 主库

发布成功不等于可以删除。候选阶段至少保持 10 个交易日，并完成后续“云端每日增量与服务迁云”
任务。只有以下条件全部成立，才生成本地删除 dry-run 清单：

- 云端连续 10 个交易日的分钟、竞价和模拟盘增量无缺口；
- 云端原始分区、研究目录快照和异机备份三份证据都可恢复；
- Mac 停止研究采集后，云端连续 5 个交易日仍不低于原本地覆盖率；
- Strategy Lab 已改读云端研究只读副本，常用回放结果可复现；
- 用户再次明确确认删除。

在此之前，Mac 的 `rquant.duckdb` 和恢复快照都不得删除。

## 失败处理

| 首个失败点 | 处理 |
|---|---|
| local monitor is running | 等进程退出；不要强行 checkpoint 活跃写库 |
| strategy lab worker is running | 等 worker 完成或停止，避免 artifact 与 DuckDB 快照错代 |
| local/remote space insufficient | 扩容或清理已确认可删缓存，再重跑原阶段 |
| publish outside post-close window / monitor active | 等工作日 15:10 后或非交易日，且云端 monitor 退出，再用原命令重跑 |
| publish timeout | 保留 state 与已发布不可变文件；确认仍在盘后后用同一命令续跑 |
| bundle verify 失败 | 停止上传/发布；保留恢复快照，修复导出问题后用新 snapshot ID |
| rsync 中断 | 相同 `upload` 命令续传 |
| target file conflict | 停止；核对云端已有研究代际，禁止覆盖或手工删除 |
| candidate marker 前崩溃 | 相同 `publish` 命令续跑；同一 publish state 验证通过后，发布器仅清理由自身精确命名的残留临时文件，复核已落文件并补最后标记 |
| active research export blocks migration cleanup | 对应研究分区仍在导出；迁移会保留其活跃临时文件，等待导出完成后重新执行相同 `publish` 命令 |

生产 DuckDB 从未参与写入，因此研究迁移失败不需要回滚生产库。失败现场应保留，先诊断首个错误，
不要用 `rm -rf` 把证据清掉。
