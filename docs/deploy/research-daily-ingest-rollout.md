# 研究数据每日增量上线手册

本文用于把 v0.20.0 的分钟/竞价每日增量以“影子候选”方式接到腾讯云。它不修改生产
`rquant.duckdb`，也不代表云端研究数据已经晋级为唯一权威。

## 安全边界

- `RESEARCH_CLOUD_INGEST_ENABLED` 默认 `false`；只部署代码不会产生新写入。
- monitor 只允许在 09:30 前首次固化当日 Pool1/Pool2 预期清单；盘中重启只能复用完全一致的
  清单，不能缩小分母或覆盖证据。
- 日终任务只读 `rquant_ro.duckdb`，直接向 Tushare 补齐当日清单分钟和集合竞价。
- 分钟、竞价、catalog 和只读副本先写入隔离 transaction 目录。全部成功后才创建持久
  publish journal 并切换正式 manifest/catalog/current；异常即时回滚，进程硬中断会在下次
  `research-ingest` 开始前自动回滚。journal 存在时状态入口始终 fail closed。
- 缺清单、分钟覆盖不足、竞价分母缺失或覆盖低于 98% 时状态为 `degraded`，退出码为 2，
  systemd 应触发一次受去重保护的异常告警。
- 存量迁移和日增量共用 `research-publish.lock`。一旦
  `research-authority-candidate.json` 或 `research-authority-current.json` 已建立，非 dry-run
  的 `research-export` 会拒绝直接修改正式目录；禁止绕过 CLI 调用底层 exporter。回滚会在
  改动任何文件前，一次性校验全部备份、可变目标、不可变版本和 observation 的 CAS；发现
  备份损坏、既有版本丢失或第三方代际时保留 journal 并 fail closed。首次创建 transaction、
  observation 等多级目录时逐级 fsync 父目录项，覆盖主机断电恢复场景。
- 连续 10 个交易日均为 `candidate` 前，不删除 Mac 主库、不切换 Lab 默认数据源。

## 代码部署后的只读验收

```bash
cd /home/lighthouse/rquant

.venv/bin/rquant research-authority-status
.venv/bin/rquant research-ingest --date "$(date +%F)" --dry-run
```

首次 dry-run 在当天 17:00 daily 尚未完成前可能显示竞价分母为 0；这正是调度放在 18:10
而不是 15:20 的原因。dry-run 不请求 Tushare、不写 lake/catalog/marker。

## 独立基础设施发布

systemd 变更不进入标准 `deploy-production.sh`。单独的基础设施 PR 必须先在腾讯云验证：

```bash
systemd-analyze calendar 'Mon..Fri *-*-* 18:10:00' --iterations 5
systemd-analyze verify \
  /tmp/rquant-research-ingest.service \
  /tmp/rquant-research-ingest.timer
```

用户明确授权后，安装 unit、执行 `daemon-reload`，再把云端 `.env` 的开关改为：

```dotenv
RESEARCH_CLOUD_INGEST_ENABLED=true
```

启用 timer 前先手工运行一次目标交易日，确认返回 `candidate` 或可解释的 `degraded`；不得
为了得到绿色结果手工删除 issue 或降低覆盖门槛。

## 每日验收

```bash
cd /home/lighthouse/rquant

systemctl status rquant-research-ingest.timer --no-pager -n 20
systemctl status rquant-research-ingest.service --no-pager -n 40
journalctl -u rquant-research-ingest.service --since today --no-pager
.venv/bin/rquant research-authority-status
```

状态至少应满足：

1. `catalog_hash_matches=true`、`readonly_catalog_hash_matches=true`；
2. 最新交易日与当日一致；
3. minute 覆盖率为 1，且每只恰好覆盖 241 个交易分钟格、没有午休或盘后异常分钟；竞价
   `daily_bar` 分母相对近日全市场规模完整，正向覆盖率和观测精度均不低于 0.98；
4. `stable_trading_days` 只在相邻交易日连续通过时递增，current 与每一份历史 observation
   的哈希链、manifest 和 Parquet 文件必须一致；
5. 第 10 个合格交易日只获得“可评估晋级”资格，不自动删除 Mac 数据或切换消费者。

第 10 日状态核验会对 bootstrap 与增量的全部 catalog 分区重新计算物理文件 SHA-256，耗时
明显高于普通日检查；这是晋级门的一部分，不能为追求页面响应速度跳过。

## 回滚

异常时先停用新增 timer 并关闭开关：

```bash
sudo systemctl disable --now rquant-research-ingest.timer
```

保留 lake 新版本、观察 JSON 和失败证据，不手工修改 current 或 journal。代码回滚到 v0.19.0
后，生产 monitor/daily 仍按原链路运行；研究增量没有写过生产 DuckDB。若存在 publish journal，
应先用同版 `research-ingest` 触发自动回滚；自动恢复失败时保留 transaction 目录并从上一份已
验证备份恢复，禁止直接改 JSON 哈希。
