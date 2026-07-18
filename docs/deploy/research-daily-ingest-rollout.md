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

systemd 变更不进入标准 `deploy-production.sh`。先用受控部署器发布应用代码 tag，并确认
`scripts/run-research-ingest-daily.sh` 已存在且可执行；随后把基础设施分支的候选 unit 上传到
腾讯云 `/tmp`，在基础设施 PR push 前做原样验证：

```bash
systemd-analyze calendar 'Mon..Fri *-*-* 18:10:00' --iterations 5
systemd-analyze verify \
  /tmp/rquant-research-ingest.service \
  /tmp/rquant-research-ingest.timer
```

验证通过并合并基础设施 PR 后，必须把生产 `main` 快进到该精确基础设施 SHA，再安装 unit。
这一步是受控部署器拒绝 protected path 后的显式基础设施发布路径，也保证下一版代码发布不会
再次把本次 systemd 变更算进 diff。禁止使用“拉最新 main”代替精确 SHA：

```bash
(
  set -Eeuo pipefail
  cd /home/lighthouse/rquant
  TARGET_SHA=<基础设施_PR_合并后的40位SHA>
  git fetch origin main
  test "$(git rev-parse --abbrev-ref HEAD)" = main
  test -z "$(git status --porcelain --untracked-files=no)"
  test "$(git rev-parse --verify "${TARGET_SHA}^{commit}")" = "${TARGET_SHA}"
  git merge-base --is-ancestor HEAD "${TARGET_SHA}"
  git merge-base --is-ancestor "${TARGET_SHA}" origin/main

  CHANGED_FILES="$(git diff --name-only HEAD.."${TARGET_SHA}")"
  test -n "${CHANGED_FILES}"
  while IFS= read -r path; do
    case "${path}" in
      deploy/systemd/rquant-research-ingest.service|\
      deploy/systemd/rquant-research-ingest.timer|\
      deploy/systemd/README.md|scripts/deploy.sh|\
      tests/unit/test_research_ingest_systemd.py) ;;
      *) echo "unexpected infrastructure path: ${path}" >&2; exit 1 ;;
    esac
  done <<< "${CHANGED_FILES}"

  git merge --ff-only "${TARGET_SHA}"
  systemd-analyze verify \
    deploy/systemd/rquant-research-ingest.service \
    deploy/systemd/rquant-research-ingest.timer
  sudo install -m 0644 deploy/systemd/rquant-research-ingest.service /etc/systemd/system/
  sudo install -m 0644 deploy/systemd/rquant-research-ingest.timer /etc/systemd/system/
  sudo systemctl daemon-reload
  systemctl cat rquant-research-ingest.service rquant-research-ingest.timer
)
```

用户明确授权后，安装 unit、执行 `daemon-reload`，再把云端 `.env` 的开关改为：

```dotenv
RESEARCH_CLOUD_INGEST_ENABLED=true
```

启用 timer 前先手工运行一次目标交易日：

```bash
cd /home/lighthouse/rquant
scripts/run-research-ingest-daily.sh
```

确认手工运行返回 `candidate` 或可解释的 `degraded` 后才能启用 timer；不得为了得到绿色结果
手工删除 issue 或降低覆盖门槛。`degraded` 返回退出码 2，不应启用 timer，必须先理解并处理
issue。确认首次运行退出码 0 后，才单独执行：

```bash
sudo systemctl enable --now rquant-research-ingest.timer
```

timer 不设置 `Persistent=true`，避免服务器隔日恢复后用错误日期补跑；runner 启动时固定一次
目标交易日，网络、daily 未完成、副本刷新失败和副本不完整最多尝试 4 次、间隔 15 分钟，
不会因滑动 systemd 限速跨夜续跑；`degraded`（2）和开关关闭（3）不重试。
未显式指定日期且权威 SSE 日历明确休市时，CLI 返回 `skipped` 和退出码 0，日历缺口仍按故障
处理。

若整夜均失败，在后续非交易窗口按 observation 链从最早漏日开始逐日恢复：

```bash
.venv/bin/rquant research-ingest --date 2026-07-16 --recover
```

恢复模式逐只调用历史 `stk_mins`，只允许接在当前 observation 的下一个交易日，拒绝倒序修补
或跨越缺口，避免重写已发布证据链。

已经位于 observation 链中间、且只缺集合竞价的数据不能使用 `--recover`。在交易保护窗口外
先用同一组重复 `--date` 真实请求接口并生成计划：

```bash
.venv/bin/rquant research-repair-auction \
  --date 2026-04-20 \
  --date 2026-07-07
```

核对每一天的预期代码数、有效代码数、反向精确率和 `changed` 后，再原样复用日期集执行：

```bash
.venv/bin/rquant research-repair-auction \
  --date 2026-04-20 \
  --date 2026-07-07 \
  --apply \
  --plan-id <预演输出的plan_id>
```

预演会真实访问 Tushare，但不创建或改写本地文件。apply 会重新取数；接口业务内容、
authority、主副 catalog 或任一目标 manifest 发生变化都会令旧 plan 失效。批次中任意日期
低于双侧 98% 门或任一发布步骤失败时，全部日期保持修复前状态。成功修复不会删除旧内容寻址
版本，但会把稳定观察天数重置为 0。

已经位于 observation 链中间、且生产只读副本有完整分钟会话、研究湖历史分区缺失时，
不能用 `research-ingest --recover`，也不能恢复旧 `research-export` 写权限。先用已完成的
策略分钟回补 manifest 做只读预演：

```bash
.venv/bin/rquant research-repair-minute \
  --manifest-id <已完成的manifest_id>
```

重点核对 `required_session_count`、`lake_complete_session_count`、
`missing_session_count`、逐日 `target_session_count` 和 `plan_id`。预演不会请求
Tushare，也不会创建 transaction 或改写研究湖。确认后原样复用 manifest 和 plan：

```bash
.venv/bin/rquant research-repair-minute \
  --manifest-id <同一个manifest_id> \
  --apply \
  --plan-id <预演输出的plan_id>
```

apply 会重建计划并核对生产分钟行、authority、主副 catalog 和每个旧 manifest 的内容；
任何输入变化都会令旧 plan 失效。整批按不可变版本、manifests、主 catalog、只读 catalog、
observation/current 的顺序发布，任一步失败都回到批次前状态。成功后
`stable_trading_days=0`，下一次相邻交易日 daily 才重新累计为 1。

这里有两个不同的“时间边界”：新建 `backfill-plan` 时，可观测候选截止日会随最新已收盘
交易日持续向前移动；一旦 manifest completed，它的资格、窗口和 unavailable 声明就是
不可变审计快照。历史修复只补这个冻结范围，不能借修复命令扩大到后来新增的交易日。
要研究更晚样本，应重新生成新的 manifest。

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
if grep -q '^RESEARCH_CLOUD_INGEST_ENABLED=' .env; then
  sed -i 's/^RESEARCH_CLOUD_INGEST_ENABLED=.*/RESEARCH_CLOUD_INGEST_ENABLED=false/' .env
else
  printf '\nRESEARCH_CLOUD_INGEST_ENABLED=false\n' >> .env
fi
```

关闭开关后 v0.21.0 应用代码保持惰性，不需要为了停用研究增量倒退生产 checkout；生产
monitor/daily 仍按原链路运行，研究增量没有写过生产 DuckDB。保留 lake 新版本、观察 JSON
和失败证据，不手工修改 current 或 journal。若存在 publish journal，应先用同版
`research-ingest` 触发自动回滚；自动恢复失败时保留 transaction 目录并从上一份已验证备份
恢复，禁止直接改 JSON 哈希。
