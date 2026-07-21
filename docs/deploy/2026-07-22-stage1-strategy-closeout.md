# Stage 1 按策略独立收口操作手册

## 目的

Stage 1 不再把 N 字、科创/创业放量和集合竞价独立 B 串成一条全有或全无的长链。
每次操作只绑定一个策略、一个 manifest、一个日期范围和一个代码提交：

- `n_shape` 与 `growth_board_surge` 各自形成 `comparable` 固定回放；
- `auction_gap` 独立 B 研究线可以审计地标记为 `retired`；
- 集合竞价历史数据仍保留，继续作为其他策略的特征，不随策略退役删除；
- 任一策略失败只停止本次运行，不改变另外两个策略的研究状态。

## 前置条件

1. 必须使用干净、已部署的精确 Git commit，不能跟随浮动 `main`。
2. `manifest_id` 必须属于本次策略，日期范围必须完全一致。保留策略还要求 manifest 的
   `code_commit` 等于当前提交；已经 `abandoned` 的集合竞价历史 manifest 只形成退役结论，
   不产生回放，因此允许保留原提交身份。
3. 正式 apply 只能在工作日 `09:15-15:10` 之外运行。
4. 旧 manifest 若由旧代码提交生成，不能在新提交下冒充正式证据；重新运行
   `backfill-plan`。已有研究湖分钟数据会被复用，通常不会重新下载完整历史。
5. 脚本只接受已经 `completed` 的活跃 manifest，或已经 `abandoned` 的
   `auction_gap` manifest；`pending/running/failed` 均失败关闭。

## 只读预演

先单独生成并核对新 manifest：

```bash
cd /home/lighthouse/rquant
export RQUANT_CODE_COMMIT="$(git rev-parse HEAD)"

.venv/bin/rquant backfill-plan \
  --strategy n_shape \
  --start-date 2026-04-01
```

记录输出中的 `manifest_id`、`effective_end_date` 和代码提交。若任务数不为零，先在安全
窗口分段运行 `backfill-run`，直至 `backfill-status` 为 `completed`。

然后运行单策略验收预演，不带 `--apply`：

```bash
bash scripts/run-stage1-strategy-acceptance.sh \
  --strategy n_shape \
  --manifest-id <64位manifest_id> \
  --start-date 2026-04-01 \
  --end-date <effective_end_date> \
  --expected-code-commit "$(git rev-parse HEAD)"
```

预演不停止 timer、不写主库、不写 research catalog。证据保存在：

```text
logs/stage1-acceptance/<strategy>-<manifest_id>/
```

重点核对 `acceptance-plan.json`：

- `disposition=ready`；
- `manifest_status.status=completed`；
- `spec` 的策略、日期、manifest 和 commit 均准确；
- `estimated_snapshot_scan_rows`、临时磁盘预算和下一保护窗口足以容纳本次操作；
- `minute-repair-preview.json` 只能是 `planned` 或 `unchanged`。

## 正式应用

在交易保护窗口外，原样追加 `--apply`：

```bash
bash scripts/run-stage1-strategy-acceptance.sh \
  --strategy n_shape \
  --manifest-id <64位manifest_id> \
  --start-date 2026-04-01 \
  --end-date <effective_end_date> \
  --expected-code-commit "$(git rev-parse HEAD)" \
  --apply
```

脚本按以下固定顺序执行：

1. 再次只读校验单策略 acceptance plan；
2. 保存分钟修复 preview；
3. 记录并停止原本 active 的写入 timers，确认写服务均已停止；
4. 必要时按 preview 的精确 `plan_id` 应用分钟研究湖修复；
5. 主动刷新只读副本；
6. 生成 snapshot preview，再用同一 `snapshot-as-of.txt` 应用 snapshot；
7. 运行覆盖完整日期范围的 Stage 1 data audit，要求 `P0=0`；
8. 再次刷新只读副本，运行单策略 formal smoke，要求 `comparable`；
9. 刷新副本、恢复原 timers、运行 preflight。

脚本使用 `set -Eeuo pipefail` 和 `EXIT` trap。任一步失败都会停在首个失败点并尝试恢复
原 timer 集合。`snapshot-as-of.txt` 只在快照预演成功后固化；同一 manifest 重跑会复用
相同 snapshot 身份和 binding，不制造冲突记录。

## 集合竞价独立 B 退役

先对精确旧 manifest 执行终止预演：

```bash
.venv/bin/rquant backfill-abandon \
  --manifest-id 3d5893dddfa0f8cd17cddec40701c216e423e9d818697dfff9a5d71b60200d3c \
  --reason "集合竞价独立B研究线样本成本过高且已决定退役；保留竞价数据供其他策略使用"
```

必须核对 `succeeded=627`、`pending=21099`、`failed=0`，且已写分钟数据和请求统计保持
不变。正式 apply 需另行取得生产研究状态写入授权，并原样复用预演返回的 `plan_id`。

终止后运行单策略验收预演应返回：

```text
ROLLOUT_RESULT=retired
```

这不是失败，也不会查询或阻塞 N 字、科创/创业放量的 manifest。

## 最终验收字段

每个保留策略必须独立保存以下证据：

- `audit_run_id`
- `snapshot_id`
- `binding_hash`
- `strategy_spec_hash`
- `result_hash`
- `sample_count`
- 候选数、成交数、初始收益与胜率指标
- 精确 tag、Git SHA、主副本摘要、preflight 和 timer 状态

只有上述证据齐全且 formal smoke 为 `comparable`，该策略才通过 Stage 1。样本量与收益
显著性仍由后续 walk-forward 阶段判断，`comparable` 不等于策略已经可投入本金。
