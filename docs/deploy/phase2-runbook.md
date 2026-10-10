# 二期：page-control + 盯盘（monitor）升级 runbook（仅准备，未执行）

目标：把 main 上的 `ack_alert` / `add_watchlist_item` / `save_alert_rule` 处理、manual_watchlist 与
alert_rule 的 Serving 投影、monitor 读取自选与告警规则，经正式的 `rquant-production-deploy.pyz`
（`scripts/deploy-production.sh`）发布到 82.156.0.68。**本文只做准备与 dry-run 设计，不发布。**

## 0. 阻塞项（需要用户决定）

正式部署器在任何 checkout 之前先跑 R07 两阶段门（`docs/production-release.md` 的决策表）：

| 侧 | 状态 |
|---|---|
| 生产已装（producer `df621ef`） | policy `deployment_mode=disabled_for_bootstrap`（= Release A 已装） |
| 目标 main（`aa11b2b9` 之后） | policy 仍是 `disabled_for_bootstrap` |

决策表：installed=`disabled_for_bootstrap` → target 只能是 `enforced` 且 `bootstrap_predecessor` 精确等于
`df621ef` 的 (commit, tree)，并且部署器要从 GitHub 拉 **`.github/workflows/ci.yml`** 三个 job
（`r07-differential-gate-py311/py312/evidence`）为该 target commit 产出的 `r07-dr-gate/evidence-v1.json`。
现在 main 的 policy 不是 enforced，且 `ci.yml` 已按用户决定停用 → **部署器必然拒绝**。可选方案：

- A（走正式流程）：在一个 release PR 里把 policy 改为 `enforced` + predecessor=`df621ef`/`7c4d17c…`，
  并只恢复 `ci.yml` 的 R07 三个 job（`push: main` + `workflow_dispatch`，不恢复 Full suite）。风险：R07 差分门
  对 main 上 1 万+ 行改动是否通过未知（PR 上已见 "frozen R07 baseline is not the merge base" 报错），可能需要
  重新冻结 baseline，这是 Codex 那套治理的主体，工作量大。
- B（绕开部署器，手工最小升级）：不走 pyz，只替换 page-control/notifier 的代码与重启，违反现有发布治理，需用户明确批准。
- C（推迟）：二期等周一交易日数据恢复、并决定 A/B 后再做。**推荐 C → A。**

## 1. 预检（只读，可在批准后执行）

```bash
ssh -i ~/.ssh/rquant_deploy lighthouse@82.156.0.68
cd ~/rquant && git rev-parse HEAD && git status --porcelain | wc -l     # 期望 df621ef…、0
git show HEAD:tests/fixtures/r07_differential_gate/policy-v1.json | python3 -c 'import json,sys;print(json.load(sys.stdin)["evidence_channel"]["deployment_mode"])'
sudo -n -l | grep rquant-production-deploy                             # 部署器 sudo 规则在
systemctl list-units 'rquant-runtime-*' --no-legend | awk '{print $3}' | sort | uniq -c
ls ~/rquant/var/r07-dr-evidence 2>/dev/null | head                     # evidence cache 目录
```

## 2. dry-run（批准 A 且 release PR 合并、evidence 产出后）

```bash
cd ~/rquant
export RQUANT_RUNTIME_PRODUCTION_INPUTS=$HOME/rquant/data/runtime-production-inputs.json
export RQUANT_RUNTIME_PROFILE_OUTPUT_DIR=$HOME/rquant/data/runtime-profiles
export RQUANT_RUNTIME_ROOT=$HOME/rquant/data/runtime
bash scripts/deploy-production.sh --target <release-tag> --dry-run       # 只输出 change plan / R07 判定
```

验收 dry-run 输出：`r07_gate=enforced`、change plan 只含 page-control / notifier / monitor 相关服务、
无 schema 破坏性迁移（Serving 新增的 manual_watchlist / alert_rule 是可选投影）。

## 3. 发布（另行批准，非交易时段）

1. 备份：`~/rquant` HEAD、`data/runtime/current` 指向、`/etc/systemd/system/rquant-*`、Serving current 指针。
2. `bash scripts/deploy-production.sh --target <release-tag>`（部署器自带两次 preflight、失败自动 reset 回 previous）。
3. 验证：page-control 收到网页 `ack_alert` → Serving 下一代出现 alert_ack；`add_watchlist_item` → monitor 日志出现手工自选；
   告警规则关闭某池后该池不再推送；所有 `rquant-runtime-*` active；28080 ok。
4. 回滚：`bash scripts/deploy-production.sh --target df621ef`（同样过 R07 表：enforced→enforced 允许）或部署器 recovery。
