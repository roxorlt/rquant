# iTick 证伪测试工具

> **☁️ 云端已部署（2026-07-28）**：脚本与 token 已在 `82.156.0.68:~/itick_probe/`，crontab 每个交易日
> `09:10 run_collect.sh 开录` → `15:12 pkill 停录` → `15:20 run_report.sh 出报告并直推 PushDeer`
> （摘要推手机，完整报告在云端 `report-YYYY-MM-DD.md`；当日 stk_mins 未出数时自动退 rt_min_daily 做参照）。
> 判定按 **1 个交易日** 出结论（一票否决项单日足够）；cron 留着零成本继续积累断线/开盘稳定性样本。
> 测试标的：600519（主板高流动）/ 300750（创业板）/ 003040（低流动小票）。
> WS 握手已冒烟验证通过（connected → authenticated → subscribe Successfully）。
> 白名单：iTick 控制台已加 82.156.0.68。token 在云端 `~/itick_probe/.env` 与本地 rQuant `.env`（均不进 git）。
> 下面的手动用法仅作参考/本地调试。

对应 [../EVALUATION.md](../EVALUATION.md) §3「两周证伪测试协议」的 #2（延迟）、#4（断线）、#6（成交量完整性）、#7（时间戳）。

## 前置

1. 到 https://itick.org 注册免费账号拿 API token（**不要绑卡**）
2. **在云端 82.156.0.68 上跑**，本地 Mac 测的延迟不算数（跨境链路是主要风险源）
3. Free 层限制：1 个 WS 连接、3 个订阅——选 3 只代表标的（主板高流动性 / 创业板 / 低流动性小票）

## 用法

```bash
export ITICK_TOKEN=你的token

# 盘中（建议 09:20 前启动，收盘后自动停）
uv run --with websocket-client python bar_consistency.py collect \
    --symbols '600519$SH,300750$SZ,003040$SZ' --out ~/itick_probe

# 收盘后（需要能 import rquant，即在 /home/lighthouse/rquant 的 venv 里跑）
uv run --with websocket-client python bar_consistency.py report \
    --data ~/itick_probe --date 2026-07-29 | tee ~/itick_probe/report-2026-07-29.md
```

## 报告怎么读

- **量比中位 ≈ 1.00 且波动小** → WS 是全量成交推送，footprint/聚合可用
- **量比稳定 < 1** → 采样推送，可校准使用但 footprint 数字只是下限
- **量比不稳定** → 数据不可用，测试 #6 判负 → 按协议直接放弃，不升级付费
- 延迟看 **P99** 不看 P50；断线次数 × 时长 = 永久数据缺口（无补数据机制）

## 注意

- 原始流水按天落 `raw-YYYY-MM-DD.jsonl`（含本机接收时刻），断线/重连记 `events-YYYY-MM-DD.jsonl`
- 落盘目录放在 rQuant `data/` 之外（如 `~/itick_probe`），**绝不写主 DuckDB**
- tick 的 `v` 字段疑似当日累计量（文档示例值与 quote 的累计量一致），脚本自动检测并差分，报告里会标注判定结果
