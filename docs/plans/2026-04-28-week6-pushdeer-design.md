# Week 6: PushDeer 告警通知 — 设计文档

**日期**：2026-04-28
**状态**：已确认，待实施
**参考实现**：`30-projects/xueqiuFollow/src/notifier.py`

---

## 背景与目标

替换原计划的 cc2im（受限于微信 token 限制）为 PushDeer。完成后：

- 替代 `monitor.py` 当前的 osascript 弹窗（云端零迁移成本）
- 让用户 4 个推送维度即时感知系统状态：盘中触发、收盘汇总、每日选股、系统异常、启停心跳
- 解决 4/24 APScheduler 死亡 1 周才发现的"沉默失败"痛点（D 类异常推送）

**现阶段范围**：只推 admin（刘彤）的两个 PushDeer key（iPhone + Mac），来自 `xueqiuFollow/config.yaml` admin 组。后续如增加订阅者再扩展 group 概念。

---

## 触发场景（5 类）

| 代号 | 场景 | 频率 | 形态 |
|------|------|------|------|
| A | 盘中档位触发（40%/30%/20%/强止/弱止） | 实时（每事件一条） | 单条详细 |
| B | Pool 2 退出（breakdown 自动踢，expired 保留） | 收盘后一条（无事件不推） | 汇总分组 |
| C | 每日 17:00 筛选完成 | 每日一条 | 汇总分组 |
| D | 调度/系统异常（被动 try/except） | 异常时实时 | stack trace |
| E | Monitor 启停心跳 | 09:30 + 15:05 各一条 | 简短状态 |

---

## 消息格式

PushDeer markdown，title 简短（推送预览只显示 title），body 详细。

### A. 档位触发

```
title:  002415.SZ 海康威视 40档 ¥12.30

body:   # 002415.SZ | 40%档触发 | 现价 ¥12.30
        - bodyTop: ¥13.20
        - 40档:    ¥12.36
        - 30档:    ¥12.22
        - 20档:    ¥12.08
        - bodyBtm: ¥11.80
        - 强止：¥11.80 | 弱止：¥11.52
        - pool2，入池 04-18 第 5 日
```

价格阶梯从高到低排列，眼睛一扫看出现价在哪一档（轻量回应"全是数字不直观"反馈，不画 K 线但有空间感）。

**实现备注**：股票名称 watchlist 不存，需从 `stock_basic` 表 join 取，messages.py 内查询。

### B. Pool 2 退出汇总（收盘后批量，无事件不推）

```
title:  Pool 2 退出 04-28: 踢出 2 / 待决策 1

body:   ## 自动踢出（跌破止损）
        - 002415.SZ 海康威视 收盘 ¥11.65 < 弱止 ¥11.80
        - 600519.SH 茅台 收盘 ¥1620 < 强止 ¥1650

        ## 待决策（超期已保留）
        - 002846.SZ 入池 04-24 第 3 日
          → `rquant pool2 remove 002846.SZ`
```

### C. 每日筛选汇总（17:00 流水线完成）

```
title:  每日选股 04-28: P1 命中 12, P2 持仓 5

body:   ## Pool 1 候选
        - 600519.SH 茅台 收 ¥1685
        - 002415.SZ 海康威视 收 ¥12.50
        ...

        ## Pool 2 持仓
        - 002415.SZ 入池 04-18 第 5 日
        ...

        耗时 28s
```

### D. 系统异常（实时）

```
title:  rQuant 异常: ingest_daily 失败

body:   组件：ingest_daily
        时间：2026-04-28 17:00:15
        异常：ConnectionError: Tushare timeout

        ```python
        <stack trace 前 15 行>
        ```
```

### E. 启停心跳

```
title:  ▶ Monitor 启动 26 只
body:   pool2=13, pool1=13
        watchlist: 002415, 600519, ...
```

```
title:  ⏹ Monitor 结束: 触发 8 次
body:   40档 5 / 30档 2 / 强止 1
        Pool 2 自动踢出 2 只
```

emoji 仅用于消息开头作为类型标识（▶/⏹/❌），其他全文字。

---

## 架构

```
src/rquant/notify/
├── __init__.py     # notify(scene, **kwargs) 统一入口 + 开关
├── client.py       # PushDeer HTTP 客户端
└── messages.py     # 5 种场景的消息构造函数
```

**关键决策**：
- `notify/` 模块独立，不依赖 monitor/pipeline——以便 cli/未来 watchdog 也能调用
- 一个统一入口 `notify(scene: Literal["price_level","pool2_exit","daily_summary","error","heartbeat"], **kwargs)`
- 内部按场景路由到对应的 messages 构造函数 → client 推送
- 失败：`logger.error(...)` 记录但不抛——业务方不需要 try/except 包裹 notify 调用

### 失败处理（统一规则）

```python
def notify(scene: str, **kwargs) -> None:
    if not settings.notify_enabled:
        return
    if not getattr(settings, f"notify_{scene}_enabled", True):
        return
    try:
        title, body = build_message(scene, **kwargs)
        client.push(title, body)
    except Exception as e:
        logger.error(f"通知失败 {scene}: {e}")
        # 不再抛出，不阻塞业务
```

### HTTP 客户端

- `requests.post(endpoint, data=..., timeout=10)`
- 多 key 用 `ThreadPoolExecutor` 并发推（参考 xueqiuFollow `_send_pushdeer`）
- timeout 失败 → 捕获 → 记日志，不重试（PushDeer 历史失败率低，重试反而增加复杂度）
- payload：`{"pushkey": key, "text": title, "desp": body, "type": "markdown"}`
- 成功判定：`response.json()["code"] == 0`

---

## 注入点

| 场景 | 位置 | 时机 | 改动 |
|------|------|------|------|
| A | `monitor.py:run_monitor()` while 循环内，`alert_price_level` 调用处 | 实时 | 替换 osascript 调用 |
| B | `monitor.py:check_exits()` 末尾 | 收盘后批量 | breakdown 自动 update_pool2_exit；expired 仅记录；末尾统一 notify |
| C | `pipeline.py:run_daily_pipeline()` 末尾 | 流水线完成后 | 加 notify("daily_summary", ...) |
| D | `cli.py:_ingest_with_retry`、`pipeline.run_daily_pipeline`、`monitor.run_monitor` 最外层 try/except | 异常时 | 各入口包一层 try/except |
| E | `monitor.py:run_monitor()` 进 watchlist 后 + return 前 | 启停 | 两处加 notify 调用 |

**重要**：A 完全替代 osascript 弹窗（不再共存）；B 从"踢出/保留"二选一弹窗改为"breakdown 自动踢，expired 自动保留 + 推送"。

---

## 配置

`.env` + `.env.example` + `config.py` 追加：

```
NOTIFY_ENABLED=true            # 总开关
NOTIFY_PRICE_LEVEL=true        # A
NOTIFY_POOL2_EXIT=true         # B
NOTIFY_DAILY_SUMMARY=true      # C
NOTIFY_ERROR=true              # D
NOTIFY_HEARTBEAT=true          # E
```

`PUSHDEER_KEYS` 和 `PUSHDEER_ENDPOINT` 已在 `.env` 中（v0.5.1 已就位）。

---

## 测试策略

| 层级 | 内容 |
|------|------|
| 单测 | `client.py` mock requests，验证 payload + 失败处理；`messages.py` 5 个场景格式快照测试；`__init__.py` 开关逻辑 |
| 集成 | monitor / pipeline / cli 注入点 mock notify，验证调用次数 + 关键参数 |
| 手动验证 | 加 CLI 命令 `rquant notify-test` 一键发测试消息验证通道 |

---

## 实施步骤

5 个 commit，1 个分支 `feat/week6-pushdeer`：

| # | 内容 | Commit |
|---|------|--------|
| 1 | notify 模块本身（client + messages + __init__ + 单测） | feat(notify): add PushDeer client and message builders |
| 2 | CLI 加 notify-test + 接入 cli/pipeline/monitor 入口的异常 try/except (D) | feat(notify): add notify-test CLI and error reporting |
| 3 | monitor.py 接入 A + E（替换 osascript，加启停心跳） | feat(notify): replace osascript alerts with PushDeer (A/E) |
| 4 | monitor.py check_exits 改造：breakdown 自动踢，expired 保留 + B 推送 | refactor(monitor): auto-exit on breakdown, notify on expiry (B) |
| 5 | pipeline.py 末尾接入 C | feat(notify): daily summary push after pipeline (C) |

**回退路径**：每个 phase 独立可回滚（osascript 删了用 git revert 拿回；改 `NOTIFY_*_ENABLED=false` 直接关功能）

---

## 工作量

约 1-1.5 天（含测试 + 端到端验证）。

完成后打 tag `v0.6.0`（Week 6 完成）。
