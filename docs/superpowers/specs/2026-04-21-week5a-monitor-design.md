# Week 5a：盘中监控 + 持久 Pool 2 设计

## 目标

盘中实时监控 Pool 1 和 Pool 2 标的的价格走势，当价格触达预设档位时弹 macOS 弹窗提醒。Pool 2 从「每日快照」升级为「持久池子」，有明确的进池/退池机制。

## 架构

```
已有系统                              新增（Week 5a）
─────────                            ──────────────
rquant serve (17:00)                 rquant monitor (09:15-15:05)
  └─ ingest + pipeline                   └─ 同步轮询循环（5 秒）
      └─ screen_result 表                    ├─ akshare 实时行情
      └─【新增】同步 pool2_watch             ├─ 档位检测
                                             ├─ macOS 弹窗（osascript）
         daily_state 表 ──读取→              ├─ monitor_event 表（新）
         pool2_watch 表 ←→                   └─ 退出检查（收盘后）
```

**进程模型**：monitor 与 serve 完全独立，各自一个 launchd 服务。

**不改动**：presets.py、screen 模块、ingest.py — 盘后筛选流程不变。

## 新增组件

| 组件 | 说明 |
|------|------|
| `src/rquant/monitor.py` | 监控核心逻辑（轮询 + 档位检测 + 退出检查） |
| `pool2_watch` 表 | 持久 Pool 2 池子 |
| `monitor_event` 表 | 盘中事件日志 |
| `rquant monitor` CLI 子命令 | 启动监控进程 |
| `rquant pool2 list / remove` CLI 子命令 | 查看/管理持久池 |
| `pipeline.py` 尾部新增 | Pool 2 screen_result → pool2_watch 同步 |
| `com.roxor.rquant-monitor.plist` | macOS launchd 自启配置 |

## 持久 Pool 2 生命周期

### N 形态完整时间线

```
T 日：标的 A 涨停（首板）
T+1 日 17:00：A 符合 Pool 1 → 进入 T+1 的 screen_result (pool1)
T+2 日 09:15：盘中监控 T+1 的 Pool 1，包含 A
T+2 日 17:00：A 符合 Pool 2 → 进入 screen_result (pool2)
              pipeline 同步 → A 进入 pool2_watch（active）
T+3 日 09:15：盘中监控 pool2_watch，包含 A（第 1 天）
T+3 日 15:05：退出检查
T+4 日 09:15：盘中监控 pool2_watch，包含 A（第 2 天）
...
T+N 日 15:05：第 3 天到期 → 弹窗询问踢/留
```

### 入池（pipeline.py 负责，17:00）

pipeline 跑完 Pool 2 筛选后，扫描 screen_result 中的新 Pool 2 票：
- 若 pool2_watch 中**不存在**该 ts_code → INSERT，status = active
- 若已存在且 status = active → 跳过（不重复入池）
- 若已存在且 status = exited → 更新为 active（重新入池）

入池时计算并存储 5 个档位价（基于涨停日 body）。

### 退出（monitor.py 负责，15:05）

收盘后逐只检查退出条件，**所有退出都弹窗确认**：

**条件 1 — 跌破位**：当日收盘价 < 强止损 或 弱止损

弹窗格式：
```
002415.SZ | 退出确认

昨收：¥11.65（跌破强止 ¥11.80）
入池：04-18（第2天）
40：¥12.36 | 30：¥12.22 | 20：¥12.08
强止：¥11.80 | 弱止：¥11.52

按钮：[踢出] [保留]
```

**条件 2 — 超过观察期**：在池中满 3 个交易日未退出

弹窗格式：
```
002415.SZ | 观察期满

入池：04-18（已满3天）
昨收：¥12.50
40：¥12.36 ✓ | 30：¥12.22 | 20：¥12.08
强止：¥11.80 | 弱止：¥11.52

按钮：[踢出] [保留]
```

已触达的档位用 ✓ 标记（从 monitor_event 查询）。

用户选「保留」则继续观察，次日到期再次询问。
用户选「踢出」则标记 status = exited，记录 exit_date 和 exit_reason。

## 盘中监控

### 监控对象

| 来源 | 内容 | 档位 |
|------|------|------|
| 昨日 Pool 1（screen_result） | 当天新鲜的首板次日票 | 40/30/20/强止/弱止 |
| pool2_watch (active) | 持久池全量 | 40/30/20/强止/弱止 |

同一只票同时在 Pool 1 和 Pool 2 中 → 去重，保留 Pool 2 标记。

### 档位价格计算

基于涨停日 K 线实体：

```
body = body_upper - body_lower

level_40  = body_lower + body × 0.4
level_30  = body_lower + body × 0.3
level_20  = body_lower + body × 0.2
stop_strong = body_lower
stop_weak   = body_lower - body × 0.2
```

Pool 2 标的：档位在入池时预算好，存 pool2_watch。
Pool 1 标的：monitor 启动时从 daily_state 查涨停日 body 值，临时计算。

### 触发规则

- 每个档位每只票每天**只触发一次**（Option A）
- 判断条件：`当前价 <= 档位价` 或 `当日最低价 <= 档位价`（补漏机制）
- 触发后：存 monitor_event + Popen osascript 弹窗

### 弹窗格式

标题：`{ts_code} | {level}`

正文：
```
current：¥12.35
40：¥12.80 | 30：¥12.40 | 20：¥12.00
body：¥11.80 — ¥13.20
强止：¥11.80 | 弱止：¥11.52
```

强止/弱止触发时 level 显示 `强止` / `弱止`。

### 轮询参数

- 间隔：5 秒（CLI 可配）
- 数据源：akshare（东方财富源，~3 秒延迟，免费无硬限流）
- 交易时段：09:30-11:30 + 13:00-15:00，午休期间暂停轮询

## 表结构

### pool2_watch

```sql
CREATE TABLE pool2_watch (
    ts_code       VARCHAR   PRIMARY KEY,
    entry_date    DATE      NOT NULL,
    limit_up_date DATE      NOT NULL,
    body_upper    DOUBLE    NOT NULL,
    body_lower    DOUBLE    NOT NULL,
    level_40      DOUBLE    NOT NULL,
    level_30      DOUBLE    NOT NULL,
    level_20      DOUBLE    NOT NULL,
    stop_strong   DOUBLE    NOT NULL,
    stop_weak     DOUBLE    NOT NULL,
    status        VARCHAR   DEFAULT 'active',
    exit_date     DATE,
    exit_reason   VARCHAR,
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### monitor_event

```sql
CREATE TABLE monitor_event (
    trade_date    DATE      NOT NULL,
    ts_code       VARCHAR   NOT NULL,
    level         VARCHAR   NOT NULL,
    trigger_price DOUBLE,
    level_price   DOUBLE,
    trigger_time  TIMESTAMP NOT NULL,
    trigger_type  VARCHAR,
    pool          VARCHAR,
    body_upper    DOUBLE,
    body_lower    DOUBLE,
    PRIMARY KEY (trade_date, ts_code, level)
);
```

## CLI 命令

```
rquant monitor              # 启动盘中监控
rquant monitor --interval 5 # 自定义轮询间隔（秒）
rquant pool2 list            # 查看持久池当前标的
rquant pool2 remove 002415.SZ  # 手动踢出
```

## 进程生命周期

```
09:15  启动
       ├── 读 pool2_watch (active) → Pool 2 watchlist
       ├── 读 screen_result (昨日 Pool 1) → Pool 1 watchlist
       │   └── 查 daily_state 获取涨停日 body → 算 5 档位
       └── 去重（Pool 1 ∩ Pool 2 → 保留 Pool 2）

09:30  开始轮询（每 5 秒）

11:30  暂停轮询

13:00  恢复轮询

15:05  收盘
       ├── 打印当日事件汇总
       ├── 退出检查：逐只弹窗确认（跌破位 / 超期）
       └── 进程退出

17:00  pipeline 跑完后同步新入池（pipeline.py 负责）
```

## 非交易日处理

monitor 启动时通过 akshare 交易日历（`ak.tool_trade_date_hist_sina()`）检查**今天**是否为 A 股交易日。周末和中国法定节假日（元旦、春节、清明、劳动节、端午、中秋、国庆）均不是交易日。若今天非交易日，打印日志后以 exit code 0 正常退出（避免 launchd KeepAlive 反复重启）。

## launchd 配置

`deploy/com.roxor.rquant-monitor.plist`：
- 指向 `rquant monitor`
- RunAtLoad: true
- KeepAlive: SuccessfulExit = false（异常退出时重启）
- 日志：`logs/launchd-monitor-stdout.log`

## 模块职责边界

| 模块 | 负责 | 不负责 |
|------|------|--------|
| `pipeline.py` | Pool 2 入池同步 | 退出检查 |
| `monitor.py` | 盘中轮询 + 弹窗 + 退出检查 | 入池 |
| `storage/duckdb.py` | pool2_watch / monitor_event CRUD | 业务逻辑 |

## 依赖

新增：`akshare`（实时行情）

## 不在 Week 5a 范围

- 用户持仓记录 → Week 5b
- 持仓止损/止盈监控 → Week 5b
- cc2im 推送通知 → Week 6
