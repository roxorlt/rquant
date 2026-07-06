# 盘中 30 分钟脉搏 + 午间战报(morning-pulse / midday-report)

**日期**:2026-07-06　**分支**:feat/midday-briefing
**执行**:Opus 4.8 subagent(coding)/ Fable 5(规划·测试设计·验收)
**用户确认**:ABCD 全做;A 升级为上午每 30 分钟一推 + 午间总结;推送与 markdown 双出;
Mac 午休开盖锁屏(launchd 可用)。

## 需求映射

| 编号 | 内容 | 落点 |
|---|---|---|
| A+ | 上午 10:00/10:30/11:00/11:30 各推一份 30 分钟脉搏(短,增量为主);12:00 推午间战报(全,罗列+总结上午) | CLI `morning-pulse` / `midday-report` + launchd |
| B | 下午候选观察池(半日量能预筛,创业/科创) | 战报一节 |
| C | 每槽位快照+板块聚合落 parquet(数据资产,后续做上午→下午持续性研究) | data/midday/ |
| D | 持仓午间体检(paper_position 当前为空→无仓静默跳过该节) | 战报一节 |

## 硬约束(不可violate)

1. **绝不碰 DuckDB 主库**:读只走 `open_readonly_store()`(T-1 数据);自产数据全部落
   **parquet**(`data/midday/`),报告落 markdown(`data/reports/midday/`),两目录进 .gitignore;
2. 实时数据只从外部源现拉:复用 `panorama_data.fetch_market_snapshot`(三级路由)、
   `fetch_sector_fund_flow`、`add_limit_prices`;
3. 非交易日静默退出(复用 `monitor.is_trading_day`);
4. 推送走现有 `rquant.notify` 场景机制(新增 scene,格式对齐现有 messages),只推 admin;
5. 不新增第三方依赖;pandas to_parquet 需要引擎——**先确认 venv 里 pyarrow/fastparquet
   是否已装**(duckdb 生态大概率已带 pyarrow);两者都没有则降级 to_pickle(.pkl),
   保持读写接口封装在一个 storage helper 里,格式可换。

## 架构

### 新模块 `src/rquant/midday_briefing.py`(数据+报告)+ CLI 两命令

```
morning-pulse  流程:守卫(交易日/槽位) → 拉快照+add_limit_prices → 拉资金流(行业+概念)
  → 板块聚合(kpl 口径,复用 build_board_overview) → 落盘 slot parquet
  → 与上一槽位 parquet 求增量 → 渲染 pulse 报文 → PushDeer + markdown append
midday-report  流程:同上取 11:30 后最新快照 → 全量战报(下述五节) → 落盘 digest
  → PushDeer + markdown append
```

### 槽位与守卫

- 槽位表:10:00 / 10:30 / 11:00 / 11:30(pulse),12:00(digest);
- `morning-pulse` 无参运行时按当前时间归槽(容差 ±5 分钟),**迟到 >10 分钟跳过并
  log**(Mac 睡过头的过期脉搏没意义);`--slot HH:MM --force` 手动补跑;
- 幂等/去重:当日该槽位 parquet 已存在且已推送(报文落盘标记)→ 跳过,`--force` 覆盖;
- `--dry-run`:全流程跑但不推送(打印报文),parquet 照落(便于人工核对)。

### 落盘布局(方案 C)

```
data/midday/YYYY-MM-DD/
  snapshot_1000.parquet    # 全市场快照(含 limit_up_price 等衍生列)
  boards_1000.parquet      # 三体系板块聚合(build_board_overview 输出 + system 列)
  snapshot_1030.parquet ...
  meta.json                # 各槽位 {fetched_at, route, pushed: bool}
data/reports/midday/YYYY-MM-DD.md   # 当日报告(pulse 逐节 append,digest 收尾)
```

### 30 分钟脉搏报文(短,手机一屏)

```
【脉搏 10:30】涨停 47(+9) 炸板 6(+2) 涨跌比 2871/2130
新晋涨停:XX(题材A) XX(题材B) ...(最多8只,附题材)
题材热度:人形机器人 5板(+2) | 存储芯片 3板(+1) | ...top5,Δ为对上一槽位
放量异动新增:XX(半日量比2.1) ...(创业/科创,最多5只)
```
增量(Δ)全部来自与上一槽位 parquet 对比;10:00 首槽无前项,只报绝对值。

### 午间战报(digest,五节)

1. **情绪温度**:涨停/炸板/跌停家数、炸板率、涨跌比;对比维度——今日各槽位走势
   罗列(10:00→11:30 涨停数序列)+ 昨日全天终值(T-1 副本 limit_list_daily 家数);
2. **连板梯队**:今日连板 = limit_list_daily(T-1) 连板数 + 今日快照涨停判定现算
   (昨 N 板+今涨停=N+1 板;昨无+今涨停=首板);按高度分组罗列,标题材;
3. **最强题材 Top5**:kpl 口径,涨停数×半日成交额×资金流综合;附上午四槽位的
   涨停数演变(哪个题材在加速);
4. **下午候选观察池(方案 B)**:创业/科创,条件=半日额 ≥ 0.8 × 20 日全日均额
   (daily_bar.amount 千元 ×1000 对齐快照元)且 0 < pct_chg < 15(未涨停)且
   现价 ≥ 快照均价(amount/volume 近似 VWAP);按半日量比降序 top20,
   列:代码/名称/题材/半日量比/涨幅/距涨停空间;
5. **持仓体检(方案 D)**:paper_position status=open 逐仓 mark-to-market
   (快照现价 vs entry/stop/take-profit),输出浮盈%、距止损%、板块半日强弱;
   **无活跃持仓 → 该节整体省略**(不输出"无持仓"占位)。

### 调度(launchd,Mac 本地)

- `deploy/launchd/com.roxor.rquant-morning-pulse.plist`:StartCalendarInterval 数组
  4 项(Weekday 1-5 × 10:00/10:30/11:00/11:30),跑 `rquant morning-pulse`;
- `deploy/launchd/com.roxor.rquant-midday-report.plist`:12:00 × Weekday 1-5,
  跑 `rquant midday-report`;
- 两 plist 模式对齐现有 com.roxor.rquant-monitor(WorkingDirectory 主 checkout、
  venv 绝对路径、日志 logs/midday-briefing.log);
- 提供 `scripts/install-midday-launchd.sh`(cp + launchctl bootstrap,幂等);
- 注意 launchd Weekday 语义(0=周日),交易日最终判定仍在 CLI 内部(节假日兜底)。

### 推送

- notify 新 scene(如 `morning_pulse` / `midday_report`),报文 title 短 body 长,
  对齐 `notify/messages.py` 现有风格;PushDeer 只 admin(现有 PUSHDEER_KEYS)。

## 测试用例(coding agent 交付,全绿为验收前提)

### U 组(pytest,离线;快照/资金流全部用 RQUANT_PANORAMA_FAKE 或注入 fixture)

- U1 槽位归属:10:03→1000、10:36→1030、10:44(迟到>10min)→跳过、11:58→无 pulse 槽;
- U2 落盘幂等:同槽位重跑覆盖同一文件不产生副本;meta.json pushed 标记去重生效,
  --force 绕过;
- U3 增量计算:两份 fixture 快照 → 新晋涨停名单/涨停数 Δ/炸板 Δ 正确;首槽无前项
  不崩、Δ 省略;
- U4 连板现算:fixture limit_list_daily(昨 2 板/昨 1 板/昨无)× 今日快照(涨停/未涨停)
  → 3 板/2 板/首板/无 四路都对;
- U5 候选池:单位换算正确(daily_bar 千元→元,故意造 1000 倍陷阱数据验证)、
  阈值边界(0.8 倍恰好/涨停排除/VWAP 下方排除)、top20 截断;
- U6 持仓体检:fixture 持仓 → 浮盈/距止损计算正确;空持仓 → 该节完全不出现在报文;
- U7 报文渲染:pulse 与 digest 各 section 在 fixture 下的关键行断言(不做全文 golden,
  断关键数字与结构);
- U8 守卫:非交易日直接退出(mock is_trading_day)不拉快照不落盘;快照三路全失败
  (空表)→ 报文降级为"快照不可用"短讯 + 不写 snapshot parquet,不崩;
- U9 notify:dry-run 不调 push(spy);正常路径 scene 与 title/body 传参正确(mock)。

### E 组(集成,Fable 5 验收执行)

- E1 fake 模式 `--dry-run` 跑 morning-pulse(--slot 1000/1030 两次)+ midday-report:
  报文打印完整、parquet/meta/markdown 落盘齐、二次 1030 与 1000 有 Δ;
- E2 真实模式 `--dry-run --force` 各跑一次(当前盘中/盘后数据):不崩、报文数字合理、
  只读副本无写、主库无锁冲突;
- E3 真实推送一条到 PushDeer(用户手机收到 = 用户侧验收项);
- E4 launchd plist 安装脚本干跑(bootstrap 后 launchctl list 可见、print 触发时间正确),
  今晚挂上,明早 10:00 首个自然触发为最终验收。

## 验收标准(合 PR 硬条件)

1. U 组全绿 + 全量 pytest(底线 876)0 失败;ruff 干净;
2. E1/E2 通过,E3 用户收到推送;
3. 锁纪律 review:全文无主库写路径、无裸 duckdb.connect;
4. CHANGELOG [Unreleased] 更新;plist 不进 systemd 目录(本地 launchd 专属)。

## 分期与后续

- 本期交付上述全部;
- 二期(数据攒 ~20 天后):用 data/midday/ 历史做「上午强势→下午延续」胜率统计,
  给战报指标加历史胜率标注(方案 E);
- 半日→全日量能外推系数标定(373 只分钟样本)可与二期同做,先用 0.8 经验阈值。
