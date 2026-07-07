# surge/全景页快照数据源:爬东财新浪 → tushare rt_min(根治 IP 反爬)

**日期**:2026-07-07　**分支**:feat/surge-tushare-datasource
**执行**:Opus 4.8 subagent(coding)/ Fable 5(规划·测试设计·验收)
**背景**:2026-07-07 盘中,云端 IP 被东财(RemoteDisconnected)+ 新浪(HTML 反爬页)
双双拉黑,免费爬取全死,surge 零快照饿死、一早无推送。用户付费 tushare,rt_min/
rt_min_daily(token 认证不吃 IP 反爬)实测从本机秒回。

## 实测确认(2026-07-07 11:08,本机)

- **rt_min**(doc 374)批量:一次传全部 2011 只创业科创 → 返回 2008 行 0.1s;每只最新
  一根分钟 K(ts_code/trade_time/close/vol/**amount=当分钟量**)。**全市场粗筛 1 调/分**。
- **rt_min_daily**(doc 457)单只:300499 返回 99 行(9:30→11:08 全序列),每根当分钟量,
  **cumsum = 当日累计额 8.17 亿**(权威;昨回测 trends2 重构值虚高)。**单只一调,确认候选用**。
- adapter 已封装 `TushareAdapter.rt_min(ts_codes, freq)` / `rt_min_daily(ts_codes, freq)`。

## 架构(两层漏斗,全 tushare,单取数者喂 surge+全景)

```
每分钟主循环:
  1) rt_min(全部 A 股 ~5500,1 调) → 每只最新分钟量+价
  2) 内存累加器 per-stock 当日累计额(按 trade_time 分钟去重加,防同分钟重复)
  3) 组装快照(price/pre_close/涨停价/amount=累计额)→ 落 snapshot_full.parquet(喂全景)
  4) 检测层 filter 创业科创 → 粗筛(累计额 ≥ k_rough×avg20×curve)→ 候选
  5) 新候选:rt_min_daily(单只,1调/只) → 精确 cumsum 当日累计序列
     → rel_cum_N = today_cum(t)/前N日同刻累计中位(stk_mins) → v3 门 → 推送
```

### D1:全市场快照改 rt_min + 累加器

- 新 `fetch_full_market_snapshot()` 内部:调 `rt_min(全A股代码, "1min")`(代码全集从
  stock_basic 预载,排除退市/停牌容错)→ 归一化(price=close、amount=当分钟量、
  vol);**不再 import 东财/新浪路由**;
- **累加器** `CumulativeTracker`(模块级或 watcher 持有):`update(rt_min_df) -> snapshot_df`,
  per ts_code 记 `{last_minute, cum_amount}`;仅当 rt_min 的 trade_time 分钟 > last_minute
  才把该分钟 amount 累加(防同分钟重复计、防回退);输出快照的 amount = 累加后当日累计;
  price/high/low/pre_close 用 rt_min 最新值;涨停价用 add_limit_prices(昨收从
  只读副本 daily_bar T-1,预载);
- **重启续算**:启动时读**上一份** snapshot_full.parquet(若存在且为当日),用其 amount
  seed 累加器(续到最近一 tick,重启只丢 tick 间隙,confirm 层 rt_min_daily 恒精确兜底);
- amount 单位:rt_min amount 与 stk_mins 同族(确认层比值口径一致);快照给全景的 amount
  为当日累计(元),与旧东财 f6 语义一致——**核对 rt_min amount 单位(元 or 千元),归一到元**。

### D2:确认层用 rt_min_daily(精确)

- 候选确认时,今日累计序列改用 `rt_min_daily(候选, "1min")` 的 cumsum(替代原先从快照读的
  近似累计),前 N 日同刻基线仍用 stk_mins(不变);当日缓存(同一候选一天一调,不重复);
- 若 rt_min_daily 返回空/失败 → 退回累加器的近似累计(不阻塞,记 warning)。

### D3:tushare 限频与容错

- rt_min 1 调/分(全市场);rt_min_daily N 调/分(N=当分钟新候选,通常个位数~几十);
- 复用 adapter 现有限频/重试;单 tick 内 rt_min 失败 → 本分钟 miss(现有熔断退避不变,
  但不再需要 sina);rt_min_daily 单只失败 → 该候选延后重试,不阻塞队列;
- **socks 路由整段删除**(tushare 不需要代理);删掉 `_fetch_em_clist`/`_snapshot_routes`/
  东财新浪 import(surge 侧);全景 poller 的东财三路**保留**(本机 Mac 仍用),仅 surge 换源。

### D4:全景页受益(零改动)

surge 每分钟产出的 snapshot_full.parquet 现在来自 tushare(全 A 股 + 累计额),全景 poller
读共享 feed 即可(D1 云端 feed 路由已在)——**全景页数据源随之从 tushare 来,不再依赖云端
爬东财/新浪**。全景 poller 自身代码不改(仍以 feed 为第 0 路由,feed 新鲜就不自拉)。

## 交付物

1. `src/rquant/surge_watch.py`:fetch_full_market_snapshot 改 rt_min + CumulativeTracker;
   确认层今日累计改 rt_min_daily;删东财新浪/socks 路由;重启 seed;
2. 累加器逻辑(可单测的纯函数/类:分钟去重累加、seed、快照组装);
3. adapter 注入:run_surge_watch 已有 minute_fetcher 注入口;新增 snapshot 取数注入口
   (测试用 fake rt_min,不碰网络);
4. 测试:累加器(分钟去重/seed/回退)、rt_min 快照归一(mock)、rt_min_daily 确认(mock)、
   重启续算(读旧 parquet seed)、rt_min 失败→miss、simulate 三戏路仍过;
5. CHANGELOG。

## 测试用例(全离线,mock tushare)

- U1 累加器:连续两 tick 同分钟不重复加、新分钟才加、乱序/回退分钟不减;
- U2 快照组装:rt_min_df → snapshot(price/amount=累计/涨停价),单位归一到元;
- U3 seed:给旧 snapshot_full.parquet(当日)→ 累加器 seed 到其 amount,后续续加;
  旧 parquet 非当日 → 不 seed(从零);
- U4 确认:rt_min_daily mock 全序列 → cumsum today_cum → rel_cum_N 正确;返回空→退累加器近似;
- U5 rt_min 失败 → 本 tick miss route=none(熔断退避不变);
- U6 simulate 三戏路(注入 fake rt_min/rt_min_daily)仍全对;
- U7 回归:去掉东财新浪后既有 surge 测试更新(snapshot_fetcher 注入 fake,不再 mock _fetch_em_clist);
- 全量 pytest 底线 1030。

## 验收

U 全绿 + 全量 ≥1030 + ruff;盘中零 DB 写(累加器纯内存,快照 parquet);
不新增依赖(tushare 已在);**盘中真实验证由协调者/用户在云端跑**(本机也可跑 rt_min
验证,tushare 本机可达);CHANGELOG。

## 明确不做

- 不改 adapter 的 rt_min/rt_min_daily(已够用);
- 不改全景 poller 代码(仅数据源经 feed 间接换);
- 全景 poller 的东财三路自拉保留(Mac 本机用),不删——只 surge 侧换 tushare。
