# 每分钟爆量推送(surge-watch)+ 取数迁云端

**日期**:2026-07-06　**分支**:feat/surge-watch
**执行**:Opus 4.8 subagent(coding)/ Fable 5(规划·测试设计·验收)
**用户确认**:立项;**确认层用近 3 天基准判断爆量**(非回测的 20 日——与板块窗口 3 日
同哲学:短窗口抓当下;工程红利:每候选只拉 3 天分钟历史,tushare 配额无压力)。

## 背景与量级预期

- 回测实测(285 交易日,创业/科创+三件套+VWAP):日均 5.2 个信号、p90=10、
  90% 集中在 9:30-10:00 → 去重后每分钟批次常态 0-2 只,开盘高峰 3-5 只;
- 2026-07-06 实测办公网 Mac IP 被东财/sina 同时风控 → 每分钟级拉取**必须在云端**
  (82.156.0.68,IP 干净,systemd 体系现成,deploy.sh 一键部署)。

## 产品行为

**云端常驻服务 `rquant surge-watch`**(单进程循环,systemd timer 9:25 Mon-Fri 拉起,
15:02 自然退出),盘中每分钟:

1. **拉快照**(范围:**创业板+科创板全量** ~1900 只,与已验证策略同域;
   `RQUANT_SURGE_BOARDS` 可扩主板):em clist 直连(fs=`m:0+t:80` 创业 +
   `m:1+t:23` 科创,浏览器 UA/Connection:close/trust_env=False,分页自适应,
   每分钟约 20 请求)→ 失败本分钟记 miss 跳过(连续 miss 熔断退避,见守卫);
2. **落盘** `data/surge_live/snapshot.parquet`(原子写 tmp+rename,给 P2 Mac 消费);
   当日累计额序列驻内存(~1900 股 × 241 分钟 float,兆级),收盘整体落
   `data/surge_live/YYYY-MM-DD-series.parquet` 留研究;
3. **粗筛**(零外部调用):`cum_amount(t) ≥ K_rough × avg_amount_20d × curve(t)`,
   K_rough 默认 1.5(宽门,只进候选不推送);且 pct_chg>0、非 ST、有 20 日基线
   (缺基线的次新自动落选);curve 为**盘中累计额进度曲线**(见标定);
4. **确认层(近 3 天口径,用户 pinned)**——对当日新候选:
   - 经 tushare `stk_mins` 拉该股**近 3 个交易日** 1min bars(限频队列默认
     2 次/分,当日缓存不重拉),构造 3 日同刻累计额中位基准;
   - `rel_cum_3d = cum_amount(t) / median_3d_same_time_cum ≥ K_confirm`(默认 2.0);
   - 且现价 ≥ 当日均价(快照 amount/volume 近似 VWAP,对齐回测 require_vwap_strength);
   - 观察字段(入报文不做门):本分钟增量 vs 3 日同分钟中位、题材(kpl)、距涨停空间;
5. **推送**:本分钟新确认聚合一条 PushDeer(新 scene `surge_watch`,只 admin);
   **每票每日仅推一次**;9:33 前静默收集(开盘前 3 分钟太乱);单条最多 8 只,
   超出折叠「另有 X 只」;同时 append `data/surge_live/events-YYYY-MM-DD.jsonl`;
6. **守卫**:非交易日启动即退;午休 sleep;快照连续 5 分钟 miss → 推一条降级告警
   (每日至多一条)并退避(60→120→300s 封顶);tushare 失败该候选延后重试。

**口径说明(诚实标注)**:确认层 3 日窗口与回测验证的 20 日不同源,K_rough/K_confirm
为产品初始值(非回测标定),跑几天按实际推送量级调参——报文尾注明口径版本。

## 数据与纪律

- **云端 DuckDB 只读副本**(cloud rquant_ro.duckdb,5min 同步)仅在 **9:25 启动时**
  读一次基线:20 日均成交额(daily_bar.amount 千元 ×1000)、stock_basic 名称/板块、
  昨收与涨停价推算输入(state derive 复用)、kpl 题材成分——全部载内存,盘中零 DB 访问;
  **绝不碰云端主库**(monitor 是常驻写者);
- 自产数据全 parquet/jsonl,不写任何 DuckDB;
- tushare token 用云端 .env 现有配置;stk_mins 限频/重试模式对齐现有分钟回补实现。

## 盘中进度曲线标定(一次性交付物)

`scripts/calibrate-intraday-curve.py`:从**本地** minute_bar(373 只样本)算
市场级累计成交额进度曲线——每股每日归一化(t 时刻累计额/全日额),先股内取中位、
再跨股跨日取中位,输出 241 点单调序列 → `src/rquant/data/intraday_progress_curve.json`
(进 git,随代码到云端)。surge-watch 启动时加载;文件缺失 → 线性曲线兜底并 warning。

## P2:Mac 消费云端数据(本期代码就绪,配置后生效)

- panorama `SourcePoller` 快照新增**第 0 优先路由**:HTTP GET
  `RQUANT_CLOUD_FEED_URL`(指向云 nginx 暴露的 `data/surge_live/snapshot.parquet`,
  basic auth 凭据 env 配置),成功(且 as_of ≤120s)则本机不再自拉;
  **env 未配置 → 行为与现状完全一致(零风险)**;HTTP 失败回落现有三路;
- 云 nginx 配置块(28080 站点加 location)写进部署清单,用户 pair 执行;
- 注意快照范围差异:surge-watch 只拉创业/科创,panorama 需要全市场 → 云端 fetcher
  **每 5 分钟**额外拉一次全市场快照落 `snapshot_full.parquet` 供 Mac 用
  (全市场约 60 请求/5min,云端 IP 负担可控),Mac feed 路由读 full 版。

## 交付物清单

1. `src/rquant/surge_watch.py`(检测器:基线预载/循环/粗筛/确认/推送/守卫/落盘);
2. CLI `rquant surge-watch [--dry-run] [--simulate DIR] [--force-session]`
   (--simulate:读目录内快照序列离线回放,可测性设施;--force-session:忽略时段
   守卫,盘后验收用);
3. `scripts/calibrate-intraday-curve.py` + 标定产物 json(agent 在本地实际跑出);
4. notify 新 scene `surge_watch`(+ 降级告警复用 error 场景);
5. `deploy/systemd/rquant-surge-watch.service` + `.timer`(OnCalendar=
   `Mon..Fri *-*-* 09:25:00`,已知安全语法;service 类型 oneshot 长跑,15:02 自退);
6. panorama_poller 云端 feed 第 0 路由(env 门控);
7. `docs/deploy/2026-07-06-surge-watch-deploy.md`:pair 模式部署清单(deploy.sh +
   systemd-analyze 验证命令 + nginx 配置块 + .env 新增项);
8. 测试(下节)+ CHANGELOG。

## 测试用例

### U 组(pytest,全离线,注入时钟/mock 源)

- U1 曲线标定:fixture 分钟数据 → 241 点、单调不减、首 0 尾 1、中位口径正确;
  文件缺失 → 线性兜底 + warning;
- U2 粗筛:阈值边界(恰好 K_rough 倍)、ST 排除、pct_chg≤0 排除、缺 20 日基线跳过、
  千元→元单位陷阱(故意 1000 倍错误数据验证);
- U3 分钟序列:连续快照 → 累计序列与分钟增量正确;快照 miss → 该分钟 NaN 不崩、
  后续恢复;
- U4 确认层:3 日同刻中位构造(fixture 3 天分钟 bars)、K_confirm 边界、VWAP 门
  拦截、当日缓存命中不重拉(tushare spy 调用数);
- U5 去重/静默/折叠:每票每日一次;9:33 前不推(收集不丢);9 只 → 8+折叠;
- U6 tushare 限频队列:10 候选爆发 → 2/min 排队,确认顺序与延迟正确(注入时钟,
  不真 sleep);失败候选延后重试不阻塞队列;
- U7 守卫:非交易日退出;11:30/13:00/14:57 时段边界;快照连续 5 miss → 降级告警
  恰一条/日 + 退避序列 60/120/300;
- U8 落盘:events jsonl 结构、收盘 series parquet、snapshot 原子写;
- U9 poller 云端 feed:env 配 + HTTP mock 新鲜 → 本机 fetch spy 零调用;陈旧/失败
  → 回落三路;env 未配 → 现有测试全部原样通过(回归);
- U10 推送:scene 传参、报文结构(题材/量比/距涨停字段)、dry-run 零推送。

### E 组(Fable 5 验收)

- E1 `--simulate`:构造一天快照序列 fixture(含一只 10:05 爆量、一只 9:31 爆量
  但 9:33 才该推、一只粗筛过但确认不过)→ 回放后 events/推送 mock 逐条核对;
- E2 本地真实源 `--dry-run --force-session` 跑 3 分钟:快照拉取成功(或降级正确)、
  无异常、报文合理;
- E3 云端部署(pair 模式,用户执行):deploy.sh 输出核对 + timer 下次触发时间正确
  + nginx feed URL 可访问(curl 带 auth 200);
- E4 次日盘中:9:25 自启、首批推送到手机、观察每分钟批次量级(验证 5.2/日预期,
  超预期则调 K 参数)——用户侧终验。

## 验收标准(合 PR 硬条件)

1. U 组全绿 + 全量 pytest(底线 910)+ ruff;
2. E1/E2 通过;标定产物 json 已生成并 sanity(单调/241 点);
3. 锁纪律:全文无主库写、无裸 duckdb.connect、盘中零 DB 访问(启动预载除外);
4. systemd unit 语法经 deploy.sh post-check 或用户 systemd-analyze 验证后才算部署完成;
5. CHANGELOG 更新;部署清单文档齐。
