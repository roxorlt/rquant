# 全景页上云:云端自取数 + 28080 直连,彻底去隧道

**日期**:2026-07-06　**分支**:feat/panorama-cloud
**执行**:Opus 4.8 subagent(coding)/ Fable 5(规划·测试设计·验收)
**背景**:办公网 DPI 掐 SSH(与当初掐 frp 同源),反向隧道方案死亡;今日事故链
(僵尸端口→重连风暴→sshd 配置事故)全程复盘毕。云端取数地基今日已就绪
(surge-watch 落盘、dc_board 在云、em 从云端 IP 干净可达)。

## 目标拓扑

```
现状:手机 → 云nginx:28080 → 隧道18506 → Mac:8506(Mac拉数,被办公网风控)
目标:手机/朋友(任意网络) → 云nginx:28080(basic auth) → 云127.0.0.1:8506
      (云端 streamlit,poller 读云端本地 feed + 自拉)——零隧道、零 Mac 依赖
```

Mac 本地 8506 照常保留(本机看盘用),两套独立运行同一代码。

## 关键设计决策

### D1:云端盘中只保留一个全市场取数者(surge-watch),全景页喂共享数据

- **surge_watch 改造**:盘中每分钟直接拉**全市场**快照(原来只拉创业/科创),
  检测层从全量里过滤 boards(gem/star,行为不变);`snapshot_full.parquet`
  落盘节奏 5min→**每分钟**(与主循环同拍,不再单独拉)。请求量对比:
  原方案(surge 拉创业科创 20 页 + 全景独立拉全市场 59 页)≈79 页/分 →
  新方案(一次全市场 59 页共用)——**云端 em 负载反而下降**;
- **panorama_poller 云端 feed 路由支持本地文件**:`RQUANT_CLOUD_FEED_URL`
  以 `/` 开头(或 `file://`)时按本地路径读(mtime 判新鲜 ≤120s),命中即用、
  陈旧/缺失回落自拉三路。云端配置指到
  `/home/lighthouse/rquant/data/surge_live/snapshot_full.parquet`;
  (HTTP 分支保留,Mac 侧远程读云端 feed 的 P2 能力不变)。

### D2:poller 分时段节奏(云端 24/7 常驻的取数卫生)

`SourcePoller` 增加 `off_hours_interval: float = 600`:交易时段(工作日
09:00-15:10)用 interval(60s),其余时间 600s——非盘中数据不变,没必要每分钟
打源。判定用本地时间+周一至五(节假日不额外判,600s 打一次无害)。
盘中云端 poller 大多数时候命中 surge 的本地 feed(零请求),自拉只是兜底。

### D3:kpl 题材成分上云(默认体系「开盘啦题材」在云端不能是空的)

新增 `deploy/systemd/rquant-kpl-snapshot.{service,timer}`:工作日 16:35 跑
`rquant data-backfill --dataset kpl_concept`(写云端主库 kpl_concept_member
快照表)。写者串行:16:35 槽位空闲(monitor 15:02 止、daily 17:00 起、
backup/replica-sync 每 5min 是只读拷贝不写主库——**agent 核对 backup.timer
实际行为后在部署文档注明**)。replica-sync 5 分钟内把表带进只读副本,
全景页即可读到。OnCalendar 用已知安全语法 `Mon..Fri *-*-* 16:35:00`。

### D4:nginx 28080 从隧道反代改本地反代

`deploy/nginx/rquant-panorama-cloud.conf`(28080 server block):
`proxy_pass http://127.0.0.1:8506`,WebSocket 头齐全(streamlit 必需),
`auth_basic_user_file /www/server/nginx/conf/.htpasswd-panorama`(朋友账号
体系原样继承)。部署时覆盖服务器上手工维护的 panorama.conf。

### D5:云端 panorama systemd service

`deploy/systemd/rquant-panorama.service` 模式对齐 rquant-dashboard.service
(Type=simple/EnvironmentFile=.env/Restart=always),要点:
- `--server.port 8506 --server.address 127.0.0.1`(只给 nginx,不直接对外);
- 环境:`RQUANT_CLOUD_FEED_URL=/home/lighthouse/rquant/data/surge_live/snapshot_full.parquet`、
  `RQUANT_PANORAMA_SOCKS=`(空,云端无本地代理,禁用该级);
  写进 unit 的 Environment= 行(不polluting .env,这两项是云端部署形态专属)。

## 交付物

1. `src/rquant/surge_watch.py`:全市场单次拉取重构(检测层 boards 过滤下移)、
   snapshot_full 每分钟落盘;配置字段与默认值调整,注释写清共享语义;
2. `src/rquant/panorama_poller.py`:cloud feed 本地文件分支 + `off_hours_interval`;
3. `deploy/systemd/rquant-panorama.service`、`rquant-kpl-snapshot.{service,timer}`;
4. `deploy/nginx/rquant-panorama-cloud.conf`;
5. `docs/deploy/2026-07-06-panorama-cloud-deploy.md`:pair 部署清单(deploy.sh、
   uv sync 确认 streamlit/altair、nginx cp+reload、kpl 首跑手动触发、验证 curl、
   Mac 侧无需任何改动的说明、回滚);
6. 测试 + CHANGELOG。

## 测试用例

### U 组(全离线)

- U1 poller 本地 feed:路径存在且 mtime 新鲜→用之且自拉 spy 零调用;陈旧(>120s)
  →回落自拉;文件缺失→回落;`file://` 前缀与裸路径等价;HTTP 分支回归不破;
- U2 off_hours_interval:注入时钟——盘中时刻返回 60s 节奏、盘外/周末 600s;
  边界 09:00/15:10;
- U3 surge 全市场重构:mock 全市场 rows(含主板/创业/科创/ST)→检测候选只含
  gem/star 且排 ST(行为与改前一致,用改前同款 fixture 断言);snapshot_full
  落盘每 tick 一次且含主板行;
- U4 surge 请求层:fs 参数为全市场(不再是 gem/star 两段),分页语义不变;
- U5 回归:全量 pytest 底线 963(改 surge fixture 允许最小更新,语义不放松)。

### E 组(Fable 5 验收)

- E1 fake 模式 8516:全景页功能回归(默认体系/联动/图表);
- E2 本地造 fresh snapshot_full.parquet + 配 env 指向它 → 起全景页,验证
  状态行路由显示 cloud_feed 且无自拉(日志);
- E3 surge --simulate 回归(全市场重构后三戏路 fixture 仍全对);
- E4 云端部署(pair):deploy.sh → nginx cp+reload → 手机/朋友直访 28080
  出账号框、登录见全景页;kpl timer 首跑后默认体系有数据;
- E5 次日盘中:云端全景页数据每分钟更新(状态行 age<90s)、路由显示
  cloud_feed(共享 surge 快照),朋友网络可访问。

## 验收标准

U 全绿 + 全量 pytest ≥963 + ruff;E1-E3 本地全过;部署清单完整可执行;
锁纪律不变(云端 panorama 只读副本 + parquet);CHANGELOG。

## 明确不做(本期)

- 不迁 nl-screen/canvas/Lab(只迁全景页);
- 不动 Mac 本地全景页与 midday/launchd(照常运行);
- 不删隧道配置(留档,deploy/tunnel README 加一段"已被云端直跑取代"注记)。
