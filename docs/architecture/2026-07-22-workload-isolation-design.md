# rQuant 工作负载解耦与故障隔离架构设计

**状态：** 设计基线，正在按隔离运行时一次性实施
**日期：** 2026-07-22
**适用范围：** 数据采集、盘中监控、信号与通知、盘后生产流水线、研究回补、策略回测、自动优化、模拟盘和页面查询
**不包含：** 实盘自动下单、Tick/Level2、高频交易、Kafka/PostgreSQL/Kubernetes 等新增基础设施

## 0. 实施与验收映射

本轮实现不以“文件已创建”为完成条件，而以跨层可恢复行为为准。当前开发分支的
模块与本设计对应如下：

| 设计能力 | 实现入口 | 当前证据 |
|---|---|---|
| 持久 Lab Job Center | `lab_job_center.py`、`lab_worker.py`、`lab_eta.py` | 任务租约、checkpoint、暂停续跑、ETA 和页面断开续跑测试 |
| 统一分钟网关 | `market_minute_gateway.py`、`live_spool.py` | 额度租约、修订代际、stale/degraded、独立 cursor 测试 |
| PIT 盘中特征 | `intraday_feature_engine.py`、`feature_live_service.py` | 同刻基准、累计进度、5/10 分钟加速度及缺分钟测试 |
| 独立策略 runner | `strategy_runner.py`、`strategy_live_service.py` | 冻结 StrategySpec、独立状态库、崩溃重放不重复信号 |
| 信号、通知、模拟盘 | `signal_bus.py`、`notification_worker.py`、`paper_signal_*` | 原子路由、持久 outbox、未知投递、T+1、下一分钟成交及幂等测试 |
| serving 与只读页面 | `serving_read_models.py`、`serving_publisher.py` | 不可变代际、哈希验证、零写副作用 ServingReader |
| 慢变参考数据 | `reference_data_registry.py` | ST、停牌、上市、板块、复权和涨跌停制度双时间 as-of 测试 |
| 实验与晋级 | `experiment_registry.py` | 预注册、外层样本、BH 校正、真实前向证据与晋级门测试 |
| 灾难恢复 | `recovery_manifest.py` | 全角色快照、哈希恢复、原子指针和故障注入演练 |
| 密钥最小权限 | `secret_scope.py` | 数据源、通知和无密钥 worker 的 capability 测试 |

发布前仍必须完成三道总验收：

1. systemd unit 与 live/serving/research slice 在云端通过原样解析和资源限制检查；
2. 固定 fixture 从分钟批次到 serving 的端到端回放及六类故障演练全部通过；
3. 新旧实时链路先影子并行，达到退休门后再移除旧的 `monitor`/`surge-watch` 重叠职责。

## 1. 结论

rQuant 可以继续解耦，而且应该继续解耦。推荐目标不是把一个个人项目拆成大量网络微服务，而是建立：

> **单写者单元 + 不可变数据批次 + 独立状态库 + 原子发布指针 + 受限后台任务**

这套结构把系统分成七条互不直接写对方存储的流水线：

1. 行情与业务数据采集
2. 数据校验与权威发布
3. 盘中特征计算
4. 策略推理与信号生成
5. 通知和模拟盘消费
6. 研究、回测与自动优化
7. 页面查询与结果展示

在同一台腾讯云服务器上可以做到进程、写锁和大部分资源隔离，但不能消除共享 CPU、内存、磁盘和网络带宽的物理影响。最终推荐拓扑是：

- 腾讯云主机只承担实时关键链路、日终生产发布和轻量页面服务。
- Mac 或独立研究 worker 承担大规模回补、回测和参数搜索。
- 两边只交换带哈希和版本号的不可变数据快照与研究结果，不远程共享一个可写数据库文件。

## 2. 为什么当前仍会互相影响

当前架构已经完成两项重要隔离：

- Dashboard 等只读消费者优先读取 `rquant_ro.duckdb`。
- 历史分钟、竞价和研究元数据进入独立 `research.duckdb` 与分区 Parquet 湖。

剩余耦合主要来自以下位置：

| 当前关系 | 影响 |
|---|---|
| `monitor` 同时拉行情、写分钟线、算信号、写事件并调用通知 | 任一环节变慢或异常都会拖长整轮监控 |
| `surge-watch` 独立拉全市场分钟快照并直接通知 | 与 `monitor` 重复访问数据源，两个策略没有共享统一分钟时钟和质量状态 |
| 盘中长进程持有生产 DuckDB 写连接 | 其他写任务只能等收盘；新连接还可能撞 DuckDB 文件锁 |
| 日终采集、筛选、池子同步和发布靠一个流水线串行完成 | 某个数据源失败会阻塞后续全部步骤，恢复粒度较粗 |
| Strategy Lab 页面仍可能参与计算编排 | Streamlit rerun、切 tab 或浏览器断开会影响用户感知与任务状态 |
| 历史回补包含预演、网络下载、落湖、快照、审计和回放 | 一个命令可能运行数小时，无法安全利用开盘前或午休的小窗口 |
| 信号生成与 PushDeer/PushPlus 调用相邻 | 通知超时会影响信号链路，重启后还需要额外防重复 |
| 生产代码版本与研究采集、快照、策略语义绑得过紧 | 无关代码变更也可能使研究 observation 降级 |

这里的关键问题不是进程数量，而是职责、写入所有权和发布边界不清晰。

## 3. 解耦目标和可验证标准

### 3.1 四级隔离

| 级别 | 含义 | rQuant 目标 |
|---|---|---|
| L1 进程隔离 | 一个进程崩溃不带走其他进程 | 所有关键链路由独立 systemd unit 管理 |
| L2 存储隔离 | 一个任务的写锁、迁移或损坏不阻塞其他任务 | 每个可变数据库只有一个 owner；跨单元只读不可变产物 |
| L3 资源隔离 | 重计算不能耗尽实时链路的 CPU、内存、I/O | systemd slice/cgroup；研究任务限额并可暂停续跑 |
| L4 主机隔离 | 研究机故障、满盘或重启不影响盘中系统 | 实时云主机与 Mac/独立研究 worker 分离 |

### 3.2 必须通过的故障测试

1. 杀掉一次回测 worker，盘中行情采集、信号和通知延迟不变。
2. 关闭 Strategy Lab 页面，后台任务继续运行，重新打开后能看到原进度。
3. 暂停通知服务，信号仍完整落盘；恢复后只补发应发且未过期的事件。
4. 某一分钟行情迟到或缺失时，系统标记 `degraded`，不能用 0 成交量代替。
5. 重启任一策略 runner，不能重新推送同一个信号。
6. 研究回补达到 CPU、内存或时限上限后安全 checkpoint，不能破坏已发布数据。
7. 同一快照、策略版本和参数重复回放，结果与事件序列完全一致。
8. Dashboard、Panorama、NL Screen 和临时查询均不连接生产写库。

## 4. 三种可选方案

### 4.1 方案 A：同机逻辑拆分

所有服务仍在腾讯云主机运行，但拆分数据库、目录、systemd unit 和资源组。

优点：改造成本最低，部署和备份方式变化小。
缺点：研究计算仍与实时链路共享物理资源；磁盘满或主机故障仍是共同故障点。

### 4.2 方案 B：实时云端 + 混合研究 worker

腾讯云承载实时关键链路和轻量日终任务；研究任务由云端受限 worker 或 Mac worker 领取。输入和输出都是不可变快照。

优点：先保持现有技术栈，再获得接近完整的故障隔离；可以按任务规模选择执行位置。
缺点：需要数据快照传输、任务租约和 worker 身份管理。

### 4.3 方案 C：完整分布式服务

引入消息队列、对象存储、独立数据库和容器编排。

优点：扩展上限高。
缺点：对当前个人项目明显过重，运维成本会超过策略研究收益。

### 4.4 推荐

采用 **方案 B**，但按 A 的方式先完成逻辑拆分。只有当任务量和故障数据证明现有文件队列、SQLite 和 systemd 不够时，才评估方案 C。

## 5. 目标架构

```text
                          rQuant 控制面
              任务账本 / 水位 / 租约 / 心跳 / 运行策略
                               |
       +-----------------------+-----------------------+
       |                       |                       |
       v                       v                       v
  数据采集面              实时推理面                研究计算面
  source gateway          feature-live              research worker
       |                  strategy runners            backtest/optimize
       v                       |                       |
 raw immutable batches        v                       v
       |                  signal spool           result artifacts
       v                       |                       |
 validator/publisher          v                       v
       |                 signal router          experiment registry
       v                       |                       |
 canonical authority          +----------+------------+
       |                                  |
       +-------------+--------------------+
                     v
                serving publisher
                     |
          serving DB / current snapshots
                     |
           Dashboard / Lab / Panorama

 signal router -> notifier
 signal router -> paper broker
 paper broker  -> paper account store
```

### 5.1 设计规则

1. **一个可变存储只能有一个写入者。** 其他进程通过只读副本、不可变文件或事件 spool 读取。
2. **消费者不调用上游数据源。** Tushare、AKShare、Ashare、mootdx 的访问集中在 source gateway。
3. **采集不包含策略语义。** 原始数据先保存，再由特征和策略层解释。
4. **信号不等于通知。** 信号先进入可恢复 outbox，再由通知服务投递。
5. **研究不读取正在变化的生产状态。** 每次研究绑定精确 dataset snapshot。
6. **页面不执行长任务。** 页面只提交 job spec、取消任务、读取进度和结果。
7. **发布是原子的。** 先生成候选代际，校验通过后只切换 `current.json` 或只读副本。
8. **失败默认保留证据。** 原始批次、manifest、错误和旧 current 指针都可审计、可回滚。

## 6. 各单元的职责与数据所有权

| 单元 | 输入 | 唯一写入 | 输出 | 失败时不应影响 |
|---|---|---|---|---|
| `source-gateway` | 外部数据源、watchlist | `live/raw`、source 状态 | 带序号原始批次 | 策略、通知、页面已有数据读取 |
| `data-publisher` | raw batch | 生产 DuckDB、canonical manifest | 已校验权威代际 | 研究旧快照、页面旧 serving |
| `feature-live` | raw/canonical current | `live/features` | PIT 特征快照 | 原始数据采集 |
| `strategy@name` | 特征快照、冻结 StrategySpec | 自己的 runner state | signal envelope 文件 | 其他策略、通知、采集 |
| `signal-router` | signal spool | `signal_bus.sqlite3` | 有全局序号的信号流 | 策略继续积压 spool |
| `notifier` | signal bus | `notification_state.sqlite3` | PushDeer/PushPlus 结果 | 信号生成、模拟盘 |
| `paper-broker` | signal bus、报价 | `paper_account.sqlite3` | 模拟成交和持仓事件 | 通知、策略、研究 |
| `research-ingest` | sealed canonical/live day | research candidate/catalog | 分区 Parquet、research DB | 次日实时监控 |
| `lab-scheduler` | typed job spec | `lab_jobs.sqlite3` | 任务租约和状态 | 页面、实时链路 |
| `research-worker` | snapshot、job lease | 独立 artifact 目录 | 回放/优化结果 | 生产数据和实时服务 |
| `serving-publisher` | 各只读摘要 | `serving.duckdb` 或 serving 代际 | 页面查询模型 | 上游权威数据 |
| Streamlit apps | serving/current/result | 无业务写入 | 页面 | 所有后台任务 |

不允许出现以下关系：

- 策略 runner 直接写生产 DuckDB。
- Strategy Lab 直接启动一个没有持久 job id 的长回测。
- notifier 回调修改策略状态。
- research worker 修改 production current 指针。
- 两个采集进程分别请求同一个全市场分钟接口。

## 7. 数据采集面的详细设计

### 7.1 按频率和故障域拆分数据通道

`source-gateway` 是逻辑边界，不要求所有接口塞进一个大进程。建议用同一套 adapter、批次契约和限流规则，按数据通道运行独立 unit：

| 通道 | 典型频率 | 范围 | 用途 |
|---|---|---|---|
| `auction_match` | 09:25 后一次或少量重试 | 当日策略域 | 竞价缺口、竞价量、候选初筛 |
| `watchlist_quote` | 约 5 秒 | 当前池与观察标的 | 价格触线、临近涨停、快速风险状态 |
| `market_minute` | 1 分钟 | 全市场或策略域 | 同刻放量、累计进度、板块/市场环境、分钟回放 |
| `daily_close` | 收盘后一次 | 全市场 | 日线、复权、池子和盘后研究基线 |
| `reference_slow` | 每日或按变更 | 全市场 | 证券状态、交易日历、板块、公司行为和基础信息 |

推荐的 unit 形态是 `rquant-feed@auction-match.service`、`rquant-feed@watchlist-quote.service` 和 `rquant-feed@market-minute.service`。一个通道崩溃不应停止其他通道，但同一接口的请求仍由唯一通道负责。`monitor` 和 `surge-watch` 不再各自拉取重叠数据。

不同端点的频控预算静态分配并记录实际调用量；历史回补额外领取离线额度 lease。这样实时采集不会因为一个研究 worker 并发拉取而被 Tushare 限流。

### 7.2 原始批次信封

每个批次至少携带：

```text
dataset_id
schema_version
source
source_request_id
batch_id
sequence
event_time_start / event_time_end
source_time
received_at
available_at
row_count
content_sha256
quality_status
producer_version
```

其中：

- `event_time` 表示市场中实际发生时间。
- `received_at` 表示 rQuant 真正收到数据的时间。
- `available_at` 表示策略最早可以合法看见数据的时间，是防止未来函数的关键字段。
- `quality_status` 只能是 `candidate`、`published`、`degraded`、`quarantined` 等显式状态，不能把缺失数据静默填 0。

### 7.3 文件发布方式

实时消费者需要最新数据，研究又需要完整历史。可以同时提供：

1. `current/*.parquet`：原子替换的最新快照，供低延迟消费者读取。
2. `spool/<date>/<sequence>-<hash>.parquet`：不可变批次，供可靠消费和断点恢复。
3. 收盘后 compaction：把当日小文件合并为一个或少量分区文件，并生成 manifest。

消费者用自己的 cursor 记录最后处理 sequence。即使处理器停机十分钟，也能按 spool 顺序补读，而不是只看到最新快照。

### 7.4 数据源网关还应统一处理

- Tushare 调用频控、批量拆分和剩余额度。
- 数据源健康、延迟、空响应、字段变化和重试退避。
- 同一请求去重，避免两个策略重复耗费权限。
- 主源/备用源差异比较，但不静默混用不同口径。
- 迟到、乱序和修订数据；修订必须生成新 revision，不覆盖历史证据。
- 网络断开时继续发布明确的 stale 状态，不伪造新行情。

## 8. 实时特征与策略推理解耦

### 8.1 `feature-live`

共享且计算成本较高的特征只算一次，例如：

- 同一分钟历史成交额基准。
- 截至当前分钟的累计成交额进度。
- 5/10 分钟日内加速度。
- VWAP、价格相对 VWAP 的距离与斜率。
- 内外盘方向、上涨/下跌成交额代理指标及质量标签。
- 90 日价量分布、筹码密集区、当前价格历史百分位。
- 市场、板块、涨停和情绪环境的时点可见值。
- 复权口径、涨跌停价、停牌和证券状态。

历史 `feature-replay` 与盘中 `feature-live` 共享同一个特征契约和纯计算核心。允许一个用向量化批计算、一个用增量状态，但必须通过前缀不变和逐分钟一致性测试。

### 8.2 每个策略独立 runner

建议用 systemd template 运行：

```text
rquant-strategy@n-shape.service
rquant-strategy@growth-board-surge.service
rquant-strategy@auction-gap.service
```

每个 runner：

- 只读取冻结 StrategySpec、特征快照和自己的 state。
- 独立维护候选、等待 B、持仓观察和 S 条件状态机。
- 输出结构化 `SignalEnvelope`，不直接通知、不写模拟仓。
- 有独立超时、重启、熔断和运行指标。

这样一套新策略出现 bug 时，不会停止另外两套策略。

### 8.3 共享特征服务的风险

`feature-live` 会成为共享依赖，因此必须：

- 保留原始行情，让它重启后可按 sequence 补算。
- 发布每个 feature batch 的输入 hash、特征语义版本和水位。
- 某个可选特征失败时标记该字段 unavailable，不让整个基础特征批次失败。
- 策略声明 required/optional features；缺 required 时 fail closed，缺 optional 时按冻结规则降级。

## 9. 信号、通知和模拟盘解耦

### 9.1 信号事件

`SignalEnvelope` 至少包含：

```text
signal_id                # 内容确定的幂等 ID
strategy_id/version
parameter_fingerprint
dataset_snapshot_id
feature_snapshot_id
event_time
available_at
candidate_id
action                   # WATCH / B_INTENT / REDUCE / S_INTENT / CANCEL
reason_codes
evidence
expires_at
producer_commit
```

runner 先把事件原子写入自己的 spool。`signal-router` 是 `signal_bus.sqlite3` 的唯一写入者，负责全局排序、schema 校验、幂等和隔离坏事件。

### 9.2 通知服务

通知服务只关心：

- 哪些 signal scene 需要推送。
- recipient/channel 路由。
- 幂等键、冷却时间、有效期和升级策略。
- 失败重试、死信和恢复通知。

通知 API 超时不会阻塞策略。重启后按 `signal_id + recipient + channel` 判断是否已成功投递，从结构上根治重复 Push，而不是只靠进程内集合。

### 9.3 模拟盘服务

`paper-broker` 是独立消费者：

- 将 `B_INTENT`/`S_INTENT` 与当时可成交价格、T+1、涨跌停、停牌、手续费和滑点结合。
- 自己维护现金、持仓、冻结数量和订单状态。
- 每个成交引用原始 signal 和 feature snapshot。
- 失败或暂停不会改变策略信号历史。

策略效果、通知到达率和模拟成交表现因此可以分别评估。

## 10. 盘后生产流水线解耦

把当前日终大命令拆成可恢复 DAG，但仍用 systemd 和 SQLite 账本，不引入 Airflow：

```text
daily-capture
    -> daily-validate
    -> canonical-publish
    -> pool-build
    -> serving-refresh
    -> replica-sync
    -> research-ingest
    -> backup
```

每一步都有：

- 独立 `run_id`、输入版本、输出 manifest 和退出状态。
- `preview -> apply -> verify -> publish` 四阶段。
- 内容一致时幂等跳过。
- 失败后只重跑当前步骤，不重拉已经成功的数据。
- 发布前的候选目录；校验通过后原子切换 current。

日线采集失败时，可以保留昨天的 published authority 并显示 stale；不能发布半套当天数据。

## 11. 研究、回补、回测与优化解耦

### 11.1 研究输入必须封存

每个研究任务只接收 `ResearchRunSpec`：

```text
job_id
job_type
dataset_snapshot_id
strategy_spec_id
feature_contract_version
parameter_space
train/validation/test ranges
cost/slippage model
random_seed
resource_class
deadline
```

任务启动后不能因为 catalog current 改变而自动换数据。

### 11.2 持久任务中心

`lab_jobs.sqlite3` 由 scheduler 单写，记录：

- queued/running/checkpointed/succeeded/failed/cancelled。
- 当前 phase、已完成数量、吞吐、ETA 和最后 heartbeat。
- worker、租约到期时间、重试次数和首个失败点。
- 输入快照、代码、参数和输出 artifact hash。

Streamlit 只创建 job spec 和读取状态。浏览器卡死、刷新、切 tab 或关闭不影响任务。

### 11.3 分段与 checkpoint

长任务按可验证边界拆分：

- 回补：按 `trade_date x ts_code` 或固定 session shard。
- 特征：按交易日分区，跨日状态显式作为输入 checkpoint。
- 回放：按冻结时间窗口或策略候选批次。
- 参数搜索：先低成本消融和粗筛，再对少量候选做完整 walk-forward。

每个 shard 完成后写不可变结果和 hash；超时只停止领取新 shard。此前已经完成的工作不回滚、不重算。

### 11.4 执行位置

| 任务 | 默认位置 | 盘中规则 |
|---|---|---|
| 数据质量只读审计 | 云端低优先级 | 可运行，但不得连接生产写库 |
| 小型固定回放 | 云端 batch slice | 可按资源预算运行 |
| 大规模分钟回补 | Mac/独立 research worker | 可运行，但需独立 Tushare 额度闸门 |
| 参数组合搜索 | Mac/独立 research worker | 不占用实时主机 CPU/I/O |
| production publish/migration | 云端 live host | 仍只在交易保护窗口外 |
| serving refresh | 云端短任务 | 原子发布且资源受限时可盘中运行 |

完成物理隔离后，午休可以用于研究计算；但任何生产写入、服务部署、schema migration 仍遵守交易保护窗口。

## 12. 页面与查询层解耦

### 12.1 Serving 模型

页面不再横跨生产、研究和任务状态库做大 join。由 `serving-publisher` 生成面向页面的小型数据模型：

- 最新运行与数据新鲜度。
- 当前候选、信号、模拟持仓和通知状态。
- 策略版本、最近研究结论和可信度等级。
- job 列表、进度、ETA 和结果摘要。
- 预聚合回测对比、消融和 walk-forward 图表数据。

原始分钟和完整交易明细按需从不可变 artifact 读取，不放在首页查询路径。

### 12.2 页面只做四件事

1. 查看 current 状态。
2. 构造并提交 typed job spec。
3. 取消尚可取消的任务。
4. 查看或导出 immutable result artifact。

页面自身没有策略全局变量，也不把任务结果只保存在 `st.session_state`。

## 13. 建议的物理目录

```text
/home/lighthouse/rquant/data/
  operational/
    rquant.duckdb
    rquant_ro.duckdb
    generations/
  live/
    raw/YYYY-MM-DD/<channel>/
    current/
    features/YYYY-MM-DD/
    manifests/
  spool/
    signals/<strategy>/YYYY-MM-DD/
    quarantined/
  state/
    source_gateway.sqlite3
    signal_bus.sqlite3
    notification_state.sqlite3
    paper_account.sqlite3
    lab_jobs.sqlite3
    backfill_state.sqlite3
    strategies/<strategy_id>.sqlite3
  research/
    catalog/research.duckdb
    catalog/research_ro.duckdb
    lake/<dataset>/<partition>/
    snapshots/<snapshot_id>/
  artifacts/
    jobs/<job_id>/
    experiments/<experiment_id>/
  serving/
    generations/<generation_id>/serving.duckdb
    current.json
  backups/
```

目录名可以在迁移时兼容现有路径；关键是 owner 和发布协议，而不是一次性移动全部文件。

## 14. 资源隔离与调度

### 14.1 systemd slices

建立一个父预算和四个资源 plane。dash slice 名由 systemd 解析为
`/rquant.slice/rquant-live.slice` 等层级；运行时审计以 `systemctl show ... ControlGroup` 为准，
不硬编码 cgroup 路径：

| Slice | 成员 | 优先级 |
|---|---|---|
| `rquant-live.slice` | source、feature-live、strategies、signal-router、notifier | 最高 |
| `rquant-serving.slice` | dashboard、panorama、serving publisher | 中等 |
| `rquant-research.slice` | ingest、repair、backfill、replay、optimizer | 最低 |
| `rquant-maintenance.slice` | backup、replica-sync | 最低，按并发峰值求和 |

当前最低准入基线是实测 2 CPU / 7.51 GiB 可见内存的 8 GiB 标称主机。生产证据为 monitor
current 2415 MiB、peak 2814 MiB，backup peak 1303 MiB。父级与 live 的
`MemoryLow=3072M` 使祖先保护可兑现；父级/live/serving 只设
`MemoryHigh=6144M/3840M/512M`，maintenance 在证据完成前不设 `MemoryHigh` 或 hard cap，只保留
低 CPU/IO 权重。research 独立保持
`MemoryMax=768M` 与精确 `CPUQuota=100%`，在 2 CPU 主机上最多占用一个核。

正常 research 运行态的静态上界为 live 3840 + serving 512 + research 768 + OS/其他
`system.slice` 1280 = 6400 MiB。maintenance 没有可信 aggregate 峰值，不能再宣称其运行态总量
低于 7680 MiB；backup 与 replica 可并发，文件缓存也不能用 512 MiB service cap 强杀。二者与
research 通过固定 root-owned flock wrapper 做全生命周期跨 plane 排他：maintenance pending
阻止新 research，可抢占已运行 research，并有有界等待；同 plane 仍允许并发，timer calendar 不变。
wrapper 路径不可由 `.env` 覆盖，安装时配套发布 root-owned SHA-256；registry 使用
PID + process starttime + boot ID 防止 PID 复用误杀，并在 intent lock 内回收 crash 遗留项。

research-ingest 不在 research plane 内执行 replica refresh。systemd 使用
`Requires=rquant-replica-sync.service` + `After=` 在同一启动事务中编排独立 maintenance oneshot，
同名 timer job 由 systemd 合并；required job 失败或后续 generation readiness 不通过都阻止 ingest。

云端候选采样器 append-only 保存至少 24 小时 canonical hash-chain 原始样本。严格 schema 要求
每个 sample 带 Linux boot ID、wall timestamp 与 `CLOCK_BOOTTIME` 纳秒值。同 boot 连续段内双时钟
必须严格递增，5 分钟 cadence 允许的单次 timer jitter 上限为 450 秒；完整 24 小时窗口至少有
289 个端点样本。重启前后的段可共存于 raw 链，但不能合并凑窗口。摘要还要求 backup/replica
非零成功 runs、样本数、成功持续时长、raw SHA-256、OS/system.slice peak、最小 MemAvailable 和
完整同 boot 窗口。strict gate 重放整条链、重新汇总并逐字段对比声明摘要，伪 raw、稀疏两点窗口
或仅重写自声明 SHA fail closed。证据完整但尚未发布经评审 maintenance 阈值时，静态/health 仍为
pending calibration，strict gate 与 research admission 继续阻塞，避免瞬时观测自动变成生产阈值。

静态 fixture 只证明声明和 aggregate arithmetic。原始腾讯云必须运行固定路径的
`scripts/verify-workload-isolation.sh`，由真实 `systemd-analyze verify/calendar`、loaded service
实例枚举、resolved `Slice/ControlGroup`、`memory.low` 和 research `cpu.max` fail-closed 验收；
macOS 结果不能替代该 gate。

旧 runtime template 的 accept migration 使用固定持久 journal，并从 startup recovery 到 journal
cleanup 全程持有 root:root `0600` 的固定 flock；并发调用可预测地报 busy。所有 mutation 前先保存
unit 文件与可恢复状态矩阵；phase/state 只通过同目录 temp、fsync file、rename、fsync directory
发布，phase 保留 last-good 冗余。`ERR/TERM/INT/HUP` 共用 rollback，SIGKILL/断电后的下一次启动先
恢复 journal 再 preview/accept。phase 撕裂时从 last-good 恢复，冗余也损坏则执行 fail-safe rollback；
unit/state journal 缺失字段或损坏时仍拒绝继续，不猜测生产 unit 状态。

### 14.2 资源准入

scheduler 在领取任务前检查：

- 当前是否交易日与所处 session。
- live 服务延迟和 backlog 是否健康。
- 剩余内存、磁盘空间、I/O 压力。
- 数据源额度和下一次重置时间。
- 任务能否在 deadline 前完成；不能则只领取更小 shard。

研究任务不是简单地“盘中全部禁止”。满足只读、低资源、无数据源冲突和可抢占四个条件时，可以运行。

Strategy Lab worker 的资源准入位于 claim 可见之后、claim consume 和
`LabClaimSpool.execution_admission` 之前。资源不足只返回 `deferred`，claim 继续留在 pending；
它不会打开研究数据、执行 adapter、写结果或发布失败终态。达到 `retry_at` 后重新读取实时资源
快照；期间若 scheduler 撤销或替换 claim，原 claim 不会复活。资源准入与执行 exactly-once
围栏是两个独立协议，前者不能写后者的 admission marker。

每个 shard 的请求由冻结的 `ResearchRunSpec.resource_class` 和
`LabShardWorkPlan(work_units, static_duration_ms)` 确定性派生。估算公式为：

```text
memory = min(memory_cap,
             memory_base
             + ceil(work_units / work_step) * memory_per_work_step
             + ceil(static_duration_ms / duration_step) * memory_per_duration_step)
disk   = min(disk_cap,
             disk_base
             + ceil(work_units / work_step) * disk_per_work_step
             + ceil(static_duration_ms / duration_step) * disk_per_duration_step)
```

当前版本的固定映射如下；这些值是准入上界，不是实际用量预测，后续只能通过版本化变更和实测
校准调整：

| class | memory base/cap | memory +work/+duration | disk base/cap | disk +work/+duration | step(work/time) | preemptible |
|---|---:|---:|---:|---:|---:|---|
| interactive | 256 MiB / 1.5 GiB | 64 / 32 MiB | 128 MiB / 4 GiB | 32 / 64 MiB | 500 / 15 min | yes |
| standard | 512 MiB / 4 GiB | 64 / 64 MiB | 256 MiB / 16 GiB | 64 / 256 MiB | 1,000 / 60 min | yes |
| heavy | 1 GiB / 8 GiB | 128 / 128 MiB | 512 MiB / 64 GiB | 256 MiB / 1 GiB | 5,000 / 6 h | no |

现有 adapter 只读取不可变本地 snapshot，因此 `expected_quota_units=0`、`source=None`。
未来若某个研究 adapter 需要调用行情源，必须新增版本化请求映射并由显式 quota lease provider
提供 `SourceQuotaLease`，不能暗中改变现有回放口径。兼容模式可以不注入资源 provider；一旦
以 isolated research runtime 启动，snapshot 和 policy provider 缺失或读取失败都必须 fail closed，
且 claim 保持可重试。

### 14.3 当前服务器的现实边界

同机隔离后仍存在以下共同风险：

- 磁盘写满。
- 内核 OOM 或主机重启。
- 同一公网出口和 Tushare 账号额度。
- 大量 Parquet 扫描造成 page cache 和 I/O 抖动。

因此全量回补和大参数搜索最终应迁到 Mac 或第二台 research worker；云端只保留轻量任务和最后验收。

## 15. 还需要补上的横向能力

这些能力不属于某一个策略，但会决定所有策略结论是否可信。

### 15.1 数据修订和迟到处理

- 同一 `ts_code + event_time` 的新值不能悄悄覆盖旧值。
- 保存 revision、来源、首次可见时间和替换原因。
- 已发布研究快照不随数据修订自动改变；需要显式生成新 snapshot 并比较影响。

### 15.2 慢变参考数据独立发布

交易日历、证券名称/ST/停牌状态、上市退市、板块归属、复权因子和涨跌停制度不能散落在各策略里临时查询。它们应作为 `reference_slow` 数据集独立版本化：

- 每条记录带有效起止时间和 `available_at`。
- 历史回放按当时可见版本做 as-of join。
- 公司行为修订产生新代际，不反向修改已封存研究快照。
- 策略只引用统一的证券状态和价格口径，避免各自处理 ST、除权和停牌。

### 15.3 Schema 演进

- 每个数据集和事件都有 schema version。
- producer 和 consumer 声明兼容范围。
- 新字段先 optional，完成双读验证后再变 required。
- migration 与应用发布分开验收，不能让页面启动时顺手迁生产库。

### 15.4 特征与策略注册表

- `FeatureContract` 记录定义、输入、窗口、复权口径、PIT 规则和版本。
- `StrategySpec` 记录 required features、状态机、参数和退出规则。
- `Experiment` 记录数据快照、代码、参数、样本切分、费用模型和结果。
- `Promotion` 记录研究候选进入 shadow/monitor 的审批和证据。

这会避免“策略名字相同，但实际口径已经变化”的混算。

### 15.5 研究选择偏差治理

- 自动优化只在训练和内层验证选择参数。
- 外层测试与前瞻样本不参与挑选。
- 同时展示样本量、收益分布、最大回撤、费用后收益和置信区间。
- 记录一共尝试过多少组参数，并对多重比较做惩罚。
- 失败实验也保留，避免只看到幸存结果。

### 15.6 冷热分层和保留策略

- 热数据：最近 5-20 个交易日 live/current，云端快速访问。
- 温数据：研究窗口所需的 Parquet 分区，按策略 snapshot 引用。
- 冷数据：旧 raw、旧候选和历史实验压缩归档。
- 任何删除先查引用计数；被 snapshot、实验或审计引用的文件不能删除。

### 15.7 灾难恢复

- 分别备份 production、state、research catalog、lake manifest 和 artifact metadata。
- 不能只备份 DuckDB，而遗漏 SQLite 状态和 current 指针。
- 每月做一次实际恢复演练，验证 hash、行数、最大日期和策略回放。
- 定义初始目标：实时状态最多丢一个采集批次，研究产物可由 manifest 重建。

### 15.8 配置与密钥

- source token 只由 gateway 使用；策略进程不持有不需要的密钥。
- 策略参数使用冻结配置包，不依赖运行时随意修改 `.env`。
- 每次运行记录配置 fingerprint，但不记录 secret 值。
- 生产代码发布、数据发布和策略晋级是三种不同动作，使用不同审计记录。

## 16. 可观测性与初始 SLO

建议所有单元统一暴露以下状态：

- `last_event_time`、`last_received_at`、`last_published_at`。
- 当前 sequence、水位、backlog 和最老未处理事件年龄。
- 每批行数、耗时、重试和质量状态。
- CPU、RSS、I/O、磁盘剩余和数据源调用量。
- 当前版本、schema、feature contract、strategy spec。

初始 SLO 作为测量目标，不作为收益承诺：

| 指标 | 初始目标 |
|---|---|
| 盘中分钟批次发布延迟 | p95 小于 10 秒，按真实数据源能力校准 |
| 已发布行情到策略信号 | p95 小于 5 秒 |
| 信号到通知首次尝试 | p95 小于 5 秒 |
| 页面 serving 数据延迟 | 实时页不超过 1 分钟，健康页不超过 5 分钟 |
| 信号重复投递 | 同 recipient/channel 的成功投递为 0 重复 |
| 研究任务 ETA | 运行 3 个 shard 后给出区间估计并持续更新 |
| 研究资源影响 | live 延迟和 miss 率不能因 batch 任务显著恶化 |

## 17. 分阶段迁移方案

迁移必须渐进，不做一次性大重构。

### Phase 0：关闭当前 Stage 1

- 完成 N 字和科创/创业放量的独立 snapshot、审计与固定回放。
- 保持当前生产 SHA 与数据修复纪律。
- 不在这一步修改 live 服务拓扑。

### Phase 1：建立契约和任务控制面

- 新增 `BatchEnvelope`、`SignalEnvelope`、`ResearchRunSpec` Pydantic 模型。
- 建立 `lab_jobs.sqlite3`、租约、heartbeat、checkpoint 和 ETA。
- Strategy Lab 改为只提交任务。
- 先解决页面卡死、切 tab 丢结果和长任务不可恢复。

### Phase 2：统一盘中数据源网关

- 把 `surge-watch` 的全市场分钟抓取提取为独立 source gateway。
- 发布 current 快照和不可变 spool；保留旧路径影子运行比较 5-10 个交易日。
- `surge-watch` 先切读新 feed，再让 `monitor` 的分钟特征切换。
- 5 秒 watchlist quote 保持独立 channel，不强行降为一分钟。

### Phase 3：拆分特征、策略和通知

- 建立共享 PIT feature batch。
- 策略 runner 先以 shadow 模式读取新特征，与旧 monitor 输出逐事件比对。
- 上线 signal-router 与持久通知 outbox。
- 一致性达标后，移除策略进程里的直接 notify。

#### Legacy Shadow 生产导出契约

旧 `monitor`、旧 `surge-watch` 与 isolated runner 只向
`data/legacy-shadow/{monitor,surge,isolated-runners}` 发布不可变比较证据，不得写 serving、
shadow report authority 或 DuckDB 主库。新批次只能在交易日 15:00–15:05（Asia/Shanghai）
发布；`captured_at` 和 completion `produced_at` 保留真实 UTC 时刻，`as_of`/`complete_through`
绑定 15:00 session close。窗口外只能验收并恢复窗口内已经完整落盘的 `.staging-*` 或已
rename 未 seal 批次，不能重新采集或生成证据。production wrapper 不接收调用方时间；真实
wall clock、Linux boot id 与 monotonic clock 由内部 production factory 读取。窗口内 staging
携带签名 recovery marker，绑定上述时钟、batch digest、source/date/commit/version；
`legacy-shadow-recover` 只有 openat 验证和 promote 能力，没有原始 rows/events 输入。

recovery namespace 只能走 root helper 的双阶段协议：`capture-recovery` 在真正开始物化时读取
`CLOCK_REALTIME`、`CLOCK_BOOTTIME` 和 `/proc/sys/kernel/random/boot_id` 并签发 capture token；
`sign-recovery` 在所有 payload 已 fsync 后重新读取同一组可信时钟，核对 token、canonical draft、
交易日与受保护 SSE calendar，再注入真实 `produced_at` 并签 marker。开始或完成任一时刻不在
15:00–15:05 都拒绝签发；generic signer 禁止 recovery namespace，调用方提交的时间字段也拒绝。
`rquant-shadow-report-signer --validate-key-material` 必须同时验私钥 manifest 与 root-owned 0600、
content-addressed recovery calendar。该 helper 属于 deploy 基础设施，安装后仍需单独 cloud preflight；
本阶段不通过修改 systemd unit 绕过这道 gate。

`surge-watch` 的 completion 还要求 09:30 至 15:00 成功 snapshot 覆盖：记录首末成功、活跃
交易时段最大间隔、连续 miss、route 与全市场覆盖。空 DataFrame、`None`、provider 异常、非
`tushare_rt` route、未知代码或低于 stock-basic universe 的 98%/4000 只保守门槛都记为采集失败；
proof 至少包含一条非空全市场成功响应。缺开盘或收盘覆盖、超过一个连续 miss、超过 125 秒
活跃间隔、收盘仍有未恢复 miss 都只产生 degraded。`monitor` 使用 DuckDB `fetchmany` 游标分页
写入有界独立 spool，退出 DuckDB context 后才签名和发布，禁止全表 `fetchdf` 物化。

isolated fan-in 从同一份 production profile 精确解析 `n_shape` 与 `growth_board_surge` 两份
`STRATEGY_LIVE` manifest，不信任 router 的重复配置。导出 marker/manifest 与 consumer settings
同时绑定 producer manifest fingerprint、commit、service/instance/version、strategy
registration/spec、evaluator 和 executable；runner version 可以且应独立于 Shadow report service
version。任一字段不一致都视为 unavailable/degraded，不进入策略 mismatch 统计。

该目录的部署前提固定为**同机本地 POSIX 文件系统**：building、staging 和 session 必须位于
同一 mount，依赖 parent `dir_fd`、`O_NOFOLLOW`、regular-file `fstat`、目录项复核、`fsync`
和同文件系统原子 `renameat`。Linux production 对 mount type fail closed，只允许明确列出的
本地文件系统；NFS、SMB、FUSE、overlay 和 unknown mount 一律拒绝。macOS 仅允许显式
`test-only-local-posix` 测试依赖，该 override 不能进入 production factory。部署 preflight 必须把
`legacy-shadow-filesystem/v1` 视为硬契约。文件 mode 只用于减少误操作，0444 不是安全边界；
不可变性来自签名 marker、digest、受保护 owner/目录和完整 openat/fstat/fsync/renameat 链。reader
只接受完整且签名验收通过、receipt/commit/date 全部绑定的 batch；缺失、迟到、部分、篡改或
外部绑定失败统一 degraded，不进入 mismatch 统计。

### Phase 4：独立模拟盘和 serving

- paper-broker 消费 signal bus，执行 T+1、涨跌停、费用和滑点模型。
- serving publisher 产出页面查询模型。
- Dashboard/Panorama/Lab 不再跨多个权威库即时大 join。

### Phase 5：研究 worker 和物理隔离

- 研究任务按 immutable snapshot 分发。
- Mac worker 先接大回补和参数搜索；云端保留轻任务。
- 增加资源 class、数据源额度 lease 和断点上传。
- 在真实遥测证明需要后，再评估第二台云研究机。

### Phase 6：恢复演练和旧路径下线

- 执行 kill、断网、磁盘阈值、通知失败、坏批次和 worker 失联演练。
- 验证 current 回滚、outbox 补发、checkpoint 续跑和研究结果可重现。
- 旧 monitor 内嵌采集/通知路径只在连续 10-20 个交易日影子一致后删除。

## 18. 每阶段验收门

| 阶段 | 必须满足才可继续 |
|---|---|
| Phase 1 | 浏览器关闭后任务继续；ETA、取消、恢复和历史结果可用 |
| Phase 2 | 新旧分钟 feed 行数、时间、OHLCVA 与缺失状态逐日对账 |
| Phase 3 | replay/live 前缀一致；旧/新策略信号差异均有解释 |
| Phase 4 | 每笔模拟成交引用信号与价格证据；账户日终可对账 |
| Phase 5 | 研究满载时 live SLO 不退化；worker 中断可续跑 |
| Phase 6 | 故障演练通过；旧路径有明确回滚版本和删除清单 |

## 19. 与现有路线图的关系

本设计不替代 `2026-07-22-rquant-trustworthy-research-platform-implementation.md` 的策略可信度路线，而是补足其运行时隔离：

- v0.27 PIT 特征引擎应同时产出 live/replay 统一特征批次。
- v0.28 StrategySpec 和状态机应落在独立 strategy runner 中。
- v0.29 walk-forward 和统计惩罚由 research worker 执行。
- v0.30 前瞻模拟盘由独立 paper-broker 消费信号。
- v0.31 Strategy Lab 通过 job center 提交后台任务。

两条路线共同目标是：既不让“未来数据”进入当时决策，也不让“研究计算”影响当时的真实监控。

## 20. 近期最值得先做的三件事

在当前 Stage 1 完成后，按以下顺序实施：

1. **持久 Lab Job Center**：直接解决页面卡死、运行结果丢失、无 ETA 和长任务不可恢复。
2. **统一 `market_minute` source gateway**：消除 `monitor` 与 `surge-watch` 的重叠取数和时钟差异。
3. **SignalEnvelope + notifier outbox**：把信号正确性与 Push 稳定性分开，结构性解决重复通知。

这三项完成后，rQuant 才具备低风险继续扩展新策略的运行基础。
