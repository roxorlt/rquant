# 选股按需加载的合成规模证据

2026-09-27，在 macOS arm64、Python 3.12.13、DuckDB 1.5.2、pandas 3.0.2 上运行。该证据对应 `scripts/benchmark_screen_selective.py`，使用 `DuckDBStore` 创建正式 schema；8,000 只股票，日线/指标/日基础数据各 91 个合成交易日，状态和 PIT 股票身份各 500 个合成交易日。合成副本约 172 MiB，所有值重复，压缩率高于真实数据库。

构造夹具和查询分别在独立进程运行；下表只计查询子进程，从调用开始到取得筛选结果的 `time.perf_counter` 耗时。峰值是 Darwin `resource.getrusage(RUSAGE_SELF).ru_maxrss` 的**进程 RSS 字节数**换算为 MiB，包含 Python、pandas 和 DuckDB；没有用 `tracemalloc` 代替。各查询独立进程，未把夹具创建的峰值混入查询。两条路径读取同一个副本，旧路径使用未传 `required_columns` 的 `load_universe`，新路径使用 `VerifiedReplicaScreenSource.load`。

| 工作负载 | 路径 | 列数 | 峰值 RSS | 耗时 |
|---|---|---:|---:|---:|
| T-30 + 前 60 日量比 | 旧完整加载 | 2,735 | 1,010.9 MiB | 2.589 s |
| T-30 + 前 60 日量比 | 按需加载，3 次 | 69 | 275.1–277.8 MiB | 0.208–0.217 s |
| 上述量比 + 500 日曾涨停聚合 | 旧完整加载，2 次 | 2,736 | 1,297.6–1,371.5 MiB | 2.755–2.886 s |
| 上述量比 + 500 日曾涨停聚合 | 按需加载，3 次 | 70 | 284.3–292.1 MiB | 2.494–2.571 s |

两种 500 日聚合路径均给 8,000 只股票完整的已知结果，且聚合命中数一致。领域单测另用**同一副本**逐列、逐规则对照完整与按需结果，覆盖 26 条注册规则、T-30 + 60 日量比、500 日聚合、缺失事实、PIT 状态与重复行。按需路径保持原聚合声明；聚合每批最多 64 只股票，副本查询连接固定单线程。旧 `load_universe` 默认调用保持原样。

此前独立审查的约 691 MB 是另一份 8,000 股合成夹具的旧路径峰值；本表采用更密的正式 schema 夹具，不能把两者当成同一次测量。按需路径在本夹具低于 384 MiB 目标；约 10 GB 的真实只读副本、Linux cgroup `MemoryHigh=384M` / `MemoryMax=640M`、并发请求和系统负载尚未测量，网页接入前必须另行验证，当前证据不代表生产可用。

复现时在装有项目锁定依赖的 Python 环境中，依次执行：

```bash
python scripts/benchmark_screen_selective.py prepare /private/tmp/rquant-screen-bench
python scripts/benchmark_screen_selective.py query /private/tmp/rquant-screen-bench full
python scripts/benchmark_screen_selective.py query /private/tmp/rquant-screen-bench selective
python scripts/benchmark_screen_selective.py query /private/tmp/rquant-screen-bench full-aggregate
python scripts/benchmark_screen_selective.py query /private/tmp/rquant-screen-bench selective-aggregate
```

`prepare` 要求目标目录里没有既存数据库文件；脚本只写给定的合成目录，不读取 `.env` 或生产库。
