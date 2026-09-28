# 公式池逐日协调器（本地受信入口）

协调器先核验完整定义目录，再为目标交易日准入任务、核验成功任务并发布逐日结果。默认逐页处理最多 64 个；`--all` 在同一次调用内按稳定页遍历完整目录，最多 512 个，最终再次核验目录及两份来源。共享任务由现有 `rquant.formula_market_worker_entry` 单独执行。等待运行的池需要在 worker 完成后再次调用协调器；命令不会自行轮询或启动 worker。

配置必须是当前用户拥有、权限 `0600` 的规范 JSON 文件。所有路径均写绝对路径，目录由当前用户持有且权限 `0700`，不要指向生产目录。格式如下，其中各路径需要分别指向实际的本地合成或授权数据：

```json
{"market":{"universe_root":"/absolute/private/universe","projection_root":"/absolute/private/history","state_path":"/absolute/private/tasks/jobs.sqlite","artifact_directory":"/absolute/private/artifacts"},"definition_root":"/absolute/private/formula-definitions","rule_pool_root":"/absolute/private/rule-pools","daily_result_root":"/absolute/private/daily-results"}
```

在仓库根目录执行，日期必须已过北京时间当日 17:00 且两份来源均已完成：

```bash
PYTHONPATH=src python -m rquant.formula_pool_batch --config /absolute/private/batch.json --trade-date 2026-04-16 --limit 32
PYTHONPATH=src python -m rquant.formula_pool_batch --config /absolute/private/batch.json --trade-date 2026-04-16 --all
PYTHONPATH=src python -m rquant.formula_market_worker_entry --config /absolute/private/market.json
```

逐页命令输出本页状态与精确计数。`next_cursor` 非空时，将其作为下一次 `--cursor` 参数继续；游标绑定目录版本及目标日两份来源，来源或目录变化后须从第一页重启。分页结果的 `unprocessed_count` 是**本次调用未覆盖**的池数，包含前页和后页；其他状态计数也仅覆盖本页。

`--all` 输出整个目录的 `total_count`、`completed_count`、`waiting_count`、`failed_count`、各池状态，以及目录、名单、行情来源身份。空目录在来源可信时 `all_complete=true`。非空目录只有每只池均已重读核验对应版本、交易日与双来源的封存结果，`all_complete` 才为 `true`；零命中可以完成。`waiting` 表示排队、运行中或共享任务占用；`failed` 带安全错误类别，不能按成功处理。逐日结果的具体成员仍保存在私有结果目录，命令只返回摘要和证据摘要。跨页目录或来源变化会使整次命令失败，不输出部分结果。

命令返回码：`0` 表示成功输出可信状态（包括等待或失败的池，此时需读取 `all_complete`）；`1` 表示目录、来源或处理过程不可用，且不会输出 JSON；`2` 表示配置、日期或 `--all` 与分页参数混用等输入错误。`--all` 不接受 `--limit` 或 `--cursor`。一次命令只处理指定的一天，不跨日追赶；定时调用方应显式提供交易日。

此入口没有定时器或线上默认配置；接入生产调度、目录权限与容量验收须另行处理。
