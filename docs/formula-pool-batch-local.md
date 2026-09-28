# 公式池逐日协调器（本地受信入口）

一次调用读取全部已保存公式池定义，先完整核验目录，再按池名顺序处理最多 64 个。它只为目标交易日准入任务、核验成功任务并发布逐日结果；共享任务由现有 `rquant.formula_market_worker_entry` 单独执行。等待运行的池需要在 worker 完成后再次调用协调器。

配置必须是当前用户拥有、权限 `0600` 的规范 JSON 文件。所有路径均写绝对路径，目录由当前用户持有且权限 `0700`，不要指向生产目录。格式如下，其中各路径需要分别指向实际的本地合成或授权数据：

```json
{"market":{"universe_root":"/absolute/private/universe","projection_root":"/absolute/private/history","state_path":"/absolute/private/tasks/jobs.sqlite","artifact_directory":"/absolute/private/artifacts"},"definition_root":"/absolute/private/formula-definitions","rule_pool_root":"/absolute/private/rule-pools","daily_result_root":"/absolute/private/daily-results"}
```

在仓库根目录执行，日期必须已过北京时间当日 17:00 且两份来源均已完成：

```bash
PYTHONPATH=src python -m rquant.formula_pool_batch --config /absolute/private/batch.json --trade-date 2026-04-16 --limit 32
PYTHONPATH=src python -m rquant.formula_market_worker_entry --config /absolute/private/market.json
```

第一条命令输出带交易日、目录身份、各池状态与精确计数的 JSON。`next_cursor` 非空时，将其作为下一次 `--cursor` 参数继续当前页之后的池；游标绑定目录版本及目标日两份来源，来源或目录变化后须从第一页重启。分页结果的 `unprocessed_count` 是**本次调用未覆盖**的池数，包含前页和后页；其他状态计数也仅覆盖本页。分页走完之后，应从第一页重新逐页核对全部池，或用同一目录及来源身份聚合每页证据；不能只看最后一页宣称全日完成。`all_complete` 仅可能在单次覆盖全部池、且每池逐日结果均已重读核验时为 `true`。池状态 `waiting` 表示排队、运行中或共享任务占用；`failed` 带安全错误类别，不能按成功处理。逐日结果的具体成员仍保存在私有结果目录，批次 JSON 只返回摘要和证据摘要。

此入口没有定时器或线上默认配置；接入生产调度、目录权限与容量验收须另行处理。
