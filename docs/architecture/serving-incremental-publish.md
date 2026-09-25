# serving 增量发布：测量与后续设计

2026-09-25，`cc/20260925-intraday-incremental`。这份文档回答两件事：周一（2026-09-28）之前对
`serving.publisher.v1` 做了什么，以及之后要把它改成「每轮只做变了的部分」该怎么做。

## 1. 测量

- **主机 09-24 全天回放**（v0.33.22，每分钟一步，共 350 步）：合计 4,807 s，平均每步 13.7 s，最长 15.9 s。
  生产上 serving 每轮做完睡 30 s，所以它一直占着约 13.7 / (13.7 + 30) ≈ 0.31 个核。
- **本机主机规模夹具**（`/Users/roxor/.rquant-perf-scratch/fixture-big`：5,470 只竞价代码，与主机当天的 5,475 只相当；
  9:15–10:05 共 50 步，cProfile）：118.6 s，每步 2.4 s（本机比主机快约 5 倍）。时间分布：

| 环节 | 占比 | 说明 |
|---|---|---|
| `ServingSnapshotAssembler.assemble` | 73% | 六个来源权威每轮都重新读、重新解析、重新校验 |
| 其中：解析来源文档（`_parse_document` → `model_validate_json`） | 24% | 参考慢源的全市场投影每轮完整解析一次 |
| 其中：投影逐行校验（`validate_projection`，一轮约 34 万次 `_projection_json_bytes`） | 过半 | 同一批投影在一轮里被校验约四次：解析文档、`ServingProjectionInput.bind`、构造 `ServingReadModelInput`、step 里的 `ServingRuntimeSnapshot.model_validate` |
| 其中：内容身份校验（`validate_content_identity`，`canonical_sha256`） | 9% | |
| `publish_generation`（建 DuckDB、校验、哈希） | 6% | 35 次真正建了新代 |
| `build_serving_read_models` | 2% | |

结论：贵的不是「发布」，而是每轮把从早到晚都不变的参考慢源（全市场）重新读和校验若干遍。

## 2. 为什么「来源没变就不发布」省不下 CPU

发布器已经有这道闸（#271 的 `_generation_already_current`）：六个来源的 generation 与水位都没变时，不建库、不切指针。
但这道闸在 `assemble` 与 `build_serving_read_models` **之后**，要判断「变没变」就得先把六份文档全部解析完。而且盘中
`signals`、`paper_accounts`、`runtime_health` 基本每分钟都会变，夹具全天 350 步里切了 244 代。所以只加一道「没变就不发」
不会让 CPU 降多少。

## 3. 周一的缓解（本分支已做）

`runtime_production_profile.SERVING_PUBLISHER_INTERVAL_SECONDS` 从 30 改成 60。这是生成清单里的一个数，装机时随新
profile 生效，不改任何读写逻辑：

- CPU：主机上约 13.7 / (13.7 + 60) ≈ 0.19 个核（原来 0.31）。
- ③b「当日信号进 serving」的延迟上限：通知器把信号写进自己的 serving 权威之后，最多再过「一次等待 + 一步」，也就是
  60 s + 约 16 s ≈ 76 s（原来 30 s + 16 s ≈ 46 s）。上游 strategy → router → notifier 各 2 s 一轮，另加几秒。
- 心跳：一轮约 76 s，仍在 `stale_after_seconds`（120 s）之内；看板的 serving 过期阈值是 10 分钟。

## 4. 之后的增量发布设计

按改动由小到大排，每一步都要能证明「发布出来的表逐行相同」：

1. **来源文档按内容哈希记住解析结果**（`runtime_serving_authority`）。每轮照旧读 `current.json`、读代文件并算 sha256（这一步
   便宜），只有 sha256 与上一轮不同才重新解析与校验；相同就直接用上一轮的 `SourceReadResult`。这与本分支给分钟 spool
   与约束发布器做的记忆是同一个原则：只跳过「对完全相同的字节再算一遍」。预计省掉约三分之一。
2. **投影只校验一次**（`serving_read_models`，需与该模块的维护者协调，本分支没动它）。按
   `(dataset_id, generation_id)` 记住已绑定、已校验的 `ServingProjectionInput`；组装 `ServingReadModelInput` 与
   `ServingRuntimeSnapshot` 时不再对已校验的投影重复逐行校验（例如这两个容器对已验证的子对象不再 revalidate）。预计再省一半以上。
3. **按表增量建库**（`serving_publisher`）。每张表记内容哈希；只有 `signals`、`runtime_health` 这类小表变了时，新一代直接从上一代
   的 DuckDB 以只读 ATTACH 复制未变的大表（全市场投影），只重建变了的表，再按表哈希校验。
4. **最前面的闸**：先只读六个 `current.json` 的字节，与上一轮完全相同且没有随观测时刻变化的新鲜度状态时，整轮直接返回
   上一轮的结果。需要先确认 `SourceReadResult.status` 不随 `as_of` 变化。

等价口径：对同一组来源，新旧两条路径发布的每张表逐行相同、manifest 的 `source_generations` 与水位相同
（DuckDB 文件字节本身不要求相同，generation id 因 `content_sha256` 与 `built_at` 不同是预期的）。
