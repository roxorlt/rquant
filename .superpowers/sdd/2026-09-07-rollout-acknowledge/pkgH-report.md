# 包 H 报告：schema rollout 的 A（16 个 unit 写权限）+ 受限 B（安装器代做 PREPARE 承认）

- **worktree**：`/Users/roxor/brain/30-projects/rQuant/.worktrees/ra-h-cc`
- **分支**：`cc/20260907-rollout-acknowledge`，base = 包 G 的 `d4257b5`，**已 merge 包 G 最新 HEAD `e3b0aaf`**
- **HEAD**：本报告自己那条 `docs:` commit（**未 push**）；它之前的 5 条 SHA 见下
- **改动面**：16 个 unit 文件 + 3 个源文件 + 6 个测试文件 + `CHANGELOG.md` + `DEPLOY.md`
  （`git diff --stat d4257b5..HEAD`：27 files changed，unit 16 行改动 + 其余 1821 插入 / 21 删除）

```
<本报告>   docs: record the rollout grant and the installer acknowledgement (#227)
0943c70    fix(runtime): refuse a raised installer ceiling in the preview too (#227)
f71497b    merge: take package G's loop-side rollout writes and changelog (#227)
fb464f5    test(runtime): acknowledge sixteen real plans end to end, and record the step
cfba46a    feat(runtime): let the installer carry a prepared rollout to DUAL_WRITE (#227)
0d727c4    deploy(systemd): let a schema rollout participant write the rollout root (#227)
```

---

## 1. A：16 个 unit 的 `ReadWritePaths`

### 1.1 改了什么

每个 unit 的**唯一那条** `ReadWritePaths=` 行末尾追加一项，其余一字不动：

```
 -/home/lighthouse/rquant/data/runtime/control/schema-rollouts
```

`-` 前缀是 #192 要求的：首装时这个目录还不存在，无前缀的缺失路径会在挂载命名空间阶段
`226/NAMESPACE`，在 wrapper 的第一条指令之前。

**必须追加到同一行，不能新开一行**：`tests/unit/test_runtime_systemd_services.py` 用
`configparser(strict=True)` 读 unit，重复 key 会直接抛 `DuplicateOptionError`；
`test_runtime_systemd_read_write_paths.py` 也断言 `len(declarations) == 1`。

### 1.2 diff（16 个文件，各 1 行；`$R` = `/home/lighthouse/rquant/data/runtime`）

| unit | 改后的 `ReadWritePaths=` |
|---|---|
| `rquant-runtime-auction-match@.service` | `$R/control/auction-match-sources/%i $R/live/auction-match `**`-$R/control/schema-rollouts`** |
| `rquant-runtime-auction-universe@.service` | `-$R/control/auction-universe-publishers/%i -$R/authorities/auction-universe `**`-$R/control/schema-rollouts`** |
| `rquant-runtime-candidate@.service` | `-$R/control/candidates/%i -$R/live/candidates/%i `**`-$R/control/schema-rollouts`** |
| `rquant-runtime-feature@.service` | `-$R/control/features/%i -$R/live/features `**`-$R/control/schema-rollouts`** |
| `rquant-runtime-lab-jobs@.service` | `-$R/control/lab-jobs-publishers/%i -$R/research/serving-authorities/lab-jobs `**`-$R/control/schema-rollouts`** |
| `rquant-runtime-market-minute@.service` | `$R/control/market-minute-sources/%i $R/live/market-minute `**`-$R/control/schema-rollouts`** |
| `rquant-runtime-notifier@.service` | `$R/control/notifiers/%i $R/live/notifications/%i `**`-$R/control/schema-rollouts`** |
| `rquant-runtime-paper-broker@.service` | `-$R/control/paper-brokers/%i -$R/live/paper-brokers/%i `**`-$R/control/schema-rollouts`** |
| `rquant-runtime-paper-constraint@.service` | `-$R/control/paper-constraints/%i -$R/authorities/paper-execution `**`-$R/control/schema-rollouts`** |
| `rquant-runtime-promotions@.service` | `-$R/control/promotions-publishers/%i -$R/research/serving-authorities/promotions `**`-$R/control/schema-rollouts`** |
| `rquant-runtime-reference-slow-publisher@.service` | `$R/control/reference-slow-publishers/%i $R/authorities/reference-slow `**`-$R/control/schema-rollouts`** |
| `rquant-runtime-reference-slow-source@.service` | `$R/control/reference-slow-sources/%i $R/live/reference-slow `**`-$R/control/schema-rollouts`** |
| `rquant-runtime-runtime-health@.service` | `-$R/control/runtime-health-publishers/%i -$R/control/authority-runtime-health `**`-$R/control/schema-rollouts`** |
| `rquant-runtime-serving@.service` | `-$R/control/serving-publishers/%i -$R/serving `**`-$R/control/schema-rollouts`** |
| `rquant-runtime-signal-router@.service` | `-$R/control/signal-routers/%i -$R/live/signal-bus `**`-$R/control/schema-rollouts`** |
| `rquant-runtime-strategy@.service` | `-$R/control/strategies/%i -$R/live/strategies/%i `**`-$R/control/schema-rollouts`** |

`git diff --stat`：**16 files changed, 16 insertions(+), 16 deletions(-)**。
`ExecStart` / `Slice` / `User` / `ReadOnlyPaths` / `InaccessiblePaths` **一行未动**（由
`test_exec_start_and_slice_are_untouched` 与新模块的
`test_the_participant_still_runs_the_role_this_file_says_it_does` 钉住）。

**没改的 7 个**：`artifact-catalog`、`daily-close`、`daily-orchestrator`、`recovery`、
`recovery-rehearsal`、`shadow`、`watchlist-quote`。16 + 7 = 23，与 `rquant-runtime-*@.service`
的总数对上。

### 1.3 `rquant-runtime-serving@` 是嵌套那一例

它的 `ReadOnlyPaths` 含整个 `-$R/control`。`systemd.exec(5)` 原话：
「Nest `ReadWritePaths=` inside of `ReadOnlyPaths=` in order to provide writable
subdirectories within read-only directories.」所以嵌套授权成立，且这一条被
`test_the_serving_unit_is_the_nested_case_and_still_holds_control_read_only` 单独钉住。
反方向（`ReadOnlyPaths=` / `InaccessiblePaths=` 落在 rollout 根之上或之下）被
`test_the_rollout_root_is_never_taken_back_by_another_directive` 拒绝。

### 1.4 为什么是整个目录而不是 per-plan

`plan_id` 是计划的内容哈希，每一代都变，静态 unit 文件追不上；写死一代的 `plan_id` 会在下一代
静默变成「授权了一个不存在的路径」，也就是 #227 再来一次、而且没有错误信息。per-plan 粒度只能靠
安装器生成 drop-in，那是另一次授权。`test_the_grant_is_the_whole_rollout_root_and_never_one_plan`
禁止任何 unit 出现 `…/schema-rollouts/<某个子路径>`。

### 1.5 哪 16 个不是抄来的

`tests/unit/test_runtime_systemd_schema_rollout_paths.py` 里的 `PARTICIPANTS` 是钉在 unit 文件上的
清单；**它本身由端到端从真实装机推导出来的集合校验**
（`test_the_granted_units_are_exactly_the_participants_of_the_prepared_plans`）：真装两代 bundle →
读回 16 份计划 → 取每份计划的 `plan.producers` 与 `registry.consumers` → 按 manifest 的
`service_kind` 映射到 unit 模板 → 断言恰好等于那 16 个。

推导出来的原始名单（我自己实测，不是转述审查）：

- **生产者参与方 19 个 service id → 15 个 unit 模板**：`auction-match.source.v1`、
  `auction-universe.publisher.v1`、`candidate.{auction_gap,growth_board_surge,n_shape}.v1`、
  `feature.intraday-pit.v1`、`lab-jobs.serving.v1`、`market-minute.source.v1`、
  `notifier.admin.shadow.v1`、`paper-broker.shadow-main.v1`、`paper-constraint.market.v1`、
  `promotions.serving.v1`、`reference-slow.publisher.v1`、`reference-slow.source.v1`、
  `runtime-health.all.v1`、`signal-router.all-strategies.v1`、
  `strategy.{auction_gap,growth_board_surge,n_shape}.v1`。
- **消费者 14 个 service id**，其中 13 个已在生产者集合里，多出来的只有
  `serving.publisher.v1` → `rquant-runtime-serving@`。
  **注意消费者不止「要 serving generation 回执」那 5 个**：`load_runtime_schema_service_bindings`
  在 `CONSUMER_ACK` 阶段对**每一个**消费者都写一条能力回执（需要 serving generation 的走 binding，
  其余当场 `acknowledge_consumer`），所以判据是「计划的全部消费者」，不是「requires_serving_generation_ack 的消费者」。
  结论上两种算法给出同一个 16，但判据要写对。

### 1.6 云端还要做的（协调者）

mac 上没有 systemd，语义测不出。**push / 部署前必须在 82.156.0.68 上对这 16 个 unit 跑
`systemd-analyze verify`**，并起一个参与方 role 实测沙箱真的放开了。本包只做了本地改动与本地测试。

---

## 2. B：`rquant runtime-schema-rollout acknowledge`

### 2.1 用法

```bash
# 预览：一个字节都不写（只读打开），可以在只读的 rollout 根上跑
rquant runtime-schema-rollout acknowledge --runtime-root <运行根> --dry-run

# 执行
rquant runtime-schema-rollout acknowledge --runtime-root <运行根>
```

命令与 `runtime-deployment-profile` / `runtime-production-prerequisites` /
`runtime-production-profile` 一样进了 `CONFIGURATION_FREE_COMMANDS`：它跑在同一个窗口、同一份
无 `.env` 的 bootstrap worktree（`${WT}`），而**已部署的 checkout 还没有这条命令**，没有别的
worktree 可退。`tests/unit/test_cli_configuration_free_dispatch.py` 的四条守卫同时覆盖了它
（自己的 parser 可达、handler 与其 import 链不碰 `rquant.config`、免配置集合是闭集）。

输出（`sort_keys` 的 JSON）：

```json
{
  "status": "applied",            // 或 "dry_run"
  "plans": 16,
  "changed": 16,
  "acknowledgements": [
    {
      "plan_id": "…",
      "dataset_id": "runtime.strategy_candidate.snapshot",
      "target_generation_id": "…",
      "journal_mode_before": "wal",      // 生产上现在就是 wal
      "journal_mode_after": "rollback",
      "phase_before": "prepare",
      "phase_after": "dual_write",
      "acknowledged_producers": ["candidate.auction_gap.v1", "…"],
      "already_acknowledged_producers": [],
      "advanced": true,
      "skipped_reason": null
    }
  ]
}
```

### 2.2 它做什么、不做什么

| | 行为 |
|---|---|
| 选计划 | 遍历 `control/schema-rollouts`，只处理 `authority.target_generation_id` 等于 `<运行根>/current` 所指代的那些；其余报 `plan does not target the current generation`，**连 journal 布局都不碰**（在打开库之前就判掉） |
| 只在 PREPARE 上动手 | 阶段不是 PREPARE 就报 `plan is past PREPARE (phase …)`，不写。DUAL_WRITE 之后的计划等的是生产者的双写记录、消费者的回执，安装器一概不碰 |
| 承认 | 对计划里每个还没记过 PREPARE 承认的生产者调一次 `store.acknowledge`，`operation_id` 是 `installer-prepare:<plan>:<participant>`；已记过的进 `already_acknowledged_producers` |
| 推进 | 全部生产者齐了之后 `store.advance` 到 `SCHEMA_ROLLOUT_INSTALLER_PHASE_CEILING`，`operation_id` 是 `installer-dual-write:<plan>` |
| **天花板** | `SCHEMA_ROLLOUT_INSTALLER_PHASE_CEILING = RolloutPhase.DUAL_WRITE`。`_require_installer_phase_ceiling()` 在**预览与执行两条路径的最前面**都跑一次，常量一旦不是 DUAL_WRITE 就抛错停下，一份计划都不动 |
| 状态库转换 | apply 的写者打开顺带把旧版留下的 WAL 库转成回滚日志（`journal_mode` 记在库头），逐份记录转换前后 |
| 幂等 | 再跑一次 `changed` 是 0，库文件字节级不变 |

### 2.3 为什么 PREPARE 承认可以代做，DUAL_WRITE 之后不行

`store.acknowledge` 的每个入参——`participant_fingerprint` 来自 `authority.plan.producers`、
`declaration_fingerprint` 来自 `authority.plan.new_declaration_fingerprint`——**全部来自冻结的
计划本身**，而 `store.acknowledge` 又会拿冻结注册表把它们逐个重验一遍。所以搬到安装器不产生
任何计划里没有的信息。

离开 DUAL_WRITE 不是这样：`_validate_phase_exit` 要求库里真有生产者写下的双写一致性证据；
CUTOVER 要求可信消费者的回执。两者都带真实运行期信息，代签就是放宽。

### 2.4 dry-run 为什么不转换 WAL

只读打开读不了 WAL 库（包 G 实测：目录 0555、无 sidecar 时 `mode=ro` 抛
`attempt to write a readonly database`）。让 dry-run 顺手转换，就是让「预览」执行它要预览的那个
改动。所以 dry-run 对 WAL 库报 `journal_mode_before: wal` + `phase_before: null` +
`skipped_reason` 说明「要先 apply」，**库仍然是 WAL**（用例断言过）。
**生产上那 16 份现在全是 WAL，所以第一次 dry-run 会是这个形状——预期，不是故障。**

### 2.5 改了哪些源文件

| 文件 | 改动 |
|---|---|
| `src/rquant/schema_compatibility.py` | 新增模块级 `persisted_rollout_journal_layout(path)`（读库头第 18 字节，不会顺手建 `-shm`），`SchemaRolloutStore._persisted_journal_layout` 改为调用它并保留自己的失败关闭措辞；进 `__all__`。**行为一字未变**，是把包 G 已有的探针提成公开函数，避免安装器再抄一份 |
| `src/rquant/runtime_deployment_bundle.py` | 新增 `SCHEMA_ROLLOUT_INSTALLER_PHASE_CEILING`、`RuntimeSchemaRolloutAcknowledgement`（Pydantic）、`_require_installer_phase_ceiling`、`_rollout_acknowledgement_preview`、`_apply_rollout_acknowledgement`、`acknowledge_runtime_schema_rollout_preparation`；四项进 `__all__` |
| `src/rquant/cli.py` | 新增 `cmd_runtime_schema_rollout` 与 `runtime-schema-rollout acknowledge` 子命令；加入 `CONFIGURATION_FREE_COMMANDS` |

**没动**：`prepare_runtime_schema_rollout` / `advance_runtime_schema_rollout` /
`rollback_runtime_schema_rollout` / `load_runtime_schema_service_bindings` /
`runtime_schema_registry.py` / `runtime_deployment_profile.py`；`.env`、发布原语、stage、
`runtime_authority*`、wrapper 一律未碰。

---

## 3. 端到端证据

### 3.1 新增 e2e：`tests/integration/test_route_a_rollout_acknowledge_e2e.py`（13 例）

复用包 G 的 `rollout` fixture（**真装两代 bundle**、真 stage + publish、`control/schema-rollouts`
每个目录摘掉写位），加上本包要证的那一半：

| 用例 | 证明什么 |
|---|---|
| `test_the_granted_units_are_exactly_the_participants_of_the_prepared_plans` | 16 份计划的参与方推导出来的 unit 集合 == unit 文件钉住的 16 个 |
| `test_the_preview_lists_all_sixteen_plans_without_any_write_at_all` | **在封住的（0555）rollout 根上**跑 dry-run：16 份全是 PREPARE，库文件 sha256 一字未变，没有 sidecar，阶段没动 |
| `test_acknowledging_carries_all_sixteen_plans_to_dual_write` | apply 后 16 份全 DUAL_WRITE、全是回滚日志、无 sidecar |
| `test_a_second_acknowledgement_changes_nothing` | 幂等：`changed` 全 False，16 份库字节级不变 |
| `test_the_installer_refuses_to_carry_a_plan_past_dual_write` | **反向**：天花板改成 CUTOVER ⇒ 抛错，16 份仍在 PREPARE |
| `test_a_wal_store_left_by_the_installed_build_is_converted_by_the_acknowledgement` | 把一份计划改回 WAL（= 生产现状）：dry-run 报告但不转换；apply 报 `wal → rollback`、无 sidecar、阶段到 DUAL_WRITE |
| `test_a_producer_of_the_three_producer_plan_no_longer_fails_first[0]` / `[1]` | **验收核心**：`runtime.strategy_candidate.snapshot` 的三生产者计划，第 0、1 个 `candidate_publisher` 实例——承认之前各抛 `schema producer startup is waiting for every producer PREPARE ACK`，承认之后 rc 0、进服务循环一轮 |
| `test_the_second_three_producer_plan_stops_blocking_its_roles_too` | `strategy_live` 同样先抛「等其他生产者」，承认之后**不再是这个原因**（它在本 harness 里另有主机形状问题，用例只断言 rollout 不再是拦路的那一条） |
| `test_a_single_producer_role_still_comes_up_after_the_acknowledgement` | `auction_universe_publisher` 本来就不被挡，承认之后仍然 rc 0（它的计划此时在 DUAL_WRITE，循环带的是 dual-write binding） |
| `test_a_read_only_role_still_comes_up_over_the_acknowledged_root[serving_publisher]` / `[watchlist_quote_source]` | 包 G 的两个只读 role 在「安装器刚写过 16 份库」的根上仍然起得来——转换没有把库留成沙箱读不了的布局 |
| `test_a_dual_write_commit_under_an_unwritable_rollout_root_still_names_the_sandbox` | **验收 4**：承认之后把根封回只读，用真实 manifest 取真实 `RuntimeSchemaDualWriteBinding`、真实 `prepare_payload`，再 `commit_payload` ⇒ 包 G 的措辞，点名 `market-minute.source.v1`、库路径、`ReadWritePaths`、`control/schema-rollouts` |

**macOS 运行记录**（`.venv/bin/python -m pytest`，Python 3.11.15）：`13 passed in 336.70s`。

**Linux 运行记录**（Docker `python:3.11-slim`，非 root `runner` uid 1000）：见 §3.3。

### 3.2 新增单元用例

| 文件 | 例数 | 内容 |
|---|---|---|
| `tests/unit/test_runtime_systemd_schema_rollout_paths.py` | **91**（含参数化） | A 的全部判据：16 + 7 == 磁盘上的 23；16 个各有且仅有一条 `-` 前缀的授权；7 个整份文件不含 `schema-rollouts`；`deploy/systemd/` 里提到 `schema-rollouts` 的文件恰好是那 16 个；每个 unit 的授权 == 「改动前的原样 + 追加项」；不得出现 per-plan 路径；`ReadOnlyPaths`/`InaccessiblePaths` 不得把授权收回；serving 的嵌套情形；每个 unit 仍跑它声称的 role |
| `tests/unit/test_runtime_schema_rollout_acknowledge.py` | **20** | B 的全部判据：真装两代 bundle（本地画像，含两个 candidate publisher + 一个 strategy live，使 `runtime.strategy_candidate.snapshot` 带两个生产者）→ 承认 → DUAL_WRITE；不越界；天花板被抬高时执行与预览**都**拒；幂等；半完成的 PREPARE 轮被补齐；承认进的是计划自己的哈希链；生产者启动不再等；WAL 转换；dry-run 不写不转换；跨代计划跳过；无 `current` 拒绝；空 rollout 根返回空；预览用的是只读句柄；CLI 两条（预览→执行→再执行、逐份报告转换） |
| `tests/unit/test_runtime_systemd_services.py` | 改 | 16 个模板的精确写集合各加一项（其余 7 个原样，`watchlist-quote` / `shadow` / `artifact-catalog` / `daily-close` 保持不变） |
| `tests/unit/test_runtime_systemd_read_write_paths.py` | 改 | 第一关 16 个 unit 中与本次重叠的 11 个，授权元组末尾加一项 |
| `tests/unit/test_cli_configuration_free_dispatch.py` | 改 | 免配置集合从 3 条扩到 4 条，四条守卫同步覆盖新命令 |

### 3.3 Linux（Docker，非 root）

Docker `python:3.11-slim`，非 root `runner`（uid 1000），`uv sync --frozen --python 3.11`
装出 **Python 3.11.16 / SQLite 3.46.1**，`/src` 只读挂载后拷进 `/work`：

```
3.11.16 3.46.1
1000
platform linux -- Python 3.11.16, pytest-9.0.3, pluggy-1.6.0

tests/unit/test_runtime_systemd_schema_rollout_paths.py ................ [  3%]
........................................................................ [ 20%]
...                                                                      [ 21%]
tests/unit/test_runtime_systemd_read_write_paths.py .................... [ 26%]
.............................                                            [ 33%]
tests/unit/test_runtime_systemd_services.py ............................ [ 40%]
........................................................................ [ 57%]
.......................................................                  [ 70%]
tests/unit/test_runtime_schema_rollout_acknowledge.py .................. [ 74%]
..                                                                       [ 75%]
tests/unit/test_cli_configuration_free_dispatch.py .............         [ 78%]
tests/unit/test_schema_compatibility.py ................................ [ 85%]
..............                                                           [ 89%]
tests/unit/test_runtime_schema_registry.py .......................       [ 94%]
tests/integration/test_route_a_rollout_acknowledge_e2e.py .............  [ 97%]
tests/integration/test_route_a_schema_rollout_sandbox_e2e.py ..........  [100%]

======================= 420 passed in 727.78s (0:12:07) ========================
```

**420 passed，0 failed，0 skipped**——uid 1000 下那两处 `os.geteuid() == 0` 的 skip 条件不成立，
13 例新 e2e 与包 G 的 10 例沙箱 e2e 全部实跑。这就是简报要求的「e2e 在 Docker 非 root 下真装两代
bundle」的证据：新 e2e 的 13 例里包含真装两代 bundle、16 份计划、dry-run、apply、幂等、
天花板反向、两个生产者实例承认前后的对照，以及主循环写者在只读根上的措辞。

### 3.4 macOS 回归

| 批次 | 结果 |
|---|---|
| `test_route_a_rollout_acknowledge_e2e.py`（新增 e2e） | **13 passed**（336.70 s） |
| `test_runtime_systemd_schema_rollout_paths.py` + `test_runtime_systemd_read_write_paths.py` + `test_runtime_systemd_services.py` | **295 passed**（20.53 s） |
| `test_runtime_schema_rollout_acknowledge.py` | **20 passed**（20.14 s） |
| `test_runtime_deployment_bundle` + `test_runtime_schema_registry` + `test_runtime_service_main` + `test_tp9_role_child_runtime` + `test_runtime_deployment_profile` + `test_runtime_production_profile` + `test_schema_compatibility` + `test_cli` + `test_runtime_deployment_profile_cli` + `test_cli_configuration_free_dispatch` | **665 passed**（505.31 s） |

`ruff check`：本次改动的 3 个源文件 + 5 个测试文件全过。
`ruff format --check`：我新写的三个文件全过；`test_runtime_systemd_services.py` 与
`test_runtime_systemd_read_write_paths.py` 会被 formatter 重排，**stash 掉我的改动后同样报
reformat**，属存量、不是本次引入，没有顺手格式化以免制造噪声（与包 G 同一处置）。

---

## 4. 变异表（8 条，全部必红）

脚本 `/tmp/ra-h-mut/apply.py`（逐条 apply → 跑 → revert，revert 后 `git status` 干净）。
批次：`SYSTEMD` = 三个 systemd 用例文件（295 例）；`ACK` =
`test_runtime_schema_rollout_acknowledge.py`（20 例）；`SCHEMA` =
`test_schema_compatibility.py` + `test_runtime_schema_registry.py`（69 例）。

### M1 — B 越过 DUAL_WRITE（天花板改成 CUTOVER）

```python
# 原文
SCHEMA_ROLLOUT_INSTALLER_PHASE_CEILING: Final[RolloutPhase] = RolloutPhase.DUAL_WRITE
# 变异为
SCHEMA_ROLLOUT_INSTALLER_PHASE_CEILING: Final[RolloutPhase] = RolloutPhase.CUTOVER
```

- `ACK`：**红**，15 failed / 5 passed。含
  `test_every_prepared_plan_is_carried_to_dual_write`、`test_no_plan_is_ever_carried_past_dual_write`、
  `test_the_dry_run_lists_every_plan_and_writes_nothing`、`test_the_command_previews_then_applies` 等。

### M2 — A 少给一个参与方（`rquant-runtime-feature@` 去掉授权）

```
# 原文
ReadWritePaths=-…/control/features/%i -…/live/features -…/control/schema-rollouts
# 变异为
ReadWritePaths=-…/control/features/%i -…/live/features
```

- `SYSTEMD`：**红**，5 failed / 290 passed：
  `test_a_participant_may_write_the_rollout_root[rquant-runtime-feature@.service]`、
  `test_no_unit_outside_the_sixteen_names_the_rollout_root`、
  `test_the_ruling_appended_one_entry_and_widened_nothing_else[rquant-runtime-feature@.service]`、
  `test_the_prefix_is_the_only_change_to_the_granted_paths[rquant-runtime-feature@.service]`、
  `test_runtime_templates_only_write_their_plane_and_shared_control_root`。

### M3 — A 多给一个非参与方（`rquant-runtime-watchlist-quote@` 加上授权）

```
# 变异为（追加一项）
ReadWritePaths=-…/control/watchlist-quote-sources/%i -…/live/watchlist-quote -…/control/schema-rollouts
```

- `SYSTEMD`：**红**，5 failed / 290 passed：
  `test_a_bystander_is_given_no_access_to_the_rollout_root_at_all[rquant-runtime-watchlist-quote@.service]`、
  `test_no_unit_outside_the_sixteen_names_the_rollout_root`、
  `test_the_prefix_is_the_only_change_to_the_granted_paths[…]`、
  `test_watchlist_quote_has_an_independent_least_privilege_live_unit`、
  `test_runtime_templates_only_write_their_plane_and_shared_control_root`。

### M4 — 转换不改 journal 模式（写者改回 WAL）

```python
# 原文
        connection.execute("PRAGMA journal_mode = DELETE")
# 变异为
        connection.execute("PRAGMA journal_mode = WAL")
```

- `ACK` + `SCHEMA`：**红**，25 failed / 64 passed。本包侧 15 条全红（含
  `test_applying_converts_a_store_an_older_build_left_in_wal`、
  `test_an_untouched_store_reports_the_same_layout_before_and_after`），
  包 G 侧另有 10 条红（`test_the_writer_leaves_the_rollout_store_in_rollback_journal_mode`、
  `test_a_store_left_in_wal_by_an_older_build_converts_on_the_next_writer_open` 等）。

### M5 — 预览改成写者打开

```python
# 原文
    _authority, store = load_runtime_schema_rollout(root, plan_id=plan_id, read_only=True)
# 变异为
    _authority, store = load_runtime_schema_rollout(root, plan_id=plan_id, read_only=False)
```

- `ACK`：**红**，1 failed / 19 passed：`test_the_read_only_store_is_what_the_preview_opens`。
- 说明：库已经是回滚日志、目录在单元测试里可写，所以「预览读到什么」照不出差别；
  唯一能钉住的是**这次打开是不是只读句柄**，用例直接观察 `SchemaRolloutStore.__init__` 的入参。

### M6 — 去掉「只在 PREPARE 上动手」这道门

```python
# 原文
    if state.phase is not RolloutPhase.PREPARE:
# 变异为
    if False:
```

- `ACK`：**红**，3 failed / 17 passed：`test_no_plan_is_ever_carried_past_dual_write`、
  `test_a_second_run_records_nothing_and_says_so`、`test_the_command_previews_then_applies`。
  失败原因是 `ValueError: rollout phases must advance consecutively`——第二次运行会试图从
  DUAL_WRITE 再推一次。

### M7 — CLI 丢掉 `--dry-run`

```python
# 原文
        dry_run=bool(args.dry_run),
# 变异为
        dry_run=False,
```

- `ACK`：**红**，1 failed / 19 passed：`test_the_command_previews_then_applies`
  （预览之后阶段本应还是 PREPARE）。

### M8 — 天花板抬高 **且** 把守卫整个去掉（纵深验证）

```python
# 两处同时变异：常量 → CUTOVER；
#   if SCHEMA_ROLLOUT_INSTALLER_PHASE_CEILING is not RolloutPhase.DUAL_WRITE:  →  if False:
```

- `ACK`：**红**，14 failed / 6 passed。失败原因不再是本包的守卫，而是
  `ValueError: rollout phases must advance consecutively` ——`SchemaRolloutStore.advance`
  自己拒绝跨阶段跳。**即使有人把安装器的守卫删掉，状态库仍然不让越过 DUAL_WRITE。**

### 变异覆盖对照

| 简报要求的变异 | 对应 | 结果 |
|---|---|---|
| B 越过 DUAL_WRITE | M1、M8 | 红 |
| A 少一个 unit | M2 | 红 |
| A 多给一个非参与方 | M3 | 红 |
| 转换不改 journal 模式 | M4 | 红 |
| （自补）预览开写者 | M5 | 红 |
| （自补）不限 PREPARE | M6 | 红 |
| （自补）CLI 丢 dry-run | M7 | 红 |

---

## 5. 两版本数字

| | 收集用例 |
|---|---|
| `origin/main`（`e474efd`，包 G 报告实测） | **13904** |
| 包 G HEAD（`e3b0aaf`） | **13928** |
| 本分支 HEAD | **14053** |

本包净增 **+125**，逐项对得上：`test_runtime_systemd_schema_rollout_paths.py` 91 +
`test_runtime_schema_rollout_acknowledge.py` 20 +
`test_route_a_rollout_acknowledge_e2e.py` 13 +
`test_cli_configuration_free_dispatch.py` 的参数化新增 1 = 125。
新增用例都不带 `linux_exact` 标记，全部进分片。

**⚠️ 集成时必做**（按简报「不重生成 manifest」的约束，本包没动）：

1. `uv run python scripts/full_suite_shards.py generate --manifest-dir tests/manifests/full-suite-v1`
2. 把 `tests/unit/test_assert_full_suite_shards.py` 里的字面量 `13904` 改成集成后的最终数字
   （只做本包的话是 **14053**；与包 G 或其他包同批集成时按合并后的收集结果为准，
   两步缺一不可，只跑生成器仍然是红）。
3. R07 policy 重冻结（本包改了仓库文件），**必须是 PR 的最后一个 commit**，py3.11 / py3.12 各跑
   一次，**不能用 3.13**。顺序：先 1、2，再 3。

---

## 6. 下一窗口：acknowledge 放在哪一步

**结论：紧跟第 ④ 步 `runtime-deployment-profile`（装 bundle），在第 ⑤ 步 stage 之前。**
已写进 `DEPLOY.md`「路线 A 前置」第 30 条。

依据三条，逐条都是我实测或读代码确认的：

1. **前提只有两个，都是第 ④ 步的产物**：计划已落盘（`install_runtime_deployment_profile` 备的），
   以及 `<运行根>/current` 指向计划的目标代。从第 ④ 步到起 unit 之间**没有任何一步会动
   `data/runtime/current`**——publish 换的是 `/var/lib/rquant/runtime-authority/current.json`，
   是另一个文件。
2. **stage 与它互不相干**：`runtime_authority_stage` 从 legacy 根只读两样东西——
   `<legacy root>/generations/<代>/manifests/*.json`（`legacy_services`）与 `current`
   （`legacy_generation_id`）；**既不读也不写 `control/schema-rollouts`**。所以 acknowledge 放在
   stage 前后都不会触发「stage 与 publish 之间根被改动」那类拒绝，位置由别的理由决定。
3. **决定性的是 WAL 转换的时机**：生产上那 16 份状态库现在是 WAL，只有写者打开一次才转得过来，
   而 acknowledge 正好是写者打开。把它放在 96 s 的 root publish **之前**，意味着「库能不能打开、
   能不能转换、有没有权限问题」在一条便宜的本地命令里就有答案；放在 publish 之后，同样的问题
   要等一次 root 事务跑完才暴露，而那时窗口已经进入不容易回退的阶段。

配套注意：

- **先 `--dry-run`**。它只读打开，一个字节都不写，可以在任何时候跑。
  生产上 16 份现在是 WAL，所以第一次 dry-run 会把它们全报成
  `journal_mode_before: wal` / `phase_before: null` / `skipped_reason` 说要先 apply——**预期形状**。
- **这一步是生产数据库写入**（往 16 条哈希链上追加事件），按受控自动发布模式第 7 条
  需要 owner 单独明确授权，不走无人值守发布器。
- 若窗口在这一步之后失败并把 `data/runtime/current` 回退到上一代：计划停在 DUAL_WRITE，
  但不再是当前代，之后任何一次 acknowledge 都会跳过它们（输出写
  `plan does not target the current generation`），不会被误当成本代的进度。
- 第 28 条那条 live 平面固定启动顺序**照旧**。acknowledge 只消掉「等其他生产者承认」这一类失败，
  消不掉 `strategy_live` ↔ `signal_router` 的 signal bus 循环依赖（#220）。
- **#228 仍然在**：只要 `changed_runtime_schema_channels` 的指纹里带 `producer_commit`，
  今后每一次纯代码发布都会凭空生出这十六份计划，acknowledge 就得每次都跑一遍。
  这一步把成本从「两次失败 + 告警」降到「一条命令」，但没有消除成因。

---

## 7. 边界核对

| 简报约束 | 执行情况 |
|---|---|
| 只改 16 个 unit 的 `ReadWritePaths`，其余 unit 一字不动 | ✅ `git diff --stat` 16 files / 16+ / 16-，每个文件只有那一行 |
| 除 16 个 unit 外不动 `deploy/` | ✅ `deploy/` 下其余文件未改 |
| 不动 `.env` / 发布原语 / stage / `runtime_authority*` / wrapper | ✅ 未改。测试用私有环境根 `/Users/roxor/rq-rag-ra-h-cc/env.sh`，worktree 无 `.env` |
| B 绝不越过 DUAL_WRITE | ✅ 天花板常量 + 两条路径入口的守卫 + 状态库自身的连续阶段校验，三层；M1 / M8 必红 |
| 不放宽 CUTOVER | ✅ 未碰 `_validate_phase_exit`、未碰 `advance` |
| 不 skip / xfail | ✅ 新增用例里只有两处 `pytest.skip`，条件都是 `os.geteuid() == 0`（root 下 mode 位不生效，用例前提不成立）；Docker 里跑的是 uid 1000，全部实跑 |
| 变异 ≥ 4 | ✅ 8 条，全红 |
| e2e 在 Docker 非 root 下真装两代 bundle | ✅ 见 §3.3 |
| trailer | ✅ 每个 commit 带 `Co-Authored-By` 与 `Claude-Session` |
| 不 push | ✅ 未 push |
| 不重冻结 R07 | ✅ 未动（集成时必做，见 §5） |
| 不重生成 manifest | ✅ 未动（集成时必做，见 §5） |
| 测试用 `.venv/bin/python -m pytest` | ✅ |
| 合并包 G 最新 HEAD | ✅ `f71497b` merge `e3b0aaf`，只有 `CHANGELOG.md` 一处自动合并，无冲突 |
