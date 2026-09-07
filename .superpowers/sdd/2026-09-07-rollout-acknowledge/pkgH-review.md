# 包 H 独立审查：16 个 unit 的 rollout 写权限（A）+ 安装器代做 PREPARE 承认（B）

- 审查对象：`/Users/roxor/brain/30-projects/rQuant/.worktrees/ra-h-cc`，分支 `cc/20260907-rollout-acknowledge`，HEAD `f28249a`（含包 G 的 8 条 commit，base `e474efd`）
- 标准：`/Users/roxor/brain/30-projects/rQuant/.worktrees/release-a-cc/.superpowers/sdd/2026-09-03-release-a-rollout/route-a-pkgH-brief.md`、同目录 `route-a-rulings.md` 裁决 17（owner 原文「同意 A 加 B，同意转换状态库」）、`gh issue view 227 228`、`/Users/roxor/brain/30-projects/rQuant/.worktrees/ra-g-cc/.superpowers/sdd/2026-09-07-rollout-store-readonly/pkgG-review.md` §5/§6
- 审查环境：本机 macOS `/Users/roxor/brain/30-projects/rQuant/.worktrees/ra-h-cc/.venv`（Python 3.11.15）；Linux 用 Docker `python:3.11-slim`、非 root（uid 1000）、Python 3.11.16 / SQLite 3.46.1；私有工作根 `/Users/roxor/rq-rahr-review`（已删除）
- 本审查不改代码、不提交、未读写 `.env`；每次变异后都 `git checkout -- .` 复原，收尾 `git status --porcelain` 为空

---

## 裁定

**不通过：两条 must-fix，都在 B 侧，都能在生产上直接踩到。**

分开说：

- **A（16 个 unit 的 `ReadWritePaths`）合格。** 改动面、参与方名单、测试锁定、边界都对得上，我自己从真实生产画像重新推了一遍参与方，得到的就是那 16 个，一个不多一个不少。
- **B（安装器代做 PREPARE 承认）的授权边界守得住**：天花板常量、两条路径的入口守卫、状态库自身的连续阶段校验，三层都在，变异全红。**但它在生产上跑不通**：
  - **MF1**：生产计划的 `deadline` 是 `started_at + 600 秒`（生产画像默认值）。过了这 10 分钟，apply 会抛一个没被接住的 `ValueError: rollout deadline has expired`，而且是在已经把前面若干份库的 journal 改写之后抛，命令中途死掉、不输出 JSON、不回滚。`--dry-run` 完全不看 deadline，仍然报 `advanced: true` / `phase_after: dual_write`——预览承诺了 apply 做不到的事。
  - **MF2**：不是当前代的计划在打开库之前就被跳过，**journal 布局一个字节都不碰**；而 `load_runtime_schema_service_bindings` 是先打开每一份计划的库、再判断是不是当前代。所以生产上现存那 16 份 WAL 库（属于代 `bf2da6d8…`），在下一次装新代之后既不会被 acknowledge 转换，又会在每个 kind-backed role 启动时把它挡住——就是 #227 原样重演。`DEPLOY.md` 第 30 条把 acknowledge 放在装 bundle 之后，正好命中这个顺序。

两条我都在本机复现过（见第四节）。测试之所以全绿，是因为夹具给的 deadline 是十年、并且世界里只有一代计划。

A 部分可以按原样合入；B 部分的代码不需要推翻，改动量很小（见 must-fix 里给的修法），但**修完之前不能按 DEPLOY 第 30 条去生产窗口执行**。

---

## 一、A：16 个 unit（必查 1）

### 1.1 改动面：恰好 16 个文件、每个恰好一行、同一行追加

```
$ git diff --stat e474efd..HEAD -- deploy/systemd/
 16 files changed, 16 insertions(+), 16 deletions(-)
```

逐个看过 `git diff`：每个文件只动 `ReadWritePaths=` 那一行，在原有条目后面追加
` -/home/lighthouse/rquant/data/runtime/control/schema-rollouts`，`ExecStart` / `Slice` /
`User` / `ReadOnlyPaths` / `InaccessiblePaths` 一字未动。追加在同一行是必须的：
`tests/unit/test_runtime_systemd_services.py` 用 `configparser(strict=True)` 读 unit，
另起一行会直接抛 `DuplicateOptionError`。`-` 前缀（缺失即忽略）也在，符合 #192。

`deploy/` 全目录里提到 `schema-rollouts` 的文件恰好是这 16 个（我自己 grep 过整个
`deploy/`，包括 timer、oneshot、arbiter）。另外 7 个 runtime unit 整份文件不含这个字符串。
16 + 7 = 23，与 `deploy/systemd/rquant-runtime-*@.service` 的总数对上。

我另外查了一遍反向收回：23 个 unit 的 `InaccessiblePaths` 都只有
`.env` / `current/secrets` / `current/credentials`（lab-jobs 多一个 `/etc/rquant/...`），
没有一条落在 rollout 根上或它下面。`rquant-runtime-serving@` 是嵌套那一例，它的
`ReadOnlyPaths` 含整个 `-…/data/runtime/control`，`systemd.exec(5)` 明确写着
「Nest `ReadWritePaths=` inside of `ReadOnlyPaths=`」是打开只读目录下可写子目录的正规做法，
所以嵌套成立。

### 1.2 参与方名单：我自己从生产画像推了一遍，结果就是那 16 个

我没有采信报告的清单，也没有采信包 G 审查的清单，而是写了一个独立脚本，走生产画像自己的
构造路径（`tests.unit.test_runtime_production_profile._inputs` →
`install_production_runtime_prerequisites` → `build_production_runtime_profile` →
`build_runtime_schema_contract_bundle`），然后按 `install_runtime_deployment_profile` 真正
用的判据（`profile.schema_rollout_policies` = 既有生产者又有消费者的 channel）取参与方：

```
schema_rollout_policies: 16
producer service ids: 19
consumer service ids: 14
union service ids: 20
consumers-only (not producers): ['serving.publisher.v1']
union kinds: 16
derived units == pinned PARTICIPANTS: True 16
missing: [] extra: []
multi-producer channels:
   runtime.strategy_candidate.snapshot 3 ['candidate.auction_gap.v1', 'candidate.growth_board_surge.v1', 'candidate.n_shape.v1']
   runtime.strategy_signal.envelope 3 ['strategy.auction_gap.v1', 'strategy.growth_board_surge.v1', 'strategy.n_shape.v1']
```

结论逐条：

- **16 份计划**，与生产上实际备下的份数一致。
- **19 个生产者 service id → 15 个 unit 模板**；消费者 14 个，其中 13 个已经在生产者集合里，
  多出来的只有 `serving.publisher.v1` → `rquant-runtime-serving@`。合计 **16 个 unit 模板**。
- **实现者对判据的纠正是对的**。我读了 `load_runtime_schema_service_bindings` 的
  `CONSUMER_ACK` 分支：它对**计划的每一个**消费者都要写一条能力回执，
  `requires_serving_generation_ack` 的那些通过 binding 在主循环里写，其余的当场
  `acknowledge_consumer`，两条都是往哈希链上追加。所以判据是
  `plan.producers ∪ registry.consumers`，不是「只算 requires_serving_generation_ack 的消费者」。
  我顺手核过：在当前生产画像里 `requires_serving_generation_ack` 的消费者只有
  `serving.publisher.v1` 一个，所以两种判据碰巧给出同一个 16——**答案相同，判据不同**，
  写对判据是有意义的，将来出现一个「只消费、不产出、又不要 serving 回执」的 service 时，
  窄判据会漏掉它。
- `authority.registry` 是**按 channel 建的**（`build_runtime_schema_rollout` 里
  `consumers` 来自 `new_channel.consumers`），不是全局注册表，所以这个并集就是这份计划的参与方，
  不会把无关服务卷进来。

`tests/unit/test_runtime_systemd_schema_rollout_paths.py` 把 16/7 钉死了（91 个用例，含参数化），
并且 `tests/integration/test_route_a_rollout_acknowledge_e2e.py::test_the_granted_units_are_exactly_the_participants_of_the_prepared_plans`
用真实两代装机把这份清单反推了一遍——这一条我在 Linux 非 root 下实跑通过。

### 1.3 整个目录而不是 per-plan：理由成立

`plan_id` 是计划内容的 SHA-256（`LiveSchemaRolloutPlan.plan_id` 我读过，覆盖 dataset、
两个声明指纹、生产者、消费者、注册表指纹、目标代等），每一代都变，静态 unit 文件追不上。
写死一代的 `plan_id`，下一代就变成「授权了一个不存在的路径」，而且因为带 `-` 前缀，
连报错都没有。用例 `test_the_grant_is_the_whole_rollout_root_and_never_one_plan` 禁止任何 unit
出现 `…/schema-rollouts/<子路径>`。per-plan 只能靠安装器生成 drop-in，那是另一次授权。

---

## 二、B：安装器代做 PREPARE 承认（必查 2）

### 2.1 天花板：两条路径都检查

`SCHEMA_ROLLOUT_INSTALLER_PHASE_CEILING: Final[RolloutPhase] = RolloutPhase.DUAL_WRITE`。
`_require_installer_phase_ceiling()` 在 `_rollout_acknowledgement_preview` 与
`_apply_rollout_acknowledgement` 的**第一行**各跑一次，都在读任何一份计划之前。预览那一处是
最后一条 commit `0943c70` 补的，方向对：预览是操作员做决定前读的东西，它不该预告一件代码
永远不许做的事。

三层防护我逐层验过：

1. 常量本身（M1：改成 CUTOVER，20 个用例里红 15）；
2. `_require_installer_phase_ceiling` 的守卫；
3. **状态库自己**：`SchemaRolloutStore.advance` 里
   `if _FORWARD_PHASES.index(target) != _FORWARD_PHASES.index(current) + 1: raise ValueError("rollout phases must advance consecutively")`。
   M8（常量抬高 + 守卫整个删掉）之后，红的原因不再是包 H 的守卫，而是这一句——**即使有人把
   安装器的守卫删干净，状态库仍然不让从 PREPARE 跳到 CUTOVER**。

### 2.2 只动 PREPARE、且只动目标为 `current` 的计划

- 代不对：`_apply_rollout_acknowledgement` 在**打开库之前**读 `authority.json` 判代，不是本代就
  原样返回，`skipped_reason` 写 `plan does not target the current generation`。我实测确认过它
  连 journal 布局都不碰（这一点正是 MF2 的成因，见第四节）。
- 阶段不是 PREPARE：写 `plan is past PREPARE (phase …)` 并返回。
  **一处措辞不精确**：这时候库已经被以写者身份打开过了（`load_runtime_schema_rollout` 在判阶段
  之前），所以 journal 头会被写。「不写」应该说成「不往哈希链上追加事件」。

### 2.3 承认本身没有额外证据量——这一条成立

我读了 `SchemaRolloutStore.acknowledge`：它拿 `plan.producers` / `plan.consumers` 重新查一遍
参与方，比对 `participant.contract_fingerprint`，再比对
`declaration_fingerprint == plan.new_declaration_fingerprint`。安装器传进去的两个指纹本来
就取自同一份冻结计划，所以搬到安装器确实不产生计划里没有的信息。

**离开 DUAL_WRITE 不是这样**，门在
`SchemaRolloutStore._validate_phase_exit`（`src/rquant/schema_compatibility.py:1605`）：
`current is RolloutPhase.DUAL_WRITE` 分支要求 `schema_dual_write_evidence` 或
`schema_dual_write_value` 里真有行，而这些行只由
`RuntimeSchemaDualWriteBinding.commit_payload` → `record_dual_write_values` 写入，
也就是生产者主循环真的双写过一次；CONSUMER_ACK 分支要求可信消费者回执齐全且不过期。
安装器这条 CLI 一次都没有调过这两个入口。**所以 CUTOVER 目前确实只能由真实双写证据驱动。**

我按简报要求把这条门单独变异了（SA1，见第四节）：**它没有任何测试保护**。

### 2.4 WAL → 回滚日志的转换与记录

apply 走 `load_runtime_schema_rollout(root, plan_id=plan_id)`（默认写模式），写者的
`_connect` 里那句 `PRAGMA journal_mode = DELETE` 把库头第 18 字节从 2 改成 1，转换就发生在这里。
转换前后各调一次 `persisted_rollout_journal_layout`（读库头 20 字节，不会顺手建 `-shm`），
逐份记进 `journal_mode_before` / `journal_mode_after`。M4（把 DELETE 改回 WAL）在
ACK + SCHEMA 89 个用例里红 25，与报告逐字一致。

### 2.5 `--dry-run` 只读、不转换

预览走 `read_only=True`，并且在打开之前先用库头判一次布局：是 WAL 就直接返回
`skipped_reason`，不去开它。理由站得住：转换正是操作员要预览的那个改动，预览把它做掉就不是预览。
用例 `test_the_dry_run_refuses_to_convert_a_wal_store_just_to_preview_it` 钉住了这一点，
M5（预览改成写者打开）在报告里红 1，我复跑 M1/M4 时顺带确认了同一批用例的存在。

### 2.6 幂等

第二次跑 `changed` 全 False、库文件 sha256 逐份不变、`skipped_reason` 写
`plan is past PREPARE (phase dual_write)`。我在 macOS e2e 与 Linux e2e 里都实跑过
（`test_a_second_acknowledgement_changes_nothing`）。

---

## 三、e2e：我自己复跑（必查 3）

### 3.1 Linux，Docker `python:3.11-slim`，非 root

镜像里装 `git` 与 `openssh-client`，建 uid 1000 的 `runner`，`umask 022`（Debian 私有组默认
umask 002 会让父目录变成 0775，`runtime_authority` 的可信目录校验会直接拒——这是我这边的环境
问题，不是代码问题），`uv sync --frozen --python 3.11`：

```
3.11.16 3.46.1
uid=1000
tests/unit/test_runtime_systemd_schema_rollout_paths.py …
tests/unit/test_runtime_systemd_read_write_paths.py …
tests/unit/test_runtime_systemd_services.py …
tests/unit/test_runtime_schema_rollout_acknowledge.py …
tests/unit/test_cli_configuration_free_dispatch.py …
tests/unit/test_schema_compatibility.py …
tests/unit/test_runtime_schema_registry.py …
tests/integration/test_route_a_rollout_acknowledge_e2e.py …
tests/integration/test_route_a_schema_rollout_sandbox_e2e.py …
======================= 420 passed in 720.96s (0:12:00) ========================
```

**420 passed，0 failed，0 skipped**，与报告的数字一致。uid 1000 下那两处
`os.geteuid() == 0` 的 skip 条件不成立，13 例新 e2e 与包 G 的 10 例沙箱 e2e 全部实跑。

简报点名要看的六件事，在这 13 例里逐条成立（我读了用例正文，不是只看绿点）：
真装两代 bundle → `acknowledge --dry-run` 在**封住的 0555 根**上列出 16 份且库字节不变 →
apply 后 16 份全 DUAL_WRITE、全回滚日志、无 sidecar → 再跑一次 0 改动 →
三生产者计划的第 0、1 个 `candidate_publisher` 实例承认前抛
`schema producer startup is waiting for every producer PREPARE ACK`、承认后 rc 0 跑完一轮 →
根再封只读之后真 `commit_payload` 给出包 G 的措辞（点名 `market-minute.source.v1`、
库路径、`ReadWritePaths`、`control/schema-rollouts`）。

### 3.2 macOS

| 批次 | 我的结果 | 报告 |
|---|---|---|
| `test_route_a_rollout_acknowledge_e2e.py` | **13 passed**（355.32 s） | 13 passed（336.70 s） |
| 三个 systemd 用例文件 | **295 passed**（1.71 s） | 295 passed |
| `test_runtime_schema_rollout_acknowledge.py` + `test_cli_configuration_free_dispatch.py` | **33 passed**（20 + 13） | 20 passed |
| 十个文件的回归批 | **664 passed / 1 failed** | 665 passed |

295 + 20 + 13 = **328**，与简报要的「macOS 665 + 328」对上。

那一条 failed 是 `tests/unit/test_cli.py::TestLabWorkerCli::test_cli_resource_bindings_run_real_bounded_spawn_probe`，
报 `RuntimeResourceAdmissionError: CPU load probe failed`——当时本机同时在跑 Docker 那一轮，
CPU 负载探针不过。**单独重跑该用例 1 passed**，与本次改动无关，属环境噪声。

---

## 四、变异与我自己的复现（必查 4）

批次：`SYSTEMD` = 三个 systemd 用例文件（295）；`ACK` =
`tests/unit/test_runtime_schema_rollout_acknowledge.py`（20）；`SCHEMA` =
`test_schema_compatibility.py` + `test_runtime_schema_registry.py`（69，基线 69 passed）；
`DISPATCH` = `test_cli_configuration_free_dispatch.py`（13）。每条变异后 `git checkout -- .` 复原。

### 4.1 复跑报告那 5 条，逐条与报告一致

| 变异 | 内容 | 批次 | 我的结果 |
|---|---|---|---|
| M1 | 天花板常量 → `CUTOVER` | ACK | **红 15 / 绿 5**，报 `the installer may carry a schema rollout no further than dual_write` |
| M2 | `rquant-runtime-feature@` 去掉授权 | SYSTEMD | **红 5 / 绿 290**（用例名与报告逐条相同） |
| M3 | `rquant-runtime-watchlist-quote@` 加上授权 | SYSTEMD | **红 5 / 绿 290** |
| M4 | 写者 `PRAGMA journal_mode = WAL` | ACK+SCHEMA | **红 25 / 绿 64** |
| M8 | 天花板 → CUTOVER **且**守卫删掉 | ACK | **红 14 / 绿 6**，失败原因是 `ValueError: rollout phases must advance consecutively` |

M8 这一条是纵深的证据：状态库自身拒绝跨阶段跳。

### 4.2 我自补 5 条

| 变异 | 内容 | 批次 | 结果 | 说明 |
|---|---|---|---|---|
| **SA1** | `_validate_phase_exit` 的 `elif current is RolloutPhase.DUAL_WRITE:` → `elif False:` | SCHEMA + ACK（89）、`tests/integration/test_schema_rollout_e2e.py`（4） | **全绿，存活** | **这就是「CUTOVER 只能由真实双写证据驱动」那条门，它没有任何测试保护**。我 grep 过全仓库：`dual_write lacks consistency evidence` / `dual_write lacks target generation consistency evidence` 两句错误文本在 `tests/` 里一次都没出现，所有 `target_phase=RolloutPhase.CONSUMER_ACK` 的用例都是先写证据再推进的正路 |
| **SA2** | `_apply_rollout_acknowledgement` 里 `if declared.target_generation_id != generation_id:` → `if False:` | ACK | **红 1 / 绿 19**（`test_a_plan_that_targets_another_generation_is_left_alone`） | 代际判据有保护 |
| **SA3** | 删掉 advance 之后那段 `if state.phase is not RolloutPhase.DUAL_WRITE: raise …` | ACK | **全绿，存活** | 冗余守卫，没人钉住。包 G 刚以「冗余守卫会让变异活下来」为由删掉 `PRAGMA query_only`，同一把尺子该量到这里 |
| **SA4** | 把 `installer-prepare:` / `installer-dual-write:` 的 `operation_id` 改成 `service-…` | ACK | **红 1 / 绿 19**（`test_the_acknowledgements_are_in_the_plans_own_hash_chain`） | 安装器代签在哈希链上的**身份标记**有保护，事后可审计 |
| **SA5** | 让 `runtime_deployment_bundle` 在模块层 `import rquant.config` | DISPATCH | **红 3 / 绿 10** | 免配置性质有保护，但是**间接的**：红的是既有的 `rquant.runtime_deployment_profile` 探针（它 import 了 bundle），`HANDLER_MODULES` 本身没加新命令的 import 链 |

### 4.3 我自己复现的两个生产可达缺陷

两个探针都是临时用例，跑完已经移出仓库（`git status --porcelain` 为空）。

**探针 P1 — 计划过期（MF1 的证据）。** 用包 H 自己的 `rollout` 夹具，先把 4 份库都改回 WAL
（生产现状），再用一个超过 `plan.deadline` 的 `now` 跑 apply：

```
RAISED: ValueError rollout deadline has expired
LAYOUTS BEFORE: ['wal']
LAYOUTS AFTER: ['rollback', 'wal', 'wal', 'wal']
```

第一份库的 journal 已经被改写，然后命令抛了一个没被接住的 `ValueError` 死掉，后三份原封不动。
没有 JSON 输出，没有 per-plan 的 `skipped_reason`，没有回滚。

同一个 `now` 跑 `--dry-run`：

```
DRY RUN plans: 4
phase_after: ['dual_write']
advanced: [True]
skipped: ['None']
```

预览完全不看 deadline，报告「会推进到 dual_write」。

**为什么这在生产上必然发生**：`src/rquant/runtime_production_profile.py:134` 写着
`schema_rollout_stage_timeout_seconds: PositiveSeconds = 600`，
`install_runtime_deployment_profile` 用它算 `deadline = started_at + 600 秒`。
云服务器 82.156.0.68（lighthouse 用户）上那 16 份计划是 2026-09-07 16:4x 备下的，
**deadline 早就过了**。而 `SchemaRolloutStore.acknowledge` 与 `advance` 都会先跑
`_validate_time`，`now > plan.deadline` 就抛。测试之所以看不见，是因为
`tests/unit/test_runtime_schema_rollout_acknowledge.py:71` 的
`DEADLINE = STARTED_AT + timedelta(days=3650)`（注释里也写明了是为了绕开
`_validate_time`），e2e 则是装完立刻承认、几秒之内跑完。

**探针 P2 — 上一代留下的 WAL 库（MF2 的证据）。** 同一个夹具：先把 4 份库都改回 WAL，
再装第三代 bundle（模拟下一窗口的第 ④ 步，`current` 因此换代），然后按 `DEPLOY.md` 第 30 条
跑 acknowledge，最后起一个 role：

```
layouts now: ['wal']
third generation: 90a18a27ae6c
plans seen: 4
skipped_reason: ['plan does not target the current generation']
layouts after acknowledge: ['wal']
ROLE START RAISED: SchemaRolloutStateUnavailableError schema rollout state …/control/schema-rollouts/1461e544…/state.sqlite3 is in WAL journal mode, which cannot be read without …
```

也就是：acknowledge 跳过旧代计划、**不转换**它们的 journal；而
`load_runtime_schema_service_bindings` 是**先打开每一份计划的库、再判代**
（`src/rquant/runtime_deployment_bundle.py:2259-2262`），所以旧代那份 WAL 库把角色挡在门外，
错误形状与 #227 一模一样。

补一句会让这件事变成长期问题的事实：**没有任何代码删除旧的计划目录**（我 grep 过
`shutil.rmtree`，只用于 generation 暂存目录）。叠加 #228（每次纯代码发布凭空生 16 份计划），
`control/schema-rollouts` 下的目录数每发一版加 16，而准入每次启动都要把它们全部打开一遍。

---

## 五、must-fix / should-fix / note

### must-fix

**MF1 — 过期计划：apply 抛裸异常并留下半转换的库，dry-run 还预告它做不到的事。**
证据见 4.3 探针 P1。范围内：owner 授权的是「acknowledge CLI 以写者打开完成状态库转换」，
而这条命令在生产上现有的 16 份计划上跑不到底；`DEPLOY.md` 第 30 条是要在生产窗口照着敲的。
最小修法（两件都要做）：

1. `_apply_rollout_acknowledgement` 把 `_validate_time` 会拒的情形变成**每份计划自己的
   `skipped_reason`**（例如「plan deadline expired at …；需要重新 `prepare` 或由 owner 决定」），
   而不是让 `ValueError` 穿过 CLI；命令整体返回非 0 但把 16 份的报告打完整。
2. `_rollout_acknowledgement_preview` 也判一次 deadline，让预览与执行说同一句话。
   顺带补一条用例：夹具用**生产那个 600 秒**的 deadline，`now` 取过期时刻，断言 apply 不写
   哈希链、报告完整、返回码非 0。

如果决定「10 分钟窗口内必须跑完」也是一种答案，那就要写进 `DEPLOY.md` 第 30 条，并且明确
超时之后的补救动作是什么（重新 `prepare` 属于生产写入，要 owner 再授权）。

**MF2 — 上一代的 WAL 库既不被转换、又会挡住所有角色。**
证据见 4.3 探针 P2。范围内：裁决 17 的原文是「同意转换状态库」，简报 B 写的是
「以写者身份打开**每份** `control/schema-rollouts/<plan>/state.sqlite3`」；实现只覆盖当前代。
两条修法二选一（我倾向第一条，改动最小且不越授权）：

1. **acknowledge 对非当前代的计划也做一次写者打开**（只转换 journal、绝不承认、绝不推进阶段），
   报告里照旧写 `plan does not target the current generation`，但 `journal_mode_after` 如实反映
   转换。这仍然在「转换状态库」的授权内，不触碰任何阶段机。
2. 或者改准入：`load_runtime_schema_service_bindings` 先读 `authority.json` 判代，
   不是本代就跳过、根本不打开状态库。这样更彻底（旧代库爱是什么布局是什么布局），
   但它改的是包 G 的准入路径，属于另一次评审的范围。

无论选哪条，`DEPLOY.md` 第 30 条里「这一步顺带完成转换」这句在改好之前是不成立的，
要么改代码要么把包 G 审查 §7.4 第 2 条那个手工 `PRAGMA journal_mode = DELETE` 清单动作写回去。

### should-fix

**S1 — `_validate_phase_exit` 的 DUAL_WRITE 出口没有任何测试保护（SA1 存活）。**
这是「安装器止于 DUAL_WRITE 是安全的」这整套论证所依赖的那扇门。补一条负向用例即可：
计划推到 DUAL_WRITE、一条双写证据都不写、`advance` 到 CONSUMER_ACK，断言抛
`dual_write lacks consistency evidence`。

**S2 — `_apply_rollout_acknowledgement` 里 advance 之后那段断言是冗余守卫（SA3 存活）。**
要么补一条用例钉住它，要么删掉。包 G 刚用同一把尺子删过 `PRAGMA query_only`。

**S3 — 报告把 B 的行为说成「阶段不是 PREPARE 就不写」，实际是「不往哈希链上追加」。**
当前代的计划无论什么阶段都会被以写者身份打开一次，journal 头会被改写。措辞订正即可。

**S4 — `tests/unit/test_runtime_systemd_schema_rollout_paths.py` 的模块 docstring 指错了文件。**
它写「derivation is asserted … in `tests/integration/test_route_a_schema_rollout_sandbox_e2e.py`」，
实际推导用例在 `tests/integration/test_route_a_rollout_acknowledge_e2e.py:135`。

**S5 — `DEPLOY.md` 第 19 条仍写「这三条命令」免配置**，新增的 `runtime-schema-rollout` 是第四条。
第 30 条里虽然写了「本命令免配置」，但第 19 条是操作员查免配置命令的那一条，应同步。

**S6 — 包 G 审查 §7.2 要求订正的 CHANGELOG 措辞没有落。**
`CHANGELOG.md:58` 仍是「`immutable=1` 看不见并发写」。源码里
`SchemaRolloutStore` 的 docstring 反而写对了（「an immutable reader is pinned to the snapshot
it opened」）。集成时把 CHANGELOG 这句改成与 docstring 一致的说法。

**S7 — 包 H 把自己的报告提交进了仓库。**
`git ls-files .superpowers` 只有 `.superpowers/sdd/2026-09-07-rollout-acknowledge/pkgH-report.md`；
`origin/main` 上没有 `.superpowers/` 这个目录，包 G 的报告也没进 git（包 G 审查 N6 记的是
`?? .superpowers/`）。要么两个包统一进仓库，要么合并前把这条 `docs:` commit 从 PR 里去掉。
它还会进 R07 baseline。

### note

**N1 — 新命令的 import 链免配置是**间接**保护的。**
`HANDLER_MODULES` 仍是三个模块，没加 `rquant.runtime_deployment_bundle`。SA5 之所以红，是因为
既有的 `rquant.runtime_deployment_profile` 探针 import 了 bundle。我另外裸测过
`import rquant.runtime_deployment_bundle` 之后 `rquant.config` 不在 `sys.modules` 里，性质成立。
把新模块加进 `HANDLER_MODULES` 是一行的事。

**N2 — `ruff format --check` 会重排的是三个文件不是两个**：
`test_runtime_systemd_services.py`、`test_runtime_systemd_read_write_paths.py`、
`test_cli_configuration_free_dispatch.py`。我把 `e474efd` 的三份原样拉出来单独跑，
**三份同样报 reformat**，全是存量，不是本次引入。没有顺手格式化是对的，报告的数字少算了一个。

**N3 — 旧计划目录永不清理，叠加 #228 会线性增长。**
每发一版新增 16 个目录，准入每次启动全部打开一遍。#228 修好之前，这是 MF2 的放大器。

**N4 — `test_the_second_three_producer_plan_stops_blocking_its_roles_too` 只断言「不是这个原因」。**
报告说清楚了原因（`strategy_live` 在这个 harness 里另有主机形状问题），可以接受，但它不是
一条正向证据，别在验收表里当成「两份三生产者计划都验过了」。

**N5 — commit 与工作树卫生干净。**
14 条 commit 全部带 `Co-Authored-By: Claude Fable 5.1` 与
`Claude-Session: https://claude.ai/code/session_01Moh45U775Rd9XxYhx649fg`；
`git status --porcelain` 为空；改动面
（`git diff --name-only e474efd..HEAD`）= `deploy/systemd/` 恰好 16 个文件 + 4 个
`src/rquant/*.py`（含包 G 的三个）+ 9 个测试文件 + `CHANGELOG.md` + `DEPLOY.md` + 那份报告，
**没有** `.env`、发布原语、stage、`runtime_authority*`、wrapper。
`ruff check` 十个改动文件全过。

---

## 六、安全结论

### 6.1 A：把 `control/schema-rollouts` 的目录写权限发给 16 个 unit，到底放开了什么

**这条 grant 是「整个 rollout 根」，不是「本 unit 参与的那几份计划」，也不可能更窄。**
per-plan 授权在静态 unit 文件里做不到（`plan_id` 每代都变），文件级授权在技术上不存在
（回滚日志要在库旁边建 `state.sqlite3-journal`，WAL 要建 `-wal`/`-shm`，两种都是**目录**权限）。
所以本次授权的实际含义是：**16 个 unit 中的任意一个，都能读写 `control/schema-rollouts` 下
全部计划的目录**，包括它完全没有参与的那些 channel 的计划。

横向影响要分两半看，一半有硬约束，一半没有：

**有约束的那一半：`authority.json` 改不动。**
`load_runtime_schema_rollout` 每次都拿两代 **immutable bundle**（存放在
`generations/<id>/…`，**不在**本次授权的路径里）重新推导一遍
`build_runtime_schema_rollout(...)`，然后逐项比：
`previous_bundle_content_hash` / `target_bundle_content_hash` / `plan` / `registry`，
任何一项对不上就抛「runtime schema rollout authority differs from immutable bundles」。
所以篡改参与方名单、指纹、deadline、目标代都会被当场拒掉。这是实打实的护栏。

**没有约束的那一半：`state.sqlite3` 可以被整个重写。**
状态库是一条**无密钥**的 SHA-256 哈希链（`_event_hash` 只是把字段拼起来做
`canonical_sha256`，没有签名、没有 HMAC），`_verified_state` 只验链自洽。
拿到目录写权限的进程可以把整份库删掉重建：
- `plan_json` 必须与目录名那个 `plan_id` 对得上，而 `plan_id` 是计划内容哈希，所以计划本身伪造不了；
- 但**阶段**、**双写证据行**（`schema_dual_write_evidence` / `schema_dual_write_value`）、
  **消费者回执**都只是普通表行，而它们的合法性判据（`_trusted_consumers`、
  `_validate_consumer_receipt`）用的全是**从冻结 bundle 就能推出来的公开量**。
- 结论：**16 个 unit 里任何一个被攻破，都能把任意一份计划（含它没参与的）伪造到 CUTOVER。**
  在此之前，零个 unit 能碰这个目录，systemd 的挂载命名空间是唯一的拦阻——因为 unit 跑的是
  `User=lighthouse`，文件也是 lighthouse 的，DAC 层面本来就不拦。

**这算不算可接受？** 在「个人自用、单机、所有 unit 同属 lighthouse」这个威胁模型下，
16 个 unit 之间本来就没有互相隔离的意图（它们共享 uid、共享 `data/runtime` 的大量子树），
所以增量是「多了一处可被横向破坏的控制面」，而不是「打开了一条新的提权路径」。
配合 #227 的现实（不给这个权限，rollout 在当前架构下根本推不完），我认为这次放开是合理的。

**有没有更窄的可行方案？** 在本次授权范围内没有。往后可以窄回去的两条路，都需要单独授权：

1. **安装器生成 drop-in**：装 bundle 时按「本角色这一代参与哪几份计划」写
   `/etc/systemd/system/rquant-runtime-xxx@.service.d/schema-rollout.conf`，本体 unit 不写死。
   这能把破坏面从「全部计划」收到「自己参与的计划」，是安全上明显更好的形态；
   代价是安装器要动 `/etc/systemd`，属于高风险变更。
2. **给状态库签名**：事件链改成由安装器/控制器持私钥签，服务侧只验签。这才是根治
   「有写权限就能伪造」的办法，但改动量远超本包。

短期务必做到的两件事：**A 的 16 个 unit 必须在云服务器 82.156.0.68 上
`systemd-analyze verify` 通过**（协调者已对 16 个 unit 跑过，16/16 OK），
并且**起一个真实参与方 role 实测沙箱确实放开了**——mac 上测不出 systemd 语义。

### 6.2 B：安装器代写 PREPARE 承认，放宽了什么、为什么止于 DUAL_WRITE

**放宽的是「谁来签这条承认」，没有放宽「承认里写了什么」。**
一条 PREPARE 承认的全部入参——`participant_fingerprint`、`declaration_fingerprint`——都取自
冻结计划，`SchemaRolloutStore.acknowledge` 收到之后还会拿冻结注册表把它们逐个重验一遍，
对不上就抛「participant is not in the frozen registry」/「participant fingerprint does not
match registry」/「declaration fingerprint does not match rollout target」。所以搬到安装器
**没有引入任何计划之外的信息**。

那么真正被放宽的是语义：原来这条记录的含义是「这个生产者进程真的起来了、并且它的 manifest
指纹与计划对得上」，现在变成「安装器确认计划里写着这个生产者，且指纹与冻结注册表一致」。
**丢掉的是「进程真的起来过」这一个比特。** 这个比特在协议里没有被任何后续判据消费——
DUAL_WRITE 的出口要的是双写记录，CUTOVER 的出口要的是消费者回执，两者都不看「谁签的 PREPARE」——
所以丢掉它不会让后面的门变松。谁背书？**安装器背书，而且背书是留痕的**：
`operation_id` 是 `installer-prepare:<plan>:<participant>` / `installer-dual-write:<plan>`，
与服务自签的 `service-prepare:<generation>:<service>` 明显不同，且 `operation_id` 进
`schema_rollout_event` 表、进 `_event_hash`，事后审计能一眼分清哪条是代签的。
我用变异 SA4 确认这个身份标记有测试保护。

**为什么止于 DUAL_WRITE：因为 DUAL_WRITE 的出口开始要真运行期证据。**
门在 `_validate_phase_exit`：离开 DUAL_WRITE 必须在
`schema_dual_write_evidence` 或 `schema_dual_write_value` 里有行，而这些行只由
`RuntimeSchemaDualWriteBinding.commit_payload` → `record_dual_write_values` 写入，
也就是**生产者主循环真的按新旧两份声明各写了一次并比对过**；离开 CONSUMER_ACK 还要求
可信消费者的回执齐全且不超过 `consumer_ack_max_age_seconds`。这两样安装器推不出来，
代签就是凭空造证据。

**CUTOVER 目前仍然只能由真实双写证据驱动**，我按简报要求找到并变异了这扇门：

- 正向：安装器这条 CLI 从头到尾没有调用过 `record_dual_write_values` /
  `record_dual_write_evidence` / `acknowledge_consumer`，只调 `acknowledge` 与 `advance`。
- 纵深：M8 证明即使把安装器的天花板守卫整个删掉、常量改成 CUTOVER，
  `SchemaRolloutStore.advance` 仍以「rollout phases must advance consecutively」拒绝跨阶段跳。
- **但是**（SA1）：把 `_validate_phase_exit` 里 DUAL_WRITE 那一整段判据换成永不执行，
  `test_schema_compatibility.py` + `test_runtime_schema_registry.py` +
  `test_runtime_schema_rollout_acknowledge.py`（89 例）与
  `tests/integration/test_schema_rollout_e2e.py`（4 例）**全绿**。
  也就是说：**这条门今天是对的，但没有任何测试拦着别人把它拆了。**
  这是 S1，建议合前补一条负向用例。

最后一条与安全直接相关的现实：**B 目前在生产上跑不到底**（MF1 的过期、MF2 的跨代 WAL）。
这不是「放宽」，是「失败关闭得太硬且太晚」——apply 会在改写了一部分库的 journal 之后
抛裸异常退出。失败关闭方向是对的，但半途而废加上没有报告，会让操作员在生产窗口里失去
「现在到底改到哪一份了」的判断依据。修 MF1 时请优先保证「要么整份都不动，要么把每一份的
结果都报出来」。

---

## 七、集成输入

### 7.1 full-suite manifest：13904 → **14053**（本分支单独集成时）

我自己在本分支跑的收集：

```
$ .venv/bin/python -m pytest -q --collect-only
14053/14057 tests collected (4 deselected)
```

`origin/main`（`e474efd`）是 13904，包 G HEAD 是 13928，本分支 14053，本包净增 125。
现在 CI 的 full-suite 契约是红的，我复现了：

```
FAILED tests/unit/test_assert_full_suite_shards.py::test_checked_in_manifest_matches_exact_collection_without_missing_or_duplicate_cases
1 failed, 12 passed
```

集成时两件事缺一不可：

1. `uv run python scripts/full_suite_shards.py generate --manifest-dir tests/manifests/full-suite-v1`；
2. **把 `tests/unit/test_assert_full_suite_shards.py:417` 的字面量 `13904` 改成集成后的最终数字**
   （只集成本分支就是 `14053`；与别的包同批合并时按合并后的收集结果为准）。
   只跑生成器仍然是红。

commit 措辞照 `a593843`：`chore(test): regenerate the full-suite shards for …`。

### 7.2 CHANGELOG

`[Unreleased]` 已经包含包 G 的 `Fixed` 与包 H 的 `Added` + `Changed`，三段内容完整、
与代码对得上。集成时补三处：

- 按 S6 把 `CHANGELOG.md:58` 的「`immutable=1` 看不见并发写」改成与
  `SchemaRolloutStore` docstring 一致的说法（`immutable=1` 等于向 SQLite 承诺文件不变，
  而 rollout 控制器会在服务运行期改它，行为未定义）；
- MF1 / MF2 修完之后，把「安装器代做承认」那一段里关于生产 16 份 WAL 库的说法按修法订正；
- 如果按 S7 把 `.superpowers/` 撤出 PR，CHANGELOG 不受影响，但改动面统计要重算。

### 7.3 DEPLOY

**第 30 条在 MF1 / MF2 修完之前不能照着执行。** 具体要改的：

- 「十六份状态库现在是 WAL……这一步顺带完成转换」——按 MF2 的修法重写，
  或者把包 G 审查 §7.4 第 2 条那个手工转换清单动作补回去；
- 补一句 deadline：计划的 `deadline` 是 `started_at + schema_rollout_stage_timeout_seconds`
  （生产画像默认 600 秒），所以这一步与第 ④ 步之间的间隔有硬上限，或者按 MF1 改成
  逐份跳过并报告；
- 第 19 条同步加上第四条免配置命令（S5）。

第 30 条其余部分我核过，与代码一致：
① acknowledge 的前提只有「计划已落盘」与「`data/runtime/current` 指向计划目标代」，
两者都是第 ④ 步（`runtime-deployment-profile`）的产物；
② `runtime_authority_stage` 只从 legacy 根读 `generations/<代>/manifests/*.json` 与 `current`
（我读了 `src/rquant/runtime_authority_stage.py` 的 `legacy_services` 与
`legacy_generation_binding`），**既不读也不写** `control/schema-rollouts`，所以 acknowledge 与
stage/publish 互不干扰；
③「须 owner 单独授权」「先 dry-run」两条都写了，与受控自动发布模式第 7 条一致。

### 7.4 R07

本包改了 `deploy/systemd/` 16 个文件、`src/rquant/` 3 个文件、测试 6 个文件、
`CHANGELOG.md`、`DEPLOY.md`，以及（按 S7 待定的）那份报告。**R07 policy 必须重冻结**，
按 `acceptance-pra.md` 的 G-5：重冻结是 PR 的最后一个 commit，py3.11 / py3.12 各跑一次，
**不能用 3.13**。顺序：先 7.1 的 manifest 重生成，再 R07 重冻结。

（提醒：本 worktree 只有 `.venv`（3.11.15），没有 `.venv312`；R07 那一步需要 3.12 环境，
集成前要先建。）

### 7.5 发版号

建议 **v0.33.1**：本包对外行为是「16 个 unit 多一条写授权」加「多一条 CLI 子命令」，
没有破坏性变更，也没有改任何已有命令的语义，patch 位合适。
但 **MF1 / MF2 修完之前不要打 tag**——tag 一打就意味着这一版可以照 `DEPLOY.md` 第 30 条上生产，
而那条路现在走不通。

### 7.6 下一窗口的顺序建议（在两条 must-fix 修完的前提下）

1. 权威链换代（本包改了 `src/rquant/`，必须重新 stage + publish）；
2. 第 ④ 步装 bundle；
3. **紧接着**跑 `acknowledge --dry-run`，确认 16 份的形状；
4. 拿到 owner 对「生产数据库写入」的单独授权后跑 apply，逐份核对
   `journal_mode_after: rollback` / `phase_after: dual_write`、无残留 `-wal` / `-shm`；
5. 再 stage + publish；
6. 按 DEPLOY 第 28 条的固定顺序起 unit。

第 3、4 步与第 2 步之间的间隔受 MF1 那个 600 秒 deadline 约束，改法定下来之前，
这个顺序里必须写明间隔上限。
