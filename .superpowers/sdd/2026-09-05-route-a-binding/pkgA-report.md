# 包 A 交付报告：让路线 A 在结构上可行（#207 / #186 / #188）

worktree `/Users/roxor/brain/30-projects/rQuant/.worktrees/ra-a-cc`，分支 `cc/20260905-route-a-binding`，
base = `origin/main` tip `2db3846`。**未 push**，未重冻结 R07，未重生成 `tests/manifests/full-suite-v1`
（新增 40 个用例，集成阶段一起重生成；当前 index 记的是 13653）。

## 1. commit 列表

| commit | 内容 |
|---|---|
| `3f1af9d` | `fix(runtime): stop constructing Settings while importing the recovery chain`（#186 / #188） |
| `0f48b83` | `fix(runtime): bind the schema loader to the legacy generation, not the authority one`（#207，裁决方向一） |
| `877d17a` | `test(runtime): accept #207 on a real bundle, a real staged manifest, two real roles`（端到端验收 + CI job） |
| `7f9af41` | `test(runtime): bound the stranger-manifest case to one loop iteration`（变异期间发现的用例缺陷，见 §5 M2） |
| `7ae0aa5` | `docs(test): name the seams the Route A acceptance actually uses`（两处 docstring 校准） |

改动面（`git diff --stat origin/main..HEAD`）：

```
 .github/workflows/ci.yml                              |  79 +
 src/rquant/dashboard/strategy_lab_data.py             |  39 +-
 src/rquant/dashboard/strategy_lab_runs.py             |  36 +-
 src/rquant/runtime_authority_stage.py                 |  38 +
 src/rquant/runtime_legacy_generation_binding.py       | 155 +++
 src/rquant/runtime_service_main.py                    | 140 +-
 tests/integration/test_route_a_legacy_binding_e2e.py  | 681 +++
 tests/unit/test_runtime_authority_publish.py          | 121 +-
 tests/unit/test_runtime_legacy_generation_binding.py  | 151 +++
 tests/unit/test_tp9_role_child_runtime.py             | 389 +-
 10 files changed, 1789 insertions(+), 40 deletions(-)
```

`deploy/`、`.env`、`runtime_authority.py`、`runtime_authority_publish.py`、stage 的 bootstrap 路径、
builder 的 plane/settings 断言一律未动。无 skip、无 xfail（唯一的 `skipif` 是 Linux-only 门的平台守卫，
沿用仓库既有 `linux_exact` 约定，且该用例已在 Linux 容器里真实跑绿，见 §4）。

## 2. #207 的改法（裁决方向一）

### 2.1 落点

* **stage 侧**（`runtime_authority_stage.py`，新增 `legacy_generation_binding()`）：每次 staging 都往
  generation 根目录写一份 `legacy-binding.json`。`--legacy-runtime-root` 模式下把 `--legacy-generation`
  （通常是字面量 `current`）解析成它真正指向的 64 hex 部署哈希后记下来，连同 legacy runtime root 一起；
  `--bootstrap-from-checkout` 模式下写 `mode: "bootstrap"`，两个字段为 `null`。
* **文档模块**（新增 `src/rquant/runtime_legacy_generation_binding.py`）：只依赖标准库与 `rquant.strict_json`，
  staging 工具与角色入口共用，角色不会因此把 staging 工具拖进 wrapper 子进程。
* **运行期**（`runtime_service_main.py`）：
  * `legacy_current_generation()` 取代原来只回答有无的 `legacy_runtime_root_is_current()`（后者保留，
    改成基于前者的谓词），直接把 `<root>/current` 指向的 legacy generation id 拿出来；
  * `:292` 的 manifest 路径校验抽成 `_authority_generation_directory()`，行为一字未改，两处复用；
  * 新增 `resolve_legacy_schema_generation()`：读 `<generation>/legacy-binding.json`，要求
    `mode == "legacy"`、`runtime_root` 等于本角色由 control root 推出的根、`generation_id` 等于
    `<root>/current` 当前解析到的 generation，通过后才把这个 legacy id 交给
    `load_runtime_schema_service_bindings`；
  * 非 `--authority-runtime` 的旧链路仍旧用 `args.expected_generation`，行为不变；降级分支
    （Route B，无 `current`）完全不读这份文档，行为不变。

### 2.2 为什么放在 generation 里而不是 plan 里

先查过 stage 产物：`plan.json` 的字段集合被 `runtime_authority_publish._PLAN_FIELDS` 冻死，且 plan 不进
generation，角色运行时读不到；generation full manifest 的字段集合被
`runtime_authority._GENERATION_MANIFEST_FIELDS` 冻死。两个文件都在「不动」清单里，所以新字段落成
generation 里的一份**受清单覆盖的文件**——这反而比加字段更强：它的 sha256 进 full manifest 的 `entries`，
full manifest 的 sha256 **就是** authority generation id，wrapper 在 exec 前先拿它比对权威链 slot，再对
每一条 entry 逐个核对磁盘上的 owner / mode / nlink / sha256（`_verify.verify_code_identity`）。
plan 的 `staged_files` 里也自动出现 `generation/legacy-binding.json` 及其摘要，dry-run 的操作员看得见。

## 3. 信任绑定语义变化（**请向 owner 点名这一段**）

**改之前**：一个 kind-backed 角色在非降级分支上只有一条绑定——`--expected-generation`（权威链 slot 给的
full-manifest sha256）。它被同时用作「这份 service manifest 属于哪一代权威 generation」和「schema
bindings 该按哪一代 legacy bundle 装载」。后者是错配：两个 id 由完全不同的算法产生，永远不等，所以
`data/runtime/current` 一出现，15 个 kind-backed 角色全部硬失败在
`ValueError: runtime schema service generation is not current`。

**改之后**：两个名字空间各自绑定，都不能跳过。

```
权威链 current.json 的 slot
  └─ generation_id = sha256(<generation>/full-manifest.json)      ← wrapper 核对
       └─ full-manifest.json 的 entries 覆盖并锁住 legacy-binding.json 的 sha256  ← wrapper 逐条核对
            └─ legacy-binding.json 记的 legacy generation
                 └─ 必须等于 <runtime root>/current 当前解析到的 generation      ← 角色核对（新增）
                      └─ load_runtime_schema_service_bindings 再自查一次
                         _current_target(root) == generations/<该 id>，并核对 manifest 指纹
```

同时 `:292`（manifest 必须坐落在名字等于 `--expected-generation` 的 generation 目录里）原样保留。

**新增的信任前提**（owner 需要知道自己在信什么）：

1. 「这一代 schema 是当前代」的判据，从「权威链说了算」变成「**staging 当时看到的 legacy generation**
   与「**角色启动时 `current` 指向的 legacy generation**」必须一致」。也就是说，判据的一半来自 staging
   时刻的观察，被冻进不可变 generation 里；另一半来自运行时刻的指针。
2. 因此**换 legacy 代必须重新 stage + publish 权威链**。运维含义：只把 `data/runtime/current` 切到新的
   legacy generation、不换权威 generation，所有 kind-backed 角色会拒绝启动，报
   `runtime legacy generation binding does not match the current pointer`。这不是回归，是刻意的——
   否则 `current` 一被挪动，角色就会拿一份和自己 manifest 无关的 bundle 去装 schema bindings。
3. 同一批 service manifest 在 bootstrap 与 legacy 两种模式下**不再产生同一个 generation id**（原
   `test_legacy_mode_..._yields_the_same_generation` 断言的正是这条等式，已按新语义重写）。这条歧义本身
   就是要消掉的：两个不同的 legacy generation 若 manifest 完全相同，从前会 stage 出同一个权威
   generation，交叉核对也就无从谈起。**`profile_id` 不变**（它由解释器闭包与实例标签算出，不含这份文档），
   所以 **#190 不会被路线 A 触发**，与包 0 的判定一致——本轮实测两次 staging 的 profile 都是
   `af5cdf679c2a42ccba5c62eb5c08b60c69cb05549d1493ddbf11bb7534fdf42b`，只有 generation id 不同。
4. **一份缺失的 `legacy-binding.json` 是拒绝，不是放行**。云端现役的那一代权威 generation 是本改动之前
   staged 的，没有这份文档；它今天跑的是 Route B（无 `current`），走降级分支，不受影响。但**包 C 装完
   bundle 之后，必须用带本改动的代码重新 stage + publish**，否则角色会报
   `runtime legacy generation binding is unavailable or contains a symlink`。

## 4. 端到端用例的真实产物证据

用例文件：`tests/integration/test_route_a_legacy_binding_e2e.py`（16 个用例，其中 1 个 Linux-only）。
路径上**没有任何打桩**：

* `install_runtime_deployment_profile` 装的是**真 bundle**（26 份 manifest，来自真 `build_production_runtime_profile`）；
* `runtime-authority-stage --legacy-runtime-root` 是**真 stage**，逐字节拷 manifest 并写真 `legacy-binding.json`；
* `publish_staging` 是**真发布**，落 root-owned 权威链；
* argv 由 **wrapper 自己的 `_verify.resolve_launch`** 推出（意味着它已经把 full manifest 与 slot 对过哈希、
  把每条 entry 在磁盘上核过一遍）；
* `runtime_service_main.run()` 用这份 argv 真跑，`load_runtime_schema_service_bindings` 是**真的那一个**
  （观测用例是把它包起来，不是替掉），服务主循环真的进了并真跑了一次迭代。

仅有的两处接缝，都不在被测路径上：`_seal_runtime_credentials` / `_recover_runtime_credentials`
（封 credstore 需要 sudo 下的 `systemd-creds`，任何测试都拿不到），以及 `World` 已有的那套
`/etc/rquant`、`/var/lib/rquant`、系统解释器常量指向临时根。

### 4.1 装出来的 bundle 目录树（真实产物，mac 端 basetemp 保留后 dump）

```
<basetemp>/source/runtime/
├── authorities/
├── control/            (25 个 kind 目录)
├── current -> generations/c0d218d83daba1a46edb2bd7b0b90711ea97d6c01a19104c41ef5b8b5532db70   ← 相对 symlink
├── generations/
├── live/
├── research/
└── serving/

<basetemp>/source/runtime/current/
├── deployment-profile.json      73487 bytes     ← #201 说的那份，真的在
├── generation-basis.json         9891 bytes
├── manifests/                   26 份 svc-<64hex>.json
├── runtime.env                    191 bytes
├── schema-bootstrap.json          356 bytes
└── schema-contracts.json       286750 bytes
```

### 4.2 权威 generation 与 manifest 路径

```
<basetemp>/root/var/lib/rquant/runtime-authority/generations/
└── 76d07f1342eddfb4eaa519c02b26b051c1fd3fe04821caeb5a7f83eb6b63413b/   (dr-xr-xr-x)
    ├── bin/  cwd/  lib/  scripts/  src/
    ├── full-manifest.json     14911 bytes  (-r--r--r--)
    ├── legacy-binding.json      265 bytes  (-r--r--r--)   ← 新增
    ├── manifests/              28 份（26 份 bundle 的 + 2 份 orphan 角色的）
    └── pyvenv.cfg
```

wrapper 给 `serving_publisher` 推出的 `--manifest`：
`<...>/generations/76d07f13…413b/manifests/svc-<64hex>.json`。

### 4.3 两条绑定链的实测数字

```
$ shasum -a 256 <gen>/full-manifest.json
76d07f1342eddfb4eaa519c02b26b051c1fd3fe04821caeb5a7f83eb6b63413b   ← 等于权威 slot 的 generation_id

$ cat <gen>/legacy-binding.json
{"generation_id":"c0d218d83daba1a46edb2bd7b0b90711ea97d6c01a19104c41ef5b8b5532db70",
 "mode":"legacy",
 "runtime_root":"<basetemp>/source/runtime",
 "schema_id":"rquant-legacy-generation-binding/v1","schema_version":1}

full-manifest.json 里它的那条 entry：
{"mode": 292, "nlink": 1, "owner_uid": 502, "path": "legacy-binding.json",
 "sha256": "406bd2548475eda2f85a8da77f6a6d936d3d0ed2cef8c71a9d93a2919dfcdf61",
 "size": 265, "type": "file"}

$ readlink <basetemp>/source/runtime/current
generations/c0d218d83daba1a46edb2bd7b0b90711ea97d6c01a19104c41ef5b8b5532db70
```

即：权威 id `76d07f13…` ⟶（哈希覆盖）⟶ 文档 `406bd254…` ⟶（内容）⟶ legacy id `c0d218d8…` ⟶（等于）⟶
`current` 的目标。观测用例断言交给 loader 的正是 `c0d218d8…`，不是 `76d07f13…`。

### 4.4 主循环真的进了

`serving_publisher` 与 `paper_constraint_publisher` 各跑一次迭代后停（停止事件在循环自己的
`wait()` 里置位，不是预先置位——预先置位只能证明进了循环、证明不了跑过一步）。回读它们自己发布的
心跳：

```
<runtime root>/control/serving-publishers/svc-<64hex>/heartbeats/<64hex>.json
{"consecutive_failures":1, ... ,
 "last_error":"RuntimeError: signals reader failed: ServingSourceAuthorityUnavailableError: current authority is unavailable",
 "service_id":"serving.publisher.v1","status":"stopped","stop_reason":"loop completed",
 "total_failures":1,"total_successes":0}
```

`paper_constraint_publisher` 同形，`last_error` 是
`RuntimeError: paper constraints require a visible market-minute batch`。
两条都是「新装的根上还没有数据」，不是 `runtime schema service generation is not current`——
用例逐字断言了这一点。

### 4.5 Linux-only 门（在真 Linux 容器里跑过）

`PRODUCTION_ROLE_POLICY` 把 26 个 control root 全部写死在
`/home/lighthouse/rquant/data/runtime/control/...`，角色的 runtime root 就是从这个路径做算术推出来的。
可移植的 15 个用例把这一个前缀搬到临时根（与 stage 侧 `PRODUCTION_RUNTIME_ROOT` 是同一个接缝），
并有一个用例逐项钉死「除这一个 argv 元素外一字不差、且两侧 `runtime_root_from_control_root` 各自正确」。

**Linux-only test id**（mac 上跑不到，`-m linux_exact` 才选中）：

```
tests/integration/test_route_a_legacy_binding_e2e.py::test_the_wrapper_argv_runs_verbatim_against_the_frozen_production_root
```

它把 bundle 直接装到字面量 `/home/lighthouse/rquant/data/runtime`，argv 一字不改地用 wrapper 推出来的。
本机 Docker（`python:3.11-slim`，root）实测：

```
$ docker run --rm -v <repo>:<repo>:ro python:3.11-slim bash /ci.sh
...
running the wrapper preflight
PASSED tests/integration/test_route_a_legacy_binding_e2e.py::test_the_wrapper_argv_runs_verbatim_against_the_frozen_production_root
1 passed, 15 deselected in 14.68s
```

同一容器里把 CI job 的完整命令重放一遍（含 JUnit 契约校验）：

```
39 passed in 197.80s (0:03:17)
CI-CONTRACT-OK
```

另外把三个文件在 Linux 容器里全跑一遍（`-m 'not network'`，即包含 linux_exact）：**117 passed**。

CI 接线：新增 job `route-a-legacy-binding-linux`（3.11 / 3.12 两版），步骤里
`sudo install -d -o $(id -u) /home/lighthouse` 造出那个根，跑完用
`tests/support/assert_junit_contract.py --suites 1 --tests 39 --failures 0 --errors 0 --skipped 0 --cases 39`
把「零 skip」钉死，并 `grep` 确认 JUnit 里出现了 verbatim 那条 test id。

## 5. 变异表（7 条，逐条原文记录）

每条：先 commit 干净 → 改 → 跑定向套件 → `git checkout --` 还原。全部红。

| # | 变异 | 命中的用例与原文 |
|---|---|---|
| **M1** | 去掉 legacy 比对（`run()` 又把 `args.expected_generation` 交给 loader） | 单元 10 failed / 9 passed：`test_t9_5_existing_runtime_root_is_used_for_schema_bindings - AssertionError: assert 'ffffffffffff...fffffffffffff' == 'cccccccccccc...cc...`；`test_r207_schema_bindings_load_against_the_generation_the_pointer_names` 同形；其余 8 条 `Failed: DID NOT RAISE <class 'ValueError'>`。**端到端 4 failed**，四条全是 `ValueError: runtime schema service generation is not current`——即 #207 原始症状被原样复现 |
| **M2** | 去掉 `:292` 的 manifest 路径校验 | 单元 2 failed / 16 passed：`test_t9_4_authority_manifest_must_sit_in_the_expected_generation[<lambda>-generation does not match] - Failed: DID NOT RAISE <class 'ValueError'>`；`test_r207_the_manifest_path_check_still_binds_the_authority_generation` 同。端到端 1 failed：`test_a_manifest_from_another_authority_generation_is_refused - Failed: DID NOT RAISE <class 'ValueError'>`。**副产品**：这一条第一次跑时把端到端用例挂死了——去掉校验后角色不再拒绝，改为按真实 interval 无限循环。用例缺陷，已单独修（`7f9af41`），改用同一套「跑一次迭代就停」的驱动，重跑后 12 秒内变红 |
| **M3** | 去掉新增的交叉核对（`resolve_legacy_schema_generation` 直接返回指针里的 id） | 单元 8 failed / 4 passed，全部 `Failed: DID NOT RAISE <class 'ValueError'>`（缺文档 / bootstrap 代 / 错 runtime root / 指针挪动 / 组写 / 他写 / symlink / 畸形）。端到端 3 failed：`test_a_current_pointer_moved_to_another_generation_is_refused - AssertionError: Regex pattern did not match.`、`test_a_bootstrap_staged_generation_over_a_real_current_is_refused - rquant.runtime_schema_registry.RuntimeSchemaCompatibilityError: runtime sch...`、`test_a_removed_binding_document_is_refused_rather_than_assumed - Failed: DID NOT RAISE`。值得记一笔：bootstrap 那条会被 loader 的 manifest 指纹校验兜住，但**指针挪动那条兜不住**（两代 manifest 相同），正是交叉核对的存在理由 |
| **M4** | `current` symlink 改绝对路径（`_publish_current` 写绝对目标，同时放宽运行期对绝对目标的拒绝） | 端到端 3 errors：`rquant.runtime_authority_publish.RuntimeAuthorityStageError: legacy current pointer is malformed: /Users/.../source/runtime/generations/f4901cbd...` |
| **M5** | 惰性化回退（两个 dashboard 模块改回模块级 `from rquant.config import settings`） | 3 failed：`test_r186_recovery_call_time_modules_import_in_the_wrapper_child_environment - AssertionError: recovery chain modules still die in the child environment:`；`test_r186_the_probed_chain_is_the_one_the_recovery_roles_actually_walk - AssertionError: , input_type=dict]`；`test_r186_no_module_on_the_recovery_chain_reads_settings_at_import - AssertionError: module-level settings import is back:`（原文列出 `dashboard/strategy_lab_runs.py:17` 与 `dashboard/strategy_lab_data.py:20`） |
| **M6** | 只去掉交叉核对里的 runtime root 比对 | 1 failed / 11 passed：`test_r207_a_binding_for_another_runtime_root_is_refused - Failed: DID NOT RAISE <class 'ValueError'>` |
| **M7** | stage 在 legacy 模式下也写 `mode: bootstrap`（等于不记录 legacy generation） | 单元 2 failed / 1 passed：`test_legacy_mode_copies_manifests_verbatim_and_records_the_generation_it_came_from`、`test_legacy_mode_resolves_current_and_pins_the_generation_into_the_authority_id`。端到端 3 failed：`test_the_staged_generation_copies_the_bundle_manifests_and_names_its_generation - AssertionError: assert 'bootstrap' == 'legacy'`，两个角色 `ValueError: runtime generation was staged from the checkout and cannot bind...` |

## 6. 两版本数字

定向套件（13 个文件：新增 2 个 + 受影响的 11 个）：

| Python | 结果 | 耗时 |
|---|---|---|
| 3.11（`.venv/bin/python -m pytest`） | **604 passed, 1 deselected** | 383.96s |
| 3.12（`.venv312/bin/python -m pytest`） | **604 passed, 1 deselected** | 395.94s |

`1 deselected` 就是 §4.5 那条 `linux_exact` 用例（默认 `addopts` 排除），它已在 Linux 容器里单独跑绿。

Linux 容器（`python:3.11-slim`，uv 0.10.11，`uv sync --frozen`）：

* `-m 'not network'` 跑 `test_route_a_legacy_binding_e2e.py` + `test_runtime_legacy_generation_binding.py`
  + `test_tp9_role_child_runtime.py`：**117 passed in 212.71s**
* CI job 命令重放（两个文件 + JUnit 契约）：**39 passed in 197.80s**，`CI-CONTRACT-OK`

lint：`ruff check` 只查改动文件，全部 `All checks passed!`。

## 7. #186 / #188 的一处偏离（已实现，请复核）

issue 里给的链是四层，最深一层是 `dashboard/strategy_lab_runs.py:17`。按 issue 只改这一处**修不好**：
实测（wrapper 白名单环境 `-I -S` + 只有 `LANG`/`LC_ALL`/`TZ`）`rquant.runtime_recovery_coordinator`
仍旧 `ValidationError`。原因是它模块级的 `_build_strategy_replay_executable_fingerprint()` 还会导入
`rquant.dashboard.strategy_lab_data`，那个模块也在 import 期读四个 settings 路径。两处都按
`page_control_service` 的写法改成函数内 `get_settings()` + PEP 562 `__getattr__` 之后，
`runtime_recovery_backup` / `runtime_recovery_coordinator` / `formal_smoke_replay` /
`dashboard.strategy_lab_data` / `dashboard.strategy_lab_runs` / `runtime_recovery_production` 六个模块
在子环境里全部 import 成功。

探针不是 grep import 语句，而是在子进程里 import `runtime_recovery_backup` 之后把 `sys.modules` 里所有
`rquant.*` 读回来做断言——以后再多一条边，同一条用例就会覆盖到，而不是悄悄变窄。

## 8. 遗留 / 交接

1. **`tests/manifests/full-suite-v1` 需要重生成**：新增 40 个用例（e2e 16 + 新单元 23 + tp9 新增等），
   `index.json` 的 13653 会对不上。按简报要求本轮不做，集成阶段与包 B 一起重生成。
2. **包 C 必须用带本改动的代码重新 stage + publish**：装完 bundle 后 `runtime-authority-stage
   --legacy-runtime-root <root> --legacy-generation current`，dry-run 时可以在
   `plan.json` 的 `staged_files["generation/legacy-binding.json"]` 上核对摘要，apply 后可以直接
   `cat <staging>/generation/legacy-binding.json` 核对里面记的 legacy generation 是否等于
   `readlink <root>/current` 的目标。
3. **不要只挪 `current`**：见 §3 第 2 条，运维含义要写进 DEPLOY.md 的路线 A 段落。
4. **新增 CI job `route-a-legacy-binding-linux`** 依赖 `sudo install -d /home/lighthouse`；如果 owner
   不接受在 runner 上造这个目录，去掉该 job 也不影响其余 15 个用例（它们进四个 shard），但 verbatim
   那条就只剩本地 Docker 证据。
5. **`CHANGELOG.md` 未动**：包 A 与包 B 并行、要求文件不重叠，改它必冲突。合 main 前由集成阶段
   在 `[Unreleased]` 里补 `Fixed`（#207 / #186 / #188）与 `Added`（`legacy-binding.json`）两条。
6. 私有测试根 `/Users/roxor/rq-raa-a`（含 basetemp、Docker 脚本、变异原文）已在收尾时删除；本报告
   §4 的产物证据是从那次运行里 dump 出来的原文。
