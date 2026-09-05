# 包 A 独立审查报告（#207 / #186 / #188）

**裁定：`PKGA-REVIEW-APPROVED`**（0 条 must-fix；7 条 should-fix，均不阻塞合并；若集成阶段不处理 S-1、S-2，请在合 main 的 commit message 里注明「已知并接受」）

- 被审对象：worktree `/Users/roxor/brain/30-projects/rQuant/.worktrees/ra-a-cc`，分支 `cc/20260905-route-a-binding`，
  base `2db3846`，5 commit `3f1af9d` → `0f48b83` → `877d17a` → `7f9af41` → `7ae0aa5`。
- 审查标准：`route-a-pkgA-brief.md` 改法 1–4 与边界、`route-a-rulings.md` 裁决 1（方向一）、
  `gh issue view 207 186 188 -R roxorlt/rquant`、`route-a-scope.md` §4.1/§4.2。
- 审查方式：**全部结论自己复现**。本机 7 条变异逐条重跑、5 条自写 pytest 探针、
  两版本定向套件（683 用例）重跑、recovery 闭包 155 个模块在 wrapper 子环境下逐个 import、
  两个 root 制品在 base 与 HEAD 上各自构建后哈希对拍、
  Linux 容器（`python:3.11-slim`，**非 root uid 1001**，对齐 GitHub runner）重放整个新 CI job。
  审查过程未改任何代码、未 commit、未读写 `.env`；变异一律先确认已提交再改、跑完 `git checkout --` 还原；
  私有根 `/Users/roxor/rq-raar-rev`（已删除），全程 `git status` 只有未跟踪的 `.superpowers/`。
- 一句话结论：**改法与裁决一致，验收是真的，安全性比报告自己说的更站得住 ——
  但报告为交叉核对给出的那条理由是错的（S-2），正确的理由我另外证出来了（§4.1）。**

---

## 1. 逐条核读

### 1.1 #207 修法与裁决一致，且没有放宽任何现有断言 —— 通过

| 裁决要求 | 落点 | 我的核验 |
|---|---|---|
| 用 `<root>/current` 解析出的 legacy generation id 调 binding | `runtime_service_main.py:200` `legacy_current_generation()` 返回 id；`:670` 把它交给 `resolve_legacy_schema_generation()`；`:711` `generation_id=schema_generation` | 变异 M1（改回 `args.expected_generation`）单元 10 红、端到端 **10 红**，其中 7 条原文就是 `ValueError: runtime schema service generation is not current` —— #207 的原始症状在真 bundle 上被原样复现 |
| `:292` 的 manifest 路径校验原样保留 | 抽成 `_authority_generation_directory()`（`:319`），两条断言逐字未改（`:329` / `:331`），`load_authority_service_manifest` 与 `resolve_legacy_schema_generation` 各调一次 | 逐字比对 diff：两行字符级相同，只是位置移动；变异 M2（删掉这两条）单元 3 红、端到端 `test_a_manifest_from_another_authority_generation_is_refused` 12.78 秒变红 |
| 新增显式交叉核对 | `resolve_legacy_schema_generation()`（`:335`）读 `<generation>/legacy-binding.json`，依次要求 `mode == "legacy"`（`:382`）、`runtime_root` 等于本角色推出的根（`:387`）、`generation_id` 等于 `<root>/current` 当前解析到的 id（`:389`） | 变异 M3（直接返回指针 id）单元 8 红、端到端 3 红；变异 M6（只删 runtime root 比对）1 红 |
| 不得放宽现有断言 | 只做了「加」：`_read_authority_manifest` → `_read_authority_document(label=...)`，label 默认 `"runtime service manifest"`，manifest 路径的报错文本一字未变 | 逐行读 diff 确认；两版本 683 用例全绿，其中含 `test_t9_4_*`、`test_runtime_exec_wrapper` 等既有断言 |

**放哪里的判断也核过**：`plan.json` 的字段集被 `runtime_authority_publish._PLAN_FIELDS` 冻死、
generation full manifest 字段被 `runtime_authority._GENERATION_MANIFEST_FIELDS` 冻死，两者都在「不动」清单里；
落成 generation 根下一份 **被 full manifest entries 覆盖的文件** 确实比加字段更强 ——
我在真产物上确认过 `legacy-binding.json` 出现在 `full-manifest.json` 的 `entries` 里，`mode` = `0o444`（十进制 292），
`owner_uid` = 运行者 uid（生产上是 root，`_verify.OWNER_UID = 0`）。

### 1.2 #186 / #188 —— 通过，且比 issue 给的链更完整

- 两处惰性化（`dashboard/strategy_lab_runs.py`、`dashboard/strategy_lab_data.py`）写法与 PA-1 的
  `page_control_service` 一致：函数内 `get_settings()` + PEP 562 `__getattr__`，`globals().get("settings")`
  让测试绑定的假 settings 仍然优先。
- **探针有区分力**：变异 M5（两个模块改回模块级 `from rquant.config import settings`）→
  `test_r186_recovery_call_time_modules_import_in_the_wrapper_child_environment`、
  `test_r186_the_probed_chain_is_the_one_the_recovery_roles_actually_walk`、
  `test_r186_no_module_on_the_recovery_chain_reads_settings_at_import`、
  以及 `test_t9_7_lazified_modules_still_expose_the_settings_attribute` 两个新参数，共 5 条红。
- **我自己在 wrapper 白名单环境下重跑了一遍**（`env -i LANG=C LC_ALL=C TZ=UTC`，解释器 `-I -S`，
  `sys.path` 只有 generation 的 `src` 与 site-packages）：
  `runtime_recovery_backup` / `runtime_recovery_coordinator` / `runtime_recovery_production` /
  `runtime_recovery_service` / `formal_smoke_replay` / `dashboard.strategy_lab_runs` /
  `dashboard.strategy_lab_data` 七个模块全部 `OK`。
- **有没有别的遗漏**：我把 `import rquant.runtime_recovery_backup` 之后 `sys.modules` 里的
  **全部 155 个 `rquant.*` 模块**逐个在同一环境下单独 import 了一遍，**0 失败**；
  并确认 `rquant.config` 在整条链走完后仍未构造过 `Settings` 实例。issue 里的四层链是低估的，
  报告 §7 说的「实际是六个模块」属实。

### 1.3 端到端用例 —— 通过，是真跑，不是打桩

我按简报要求逐项验证「路径上没有打桩」：

- `install_runtime_deployment_profile` 是真的（真 profile、真 bundle、26 份 manifest，
  `current -> generations/<64hex>` 是真的相对 symlink，`deployment-profile.json` 真的在）；
- stage 是真的（`legacy_services` 逐字节拷 manifest，`legacy_generation_binding` 写真文档）；
- publish 是真的（root-owned 权威链、`wrapper_preflight` 真跑）；
- argv 由 **wrapper 自己的 `_verify.resolve_launch`** 推出（意味着 full manifest 已经对过 slot 哈希、
  每条 entry 已经在磁盘上核过 owner/mode/nlink/sha256）；
- `runtime_service_main.run()` 用这份 argv 真跑，`load_runtime_schema_service_bindings` 是**真的那一个**
  （观测用例 `observe()` 包住真函数再调，不是替掉）。

**两个角色的 `last_error` 确实是「新根无数据」不是 `not current`**：
`test_the_iteration_the_loop_ran_never_reports_the_binding_failure` 逐字断言 heartbeat 里
`total_failures + total_successes == 1`、`stop_reason == "loop completed"`、文本里不含
`runtime schema service generation is not current`，`last_error` 只能是
`ServingSourceAuthorityUnavailableError: current authority is unavailable` 或
`paper constraints require a visible market-minute batch`。两版本各跑一次全绿。

反向用例三条（挪 current / manifest 放错 generation / 交叉核对字段被改）都在，
且我用 M3、M2、M7 分别让它们变红过。

**M1 复跑**：去掉 legacy 比对后端到端 **10 failed / 5 passed**，其中 7 条 `not current` —— 报告 §5 写的「4 failed」偏低（见 N-3）。
**M3 复跑**：3 failed，与报告一致；但**报告对这条的解读需要更正**，见 §3 S-2 与 §4 安全结论。

### 1.4 新增 CI job `route-a-legacy-binding-linux` —— 可行，但有一处浪费

- `sudo install -d -m 0755 -o "$(id -u)" -g "$(id -g)" /home/lighthouse` 在 GitHub `ubuntu-24.04` runner 上可行
  （runner 有免密 sudo；`/home/lighthouse` 不存在）。用例自身还有 `if PRODUCTION_ROOT.exists(): raise AssertionError`
  的防护，不会误伤真机。
- 零-skip 契约是真的：`assert_junit_contract.py --suites 1 --tests 39 --failures 0 --errors 0 --skipped 0 --cases 39`
  逐项比对 JUnit 属性，`--skipped 0` 使得「Linux-only 那条被 skip 掉」立刻红；再加一条 `grep` 钉死那条 test id 出现过。
- 39 = e2e 16（15 可移植 + 1 `linux_exact`）+ binding 单元 23，我核对过收集数。
- **我在 GitHub runner 的真实身份下重放过整个 job**（实现者只在容器里以 root 跑过）：
  `python:3.11-slim` 容器，建 uid 1001 非 root 用户，
  `install -d -m 0755 -o 1001 /home/lighthouse`，`uv sync --frozen`，
  再原样执行 job 的 pytest 与 `assert_junit_contract.py` 命令：

  ```
  RUNNING-AS: uid=1001(ci) gid=1001(ci) groups=1001(ci)
  ======================== 39 passed in 204.51s (0:03:24) ========================
  CI-CONTRACT-OK
  ```

  **前提是 umask 022**（GitHub runner 的默认值）。umask 002 下会红，见 §3 S-6 —— 不影响 GitHub，但要知道。
- **会和现有分片重复**：见 §3 S-1。
- 另外发现 Linux-only 那条在同一台机器上**跑第二遍必红**，见 §3 S-7。GitHub 每个 job 一台全新 VM，不受影响。

### 1.5 变异表与两版本数字 —— 复现通过（细节有出入）

我逐条重跑了 7 条变异，**全部红**：

| # | 变异 | 我的复跑结果（原文摘要） | 与报告 |
|---|---|---|---|
| M1 | `run()` 改回把 `args.expected_generation` 交给 loader | 单元 **10 failed / 9 passed**；端到端 **10 failed / 5 passed**，7 条 `ValueError: runtime schema service generation is not current` | 单元一致；端到端报告写 4，实测 10 |
| M2 | 删掉 `_authority_generation_directory` 的两条校验 | 单元 3 failed（`test_t9_4_*` ×2、`test_r207_the_manifest_path_check_still_binds_the_authority_generation`）；端到端 `test_a_manifest_from_another_authority_generation_is_refused` **12.78s 变红，没有挂死** | 一致（我多删一条断言，故 3 而非 2）；`7f9af41` 的修确实生效 |
| M3 | `resolve_legacy_schema_generation` 直接返回指针 id | 单元 **8 failed / 11 passed**（缺文档 / bootstrap 代 / 错 root / 指针挪动 / 组写 / 他写 / symlink / 畸形）；端到端 3 failed | 数字一致，**解读需更正**（S-2） |
| M4 | `_publish_current` 写绝对目标 + 运行期容忍绝对目标 | 端到端 3 errors，**比报告更早被拦**：`ValueError: runtime replacement pointer escapes the generation directory`（`runtime_deployment_bundle.py:2633`），根本没走到 stage | 更强 |
| M5 | 两个 dashboard 模块改回模块级 settings | 5 failed（3 条 R186 + 2 条 T9-7 新参数） | 一致 |
| M6 | 只删交叉核对里的 runtime root 比对 | **1 failed / 11 passed**：`test_r207_a_binding_for_another_runtime_root_is_refused - Failed: DID NOT RAISE` | 一致 |
| M7 | stage 在 legacy 模式下也写 `mode: bootstrap` | 单元 2 failed；端到端 3 failed（`assert 'bootstrap' == 'legacy'`，两个角色 `ValueError: runtime generation was staged from the checkout and cannot bind...`） | 一致 |

**两版本数字**：报告写的「13 个文件 604 passed」我没能还原出同一份文件清单，
于是自己取了一个 **13 文件的超集（683 个用例）** 重跑：

| Python | 结果 | 耗时 |
|---|---|---|
| 3.11（`.venv/bin/python`） | **683 passed, 1 deselected** | 397.28s |
| 3.12（`.venv312/bin/python`） | **683 passed, 1 deselected** | 387.82s |

（文件清单：新增的 2 个 + `test_runtime_authority_publish` / `test_tp9_role_child_runtime` /
`test_runtime_service_main` / `test_runtime_authority` / `test_runtime_exec_wrapper` /
`test_strategy_lab_runs` / `test_strategy_lab_data` / `test_runtime_recovery_backup` /
`test_runtime_recovery_coordinator` / `test_formal_smoke_replay` / `test_logging_lazy_settings`。
`1 deselected` 就是那条 `linux_exact`。）

**ruff**：9 个改动文件 `All checks passed!`（ruff 0.15.10）。
**git status**：只有未跟踪的 `.superpowers/`，无 `.env`。
**改动面无越界**：`git diff --name-only 2db3846..HEAD` 只有 10 个文件，
`deploy/`、`.env`、`runtime_authority.py`、`runtime_authority_publish.py` 一个没碰；
stage 的 bootstrap **manifest 推导路径**（`derive_bootstrap_services`）一行未改；
builder 的 plane/settings 断言一行未改；新增 skip 只有那一条平台守卫 `skipif`，无 xfail。

### 1.6 报告自报的 M2 副产品，是否削弱「真跑主循环」的证据力 —— 不削弱，反而是加强

我把三个 commit 的先后关系查清楚了：

- `_StopAfterOneIteration` 在**第一个验收 commit `877d17a` 里就已经是现在这样**，不是 M2 之后才改的；
- `7f9af41` 只动了**一行**，把**反向用例** `test_a_manifest_from_another_authority_generation_is_refused`
  从裸 `service_main.run(...)` 换成同一套 `run_role()` 驱动。正向验收用例从头到尾没被动过。

而这套驱动本身比「预先置位」强：我读了 `runtime_service_control.run_service_loop`（`:449-465`），
循环是 `while not stop_event.is_set(): step(); ... stop_event.wait(interval)`，
而 `_StopAfterOneIteration.is_set()` 首次返回 False、`wait()` 被调用时才置位并计数，
`run_role()` 再 `assert stop.iterations == 1`。所以「真的执行了一步 step，且只执行了一步」是被钉死的，
heartbeat 里 `total_failures + total_successes == 1` 也从产物侧印证了同一件事。

**唯一的问题是文档**：`test_a_kind_backed_role_reaches_its_service_loop_over_a_real_current` 的 docstring
仍然写着「The stop event is pre-set, so the loop is entered and left without executing a step」，
与实际机制矛盾，而且**低估**了自己的证据力。见 §3 S-3。

---

## 2. must-fix

**无。** 简报改法 1–4 与边界、裁决 1 的每一项，我都能自己复现出满足的证据。

---

## 3. should-fix（不阻塞合并，建议集成阶段一并处理）

### S-1 · 新 CI job 与四个分片重复跑同一批 38 个用例

`route-a-legacy-binding-linux` 用 `-m 'not network'` 跑**整两个文件**（39 条）。
`full-suite-shard` 也是 `runs-on: ubuntu-24.04`、同样 3.11/3.12 两版，
重生成 manifest 之后这 38 条可移植用例会**进分片再跑一遍**，环境完全相同，覆盖零增量。
仓库现有约定是反过来的：`formal-smoke-real-generation-linux` 用 `-m linux_exact`，
只跑分片按 `addopts` 排除掉的那部分（我核过 `test_formal_smoke_real_generation_linux_e2e`
在四个 shard-*.jsonl 里出现 **0 次**）。
（`paper-sqlite-image` 确有重复三条 nodeid 的先例，但它是为了换一个 sqlite 镜像环境跑，本 job 没有环境差异。）

建议改成 `-m linux_exact` 只跑 `tests/integration/test_route_a_legacy_binding_e2e.py`，
契约相应改为 `--tests 1 --cases 1 --skipped 0 --failures 0 --errors 0`
（`--skipped 0` 依然把「被 skip 掉」钉死；`--tests 1` 还顺带把「有人误给可移植用例打上 `linux_exact`」钉死）。
按实测耗时估算，每轮 CI 可省约 2 × 3.3min × 2 = 13 分钟 runner 时间
（考虑到 8/27 之前打满过免费额度的历史，值得省）。

### S-2 · 报告 §5 M3 对「指针挪动」的解读与事实不符，且这条恰好是安全结论的承重点

报告写「bootstrap 那条会被 loader 的 manifest 指纹校验兜住，但**指针挪动那条兜不住**（两代 manifest 相同），
正是交叉核对的存在理由」。我复跑 M3 拿到的原文是：

```
E       AssertionError: Regex pattern did not match.
E         Expected regex: 'does not match the current pointer'
E         Actual message: 'runtime schema generation hash mismatch'
```

也就是说，**端到端用例里那个「另一代」是 `shutil.copytree` 复制出来的**，
它的 `generation-basis.json` 内容哈希不等于新目录名，被
`runtime_deployment_bundle.py:1968` 的 `canonical_sha256(basis) != generation_id` 拦住了。
用例本身没问题（它断言的是**具体那条错误消息**，交叉核对一删就红，仍是有效回归测试），
**问题在于「交叉核对是唯一防线」这个结论没有被这条用例证明。**

我另外补了三条探针，用**两次真实安装**造出真正的兄弟代（见 §4 安全结论），
结论是：交叉核对**确实**是唯一防线，但理由不是报告说的那个。
建议：①把报告 §5 M3 与 `runtime_legacy_generation_binding.py` 模块 docstring 里这句话改对；
②把「两次真实安装 → 兄弟代 → 挪 current」这条补进 `test_route_a_legacy_binding_e2e.py`
（我的探针代码在 §6，可直接搬）。

### S-3 · 一条正向验收用例的 docstring 描述的是已经不存在的机制

`tests/integration/test_route_a_legacy_binding_e2e.py`
`test_a_kind_backed_role_reaches_its_service_loop_over_a_real_current` 的 docstring：
「The stop event is pre-set, so the loop is entered and left without executing a step —
what is under test is the binding, and a step failure would say nothing about it either way.」
实际用的是 `_StopAfterOneIteration`（真跑一步）。这句话正好落在「主循环证据力」这个被点名的问题上，
读的人会据此低估证据。改成一句话说清「跑满一次迭代」即可。

同类：报告 §4.5 还写着「与 stage 侧 `PRODUCTION_RUNTIME_ROOT` 是同一个接缝」，
但 `7ae0aa5` 已经把这个接缝换成 `_relocate`，报告未同步。

### S-4 · `build_stage_plan` 在一次 staging 里把 `<root>/current` 解析了两次

`collect_services(options)` → `legacy_services()` 里 readlink 一次（决定拷哪一代的 manifest），
`legacy_generation_binding(options)` 里又 readlink 一次（决定文档记哪一代）。
两次之间若指针被移动，文档记的代与实际拷的 manifest 会来自不同代 —— 之后角色反而会正常启动。
触发条件是「staging 运行期间有人（root/lighthouse）并发挪 `current`」，概率极低，但修起来很便宜：
`legacy_generation_directory()` 只解析一次，把 `directory.name` 同时传给两处。

### S-6 · 端到端用例对 `last_error` 用了精确字符串白名单，在 umask 002 的 Linux 上假红

`test_the_iteration_the_loop_ran_never_reports_the_binding_failure` 最后一条断言是
`heartbeat["last_error"] in {None, "...ServingSourceAuthorityUnavailableError: current authority is unavailable", "...paper constraints require a visible market-minute batch"}`。
我在 Linux 容器里以 umask **002** 跑（Debian 的 `su` 因为 `USERGROUPS_ENAB` 会给私有组用户 002），
`serving_publisher` 这一条**必红**（连跑三遍，三遍都红）：

```
E  AssertionError: assert 'RuntimeError: signals reader failed:
   ServingSourceAuthorityIntegrityError: authority directory identity is unsafe' in {...}
```

根因不在包 A：`runtime_serving_authority._is_trusted_path_node`（`:1503-1506`）要求路径节点没有
group/other 写位，umask 002 下运行期新建的目录是 0775，于是 serving 那一步换了个错误。
换成 umask 022 或 077 同一条**双双通过**（我各验了一遍），full job 在 umask 022 下 39 passed。

这条断言想说的是「**不是 #207 那句**」，而它上面两行的
`assert NOT_CURRENT not in text` 与 `assert "schema service generation" not in text` 已经把这件事说完了。
精确白名单只是给自己加了一个与环境耦合的失败面。建议改成「`last_error` 为 `None` 或不含
`schema service generation`」，或者至少把 `ServingSourceAuthorityIntegrityError` 也纳入允许集。

### S-7 · Linux-only 那条在同一台机器上跑第二遍必红（清理不彻底 + 守卫范围过窄）

`test_the_wrapper_argv_runs_verbatim_against_the_frozen_production_root` 的 `finally` 里是
`shutil.rmtree(Path("/home/lighthouse/rquant"), ignore_errors=True)`。我在容器里连跑两遍：

```
--- verbatim run 1 ---   1 passed, 15 deselected in 15.30s
leftover: drwxr-xr-x 3 ci ci 4096 /home/lighthouse/rquant     ← 没删掉
--- verbatim run 2 ---   FAILED ... DefinitionConflictError: logical id and version already
                         contain different content
```

两个问题叠在一起：①`ignore_errors=True` 把删不掉这件事吞了；
②守卫 `if PRODUCTION_ROOT.exists()` 看的是 `/home/lighthouse/rquant/data/runtime`，
但用例真正会创建并删除的是整棵 `/home/lighthouse/rquant`，残留在 `data/runtime` 之外的东西
（definition registry 之类）不会触发守卫，只会在下一次跑时以另一种错误炸掉。

**对 GitHub CI 无影响**（每个 job 一台全新 VM，只跑一次），
**对生产机也安全**（那台机上 `data/runtime` 一定存在，守卫会拦住）。
受影响的是自托管 runner 与 Linux 开发机：跑第二遍就红，且要手工清理才能恢复。
建议把守卫改成看 `/home/lighthouse/rquant`（它真正会删的那棵树），
清理改成先 `chmod -R u+rwX` 再 `rmtree(..., ignore_errors=False)`。

### S-5 · 缺文档的报错措辞会把运维带偏

老 generation（本改动之前 staged 的）没有 `legacy-binding.json`，
角色报的是 `runtime legacy generation binding is unavailable or contains a symlink`
（沿用 manifest reader 的措辞）。包 C 现场看到这句会去找 symlink，实际原因是「这一代太老，得重新 stage + publish」。
建议在 `_read_authority_document` 里给这个 label 单独一条更直白的消息，或在 runbook 里预先写上这句原文与含义。
（我已经确认这条路径**是拒绝不是放行**，见 §4。）

---

## 4. 安全结论（可引用）

### 4.0 改后的信任链（我逐层核过，每层都指出了执行者）

```
/var/lib/rquant/runtime-authority/current.json 的 slot            ← root 0444，wrapper 读
  └─ generation_id = sha256(<generation>/full-manifest.json)      ← _verify.load_generation_manifest 对哈希
       └─ full-manifest.json 的 entries 覆盖 legacy-binding.json  ← _verify.verify_code_identity 逐条核
            ·  owner_uid（生产 = 0）、mode（0o444 = 292）、nlink、sha256、非 symlink、非目录
       └─ legacy-binding.json 记的 legacy generation
            └─ 必须 == <runtime root>/current 当前解析到的 id     ← runtime_service_main.py:389（新增）
            └─ 必须 == 本角色由 control root 推出的 runtime root  ← runtime_service_main.py:387（新增）
            └─ mode 必须是 "legacy"，bootstrap 代直接拒            ← runtime_service_main.py:382（新增）
  └─ manifest 必须坐落在名字 == --expected-generation 的目录里     ← runtime_service_main.py:331（原样保留）
```

wrapper 与子进程 bootstrap **各独立跑一遍 `resolve_launch`**，所以这份文档在角色进程存在之前已被核过两次。

### 4.1 问题①：有人（攻击者或运维失误）把 `<root>/current` 挪到另一代（两代 manifest 相同），谁拒、拒在哪一行

**拒绝者：角色自己，`src/rquant/runtime_service_main.py:389`**

```python
if binding.generation_id != legacy_generation:
    raise ValueError("runtime legacy generation binding does not match the current pointer")
```

**而且它确实是唯一的防线。** 这一条我没有采信报告的说法，自己造了真产物来证：

1. **兄弟代真的能存在**（探针 `test_probe_a_second_real_install_is_a_sibling_generation_with_the_same_manifests`）：
   在同一个 runtime root 上跑**第二次真实 `install_runtime_deployment_profile`**（只轮换一个通知凭证值，
   属于普通重新部署），得到第二个 generation。两代的 `manifests/` **逐字节完全相同**
   （generation id 由 `generation-basis.json` 的内容哈希决定，basis 里除 `manifest_sha256` 外
   还有 `capability_sha256` / `schema_bootstrap_sha256`，所以凭证轮换或换一次 bootstrap 理由
   就足以造出「manifest 相同、id 不同」的兄弟代）。第二代**能通过 `_load_generation_schema_bundle` 的全部自检**
   （basis 哈希 == 自己的目录名、schema 契约哈希吻合、producer_commit 吻合）。
2. **交叉核对在，就拒**（探针 `test_probe_the_cross_check_is_the_only_thing_that_refuses_the_sibling`）：
   权威链按第一代 stage + publish，随后 `current` 指向第二代，`serving_publisher` 起不来，
   原文 `ValueError: runtime legacy generation binding does not match the current pointer`。
3. **交叉核对不在，就放行**（探针 `test_probe_the_loader_alone_accepts_the_sibling`）：
   同一个世界，仅把 `resolve_legacy_schema_generation` 换成「直接返回指针里的 id」，
   角色 `run()` 返回 0，正常进主循环，`load_runtime_schema_service_bindings` 收到的是**第二代**的 id。
   `_current_target(root) == generations/<id>` 通过（因为传的就是指针自己的 id），
   `manifest_fingerprints[service_id] == manifest.manifest_fingerprint` 也通过（两代 manifest 相同）。

**结论可引用**：Route A 之后，`data/runtime/current` 是 lighthouse 可写的；
把它挪到另一个合法安装出来的兄弟代，在这次改动**之前**（以及在任何只传 legacy id、不做交叉核对的写法下）
是**静默接受**的——角色会拿一份与自己 manifest 无关的 bundle 去装 schema bindings。
改动之后是**响亮拒绝**，拒绝点是 `runtime_service_main.py:389`，一行。

### 4.2 问题②：bootstrap 与 legacy 两种模式不再产生同一个 generation id，有没有引入回归；**向后兼容判定**

**（a）「不再同 id」本身无回归。** 全仓库只有一处依赖过这条等式
（`test_legacy_mode_copies_manifests_verbatim_and_yields_the_same_generation` 的那句 `==`），
已按新语义重写成 `!=`，同时补上 `profile_id` 相等的断言。我 grep 过 `src/` 与 `tests/`，没有第二处。
`profile_id` 由解释器闭包与实例标签算出、不含这份文档，实测两次 staging 的 profile 相同 ——
**#190 不被路线 A 触发**，与包 0 判定一致。

**（b）向后兼容判定：包 C 第 10 步之前，两个在跑的 serving unit 会不会被「新 runtime-exec」打死？**

**结论：不会——因为根本不存在「新 runtime-exec」。** 三条实证：

1. **两个 root 制品的哈希，base 与 HEAD 逐字节相同**（我用两个 builder 各构建了两次）：

   ```
   rquant-runtime-exec.pyz       a5d9b3fff7388f7aa35a951a6b6bc51e3e9faf69bf8b94b598c7c69b2c9c9c5e   (base == HEAD)
   rquant-production-deploy.pyz  a41db437091f1786e10ed0cb01603c6822b6cff2199f20006613869c0c2b6757   (base == HEAD)
   ```

   `build-runtime-exec-pyz.py` 只打包 `src/rquant/runtime_exec_wrapper/**/*.py`；
   `build-production-deploy-pyz.py` 只打包 `strict_json.py` / `runtime_authority.py` /
   `runtime_authority_publish.py` / wrapper 两个文件。**包 A 一个都没改**。
2. **角色代码是随 generation 冻结的，不是从 checkout 读的**：
   `CHECKOUT_SOURCE_PATHS = ("src/rquant", "scripts/strict_json.py")`，
   整棵 `src/rquant`（含 `runtime_service_main.py`）被镜像进 generation；
   `_verify.resolve_launch` 强制 `python_path` / `working_directory` / `app_source` / 每个 site-packages 根
   都必须在 `generation_path + "/"` 之下。所以 sequence 2 的角色**跑的永远是 sequence 2 里那份旧代码**，
   新代码不可能作用到它身上。
3. **真正会打死它们的是「`data/runtime/current` 出现」这件事本身，而且这是旧 bug 不是新代码**：
   我复跑 M1（把 `run()` 退回旧写法）在真 bundle 上得到端到端 10 红、7 条
   `ValueError: runtime schema service generation is not current` —— 这正是 sequence 2 的旧代码
   在包 C 第 8 步之后重启时会遇到的东西。

   **对包 C 的直接含义（请修正简报第 2 步）**：
   - 「换 runtime-exec」这个动作**不需要做**（哈希没变），因此也不存在「换 runtime-exec 会不会打死在跑的服务」这个问题；
   - 需要提前停两个 serving unit 的**真正触发点是第 8 步（`deployment-profile --apply` 写出 `current`）**，
     不是第 10 步。第 8 步之后、第 10 步 publish 之前，这两个 unit 一旦因为任何原因重启，
     都会以 `not current` 起不来（旧代码）；publish 之后若还没重 stage，则会以
     `runtime legacy generation binding is unavailable...` 起不来（新代码）。
     所以稳妥顺序是：**第 8 步之前先停**，第 10 步 publish 之后再按 C-2 探路启动。

**（c）新代码对没有 `legacy-binding.json` 的旧 generation：是拒绝，不是按 bootstrap 降级。**
我自己造了这个形状验证（探针 `test_probe_a_generation_staged_before_the_document_existed_fails_closed`）：
把已发布 generation 里的文档连同它在 `full-manifest.json` 里的 entry 一起删掉
（这正是「本改动之前 staged 的一代」的形状，wrapper 因此不会报缺文件），
角色报 `ValueError: runtime legacy generation binding is unavailable or contains a symlink`。
拒绝不放行 —— 这也回答了问题③。

需要说清的边界：这条拒绝**只在 `<root>/current` 存在时才会走到**。
现役 sequence 2 今天跑的是 Route B（无 `current`），走降级分支，压根不读这份文档，**不受影响**。

### 4.3 问题③：缺文档是拒绝而非放行 —— 有两层证据

- 仓库自带两条用例：`test_r207_a_generation_without_the_binding_document_is_refused`（单元）、
  `test_a_removed_binding_document_is_refused_rather_than_assumed`（端到端）。
  变异 M3 让它们双双 `DID NOT RAISE`，证明它们真的在测这件事。
- 我自己的探针（见 4.2c）在**真发布的权威链**上把文档与它的 manifest entry 一起摘掉，仍然拒绝。

另外 stage 侧也不给「省略文档」留口子：Route B 明确写 `mode: "bootstrap"` 而不是不写文件，
所以「文件不在」只可能意味着「这一代是本改动之前 staged 的」，语义唯一。

### 4.4 问题④：`legacy-binding.json` 的 mode / 属主，wrapper exec 前逐条核对是否覆盖它 —— 覆盖，我另外补测了 mode

- `_verify.verify_code_identity`（`_verify.py:533-568`）对 full manifest 的**每一条 entry** 核：
  存在、非 symlink、是普通文件、`nlink`、非 import 逃逸的 basename、`sha256` 与 `size`、
  `st_uid == entry["owner_uid"] == expected_owner_uid`（生产 = 0）、`S_IMODE == entry["mode"]`。
  `legacy-binding.json` 是 manifest entry，所以全都适用。
- 仓库自带的 `test_the_wrapper_verifies_the_binding_document_as_part_of_the_code_identity`
  只测了**内容篡改**（"manifested generation node changed"）。
  我补了一条探针 `test_probe_the_wrapper_refuses_a_mode_change_on_the_binding_document`：
  内容一个字节不改、只把 0444 改成 0644，`resolve_launch` 报
  `RuntimeExecError: a manifested generation node mode changed: legacy-binding.json`，**通过**。
  同一条探针里还断言了 entry 的 `mode == 0o444`、`owner_uid == 期望 uid`。
- 角色侧 `_read_authority_document` 另有一道自查：必须是普通文件、`st_uid ∈ {0, geteuid()}`、
  不能对 group/other 可写、路径逐段 `O_NOFOLLOW`。单元里有 0o464 / 0o446 / symlink 三条用例，M3 让它们全红。

### 4.5 三条需要 owner 知道的新信任前提（这一段就是「点名」）

1. **「这一代 schema 是当前代」的判据，一半来自 staging 时刻的观察。**
   `legacy-binding.json` 记的是 staging 那一刻 `readlink current` 的结果，被冻进不可变 generation；
   另一半是角色启动时刻的指针。两者必须一致。
2. **换 legacy 代必须重新 stage + publish 权威链。** 只把 `data/runtime/current` 切到新 legacy 代、
   不换权威 generation，全部 kind-backed 角色会拒绝启动，报
   `runtime legacy generation binding does not match the current pointer`。这是刻意的，不是回归。
3. **（note，非本次引入）指针被「弄坏」而不是「挪走」，是静默降级不是拒绝。**
   `<root>/current` 是 lighthouse 可写的。若它被换成绝对目标 symlink、悬空 symlink、
   普通文件，或干脆删掉，`legacy_current_generation()` 返回 `None`，
   22 个 kind-backed 角色里的 21 个会**走降级分支**（只打一条 WARNING 日志，关掉 schema dual write
   与 artifact terminal lifecycle），只有 `strategy_live` 会硬失败。
   这是 base 就有的语义（`test_t9_6_a_root_without_a_usable_current_still_degrades` 三个形状），
   本次未改；但路线 A 之前这条路走不到，之后它就是可达的。
   若 owner 认为「盘中被静默降级」不可接受，可以另开 issue 让降级分支在
   「根目录存在但指针形状不对」时也硬失败（现在是「指针不存在」与「指针坏了」同等对待）。
   写侧倒是拦得住：`_replace_current` 自己拒绝绝对目标（M4 实测原文
   `runtime replacement pointer escapes the generation directory`），所以坏指针只能是人手造的。

---

## 5. note（不要求改）

- **N-1**：给 bootstrap 模式也写文档，等于**改变了 Route B 的 staging 产物**（generation 里多一个文件 ⇒
  bootstrap 的 generation id 也变了）。严格读简报「不动 stage 的 bootstrap 路径」这句，这算擦边；
  但简报改法 1 同时写了「没有则在 manifest 或 plan 里加一个字段，属 stage 侧改动，允许」，
  而 manifest 推导逻辑（`derive_bootstrap_services`）确实一行未改，实现选的是这两句里更强的那一个解法。
  我判定为合规。实际影响也接近于零：`runtime_service_main.py` 本身就在 generation 里，
  任何对它的改动都会换 generation id。
- **N-2**：`_read_authority_document` 是先 `stream.read()` 读完再判 `MAX_LEGACY_BINDING_BYTES`，
  不是边读边限。这与既有 manifest reader 同款，且文件在 exec 前已被 wrapper 按 sha256+size 核过，
  只有 root 在 exec 之后改文件才构得成竞争。非回归。
- **N-3**：报告 §5 M1 的端到端数字（4 failed）低于实测（10 failed / 7 条 `not current`）；
  §1 的 diff stat（681 / 1789）比实际（682 / 1790）少一行，应该是在 `7ae0aa5` 之前生成的；
  §8.1 说「新增 40 个用例」，实测按默认 `addopts` 收集是 **+56** 条（见 §6）。
  三处都是低估，不影响结论，但集成阶段重生成 manifest 时要用对数字。
- **N-4**：`assert_junit_contract.py --tests 39` 是硬编码计数，以后往这两个文件里加任何一条用例
  都要同步改 CI。这是仓库既有约定（`paper-sqlite-image` 一样），只是提醒。
- **N-5**：`legacy_generation_binding()` 里 `runtime_root=legacy_root.as_posix()`（只 `abspath`，不 `realpath`），
  角色侧比的也是 `abspath`。两边都用字面路径，所以 stage 时必须传**与 `PRODUCTION_ROLE_POLICY` 完全一致**的
  `/home/lighthouse/rquant/data/runtime`；传相对路径、带 symlink 分量的等价路径都会导致角色拒绝启动
  （fail-closed，方向安全，但会浪费一次现场排查）。写进 runbook 即可。
- **N-6**：`daily` 这一个 role 也走 `rquant.runtime_service_main`，但它不是 kind-backed、
  不带 `--authority-runtime`（`TCB-2` 用例钉的是「22 个 kind-backed role」），
  走 `else` 分支 `schema_generation = args.expected_generation`，行为与 base 完全一致。

---

## 6. 集成输入

### 6.1 `tests/manifests/full-suite-v1` 重生成：预期新增 **56** 条（不是 40）

我按默认 `addopts`（`-m 'not network and not linux_exact'`）分文件点过：

| 文件 | base | HEAD | 增量 |
|---|---:|---:|---:|
| `tests/integration/test_route_a_legacy_binding_e2e.py` | 0 | 15（另有 1 条 `linux_exact` 不进分片） | **+15** |
| `tests/unit/test_runtime_legacy_generation_binding.py` | 0 | 23 | **+23** |
| `tests/unit/test_tp9_role_child_runtime.py` | 61 | 78 | **+17** |
| `tests/unit/test_runtime_authority_publish.py` | 77 | 78 | **+1** |
| 合计 | | | **+56** |

只算包 A 的话，`index.json` 的 `full_suite.cases` 应从 `13653` 变成 `13709`；
`skips` 不变（新增 skip 只有那条 `linux_exact` 的平台守卫，而它被 `addopts` 排除在收集之外）。
包 B 的增量要另算，最终以重新收集为准。

### 6.2 `CHANGELOG.md`（`[Unreleased]`）

```markdown
### Fixed
- 路线 A 下每个 kind-backed 角色都因为权威 generation id 与 legacy generation id 是两个命名空间而
  硬失败在 `runtime schema service generation is not current`（#207，取代 #187 的窄读法）。
  `runtime_service_main` 现在用 `<runtime root>/current` 解析出的 legacy generation 装载 schema
  bindings，权威 generation 仍由既有的 manifest 路径校验绑定。
- 两个 recovery role 在 runtime-exec wrapper 子环境里 import 期构造 `Settings` 即死（#186、#188）。
  `dashboard/strategy_lab_runs.py` 与 `dashboard/strategy_lab_data.py` 改为函数内 `get_settings()`
  加 PEP 562 `__getattr__`；issue 里的四层链实际是六个模块。

### Added
- `runtime-authority-stage` 往每一代 generation 根目录写 `legacy-binding.json`，记录这一代是从哪个
  legacy runtime root 的哪一代 deployment 上 stage 出来的（Route B 记 `mode: "bootstrap"`）。
  它的 sha256 进 full manifest，因此进权威 generation id，wrapper 在 exec 前逐条核对；
  角色启动时要求它记的 legacy generation 与 `<root>/current` 当前解析到的一致。
- CI job `route-a-legacy-binding-linux`（3.11 / 3.12），在真 `/home/lighthouse/rquant/data/runtime`
  上跑路线 A 的 Linux 端到端验收，零-skip JUnit 契约。
```

### 6.3 `DEPLOY.md` / runbook 要点

1. **换 legacy 代必须重新 stage + publish。** 只切 `data/runtime/current` 不换权威 generation，
   全部 kind-backed 角色拒绝启动，原文
   `ValueError: runtime legacy generation binding does not match the current pointer`。
2. **stage 时 `--legacy-runtime-root` 必须写字面量 `/home/lighthouse/rquant/data/runtime`**
   （绝对、无 symlink 分量、无尾斜杠）。文档记的是 `abspath` 后的字符串，角色比的也是 `abspath`，
   不一致就报 `... names another runtime root`（N-5）。
3. **dry-run 的核对点**：`plan.json` 的 `staged_files["generation/legacy-binding.json"]` 有摘要；
   apply 后 `cat <staging>/generation/legacy-binding.json`，其中 `generation_id` 必须等于
   `readlink /home/lighthouse/rquant/data/runtime/current` 的目标，`runtime_root` 必须等于上面那个字面量；
   并确认它出现在 `full-manifest.json` 的 `entries` 里、`mode` 为 `292`（0o444）。
4. **回滚含义**：路线 A 回退（删 `data/runtime/current`）之后，同一代权威 generation 的角色会自动回到
   降级分支（Route B），不需要再换 generation。
5. **包 C 顺序（修正简报第 2、10 步）**：
   - 第 2 步「换 runtime-exec」**不需要**——`rquant-runtime-exec.pyz` 与 `rquant-production-deploy.pyz`
     在 base 与本分支上哈希逐字节相同（`a5d9b3ff…9c5e` / `a41db437…6757`），包 A 没碰它们的输入。
     #208 改 sealer helper 那一项仍按包 B 的结论走。
   - **两个 serving unit 要在第 8 步（`deployment-profile --apply`，写出 `current`）之前停**，
     不是第 10 步之前。第 8 步之后它们一旦重启就会以旧代码的 `not current` 失败；
     第 10 步 publish 之后、若这一代不是用本分支代码 stage 的，则会以
     `runtime legacy generation binding is unavailable or contains a symlink` 失败。
   - 第 11 步「任何 `not current` 出现即停下回报」这条判据继续有效，且现在含义更精确：
     出现 `not current` = 角色跑的是旧代码；出现 `binding is unavailable` = generation 是旧代码 stage 的；
     出现 `does not match the current pointer` = 指针与这一代对不上（该重 stage）。
6. **R07**：本分支未重冻结 baseline，也未改 `CLAUDE.md` / `AGENTS.md`。按 workload-isolation 的既有约束，
   包 A 与包 B 谁后合并谁在 merge commit 上重冻结，且**重冻结必须是最后一个 commit**。

### 6.4 建议在集成阶段一并落的小改（对应 S-1 / S-2 / S-3 / S-6 / S-7）

- `.github/workflows/ci.yml`：新 job 改 `-m linux_exact` + 契约 `--tests 1 --cases 1`（S-1）。
- `tests/integration/test_route_a_legacy_binding_e2e.py`：
  - 补一条「两次真实安装 → 兄弟代 → 挪 current」的用例（S-2，代码见 §7 探针）；
  - 改掉 `test_a_kind_backed_role_reaches_its_service_loop_over_a_real_current` 那句「stop event is pre-set」（S-3）；
  - `last_error` 的精确白名单放宽成「不含 `schema service generation`」（S-6）；
  - Linux-only 那条的守卫改看 `/home/lighthouse/rquant`，清理改成 `chmod -R u+rwX` 后 `rmtree(ignore_errors=False)`（S-7）。
- `src/rquant/runtime_legacy_generation_binding.py` 模块 docstring + 报告 §5 M3：
  把「loader 兜不住是因为两代 manifest 相同」改成「loader 兜不住是因为兄弟代各自的 basis 都自洽、
  manifest 指纹又相同」。
- `src/rquant/runtime_authority_stage.py`：`build_stage_plan` 只解析一次 `current`（S-4）。
- runbook：写上「缺 `legacy-binding.json` 的报错措辞含义」（S-5）与 §6.3 的五条。

---

## 7. 附：审查过程用到的命令与产物

- 私有根：`/Users/roxor/rq-raar-rev`（含 `env-recipe.sh`、探针、base 检出、变异原文、Docker 日志），审查结束后删除。
- 两版本定向套件：`.venv/bin/python -m pytest -q --basetemp=<私有根>/tmp/bt311 <13 个文件>`，3.12 同形。
- root 制品对拍：`git archive 2db3846 | tar -x -C <base>`，两棵树各跑
  `scripts/build-runtime-exec-pyz.py` 与 `scripts/build-production-deploy-pyz.py`，`shasum -a 256` 比对。
- wrapper 子环境 import 探针：
  `env -i LANG=C LC_ALL=C TZ=UTC .venv/bin/python -I -S -c "import sys; sys.path[:0]=[<src>,<site-packages>]; __import__(m)"`，
  对 recovery 闭包的 155 个 `rquant.*` 模块逐个跑。
- Linux 非 root 容器重放：`docker run --rm python:3.11-slim`，容器内建 uid 1001 用户、
  `install -d -m 0755 -o 1001 /home/lighthouse`、`uv sync --frozen`，再原样执行 CI job 的 pytest 与契约命令。

### 7.1 兄弟代探针源码（S-2 建议直接搬进 `test_route_a_legacy_binding_e2e.py`）

审查时它是仓库外的一个独立文件，靠 `PYTHONPATH=<worktree>` + `-c pyproject.toml --rootdir=<worktree>` 跑起来；
搬进仓库后 `recorded_install` 这个 fixture 与两个用例可以直接用，`_route_a_world` / `SERVING_ROLE` /
`GENERATION_LEGACY_BINDING_NAME` 都是同文件里现成的。

```python
@pytest.fixture
def recorded_install(monkeypatch: pytest.MonkeyPatch):
    """Capture the real install call so a probe can repeat it verbatim."""

    import rquant.runtime_deployment_profile as profile_module

    captured: dict[str, Any] = {}
    real = profile_module.install_runtime_deployment_profile

    def recorder(profile, **kwargs):
        captured.update(kwargs)
        captured["profile"] = profile
        return real(profile, **kwargs)

    monkeypatch.setattr(profile_module, "install_runtime_deployment_profile", recorder)
    monkeypatch.setattr(
        "rquant.runtime_production_profile.install_runtime_deployment_profile",
        recorder,
        raising=False,
    )
    return captured, real


def _second_install(captured: dict[str, Any], real) -> str:
    """A second real install on the same runtime root: one rotated notify capability.

    The schema registry is already bootstrapped, so no bootstrap reason may be passed.
    Service manifests do not carry capability values, so they come out byte-identical while
    `generation-basis.json` - and therefore the legacy generation id - changes.
    """

    kwargs = dict(captured)
    profile = kwargs.pop("profile")
    kwargs.pop("schema_bootstrap_reason", None)
    environ = dict(kwargs["environ"])
    environ["PUSHDEER_KEYS"] = "pushdeer-rotated"
    kwargs["environ"] = environ
    return real(profile, **kwargs).generation_hash


def test_a_sibling_generation_with_the_same_manifests_is_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, recorded_install
) -> None:
    """`current` swung to a *legitimately installed* sibling, which is the case the schema
    loader cannot see: its basis hashes to its own directory name, its manifest fingerprints
    and producer commit are identical, so only the binding document knows the difference."""

    captured, real = recorded_install
    route = _route_a_world(tmp_path, monkeypatch)
    first = route.receipt.generation_hash
    route.stage_and_publish()
    second = _second_install(captured, real)
    assert second != first
    left = route.runtime_root / "generations" / first / "manifests"
    right = route.runtime_root / "generations" / second / "manifests"
    for path in sorted(left.iterdir()):
        assert path.read_bytes() == (right / path.name).read_bytes()

    with pytest.raises(ValueError, match="does not match the current pointer"):
        route.run_role(SERVING_ROLE)
```

另外三条我用过的探针（wrapper 只改 mode 就拒、缺文档 + 缺 manifest entry 仍拒、
交叉核对被中和后角色接受兄弟代）在 §4.1 / §4.2c / §4.4 里各自引了结论，
它们主要是审查用的一次性证据，不一定都要进仓库。
