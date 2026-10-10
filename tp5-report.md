# TP5 / PR-C 实现报告：目录 owner 谓词按 `allowed_modes` 推导

**分支** `cc/20260903-prc-phasec-owner-predicate` · **base** `origin/main` =
`16a1f019dbc5e081f3742018659489bb28f99baa` · **未 push、未开 PR**

| commit | 内容 |
|---|---|
| `b43cb9c` | `test(shadow): pin the directory owner predicate to allowed_modes`（C1–C6 + S13，30 个新用例，其中 7 个对当时的谓词是红的） |
| `d69fa93` | `fix(shadow): derive the directory owner set from allowed_modes`（TCB 语义变更本体，安全论证三条写在 commit message 里） |
| `10141b6` | `test(shadow): take the group half of the 0o022 guard hostage`（审查 S1，**只改测试文件**，+18 行） |

> **审查后修订（`TP5-REVIEW-APPROVED`，must 0 / should 2）**
> - **S1** → `10141b6`：C4 补 mode `0o575`（group 写位）的两条拒绝断言，把审查员的 m5
>   （`0o022` → `0o002`）从「74 全绿的漏网」变成会红。只改测试文件，源码零改动。
> - **S2** → 本报告 §1：60 条基线红改按错误形态聚类重写，只有 9 条子进程退出码属 `#179`
>   形态，其余是本地 macOS 环境态，不再整体挂到该 issue 名下。
> - **N2**（note）：顺手把 `tp5-baseline/mutations/apply-mutation.py` 补齐到 m0–m6。
> - **N1**（docstring 20 行长于估算）：审查结论是「内容准确，不建议压缩」，未动。

改动面只有两个文件，`git diff --stat 16a1f01..HEAD`：

```
 src/rquant/legacy_shadow_export.py      |  29 ++-
 tests/unit/test_legacy_shadow_export.py | 357 ++++++++++++++++++++++++++++++++
 2 files changed, 385 insertions(+), 1 deletion(-)
```

`legacy_shadow_export.py` 的改动全部落在 `_open_child_directory_at` 一个函数内：docstring
（+20 行）、`owners` 推导（+5 行含注释）、`0o022` 子句（+1 行）、`st_uid != os.geteuid()`
改成 `st_uid not in owners`（改 1 行）。**14 个调用点一行没动**（diff 里出现的两处函数名都是
hunk header 的上下文，不是增删行）。

模块内 14 个调用点与规格 v2 的行号逐行核对一致：475 / 528 / 673 / 880 / 1704 / 2016 / 2355 /
2438 / 2593 / 2623 / 3073 / 3186 / 3224 / 3709。模块外还有
`scripts/build-signal-family-shadow-fixture.py:298` 与 `:345` 两处，两者都传
`frozenset({_ROOT_MODE})` ⇒ owner 推导为 `{euid}` ⇒ 行为逐位不变，该脚本零改动（已实测确认
它的 `allowed_modes` 仍是 `{_ROOT_MODE}`，不是靠规格转述）。

---

## 1. 清单逐条对照

### §1 全量基线（G0-1 ～ G0-4 / G-1）

| # | 结论 |
|---|---|
| G0-1 | 已取，见 `tp5-baseline/full-baseline-py311.txt`（`uv run pytest -q`，在 base commit `16a1f01` 的**干净 detached worktree** `/Users/roxor/rq-tp5-baseline-wt` 里跑）。结果：**60 failed / 13215 passed / 56 skipped / 2 deselected**，`60+13215+56 = 13331` 正好是冻结的 full-suite case 总数 |
| G0-2 | 改动面外只有一条红（R07 不动点），已完成比对与归因（见下方「§1 的一条红」） |
| G0-3 | 没有用两文件复现推断全量结论；两文件的 `#179` 污染现象不在本包的判断链上 |
| G0-4 | 全量基线用 `uv run pytest`。**偏离见 §3-D1**：另外补跑了 `.venv312/bin/python -m pytest` 的 3.12 对照 |

**基线上有 60 条既有红，全部在本包改动面之外。** 按错误形态聚类（数字是我从
`tp5-baseline/full-baseline-py311.txt` 的 `FAILED` 行重新算的，六类相加正好 60，完整 node id
清单见 `tp5-baseline/full-baseline-failures.txt`）：

| 条数 | 形态 | 主要落点 | 性质 |
|---|---|---|---|
| 13 | `AttributeError: module 'os' has no attribute 'waitid'` | `tests/integration/test_formal_smoke_generation_execution.py` | **macOS 平台缺 `os.waitid`**，Linux 上不存在 |
| 13 | `AssertionError: Regex pattern did not match` | `test_authority_runtime_installer.py`（12）、`test_lab_artifacts.py`（1） | 期望的错误文案没出现，多半是私有目录谓词族在本地根下走了另一分支 |
| 12 | 各自的领域异常 / 断言 | `test_contained_subprocess`、`test_authority_runtime_installer`、`test_lab_claim_finalizer_runtime_stage7`、`test_runtime_recovery_artifacts`、`test_production_deploy_bootstrap`、`test_watchlist_quote_provider` 等 | 混杂，逐条形态各异 |
| 9 | 子进程退出码不符（`assert 1 == 73` / `1 == 91` / `1 == 17` / `0 == 130` / `0 == 129` / `1 == 0`） | `test_runtime_shadow_job`（4）、`test_workload_isolation_migration`（2）、`test_runtime_shadow_validation` / `test_runtime_resource_admission` / `test_runtime_builder_retention` 各 1 | **这一类才是 [#179](https://github.com/roxorlt/rquant/issues/179) 的形态**，含清单 §1 点名的 `test_publication_recovers_after_hard_exit_between_link_and_cleanup` |
| 8 | `Failed: DID NOT RAISE` | `test_experiment_registry`（2）、`test_contained_subprocess`（2）、`test_artifact_retention_catalog` / `test_lab_artifact_catalog` / `test_lab_daemon` / `test_live_spool` 各 1 | 本地环境下该抛的没抛 |
| 5 | IPC / 超时（`EOFError` ×3、`_queue.Empty`、`no competitor reported for 120s`） | `test_runtime_recovery_coordinator`、`test_runtime_recovery_artifacts`、`test_runtime_resource_admission`、`test_replay_lineage_authority` | 本地机器负载 / 多进程握手 |

**归因口径（按审查 S2 收窄）**：只有那 9 条子进程退出码属于 `#179` 的形态，其余约 50 条是
**本地 macOS 环境态**（`os.waitid` 缺失最典型，`waitid` 在 macOS 的 `os` 模块里根本不存在），
不应记到 `#179` 名下。**权威信号以集成阶段的 Linux CI 为准**；本地这份基线的作用是给 G0-2
提供比对基准，不是给这些红下结论。**按 G0-2 / G0-4 只留证不修、不 skip、不 rerun。**

**关键对照（本包唯一需要的结论）**：五个关键文件
——`test_legacy_shadow_export.py`、`test_runtime_builder_shadow.py`、
`test_signal_family_verifier_harness.py`、`tests/integration/test_signal_family_verification_reset.py`、
`test_assert_full_suite_shards.py`——在基线上**一条红都没有**，所以本包的 scoped 结果与基线
直接可比，那条 R07 红的归因链是干净的。

**基线取证有一次返工，如实记录**：13:33 我在**工作 worktree 里**起了第一次全量跑，随后在同一
棵树上开始改代码。R07 与 shard manifest 这两组用例是**运行时读工作树**的，所以那次跑被自己的
改动污染，14:00 kill 掉、留证为 `tp5-baseline/ABORTED-contaminated-full-run.txt`（文件头写明
作废理由），基线在干净 worktree 里重取。

### §5 TCB-3

| 要求 | 结论 |
|---|---|
| 只改 `_open_child_directory_at` | ✅ |
| owner 按 `allowed_modes` 推导（含 `_SESSION_MODE` ⇒ `{0, euid}`，否则 `{euid}`） | ✅ |
| 补 `st_mode & (S_IWGRP\|S_IWOTH)` | ✅ |
| 签名不变、14 调用点不变、fixture 脚本不变 | ✅（`scripts/build-signal-family-shadow-fixture.py` 零改动） |
| 安全论证三条 + 残余风险 R1/R2/R3 原样带上 | ✅ 写进 `d69fa93` 的 commit message，并复述于本报告 §2 |

### §6 C1–C10 + S13

| # | 结论 | 证据 |
|---|---|---|
| C1 | ✅ `test_build_phase_modes_keep_the_owner_set_pinned_to_the_effective_uid`：`{0o700}` 下 owner=euid 接受、owner=0 拒绝 | `tp5-baseline/c8-both-versions.txt` |
| C2 | ✅ `test_session_mode_admits_the_root_signer_that_sealed_the_directory`：`{0o555}` 下 owner=0 与 owner=euid 都接受 | 同上 |
| C3 | ✅ `test_session_mode_still_refuses_every_other_foreign_owner`：`{0o555}` + owner=999 拒绝 | 同上 |
| C4 | ✅ `test_group_or_other_writable_directories_are_refused_even_when_allowed`：owner=0 / mode `0o557` 拒绝，**且把 `0o557` 加进 `allowed_modes` 后 owner=0 与 owner=euid 都仍拒绝**。**审查 S1 追加**：另起一个 mode `0o575`（只置 group 写位）的子目录，`allowed_modes={0o555, 0o575}` 下 owner=0 与 owner=euid 同样拒绝——`0o022` 的 group 一半从此也有回归护栏 | 同上；变异 m1 / m5 / m6 分别盯住整条与两半 |
| C5 | ✅ `test_root_sealed_session_loads_the_same_batch_as_a_publisher_owned_session`：发布后把 session chmod 成 `0o555`、`st_uid` 伪造成 0、`_signed_session_modes` 换成生产返回值，`load_accepted_legacy_shadow_export` 成功加载，并对 `dataclasses.fields(AcceptedLegacyShadowExport)` 的**七个字段逐字段**与未伪造时相等（字段名集合本身也被断言钉死，将来加字段不会静默漏比） | 同上 |
| C6 | ✅ `test_directory_owner_predicate_truth_table`：**24 行**参数化真值表，覆盖 `{0o700}` / `{0o555}` / `{0o700,0o555}` / `_signed_session_modes(生产)` / `_signed_session_modes(离线)` **五种** × `{0, euid, 999}` **三种** owner，并对前三种额外铺开两种目录 mode（把「mode 不在集合里 ⇒ 无论 owner 都拒」也钉住）。**直接调 `_open_child_directory_at`，不经任何调用点** | 同上 |
| S13 | ✅ `test_signed_session_modes_pin_the_inputs_of_the_owner_derivation`：`_signed_session_modes(production) == frozenset({0o555})`、`_signed_session_modes(offline) == frozenset({0o700})` 两条独立断言。C6 的后两行只有在这两条成立时才有意义 | 同上 |
| C7 | ⬜ **不在本包内**（云端真实发布验证，上线窗口执行，是启用 Phase C 的前置） | O-1 |
| C8 | ✅ 38 个既有用例（44 个 case）全绿，离线策略下逐位不变 | `tp5-baseline/c8-both-versions.txt`：3.11 / 3.12 各 **74 passed**（44 既有 + 30 新增） |
| C9 | ⚠️ `test_runtime_builder_shadow.py`、`test_signal_family_verification_reset.py` 全绿；`test_signal_family_verifier_harness.py` 有 **1 条 R07 不动点红**，已归因为集成步骤的机械重冻结，**不是缺陷** | `tp5-baseline/scoped-py311.txt` / `scoped-py312.txt` / `r07-attribution.txt` |
| C10 | ✅ `uv run ruff check src/rquant/legacy_shadow_export.py tests/unit/test_legacy_shadow_export.py` → `All checks passed!` | — |

### §6 TP5 不变量

| # | 结论 |
|---|---|
| I-TP5-1 | ✅ 不含 `_SESSION_MODE` 时 `owners = frozenset({os.geteuid()})`，`st_uid not in owners` 与 `st_uid != os.geteuid()` 恒等；新增的 `0o022` 子句在 `{0o700}` / `{0o555}` 下恒假。C8 的 44 个既有 case 未改一行且全绿，C5 里还有一条端到端负向控制：离线策略下伪造 `st_uid=0` 仍必须 `LegacyShadowExportUnavailableError` |
| I-TP5-2 | ✅ 结构性成立：owner 集合只由 `allowed_modes` 决定。C6 直接打谓词，不经调用点，所以「某个调用点漏放宽」这种失败模式不存在 |
| I-TP5-3 | ✅ `observed.st_mode & (stat.S_IWGRP \| stat.S_IWOTH)` 无条件参与拒绝，与 owner 无关（C4 的第三条断言就是 owner=euid 的情形） |
| I-TP5-4 | ✅ `_read_regular_at` / `_open_bound_regular` 一字未动（见 §1 的 diff） |
| I-TP5-5 | ✅ `_ensure_private_root` 的 `os.fchmod(descriptor, _ROOT_MODE)` 一字未动 |
| I-TP5-6 | ✅ `os.geteuid()` 在谓词体内每次调用求值，无模块级缓存；代码里留了 `# Evaluated per call: the process may have dropped privileges since import.` |
| I-TP5-7 | ✅ 由 C4 盯住：`0o557`（other 写位）与 `0o575`（group 写位）显式加进 `allowed_modes` 之后仍然拒绝，所以 `0o022` **两个半边都**不是死代码。变异 m1（整条）、m5（只留 other）、m6（只留 group）各自都红 |
| I-TP5-8 | ✅ 没有加任何 `S_ISVTX` 豁免 |
| — | ✅ 目录**没有**加 `nlink == 1` |
| — | ✅ 没有加额外 symlink 校验 |

### §7 改动面与禁改清单

| 文件 | 状态 |
|---|---|
| `src/rquant/legacy_shadow_export.py` | ✅ 只改 `_open_child_directory_at` |
| `tests/unit/test_legacy_shadow_export.py` | ✅ +6 个测试函数（C1–C5 + C6 参数化）+ S13 一条 = 7 个函数 / 30 个 case；`10141b6` 又给 C4 加了两条 group-writable 断言（函数数与 case 数不变，仍是 74 collected） |
| `DEPLOY.md` §16 改写 | ⬜ **未做**，见 §3-D2 |
| `CHANGELOG.md` `Fixed` 一条 | ⬜ **未做**，见 §3-D2 |
| `release-a-runbook.md` 运维说明 | ⬜ **未做**（该文件不在本仓库内），见 §3-D2 |

**禁改清单零命中**：`deploy/libexec/rquant-shadow-report-signer`、`deploy/sudoers/`、
`runtime_authority.py`、`runtime_code_generation.py`、`signal_family_root_verifier.py`、
`signal_family_verifier_entry/`、`scripts/build-signal-family-shadow-fixture.py`、
`_read_regular_at` / `_open_bound_regular` / `_ensure_private_root` 的谓词、
以及 14 个调用点——`git diff --stat 16a1f01..HEAD` 只有两个文件，逐条为真。

### §8 全局门

| # | 结论 |
|---|---|
| G-1 | ✅ 见上 |
| G-2 | ✅ 无 skip / 无 xfail / 无 rerun（C9 的那条红原样留着，没有 skip 掉） |
| G-3 | ✅ 改动面 ⊂ §7；禁改清单零命中。§7 里的三个文档条目未做，见 §3-D2 |
| G-4 | ✅ 改动面外唯一的红已留证并归因，未就地修 |
| G-5 | ✅ ruff 零新增告警 |
| G-6 | ⬜ 集成步骤（CHANGELOG / manifest / R07 不动点 / 一次 CI） |
| G-7 | ✅ `test(shadow):` / `fix(shadow):`；merge commit 由集成执行 |
| G-8 | ⬜ 合入前由协调者在里程碑报告向 owner 点名 TCB-3，材料见本报告 §2 |

---

## 2. 安全论证（原样，供里程碑报告直接引用）

> **问：放宽到 `{0, euid}` 会让 lighthouse 读到哪些本不该读的 root 目录？**
> **答：一个都没有。这次放宽授予的新文件系统读权限为零。**
>
> 1. **可达性被结构性限制在一层子目录。** `_open_child_directory_at` 用
>    `os.open(name, O_RDONLY|O_DIRECTORY|O_NOFOLLOW, dir_fd=parent_descriptor)`，并拒绝
>    空名 / `.` / `..` / 含 `/` 或 `\` 的名字。父 fd 链的起点是 `_ensure_private_root`
>    校验过的 export 根。可触达集合 = `{<export_root>/<单个合法名>}`，**永远到不了**
>    `/etc/rquant`、generation 树或任何别处。生产 export 根是
>    `/home/lighthouse/rquant/data/legacy-shadow`——本来就是 lighthouse 自己的目录。
> 2. **被放宽的 mode 本来就已经可读。** 放宽只在 `allowed_modes` 含 `0o555` 时生效。
>    `root:root 0555` 目录对所有人 r-x，lighthouse 今天用裸 `open()` 就能读；谓词的拒绝是
>    **纯策略**，不是访问控制。反过来 `root:root 0700` 目录 lighthouse 在 `os.open` 阶段
>    就 `EACCES`，谓词根本轮不到判断。
> 3. **内容信任面一字未动。** 目录被接受后，每个文件仍要过 `_read_regular_at`：
>    `S_ISREG` + `st_uid ∈ {0, euid}` + `nlink == 1` + mode 恰为 `0444` + size 上限 +
>    读前/读中/读后三次 `(st_dev, st_ino, st_size, st_mtime_ns, st_ctime_ns, st_nlink)`
>    一致；再叠 `recovery-marker.json` 的 Ed25519 签名把 session 绑到 `st_dev`/`st_ino`。
>    **没有任何一个字节的可信度判定发生变化。**
>
> **残余风险（穷尽）**：**(R1)** root 在 export 根下造一个同名 `0555` 目录——root 本来就能
> 替换任意对象，不构成新增权限。**(R2)** root 拥有但 g/o 可写——本包的 `0o022` 检查把它从
> 「靠 `allowed_modes` 间接排除」变成结构性拒绝，**风险下降**。**(R3)** 未来往
> `allowed_modes` 加带 g/o 写位的 mode——由 I-TP5-7 与 C4 盯住。

**同时要点名的更严重事实**：不只 Phase C 读不到。签名器经 `sudo -n` 以 root 运行并
`fchown(dir, 0, -1)`，而发布者 `lighthouse`（`rquant-monitor` / `rquant-surge-watch`）
在签名之后还要用同一谓词重开自己的 staging（`:1704` / `:2623` / `:3224`）⇒
**生产上根本产不出一份 accepted export**。这是代码阅读结论，**C7 是唯一能证伪它的实验**，
必须在启用 Phase C 之前在云端做。

---

## 3. 偏离

**D1（G0-4，主动偏离并说明）**：清单 G0-4 写「全程 `uv run pytest`，不用
`.venv/bin/python -m pytest`」，而 TP5 简报要求「两版本（`.venv` / `.venv312`）跑」。两条撞在
一起，处理如下：

- **§1 全量基线严格用 `uv run pytest`**（G0-4 的原意是基线口径与 CI `.github/workflows/ci.yml:68`
  一致），见 `full-baseline-py311.txt`；
- **C8 两版本对照**（`c8-both-versions.txt`）里 3.11 那一半也是 `uv run pytest`；
- **C9 + 下游消费者的两次长跑**（`scoped-py311.txt` / `scoped-py312.txt`）用的是
  `.venv/bin/python -m pytest` 与 `.venv312/bin/python -m pytest`，省掉 uv 每次重解析的开销。
  3.12 本来就只有这一个入口；3.11 这一侧我另外用 `uv run pytest` 跑过同一份
  `test_legacy_shadow_export.py` 做口径对照（同为 74 passed），两种调用没有出现分歧。

两个 Python 版本的结果逐条一致（各 1 failed / 859 passed / 7 skipped，红的是同一条 R07）。

**D2（§7 的三个文档条目未做）**：TP5 简报与本次任务书两处都写「改动面只限
`src/rquant/legacy_shadow_export.py` 的谓词函数与其测试文件」，而清单 §7 还列了
`DEPLOY.md` §16 改写、`CHANGELOG.md` 的 `Fixed`、`release-a-runbook.md` 的运维说明。
我按**指派方的硬约束**执行，三个文档条目**未动**，在此交回协调者：

- `DEPLOY.md` §16：需写入裁决（③ 按模式推导版）、「**生产发布路径自身也被同一谓词阻断**」
  这一更严重的事实、以及把 C7 列为 Phase C activation 前置；
- `CHANGELOG.md`：`Fixed` 一条（G-6 本来也把 CHANGELOG 归在集成步骤）；
- `release-a-runbook.md`：残留 `root:root 0555` staging 无法自动清理
  （`_discard_directory_at:2355` 用 `{0o700}` ⇒ owner 推导为 `{euid}`，且 lighthouse 对该
  目录无写权），**必须 root 手工 `rm -rf`**。该文件不在本仓库内，只能由持有它的人写。

**D3（C9 的一条红，留证不修）**：见下。

---

## §1 的一条红：R07 不动点

```
FAILED tests/unit/test_signal_family_verifier_harness.py
       ::TestRecomputeExpectations::test_the_recomputation_reports_a_correct_tree_as_current
```

R07 policy fixture 是从 `baseline..candidate`（baseline = v0.30.0 = `2b26280`）的 raw git
diff 推导出来的，**任何**编辑仓库文件的工作包都会让它移动。归因证据：在 base commit
`16a1f01` 的干净 detached worktree 里，这条用例连同
`tests/unit/test_assert_full_suite_shards.py` 一起 **25 passed**，所以它既不是既有红、
也不是缺陷。

`coordinator-rulings.md` §S2「过程」把 `manifest / R07 不动点` 明确划给**集成**步骤，
清单 G-6 也是这么写的，所以本包不动它。另一个理由是实操上的：现在重冻结，等集成加上
§7 要求的 `CHANGELOG.md` / `DEPLOY.md` 之后立刻又会失效，等于白做。

同一形状的还有 `tests/unit/test_assert_full_suite_shards.py`——它把 shard manifest 与
case 总数（base 上 13331）冻死，本包 +30 个 case。它**不在** C9 的验收清单里，但同样会红到
shards 重生成为止。两者都请在集成时一并处理（`scripts/r07_policy_regenerate.py` 与
`scripts/full_suite_shards.py`；R07 重冻结要放在**最后一个 commit**）。

---

## 4. 变异验证（7 个，全部应红且全部红）

每个变异都是在实现基础上单点改 `src/rquant/legacy_shadow_export.py`，跑
`tests/unit/test_legacy_shadow_export.py`，随后 `git checkout HEAD --` 还原。施加脚本是
`tp5-baseline/mutations/apply-mutation.py`（已补齐 m0–m6 七个，修掉审查 N2 点名的
「只实现了 m1–m3」），原始输出在 `tp5-baseline/mutations/`。下表是 `10141b6` 之后重跑的结果。

| # | 变异 | 结果 | 被打中的用例 |
|---|---|---|---|
| m0 | 谓词整体回退到 v0.30.0（删推导、删 `0o022`、`st_uid != geteuid()`） | **7 failed** / 67 passed | C2、C4、C5 + C6 四行——与 `tdd-red-py311.txt` 记的 TDD 红逐条同集，证明 C1–C6 不是恒真断言 |
| m1 | 删掉 `or observed.st_mode & (stat.S_IWGRP \| stat.S_IWOTH)` | **1 failed** / 73 passed | C4——证明 `0o022` 整条不是死代码 |
| m2 | owner 集合恒 `frozenset({os.geteuid()})` | **6 failed** / 68 passed | C2、C5，以及 C6 的四行 `*-root-True` |
| m3 | owner 集合恒 `frozenset({0, os.geteuid()})` | **4 failed** / 70 passed | C1、C6 的 `root-mode-448-root-False` 与 `signed-session-modes-offline-448-root-False`，以及 C5 里的 I-TP5-1 端到端负向控制 |
| m4 | 推导键从 `_SESSION_MODE in allowed_modes` 换成 `_ROOT_MODE in allowed_modes` | **7 failed** / 67 passed | C1、C2、C5 + C6 四行——两个方向同时打中 |
| **m5** | `S_IWGRP \| S_IWOTH` **只留 `S_IWOTH`**（`0o022` → `0o002`） | **1 failed** / 73 passed | C4。**这是审查员发现的漏网**：`10141b6` 之前这个变异 74 全绿，因为 C4 原来只用 `0o557`（只置 other 写位） |
| m6 | `S_IWGRP \| S_IWOTH` 只留 `S_IWGRP` | **1 failed** / 73 passed | C4 |

- m3 与 m4 是 I-TP5-1 的护栏：只要有人把「离线策略下也接受 root」偷渡进来，C1/C6/C5 三处同时红。
- m5 与 m6 是 `0o022` 两个半边各自的护栏，缺一边就会退化成「实现正确但回归无人看守」。
- 审查员另跑的三个 `_signed_session_modes` 变异（生产/离线对调 30 failed、恒 `{0o555}`
  28 failed、恒 `{0o700}` 3 failed）均由 S13 与 C6 抓到，我未重复复跑。

---

## 5. 测试 seam（S11 硬要求）

无特权进程 chown 不了，负向分支只能靠伪造 `st_uid`。`legacy_shadow_export.py` 里 `os.fstat`
出现 14 次（文件谓词、identity rebind、fsync 前后校验都在用），所以 seam **按
`stat.S_ISDIR(observed.st_mode)` 条件分派，只伪造目录**，文件观测一个字节不动：

```python
def patched_fstat(fd: int) -> os.stat_result:
    observed = real_fstat(fd)
    if stat.S_ISDIR(observed.st_mode):
        return _stat_result_with_uid(observed, uid)
    return observed
```

`os.stat` 用同一条件同样包了一层（`_read_regular_at` 会拿 `fstat` / `stat` 做身份重绑比对，
两边不一致会报 `... changed while read`）。`_stat_result_with_uid` 用
`os.stat_result(fields, {st_atime_ns, st_mtime_ns, st_ctime_ns})` 重建，纳秒时间戳不被十位
元组截断，`st_dev` / `st_ino` 原样保留——所以 recovery marker 的目录身份绑定照样成立。

**生产代码零 seam**：没有往 TCB 谓词里开任何注入口，也没有退回
`signal_family_verifier_entry/_artifact.py:305-313` 的注入式写法（调用图没有出现误伤）。

---

## 6. 复现命令

```bash
cd /Users/roxor/brain/30-projects/rQuant/.worktrees/prc-cc
source tp5-baseline/env-used.sh              # 私有根 /Users/roxor/rq-tp5-prc，umask 077

# C8（两版本）
uv run pytest tests/unit/test_legacy_shadow_export.py -q
.venv312/bin/python -m pytest tests/unit/test_legacy_shadow_export.py -q

# C9 + 下游消费者（两版本）
uv run pytest tests/unit/test_legacy_shadow_export.py \
  tests/unit/test_runtime_builder_shadow.py \
  tests/unit/test_signal_family_verifier_harness.py \
  tests/integration/test_signal_family_verification_reset.py \
  tests/unit/test_runtime_shadow_ed25519.py tests/unit/test_monitor.py \
  tests/unit/test_surge_watch.py tests/unit/test_cli.py \
  tests/unit/test_runtime_production_profile.py -q

# C10
uv run ruff check src/rquant/legacy_shadow_export.py tests/unit/test_legacy_shadow_export.py
```

证据目录 `/Users/roxor/brain/30-projects/rQuant/.worktrees/prc-cc/tp5-baseline/`：

| 文件 | 内容 |
|---|---|
| `base-commit.txt` | base commit sha |
| `full-baseline-py311.txt` | §1 全量基线（干净 worktree，`uv run pytest -q`，尾部附 provenance 块） |
| `full-baseline-failures.txt` | 基线 60 条既有红的完整 node id 清单 |
| `ABORTED-contaminated-full-run.txt` | 作废的第一次全量跑，头部写明作废理由 |
| `tdd-red-py311.txt` | 只加测试、未改实现时的 7 条红 |
| `c8-both-versions.txt` | C8 在 3.11 / 3.12 各 74 passed |
| `scoped-py311.txt` / `scoped-py312.txt` | C9 + 下游消费者两版本结果（`d69fa93` 时跑的 9 文件长跑，尾部注明在 `10141b6` 复核过） |
| `c9-four-files-py312.txt` | `10141b6` 之后 C9 四个文件在 3.12 的复跑（1 failed / 343 passed / 7 skipped，红的仍是同一条 R07） |
| `r07-attribution.txt` | 那条 R07 红的归因与 base 上的对照证据 |
| `mutations/m0..m6*.txt` | 七个变异的 `-rf` 摘要 |
| `mutations/apply-mutation.py` | 施加变异的脚本，支持 m0–m6（`python3 tp5-baseline/mutations/apply-mutation.py m5-guard-other-only`） |
| `env-used.sh` | 本次实际用的环境脚本（私有根 + `RQUANT_DISABLE_DOTENV=1`，worktree 无 `.env`） |

`tp5-baseline/` 与 `tp5-report.md` 都**不入 git**（不在 §7 改动面内），作为交付物留在
worktree 里。
