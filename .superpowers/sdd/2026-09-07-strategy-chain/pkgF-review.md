# 包 F 独立审查：live 平面策略链解锁（#218 A+B+C）

**审查对象**：worktree `/Users/roxor/brain/30-projects/rQuant/.worktrees/ra-f-cc`，分支
`cc/20260907-strategy-chain`，base `695e952`（tag `v0.32.2`），4 个 commit
`f16c100` → `85d7246` → `6c25b09` → `bc326cb`。
**标准**：`route-a-pkgF-brief.md`（改法 A/B/C 与边界）+ `route-a-218-scout.md` + `gh issue view 218 207`。
**审查环境**（全部本机复现，未 SSH、未改代码、未提交、未读写 `.env`）：

- mac `.venv` = Python 3.11.15；私有根 `/Users/roxor/rq-rafr-review`（审查结束删除）；
  worktree 无 `.env`，Settings 用五个环境变量喂假值（token 全 0），不碰任何真实凭证路径。
- Linux 用 Docker `python:3.11-slim`（容器内 Python 3.11.16，`uv sync --frozen`），
  跑的是分支的 `git clone` 副本，不挂载 worktree。
- Python 3.12 用 `UV_PROJECT_ENVIRONMENT` 建在私有根下（3.12.13），同样跑 clone 副本。
- 每次变异前确认 `git status` 干净，变异后一律 `git checkout --` 还原并复核干净。

---

## 裁定

# `PKGF-REVIEW-APPROVED`（附 1 条 must-fix，属小改动，不影响 A/B 的正确性）

A、B、C 三处改法本身都成立，证据我自己全部复现了：A 无修复时在真实 bundle + 真实 staged
generation 下必红且报的就是生产原话；B 换掉的那条比较不可能成立，新加的两条既能过也能拒，
权威绑定确实保留（我自己的 M4 让三条 integration 全部 `DID NOT RAISE`）；C 的两份文档形状、
权限、key_id 对齐、密钥不外泄都成立。must-fix 只有 C 的 `--only-missing` 一处边界：
对一个已存在但权限不是 0600 的凭证文件，它会**静默轮换**而不是保留——那正是这个参数存在
的目的所要避免的后果。改动约三行，不动 A/B。

---

## 逐条核读

### 1. A：`model_dump(mode="json")` 只改重校验、不改任何断言

**只改了循环体。** `git diff --numstat 695e952..HEAD -- src/rquant/runtime_service_main.py`
是 **+12 / -1**（其中 10 行是注释），全部落在
`build_runtime_strategy_completion_attestation_signer` 的 `:489-507`。我逐行读了改后的
整个函数：

- `:508-509` 的绑定断言一字未动：
  `if len(matches) != 1 or matches[0].manifest_fingerprint != manifest.manifest_fingerprint:`
  → `raise ValueError("strategy completion signer profile does not bind this manifest")`。
- 「不是模型实例的项照旧重验」保留（`else item`），并且有一条专门的用例
  `test_a_profile_entry_that_is_not_a_manifest_is_still_revalidated` 钉住。
- 后面的 shadow 命令、key id、keyring 校验全部原样。

**解冻不可能偷换 manifest。** `manifest_fingerprint` 是
`runtime_service_entrypoint.py:115-116` 的**计算属性** `canonical_sha256(self)`，不是存
字段，所以哪怕 `model_dump(mode="json")` 有一点点失真，指纹也会变，紧接着的绑定断言就会
拒绝。也就是说这条改法在「保真」这件事上是**失败关闭**的，不需要靠人眼审。

**无修复即红——我自己复现的生产原话。** 把 `raw = ...` 那行删掉、还原成
`model_validate(item)`，在真实 bundle + 真实 staged generation 的 e2e 下（本机
`.venv`）：

```
src/rquant/runtime_service_main.py:503: ValidationError
E   pydantic_core._pydantic_core.ValidationError: 3 validation errors for RuntimeServiceManifest
E   settings.migration
E     input was not a valid JSON value [type=invalid-json-value,
E       input_value=mappingproxy({'batch_byte.../final-artifacts/warm'}), input_type=mappingproxy]
E   settings.retention_policy
E     input was not a valid JSON value [type=invalid-json-value, ..., input_type=mappingproxy]
E   settings.worker
E     input was not a valid JSON value [type=invalid-json-value, ..., input_type=mappingproxy]

FAILED test_the_completion_signer_opens_for_every_strategy_over_a_real_installed_profile
  - ValueError: strategy completion signer profile contains invalid manifests
FAILED test_the_strategy_roles_reach_their_service_loop_over_a_real_current
  - ValueError: strategy completion signer profile contains invalid manifests
2 failed in 27.15s
```

跟 issue #218 里主机报的那一句逐字相同。（报告 §2 引的是「2 validation errors」，我实测
是 3 条——`settings.migration` / `retention_policy` / `worker`，都在 artifact_retention 那份
manifest 上。只是引用不准，结论不受影响，记 note。）

**静态用例对「12 个易感类 × AST」的判定：可靠，但覆盖面比它的 docstring 窄。**
我没有沿用报告的脚本，自己重跑了两遍扫描：

1. 动态枚举 `RuntimeContractModel` 子类（我这边加载到 **886** 个，报告写 879，差异来自
   模块加载顺序），筛「有 `JsonValue` 字段且该类没有 `mode="before"` 校验器」的，
   得到 **恰好 12 个**，与报告一致：`FeatureInstanceEnvelope`、`AppendNlQueryLog`、
   `PageControlCommandAudit`、`PageControlEffectRecord`、`PageControlReceipt`、
   `ReferenceRecord`、`RealRecoveryArtifact`、`RecoveryServiceJob`、
   `RuntimeSchemaPreparedDualWrite`、`RuntimeServiceManifest`、`DualWriteValueRecord`、
   `StrategyDecision`。
2. 对 `src/rquant` 全量 AST 扫 `<那 12 个类>.model_validate(<单参数>)`，命中 6 处，
   分类与报告一模一样。两处我做了实证核对，不是读代码推的：
   - `reference_data_registry.py:1392/1560/1590`：我构造了一个带两层嵌套的
     `ReferenceRecord` 并 `model_validate(实例)`，**通过**——因为
     `canonicalize_payload` 走 `json.loads(canonical_json(...))`，嵌套值全是原生
     dict/list，而字段是 `Mapping[str, JsonValue]`，外层 `MappingProxyType` 本身算
     `Mapping` 不算 `JsonValue`。**确实不是陷阱。**
   - `signal_family_root_verifier.py:2711`：`row` 来自
     `strict_canonical_json_loads(payload)` 解出的 list（`:2698-2711`），**不是陷阱**。
3. 另外扫了 `scripts/`：那 12 个类一次 `model_validate` 都没有。`src/rquant/runtime_exec_wrapper/`
   在 `src/rquant` 之内，已被第 2 步覆盖。

**结论：报告「修完这一处，仓库里没有同类残留」的判断，我独立复核成立。**

**我自己造的新陷阱，静态用例接得住（SA1）。** 在 `runtime_builder_strategy.py` 末尾加
`def _review_probe_sa1(manifest): return RuntimeServiceManifest.model_validate(manifest)`，
`test_no_module_hands_a_frozen_manifest_straight_to_model_validate` 立刻变红并点名
文件行号。

**但我也造出了两处它接不住的（SA2，两条都保持绿）**，见 should-fix S1。

### 2. B：recovery 改核自己的命名空间之后，权威绑定是否真的保留

**我自己复跑了 M4，而且用的是比报告更紧的形状**：只删掉
`_require_recovery_generation_binding` 里那一次
`resolve_legacy_schema_generation(...)` 调用（保留 `legacy_current_generation` 与
「给了 manifest 却没给权威 generation 就拒」这两道），其余一字不改。结果：

```
FAILED test_a_sibling_generation_with_the_same_manifests_is_refused        - DID NOT RAISE ValueError
FAILED test_a_manifest_from_another_authority_generation_is_refused        - DID NOT RAISE ValueError
FAILED test_a_bootstrap_staged_generation_over_a_real_current_is_refused   - DID NOT RAISE ValueError
3 failed, 15 passed in 142.95s
```

三条正是任务点名的那三条：**合法安装的兄弟代 / 外来 generation 的 manifest / bootstrap 代
压在真实 `current` 上**，全部 `DID NOT RAISE`。这说明权威绑定不是写在注释里，是真的在拦。
（报告的 M4 行写「unit 2 ＋ integration 3」，我这个更紧的形状下 unit **一条都没死**、
只有 integration 3 条死。这反而把报告自己的结论「权威绑定这一半的覆盖全在 e2e 上」证得
更强。记 note。）

**没有放宽任何核对，这是可拒的两条不是摆设**：

| 场景 | 结果 | 我的证据 |
|---|---|---|
| `_require_recovery_generation_binding` 开头直接 return（M2） | 7 条 unit 变红（新文件 6 条 + `test_recovery_production_runner_rejects_stale_unit_generation`） | 自己跑的 |
| recovery 侧改回比 `expected_generation`（M3，恢复原缺陷形状） | 4 条 unit 变红 | 自己跑的 |
| 两个 generation 都不给 | `recovery unit was given no generation to bind against` | 读码 + unit |
| 给了 manifest 没给权威 generation | `recovery unit manifest was given without its authority generation` | 读码 + unit |
| 没有 `current`（路线 B） | `recovery unit requires a current legacy deployment` | 读码 + unit |

**recovery 命名空间那条不是「自己比自己」。** `recovery.profile_generation` 由
`runtime_deployment_profile.py:446-449` 的 `validate_identity_and_policy` **每次加载重算**
（`canonical_sha256(recovery 段去掉 profile_generation)`），而
`bundle_recovery_profile_generation` 读的是另一份 manifest 的 settings
（`runtime_production_profile.py:1323` 由生成器写进 retention owner）。换掉 recovery 段而
不重新生成画像，两者就对不上。真正的牙齿仍然在权威链那一半，加上 profile 文档本身被
`generation-basis.json` 覆盖（e2e 的 `test_a_current_pointer_moved_to_a_copied_generation_is_refused`
证明拷贝代会被 `generation hash mismatch` 挡掉）。

**复用的确实是 #207 那个函数本体，没有弱化版。** `resolve_legacy_schema_generation`
只有一处定义（`runtime_service_main.py:335`），两个调用点：`runtime_service_main.py:697`
（#207 自己的）与 `runtime_recovery_production.py:112`。包 F 只 import 不改。
这跟 #207 的协调者裁决（用 `current` 解析到的 legacy generation 绑定，同时通过 manifest
路径保留权威 generation 的绑定）是同一条线。

**延迟 import 的说法我实测过**：把五个 Settings 变量全部 unset、`RQUANT_DISABLE_DOTENV=1`
的情况下，`import rquant.runtime_recovery_production` 与随后
`from rquant.runtime_service_main import resolve_legacy_schema_generation, legacy_current_generation`
都成功——注释说的「role child 在三名环境里应该死在自己的 profile 上而不是死在 import 上」
成立。

### 3. C：两份文档的生成器

| 必查项 | 结论 | 证据 |
|---|---|---|
| `runtime-recovery.json` 恰好 `{key_id, secret_hex}` canonical | ✅ | `test_the_credential_is_the_document_the_authenticator_accepts` 断言 `set(decoded) == {"key_id","secret_hex"}` 且 `canonical_json_bytes(decoded) == payload`；我跑过 |
| 0600 | ✅ | 同一条用例断言 `S_IMODE == 0o600`；我把 `PRIVATE_FILE_MODE` 改成 0o644（自补变异 SA3），**7 条用例变红** |
| `secret_hex` ≥ 32 字节 | ✅ | 实际是 `secrets.token_hex(64)` = 64 字节；下限 `MINIMUM_SECRET_BYTES = 32`；M5 去掉下限 → 1 条红 |
| `runtime-recovery-backup.json` 也产出 | ✅ | 全部字段派生自已安装画像，写完立刻过 `load_recovery_backup_config` + `validate_runtime_recovery_backup_config`；同 `--as-of` 逐字节可复现（有用例） |
| `--only-missing` 对已存在文件零改动 | ⚠️ **见 must-fix M-1** | 对 0600 的已存在文件确实零改动（用例断言 `read_bytes()` 相等）；对**非 0600** 的已存在文件会静默轮换 |
| `key_id` 与 `RQUANT_RECOVERY_SIGNER_KEY_ID` 对齐 | ✅ 而且是结构性对齐不是抄字面量 | 脚本读 `recovery.signer_key_id`；`runtime_deployment_profile.py:484` 发布的环境里 `"RQUANT_RECOVERY_SIGNER_KEY_ID": self.signer_key_id` 就是同一个值；生成器默认 `production-recovery-v1`（`build_runtime_production_inputs.py:931`）。有一条用例直接比这两者 |
| 本地测试不产生真实凭证 | ✅ | `profile` fixture 由 `build_production_runtime_profile(_inputs(tmp_path))` 造，两个路径都在 `tmp_path` 下；确定性用例自己传 `secret_hex`；我全程 `TMPDIR` 指到私有根，跑完私有根外没有任何 `runtime-recovery*.json` |
| secret 不打印 | ✅ | `summary` 的键恰好 `{path, key_id, created}`，有用例断言密钥不在 `json.dumps(summary)` 里；命令行没有任何接收密钥的参数；异常路径打印的是 `ProvisionError`/`ValueError` 文本，都不含密钥值 |

选独立脚本而不是给 `build_runtime_production_inputs.py` 加 subparser 的理由（那是扁平
argparse，runbook 与 `deploy-production.sh` 按现有 flag 调它，加 subparser 是破坏性改动）
成立，我核对过那个文件确实是扁平 argparse。

### 4. 启动顺序新发现（详见下面「第 4 条的判断」）

报告 §6 的四条事实我逐条自己核过，全部成立，行号也对：

- `runtime_builder_strategy.py:314` 开 `ReadonlySignalRouteAuthority(path=settings.signal_bus_path)`；
  它的 `__init__`（`signal_router_runtime.py:813`）调
  `ReadonlyStrategyRunnerSignalSource._require_safe_path`，缺文件时在 `:455` 抛
  `runner source is unavailable: <path>`。
- `runtime_builder_signal.py` 在 `:556-571` 先逐个构造 `ReadonlyStrategyRunnerSignalSource`
  （每个都要 `runner.sqlite3`），`bus = settings.open_store()` 在 `:589`，**在后面**。
- `runtime_builder_strategy.py:285-286` 的 `StrategyRunnerStore` 在 `:314` **之前**——
  「失败的 strategy 已经把 runner.sqlite3 留下了」这个救命的巧合成立。
- `deploy/systemd/rquant-runtime-strategy@.service:40` 的 `ReadWritePaths` 只有
  `control/strategies/%i` 与 `live/strategies/%i`，strategy 在内核层面建不出 bus；
  `Restart=on-failure` / `RestartSec=10s` / `StartLimitBurst=5` / `StartLimitIntervalSec=600s`
  → 大约 50 秒后三个 strategy 进 `failed`。

顺序本身我也自己跑通了：`test_a_strategy_role_without_the_signal_bus_still_creates_its_runner_database`
在真实 bundle 上把「strategy×3 失败但留下三份 runner.sqlite3 → router 起来建出 bus →
strategy×3 返回 0」整条走了一遍，绿。三条就绪探针的路径也跟生产画像一致
（`runtime_production_profile.py:792-798` 的 `<root>/live/strategies/<instance>/runner.sqlite3`、
`:899/:981/:1451` 的 `<root>/live/signal-bus/signal_bus.sqlite3` 与 `spool/`）。

### 5. 变异、两版本、ruff、改动面

**变异：报告 5 条我全部复跑，全部被杀，杀伤面基本吻合。**

| # | 变异 | 报告说 | 我实测 |
|---|---|---|---|
| M1 | A 回退成 `model_validate(item)` | unit 2 + integration 5 | unit **2**（含静态用例）；integration 我只跑了 2 条代表性的，**2 条全红**并复现生产原话 |
| M2 | `_require_recovery_generation_binding` 开头 return | unit 7 | unit **7**（6 + `test_recovery_production_runner_rejects_stale_unit_generation`） |
| M3 | recovery 侧改回比 `expected_generation` | unit 4 | unit **4** |
| M4 | 去掉 `resolve_legacy_schema_generation` 调用 | unit 2 + integration 3 | unit **0** + integration **3**（三条 `DID NOT RAISE`） |
| M5 | 去掉 secret 长度下限 | unit 1 | unit **1** |

**我自补 3 条：**

| # | 变异 | 结果 |
|---|---|---|
| SA1 | 在 `runtime_builder_strategy.py` 新加一处 `RuntimeServiceManifest.model_validate(<裸名字>)` | **红**（静态用例点名文件行号）✅ |
| SA2 | 同文件里另有 `raw = other.model_dump(...)` 时，`raw = manifest;  model_validate(raw)`；以及 `model_validate(_identity(manifest))` | **绿**（两处都漏）→ should-fix S1 |
| SA3 | `PRIVATE_FILE_MODE` 0o600 → 0o644 | **红**（7 条用例）✅ |

**两版本**（这是 CI 的 3.11 / 3.12 矩阵，报告 §4 写成了「base tag + 建议发版号」，答错了题——见 should-fix S3）：

| 环境 | 新增 40 用例 | 定向回归 |
|---|---|---|
| mac Python **3.11.15** | 40 passed（unit 14 + integration 26，279s） | 8 个 unit 文件 **310 passed**；`test_route_a_legacy_binding_e2e.py` **17 passed / 1 deselected**（234s） |
| Docker `python:3.11-slim`，Linux Python **3.11.16** | **40 passed**（340s） | — |
| Python **3.12.13**（私有根 venv，跑分支 clone） | **40 passed**（309s） | — |

三个环境的 40 条全绿，报告缺的 3.12 这一版由我补齐，**没有发现版本相关问题**。

**ruff**：`ruff check` 对全部 10 个改动/新增的 `.py` 文件 **All checks passed**。
`ruff format --check` 有 6 个文件会被重排，但 base 的 `runtime_service_main.py` 同样会被
重排，且 CI（`.github/workflows/ci.yml` → `scripts/check-core-quality.sh`）的 lint 是固定
白名单、不含这些文件、也不跑 `ruff format --check`——**不是本次引入，不算问题**。

**`git status` 干净**：审查开始、每次变异还原后各查一次，全部空；审查结束时
tracked 改动为零，唯一的 untracked 条目是本文件
（`.superpowers/sdd/2026-09-07-strategy-chain/pkgF-review.md`，本次审查的交付物）。

**改动面无越界**。`git diff --name-only 695e952..HEAD` 共 13 个文件：

```
.superpowers/sdd/2026-09-07-strategy-chain/pkgF-report.md
CHANGELOG.md
docs/operations/runtime-recovery-credentials.md
scripts/provision_runtime_recovery_credentials.py
src/rquant/runtime_recovery_production.py
src/rquant/runtime_recovery_service.py
src/rquant/runtime_service_main.py
tests/integration/test_route_a_recovery_binding_e2e.py
tests/integration/test_route_a_strategy_chain_e2e.py
tests/unit/test_provision_runtime_recovery_credentials.py
tests/unit/test_runtime_deployment_profile_cli.py
tests/unit/test_runtime_recovery_generation_binding.py
tests/unit/test_runtime_service_manifest_revalidation.py
```

`deploy/`、`.env`、发布原语、stage、`runtime_authority*`、`src/rquant/runtime_exec_wrapper/`
一个都没碰。`runtime_service_main.py` 的改动确实只在 `:489-507`。commit trailer 三条都带
`Co-Authored-By: Claude Fable 5.1` 与 `Claude-Session:`，未 push，未重冻结 R07，
未重生成 manifest（brief 明令不做）。

`rquant-runtime-exec.pyz` 不受影响这条我也核了：`scripts/build-runtime-exec-pyz.py:52-56`
的 `collect_sources` 只走 `src/rquant/runtime_exec_wrapper/**`，本包三个源文件都不在里面。

### 6. 与包 E 的合并风险：可以自动合，e2e 不会被打红

包 E（`ra-e-cc`，HEAD `1da5750`）相对 `695e952` 改 16 个文件。与包 F 的重叠只有
`src/rquant/runtime_service_main.py` 一个。

**自动合并已实测**：

```
git merge-tree --write-tree --name-only cc/20260907-strategy-chain cc/20260907-credstore-roles
→ exit 0，只输出树哈希 f06c1bb（无冲突段）
```

合出来的 `runtime_service_main.py` 里两边的改动都在：`:502` 是包 F 的解冻，`:731` 是包 E
的 `expected_generation=schema_generation`。

**`test_route_a_strategy_chain_e2e.py` 不会被包 E 打红**，理由是我读码确认的、不是猜的：
包 E 把 `load_systemd_runtime_capabilities` 从 `:664` 挪到 generation 解析之后并改传
`schema_generation`，而这个函数在 `runtime_capabilities.py:196-199` 一开头就是
「`CREDENTIALS_DIRECTORY` 为空就直接返回空能力」；包 F 的 e2e 通过 `RouteAWorld.run_role`
调真实 `run()`，环境里没有 `CREDENTIALS_DIRECTORY`，所以那一段对包 F 的 e2e 是空转。

**另一个更值得注意的交叉点，报告没提到**：包 E 改了**包 F 依赖的共享 world 文件**
`tests/integration/test_route_a_legacy_binding_e2e.py`（`_production_bundle` 的返回值从
3 元组变 4 元组，`_route_a_world` 同步改）。包 F 用的是 `_route_a_world` / `RouteAWorld` /
`_second_install` / `recorded_install` / `PRODUCTION_ROOT` 这些，**不直接调
`_production_bundle`**，所以源码层兼容；而且包 F 根本没改这个文件，git 层也不会冲突。
合并后建议把 `tests/integration/test_route_a_strategy_chain_e2e.py` 与
`test_route_a_recovery_binding_e2e.py` 再跑一遍确认（约 7 分钟），不是因为预期会红，
是因为这是两包唯一的语义接触面。

---

## must-fix

### M-1 `--only-missing` 对权限不是 0600 的已存在凭证会**静默轮换**，而不是保留

`scripts/provision_runtime_recovery_credentials.py:120-125`：

```python
def _is_private_regular_file(path: Path) -> bool:
    try:
        observed = path.lstat()
    except OSError:
        return False
    return stat.S_ISREG(observed.st_mode) and not stat.S_IMODE(observed.st_mode) & 0o077
```

`provision_recovery_credential` 里 `if only_missing and _is_private_regular_file(path)` 才
跳过写入。于是一个**已经存在、正在签名、但权限被人手动放宽成 0644** 的
`runtime-recovery.json`，在带 `--only-missing` 的日常复跑里会走进写入分支，被
`secrets.token_hex(64)` 换掉。后果正是脚本 docstring 和 `docs/operations/runtime-recovery-credentials.md`
反复强调要避免的那一个：**publication root 里已经签过的每一份 receipt 和 pointer 全部验不过**，
而汇总里只有 `"created": true` 一个字暗示发生了轮换。

**这不是读码推的，我实测过**（临时目录，真实画像，密钥不出私有根）：

```
mode after create: 0o600
only_missing on 0600 -> created: False  bytes unchanged: True      ← 正确
only_missing on 0644 -> created: True   bytes unchanged: False  mode now: 0o600
SECRET ROTATED SILENTLY
```

这条落在 brief「必查 3」的字面要求上（「`--only-missing` 对已存在文件零改动」），
而且失败模式是破坏性且静默的。

**建议改法（约三行，不动 A/B）**：`only_missing` 时把「存在但不私有」与「不存在」分开——
存在但 mode 不合规就 `raise ProvisionError`，把观测到的 mode 和路径说清楚，让操作员先
`chmod 0600` 再复跑；只有真的不存在才去铸新密钥。同时把 `docs/operations/runtime-recovery-credentials.md`
的 `--only-missing` 段落补一句「已存在但权限不合规会拒绝，不会替换」。
配套加一条用例：写好凭证后 `chmod 0644`，带 `--only-missing` 再跑，断言**文件字节不变**且
抛 `ProvisionError`——这条用例在当前实现下会红。

（`runtime-recovery-backup.json` 走同一个 `_is_private_regular_file`，但它不是密钥、
重写是幂等的，所以那一侧只是行为不一致，跟着一起修即可。）

---

## should-fix

### S1 静态守卫 `test_no_module_hands_a_frozen_manifest_straight_to_model_validate` 有两个盲区，而 docstring 的口气比它的覆盖大

用例自称「A repository-wide scan for the call shape that produced #218」。我造了两处它接
不住的（都保持绿）：

1. **同文件名字污染**：`_names_bound_to_a_thaw` 是**全文件**收集名字的，所以一个文件里只要
   任何地方有 `raw = <别的东西>.model_dump(...)`，名字 `raw` 就在这个文件的**每一个**调用
   点被放行。我在同一文件里写
   `def a(other): raw = other.model_dump(mode="json"); return raw` 加
   `def b(manifest): raw = manifest; return RuntimeServiceManifest.model_validate(raw)`，
   守卫**绿**。
2. **任意函数包一层就绕过**：`if isinstance(argument, ast.Call): continue` 跳过的是**所有**
   调用，不只是 `.model_dump(...)`。`RuntimeServiceManifest.model_validate(_identity(manifest))`
   守卫**绿**。

另外它只扫 `RuntimeServiceManifest` 一个类，其余 11 个易感类不在覆盖内（报告 §6.1 有说明，
但用例的 docstring 没说）。

**建议**：把名字放行改成函数作用域内的（或干脆只放行「参数表达式文本里出现
`.model_dump(`」这一种形状），把 `ast.Call` 的跳过收紧成「函数名以 `model_dump` 结尾」，
并把扫描的类集合从 1 个扩到那 12 个（豁免表按需增加条目）。
这一条不影响 #218 本身的正确性——真正保护 A 的是行为用例和真装机 e2e——所以是
should-fix 不是 must-fix。

### S2 `--runtime-root` 默认值就是生产路径

`build_argument_parser()` 给 `--runtime-root` 的 default 是
`/home/lighthouse/rquant/data/runtime`。这个脚本会**铸生产密钥**，把生产路径设成默认值、
不设成必填，在误跑（比如在错的机器上、或漏了参数）时少了一道人肉确认。当前它会因为
画像不存在而安全失败，所以只是 should-fix：建议改成 `required=True`，或保留默认但要求
一个 `--i-am-on-the-target-host` 之类的显式确认。

### S3 报告 §4「两版本数字」答的不是「两版本」

brief 与交付要求里的「两版本」是 CI 的 **Python 3.11 / 3.12 矩阵**（环境说明里写了
`uv sync --python 3.11` 和 `.venv312`）。报告 §4 写的是 base tag（`v0.32.2`）与建议发版号
（`v0.33.0`），而 §2 的证据表里两次运行**都是 3.11**（mac 3.11.15 + Docker 3.11.16），
worktree 里 `.venv312` 也不存在。3.12 这一版报告里没有证据。**我已自己补跑：3.12.13 下
40 条全绿**，所以这只是报告的证据缺口，不是代码问题；但下次报告里这一格要按 Python 版本填。

---

## note

- **N1** 报告 §1 说第四个 commit 是 `3c28fc9`，实际是 `bc326cb`；内容对得上，只是号写错。
- **N2** 报告 §2 引的复现原话是「2 validation errors」，我实测是 3 条
  （`settings.migration` / `retention_policy` / `worker`）。
- **N3** 报告的 M4 行写「unit 2 ＋ integration 3」。我用只删 `resolve_legacy_schema_generation`
  这一次调用的更紧形状，实测是 **unit 0 ＋ integration 3**。结论方向一致且更强
  （权威绑定的覆盖完全在 e2e 上，单测层面替代不了）。
- **N4** `test_the_unstubbed_pass_fails_on_its_payload_and_never_on_the_binding` 用
  `pytest.raises(Exception)` 加三条「消息里不含某串」的负向断言。这是这一层能做到的最好
  形式（recovery 载荷属另一个子系统），但它对「换了一种全新的绑定失败措辞」不敏感。
  以后 recovery 载荷那个子系统能在测试里发布一代 backup generation 时，这条可以升级成正向断言。
- **N5** `pyproject.toml` 的 `version = "0.31.0"` 落后于最新 tag `v0.32.2`。不是本包引入，
  但发版时要决定这个字段跟不跟着走。
- **N6** `docs/operations/runtime-recovery-credentials.md` 写得清楚且路径都是绝对路径、
  标了主机，符合项目规范；`--only-missing` 那一段按 M-1 补一句即可。

---

## 第 4 条的判断：这是代码缺陷（循环依赖），不只是 runbook 顺序

**我的裁定：算代码缺陷，而且应当单开 issue 修，但不属于包 F 的范围，本次窗口先按 runbook 顺序上。**

四条理由：

1. **现在能走通是靠一个巧合，不是靠设计。** 整条 live 平面能起来，唯一依据是
   `runtime_builder_strategy.py:285` 的 `StrategyRunnerStore` 恰好排在 `:314` 的
   `ReadonlySignalRouteAuthority` 前面——也就是说，**一次失败的启动留下的副产物**是
   下一个服务的前置条件。任何一次把构造顺序调开、或给失败路径加清理的重构，都会让整条
   级联在生产上死锁，而且不会有任何测试之外的信号。包 F 用
   `test_a_strategy_role_without_the_signal_bus_still_creates_its_runner_database` 把这个
   巧合钉住了，这是对的，但钉住一个巧合不等于消除一个循环。
2. **它逼着运维把 ERROR 当成正常。** #218 的 brief 明确要求「作废『strategy 预期 failed』
   的定性」。现在的顺序把这条定性从 `invalid manifests` 换成了
   `runner source is unavailable`——换了个原因，性质没变：仍然要求操作员看着三个 unit 报错
   而判断「这是对的」。
3. **它带一个真实的时间竞态。** `StartLimitBurst=5` / `RestartSec=10s` 给的窗口只有约 50 秒；
   router 那一步（建库 + 跑完一个 step 写出 `source.json`）要是慢过这个窗口，三个 strategy
   进 `failed`，就必须 `systemctl reset-failed` 再手动 start。报告已经建议 runbook 写显式
   重启不要指望自愈，这是对的，但这属于「用流程绕开一个竞态」。
4. **依赖是不对称的，所以有干净的解法。** router 需要 runner 里的**数据**，这是真依赖；
   strategy 需要的只是 bus 这个**容器**，而容器的属主恰恰是 router，且 strategy 的 sandbox
   在内核层面禁止它自建。这种「要一个别人拥有的空容器」的依赖，正常做法是把容器变成安装期
   产物，而不是运行期副产物。

**建议改法，按优先级：**

- **（首选）把 `live/signal-bus/signal_bus.sqlite3` 变成安装期/供给期产物**，用
  `SignalBusStore(path)` 在 bundle 安装或一个 provisioning 步骤里建出来。仓库里已有先例：
  `test_route_a_legacy_binding_e2e` 的 world 就是在安装路径上建 reference registry 的，
  包 F 的 e2e fixture 注释也点明「真机上 paper-broker 拥有它、strategy 只读」。
  这样循环彻底消失，**没有任何服务需要为了留下副产物而故意失败**，也不放宽任何失败关闭。
- **（次选，一行）** 把 `runtime_builder_signal.py:589` 的 `bus = settings.open_store()`
  提到 `:556` 的 runner 源构造之前。这样冷启动变成「router 先起（建出 bus，然后因为缺
  runner 而失败）→ strategy×3 成功 → router 成功」。仍然有一次预期失败，但失败的是**这个
  文件的属主**，语义上说得通，而且 router 的 sandbox 本来就允许它写 signal-bus。
- **（不建议）** 让 strategy 对缺失的 bus 降级——那是放宽失败关闭，报告自己也标了不推荐，
  我同意。

无论选哪一条，都不碰 TCB，也都会被
`tests/integration/test_route_a_strategy_chain_e2e.py::test_a_strategy_role_without_the_signal_bus_still_creates_its_runner_database`
接住（改哪一边它都会要求同步更新），这一点报告说得对。

**本次窗口的处置**：包 F 只写报告不改代码是正确的（`deploy/systemd/` 属高风险变更，
brief 也把 D 划给协调者）。建议协调者按报告 §6 的四步顺序写 runbook C-3，同时开一条
follow-up issue 记「signal bus 属主与冷启动循环」，把上面的首选改法写进去。

---

## 集成输入

**manifest 新增用例数：40**（我自己 `--collect-only` 数的，两个 e2e 文件里
`test_a_recovery_oneshot_reaches_its_payload_over_a_real_current` 有一个
`parametrize(2)`，9 个函数出 10 个 item）。分布：

| 文件 | 用例 |
|---|---|
| `tests/unit/test_runtime_service_manifest_revalidation.py` | 6 |
| `tests/unit/test_runtime_recovery_generation_binding.py` | 8 |
| `tests/unit/test_provision_runtime_recovery_credentials.py` | 10 |
| `tests/integration/test_route_a_strategy_chain_e2e.py` | 6 |
| `tests/integration/test_route_a_recovery_binding_e2e.py` | 10 |

`tests/unit/test_runtime_deployment_profile_cli.py` 只改 stub、不增减用例。
因此 `tests/manifests/full-suite-v1/index.json` 的 `full_suite.cases` 应从 **13800 → 13840**，
approved-skips 预期不变（本包没有 skip/xfail）。**四个 shard 必须重新生成**
（`scripts/full_suite_shards.py`），否则 CI 的 `full-suite-shard` job 必红——brief 明令
包 F 不做这件事，属集成动作。合入包 E 之后一次性重生成即可（包 E 也新增了测试文件）。

**CHANGELOG**：已有，不需要补。`[Unreleased]` 里 2 条 Fixed（#218 A、#218 B）+ 1 条 Added
（#218 C，`scripts/provision_runtime_recovery_credentials.py`），中文、指明根因与验收形式，
质量可直接进发版说明。

**DEPLOY / runbook 要点**（给协调者）：

1. **C-3 顺序改写**（作废「strategy 预期 failed = invalid manifests」这条定性）：
   ① 起 `rquant-runtime-strategy@` ×3，**预期这一轮失败**，失败信息必须是
   `runner source is unavailable: <root>/live/signal-bus/signal_bus.sqlite3`
   （**不是** `invalid manifests`——若还是 `invalid manifests`，说明装的不是本代）；
   ② 探针 1 转 READY 后起 `rquant-runtime-signal-router@`；
   ③ 探针 0 与探针 2 转 READY 后，**显式** `systemctl reset-failed` + 重启三个 strategy
   （不要指望 `Restart=on-failure` 自愈，约 50 秒的 `StartLimitBurst` 窗口不够稳）；
   ④ 最后起 `paper-broker` 与 `notifier`。
2. **三条就绪探针**（`ROOT=/home/lighthouse/rquant/data/runtime`，路径我核对过与生产画像
   `runtime_production_profile.py:792-798 / 899 / 981 / 1451` 一致）：
   - 探针 0：`test -f "$ROOT/live/signal-bus/signal_bus.sqlite3"`
   - 探针 1：`ls -1 "$ROOT"/live/strategies/*/runner.sqlite3 | wc -l` 等于 3
   - 探针 2：`test -f "$ROOT/live/signal-bus/spool/source.json"`
3. **recovery 两份文档要新增一个部署步骤**：bundle 装好、`current` 指向本代**之后**，在
   目标主机上跑
   `/home/lighthouse/rquant/.venv/bin/python scripts/provision_runtime_recovery_credentials.py --runtime-root /home/lighthouse/rquant/data/runtime --replay-start-date <YYYY-MM-DD> --replay-end-date <YYYY-MM-DD> --only-missing`，
   前置是 `install -d -m 0700 -o lighthouse -g lighthouse /home/lighthouse/rquant/data/recovery`。
   **首次落 `runtime-recovery.json` 属新增生产密钥材料，需 owner 单独明确授权**
   （受控自动发布模式第 7 条），不能走无人值守发布器。replay 窗口必须落在已发布生产数据集
   真实覆盖的范围内，是唯一需要人判断的输入。
4. 本包三个源文件都在 `src/rquant/`，按既有受控发布链需要**重新 stage + publish 一代**
   （runbook B-6′ → B-7）。`rquant-runtime-exec.pyz` 不受影响（我核过
   `scripts/build-runtime-exec-pyz.py` 的 `collect_sources` 只打 `src/rquant/runtime_exec_wrapper/**`）。
5. `deploy/systemd/` 的 `After=` 本次**不动**（包 F 没碰，正确）；上面第 4 条判断里的
   follow-up 若采纳首选改法，届时才需要评估要不要顺带清理这几个 unit 的顺序声明。

**发版号：同意 `v0.33.0`。** `[Unreleased]` 里已有 Added 条目（本包 C 又加一条新脚本），
按 SemVer 是 minor 不是 patch；base 是 `v0.32.2`。顺带确认 `pyproject.toml` 的 `version`
字段（现为 `0.31.0`）要不要一起对齐（note N5）。

**R07**：本包**没有**、也不应该重冻结。按 `0ddd6b1` 的先例，R07 的冻结对
（`signal_family_differential_gate.py` 的两个常量 + 那份架构文档 + 那个测试文件里的字面量，
四处一起动）要在**候选切出之前**指向届时的 `origin/main`。包 E、包 F 合入 main 之后，
下一个候选切出前需要**再做一次 R07 refreeze**，这是集成动作，四处必须同时改。

**参考（未经本次 SSH 核实）**：生产机 82.156.0.68 仍是 `v0.28.3`（取自 09-06 记录）。

---

## 审查动作清单（可复核）

| 动作 | 结果 |
|---|---|
| 读 brief / 勘察 / `gh issue view 218` / `gh issue view 207` | 完成 |
| 读全量 diff（13 文件 2300 行）与被改函数的上下文 | 完成 |
| mac 3.11.15 跑新增 5 个测试文件 | 40 passed / 279s |
| mac 3.11.15 跑 8 文件定向回归 | 310 passed / 39s |
| mac 3.11.15 跑 `test_route_a_legacy_binding_e2e.py`（#207 既有验收） | 17 passed, 1 deselected / 234s |
| Docker `python:3.11-slim`（Linux 3.11.16，`uv sync --frozen`，跑 clone） | 40 passed / 340s |
| Python 3.12.13（私有根 venv，跑 clone） | 40 passed / 309s |
| 复跑报告 M1–M5 | 5/5 全部被杀，杀伤面见上表 |
| 自补变异 SA1 / SA2 / SA3 | SA1 红、SA3 红、**SA2 绿 → S1** |
| 独立复现「12 个易感类」扫描 + 全仓 AST 交叉 | 与报告一致，6 处命中、判定一致 |
| 实证 `ReferenceRecord` 重验通过（非陷阱） | 通过 |
| 实证 `--only-missing` 在 0644 上静默轮换 | **复现 → M-1** |
| `git merge-tree` 试合包 E | 无冲突，两边改动都在合出的文件里 |
| `ruff check` 10 个改动文件 | All checks passed |
| 核对三条就绪探针路径与生产画像 | 一致 |
| 核对 `rquant-runtime-exec.pyz` 打包范围 | 不受本包影响 |
| `--collect-only` 数新增用例 | 40 |
