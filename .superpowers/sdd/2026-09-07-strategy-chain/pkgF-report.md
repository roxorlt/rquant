# 包 F 报告：live 平面策略链解锁（#218 A+B+C）

**worktree**：`/Users/roxor/brain/30-projects/rQuant/.worktrees/ra-f-cc`
**分支**：`cc/20260907-strategy-chain`，base `origin/main` = `695e952038aff8b426632233511d351fac2c7353`（= tag `v0.32.2`）
**状态**：五个 commit 已在本地分支上（第五个是独立审查 must-fix M-1 的修复），**未 push**；未重冻结 R07、未重生成 manifest、未动 `deploy/systemd/`。

---

## 1. 改动清单

| commit | 内容 | 文件 |
|---|---|---|
| `f16c100` | **A**：strategy 完成签名器重校验前先解冻 | `src/rquant/runtime_service_main.py`（+12/-1，仅 :489-507 重校验区）；新增 `tests/unit/test_runtime_service_manifest_revalidation.py`、`tests/integration/test_route_a_strategy_chain_e2e.py` |
| `85d7246` | **B**：recovery generation 改核自己的命名空间 + 保留权威绑定 | `src/rquant/runtime_recovery_production.py`、`src/rquant/runtime_recovery_service.py`；新增 `tests/unit/test_runtime_recovery_generation_binding.py`；改 `tests/unit/test_runtime_deployment_profile_cli.py`（三个 stub profile 补上 retention manifest） |
| `6c25b09` | **C**：recovery 两份凭证/配置文档的生成器 | 新增 `scripts/provision_runtime_recovery_credentials.py`、`tests/unit/test_provision_runtime_recovery_credentials.py`、`tests/integration/test_route_a_recovery_binding_e2e.py`、`docs/operations/runtime-recovery-credentials.md` |

第四个 commit `bc326cb` 是 `CHANGELOG.md` 的 `[Unreleased]`（两条 Fixed + 一条 Added）与本报告。

**第五个 commit 是独立审查 must-fix M-1 的修复**（见 §9）：`--only-missing` 对「已存在但权限不是
0600」的文档，原来会走进写入分支静默铸新密钥；现在把「存在但不私有」与「不存在」分开，前者拒绝
并打印实测 mode。

### A 的具体改法（`runtime_service_main.py:489-507`）

```python
for item in getattr(profile, "manifests", ()):
    raw = item.model_dump(mode="json") if isinstance(item, RuntimeServiceManifest) else item
    try:
        profile_manifests.append(RuntimeServiceManifest.model_validate(raw))
    except ValueError as exc:
        raise ValueError("strategy completion signer profile contains invalid manifests") from exc
```

指纹绑定（`:498-500` 的 `manifest_fingerprint` 相等）一字未改；「不是模型的项照旧重验」保留。

### B 的具体改法

`cmd_runtime_recovery_production` 里那一行跨命名空间比较，换成 `_require_recovery_generation_binding()` 的两条：

1. **recovery 命名空间**：`recovery.profile_generation` 必须等于 `bundle_recovery_profile_generation(profile)`——即 profile 里 `artifact_retention` manifest 的 `recovery_profile_generation` 设置。这是 bundle 在 recovery 段之外唯一记录同一个哈希的地方，而且它已经进了 `generation-basis.json`（legacy generation id 就是那份 basis 的哈希），所以不是自己比自己。找不到 / 找到多于一个 → 拒绝。
2. **权威命名空间**：`resolve_legacy_schema_generation(manifest_path, expected_generation, runtime_root, legacy_generation)`——复用 #207 的实现（**只 import，不改 `runtime_service_main`**）。

`runtime_recovery_service.main()` 现在按各自名字转发 `manifest` / `expected_generation`，`expected_profile_generation=None`；手工 CLI（`rquant runtime-recovery-production`）继续用 `--expected-profile-generation` 钉 recovery 命名空间。**两者都不给 → 拒绝**，不存在「不绑定就跑」。

### C 的具体改法

选了独立脚本 `scripts/provision_runtime_recovery_credentials.py` 而不是给 `build_runtime_production_inputs.py` 加子命令：后者是一个扁平 argparse，runbook 与 `deploy-production.sh` 都按现有 flag 调它，加 subparser 会是破坏性改动。

- `runtime-recovery-backup.json`：**全部字段从已安装画像派生**，操作员只给 `--replay-start-date` / `--replay-end-date`（真实数据集覆盖范围，是唯一需要判断的输入）与可选 `--as-of`。同画像同 `--as-of` 逐字节可复现。
- `runtime-recovery.json`：`{"key_id": <画像 signer_key_id>, "secret_hex": secrets.token_hex(64)}`，canonical JSON、0600、暂存改名原子落盘。**密钥从不打印**（汇总只有 path / key_id / created），**没有任何传入密钥的参数**，所以不经过 shell history、argv、终端。
- `--only-missing`：已存在则保留并核对（key_id 不符 → 拒绝）；轮换会让 publication root 里已签的每份 receipt 失效，所以日常复跑一律带它。
- 写完立刻用 unit 将要用的同一套加载器读回核对（`load_recovery_backup_config` + `validate_runtime_recovery_backup_config`、`RecoveryBackupAuthenticator.from_file`），核不过退出 1。
- **本地一次真实凭证都没生成**：所有测试都在 `tmp_path` 下，确定性用例自己传 `secret_hex`。

---

## 2. 端到端证据

复现基线（改之前）：在真实 bundle + 真实 staged generation 下直接调
`build_runtime_strategy_completion_attestation_signer`，报的就是生产现场那一句：

```
ValueError: strategy completion signer profile contains invalid manifests
  caused by: 2 validation errors ... input was not a valid JSON value
             [type=invalid-json-value, input_value=..., input_type=mappingproxy]
```

`run_role("strategy_live")` 同样死在这里（`runtime_service_main.py:736`）。

### 本机（macOS，`.venv` = Python 3.11.15）

| 套件 | 结果 | 耗时 |
|---|---|---|
| `tests/unit/test_runtime_service_manifest_revalidation.py` | 6 passed | 4.6s |
| `tests/unit/test_runtime_recovery_generation_binding.py` | 8 passed | 1.5s |
| `tests/unit/test_provision_runtime_recovery_credentials.py` | 10 passed | 38s |
| `tests/integration/test_route_a_strategy_chain_e2e.py` | 6 passed | 95s |
| `tests/integration/test_route_a_recovery_binding_e2e.py` | 10 passed | 142s |
| 定向回归 8 个 unit 文件（`test_runtime_service_main` / `test_tp9_role_child_runtime` / `test_runtime_recovery_service` / `test_runtime_exec_wrapper` / `test_preflight` / `test_runtime_deployment_profile_cli` + 两个新文件） | **310 passed** | 32s |
| `tests/integration/test_route_a_legacy_binding_e2e.py`（#207 的既有验收，我 import 了它的 world 与两个 helper） | 17 passed, 1 deselected（`linux_exact` gate） | 234s |

### Linux 端到端（Docker `python:3.11-slim`，Python 3.11.16，`uv sync --frozen`）

```
tests/unit/test_runtime_service_manifest_revalidation.py ......          [ 15%]
tests/unit/test_runtime_recovery_generation_binding.py ........          [ 35%]
tests/unit/test_provision_runtime_recovery_credentials.py ..........     [ 60%]
tests/integration/test_route_a_strategy_chain_e2e.py ......              [ 75%]
tests/integration/test_route_a_recovery_binding_e2e.py ..........        [100%]
======================== 40 passed in 306.59s (0:05:06) ========================
```

### 这两个 e2e 文件里「真」的部分

沿用 `tests/integration/test_route_a_legacy_binding_e2e.py` 的 world：真实
`install_runtime_deployment_bundle` legacy 根（真的 `current -> generations/<64hex>` 相对符号链接、真的
`deployment-profile.json`）、真的 `runtime-authority-stage --legacy-runtime-root` 采集本代 manifest 并写
`legacy-binding.json`、真的发布进 root-owned 权威链、wrapper 自己的 `resolve_launch` 派生并逐条校验过
argv。唯一的替换是把 `PRODUCTION_ROLE_POLICY` 冻死的 `--control-root` 前缀搬到临时 runtime root
（那个文件里已有 `test_the_remap_is_only_the_frozen_control_root_prefix` 钉住只改这一处）。

- **A**：三个 `strategy_live` 实例全部 `run()` 返回 0，进了 `run_runtime_service_manifest` 并跑完一个
  iteration；三份 `runner.sqlite3` 落盘。
- **B/C**：两个 recovery role 都用 wrapper 派生的 argv 过了 generation 绑定与三道 config 墙，
  把画像解析出的参数交给 `cmd_runtime_recovery`（recovery 载荷本身需要已发布的 backup generation，
  属另一个子系统的验收，因此是**观察**而非替换；另有一条完全不打桩的用例，证明剩下的失败是那个载荷、
  从来不是绑定）。
- **拒绝面**：`current` 指向拷贝代（画像加载器就拒）、指向**合法安装的兄弟代**（只有
  `legacy-binding.json` 看得见，是路线 A 真正的风险）、manifest 来自另一代权威 generation、
  bootstrap 代压在真实 `current` 上——四条全部拒绝。

---

## 3. 变异表（6 条，全部被杀）

| # | 变异 | 位置 | 被哪些用例杀 |
|---|---|---|---|
| **M1** | A 回退：`model_validate(item)`，不解冻 | `runtime_service_main.py:503` | unit 2/6（`test_the_signer_opens_over_a_profile_whose_other_manifests_carry_nested_settings`、静态用例 `test_no_module_hands_a_frozen_manifest_straight_to_model_validate`）＋ integration **5/6**（三条 strategy 用例 + spool + notifier） |
| **M2** | B 放宽为不核对：`_require_recovery_generation_binding` 开头直接 `return` | `runtime_recovery_production.py` | unit **7**（新文件 6 条 + `test_recovery_production_runner_rejects_stale_unit_generation`） |
| **M3** | B 核错命名空间：把 recovery 侧比较改成 `generation != expected_generation`（即恢复原缺陷的形状） | `runtime_recovery_production.py` | unit **4**（三条 bundle-record 用例 + 无 `current` 用例） |
| **M4** | B 只留 recovery 命名空间，丢掉权威绑定（`resolve_legacy_schema_generation` 不再调用） | `runtime_recovery_production.py` | unit 2 ＋ integration **3**（兄弟代、外来 manifest、bootstrap 代——三条全部 `DID NOT RAISE`） |
| **M5** | C 去掉 secret 长度下限 | `provision_runtime_recovery_credentials.py` | unit 1（`test_a_secret_below_the_authenticator_floor_is_refused`） |
| **M6** | C 的 M-1 修复回退：`_should_write` 把「不私有」重新并回「不存在」 | `provision_runtime_recovery_credentials.py` | unit **3**（0644 凭证、0644 backup config、符号链接三条全部 `DID NOT RAISE`） |

M4 值得单说：它只被 2 条 unit 杀、却被 3 条 integration 杀，说明**权威绑定这一半的覆盖全在 e2e 上**，
单测层面替代不了。

---

## 4. 两版本数字

| | 数字 | 说明 |
|---|---|---|
| **本分支的 base** | `v0.32.2` = `695e952` | `git describe --tags origin/main` 就是它；本包三个 commit 全部在它之上 |
| **建议发版号** | `v0.33.0` | `[Unreleased]` 里已经有 Added 条目（本包 C 又加了一条新脚本），按 SemVer 是 minor，不是 patch |

参考（**未经本次 SSH 核实**，取自 09-06 的记录）：生产机 82.156.0.68 上仍是 `v0.28.3`。
本包三个 commit 都改 `src/rquant/`（C 只加 `scripts/`，但同一次发布一起走），
所以按既有受控发布链需要**重新 stage + publish 一代**（runbook B-6′ → B-7），
`rquant-runtime-exec.pyz` 不受影响（`collect_sources` 只打 `runtime_exec_wrapper/**`，本包三个源文件都不在里面）。

---

## 5. 就绪探针命令

**先看一条新发现改变了顺序**（详见 §6）：`strategy_live` 起来还需要
`live/signal-bus/signal_bus.sqlite3` 存在，而只有 `signal_router` 会创建它。三条探针按下面的顺序用。

```bash
ROOT=/home/lighthouse/rquant/data/runtime
```

**探针 0 —— signal bus 是否已存在**（决定要不要先让 strategy 空转一轮）：

```bash
test -f "$ROOT/live/signal-bus/signal_bus.sqlite3" && echo READY || echo ABSENT
```

**探针 1 —— 三份 runner 数据库都在**（strategy 起完、放行 signal-router 之前）：

```bash
test "$(ls -1 "$ROOT"/live/strategies/*/runner.sqlite3 2>/dev/null | wc -l)" -eq 3 \
  && echo READY || echo WAIT
```

轮询形式（最多等 60 秒）：

```bash
for _ in $(seq 1 30); do
  [ "$(ls -1 "$ROOT"/live/strategies/*/runner.sqlite3 2>/dev/null | wc -l)" -eq 3 ] && break
  sleep 2
done
ls -l "$ROOT"/live/strategies/*/runner.sqlite3
```

**探针 2 —— router 跑完至少一个 step**（放行 paper-broker 与 notifier 之前）：

```bash
test -f "$ROOT/live/signal-bus/spool/source.json" && echo READY || echo WAIT
```

轮询形式：

```bash
for _ in $(seq 1 30); do
  [ -f "$ROOT/live/signal-bus/spool/source.json" ] && break
  sleep 2
done
ls -l "$ROOT/live/signal-bus/spool/"   # 期望看到 records/ 与 source.json
```

三条探针的产物在 `tests/integration/test_route_a_strategy_chain_e2e.py` 里都是被断言过的真实文件，
不是从代码推的。

---

## 6. 新发现：strategy_live 与 signal_router 互相等对方（**会改 D 的顺序**）

勘察 §2 说「Q1 修好后 runner.sqlite3 会自动出现 → router 自然通」。**实测下来还差一环。**

- `strategy_live` 的构造器在 `runtime_builder_strategy.py:314` 打开
  `ReadonlySignalRouteAuthority(path=settings.signal_bus_path)`，即
  `<root>/live/signal-bus/signal_bus.sqlite3`；文件不存在就抛
  `ValueError: runner source is unavailable: <...>/signal_bus.sqlite3`（`signal_router_runtime.py:455`）。
- 16 个 unit 里**只有 `signal_router` 会创建这个文件**（`runtime_builder_signal.py:589`
  `bus = settings.open_store()` → `SignalBusStore._initialize`）。`paper_consumer` 也开写模式，但它不在
  `PRODUCTION_ROLE_POLICY` 里。
- 而 `signal_router` 在 `:560-571` 先构造 `ReadonlyStrategyRunnerSignalSource`（要 `runner.sqlite3`），
  **早于** `:589` 那句建库，所以 router 在建 bus 之前就死了。
- sandbox 把这件事钉死：`rquant-runtime-strategy@.service:40` 的 `ReadWritePaths` 只有
  `control/strategies/%i` 与 `live/strategies/%i`，strategy 在内核层面就无权创建 bus。

**能走通的顺序（e2e 实测）**：strategy 的 builder 在开 route authority **之前**就构造了
`StrategyRunnerStore`，所以一个因为缺 bus 而失败的 strategy **已经把 `runner.sqlite3` 留下了**。于是：

1. 起 `rquant-runtime-strategy@` ×3 —— **预期这一轮失败**，失败信息必须是
   `runner source is unavailable: .../signal_bus.sqlite3`（**不是** `invalid manifests`）；探针 1 应转 READY。
2. 起 `rquant-runtime-signal-router@` —— 它现在能找到三份 runner，建出 `signal_bus.sqlite3` 与
   `spool/`，跑完一个 step 写出 `source.json`；探针 0 与探针 2 转 READY。
3. **再起（或让 `Restart=on-failure` 自愈）** 三个 strategy —— 这一轮返回 0，进服务循环。
4. 最后起 `paper-broker` 与 `notifier`。

注意 `rquant-runtime-strategy@.service` 的 `StartLimitIntervalSec=600s` / `StartLimitBurst=5` /
`RestartSec=10s`：第 1 步失败后大约 50 秒内必须完成第 2 步，否则三个 strategy 进 `failed`，第 3 步就得
`systemctl reset-failed` 后手动 `start`。**建议 runbook 直接写成显式的第 3 步重启，不要指望自愈。**

这一条我**只写报告、没改任何 unit 文件**（`deploy/systemd/` 属高风险变更）。如果 owner 后续想根治，
两种改法都不碰 TCB：把 router 的 `bus = settings.open_store()` 提到 runner 源检查之前（一步就通），
或者给 strategy 的 route authority 一个「首轮缺 bus 只降级不拒绝」的口子（**不推荐**，那是放宽失败关闭）。
`tests/integration/test_route_a_strategy_chain_e2e.py::test_a_strategy_role_without_the_signal_bus_still_creates_its_runner_database`
已经把当前行为钉住了，改哪一边都会被它接住。

## 6.1 同类重校验陷阱的全仓扫描（勘察要的「第三处」）

做法：动态枚举 879 个 `RuntimeContractModel` 子类，取出「有 `JsonValue` 字段且该字段**没有**
`mode="before"` 解冻校验器」的 12 个类，再用 AST 在 `src/rquant` 里找 `<那些类>.model_validate(<裸名字>)`。
命中 6 处，逐一核过：

| 位置 | 判定 |
|---|---|
| `src/rquant/runtime_service_main.py:504` | **本次修的那处**，现在传的是解冻后的 `raw` |
| `src/rquant/signal_family_root_verifier.py:2711` | `row` 来自 `strict_canonical_json_loads` 解出的 list，不是模型实例——**不是陷阱** |
| `src/rquant/page_control.py:2552` | `response` 是 transport 返回的 dict——**不是陷阱** |
| `src/rquant/reference_data_registry.py:1392 / 1560 / 1590` | `record: ReferenceRecord` 确实是已构造实例，但 `canonicalize_payload` 走 `json.loads` 再包一层 `MappingProxyType`，**嵌套值全是原生 JSON 类型**；实测重验通过——**不是陷阱** |

**结论：修完这一处，`src/rquant` 里没有同类残留。** 这个扫描已经固化成
`tests/unit/test_runtime_service_manifest_revalidation.py::test_no_module_hands_a_frozen_manifest_straight_to_model_validate`
（只针对 `RuntimeServiceManifest`，带一条具名豁免与理由），M1 变异会把它打红。

---

## 7. 与包 E 的合并说明

包 E（`ra-e-cc`，#215/#216）同时在改 `runtime_service_main.py` 的 **capability 校验区**
（把校验挪到 generation 解析之后），以及 `runtime_capabilities.py` / `runtime_authority.py` /
`runtime_service_control.py` / `adapter/tushare.py` / `notify/api.py`。

- 我在 `runtime_service_main.py` 里**只改了 `:489-507` 这一个循环体**（+12/-1），
  没有碰 `load_systemd_runtime_capabilities` 调用点（`:670` 一带）、没有碰 `run()` 的 generation 解析、
  没有碰 `resolve_legacy_schema_generation` 本体（B 只是 import 它）。
- 两边在同一文件的改动相隔约 170 行，**预期 git 能自动 merge**；先合入者为准，后者 rebase 即可。
- 唯一需要人看一眼的交叉点：包 E 若调整了 `run()` 里 `build_runtime_strategy_completion_attestation_signer`
  的调用位置或前置条件，`tests/integration/test_route_a_strategy_chain_e2e.py` 会立刻报出来（它跑真实
  `run()`）。
- 其他文件零重叠：B 改 `runtime_recovery_production.py` / `runtime_recovery_service.py`，C 全是新增文件。

## 8. 边界确认

- 五个 commit 全在本地分支 `cc/20260907-strategy-chain` 上，工作区干净。
- **未 push**、未重冻结 R07、未重生成 manifest、未动 `deploy/systemd/`、`.env`、发布原语、stage、
  `runtime_authority*`、wrapper。
- 没有 skip / xfail；没有放宽任何失败关闭（B 的两条核对都能拒，M2/M3/M4 是证据）。
- 本机未生成任何真实凭证；生产机上首次落 `runtime-recovery.json` 属新增生产密钥材料，
  **需要 owner 单独明确授权**（受控自动发布模式第 7 条），操作说明见
  `docs/operations/runtime-recovery-credentials.md`。


---

## 9. 独立审查 must-fix M-1 的修复

**审查结论**：`PKGF-REVIEW-APPROVED`，1 条 must-fix，报告在
`.superpowers/sdd/2026-09-07-strategy-chain/pkgF-review.md`。

**M-1**：`scripts/provision_runtime_recovery_credentials.py` 的 `_is_private_regular_file`
把「不存在」和「存在但 mode 带 group/other 位」都答成 `False`，于是
`if only_missing and _is_private_regular_file(path)` 让一个**已经在签名、但被人 `chmod 0644`**
的 `runtime-recovery.json` 走进写入分支，被 `secrets.token_hex(64)` 静默换掉——正是
`--only-missing` 存在的目的所要避免的后果（publication root 里已签的每份 receipt / pointer 全部
验不过）。审查员实测：0600 复跑 `created: False` 字节不变；改 0644 后同一条命令 `created: True`
密钥被换。

**改法**：`_is_private_regular_file` 换成三态的 `_classify_existing_document`
（`absent` / `private` / `insecure`，第二个返回值是实测 mode）加 `_should_write`：

- 不带 `--only-missing`：照旧写（操作员明确要求重新生成，把 0644 覆盖成 0600 正是想要的）；
- 带 `--only-missing`：`absent` → 写；`private` → 保留；**`insecure` → `ProvisionError`**，
  报错里带路径、实测 mode（`0o0644`）、期望 mode（`0o0600`）和 `chmod 0600 <path>` 的具体命令。
- 「不是普通文件」（例如有人在凭证路径上放了符号链接）也归 `insecure`——同样不静默替换。
- `runtime-recovery-backup.json` 跟着走同一条规则：它不是密钥，但静默重写会换掉 `config_id`，
  而 `--only-missing` 的语义就是「保留现有的」。

**新增用例 4 条**（`tests/unit/test_provision_runtime_recovery_credentials.py`）：

| 用例 | 断言 |
|---|---|
| `test_only_missing_refuses_a_credential_whose_mode_was_widened` | 0644 已存在 ⇒ `ProvisionError`；报错含 `0o0644` / `0o0600` / 路径；**文件字节不变**；密钥不出现在报错里 |
| `test_only_missing_refuses_a_backup_config_whose_mode_was_widened` | 同上，作用在 backup config 上 |
| `test_only_missing_refuses_a_credential_path_that_is_not_a_regular_file` | 凭证路径是符号链接 ⇒ 拒绝，链接与目标都没被动 |
| `test_without_only_missing_a_widened_credential_is_replaced_and_reprivatised` | 不带 `--only-missing` 时仍然重写，且写完是 0600（拒绝是 `--only-missing` 的规则，不是生成器的） |

**变异 M6**：把 `_should_write` 里的三态判定并回原来的两态（`if state != "private": return True`），
上面前三条用例全部 `DID NOT RAISE` 变红。

**文档**：`docs/operations/runtime-recovery-credentials.md` 的 `--only-missing` 段落补了这条行为、
一段真实报错示例，以及一句「不要为了绕过报错去掉 `--only-missing`」。

审查里的 should-fix（S1 静态守卫的两个盲区、S2 `--runtime-root` 的生产默认值、S3 报告「两版本」
答错题）与 5 条 note 未在本 commit 处理，等协调者裁决。**S3 的正解**：CI 的两版本是 Python
3.11 / 3.12 矩阵；本包本机与 Docker 两次都是 3.11（3.11.15 / 3.11.16），3.12.13 那一版由审查员
补跑，同样 40 条全绿。
