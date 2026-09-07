# 包 E 独立审查（#215 credstore 组 + #216 换代残留心跳）

审查人：独立审查员（Opus），2026-09-07。
被审对象：worktree `/Users/roxor/brain/30-projects/rQuant/.worktrees/ra-e-cc`，分支
`cc/20260907-credstore-roles`，base `695e952`，6 个 commit（`9b4a9fe` → `1da5750`）。
判据：`route-a-pkgE-brief.md` 改法 1–6 与边界、`route-a-rulings.md` 裁决 13–16、
`gh issue view 215 216`。
审查方法：所有结论都在本机重新跑过一遍，不采信实现报告里的任何数字。
私有根 `/Users/roxor/rq-raer-review`（审查结束后删除）。

---

## 裁定

**有条件通过。** 改法 1–6 全部达成，我独立复跑的证据与实现报告一致；三处需要修的问题里
只有一条是必须改的，且是文档而不是代码。

| | 数量 | 内容 |
|---|---|---|
| must-fix | 1 | `DEPLOY.md` 换代段里那两条备份命令在生产主机上跑不出来，会让「删 `current.json`」之后无法回滚 |
| should-fix | 4 | Route B 下的静默降级窗口；实现报告三处与事实不符的陈述（`ruff format`、manifest 后果、CI 接线建议）；CHANGELOG 未写 |
| note | 6 | 见下 |

代码本身我没有找到必须改的问题。改动 3 与改动 4 这两处信任面变更的安全结论见专门一节，
结论是**不放宽任何东西**。

---

## 逐条核读

### 1. 改动 3：wrapper 环境白名单（裁决 13）

**只有 7 个 role 变了，其余一字未动 —— 已核实。**
`src/rquant/runtime_authority.py` 的 `PRODUCTION_ROLE_POLICY` 共 28 条：7 条换成新常量
`_CAPABILITY_ROLE_ENVIRONMENT`，20 条仍是 `_RUNTIME_ROLE_ENVIRONMENT`，`lab_claim_finalizer`
仍是它自己那五个名字。我用脚本把 28 条的白名单逐条打出来核对过，与 `git diff` 完全一致。

这 7 个 role 恰好等于 `runtime_capabilities.CAPABILITY_KEYS` 的 7 个 kind
（`reference_slow_source`、`reference_slow_publisher`、`market_minute_source`、
`auction_match_source`、`daily_close_source`、`notifier`、`artifact_retention`），
也恰好等于 `deploy/systemd/` 里 7 个带 `LoadCredentialEncrypted=` 的 unit，
也恰好等于 sealer 的 `_SERVICE_KINDS`。三方口径一致，我逐一 grep 核对过。

**测试锁住了。** `tests/unit/test_credstore_capability_delivery.py::
test_only_the_capability_roles_receive_the_credentials_directory` 用逐条精确元组断言，
不是「包含」断言。我自己写的变异 S3（往 `_CAPABILITY_ROLE_ENVIRONMENT` 里塞一个 `APP_ENV`）
立刻红：`AssertionError: assert ('APP_ENV', ...'LC_ALL', 'TZ') == ('CREDENTIALS...'LC_ALL', 'TZ')`。
也就是说「顺手带进别的变量」这条路被钉死了。

新常量 `("CREDENTIALS_DIRECTORY", "LANG", "LC_ALL", "TZ")` 已排序去重，满足
`RuntimeProfileRole.__post_init__` 的 `names == tuple(sorted(set(names)))` 硬校验，
也满足 `_verify.build_child_environment` 对名字的三条限制（ASCII、全大写、不以 `PYTHON`/`LD_` 开头）。

**profile_id 会变这件事，机制上核实过。** `RuntimeProfileRole.payload()`
（`runtime_authority.py:765-773`）把 `environment_allowlist` 写进 profile 文档，
`profile_id` 是整份文档的 sha256，所以改白名单必然换 `profile_id`。
文件里已有的两条注释（`:107`、`:116`）记录过同类先例，说明这是既定语义。

**与 runbook §0.6 B-8 的一致性：一致。** runbook 第 239 行 B-8 那条写的是
「首发没有 prior，唯一回滚是：先停掉已启动的 unit，再 `sudo rm -f
/var/lib/rquant/runtime-authority/current.json`」；`DEPLOY.md` 新增段的 ①③④⑤ 与之同形，
②（备份两份 root 文档）是 runbook 没写而这里补上的一步，方向正确——没有它就真的回不去 sequence 3。
我另外核实了 `runtime_authority_publish.py`：`publications.jsonl` 只追加、从不回读校验，
`_stage` 只用 `(sequence == 1) is (previous is None)` 判首发，没有任何外部单调计数器，
所以「删 `current.json` → 以 sequence 1 首发 → 出问题再把两份文档 `cp -p` 回去」在机制上成立。

**但那两条备份命令本身跑不出来 —— 见 must-fix 1。**

### 2. 改动 4：capability 的 generation 命名空间（裁决 15）

**加载点确实挪到 generation 解析之后。** `runtime_service_main.run()` 里
`load_systemd_runtime_capabilities` 从原来的第 667 行（manifest 加载之后）挪到
`schema_generation` 算出来之后（现第 714 行）。我逐行读了中间那段，只有
`_retention_schema_resolver(manifest)` 和几个变量初始化，都不引用 `runtime_capabilities`，
挪动不改变任何求值顺序上的依赖。

**权威链绑定仍然保留 —— 这是我最关心的一条，结论是没有削弱。** 三层绑定同时在：

1. `run()` 开头 `load_authority_service_manifest(args.manifest,
   expected_generation=args.expected_generation)` 先把 manifest 绑到**权威链** generation；
2. `resolve_legacy_schema_generation()`（#207 / 包 A 的产物）同时校验两个命名空间：
   manifest 文件必须坐在 `<权威链 generation>/manifests/` 下，且 root-owned
   `legacy-binding.json`（它的 sha256 在 full-manifest 里，而 full-manifest 的 sha256 *就是*
   权威链 generation id）必须指名这个 runtime root 与当前 `current` 指向的那一代；
3. 凭据再绑到第 2 步算出来的 bundle generation。

所以凭据是**经由一条 root 签名的链**传递地绑到权威链上的，不是「换了个宽松的 id 去比」。
与 #207 / 包 A 的交叉核对不冲突：用的就是包 A 那个函数的返回值，没有第二条求值路径。

**M5 复跑：红。** 我把 `expected_generation=schema_generation` 改回
`args.expected_generation`，跑六个 role 进主循环：
`6 failed`，六条全是 `ValueError: systemd capability credential generation does not match runtime`。

**Route B 明确拒绝：部分达成。** `expected_generation is None`（Route B 无 legacy bundle）时，
只要凭据目录在手就抛
`runtime capability credential cannot be bound without a deployment generation`
（单元测试 `test_without_a_deployment_generation_a_present_credential_is_refused` 覆盖；
我的变异 S1 把这条拒绝去掉，测试立刻红：`Failed: DID NOT RAISE <class 'ValueError'>`）。
生产主机上 7 个 unit 都带 `LoadCredentialEncrypted=`，白名单也放行了，
所以 Route B 下这些 role 一定拿得到目录、一定会被拒。**但**凭据目录不在手时是静默返回空映射，
见 should-fix 1。

### 3. 改动 1/2 与缺陷 4：惰性化与 `backup_token=""`

**三处模块级 `settings` 都惰性化了**：`adapter/tushare.py`、`notify/api.py`、`notify/log.py`，
写法与 TP9 同形（`_settings()` 先看 `globals().get("settings")`，再退到 `get_settings()`；
外加 PEP 562 `__getattr__` 让 `模块.settings` 仍可读可 monkeypatch）。
我专门跑了依赖这个接缝的既有测试来确认没打断：`test_notify_api.py`（`mock.patch(
"rquant.notify.api.settings")`）、`test_surge_watch.py`（`monkeypatch.setattr(
notify_api.settings, ...)`）等 11 个文件 **281 passed**。

**`backup_token=""` 的语义确认为「没有备份 token」，不是「拿空 token 去调 Tushare」。**
我读了两处源码：
- `TushareAdapter.__init__`：`self._backup_token = backup_token if backup_token is not None
  else _settings().tushare_token_backup` —— 传 `""` 就是 `""`，不会回落 `Settings`；
- `_switch_to_backup`（`tushare.py:88-94`）：`if self._backup_token and not self._using_backup:` ——
  空串为假，永不切换，也就永远不会有一次「用空 token 发请求」。

主 token 那侧不受影响：`self._primary_token = token or _settings()...`，而
`_tushare_daily_close_fetcher` 在构造之前已经用
`if not token: raise RuntimeError("TUSHARE_TOKEN_MAIN capability is required")` 挡住了空值。

**PA-1 同款子进程探针确实有区分力 —— 我按两种方式各跑了一遍。**
- 忠实版 M1（把 `from rquant.config import settings` 真的加回模块级，并把调用点改回 `settings.`）：
  子进程探针 `2 failed, 2 passed`，报错原文
  `pydantic_core._pydantic_core.ValidationError: 5 validation errors for Settings`；
- **同一个 M1 打在 e2e 上：`2 passed`（全绿）。**
  这就复现了实现报告自己承认的那件事：同进程的端到端在结构上看不见 import 期的 `Settings`
  （pytest 进程能构造 `Settings`，且 `get_settings()` 早被缓存）。探针不是补充，是唯一的检测手段。
  报告主动写下这条「打偏的变异」并解释为什么改用探针，我认为这是加分项。

M9 复跑（去掉 `backup_token=backup_token`）：`2 failed, 40 passed`，
`TypeError: FakeAdapter.__init__() missing 1 required keyword-only argument: 'backup_token'`
加上探针那条 `ValidationError: 5 validation errors for Settings`。

### 4. 改动 5：失败关闭与两种措辞（裁决 16）

`load_systemd_runtime_capabilities` 在「kind 需要 capability、但没有凭据目录、且确实跑在
systemd unit 下」时明确抛错，措辞按证据分成两句：

- `/run/credentials/<unit>/capabilities.json` **在**：
  `systemd did load it for unit <unit>, so CREDENTIALS_DIRECTORY was dropped between the unit
  and this process: the runtime profile's environment allowlist for this role does not carry
  CREDENTIALS_DIRECTORY` —— 指向 profile 白名单；
- **不在**：`systemd loaded no capabilities.json for unit <unit>: check the unit's
  LoadCredentialEncrypted= line and the sealed credstore entry for this instance` —— 指向 unit 与 credstore。

两句互斥，测试也断言了互斥（一句里出现 `LoadCredentialEncrypted` 时另一句的关键词必须不出现）。
另有第三句管「目录在但里面没有 `capabilities.json`」（凭据 id 对不上）。
这三条都是**收紧**，没有任何一条放宽既有的失败关闭。

### 5. #216：supersede 三合一条件（裁决 14）

条件是 `status is STOPPED` ∧ `stopped_at is not None` ∧（`owns_service_lock` 或
`not _service_lock_is_held(root, spec)`），三者缺一不可，与裁决 14 逐字对应。

**「进程仍活着」继续拒绝 —— 核实过，而且这个结论有时序保证。**
我读了 `stop()`（`runtime_service_control.py:425-446`）：**先** `_publish` 写下
`status=stopped` + `stopped_at`，**最后**才 `flock(LOCK_UN)` + `close`。
所以不存在「锁已放、心跳还没写成 stopped」的窗口，「stopped 且锁空」等价于「优雅停过且人已走」。
被 kill / 主机崩掉留下的非 stopped 心跳仍然拒绝，需要人看。

`_lock_path_for` 与 `__init__` 里的 `self._lock_path` 是同一条路径
（`root/locks/<sha256({"service_id":...})>.lock`），我对照过两处的算法，一致。
探针取完锁立即 `LOCK_UN` 并 `close`，测试
`test_the_liveness_probe_leaves_the_lock_free` 钉住了「读不会变成占锁」。
探针答不出来（`OSError`）一律算「被持有」，方向是安全的那一边。

**对其余两个 `read_heartbeat` 调用点的影响：没有回归。**
`runtime_deployment_rollout.py:540` 本来就把 `ValueError` catch 成 `return False`，
现在拿到 `None` 也是 `return False`，行为不变；
`inspect_runtime_health` 原来会把这个 `ValueError` 抛到调用者，现在降级成 `MISSING` + `stale`，
是改善不是削弱。

变异复跑：
- M7（只看 `status=stopped ∧ stopped_at`，去掉锁探测）：`2 failed, 7 passed`，
  两条都是 `Failed: DID NOT RAISE <class 'ValueError'>`；
- 我自己的 S2（把 `except OSError: return True` 改成 `return False`，即「探针答不出来就算空」）：
  `1 failed, 8 passed`，`test_an_unreadable_lock_counts_as_held` 红。

顺带：`RuntimeServiceHeartbeat` 里确实没有 `pid` 字段（#216 描述里的「pid None」不是心跳文件的字段），
改用 flock 探测是语义等价且更强的做法，这个判断我认同。

### 6. 端到端：我在 Docker 容器里从头跑了一遍

容器 `python:3.11-slim` + `apt-get install systemd`，root，Python 3.11.16。
`systemd-creds` 版本 `systemd 257 (257.13-1~deb13u1)`，`systemd-analyze has-tpm2` = **partial**
（与生产主机 2026-09-07 窗口记录的形状一致）。

| 段 | 内容 | 结果 |
|---|---|---|
| A | `-m linux_exact`：真 sealer（`runpy` 跑仓库里的 `deploy/libexec/rquant-runtime-credential-sealer`，不注入 encrypt/decrypt）+ 真 `/usr/bin/systemd-creds encrypt` → 断言 `current.cred` 是 symlink 且 `readlink` 恰为 `generations/<bundle generation>.cred`、密文里找不到明文 → 真 `systemd-creds decrypt` 解回来逐字节相等 → 按 `LoadCredentialEncrypted` 的四条形状（0700 目录 / 文件名 `capabilities.json` / 0400 / nlink 1 / 属主本进程）落盘 → 六个 role 逐个进主循环 | **1 passed** |
| B | 反向三组：无凭据（6）、错 generation（6）、串门凭据（6） | **18 passed** |
| C | credstore e2e 整个文件 | **28 passed, 1 deselected**（7m06s） |
| D | **我自己在容器里打的变异**：白名单去掉 `CREDENTIALS_DIRECTORY`，Linux 门必须红 | **1 failed**，报错原文 `RuntimeError: TUSHARE_TOKEN_MAIN capability is required` |

D 这一条是我加的，用来确认 Linux 门不是「怎么改都绿」的摆设：它红了，而且红在
生产 2026-09-07 窗口逐字记录过的那句话上。

反向「去掉凭据明确拒绝」我确认过不是打桩：`test_a_credstore_role_without_its_credential_refuses`
先断言 `"CREDENTIALS_DIRECTORY" not in environment`（wrapper 真没给），再断言
`"validation errors for Settings" not in message`（不是又掉回旧 bug），
才断言消息里有 `capability is required` 或 `credential`。notifier 单列一条，因为它的
provider loader 在投递期才跑，空 spool 无信号可投——测试直接调 `loader()` 证明它一被调用就
`at least one notification capability is required`。这个处理是诚实的。

### 7. 变异、两版本数字、ruff、git 状态、改动面

**变异我自己复跑了 8 条（5 条复现报告的 + 3 条自补），外加容器内 1 条。** 全部走
「确认 `git status` 干净 → 打补丁 → 跑指定用例 → `git checkout -- .` → 再确认干净」。

| # | 变异 | 用例 | 结果 | 报错原文（摘） |
|---|---|---|---|---|
| R1 = M1 | `adapter/tushare.py` 恢复模块级 `from rquant.config import settings` | 子进程探针 | **2 failed, 2 passed** | `pydantic_core._pydantic_core.ValidationError: 5 validation errors for Settings` |
| R1′ | 同上补丁 | credstore e2e 两条 role 循环 | **2 passed（绿）** | —— 证实 e2e 对这条无区分力 |
| R2 = M3 | `_CAPABILITY_ROLE_ENVIRONMENT = _RUNTIME_ROLE_ENVIRONMENT` | 交付单测 + e2e 移交/循环 | **7 failed, 17 passed** | `RuntimeError: TUSHARE_TOKEN_MAIN capability is required`；`ValueError: reference slow publisher requires its isolated publication credential`；`AssertionError: assert set() == frozenset({...})` |
| R3 = M5 | `expected_generation=args.expected_generation` | 六个 role 进循环 | **6 failed** | `ValueError: systemd capability credential generation does not match runtime`（六个全中） |
| R4 = M7 | supersede 放宽为只看 `stopped ∧ stopped_at` | 心跳单测 | **2 failed, 7 passed** | `Failed: DID NOT RAISE <class 'ValueError'>` ×2 |
| R5 = M9 | `_tushare_daily_close_fetcher` 不传 `backup_token` | daily-close 网关 + 探针 | **2 failed, 40 passed** | `TypeError: ... missing 1 required keyword-only argument: 'backup_token'`；`ValidationError: 5 validation errors for Settings` |
| **S1（自补）** | Route B 下「凭据在手但无 generation」的拒绝去掉 | 交付单测 | **1 failed, 15 passed** | `Failed: DID NOT RAISE <class 'ValueError'>` |
| **S2（自补）** | `_service_lock_is_held` 的 `except OSError` 从 `True` 改成 `False` | 心跳单测 | **1 failed, 8 passed** | `AssertionError: assert False` |
| **S3（自补）** | 白名单顺手多带一个 `APP_ENV` | 交付单测 + e2e 移交 | **1 failed, 17 passed** | `AssertionError: assert ('APP_ENV', ...) == ('CREDENTIALS...', ...)` |
| **D（容器内自补）** | 白名单去掉 `CREDENTIALS_DIRECTORY`，跑 Linux 真封解门 | `-m linux_exact` | **1 failed** | `RuntimeError: TUSHARE_TOKEN_MAIN capability is required` |

R2 最有说服力：它逐字复现了生产窗口那两句报错，把「白名单缺 `CREDENTIALS_DIRECTORY`
是根因」从推断变成事实。

**两版本数字（我自己跑的，不是引用报告）：**

| 环境 | 范围 | 结果 |
|---|---|---|
| macOS · Python 3.11.15（`.venv`） | 三个新单测文件 + `test_daily_close_gateway.py` | **67 passed** |
| macOS · Python 3.11.15 | credstore e2e + 包 A legacy binding e2e | **45 passed, 2 deselected**（604.78s） |
| macOS · Python 3.11.15 | notify ×4 + surge_watch + tushare_suspension + tp9 + capabilities + service_control + sealer helper/client（11 文件） | **281 passed** |
| macOS · Python 3.12.13（`.venv312`） | 三个新单测 + daily_close + notify api/log + capabilities + service_control + tp9（9 文件） | **182 passed** |
| Linux 容器 · Python 3.11.16 · root | 见上表 A/B/C | **1 / 18 / 28 passed** |

**ruff**：`ruff check` 对全部 14 个改动文件 **All checks passed**。
`ruff format --check` **不通过**三个文件——见 should-fix 2。

**`git status`**：8 次变异前后都干净；审查结束时也干净（除本文件本身）。

**改动面无越界 —— 逐条核对：**

- `deploy/` 一行未改（我另外核实了「不需要改」这个结论：7 个 unit 的
  `LoadCredentialEncrypted=` 名字都是 `capabilities.json`，与 sealer 的
  `--name=capabilities.json` 和读取端的 `RUNTIME_CAPABILITY_CREDENTIAL_NAME` 三方一致；
  `rquant-artifact-retention.service` 写死的实例 `svc-248ba9b2…` 我算过，
  确实等于 `sha256("artifact-retention.primary.v1")`；sealer `_SERVICE_KINDS` 已是 7 个）；
- `.env`：未动，仓库里也没有；
- 发布原语 `runtime_authority_publish.py`、stage、`runtime_authority_*`：未动；
- wrapper `runtime_exec_wrapper/_verify.py`：**未动**（白名单是 profile 数据，不是 wrapper 代码）；
- `runtime_authority.py`：只新增一个常量 + 注释，加 7 条 role 的 `environment_allowlist` 换名，
  我把整个 diff 逐 hunk 看过，没有第二类改动。

### 8. 与包 F 的合并风险

**没有文本冲突。** 用 `git merge-tree --write-tree`（不落地、不改分支）验证三组：

| 组合 | 结果 |
|---|---|
| 包 E → `origin/main`（已前进 3 个 commit 到 `a90f927`） | 干净 |
| 包 E ↔ 包 F（`cc/20260907-strategy-chain`，`bc326cb`） | 干净 |
| 包 E ↔ 包 F 的更新头（`42c5d14`，审查末尾复核） | 干净 |
| 包 F → `origin/main` | 干净 |

包 F 分支在我做完实测之后又前进到 `42c5d14`（多了一条 recovery 凭据权限的修复与它自己的审查稿），
改动面仍在同一批文件里，我重跑 `merge-tree` 仍然干净；下面的实测数字是对 `bc326cb` 那一版跑的。

两包都改 `src/rquant/runtime_service_main.py`，但区域不重叠：包 F 在
`build_runtime_strategy_completion_attestation_signer`（约 489-500 行，manifest 解冻），
包 E 在 `run()`（约 664-720 行，capability 加载点）。

**合并后两边都跑绿 —— 我建了临时合并 worktree 实测：**

| 用例 | 结果 |
|---|---|
| 包 F `tests/integration/test_route_a_strategy_chain_e2e.py`（跑真实 `run()`） | **6 passed**（126.83s） |
| 包 E credstore e2e 的六个 role 循环 + 连跑 | **7 passed** |
| 包 E 三个新单测 + `test_daily_close_gateway.py` | **67 passed** |

临时 worktree 与那个临时 merge commit 审查结束后已删除，两个分支都没动。

---

## must-fix

### M-1（文档，但后果是「回不去」）：`DEPLOY.md` 换代段的备份命令在生产主机上跑不出来

现文（`DEPLOY.md`，新增段的代码块）：

```bash
sudo install -d -m 0700 /root/rquant-profile-rollover-$(date +%Y%m%d-%H%M%S)
sudo cp -p /var/lib/rquant/runtime-authority/current.json  /root/rquant-profile-rollover-*/
sudo cp -p /etc/rquant/production-runtime-profile.json     /root/rquant-profile-rollover-*/
```

两个问题：

1. **通配符是 `lighthouse` 的 shell 展开的，而 `lighthouse` 读不了 `/root`。**
   命令用 `sudo` 说明操作者是普通用户，`/root` 在 RHEL 系是 `dr-xr-x---. root root`，
   glob 匹配不到任何东西，bash 默认把 `/root/rquant-profile-rollover-*/` 原样传下去，
   `cp` 报 `No such file or directory`。
2. 就算在 root shell 里跑，第二次换代时 `/root` 下会有两个以上匹配目录，
   `cp -p 源 目录1/ 目录2/` 的语义变成「把源和目录1 拷进目录2」，同样是错的。

后果不是「命令报个错」而已：紧接着的第 ③ 步是 `sudo rm -f
/var/lib/rquant/runtime-authority/current.json`。操作者一条条粘贴、没有 `set -e`，
备份没成而 `current.json` 已删，就**回不到 sequence 3 了**（旧 profile 也会被第 ④ 步的
stage+publish 覆盖）。这正是这一段存在的理由，也是我被要求核的「回滚可行」。

改法（与 runbook §0.6 里 A-2 / A-7 已在用的 `${STAMP}` 写法一致）：

```bash
STAMP=$(date +%Y%m%d-%H%M%S)
sudo install -d -m 0700 "/root/rquant-profile-rollover-${STAMP}"
sudo cp -p /var/lib/rquant/runtime-authority/current.json  "/root/rquant-profile-rollover-${STAMP}/"
sudo cp -p /etc/rquant/production-runtime-profile.json     "/root/rquant-profile-rollover-${STAMP}/"
sudo ls -l "/root/rquant-profile-rollover-${STAMP}/"     # 两份都在，再往下走
```

最后那条 `ls` 建议保留，作为第 ③ 步的前置确认。

---

## should-fix

### S-1（代码）：Route B 下「该有凭据却一个目录都没有」仍是静默降级

`runtime_capabilities.py` 里 `expected_generation is None` 的分支排在
`_undelivered_credential_reason()` 那段失败关闭**之前**：

```python
if expected_generation is None:
    if credential_directory:
        raise ValueError("... cannot be bound without a deployment generation")
    return LoadedRuntimeCapabilities({})   # ← 需要 capability 的 kind 也走这里
```

于是 Route B + 需要 capability 的 kind + 没拿到 `CREDENTIALS_DIRECTORY` 时，
改动 5 那两句诊断语根本不可达，症状又退回 #215 之前的样子（下游报「capability is required」，
看不出是投递断了）。生产主机跑的是 Route A（`data/runtime/current` 在），所以现在不会咬到人，
但裁决 15「route B 下 capability role 明确拒绝」与裁决 16「明确失败关闭」在这个交叉口留了个洞。

改法很小：把 `required and not credential_directory` 的诊断提到 `expected_generation is None`
判断之前（或在 None 分支里复用同一个 `_undelivered_credential_reason()`），
再补一条单元用例。

### S-2（报告）：`ruff format --check` 那句话与事实不符

实现报告写「`ruff format --check` 对新文件通过；`runtime_authority.py` /
`runtime_service_main.py` 在 `origin/main` 上本来就不是 format-clean」。前半句不成立。我实测：

- `src/rquant/runtime_capabilities.py` —— **在 base `695e952` 上是 format-clean 的**，
  本次改动之后不再是（一处 f-string 拆行 ruff 想合并）；
- `tests/integration/test_route_a_credstore_roles_e2e.py`（新文件）—— 不通过；
- `tests/unit/test_credstore_role_child_environment.py`（新文件）—— 不通过。

后半句我核实了，属实（base 上那两个文件确实不是 format-clean）。

影响：**`.github/workflows/ci.yml` 里没有任何 ruff / lint job**，所以这纯属观感，不会挂 CI。
但报告里的事实陈述会被集成阶段直接引用，应当订正（顺手 `ruff format` 那三个文件也行）。

### S-3（报告 + 集成）：manifest 不重生成不是「跑不到」，是**全量分片一定红**

报告 §5.5 写「新增 3 个测试文件不在任何 shard 里，CI 全量分片跑不到」。实际更严重：
`scripts/full_suite_shards.py::validate_manifest`（716-752 行）在 `full-suite-v1`
这个非 subset profile 上会比对「收集到的 nodeid 集合」与「manifest 里的集合」，不等就抛
`ContractError: full-suite collection differs: missing=N extra=0`。
所以包 E 一旦合入而 manifest 未重生成，**四个 full-suite shard job 与 contract job 全部红**。

精确数字：这四个文件默认 `addopts`（`-m 'not network and not linux_exact'`）下收集
**57 条**（我实测 `57/58 tests collected (1 deselected)`）。报告写的「+58」是含 linux_exact
那一条的总数；进 shard 的是 **57**。`test_the_roles_run_off_a_credential_the_real_sealer_encrypted`
**不进任何 shard**，与包 A 的 verbatim 那条同例——这一点报告说对了，我核实过
`test_route_a_legacy_binding_e2e` 在 shard-2 里恰好是 17 条（18 减去 1 条 linux_exact）。

### S-4（集成）：`CHANGELOG.md` 未更新

分支 6 个 commit 没有碰 `CHANGELOG.md`。项目规范要求合 main 前写 `[Unreleased]`。
包 F 分支已经写了 38 行，包 E 没写。要点见下面「集成输入」。

---

## note

- **N-1 CI 接线：报告的建议行不通。** 报告建议把 Linux 门并进包 A 的
  `route-a-legacy-binding-linux` job，「加一步 `sudo apt-get install -y systemd`」。
  但这个门的 skip 条件是 `sys.platform == "linux" and os.geteuid() == 0 and
  shutil.which("systemd-creds")` —— **要 root**（`systemd-creds encrypt` 要读/建
  `/var/lib/systemd/credential.secret`，root:root 0400）。GitHub `ubuntu-24.04` runner 以
  `runner` 身份跑，装了 systemd 也会 skip；而那个 job 的 JUnit 契约是
  `--tests 1 --skipped 0`，一 skip 就挂。具体建议见「集成输入」。
- **N-2 `_systemd_unit_name()` 的识别面。** 它按 `/proc/self/cgroup` 每行的叶子名是否以
  `.service` 结尾判断，遇到 `init.scope`、`.control` 这类被委派/嵌套的 cgroup 叶子会返回
  `None`，从而退回旧的静默降级。生产的 7 个 unit 都是 `Type=simple`、无 `Delegate=`，
  主进程就在 `<unit>.service` 这一层，所以现在不会踩。仅作记录。
- **N-3 `artifact_retention` 没有端到端。** 报告已如实披露；简报改法 5 本身也只要求「六个 role」，
  所以不算未达成。它的凭据链路与其余六个共用同一段代码且有单元覆盖，风险可接受。
- **N-4 R-14 可以标注为已被代码取代。** runbook `release-a-runbook-v2.md:2143` 的 R-14
  （换代前人工把旧心跳文件移走）正是 #216，包 E 修好之后这一步不再需要，
  且代码的判据比 R-14 更严（多一条 flock 探测）。集成时建议在 runbook 里标一句。
- **N-5 `DEPLOY.md` 里那两个「角色策略摘要」sha256 无法按文复算。** 文档没写它是怎么算出来的
  （哪一种 canonical JSON、包不包 dataclass 的哪些字段），我按最直白的
  `canonical_json_bytes([dataclasses.asdict(r) for r in PRODUCTION_ROLE_POLICY])` 算出来的是
  `84afc30f…`，与文档里的 `681151cb…` 不同。这两个值只是「证明这一层确实变了」的辅助证据，
  不影响装机，但建议补一行「用 XXX 命令复算」，否则 owner 复核不了。
  （`profile_id` 必变这件事我已从 `RuntimeProfileRole.payload()` 这条路径独立确认，不依赖这两个摘要。）
- **N-6 base 已落后。** `origin/main` 在 base `695e952` 之后又多了 3 个 commit（`a90f927`、
  `daeb094`、`65d56da`，其中 `daeb094` 是一次 R07 重冻结，`a90f927`/`65d56da` 改的也是
  `DEPLOY.md`）。我实测合并无冲突，但集成时要按最新 main rebase/merge 后再复跑一次
  DEPLOY.md 那一段的可读性（两段都往同一个区域插内容）。

---

## 安全结论段（改动 3 与改动 4 是 TCB 相邻的信任面变更）

### 结论一：放行 `CREDENTIALS_DIRECTORY` **不会**让任何角色看到不属于它的凭据

四条独立的理由，每条我都验证过：

1. **变量的值只可能是这个 unit 自己的目录。** `CREDENTIALS_DIRECTORY` 由 systemd 为
   每个 unit 设置成 `/run/credentials/<该 unit>`，wrapper 的 `build_child_environment` 是
   **原样复制**（只拒绝含换行/NUL 的值），从不自己拼路径、也不遍历
   `/run/credentials`。整个仓库里读这个变量的地方只有一处：
   `runtime_capabilities.py:271`（我 grep 过 `src/`、`scripts/`、`deploy/` 全库）。
2. **读取本身有四道文件级校验。** `_read_private_credential` 要求路径绝对且规范化、
   `O_NOFOLLOW`、普通文件、属主等于 `geteuid()`、`nlink == 1`、`mode & 0o077 == 0`、
   大小有界，并在读后用 `(dev, ino, size, mtime_ns)` 复核没被换过。
3. **就算把一份别的角色的凭据递到这个路径上，也进不来。** 凭据明文里带
   `service_id` / `service_kind` / `instance_name` / `bundle_generation` 四项，
   四项逐个比对 manifest。e2e 有一整组 6 条参数化用例
   （`test_another_roles_credential_is_refused`）把六个 role 两两串门试了一遍，
   全部 `ValueError: ... does not match runtime`；我在 mac 与 Linux 容器里都跑过。
4. **没有顺手带进别的变量。** 新常量恰好是旧三个名字加一个，测试用精确元组断言锁死，
   我的变异 S3 加一个 `APP_ENV` 立刻红。其余 20 个 role 与 `lab_claim_finalizer`
   一字未动，同一条测试同时锁住。

**必须如实记下的一条既有事实（不是本次引入）**：`deploy/systemd/` 里 23 个 runtime unit
全部是 `User=lighthouse / Group=lighthouse`。systemd 的每 unit 凭据目录是 0700 属主为
`User=`，所以它挡的是**别的用户**，挡不住**同一个 uid 的兄弟角色**——但
`/run/credentials/<unit>` 是确定且可推算的路径，一个被攻破的 lighthouse 进程在本次改动之前
就能直接 `open()` 它。也就是说，把变量名加进白名单**没有增加任何新的读取能力**，
它只是把「这个进程自己的凭据在哪」告诉了本来就该知道的那个进程。
真正的角色间隔离靠的是上面第 3 条（凭据绑定到 service_id + instance + generation），
那一层是密码学/身份级的，本次改动只是让它第一次真正生效。

### 结论二：改用 bundle generation 校验凭据，**没有**放弃权威链绑定

这是我最担心会被悄悄放宽的一点，核完的结论是没有。凭据现在绑到
`schema_generation`，而 `schema_generation` 在路线 A 下由 `resolve_legacy_schema_generation()`
产出，那个函数（包 A / #207 的成果）同时校验两件事：manifest 文件必须坐在
`<--expected-generation>/manifests/` 目录里（`--expected-generation` 来自 root-owned
`current.json` 的 slot），以及 root-owned `legacy-binding.json` 必须指名这个 runtime root
与 `current` 当前指向的那一代；而 `legacy-binding.json` 的 sha256 在 full-manifest 里，
full-manifest 的 sha256 **就是**权威链 generation id，且 wrapper 在这个进程存在之前
已经逐文件校验过。所以凭据是经由一条 root 签名的链传递地绑到权威链上的。
另外 `run()` 一开头的 `load_authority_service_manifest(..., expected_generation=
args.expected_generation)` 也仍在，权威链那道直接绑定一行未删。
把它改回权威链 id（变异 R3/M5）会拒绝每一份正确密封的凭据——这不是「更安全」，
是纯粹的不可用，六个 role 全部报 `generation does not match`。

### 结论三：所有新增判断都是收紧，没有一处放宽

- 缺凭据目录：从「静默返回空映射」改成「明确抛错」（变异 S1 与报告的 M6 都能证明这条可达）；
- 凭据目录里没有 `capabilities.json`：新增一条独立拒绝；
- Route B 且凭据在手：新增拒绝；
- #216 的 supersede 是**唯一**一处从「拒绝」变成「放行」的改动，但它三个条件缺一不可，
  其中「锁无人持有」是靠 `stop()` 的「先写心跳、后放锁」时序保证的强条件，
  且探针答不出来一律算被持有。两条变异（M7、S2）分别从「放宽」和「探针失灵」两个方向验证过。

---

## 集成输入

### 1. `tests/manifests/full-suite-v1` 重生成：**+57**（不是 +58）

- 新增收集到 shard 的 nodeid **57 条**：`test_route_a_credstore_roles_e2e.py` 28（29 减 1 条
  `linux_exact`）+ `test_credstore_capability_delivery.py` 16 +
  `test_credstore_role_child_environment.py` 4 + `test_runtime_heartbeat_supersede.py` 9。
- `test_route_a_credstore_roles_e2e.py::test_the_roles_run_off_a_credential_the_real_sealer_encrypted`
  **不得进任何 shard**（与包 A 的 verbatim 那条、`test_formal_smoke_real_generation_linux_e2e` 同例）。
- `test_daily_close_gateway.py`（38 条）与 `test_route_a_legacy_binding_e2e.py`（17 条）
  用例数不变，只改了断言，manifest 里的条目不动。
- **不重生成就会红**，不是「跑不到」：`validate_manifest` 抛
  `full-suite collection differs: missing=57 extra=0`，四个 shard job 与 contract job 全挂。

### 2. `CHANGELOG.md`（`[Unreleased]`）要点

`### Fixed`

- **credstore 组 7 个 role 在 wrapper 下全部起不来（#215）**：三个独立缺陷。
  ① 投递断在 wrapper 白名单——`_RUNTIME_ROLE_ENVIRONMENT` 里没有 `CREDENTIALS_DIRECTORY`，
  systemd 解密好的凭据地址被静默丢弃，症状表现为下游「capability is required」；
  现给 7 个 capability role 单列 `_CAPABILITY_ROLE_ENVIRONMENT`，其余 21 个 role 一字未动并加测试锁。
  ② 凭据按 deployment bundle generation 密封，校验却拿权威链 generation 比对，两个命名空间按构造永不相等
  （与 #207 同类错）；`load_systemd_runtime_capabilities` 挪到 generation 解析之后，改用
  `schema_generation`，route B 无 bundle 时明确拒绝。
  ③ `adapter/tushare.py`、`notify/api.py`、`notify/log.py` 三处模块级
  `from rquant.config import settings` 在 import 期构造 `Settings`（同 #189 / TP9），已惰性化；
  外加 `runtime_builder_daily` 漏传 `backup_token` 这第四处（`None` 是「去 `Settings` 取」的信号）。
- **换代残留心跳挡住新一代（#216）**：`read_heartbeat` 对
  「`status=stopped` ∧ `stopped_at` 有值 ∧ 服务单例 `flock` 无人持有」的旧指纹心跳按 supersede 处理，
  下一次 `start()` 覆盖写；进程仍活着、或从未走到 `stop()` 的心跳继续拒绝。runbook R-14 的人工步骤作废。

`### Changed`（**建议单列，因为它有装机后果**）

- 7 个 capability role 的 `environment_allowlist` 新增 `CREDENTIALS_DIRECTORY`，
  `profile_id` 因此改变；下一个装机窗口必须按 `DEPLOY.md` 的换代段以 sequence 1 重新首发
  （#190 未修）。

`### Security`

- capability 凭据的 generation 校验换到 deployment bundle 命名空间，权威链绑定不变
  （manifest 路径 + root-owned `legacy-binding.json`，两条都在）；
  缺凭据目录、凭据 id 不匹配、route B 无 bundle 三种情形均由静默降级改为明确拒绝。

### 3. `DEPLOY.md` 段落核对结论

- 与 runbook §0.6 B-8 一致（停 unit → 删 `current.json` → 首发），方向正确；
- 比 runbook 多写的「备份两份 root 文档」是必要补充，但命令本身要按 **must-fix M-1** 改；
- 建议再补两句：装完新一代后判据仍是 `wrapper_preflight == 32`（不要拿 `publish --dry-run` 代替
  真 publish，#198 的教训）；R-14 的人工清心跳步骤作废。

### 4. `ci.yml` 接线建议

**不要**把这个门并进 `route-a-legacy-binding-linux`。那个 job 以非 root 的 `runner` 跑，
而 `test_the_roles_run_off_a_credential_the_real_sealer_encrypted` 的 skip 条件要 root
（`systemd-creds encrypt` 要 `/var/lib/systemd/credential.secret`，root:root 0400），
一 skip 就撞它 `--suites 1 --tests 1 --skipped 0` 的 JUnit 契约。

建议单开一个 job，用容器把 root 和 systemd 一次拿到（我审查时就是这么跑通的，
容器内 A 段 16.9 秒、整文件 7 分 06 秒）：

```yaml
  route-a-credstore-linux:
    name: Route A credstore gate (${{ matrix.python-version }})
    runs-on: ubuntu-24.04
    container: python:${{ matrix.python-version }}-slim     # 容器内是 root
    steps:
      - run: apt-get update -qq && apt-get install -y -qq systemd git ca-certificates
      # 其余照抄 route-a-legacy-binding-linux：私有根、uv==0.10.11、uv sync --frozen
      - run: |
          uv run python -m pytest -o addopts='' --strict-markers -m linux_exact -rA \
            --junitxml="${JUNIT}" tests/integration/test_route_a_credstore_roles_e2e.py
      - run: |
          uv run python tests/support/assert_junit_contract.py --path "${JUNIT}" \
            --suites 1 --tests 1 --failures 0 --errors 0 --skipped 0 --cases 1
```

`apt-get install systemd` 在 `python:3.11-slim` 上约 40 MB，容器内 `systemd-creds` 是
systemd 257，`has-tpm2` = partial，自建 `credential.secret` 为 root:root 0400 —— 与生产主机同形，
不需要 TPM。

### 5. R07

**必须在合入后重冻结。** `tests/fixtures/r07_differential_gate/policy-v1.json` 的
`source_file_snapshots` 覆盖 `src/rquant/runtime_service_main.py`（包 E 与包 F 都改了它），
而 `allowed_diff` 的 14 条路径里没有包 E 改的任何一个文件，所以不重冻结 R07 差分门必红。
包 E 分支按简报要求**没有**重冻结，做法正确；两包合完之后一次重冻结即可
（参照 main 上已有的 `daeb094 chore(r07): refreeze baseline to the route A cli dispatch merge`）。

### 6. 发版号

当前最新 tag `v0.32.2`，生产在跑的也是 `v0.32.2`。
包 E 换 `profile_id`，装机流程必须多一步「以 sequence 1 重新首发」，属于对运维可见的行为变更，
不是纯 bugfix。**建议 `v0.33.0`。** 如果包 E 与包 F 一起合，两者同一个 `v0.33.0` 即可
（包 F 也是路线 A 的解阻塞，不单独占号）。

---

## 附：审查环境与可复现命令

- 私有根 `/Users/roxor/rq-raer-review`（含全部日志与 8 个变异补丁脚本），审查结束后删除。
- 本机跑测试需要五个 `Settings` 必填项，用环境变量给（**没有读写任何 `.env`**）：
  `TUSHARE_TOKEN_MAIN`（≥32 字符的假值）、`DATA_DIR`、`DUCKDB_PATH`、`PARQUET_DIR`、`LOG_DIR`。
- Linux 门：
  `docker run --rm -v <ra-e-cc>:/src:ro -v <私有根>/linux-gate.sh:/linux-gate.sh:ro
  python:3.11-slim bash /linux-gate.sh`。
- 包 E + 包 F 合并验证用 `git merge-tree --write-tree` 加一个临时 detached worktree，
  两个分支都没有被写过，临时 worktree 已删。
