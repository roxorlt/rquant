# 包 E 交付报告：credstore 组角色在 wrapper 下可运行（#215）+ 换代残留心跳（#216）

worktree `/Users/roxor/brain/30-projects/rQuant/.worktrees/ra-e-cc`，分支 `cc/20260907-credstore-roles`，base `origin/main` = `695e952`。

---

## 1. 「密封 → 投递 → 读取」三方对照表（第 1 步，先行回报）

### 1.1 三段链路的事实

**密封端**（`runtime-deployment-profile --apply` 进程内）

| 环节 | 位置 | 事实 |
|---|---|---|
| 明文构造 | `src/rquant/runtime_deployment_bundle.py:3048-3067` | 每个入选实例一份 `serialize_runtime_credential(service_id=…, service_kind=…, instance_name="svc-"+sha256(service_id), bundle_generation=<generation_hash>, values=capabilities[service_id])`，canonical JSON，键恰好六个：`schema_version`(=2)、`service_id`、`service_kind`、`instance_name`、`bundle_generation`、`capabilities` |
| 入选条件 | `runtime_deployment_bundle.py:3055-3066` | LIVE 平面且 kind 不在 `_DEDICATED_NO_CAPABILITY_KINDS`，外加 `artifact_retention` —— 恰好 7 个 |
| 值从哪来 | `runtime_deployment_profile.py:879-897` `resolve_profile_capabilities` | 只取 profile 的 `capability_environment[service_id]` 声明的**名字**，值从 `--apply` 那个进程的 `os.environ` 读 |
| 名字谁定 | `runtime_production_profile.py:1785-1801` | 见下表「profile 声明」列；受 `CAPABILITY_KEYS` 约束（超集报错） |
| 密封动作 | `deploy/libexec/rquant-runtime-credential-sealer:230-243`、`:638-641` | root helper 跑 `/usr/bin/systemd-creds encrypt --name=capabilities.json - -`，写 `/etc/credstore.encrypted/rquant-runtime/instances/<instance>/generations/<bundle_generation>.cred`（root:root 0600），再原子把 `current.cred` symlink 指向 `generations/<bundle_generation>.cred` |
| `_SERVICE_KINDS` | `rquant-runtime-credential-sealer:36-46` | 已是 7 个（#208 在 v0.32.x 修过，勘察报告里说的「漏 5 个」已不成立） |

**投递端**（systemd unit）

| 环节 | 位置 | 事实 |
|---|---|---|
| unit 声明 | 六个模板 + retention 的第 15 行 | `LoadCredentialEncrypted=capabilities.json:/etc/credstore.encrypted/rquant-runtime/instances/%i/current.cred`（artifact-retention 写死实例 `svc-248ba9b2…`） |
| systemd 行为 | systemd 自身 | 解封后把明文放 `$CREDENTIALS_DIRECTORY/capabilities.json`（凭据 ID 必须等于 `--name=` 的值，两边都是 `capabilities.json`，对得上），0400、属主为 `User=`、每 unit 私有 ramfs |
| **断点 1** | `src/rquant/runtime_exec_wrapper/_verify.py:585-616` + `src/rquant/runtime_authority.py:81` | unit 的 ExecStart 是 wrapper，wrapper 从**空字典**起，只复制 profile 白名单里的名字。白名单 `_RUNTIME_ROLE_ENVIRONMENT = ("LANG", "LC_ALL", "TZ")` 里**没有 `CREDENTIALS_DIRECTORY`**，未登记的名字被静默丢弃 → 子进程根本不知道凭据在哪 |

**读取端**（角色子进程）

| 环节 | 位置 | 事实 |
|---|---|---|
| 调用点 | `src/rquant/runtime_service_main.py:667-672` | `load_systemd_runtime_capabilities(kind, expected_service_id=manifest.service_id, expected_instance=<manifest 文件名 stem>, expected_generation=args.expected_generation)` |
| 读取路径 | `src/rquant/runtime_capabilities.py:197-201` | 从 `$CREDENTIALS_DIRECTORY/capabilities.json` 读；**环境变量缺失时直接返回空映射，不报错** |
| **断点 2** | `runtime_capabilities.py:210-211` vs `_verify.py:699` | 即便把 `CREDENTIALS_DIRECTORY` 放行，`--expected-generation` 在 `--authority-runtime` 下取的是**权威链 slot 的 generation_id**，而凭据里写的是 **legacy deployment bundle 的 generation_hash**——两个命名空间按构造永不相等（生产实测：权威链 `e850250e…`，bundle `7d572c79…`），会以 `systemd capability credential generation does not match runtime` 拒绝。这和 #207「schema binding 拿错 id 命名空间」是同一类错，只是发生在凭据校验上 |
| **断点 3** | `runtime_service_builtin.py:1107-1117` → `src/rquant/adapter/tushare.py:17` | 三个 source role 的 `_default_adapter_factory` **先 import 再查 token**，`rquant.adapter.tushare` 的模块级 `from rquant.config import settings` 在 import 期构造 `Settings` → 生产看到的 `5 validation errors for Settings`。同类第二处：`src/rquant/notify/__init__.py:7` → `src/rquant/notify/api.py:9`，notifier 的 provider loader import 会踩，目前被 #218 的 route spool 报错挡在前面 |

### 1.2 逐 role 对照

| role | profile 声明的 capability（= 实际被密封的键） | `.cred` 落点 | unit 的 credential 名 | builder 从哪读 | 第一个撞上的断点（= 生产症状） |
|---|---|---|---|---|---|
| `reference_slow_source` | `TUSHARE_TOKEN_MAIN`、`RQ_REFERENCE_SOURCE_SIGNING_KEY_ID`、`RQ_REFERENCE_SOURCE_PRIVATE_KEY_BASE64`、`RQ_REFERENCE_SOURCE_PUBLIC_KEY` | `instances/%i/generations/<bundle gen>.cred` ← `current.cred` | `capabilities.json` | `runtime_capabilities` 映射（`runtime_service_builtin.py:342-346`）+ adapter 工厂 | **断点 3**（Settings） |
| `reference_slow_publisher` | `RQ_REFERENCE_PUBLICATION_HMAC_KEY_ID`、`RQ_REFERENCE_PUBLICATION_HMAC_SECRET_HEX`、`RQ_REFERENCE_SOURCE_SIGNING_KEY_ID`、`RQ_REFERENCE_SOURCE_PUBLIC_KEY` | 同上 | `capabilities.json` | 映射（`runtime_service_builtin.py:473-489`） | **断点 1**（空映射）→ `requires its isolated publication credential` |
| `auction_match_source` | `TUSHARE_TOKEN_MAIN` | 同上 | `capabilities.json` | 只经 adapter 工厂 | **断点 3** |
| `market_minute_source` | `TUSHARE_TOKEN_MAIN`（`CAPABILITY_KEYS` 允许 2 个，profile 只声明 1 个，`TUSHARE_TOKEN_BACKUP` 不密封） | 同上 | `capabilities.json` | 只经 adapter 工厂 | **断点 3** |
| `daily_close_source` | `TUSHARE_TOKEN_MAIN` | 同上 | `capabilities.json` | 映射（`runtime_builder_daily.py:120-123`，**先查 token 再 import**） | **断点 1**（空映射）→ `TUSHARE_TOKEN_MAIN capability is required` |
| `notifier` | `PUSHDEER_KEYS`、`PUSHPLUS_TOKENS`（另外 4 个 `*_ENDPOINT` / `*_RECIPIENT_IDS` 有默认值，不密封） | 同上 | `capabilities.json` | `capability_environment`（`runtime_builder_signal.py:860-869`，loader 在 step 期才读） | #218 的 route spool（`runtime_builder_signal.py:792`）→ 修掉后是**断点 3**（notify import）→ 再后是**断点 1** |
| `artifact_retention` | `RQ_ARTIFACT_RETENTION_WRITER_CREDENTIAL` | `instances/svc-248ba9b2…/…`（unit 写死实例） | `capabilities.json` | `capability_environment=runtime_capabilities`（`runtime_service_builtin.py:1333`） | **断点 1**（本次窗口未启动该 unit） |

### 1.3 结论

三方**名字**是对齐的（`capabilities.json` 三处一致，`.cred` 路径与 `%i` 一致，`instance_name` = `svc-sha256(service_id)` 与 manifest 文件名一致），**`deploy/systemd/` 无需改动**。全部 7 个 role 起不来，来自代码侧三个独立缺陷：

1. **断点 1（投递被 wrapper 白名单吃掉）**：`_RUNTIME_ROLE_ENVIRONMENT` 缺 `CREDENTIALS_DIRECTORY`，凭据永远到不了子进程，且 `load_systemd_runtime_capabilities` 对「环境变量不在」的处理是**静默返回空映射**，所以症状表现为下游「capability is required」，看不出是投递断了。
2. **断点 2（generation 命名空间错配）**：凭据按 bundle generation 密封，校验却拿权威链 generation 比对，修完断点 1 之后必然撞上。**这一条在 #215 里没有记录，是本包新发现。**
3. **断点 3（wrapper 白名单环境下仍构造 `Settings`）**：`adapter/tushare.py:17`、`notify/api.py:9` 两处模块级 `from rquant.config import settings`，与 #189/TP9 同类。

本机复现（`env -i LANG=C LC_ALL=C TZ=UTC .venv/bin/python -I -S`，只加 site-packages 与 `src`）：
`rquant.adapter.tushare`、`rquant.runtime_notification_providers` 双双 `ValidationError: 5 validation errors for Settings`；
`rquant.reference_slow_runtime`、`rquant.runtime_builder_daily`、`rquant.runtime_builder_signal`、`rquant.runtime_builder_retention`、`rquant.reference_data_registry` 均 import 成功。

### 1.4 拟改动清单（等确认）

| # | 文件 | 改法 | 对应断点 |
|---|---|---|---|
| 1 | `src/rquant/adapter/tushare.py` | 模块级 `from rquant.config import settings` → `get_settings()` 在 `__init__` 内取（PA-1/TP9 同款） | 3 |
| 2 | `src/rquant/notify/api.py` | 同上，`notify()` / `_scene_enabled()` 内取 | 3 |
| 3 | `src/rquant/runtime_authority.py` | 给 7 个 capability role 单列一份白名单 `("CREDENTIALS_DIRECTORY", "LANG", "LC_ALL", "TZ")`，其余 role 不变（最小授予） | 1 |
| 4 | `src/rquant/runtime_service_main.py` | 把 `load_systemd_runtime_capabilities` 挪到 generation 解析之后，用 **deployment bundle generation**（= `schema_generation`，`--authority-runtime` 下由 `resolve_legacy_schema_generation` 得出）做校验；route B（无 legacy bundle）下 capability role 明确拒绝 | 2 |
| 5 | `src/rquant/runtime_capabilities.py` | kind 在 `CAPABILITY_KEYS` 里却没有 `CREDENTIALS_DIRECTORY` 时**明确拒绝**，不再静默返回空映射（收紧，不放宽） | 1 的可诊断性 |
| 6 | `src/rquant/runtime_service_control.py` | #216：`read_heartbeat` 对「`status=stopped` 且 `stopped_at` 有值、且服务单例锁当前无人持有」的旧指纹心跳按 supersede 处理（返回 `None`，`start()` 覆盖写新心跳）；其余不匹配继续拒绝 | #216 |

**改动 3 的部署后果（必须让 owner 知道）**：`environment_allowlist` 进 `profile_id` 的哈希，改它意味着**下一次装机要发新一代 profile（sequence 4）**，`profile_id` 会变。这正是 `DEPLOY.md:534-541` 写明的既定流程（「装上 unit 跑起来后若报缺变量，再另开 PR 补白名单并换代三件套」）。本 worktree **不**重冻结 R07、**不**重生成 manifest、**不** push。

**`RuntimeServiceHeartbeat` 没有 `pid` 字段**（#216 描述里的「pid None」不是心跳文件里的字段）。所以「进程是否还活着」用服务自己的 `flock` 单例锁探测，语义等价且更强。

---

## 2. 改动清单

commit 顺序即逻辑顺序，全部在 `cc/20260907-credstore-roles`，base `695e952`，**未 push**。

| commit | 内容 |
|---|---|
| `9b4a9fe` | 本报告第 1 节（三方对照表） |
| `5b3c50e` | 三处模块级 `Settings` 惰性化 |
| `a5d9a04` | 凭据投递（白名单）+ generation 命名空间 + 失败关闭措辞 |
| `ce9b924` | 六个 role 的端到端验收 + Linux 密封解封门 + DEPLOY 换代要点 |
| `21070b4` | daily-close adapter 的第二处 `Settings`，以及抓到它的子进程探针 |

### 2.1 源码改动（8 个文件）

| 文件 | 改法 | 为什么 |
|---|---|---|
| `src/rquant/adapter/tushare.py` | 去掉模块级 `from rquant.config import settings`，改 TP9 同款 `_settings()` + PEP 562 `__getattr__` | 三个 source role 的 `_default_adapter_factory` **先 import 再查 token**，import 期就构造 `Settings` |
| `src/rquant/notify/api.py` | 同上 | `rquant/notify/__init__.py` 把它拉进来，notifier 的 provider loader import 会踩 |
| `src/rquant/notify/log.py` | 同上 | 同一条链上的第三处 |
| `src/rquant/runtime_authority.py` | 新增 `_CAPABILITY_ROLE_ENVIRONMENT = ("CREDENTIALS_DIRECTORY", "LANG", "LC_ALL", "TZ")`，只给 7 个 capability role 用；其余 20 个 role 与 `lab_claim_finalizer` 一字未动 | wrapper 从空环境起、只复制白名单里的名字，`CREDENTIALS_DIRECTORY` 未登记即被静默丢弃 |
| `src/rquant/runtime_capabilities.py` | `expected_generation` 放宽到 `str \| None`；capability role 在 systemd unit 下拿不到凭据目录时**明确拒绝**，两种成因分开报；凭据目录里没有 `capabilities.json` 时单独报 | 原来静默返回空映射，症状被下游「capability is required」掩盖 |
| `src/rquant/runtime_service_main.py` | `load_systemd_runtime_capabilities` 从 manifest 加载之后**挪到 generation 解析之后**，`expected_generation` 改用 `schema_generation`（= deployment bundle generation） | 凭据按 bundle generation 密封，原来拿权威链 generation 去比，永远不等 |
| `src/rquant/runtime_builder_daily.py` | `_tushare_daily_close_fetcher` 显式传 `backup_token=""` | `backup_token=None` 正是「去 `Settings` 取」的信号，daily_close 会在 import 修好之后**换个位置**继续报同一个错 |
| `src/rquant/runtime_service_control.py` | `read_heartbeat` 新增 supersede 分支 + `_service_lock_is_held` / `_lock_path_for`；`start()` 传 `owns_service_lock=True` | #216 |

### 2.2 测试改动（6 个文件，全部新增或加强，无 skip / 无 xfail）

| 文件 | 用例数 | 内容 |
|---|---|---|
| `tests/unit/test_credstore_capability_delivery.py`（新） | 16 | 白名单最小授予、wrapper 真放行/真丢弃、两种投递失败的措辞、generation 命名空间正反、Route B 边界、`TUSHARE_TOKEN_BACKUP` 可选性 |
| `tests/unit/test_credstore_role_child_environment.py`（新） | 4 | PA-1 同款子进程探针：`-I -S` + 只有 `LANG/LC_ALL/TZ` 的子环境里，11 个 call-time 模块逐个 import，再真构造 registry + `TushareAdapter` + 通知 provider，最后断言 `rquant.config._SETTINGS is None`；外加一条「探针能红」的自检 |
| `tests/unit/test_runtime_heartbeat_supersede.py`（新） | 9 | #216 两种情形 + 锁被持有仍拒 + 未走 `stop()` 仍拒 + 锁不可读算被持有 + 探测不留锁 |
| `tests/integration/test_route_a_credstore_roles_e2e.py`（新） | 29（含 1 条 `linux_exact`） | 见第 3 节 |
| `tests/integration/test_route_a_legacy_binding_e2e.py`（改） | 18（不变） | `_production_bundle` 把 bundle 造出的凭据明文**留下**而不是丢弃，挂到 `RouteAWorld.sealed_credentials` |
| `tests/unit/test_daily_close_gateway.py`（改） | 38（不变） | 假 adapter 的签名加 `backup_token` 并断言恰为 `""`，把新契约钉死 |

### 2.3 文档

`DEPLOY.md` 新增「⚠️ 下一个装机窗口的强制前置：必须换一代 profile（#215 修复引入）」，含新旧角色策略摘要、为什么必须首发（#190）、逐条命令与回滚。

---

## 3. 端到端证据（简报第 5 条）

### 3.1 用例文件的形状

`tests/integration/test_route_a_credstore_roles_e2e.py`，29 条，其中 1 条 `linux_exact`。沿用包 A 的
`_route_a_world`：真装 bundle（`install_runtime_deployment_profile` + 真实生产画像 + 真的
`current -> generations/<64 hex>` symlink）、真 stage、真 publish、wrapper 自己的 `resolve_launch`
推 argv 与子环境。角色跑的是 `runtime_service_main.run()`，`os.environ` 用
`mock.patch.dict(..., clear=True)` 换成 wrapper 推出来的那份、别无他物，进真主循环一次迭代。

| 组 | 条数 | 断言 |
|---|---|---|
| 凭据身份 | 3 | 每个 role 的明文 `service_id` / `service_kind` / `instance_name` / `bundle_generation` 与 manifest 对得上，且 `bundle_generation ≠ 权威链 generation`；wrapper 把 `CREDENTIALS_DIRECTORY` 交给 6 个 role（子环境恰为 `{CREDENTIALS_DIRECTORY, LANG, TZ, PWD}`），交给 `feature_live` 的恰为 `{LANG, TZ, PWD}` |
| 进主循环 | 7 | 六个 role 各一条 + 一条「六个连着跑完」。断言心跳落地、恰好一次迭代、`last_error` 不含 #215 的五种措辞 |
| 反向：无凭据 | 6 | 五个 role 构造期拒绝；notifier 单列一条（它的 provider loader 在投递期才跑，空 spool 无信号可投，`loader()` 一被调用即 `at least one notification capability is required`） |
| 反向：错 generation | 6 | 把明文里的 bundle generation 换成别的 64 hex ⇒ `generation does not match` |
| 反向：串门凭据 | 6 | 拿别的 role 的凭据 ⇒ `does not match runtime` |
| Linux 密封解封门 | 1 | 见 3.2 |

**唯一新增的接缝是一个时钟**：`build_builtin_registry` 本来就收 `clock`，冻到
2026-08-04（bundle 日历只开 2026-08-03 这一天），每个 source role 的 step 走「非交易日」早返回分支，
不会去碰 Tushare。凭据读取、builder 构造、adapter 构造全是真的。

**notifier 的 route spool 由测试预先建出来**（`SignalRouteSpool(root)`），与包 A 给
paper-constraint publisher 预建 `ReferenceRegistry` 同款：生产上这个 spool 由 signal_router 建，
#218 报的就是它没建。这不掩盖 #215 的任何东西——建好之后 notifier 才第一次走到 provider loader 的
import，也才第一次暴露那里的 `Settings`。

### 3.2 Linux 门：真 sealer + 真 systemd-creds 往返

`test_the_roles_run_off_a_credential_the_real_sealer_encrypted`（`linux_exact`，mac 上 skip）：

1. 用 `runpy.run_path` 跑仓库里的 `deploy/libexec/rquant-runtime-credential-sealer`，**不注入
   encrypt/decrypt**，请求形状与 `runtime_credential_sealer_client` 发的一致；`store_root` 移到临时目录、
   `owner_uid` 用当前 uid（测试拿不到 `/etc/credstore.encrypted` 与 uid 0 的 `sudo`，这两样也不是被测对象）；
2. helper 真的调 `/usr/bin/systemd-creds encrypt --name=capabilities.json - -`；
3. 断言 `instances/<svc>/current.cred` 是 symlink 且 `readlink` 恰为 `generations/<bundle generation>.cred`，
   密文里**找不到**明文；
4. `systemd-creds decrypt --name=capabilities.json <current.cred> -` 解回来，断言与 bundle 造的明文**逐字节相同**；
5. 按 `LoadCredentialEncrypted` 的形状落盘（目录 0700、文件名 `capabilities.json`、0400、属主为本进程、nlink 1
   —— 正是 `_read_private_credential` 校验的四条），再逐个跑六个 role 进主循环。

容器：`python:3.11-slim` + `apt-get install systemd`，root。`systemd-analyze has-tpm2` = **partial**、
`/var/lib/systemd/credential.secret` 由 `systemd-creds` 自建 **root:root 0400** —— 与生产主机
2026-09-07 窗口记录的形状一致（勘察报告 Q1 的推断在这里得到实测确认）。

```
=== systemd-creds present ===
systemd 257 (257.13-1~deb13u1)
=== linux_exact gate ===
1 passed, 28 deselected in 16.08s
=== whole credstore file, portable plus gate ===
29 passed in 413.41s (0:06:53)
=== the unit files this package touches ===
193 passed in 32.57s
LINUX-GATE-OK
```

命令原文：
```
docker run --rm -v <worktree>:/src:ro -v /Users/roxor/rq-rae-root/linux-gate.sh:/linux-gate.sh:ro \
  python:3.11-slim bash /linux-gate.sh
```

### 3.3 两版本数字

| 环境 | 范围 | 结果 |
|---|---|---|
| macOS · Python 3.11 (`.venv`) | 26 个受影响的 unit 文件 | **975 passed** |
| macOS · Python 3.11 | `test_route_a_credstore_roles_e2e.py`（可移植 28 条） | **28 passed, 1 deselected** |
| macOS · Python 3.11（终版代码复跑） | credstore e2e + 包 A legacy binding e2e 两个文件 | **45 passed, 2 deselected** |
| macOS · Python 3.11 | `test_route_a_legacy_binding_e2e.py` + `test_production_artifact_terminal_lifecycle.py` | **18 passed, 1 deselected** |
| macOS · Python 3.11 | `test_route_a_legacy_binding_e2e.py` + `test_production_artifact_terminal_lifecycle.py` + `test_runtime_deployment_bundle.py` | **153 passed, 1 deselected** |
| Linux 容器 · Python 3.11.16 | `linux_exact` 门 | **1 passed** |
| Linux 容器 · Python 3.11.16 | credstore e2e 全文件（含门） | **29 passed** |
| Linux 容器 · Python 3.11.16 | 8 个 unit 文件 | **193 passed** |
| macOS · Python 3.12 (`.venv312`) | 10 个 unit 文件 | **328 passed** |

`ruff check` 对全部改动文件通过。`ruff format --check` 对新文件通过；`runtime_authority.py` /
`runtime_service_main.py` 在 `origin/main` 上本来就不是 format-clean（已复核，非本次引入），未动。

---

## 4. 变异表（9 条，逐条原文）

每条：`git` 干净 → 打补丁 → 跑指定用例 → `git checkout -- .` 还原。补丁脚本在
Mac 本地 `/Users/roxor/rq-rae-root/mut/m*.py`，输出在 `/Users/roxor/rq-rae-root/mut-M*.log`。

| # | 变异 | 跑的用例 | 结果 | 报错原文（摘） |
|---|---|---|---|---|
| M1 | `adapter/tushare.py` 恢复模块级 `from rquant.config import settings` | `test_credstore_role_child_environment.py` | **2 failed, 2 passed** | `pydantic_core._pydantic_core.ValidationError: 5 validation errors for Settings` |
| M2 | `notify/log.py` 恢复模块级 `from rquant.config import settings` | 同上 | **2 failed, 2 passed** | `pydantic_core._pydantic_core.ValidationError: 5 validation errors for Settings` |
| M3 | `_CAPABILITY_ROLE_ENVIRONMENT = _RUNTIME_ROLE_ENVIRONMENT`（白名单去掉 `CREDENTIALS_DIRECTORY`） | 交付表 + wrapper 移交 + 六个 role 进循环 | **7 failed, 16 passed** | `AssertionError: assert set() == frozenset({'a...lisher', ...})` / `KeyError: 'CREDENTIALS_DIRECTORY'` / `RuntimeError: TUSHARE_TOKEN_MAIN capability is required` ×4 / `ValueError: reference slow publisher requires its isolated publication credential` |
| M4 | 把 `_CAPABILITY_ROLE_ENVIRONMENT` 发给全部 20 个其他 role | `test_credstore_capability_delivery.py` | **1 failed, 15 passed** | `AssertionError: assert {'artifact_re..._source', ...} == frozenset({'a...lisher', ...})` |
| M5 | `runtime_service_main` 改回 `expected_generation=args.expected_generation`（权威链命名空间） | 六个 role 进循环 | **6 failed** | `ValueError: systemd capability credential generation does not match runtime`（六个全中） |
| M6 | 改动 5 的 `reason` 恒为 `None`（回到静默返回空映射） | `test_credstore_capability_delivery.py` | **2 failed, 14 passed** | `Failed: DID NOT RAISE <class 'ValueError'>` ×2 |
| M7 | #216 supersede 放宽为只看 `status=stopped ∧ stopped_at`（去掉锁探测） | `test_runtime_heartbeat_supersede.py` | **2 failed, 7 passed** | `Failed: DID NOT RAISE <class 'ValueError'>`（`..._whose_lock_is_still_held_is_refused`、`..._unreadable_lock_counts_as_held`） |
| M8 | #216 supersede 收紧回无条件拒绝 | 同上 | **3 failed, 6 passed** | `ValueError: runtime heartbeat does not match the requested service spec` ×3 |
| M9 | `_tushare_daily_close_fetcher` 恢复 `TushareAdapter(token=token)`（不传 backup） | 子进程探针 + `test_daily_close_gateway.py` | **2 failed, 40 passed** | `ValidationError: 5 validation errors for Settings` / `TypeError: FakeAdapter.__init__() missing 1 required keyword-only argument: 'backup_token'` |

**M3 是最有说服力的一条**：它逐字复现了生产 2026-09-07 窗口记录的两句报错
（`TUSHARE_TOKEN_MAIN capability is required` 与
`reference slow publisher requires its isolated publication credential`），说明白名单缺
`CREDENTIALS_DIRECTORY` 就是那两个 role 的根因，不是推测。

**一条打偏的变异也留个记录**：第一版 M1 只替换了函数体、没有把模块级 import 加回去（因为惰性化之后
那行 import 已经不存在，`replace` 成了空操作），跑出来是 `NameError: name 'settings' is not defined`。
改成「真的把 import 加回去」之后，**e2e 反而全绿**——因为 pytest 进程的环境能构造 `Settings`，而且
`get_settings()` 早被缓存过。这正是加子进程探针（`test_credstore_role_child_environment.py`）的原因：
同进程的端到端验收在结构上就看不见 import 期 `Settings`。改用探针之后 M1 立刻红。

---

## 5. 哪些 role 修好了 / 还差什么

### 5.1 逐 role 结论

| role | 2026-09-07 窗口症状 | 本包之后 | 还需要什么 |
|---|---|---|---|
| `reference_slow_source` | `5 validation errors for Settings` | ✅ 真凭据下进主循环 | 换代 profile |
| `reference_slow_publisher` | `requires its isolated publication credential` | ✅ 真凭据下进主循环 | 换代 profile |
| `market_minute_source` | `5 validation errors for Settings` | ✅ 真凭据下进主循环 | 换代 profile |
| `auction_match_source` | `5 validation errors for Settings` | ✅ 真凭据下进主循环 | 换代 profile |
| `daily_close_source` | `TUSHARE_TOKEN_MAIN capability is required` | ✅ 真凭据下进主循环（另修了 adapter 那第二处 `Settings`） | 换代 profile |
| `notifier` | `SignalRouteSpoolIntegrityError: route spool is unavailable` | ⚠️ **凭据侧修好了**（不再构造 `Settings`，白名单放行，provider loader 能真构造）；**spool 仍缺** | 换代 profile + **#218**（signal_router 起来才会有 spool） |
| `artifact_retention` | 本次窗口未启动该 unit | ⚠️ 白名单与凭据链路与其余六个一致（有单元测试覆盖），但**没有端到端跑过** | 换代 profile + #217 的 research 资源门 |

**`deploy/systemd/` 一行未改，也不需要改。** 三方名字全部对齐，勘察报告里担心的
sealer `_SERVICE_KINDS` 缺 5 个 role 也已由 #208 修掉（当前是 7 个）。本包**没有**发现任何需要
改 unit 的问题，因此**没有新立 issue**。

### 5.2 部署后果（owner 必须知道，已写进 `DEPLOY.md`）

给 7 个 capability role 的白名单加 `CREDENTIALS_DIRECTORY` **必然换 `profile_id`**：

| | 值 |
|---|---|
| 角色策略摘要（旧，`origin/main` `695e952`） | `6282aa50fca9cfca113a966379187202bdb975a04072b1beaf9ee5b8bb1ab102` |
| 角色策略摘要（新） | `681151cbdfa310a83adb5ede906c1970913e1398960d8136b92c0b3114f44167` |
| 生产 `profile_id`（旧，sequence 3 在用） | `d2206e53…7ea0` |
| 生产 `profile_id`（新） | 装机当场由 `runtime-authority-stage` 算出（含主机闭包与实例标签，本地算不出） |

`profile_id` 是整份 profile 文档（含主机闭包哈希、实例标签）的 sha256，本地无法预先算；
上面两个「角色策略摘要」是把 `PRODUCTION_ROLE_POLICY` 单独 canonical-JSON 之后的 sha256，
用来证明这一层确实变了。

**为什么不能直接发 sequence 4**：#190——已有 `current.json` 时，发布原语拿**已安装**的 profile 校验
record 的每个 slot，`profile_id` 不同即 `RuntimeAuthorityRecordError: runtime slot profile id is not
active`，TP1 发布器在动任何 root 路径之前就拒绝。**本包不修 #190**（TCB 原语，需 owner 单独授权）。

**下个窗口的路径**（`DEPLOY.md` 新增段落里有逐条命令）：停全部 runtime unit → 把
`/var/lib/rquant/runtime-authority/current.json` 与 `/etc/rquant/production-runtime-profile.json`
备份到 `/root/rquant-profile-rollover-<stamp>/` → 删 `current.json` → 用新代码 stage + publish
（`previous is None`，走首发路径，sequence 回到 1）→ 重启 C 段 unit。
**回滚**：把两份文档原样 `cp -p` 回去再重启；旧 generation 目录内容寻址、永不删。
**credstore 的 `.cred` 不受影响**：`current.cred` 按 bundle generation 指向，与 profile 无关；
`data/runtime` 的 legacy bundle 也不必重装，本次不动 bundle generation。

### 5.3 顺带确认的两件事

- **`TUSHARE_TOKEN_BACKUP` 确实是可选的**（协调者点 ②）：`CAPABILITY_KEYS` 只给
  `market_minute_source` 允许它，生产画像**谁也不密封**；`_default_adapter_factory` 传的是空串而不是
  `None`，`_switch_to_backup` 以「token 为真」为前提，所以没有备份就是永不切换。**但**
  `runtime_builder_daily` 原来漏传这个参数，`None` 恰恰是「去 `Settings` 取」的信号——这算 #215 的
  第七种死法，本包已修（M9 变异守住）。
- **#218 的 notifier 那一条与凭据无关**：spool 是 signal_router 的产物。本包确认 notifier 在
  spool 就位后不再构造 `Settings`、凭据能读到、provider loader 能真构造。

### 5.4 边界遵守情况

不动 `deploy/systemd/`（未改一行，也未发现需要改的问题）；不动 `.env`、发布原语、stage、
`runtime_authority*` 的发布链路（只加了一个 role 环境白名单常量）；不放宽任何失败关闭
（改动 5 是**收紧**，且为了不打断 Route B 既有降级语义（T9-6），把收紧范围收窄到「确实跑在 systemd unit 下」）；
无 skip、无 xfail（唯一的 `skipif` 是 Linux 门的平台守卫，沿用仓库既有 `linux_exact` 约定，且已在真容器里跑绿两遍）；
小步 commit；**未 push、未重冻结 R07、未重生成 manifest**。

### 5.5 未做的事

- **`tests/manifests/full-suite-v1` 未重生成**：新增 3 个测试文件（29 + 16 + 4 + 9 = 58 条新用例）
  不在任何 shard 里，CI 全量分片跑不到。按简报「不重生成 manifest」，留给集成阶段统一重生成；
  重生成时注意 `test_route_a_credstore_roles_e2e.py::test_the_roles_run_off_a_credential_the_real_sealer_encrypted`
  **不应**进任何 shard（与包 A 的 verbatim 那条、`test_formal_smoke_real_generation_linux_e2e` 同例）。
- **CI 未接线**：包 A 的 `route-a-legacy-binding-linux` job 只跑它自己那一个文件的 `-m linux_exact`。
  本包的 Linux 门要么并进那个 job（改成两个文件、契约 `--tests 2 --cases 2`），要么单开一个——
  但它需要容器里装 `systemd`（约 40 MB apt），比包 A 那条重，建议并进去并在 job 里加一步
  `sudo apt-get install -y systemd`。**本轮不改 `ci.yml`**，留给集成阶段决定。
- **`artifact_retention` 没有端到端**：它的 unit 不是 runtime 模板、`once=True`，且被 #217 的
  research 资源门挡着。凭据链路与其余六个共用同一段代码，单元测试覆盖到了，但没有真跑过。
