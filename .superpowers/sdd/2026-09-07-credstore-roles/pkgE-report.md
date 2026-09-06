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
