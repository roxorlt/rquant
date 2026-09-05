# 运行时凭证密钥：生成、轮换与备份

本文覆盖 `/etc/rquant` 下五套 Ed25519 签名密钥与一组运行时能力凭证的生命周期，工具是
`scripts/install-runtime-credential-keys.sh`（`init` / `rotate` / `verify` /
`export-capabilities`）。消费者都是 root 持有的 stdlib helper，脚本因此**不 import
`rquant`、不碰 venv**，全程只用 `openssl`、`ssh-keygen` 与系统 `python3`。

路线 A 加了两块（协调者裁决 2 与 3）：

- **`completion` 钥匙串**：Shadow 完成态证明的签名公钥单独一套，不复用 `shadow` 报告钥匙
  （一钥一用）。它的清单形状与 canvas 相同（`schema_version` 1、四字段、不成链），因为目前
  还没有 completion 专属 helper，canvas helper 是树里唯一能校验这个形状的加载器。
- **六个运行时能力凭证**：`runtime_capabilities.CAPABILITY_KEYS` 里没有归属的那六个值
  （reference-slow 源签名三个、reference 发布 HMAC 两个、artifact retention writer 一个）。
  它们不是钥匙串文件，而是部署器从**进程环境**读的值
  （`runtime_deployment_profile.resolve_profile_capabilities`），所以统一存在一份 0600 清单
  `/etc/rquant/runtime-capabilities-keys.json` 里，用 `export-capabilities` 现打印现用。

> [!danger] 私钥丢失不可恢复
> 四份私钥**没有任何备份机制**，也无法从公钥环反推。丢失后 high-water 的水位链与
> daily receipt 的 `previous_manifest_hash` 链**无法重建**——`generation > 1` 要求
> `previous_manifest_hash` 非全 0，而这个值来自上一代导出的公钥环；私钥没了就再也
> 签不出能接上链的下一代。
> **`init` 跑完立刻把整个 `/etc/rquant` 做一次离线加密备份**，备份不进 git、不进聊天、
> 不进任何云盘同步目录。

## 一、十三个文件

`init` 一次性产出十三个文件（五把签名私钥 + 五份指针清单 + 一份日历 + 一把 reference 源
OpenSSH 私钥 + 一份能力凭证清单）与六个目录。生产环境属主
一律 `root:root`；带 `--prefix` 的测试根降级为调用者的 uid / 有效 gid（与
`scripts/install-runtime-credential-infra.sh --test-root` 的 `validate_metadata` 语义一致）。

| 路径（生产） | mode | 内容 |
|---|---|---|
| `/etc/rquant` | `0755` | profile 也住这里（`runtime_authority.py:44`） |
| `/etc/rquant/lab-highwater/` | `0700` | |
| `/etc/rquant/lab-highwater/hw-v1.private.pem` | `0600` | Ed25519 PKCS#8 PEM |
| `/etc/rquant/lab-highwater-keys.json` | `0600` | schema 3，六字段，有 genesis 绑定 |
| `/etc/rquant/canvas-publication/` | `0700` | |
| `/etc/rquant/canvas-publication/canvas-v1.private.pem` | `0600` | PEM |
| `/etc/rquant/canvas-publication-keys.json` | `0600` | schema 1，四字段 |
| `/etc/rquant/shadow-report/` | `0700` | |
| `/etc/rquant/shadow-report/shadow-v1.private.pem` | `0600` | PEM |
| `/etc/rquant/shadow-report/legacy-recovery-calendar.json` | `0600` | schema 1，精确六字段，自指 `content_sha256` |
| `/etc/rquant/shadow-report-keys.json` | `0600` | schema 2，五字段 |
| `/etc/rquant/daily-receipt/` | `0700` | |
| `/etc/rquant/daily-receipt/daily-v1.private.pem` | `0600` | PEM |
| `/etc/rquant/daily-receipt-keys.json` | `0600` | schema 2，六字段，**必须逐字节 canonical** |

四份 `*-trusted-keys.json` 公钥环（`0444`）**不由本脚本生成**——它们由
`scripts/install-runtime-credential-infra.sh` 从私钥现推、`install` + `mv` 到位并复验元数据。
本脚本若预先创建它们会破坏那条链路，所以 `init` 只写上表这九个文件。

### Daily 清单为什么必须逐字节 canonical

`deploy/libexec/rquant-daily-receipt-signer:197-198` 在解析后立刻做
`if _canonical_bytes(document) != payload: raise "Daily key manifest is not canonical"`，
其中 `_canonical_bytes`（`:49-56`）= `json.dumps(ensure_ascii=True, sort_keys=True,
separators=(",", ":"), allow_nan=False)`，**没有结尾换行**。注意这与
`rquant.strict_json.canonical_json_bytes`（`src/rquant/strict_json.py:54`，`ensure_ascii=False`）
不是同一个函数：一旦清单里出现非 ASCII 字符，两者产出不同，daily helper 会硬拒。
**手工编辑这份清单基本必炸**，要改就走 `rotate`。

### 恢复日历是 fail-closed 占位

`init` 写出的 `legacy-recovery-calendar.json` 默认 `open_dates` 为**空**，覆盖区间为
「今天（UTC+8）起 365 天」。空日历意味着 `_require_recovery_window`
（`rquant-shadow-report-signer:372-377` 一线）对任何交易日都不放行——即 legacy shadow
recovery 默认关闭。要启用，在 `init` 时显式给出真实的上交所交易日：

```bash
sudo bash scripts/install-runtime-credential-keys.sh init \
    --calendar-coverage-start 2026-09-01 \
    --calendar-coverage-end   2026-12-31 \
    --calendar-open-dates     2026-09-01,2026-09-02,2026-09-03
```

`open_dates` 必须严格升序、去重、且落在 `[coverage_start, coverage_end]` 内，否则
helper 报 `Shadow recovery calendar coverage is invalid`。

## 二、首次安装（runbook B-2）

```bash
# ① 干跑：只列出将创建的 6 个目录与 13 个文件，不写任何东西
sudo bash /home/lighthouse/rquant/scripts/install-runtime-credential-keys.sh init --dry-run

# ② 真正创建
sudo bash /home/lighthouse/rquant/scripts/install-runtime-credential-keys.sh init

# ③ 复核：13 行 OK + 退出码 0
sudo bash /home/lighthouse/rquant/scripts/install-runtime-credential-keys.sh verify

# ④ 立刻离线加密备份 /etc/rquant，再进 B-3 装 helper
```

`init` 是幂等的：十三个目标文件里**任何一个已存在**就整体拒绝，退出码 **3**，不覆盖、不改动。
失败时**不要手工补文件**，先看清楚缺什么，`rm` 干净后重跑 `init`（此时还没有任何东西依赖它们）。

回滚：

```bash
sudo rm -rf /etc/rquant/{lab-highwater,canvas-publication,shadow-report,shadow-completion,daily-receipt,runtime-capabilities} \
            /etc/rquant/*-keys.json
```

## 二之二、六个能力凭证怎么交给部署器

部署链第 ④ 步（`rquant runtime-deployment-profile --apply`）会把
`profile.capability_environment` 里点名的每个变量从**自己的进程环境**读出来，密封进
credstore；缺一个就直接报 `runtime capability environment <NAME> is missing`。把它们放进
环境的唯一受支持做法是现打印现用，**不要落成 env 文件**：

```bash
# 值只在这条命令的进程环境里存在，退出即消失
eval "$(sudo bash scripts/install-runtime-credential-keys.sh export-capabilities)" \
  && rquant runtime-deployment-profile --profile ... --apply
```

`export-capabilities` 把六个 `NAME=value` 打到 stdout，其中三个是密文
（`RQ_REFERENCE_SOURCE_PRIVATE_KEY_BASE64`、`RQ_REFERENCE_PUBLICATION_HMAC_SECRET_HEX`、
`RQ_ARTIFACT_RETENTION_WRITER_CREDENTIAL`），**不要把输出粘进聊天或日志**。持久副本只有
那份 0600 清单。

## 三、`verify` 的四种调用形式（按 euid 分支）

`verify` 先做只读元数据与 schema 复核（owner / mode / nlink / 大小 / 字段集 / 链绑定 /
日历自洽），再把每份清单交给**真正的消费者**跑一次自检。四个 helper 的调用形式**各不相同**，
不是笔误：

| helper | root（生产，无 `--prefix`） | 非 root（`--prefix` 测试根） |
|---|---|---|
| `rquant-lab-highwater-authority` | `--validate-key-material`（root-only，`:1148`） | **没有非 root 的 validate**，降级为 `--keys-file <p> --export-public-keyring` |
| `rquant-canvas-publication-signer` | `--validate-key-material` | `--keys-file <p> --validate-key-material`（三参形式只在 euid ≠ 0 时接受，`:266-273`） |
| `rquant-shadow-report-signer` | `--validate-key-material` | `--keys-file <p> --validate-key-material`（`:3987-3994`） |
| `rquant-daily-receipt-signer` | 零参数 + stdin 请求 `{"operation":"validate-key-material","schema_version":1}` | 零参数且 `KEYS_FILE` 是模块级常量，只能走 `runpy` 缝（与 `install-runtime-credential-infra.sh:1292` 同一个写法） |

因此 **`verify --prefix` 必须以非 root 身份运行**：canvas / shadow 的三参形式对 euid 0 直接拒收，
脚本会用退出码 2 明确报错，而不是给一个假绿。

## 四、轮换

轮换目标共八个：五套钥匙串 `highwater|canvas|shadow|completion|daily`，三个能力凭证
`reference-source|reference-publication|retention-writer`。能力凭证轮换后**必须重新跑
一次部署链第 ④ 步**，否则 credstore 里还是旧值——credstore 与 generation 绑定，换值就是换代。
`retention-writer` 轮换会把退役密钥写进 `previous_secret_hex` 并把 `sequence` 加一，这是
`ArtifactRetentionWriterCredential` 自己支持的重叠窗口；`reference-publication` 的 HMAC
没有重叠窗口，源与发布者必须在同一次部署里一起换。


```bash
sudo bash scripts/install-runtime-credential-keys.sh rotate highwater --new-key-suffix v2
sudo bash scripts/install-runtime-credential-infra.sh    # 重新导出公钥环，链在这里被校验
sudo bash scripts/install-runtime-credential-keys.sh verify
```

`rotate <highwater|canvas|shadow|daily>` 的语义：

1. 用 `openssl pkey -pubout` 从**当前**私钥推出公钥；
2. 生成新私钥（`openssl genpkey -algorithm ED25519`），`0600`；
3. 旧 key id → 旧公钥 写进 `previous_public_keys`；
4. highwater / daily 额外 `generation += 1`，并把 `previous_manifest_hash` 设成
   **上一代公钥环的 `manifest_hash`**（即消费者自己会算出的那个值）；
5. 原子替换清单，最后 `unlink` 旧私钥。

第 4 步有一道保险：如果 `/etc/rquant/<name>-trusted-keys.json` 已经存在，脚本会把自己算出的
链锚点和那份公钥环里的 `manifest_hash` 对一遍，**对不上就整个轮换中止**（清单和旧私钥都不动）。
这防的是「私钥清单已经轮过一代、但公钥环没重新导出」这种半截状态——真轮下去，
`install-runtime-credential-infra.sh` 会在导出时报
`public keyring previous manifest hash is invalid`，而那时旧私钥已经删了，链就断死了。

**轮换节奏**：没有强制过期。建议与 Release 节奏解耦，**只在怀疑泄露时轮换**。
轮换后必须立刻重跑 `install-runtime-credential-infra.sh` 让四份公钥环跟上，然后重做离线备份。

> [!warning] Canvas 与 Shadow 的轮换不成链
> 这两份清单的 schema 里**没有 `generation` / `previous_manifest_hash` 字段**
> （canvas schema 1 四字段、shadow schema 2 五字段），公钥环导出时也不做代际比对。
> 后果是：canvas / shadow 的轮换**事后不可审计**——没人能从公钥环证明「v2 是 v1 的合法后继」，
> 也拦不住把公钥环回滚成上一代。这是既有设计限制，TP2 不改它，只在这里写明。
> 需要可审计轮换时，只有 highwater 与 daily 两条链是可信的。

## 五、备份与恢复

- **备份对象**：整个 `/etc/rquant`（含四把私钥、四份清单、日历，以及 helper 装完后的四份
  `*-trusted-keys.json`）。
- **时机**：`init` 之后立刻做一次；每次 `rotate` + 重装 helper 之后再做一次。
- **形式**：离线加密介质。**不进 git、不进聊天、不进云盘同步目录**；仓库里不允许出现任何 PEM
  （`tests/unit/test_runtime_credential_keys.py` 有一条扫描测试盯着这件事）。
- **恢复**：把备份整目录还原回 `/etc/rquant`，跑 `verify` 确认十三个文件元数据与 schema 都对，
  再跑 `install-runtime-credential-infra.sh` 让公钥环与 helper 重新对齐。
- **没有备份时**：无解。只能接受链断裂，从 `generation = 1` 重新 `init`，此前所有 receipt 的
  可验证性一并丢失。

## 六、相关文件

- 生成/轮换/复核脚本：`scripts/install-runtime-credential-keys.sh`
- 基础设施安装器（装 helper、导出公钥环、写 sudoers）：`scripts/install-runtime-credential-infra.sh`
- 四个 helper 消费者：`deploy/libexec/rquant-{lab-highwater-authority,canvas-publication-signer,shadow-report-signer,daily-receipt-signer}`
- 六个能力凭证的消费者：`src/rquant/live_spool.py` 的 `ReferenceSourceBatchSigner` /
  `ReferenceSourceBatchVerifier`、`src/rquant/reference_data_registry.py` 的
  `ReferencePublicationAuthenticator`、`src/rquant/runtime_builder_retention.py` 的
  `_writer_credential_from_capabilities`；名单权威是 `src/rquant/runtime_capabilities.py`
  的 `CAPABILITY_KEYS`
- 唯一的仓内公钥环读者：`src/rquant/lab_highwater_authority.py` 的 `load_highwater_trusted_keys()`
- 契约测试：`tests/unit/test_runtime_credential_keys.py`
