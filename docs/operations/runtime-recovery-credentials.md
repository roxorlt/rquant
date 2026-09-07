# recovery 两份文档的生成：`runtime-recovery-backup.json` 与 `runtime-recovery.json`

`rquant-runtime-recovery@` 与 `rquant-runtime-recovery-rehearsal@` 启动时要读两个文件，
路径由已安装的 deployment profile 的 recovery 段指定。在 2026-09-07 之前，仓库里**没有任何
脚本、CLI 或文档写过这两个文件**（issue #218 C，勘察 `route-a-218-scout.md` §3.3：全仓只有两
处 argparse 默认值和两处测试 fixture）。生产机 `82.156.0.68`（`lighthouse` 用户）上
`/home/lighthouse/rquant/data/recovery/` 这个目录是空的，两个 oneshot 修好 generation 绑定
（#218 B）之后立刻会撞到这里。

生成器是 `scripts/provision_runtime_recovery_credentials.py`。

## 两个文件各是什么

| 路径（生产） | mode | 读取者 | 内容 |
|---|---|---|---|
| `/home/lighthouse/rquant/data/recovery/runtime-recovery-backup.json` | `0600` | `runtime_recovery_backup.load_recovery_backup_config`，随后 `runtime_deployment_profile.validate_runtime_recovery_backup_config` 与画像逐字段比对 | `RecoveryBackupConfig`，canonical JSON |
| `/home/lighthouse/rquant/data/recovery/runtime-recovery.json` | `0600` | `runtime_recovery_backup.RecoveryBackupAuthenticator.from_file` | 恰好 `{"key_id","secret_hex"}`，canonical JSON，secret 至少 32 字节 |

`key_id` 必须等于画像的 `signer_key_id`，也就是两个 unit 环境里的
`RQUANT_RECOVERY_SIGNER_KEY_ID`（生成器默认 `production-recovery-v1`）。

## 怎么跑

**只在目标主机上跑。** 脚本用 `secrets.token_hex` 现场生成 HMAC 密钥，直接以 0600 写到画像
指定的路径，**任何时候都不打印密钥内容**（汇总里只有路径、`key_id` 和是否新建），也没有
「传入一个密钥」的参数，所以密钥不会经过 shell history、argv 或终端。

```bash
# 前提：目录存在且是 0700，bundle 已经装好（<runtime root>/current 指向本代）
install -d -m 0700 -o lighthouse -g lighthouse /home/lighthouse/rquant/data/recovery

/home/lighthouse/rquant/.venv/bin/python \
    /home/lighthouse/rquant/scripts/provision_runtime_recovery_credentials.py \
    --runtime-root /home/lighthouse/rquant/data/runtime \
    --replay-start-date 2026-07-01 \
    --replay-end-date 2026-07-31 \
    --only-missing
```

必须用 checkout 自己的 venv 解释器：主机裸 `python` 没有 pydantic 和 duckdb。

- `--replay-start-date` / `--replay-end-date` 是唯一需要人判断的输入：这是
  `build_runtime_recovery_fixed_replay_expectations` 要在已发布的生产数据集上重放的日期区间，
  必须落在该数据集真实覆盖的范围内。
- `--as-of` 不传则取当前 UTC 时间。给定同一份画像和同一个 `--as-of`，产出的
  `runtime-recovery-backup.json` 逐字节相同，可以重跑复核。
- 其余全部从 `<runtime root>/current/deployment-profile.json` 读出来：两个 root、
  `target_commit`、`target_profile_generation`、`signer_key_id`、两个具名 artifact role、
  十二条 artifact role 绑定、`deadline_seconds`。所以**必须在 bundle 安装之后跑**。

脚本写完会用 unit 将要用的同一套加载器读回来核对（`load_recovery_backup_config` +
`validate_runtime_recovery_backup_config`，以及 `RecoveryBackupAuthenticator.from_file`），
核不过就报错退出 1，不会把一份到了主机上才会失败的文档留在盘上。

## `--only-missing` 与轮换

`--only-missing` 让已存在的文件保留并核对，不重新生成。**日常复跑一律带上这个参数**：
换掉 `runtime-recovery.json` 里的密钥会让 publication root 里已经签过的每一份 receipt 和
pointer 全部验不过。

**已存在但权限不是 0600（或不是普通文件）的文档会被拒绝，不会被替换**，报错里带上实测到的
mode，例如：

```
error: recovery document /home/lighthouse/rquant/data/recovery/runtime-recovery.json
already exists with mode 0o0644, not 0o0600, and --only-missing will not replace it:
restore the mode with `chmod 0600 ...` (or remove the file if it is meant to be
regenerated) and run again
```

按提示 `chmod 0600` 之后再复跑即可；**不要**为了绕过这个报错去掉 `--only-missing`——那会
直接铸一把新密钥覆盖掉正在用的那把。

需要轮换密钥时才去掉 `--only-missing`，并且要按密钥轮换流程单独取得授权——这属于
CLAUDE.md「生产密钥落盘 / 密钥轮换」一类的高风险变更，不走无人值守发布器。

## 首装属于新增生产密钥材料

第一次在生产机上落 `runtime-recovery.json` 是**新增生产密钥材料**，需要 owner 单独明确授权
（受控自动发布模式第 7 条）。文件内容不出机：不进 git、不进聊天、不进任何云盘同步目录。
