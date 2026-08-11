# Stage 7 Claim Finalizer 运维模型

## 信任边界

`lab-claim-finalizer-trust issue` 与 `rotate` 只在离线 ceremony 中运行。它们需要
offline root private key，输出 canonical Ed25519 certificate；`inspect` 只读取证书。
生产 finalizer 安装器、scheduler 和 worker 都没有 root private key，也没有签发或轮换接口。

每张证书同时绑定 `store_id`、Lab SQLite 的 `(device, inode)`、schema v16、finalizer
public-key fingerprint、purpose 与有效期。替换数据库、私钥/公钥不匹配、过期、用途不符或证书
签名无效时必须拒绝启动。所有 material、public/private key、generation pointer 与证书路径都由
`authority_path_security` 的 `openat(O_NOFOLLOW)` ancestor walk 读取；生产默认 root owner，测试
只能显式参数化 uid/gid。

## 代际安装与回滚

运行时 installer 只接收已签证书、runtime key pair、root capability secret、current-plan key pair、
受控 runtime material template 及 worker verify bundle。它在 staging generation 中写出可被
composition 直接消费的 `runtime-material.json`，将其中所有私密引用改写为同 generation 内文件，并
封存配对的 `worker-verifier.json`。daemon 与 worker 只从 `runtime-material-root/current` regular-file
pointer 选 generation；不会读取 operator 手工指定的单一 material JSON。它先复核数据库和证书 binding，
逐文件 fsync、重算 generation basis/manifest/artifact digest，最后原子切换 pointer；旧值保留在
`previous`。dry-run 不创建目录、不写文件。相同 artifact manifest 重跑为幂等；pointer 切换或 CAS
记录失败会恢复原 pointer，绝不安装 offline root private key。

rollout state 写入独立 SQLite，按 CAS 证据推进：

`OFF -> MATERIAL_INSTALLED -> PREFLIGHT_OK -> FINALIZER_READY -> V2_WORKERS_READY -> SCHEDULER_EMITS_V2 -> DRAINING -> OFF`。

worker V2 只能在 `FINALIZER_READY` 后启用，scheduler 只能在 `V2_WORKERS_READY` 后发新 V2；回滚先停
emit，drain 后才回到 `OFF`。已 `PUBLISHED` 的记录仅可审计读取，不删除、不降级，V1 路径保持独立。
shadow/candidate 仍是默认路径，live 默认关闭且必须使用独立 store/spool。

## Preflight 与观测

`rquant lab-claim-finalizer-preflight` 从 Settings/current generation 实际采集 flag、schema 16、证书
时间/purpose、database generation、key pair、owner/mode/ancestor、AF_UNIX peer、composition、worker
verify-only、scheduler surface 与短 `BEGIN IMMEDIATE` rollback。feature 全关时所有 finalizer 项为 `SKIP`；
DuckDB 与 readonly replica 不是依赖，也明确报告 `SKIP`。CLI 输出 JSON 或 Markdown，并以 0/1/2 表示
OK-or-SKIP/WARN/FAIL；`--apply` 只在 OK 时 CAS 推进 preflight phase。rotation horizon、outbox backlog、
lease/readiness/retry 为可审计观测。

## 基础设施授权清单

本阶段不执行以下操作，需另行明确授权：安装或 reload systemd unit、`systemd-analyze verify` 云端
gate、创建用户/组或目录 owner、写入 `/etc/rquant`、修改 sudoers、迁移/修复生产 SQLite、部署、重启
service、轮换真实密钥。新 unit 仅作为仓库静态模板；其常驻 `Type=simple`，没有 timer 或
`WatchdogSec`，仅运行 `lab-claim-finalizer`，并限制为 `AF_UNIX` 与指定 ReadWritePaths。
