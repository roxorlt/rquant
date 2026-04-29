# 本地热备同步（macOS）

⚠️ **此文档已过时**：sync 现在通过 HTTP API 而非 rsync over SSH。

最新部署：[`backup-api.md`](backup-api.md)

历史背景（v0.7.0 之前）：本地通过 rsync over SSH 拉云端 DuckDB。
v0.8.0 后改走 HTTP basic auth 绕开 fail2ban，sync 脚本接口不变（仍是
`scripts/sync-from-cloud.sh`），launchd plist 不变（`com.roxor.rquant-sync.plist`），
仅鉴权 + 传输方式改变。
