# rQuant 受控自动发布

## 目标

日常代码发布由 Codex 完成 PR 合并、版本 tag、腾讯云部署和验证，用户不需要登录服务器。
自动化不是任意生产权限：部署器只能部署 `origin/main` 中的精确 SemVer tag 或完整 commit，
只能重启固定的 rQuant 服务，不能修改 systemd/nginx、写生产数据库或执行任意 sudo。

## 发布链路

1. PR 必须可合并，Python 3.11/3.12 CI 全绿。
2. Codex squash merge PR，删除远端功能分支。
3. 在合并 commit 创建并推送 annotated SemVer tag。
4. 腾讯云执行：

   ```bash
   cd /home/lighthouse/rquant
   bash scripts/deploy-production.sh --target v0.13.2
   ```

5. 部署器依次执行：部署锁、tracked 工作区检查、`git fetch`、target/main 归属与快进检查、
   diff 风险分类、`git merge --ff-only <exact-sha>`、`uv sync --frozen`、preflight、
   白名单服务重启、第二次 preflight、JSONL 审计。
6. 更新依赖、preflight 或服务健康检查失败时，自动 `git reset --hard` 回上一 commit、
   恢复锁定依赖并重启已经切到新代码的服务。

## 自动拒绝

- target 是 `main`、`origin/main`、短 SHA 或包含 shell 字符，而不是 SemVer tag/完整 SHA。
- target 不属于 `origin/main`，或不是当前生产 commit 的快进后继。
- tracked 工作区存在未提交改动；`backup/` 等 untracked 文件不阻断。
- diff 包含 `deploy/systemd/`、`deploy/nginx/`、`deploy/frp/`、`deploy/sudoers/`。
- 工作日 09:15-15:10 的发布需要重启任何长驻服务。
- 另一个部署进程已持有 `logs/production-deploy.lock`。
- `sudo -n`、依赖同步、preflight 或服务健康检查失败。

部署器没有 `--force` / `--emergency` 绕过参数。高风险基础设施和生产数据操作必须另开
受控变更，并取得用户明确授权。

## 一次性安装

首次需要恢复可用的受控 SSH，并由 root 安装最小 sudoers 白名单：

```bash
cd /home/lighthouse/rquant
sudo visudo -cf deploy/sudoers/rquant-production-deploy
sudo install -o root -g root -m 0440 \
  deploy/sudoers/rquant-production-deploy \
  /etc/sudoers.d/rquant-production-deploy
sudo visudo -cf /etc/sudoers.d/rquant-production-deploy
sudo -n -l /usr/bin/systemctl restart rquant-dashboard.service
```

最后一条只检查白名单授权，不会重启服务。正式安装后，Codex 仅通过
`scripts/deploy-production.sh --target <exact-ref>` 部署。

## 预演与审计

预演会 fetch 和计算计划，但不 checkout、不更新依赖、不重启服务：

```bash
bash scripts/deploy-production.sh --target v0.13.2 --dry-run
```

退出码：`0` 成功/无需更新，`2` 策略拒绝，`75` 交易时段延期，`1` 部署或回滚失败。
审计记录位于 `/home/lighthouse/rquant/logs/production-deploy.jsonl`。

## 旧脚本边界

`scripts/deploy.sh` 保留给 systemd unit 等人工基础设施部署。它会执行交互式 sudo，且不具备
精确 target、交易时段保护和自动回滚，因此不得用于 Codex 无人值守发布。
