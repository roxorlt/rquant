# rquant-web-commands（网页写入通道）

## 做了什么
- `rquant-web-commands.service`（`deploy/systemd/`）：普通 systemd unit，无凭据、无 pyz，用当前 web
  发布的 venv 跑 `python -m rquant.web.commands_service`，监听 `127.0.0.1:8769`，`POST /v1/commands`，
  `GET /healthz`。
- 用 page control 自己的命令模型校验，按 `command_id` 幂等追加 JSONL 到固定目录
  `/home/lighthouse/rquant-web-commands/`（布局与 page control 数据目录一致）：

| 命令 | 文件 |
|---|---|
| `ack_alert` | `alert_acks/acks.jsonl` |
| `add_watchlist_item` | `watchlist/items.jsonl` |
| `save_alert_rule`（新建/修改/停用） | `alert_rules/rules.jsonl` |
| `save_canvas`（保存池子） | `canvas_saves/saves.jsonl` |

- `/etc/rquant-web.env`：`RQUANT_PAGE_CONTROL_URL=http://127.0.0.1:8769/v1/commands`。
- `rquant-web-deploy` 切发布时，如果该 unit 已启用，会一起重启它。

## 读取端现状（三期待办）
网页写入现在会落盘，但**还没有读取端消费**，所以确认/自选/规则在网页上不会回显，也不影响盯盘和通知：
1. 通知方投影（notifier 的 `SignalPageProjectionProducer`）从
   `<page_projection_canvas_catalog_root 的上级>/{alert_acks,watchlist,alert_rules}/*.jsonl` 读取，
   但线上 runtime 代 `e458c47…` 的代码是 producer_commit `df621ef`（2026-09-25），**不含这段读取代码**
   （`alert_ack_log`/`watchlist_log`/`alert_rule_log` 均不存在），而且 Serving 合同里也没有
   `alert_ack`/`manual_watchlist`/`alert_rule` 投影。只靠软链接/配置无法让它读。
   需要：用 `rquant-production-deploy.pyz` 发一个包含当前 main 的新 runtime 代，并让
   `<canvas_catalog_root>/..`（当前配置为 `~/rquant/data/runtime/serving/page-control/`，目录尚不存在）
   下的 `alert_acks`、`watchlist`、`alert_rules` 指向 `/home/lighthouse/rquant-web-commands/` 对应子目录
   （软链接，或把 `page_projection_canvas_catalog_root` 配到本目录下）。注意 page control 的
   `_append_managed_jsonl` 拒绝跟随软链接写入，但本服务直接写真实目录，读取端读软链接没有限制
   ——上线前需在新代上验证读取端是否也做了 no-follow 检查，必要时改用配置路径。
2. 盯盘 `rquant-monitor` 跑在 `~/rquant` checkout（e4e303b，2026-08-04），没有
   `load_manual_watchlist_codes`/`load_alert_rules`；而且新代码也是从 Serving 表读，不是直接读文件。
   需要先完成第 1 步，再按 runbook 二期第 4 条把盯盘迁到独立发布目录（改 unit，需单独批准）。
3. 保存池子（`save_canvas`）：runtime 的正式画布目录是签名 catalog，本服务只记录保存请求；
   要在画布/池子列表里出现，需 page control runtime 消费 `canvas_saves/saves.jsonl` 或改走正式 page control。

## 运维
```bash
sudo systemctl status rquant-web-commands
curl -s 127.0.0.1:8769/healthz
tail ~/rquant-web-commands/watchlist/items.jsonl
```
回滚：`/etc/rquant-web.env` 里删掉 `RQUANT_PAGE_CONTROL_URL` 行（恢复默认 8767，写操作返回 503），
`sudo systemctl restart rquant-web && sudo systemctl disable --now rquant-web-commands`。
