# 手动盯盘名单：可信写入入口

接续 [PageControl 原子命令](2026-09-28-manual-watchlist-command.md)及[按用户读取](2026-09-28-manual-watchlist-web-read.md)，对应 v2 C4.8。网页可把一次加入或移出请求交给私有 PageControl 准入通道，再凭持久回执和下一代 Serving 名单展示结果。本片先实现服务端接线；React 按钮与告警范围另行验收。

**分级：高风险。** 新入口能改变监控范围和未来告警触发；以下是实现前冻结的资产、边界和验收。

## 威胁与失败模型

- 资产和边界：PageControl SQLite 名单及回执为写入权威。现有 loopback TCP PageControl 命令入口仍拒绝名单命令；新入口只走受文件权限与 peer UID 保护的本机 Unix socket。Web 自身必须在私有 nginx ingress 后运行，用认证头取得 owner；请求体不得指定 owner。新通道默认关闭，生产 socket/unit 配置须另获授权。
- 失败路径：伪造 owner、绕过 CSRF、普通 TCP 提交、原命令 ID 异内容重放、响应丢失后重复发送、PageControl 已写但 Serving 尚未发布、旧版本覆盖新版本、并发触及 500 上限、socket 不可用或权限降级、网页把排队状态误报为成功。
- 不变量：Web 只把自己认证出的 owner 注入有类型命令，私有 PageControl 通道再次比对；通道身份与权限核验失败即拒绝，不回退泛用 TCP。命令成功只说明 PageControl 权威已写，页面的“已加入/已移出”要等后续同一用户 Serving 新代证实；pending、processing、响应不确定各有可恢复状态。CAS 冲突和容量错误可区分且不改名单。新通道与原子命令复用一份领域验证和事务逻辑。
- 排除：本片不修改生产 nginx/systemd/sudoers、不激活生产名单库、不接 React、告警执行或 Streamlit 停用；不访问 `.env`、凭据或生产数据。

## 验收

1. 本机受控通道默认关闭，只有显式配置、受信 peer UID 和严格 socket 权限才接受 `AddWatchlistItem` / `RemoveWatchlistItem`；与旧确认通道可独立启停。普通 TCP、其他命令和外部进程均不能从该通道写名单。
2. `POST /api/v1/watchlist/commands` 有严格小体积 JSON、同源 CSRF、私有 ingress、登录身份和结构化中文错误。网页请求只含原命令 ID、时间、股票、动作、CAS 版本和加入参数；owner 完全由服务端构造。回执明确区分排队、处理中、成功、失败、不确定，响应丢失后用相同 ID/内容续查，不创造第二次副作用。
3. 离线测试覆盖跨用户注入、伪造普通 TCP、socket 错权限/错 UID、禁用状态、重试及异内容 ID 冲突、CAS/容量、Service 成功但 Serving 仍旧和回执丢失；原有 Ack/画布/PageControl 入口聚焦回归不得退化。终候选做一次独立审查。
