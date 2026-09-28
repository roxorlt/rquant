# 价格提醒规则：私有命令准入

接续 [受信命令与原请求恢复](2026-09-29-price-alert-command-admission.md)。本片只把现有 PageControl 价格规则命令接到显式配置的本机 Unix socket，供后续受保护的 Web 入口调用；不开放网页 API、React 按钮，不发布 Serving 数据代，不启动价格评估或通知。

**分级：高风险。** 私有入口是认证用户与规则写入权威之间的边界；把调用方自称的 owner 当成用户身份，或把查询变成提交，会写错他人的规则。

## 冻结边界与失败模型

- **资产与信任边界**：规则、名单、命令/效果/回执都由同一 PageControl SQLite 权威维护。普通 TCP 命令端点继续拒绝价格规则。新入口默认关闭，仅在 PageControl 显式指定绝对 socket 路径、**独立的受信 Web UID** 和共享 socket GID 时启动；Web UID 必须不同于 PageControl 服务 UID，且两个 UID 都是只运行对应服务的专用身份。目录由 PageControl UID 拥有、共享组可进入，模式 `0710`；socket 由 PageControl UID 与共享组持有，模式 `0660`。服务端在读取报文前用内核 peer 凭据只接受指定 Web UID；客户端核对端点目录、socket 身份、权限及内核证明的 PageControl peer UID，且该 UID 必须不同于客户端自身。拒绝符号链接、已有或被替换的路径。当前生产 Web 与 PageControl 都以 `lighthouse` 运行，因此本入口**不能按现有部署配置激活**；以后变更服务身份和目录权限须单独授权并在 Linux 实测。
- **用户身份来源**：受信 Web 只可把已认证、由 nginx 覆写的用户身份传给私有入口，不能从浏览器 JSON 接受 `authenticated_owner_id`。PageControl 仅在内核证明调用进程属于专用 Web UID 后使用此包封身份；Web UID 上若有其他非 Web 进程，或 PageControl UID 上有其他不受信进程，身份边界即不成立，必须拒绝激活而不是退回同 UID 信任。规则命令体必须无 owner，不能指定存储路径或通道凭据。**启用前还须验证 Web 入站边界**：当前普通 loopback TCP 可被本机进程直连并伪造 `X-Rquant-User`；现有私有 Web socket 只按共享的 `www` 组放行，不能单凭该组证明请求来自做过 Basic Auth 的 nginx。价格规则写入口必须在受信代理身份有可核验证明、且非代理进程无法伪造用户头的条件下才启用；仅把 Web 与 PageControl 改成不同 UID 不满足此条件。代理身份方案及 nginx/systemd 权限变化需单独冻结和授权。
- **协议与操作**：提交、精确查询、仅对已存在原命令续跑，分别有固定私有路由。使用严格、大小有界的 JSON 和响应；拒绝重复键、额外字段、错误命令种类及不完整报文。查询只读，且 `(认证 owner, 命令 ID, 完整内容)` 必须匹配；未知 ID 返回缺失，同 ID 换 owner/内容返回冲突且不泄露他人回执。续跑不创建新命令，只调用 PageControl 已有的精确恢复路径。客户端核对返回的命令 ID、请求时间及成功效果的规则 ID/动作；不回退到 TCP。
- **失败路径**：假 owner、另一用户持同一命令 ID、同 ID 换 payload、未激活的规则协议、损坏的 marker/表、CAS 失败、名单版本或有效期变化、SQLite 锁、socket 路径替换、对端 UID 不符、并发相同请求、服务在提交前后断开或回执丢失。确定拒绝与连接/回执不确定须分开；不确定时调用方保留原命令，再走只读查询及原命令续跑，不新造 ID。
- **不变量**：服务端从受信包封注入 owner，PageControl 已有原子事务仍是唯一写入点；私有入口不能在未激活时隐式建表或排队。原命令的跨用户隔离覆盖提交、查询、续跑；任何校验失败都不产生规则、效果或新命令。生产 `systemd`、`nginx`、`sudoers`、真实数据库、旧 monitor 与通知均不改。

## 验收

1. 实际本机 Unix socket 测试证明：未配置、缺少独立 UID/GID 或同 UID 均不监听；旧共享 UID 的非 Web 调用者冒称另一用户在读取报文前被拒，客户端拒绝同 UID 假 socket，均无命令、效果或规则写入；独立 peer、私有路径及替换检查有效。普通 TCP 仍拒绝；超长、重复键、额外 owner、错误 Content-Length/Transfer-Encoding/Content-Type 和错误命令类型在准入前拒绝。无法在本机实测的不同 UID 正向路径必须在 Linux 隔离环境补证，不把模拟 peer 当生产证据。生产启用还需单独证明 Web 入口只能接收受信代理认证并覆写身份的请求，不能由本机其他进程伪造用户头直达。
2. 两用户同股与同命令 ID、相同命令重试、换内容冲突、未知 ID 查询/续跑、未激活/坏 marker、规则 CAS/名单失效、并发相同请求、提交成功但回执断开的原命令恢复均经聚焦验证。查询不改变命令表；客户端拒绝错 ID、时间及效果回执。
3. `PageControlService` 只用显式参数或 CLI 选项启动该私有 listener；关闭或启动失败时没有隐式外网写入口，也不改变已有监听器。直接相关 PageControl/名单/价格规则回归通过。
