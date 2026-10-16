# L2数据源股票列表订阅与原始批次接口

本接口保存L2数据源市场响应的原始批次，不写分钟表、全景快照或生产数据库。当前候选已实现股票列表控制面；初始容量配置为 20 股，本候选实际认证股票数仍为 0，尚未上线。20→40→80→100 是本轮受控测试阶梯，100 是预算上限，不是已测容量或数据源最大值。实际可用容量、地址和运行状态以同版本验收证据为准。

每股注册成交明细候选 `4013`、买委托 `4027`、卖委托 `4027` 三流，共用外层 `2357`，按已发页面关联。既有两股六流实验证据不替代本次批量容量认证。保留原始响应头、字节、字段值、style 与 SHA256；成交粒度、数量单位、交易所身份、全日完整覆盖、缺口补齐和会话续期仍未认证。

## 启动

使用 Python 3.11+。先创建 owner-only 的 state 父目录；以下路径均为占位符，私有输入内容不得进入 HTTP、命令参数或源码。

```sh
uv run python -m rquant.l2 \
  --cache-root /absolute/private/state/cache \
  --control-root /absolute/private/state/control \
  --capacity-file /absolute/private/state/capacity.json \
  --bootstrap-file /absolute/private/inputs/bootstrap.bin \
  --order-template /absolute/private/inputs/order-template.bin \
  --trade-template /absolute/private/inputs/trade-template.bin \
  --resident --max-reconnects 0

uv run python -m rquant.l2.api \
  --cache-root /absolute/private/state/cache \
  --control-root /absolute/private/state/control \
  --capacity-file /absolute/private/state/capacity.json
```

API 固定监听 `127.0.0.1:8771`，无 host/port 覆盖参数或在线文档端点。仅显式传入 `--control-root` 才启用订阅控制；不传时只有缓存 GET。既有平台 Web 的缓存路由也只有 GET，不会自动暴露写控制。调用方限同服务器或已有授权 SSH 边界，不应将无鉴权市场接口公开监听。

`--capacity-file` 是管理员提供的只读认证策略，包含 `max_symbols`、`source_tree`、`acceptance_sha256`；生产组件由部署器从验收证据生成。离线开发可显式使用 `--max-symbols`，默认 20、硬上限 100，与 `--capacity-file` 互斥；HTTP 请求不能修改容量。

在控制模式下，持久化 desired 列表是唯一股票来源，不自动注入两只默认股票。采集器先取得单缓存写锁；空列表时等待，不读取会员输入或建立上游连接。首次非空后只建立一个连接，一个 reader 同时处理待发种子和推送。每股种子完成后被动接收，HTTP GET 不重查上游，重复 POST 不重新入队已有股票。

`--resident` 与 `--duration-seconds` 互斥，只取消人工结束期限；EOF、协议、鉴权或关联错误仍停止。默认零重连；显式最多两次、间隔至少 60 秒的重连只适用于传输错误，次数不随新 epoch 重置。重启或断线先持久记录缺口，新种子不会清除缺口。SIGINT/SIGTERM 关闭连接，不自动续期会话。

## 股票列表控制

```http
POST /api/v1/l2/subscriptions
Content-Type: application/json

{"symbols":["600000","000001"]}
```

股票代码必须为以 `0`、`3` 或 `6` 开头的六位 ASCII 字符串，保留前导零；单请求最多 100 项。服务端按首次加入顺序去重，并原子追加到所有调用方共享的并集，不替换他人列表，不提供退订或租约。多请求并发合并时不会丢失已受理股票。

新增股票返回 202，表示已持久排队；已全部存在返回 200，revision 不变、`added` 为空。下面仅是结构示例，不代表已有市场数据或容量认证：

```json
{"revision":1,"symbols":["600000","000001"],"max_symbols":20,"added":["600000","000001"]}
```

非法代码、空列表、数字类型或额外字段返回 422。并集超过管理员总容量返回 429，整个请求拒绝，不部分写入。控制文件不安全或损坏返回 503 和固定错误码；错误不会包含私有内容或路径。API 不读取会员输入，也不导入或启动采集器。

```http
GET /api/v1/l2/subscriptions
```

返回 `desired`、`runtime`、`runtime_current_epoch`、`status`。desired 包含持久 revision、完整列表和容量。runtime 是有界持久快照，包含 epoch、已处理的 desired revision 和每流状态：`queued`、`sent`、`seeded`、`no_data`、`first_push_observed` 或 `failed`。它只在状态转换时更新，可能暂时落后于 desired；不表示每流最后行情时间。只有 `runtime_current_epoch=true` 时才属于当前状态的 epoch，旧 epoch 的种子不能证明当前已就绪；停止或 gap 也须结合 status 判断。此 GET 不扫描历史批次。

## 统一排队与频控

上游每个请求只包含一个标的和一个数据流；一个连接通过多个注册页面交错接收多股三流。所有调用方共享单采集器队列，下一业务发送开始距上次发送完成至少 1 秒。重复已有列表不会新增业务请求。默认每股三次种子请求；生产组件使用每类一页，100 股相应最多 300 次业务请求，另有一次初始会话发送。

`--trade-pages 3 --order-pages 3` 可显式增加启动时的有限回溯，每类最多三页。已有同日三页证据；任意日期、跨日、整日完整历史未认证，不应把分页当作自动缺口补齐。

上述一秒节奏是本采集进程的本地节流，不是数据源配额。没有账号全局、跨机器配额协调、分布式限流或 API QPS 限流。容量与读取性能必须实测，不能用一次成功种子或 `ispush=1` 证明持续订阅。

## 只读缓存与双消费者

```text
GET /api/v1/l2/status
GET /api/v1/l2/batches?after_sequence=0&limit=128
```

每个消费者独立保存全局本地游标，初始为 0；本地批次从 1 连续编号。`limit` 默认 1、范围 1–128，同时受 12 MiB 批次内容预算约束，实际可能少于 limit。股票和类型通过每批 `binding` 在客户端筛选，游标覆盖所有股票，不能自行跳过未消费序号。未来游标返回 400。

处理完整页并提交自身消费结果后，将返回的 `next_sequence` 用于下一次请求；不要使用 `status.last_sequence` 跳到末尾。空页保留游标后等待。处理失败从最后已提交游标重读。两个消费者互不影响，GET 不增加上游发送。实际 OpenAPI 结构见 [l2-openapi.json](l2-openapi.json)。

缓存未创建时 `/batches` 返回 `{"batches":[],"next_sequence":0,"status":{...}}`，status 为 `unconfigured`。市场批次包含 `binding`、epoch、`delivery`（seed 或 candidate_push）、接收时间、请求 ID、会话 FD 序号、原始响应与表字节、SHA256 及 typed raw table；小数保持字符串。原始相同行、值变化和重复响应都保留，不按 ID 去重、不分钟化。

`binding` 只证明已注册请求/页面关联，空标题表不独立证明股票或交易所身份。本地序号和 vendor ID 都不是交易所全局唯一标识。本地接收时间候选不同于源事件时间；`source_freshness` 保持 `unknown`，显示时间和数值时间为候选值。`last_market_received_at` 只由实际已提交市场批次刷新，心跳、控制帧、GET 不刷新；种子较新也不证明源事件实时。缓存超过 60 秒或连接已停止/阻断时 stale 为真。

## 持久性与部署

每个缓存只有一个写者。批次 JSON 与整份内容 SHA 先写私有临时目录，fsync 后原子重命名到连续序号目录；重命名是唯一提交点。坏批次、缺号和不安全文件类型显式失败。写者启动核验完整前缀；GET 轻量检查目录前缀，只解码所需有限批次，仍不承诺无限历史的恒定延迟。异常保留最后完整提交，gap 不代表已补齐。

独立组件使用 `/home/lighthouse/rquant-l2-rel/current`；state 下 `control` 为 API 唯一写控制目录，`capacity.json` 只读，`cache` 由采集器唯一写。会员输入为独立私有目录，API 无权读取。目录 0700、私有文件 0600；组件不切换既有 Web current、不触碰生产数据库。

部署唯一入口为 `scripts/deploy-production.sh --component l2`，先核 main 精确提交、完整 CI、正式制品 digest、源树和实际容量验收。生产首次 desired 仅取已验证列表前 20 股，即使认证容量更大也不自动订阅全部。该候选尚未通过实际容量认证，不应据此启用服务。

每个认证阶梯都须在最后种子后有共同 90 秒窗口，全部 3N 流各自有种子后的向前新 ID、至少 60 秒跨度、三个 30 秒桶和窗口最后 60 秒至少两批向前数据；另需双消费者完整游标、并发重复请求零额外发送、资源与时延证据。种子、心跳和同 ID 值变化不算向前新事件。

升级先受控停 API，再重读最新并集后改容量。回滚不恢复旧 desired 快照；若已受理并集超过旧认证容量，保留新 desired/policy/cache，两服务保持停止并报告回滚未完成，不丢 202、不扩大旧容量或自动重登。操作和回滚约束见 [受控发布说明](production-release.md#l2-独立组件)。
