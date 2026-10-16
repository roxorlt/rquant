# L2数据源本地原始批次接口

这是用户明确授权的两股票L2数据源接入，使用独立原始批次缓存，不写现有分钟表、全景快照或生产数据库。服务器部署准备中，可访问地址和运行状态待实际部署验收证据确认。

共同外层 `2357` 的原始协议实验已在同一连接上完成 180 秒被动观测：两个股票各自的成交、买委托、卖委托六流都有向前新 ID，各流到达跨度约 167–178 秒。采集器与真实 HTTP 路径的持续性、计数和当前运行状态，以随交付的独立实盘验收证据为准。

`4013` 返回成交明细候选，`4027` 返回L2数据源逐笔委托。当前采集器的六个业务请求共用外层 `2357`，按固定已发页面选择业务 kind；旧 `2267` 订单缓存仍可读取。原始响应头、数据字节、字段值和 style 全部保留。成交粒度、数量单位、交易所身份、完整日覆盖、缺口补齐和会话续期均未认证。

## 运行

在 rQuant Python 3.11+ 环境中运行。下面路径是占位符，只在本地提供已获授权的私有输入，不要把其内容放进命令参数、源码或 HTTP 请求。

```sh
uv run python -m rquant.l2 \
  --cache-root /absolute/private/l2-cache \
  --bootstrap-file /absolute/private/bootstrap.bin \
  --order-template /absolute/private/order-market-template.bin \
  --trade-template /absolute/private/trade-market-template.bin \
  --duration-seconds 3600
```

默认只注册 `603258:17` 和 `300006:33` 的成交、买委托、卖委托，共六个种子请求。一个采集进程先取得缓存写锁，之后才读取私有输入并连接固定行情端点；一个 reader 拥有 socket。种子完成后只被动接收，没有定时重查或 HTTP 触发刷新。SIGINT/SIGTERM 请求停止并关闭连接。

显式加入 `--trade-pages 3 --order-pages 3` 可在启动时分别读取最多三页，所有成功页面落到同一缓存。已验证同日范围的有限分页；任意日期、跨日读取和完整历史仍未证实。默认六个种子业务请求，每个 epoch 最多 18 个业务请求。启动阶段业务请求在单采集进程内统一节流，下一请求开始距前次发送完成至少一秒；这不是L2数据源的频率额度或频控保证。重复、无进展、坏结构或不明确空响应会停止。

默认不重连。`--max-reconnects 1` 或 `2` 可显式启用有限的传输错误重连，间隔至少 60 秒，整个 run 的次数与时长不重置。协议、鉴权或关联错误不自动重试。新 epoch 在重新读取私有输入、重新登录前先持久记录缺口；重新取得种子也不会清除缺口。

## 批量与频控

上游每次行情业务请求只对应一个标的和一个数据流。同一连接可以通过多个已注册页面交错接收多标的的被动推送；当前 `Binding`/registry 固定只允许上述两股、六流，没有任意股票池参数，也不支持单请求 `stocklist`。

HTTP 一次最多读取八个已提交市场批次，可跨股票和数据流，每批由 `binding` 标识。这个批量单位是本地市场批次，不是上游股票数量。

每个缓存只允许一个写者。当前没有账号全局、跨机器配额协调或分布式限频；两个 GET 只读缓存、不增加上游查询，API 自身也未内置 QPS 限流。持续时间、已有历史批次数和部署环境都会影响读取性能，不能把一秒节奏当作数据源配额。

## 只读 HTTP

给现有 rQuant Web 进程设置非秘密路径 `RQUANT_L2_RAW_ROOT`，或通过 `create_app(l2_cache_root=Path(...))` 注入同一路径。未配置时不注册 L2 路由。在已有 Web 环境配置下，示例启动命令为：

```sh
RQUANT_L2_RAW_ROOT=/absolute/private/l2-cache \
  uv run python -m rquant.web serve --host 127.0.0.1 --port 8768
```

采集进程和 Web 进程使用同一缓存路径。下面是启动后可用的两个 GET 路径，端口 `8768` 只是示例。

```text
GET /api/v1/l2/status
GET /api/v1/l2/batches?after_sequence=0&limit=1
```

`after_sequence` 是本地已消费批次号，最小 0；本地首批序号为 1。只能使用 0 或已提交的本地序号，未来游标返回 400，避免跳过后续批次。`limit` 为 1–8，默认 1。消费者从 `after_sequence=0` 开始，处理并提交本地批次后，把返回的 `next_sequence` 作为下一次游标循环读取；空批次时保留游标、稍后重读。处理失败则从最后成功提交的本地游标继续。该序号不是L2数据源成交序号或委托 ID。

GET 只读缓存，频繁调用也不会新增上游查询。接口没有启动、登录、订阅、刷新上游或任意日期参数。下面是已配置缓存路径、目录尚未创建时的完整返回结构示例：

```json
{
  "batches": [],
  "next_sequence": 0,
  "status": {
    "mode": "seed_then_passive_listen_candidate",
    "state": "unconfigured",
    "epoch_id": null,
    "subscription_state": "requested_unverified",
    "last_transport_received_at": null,
    "last_market_received_at": null,
    "heartbeat_count": 0,
    "control_count": 0,
    "market_batch_count": 0,
    "seed_queries": 0,
    "unknown_push_count": 0,
    "reconnect_attempts": 0,
    "gap": false,
    "error_code": null,
    "last_sequence": 0,
    "cache_age_seconds": null,
    "cache_stale": true,
    "source_freshness": "unknown"
  }
}
```

每批包含绑定的股票/市场/类型/侧、epoch、请求 ID、页面 ID、协议记录序号、收到时间、缓存时间、原始市场响应与表的十六进制字节、SHA256、字段描述符和每个 cell 的原始字节、style、解码值。小数保持字符串。相同记录、相同价格时间数量和重复响应都会保留，不做分钟化或事件去重。

`binding` 只表示请求或注册页关联。空标题的 `4013` 表没有独立证明股票归属；不得将其升级为交易所通道或认证身份。`source_fd_ordinal` 仅表示当前 TCP 会话内的协议记录序号。

`subscription_state` 为 `requested_unverified` 或 `candidate_push_observed`，表示本次采集会话的观察状态。`source_freshness` 保持 `unknown`。`last_market_received_at` 来自实际已提交的市场批次，心跳、控制帧和 GET 不会刷新它；`cache_age_seconds` 超过 60 秒或连接已停止/阻断时，`cache_stale` 为真。新收到的历史种子可以使缓存年龄较小，但不能证明源数据的实时性。L2数据源显示时间和数值时间仍作为候选时间保留在原始字段中。

缓存缺失返回 `unconfigured` 和空批次，不创建目录或连接上游。缓存损坏返回 503 和固定错误码；不会把原始错误、私有文件路径、鉴权或控制内容放进响应。

## 持久性边界

每个批次先写入私有临时目录，包含 `batch.json` 和整份 JSON 的 `sha256`，fsync 后原子重命名为连续编号目录；目录重命名是唯一提交点。未提交目录不对外可见。写者启动时完整核验前缀与哈希；随后仅追加下一批。读取时轻量检查编号和提交文件，只解码本次请求的最多八批及状态所需的最后一批，不把全部历史行加载到 API 内存。目录扫描仍随批次数增长，不承诺无限历史下的恒定延迟。

整份 JSON 哈希覆盖解码值和原始字段，避免只校验 raw cell 字节而漏掉派生值损坏。缺号、坏批次或不安全文件类型显式报错。重启前只要存在旧 epoch 数据或上次仍活跃，就先持久标记缺口。异常时保留最后完整批次；无法证明恢复完整性。

## 验收范围

共同 `2357` 的模型增量使用脱敏真实 h32/h26 订单样本验证完整值、style、SHA 和缓存往返，同时保留原 h26 `tb1.0` 成交推送及旧 `2267` 缓存兼容。Python 3.13.12 下 27 项受影响测试通过、15 项未选中；具体候选哈希与统一审查记录单独保存。

离线测试还覆盖写锁、原子提交、批次损坏、重启缺口、有限批次读取和本地 GET 不触发上游。产品的持续采集与 HTTP 消费结果以该版本的独立实际验收证据为准，不能仅由 `ispush=1`、种子、心跳或一次布尔统计确认成功。
