# 因子库：归档命令的已认证网页入口

因子定义已能以不可变版本保存，并在只读因子库页面展示；网页还没有实际写入入口。先接通**归档当前定义**这一项明确操作，复用既有 PageControl 因子受信桥和仓库 CAS。新建/编辑需要服务端可核验的特征目录与最早可用日，另片完成，不能把浏览器自报的目录或日期直接写成定义。

**任务分级：高风险。** 本片跨代理身份、私有进程边界与定义权威，错误授权或重试会改写研究定义状态。实现前冻结下述失败模型并做一次独立 SPEC 审查；最终候选做一次独立审查。

## 冻结失败模型

- **资产与信任边界**：经 nginx 证明且在独立精确操作者名单内的登录用户、同站/CSRF 请求、指定 Serving 数据代的当前因子 head 与已核验仓库实例 ID、原样归档命令 ID/请求时刻、Unix peer UID、PageControl owned actor、写端捕获的 SQLite 仓库完整身份及归档回执。浏览器不能连接 PageControl 通用 TCP 写入口或因子仓库；Web 不读写主库或仓库。
- **失败路径**：伪造用户头、已登录但无归档权限或跨站 POST；浏览器直接发 PageControl 通用命令；另一 UID 调用私有 listener；同命令 ID 跨用户/载荷重放；Serving 投影取自 A 仓库而写端是内容相同的 B 仓库；入队后写端被替换；Web 看到旧 Serving head 后仍归档新版本；提交超时后生成新命令再次归档；原命令成功但 Serving 尚未发布时显示“已归档”；跨数据代续查被旧代阻断；归档后仓库被换代却从新库恢复；私有 listener 不可用或回执损坏时页面误报成功；归档命令顺带执行其他用户的待办命令。
- **必须保持**：Web POST 要求代理身份验证、默认空且独立于普通只读/Lab 控制权限的精确 `factor_editor_users` 操作者名单、JSON 同站/CSRF 和有界载荷；新提交与可推进原命令的续查均先鉴权，非操作者不能借旧命令推进写入。listener 同样核对显式配置的操作者名单，缺任一名单时不开写入口。新命令先在同一借用的 Serving 代核对因子 ID、未归档状态、版本、内容摘要及 `factor_definition_state.registry_instance_id`；未发布、坏投影、换代或不同 head 拒绝，不靠客户端字段判定。准入服务入队前把该已核验实例 ID 与当前 backend 身份核对，并把捕获的完整身份持久绑定到内部 owned 命令；执行前再次按此身份核对，入队后换库也不能写。持久原命令续查先做 actor 绑定的精确 lookup，不再要求旧数据代；同 ID 异 actor/载荷冲突。只允许独立受控 Unix socket 的固定 Web UID 调用因子归档准入，listener 注入已认证 actor 后走 `_submit_trusted_factor_definition`；通用 HTTP、parser、service/outbox 仍拒绝外部因子写。受信路径只 claim/执行该命令，不能由网页调用顺带 drain 其他待办。PageControl 及仓库的身份栅栏、原回执恢复和 head CAS 保持原合同。回执须核对命令、actor、归档动作、因子 ID、版本和内容摘要；成功回执仅表示命令已写入，直到**后续数据代**核验同 head 的 `archived=true` 才显示“已归档”，期间写“已提交，等待更新”；若较新 head 已发布，则按原回执说明旧版本归档完成、当前已有新版本，不将新版本标成归档。不能用成功回执伪造新 Serving 状态。
- **排除**：本片不开放保存/编辑、不把归档解释为删除历史、不创建或迁移生产因子仓库、不改 nginx/systemd/UID 配置、不部署或停 Streamlit。私有 socket 和 Web 配置默认关闭；生产安装及切流须按项目既有单独授权规则另行验收。

## 合同与实现

1. 给 PageControl 因子受信入口补精确 `lookup`/`resume` 和定向 consumer drain：只接受 `ArchiveFactor` 原始 ownerless 命令并注入 `authenticated_actor_id`，内部 owned 命令附准入时捕获的完整仓库身份；查历史命令时完整比对 kind、载荷哈希、owned actor、仓库身份与原回执。执行前核对该身份，复用已审查的因子仓库 backend，不新增第二套定义写事务。
2. 独立 `factor_definition_admission.py` 的归档专用 Unix 服务与客户端复用现有私有命令模式：不同 service/Web UID、`0710` 私有父目录、`0660` socket、peer credential、精确操作者名单、固定路径/大小/JSON framing、超时与终态校验；响应不泄漏仓库路径。服务只支持 submit/lookup/resume 已有的 `ArchiveFactor`，没有 save 或通用任意命令。新命令带已核验 Serving 实例 ID 到 listener，listener 对当前 backend 身份比对后捕获完整身份；原命令续查沿用已持久化身份，不从当前库重基准。
3. Web 增加显式可选的因子准入客户端、默认空 `factor_editor_users`、`POST /api/v1/factors/definitions/{factor_id}/archive` 和原命令续查。请求包含 `generation_id`、原 `command_id`/`requested_at`、精确 `expected_head`。新提交前从同一 `GenerationTracker.borrow()` 读取已核验目录和状态；续查先经准入按原命令与 actor lookup/resume，允许 Serving 换代。状态码/模型区分 `rejected`、`pending`、`succeeded_waiting_publication`、`published`、`unavailable`，不能在传输不确定时暗中发第二个命令。Pydantic→OpenAPI→TS 类型生成，不手写接口类型。
4. React 因子详情仅在当前可信、未归档定义上显示「归档」；点击后用确认框简述「归档当前定义，历史记录仍会保留」。保留原命令供超时/刷新续查；命令成功后显示等待发布，直到新数据代证明归档；CAS/换代时给可操作刷新提示，错误不清除原命令。桌面/390px、键盘和正文技术术语检查覆盖。没有真实保存能力时不展示假「新建因子」。

## 验收

- 先做红测：无用户/无代理证明/已登录但不在操作者名单/无 CSRF、跨站、旧代/旧 head、A 的 Serving 投影与内容相同的 B 写端、核对后入队前/执行前换库、已归档、未发布或损坏目录、缺 listener、错误 peer UID、异 actor 同 ID、同 ID 异载荷、超时后原命令续查、仓库换代、坏回执、成功但 Serving 尚旧或已发布更新 head；均不能误报当前版本已归档或追加另一归档事件。
- 使用临时私有 socket/仓库与合成 Serving 代走归档 v1→命令成功→下一代归档确认；验证浏览器刷新、移动端与键盘、历史仍可读。聚焦 PageControl/准入/Web/前端测试、Ruff、`pnpm check/build/verify:dist` 和指定 Chromium 路径。已有无关全量门禁不因这一片自动重跑。
