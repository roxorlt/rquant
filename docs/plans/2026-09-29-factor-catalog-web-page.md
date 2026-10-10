# 因子库：只读网页纵向切片

承接已冻结的因子定义双表 Serving 合同，把 C3.1 的因子列表与当前定义详情接到真实 `/app/` 页面。原型决定左侧列表、右侧详情的主布局；本片没有真实检验任务与跟踪写入，页面不显示可点击的假「运行检验」「加入跟踪」按钮，也不生成假 IC 图。后续分别接通后再展示这些操作。

**任务分级：普通任务。** 跨只读 Web API、OpenAPI/TypeScript 类型和 React 页面，改变用户可见行为；不触及生产数据写入、权限或共享 Serving 发布器。验收重点是固定数据代、完整状态、清晰文案和键盘/手机交互，最终候选做一次独立审查。

## 合同与界面

1. `GET /api/v1/factors/definitions?generation_id=...` 使用现有已认证只读请求与 `GenerationTracker.borrow()`。客户端传数据代时不匹配返回 409；借用期间只查 `factor_definition_state` 与 `factor_definition` 固定列。两表都未发布时返回 `availability=unavailable`；两表齐全且核验后，分别返回 `empty` 或 `populated`。单表、owner/时间不一致、数量、排序、规范依赖 JSON、状态摘要或实际行数不一致返回 503，不伪装空库；一次请求不跨借用代重试。响应不泄漏仓库绝对路径或实例 ID。
2. Pydantic 响应模型从已核验行生成，OpenAPI 和 `schema.d.ts` 按 `web/AGENTS.md` 生成。列表提供中文名、分类、方向、版本、最早可用日与归档状态；表达式、依赖列和技术 ID 只放选中行的详情/提示。日期与统计口径不推断运行结果，定义存在不等于因子有效。
3. React `/factors` 展示真实因子表和当前定义详情，沿用原型的双栏主次关系与现有 `DataTable`、`PageHeader`、`Panel`、`Tip`、`EmptyState`、`PageSkeleton`、`useServingQuery`。桌面选中一行显示详情；390 px 下可单手选行、阅读详情且无水平溢出；键盘可选。数据代更新时清除旧选择并提示刷新，不能把不同代的行和详情拼在一起。
4. 文案短而可操作：未发布「因子库暂时无法查看，请稍后刷新」；可信空库「还没有因子」；加载用骨架，出错有重试；归档用中文状态。正文不显示 `factor_id`、摘要、`projection`、服务代号、开发阶段或未经检验的有效/偏弱判断。内部标识如确需查看，只在详情或提示；表达式作为纯文本渲染，不插入 HTML。未完成检验/跟踪不在页面上模拟成功。

## 聚焦验收

- 用 Serving fixture 覆盖未发布、可信空、保存/归档的多个当前定义、代变化 409、缺一表/错摘要/错数量/错 owner 的 503；核对仅借用一个 generation，无 SQLite 权威直连。
- React 测列表选择、归档标志、空/错误/加载/重新加载、代更新清选择、正文术语扫描；浏览器桌面与 390 px 验证布局、键盘和详情，运行 `pnpm -C web check`、构建、`verify:dist` 与受影响 Web API 测试，更新生成的 OpenAPI/TS 和 dist。
- 不因只读列表上线而停用 Streamlit；正式切流须等对应用户功能真实覆盖并按生产规则验收。
