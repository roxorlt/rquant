# web/ 前端开发约定（代理必读）

rQuant 投研平台的网页（React 19 + TypeScript + Vite，入口 `/app/`）。本文件和 `web/AGENTS.md` 内容相同，改一个就同步另一个。
仓库级规则（分支、提交、合并、部署）见根目录 `CLAUDE.md`；计划与排期见新前端计划 v2。

## 七条规则

1. **接口类型只有一个来源**：Pydantic 响应模型（`src/rquant/web/`）→ `src/api/openapi.json`（`uv run rquant web-openapi > web/src/api/openapi.json`）
   → `src/api/schema.d.ts`（`pnpm -C web gen:api`）。`tests/unit/test_web_openapi_snapshot.py` 和 `web.yml` 检查三者一致。禁止手写接口类型。
2. **每类问题只有一种写法**：表格 `src/table/DataTable.tsx`；K 线 / 分时 `src/charts/PriceChart.tsx`（lightweight-charts）；其他图表
   `src/charts/EChart.tsx`（ECharts 按需注册，新图表类型先加到 `src/charts/echarts.ts`）；池子画布 `src/charts/FlowGraph.tsx`（React Flow + dagre）；
   取数 `useServingQuery` / `useMeta`（`src/api/`）；数据状态横幅 `ServingBanner`；确认框 `ConfirmDialog`（普通 / 较重 / 高风险两步）。
3. **页面只引用自己的封装**：`@/ui`、`@/table/DataTable`、`@/charts/*`、`@/api/*`、`@/format/*`。页面、外框和报告不直接引用 antd、echarts、
   lightweight-charts、TanStack、React Flow、dagre、openapi-fetch（`biome.json` 的 `noRestrictedImports` 会报错）。
4. **TypeScript 严格模式**（含 `noUncheckedIndexedAccess`），不写 `any`；依赖在 `pnpm-lock.yaml` 里精确锁定，升级单独开 PR。
5. **库版本比训练资料新**（React 19.3、React Router 8、Vite 8、antd 6、ECharts 6、lightweight-charts 5、TanStack Table 8.21、Vitest 5）：写代码前先用 context7 查当前文档。
6. **禁用目录名**：`lib/`、`build/`、`env/`、`var/`、`parts/`、`data/`、`logs/`、`tmp/`、`temp/`、`secrets/`、`downloads/`，以及 `web/dist` 以外任何 `dist/`。
   根 `.gitignore` 会悄悄忽略它们；`pnpm -C web verify:dist` 发现被忽略的文件会失败。数据中心页因此叫 `pages/datacenter/`。
7. **设计变量只有一份**：`src/styles/tokens.css`（原型 `:root` 变量）；图表（`src/charts/tokens.ts`）和 antd（`src/ui/theme.ts`）运行时读取它。
   红涨绿跌一律经过 `toneOf` / `ChangeText`；数字、代码、价格用 `.num` / `.mono`（IBM Plex Mono）。原型 HTML 不进仓库（仓库公开）。

## 界面与文案原则（owner 2026-09-25 要求，硬性）

owner 原话：「UI UX 需要按使用人体验极致的角度做精细设计，文案保持简洁高效，内部逻辑不需要暴露给用户的可以不展示只保留功能相关必要的，
如果需要说明功能 tips 可用 hover 展示 tips 组件等方式」。

1. **只放用户要看懂或要操作的东西**。页面正文不出现：服务 id（`notifier.admin.shadow.v1`）、`svc-…`、数据代哈希、提交号、
   `projection` / `watermark` / `serving` / `generation` / `degraded:…` 这类内部词、里程碑编号（M1、S1）、「第几周」「开发中」。
   技术细节放进 tooltip 或行详情抽屉。`src/test/jargon.ts` 的 `findJargon()` 在 Vitest 和 Playwright 里检查渲染后的正文（tooltip 与抽屉打开前不在正文里）。
2. **名称用中文大白话**：服务、数据集、数据表的显示名来自网页 API（`src/rquant/web/labels.py`），前端不再各写一份映射。
3. **状态只有五种**：正常 / 注意 / 异常 / 未运行 / 等待开盘（收盘后写「已收盘」），用 `StatusBadge`（颜色 + 图标 + 短词），一句话原因放 tooltip。
   休市日、开盘前、收盘后盘中服务没有心跳是预期状态，显示「等待开盘 / 已收盘」，不能标红（规则在 `src/rquant/web/status.py`）。
4. **横幅只在影响用户看到的内容或需要用户动手时出现**，一句话说清要做什么，并给出去处（「看系统健康」）。见下一节「数据状态横幅」。
5. **文案简短直接**；数字统一格式：千分位、价格两位小数、收益用 %、相对时间（「3 分钟前」，绝对时间放 tooltip，`RelativeTime`）；缺失值一律「—」。
6. **说明用 tooltip**（`Tip`，封装 antd Tooltip：桌面悬停和键盘聚焦，手机点按），页面上不写成段的帮助文字。
7. **细节**：间距一致；数字等宽对齐（`.num`，tabular-nums）；空状态说清为什么空、接下来会怎样（「今天还没有信号」「09:30 开盘后出现」，`EmptyState`）；
   加载用骨架屏（`PageSkeleton` 等），不用转圈；键盘焦点可见；390 px 宽度单手可用（底部常用页面栏，次要列用 `DataColumn.secondary` 在手机上隐藏）。
8. **还没做的页面**只显示「即将上线」卡片和一句话说明（`pages.ts` 的 `summary`），不写里程碑和周数。

## 数据状态横幅

横幅（外框里的 `ServingBanner`）只看**数据代本身**，由网页 API 的 `serving_meta()`（`src/rquant/web/serving.py`）判定：
没有可用数据代或 `built_at` 在未来 → 不可用；数据代比 `stale_after`（默认 600 秒）旧 → 过期；更新的数据代没通过校验、仍在用上一代 → 降级；否则正常。
各数据集水位**不参与**：生产上 `runtime_health` 只要有一个服务降级就是 degraded（现在一直是），`lab_jobs` 在研究面服务上线前一直 unavailable，
按水位判会让每一页永远挂着横幅。水位作为数据展示在系统健康的「数据新鲜度」表和数据标记的 tooltip 里。测试：`tests/unit/test_web_meta.py`、`src/app/Shell.test.tsx`。

## 目录

| 目录 | 放什么 |
|---|---|
| `src/app/` | 外框（顶栏、左侧导航、手机导航、「我的」菜单、数据代标记）、路由、页面登记表 `pages.ts` |
| `src/pages/<页面>/index.tsx` | 每页一个目录（WS-B）；未完成的页面显示「即将上线」；`pages/shared/` 放页面之间共用的小单元格 |
| `src/ui/` | antd 的薄封装与小组件，统一从 `@/ui` 引用 |
| `src/table/`、`src/charts/` | 唯一的表格与图表封装 |
| `src/api/` | 类型化客户端、生成的 schema、`useMeta`、`useServingQuery` |
| `src/format/` | 红涨绿跌、亿 / 万 / 百分比、上海时区时间、240 根分钟轴 |
| `src/reports/`、`public/reports/` | 「我的 → 报告」：差距总览（`gap-status.json`，每个发布列车更新）和调研报告快照 |
| `src/test/` | Vitest 的 setup、MSW、合成数据、`jargon.ts`（正文内部词检查） |
| `e2e/` | Playwright：合成 serving 数据代（或 `RQ_E2E_REPLAY_ROOT` 指向的回放副本）+ 固定时钟的网页 API（`scripts/serve_web_fixture.py`）+ 模仿 nginx 的 `/app/` 静态服务 |
| `scripts/` | `verify-dist.mjs`（重新编译并核对提交的 `dist/`）、`check-size.mjs`（首屏体积上限） |

## 改完前端要做的事

1. `pnpm -C web check`（Biome、tsc、Vitest）。
2. `pnpm -C web build`，把 `web/dist/` 一起提交；`pnpm -C web verify:dist` 必须通过（CI 在 Linux 上重新编译并比对）。
3. `pnpm -C web e2e`（需要仓库的 Python 环境：`uv sync --python 3.11`）。
4. 改了网页 API：重新生成 `openapi.json` 和 `schema.d.ts`，并跑 `uv run pytest tests/unit/test_web_*.py -q`。

## 生产约束

- 静态文件由 nginx 在 `/app/` 下直接发送，内容安全策略禁止内联脚本和外部资源：不要在 `index.html` 里写脚本，不要引用 CDN 或 Google Fonts。
- 编译用相对路径（`base: "./"`）+ hash 路由（`/app/#/panorama`），接口地址也按页面相对计算（`/app/api/v1/...`）。
- 网页 API 只读 serving 数据代；所有写操作将来都经 PageControl，并带 `X-Rquant-Csrf: 1`（`src/rquant/web/security.py`）。
