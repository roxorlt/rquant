# web/ 前端开发约定（代理必读）

rQuant 投研平台的网页（React 19 + TypeScript + Vite，入口 `/app/`）。本文件和 `web/CLAUDE.md` 内容相同，改一个就同步另一个。
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

## 目录

| 目录 | 放什么 |
|---|---|
| `src/app/` | 外框（顶栏、左侧导航、手机导航、「我的」菜单、数据代标记）、路由、页面登记表 `pages.ts` |
| `src/pages/<页面>/index.tsx` | 每页一个目录（WS-B）；M0 全部是占位页 |
| `src/ui/` | antd 的薄封装与小组件，统一从 `@/ui` 引用 |
| `src/table/`、`src/charts/` | 唯一的表格与图表封装 |
| `src/api/` | 类型化客户端、生成的 schema、`useMeta`、`useServingQuery` |
| `src/format/` | 红涨绿跌、亿 / 万 / 百分比、上海时区时间、240 根分钟轴 |
| `src/reports/`、`public/reports/` | 「我的 → 报告」：差距总览（`gap-status.json`，每个发布列车更新）和调研报告快照 |
| `src/test/` | Vitest 的 setup、MSW、合成数据 |
| `e2e/` | Playwright：合成 serving 数据代 + 网页 API + 模仿 nginx 的 `/app/` 静态服务 |
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
