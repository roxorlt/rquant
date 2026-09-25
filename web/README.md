# rQuant 网页前端（web/）

React 19 + TypeScript + Vite 的单页应用，生产入口 `http://82.156.0.68:8081/app/`（nginx 发送 `web/dist`，`/app/api/` 转到只读网页 API）。
开发约定见 `web/CLAUDE.md`。

## 准备

- Node 22.22.2（`web/.nvmrc`）和 pnpm 10.33（`package.json` 的 `packageManager`）。
- 端到端测试还需要仓库的 Python 环境：在仓库根目录 `uv sync --python 3.11`。

```bash
pnpm -C web install --frozen-lockfile
```

## 本地开发

```bash
# 终端 1：网页 API，读一份合成 serving 数据代（数据是编造的，见 tests/support/web_serving_fixture.py）
uv run python scripts/build_web_fixture.py --out /Users/<你>/rquant-web-serving --scenario panorama --replace
RQUANT_SERVING_ROOT=/Users/<你>/rquant-web-serving RQUANT_WEB_STALE_AFTER_SECONDS=315360000 \
  uv run rquant web-serve --bind 127.0.0.1:8768

# 终端 2：前端开发服务器（/api 转到 127.0.0.1:8768）
pnpm -C web dev          # http://127.0.0.1:5173/
```

## 测试

```bash
pnpm -C web check        # Biome（lint + 格式）、tsc、Vitest
pnpm -C web test         # 只跑 Vitest
pnpm -C web e2e          # Playwright：自动生成合成数据代、启动网页 API 和 /app/ 静态服务，1440 与 390 两种宽度
```

首次运行 e2e 前安装浏览器：`pnpm -C web exec playwright install --only-shell chromium`。
截图（不作为基准）：`RQ_WEB_SCREENSHOT_DIR=<目录> pnpm -C web exec playwright test -c e2e/playwright.config.ts screenshots`。

## 编译

```bash
pnpm -C web build        # 类型检查 + 编译到 web/dist（要提交）
pnpm -C web verify:dist  # 重新编译并确认 git 里的 web/dist 就是这次的结果，再查首屏体积上限（550 KB gzip）
pnpm -C web size         # 只看体积
```

## 接口类型

```bash
uv run rquant web-openapi > web/src/api/openapi.json
pnpm -C web gen:api      # 由 openapi.json 生成 src/api/schema.d.ts
```
