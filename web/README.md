# rQuant web (React MVP)

Lean read-only React app served at `/app/` by the FastAPI app in `src/rquant/web/`.

Pages: 总览, 选股与排序, 池子, 回测结果, 模拟盘, 告警时间线 (+确认), 市场全景, 系统健康.

Writes (only three): save pool, acknowledge alert, add to watchlist. The API forwards each
one through `rquant.web.page_control.forward` to the page-control service
(`RQUANT_PAGE_CONTROL_URL`, default `http://127.0.0.1:8767/v1/commands`); the web process
writes nothing itself.

## Run

Node 22 + pnpm (corepack), Python via uv.

```bash
cd web && pnpm install && pnpm run build && cd ..
uv run python -m rquant.web serve --fixture   # invented demo data, http://127.0.0.1:8768/app/
uv run python -m rquant.web serve             # real Serving generation (RQUANT_SERVING_ROOT)
```

Dev server with hot reload: `uv run python -m rquant.web serve --fixture` and `pnpm dev`
(Vite proxies `/api` to :8768).

## Check

```bash
pnpm run lint && pnpm run typecheck && pnpm test   # biome, tsc, vitest
pnpm run gen:api                                   # regenerate OpenAPI types after API changes
pnpm run e2e                                       # Playwright smoke against the fixture server
uv run pytest -q tests/web                         # API tests
```

`web/dist` is build output and is not committed.
