import { fireEvent, screen, waitFor, within } from "@testing-library/react";
import type { CustomBlockItem, ScreenerRunRequest } from "@/api/endpoints";
import { renderApp } from "@/test/render";
import { resetRememberedWatch } from "./index";

const serving = { state: "ready", generation_id: "g-canvas", generated_at: "2026-10-20T07:00:00Z" };

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

const MARKET_ROWS = [
  {
    trade_date: "2026-10-20",
    code: "600519.SH",
    name: "贵州茅台",
    preset: "breakout",
    presets: ["breakout"],
    close: 1620,
    pct_chg: 2.1,
    roe: 24.5,
  },
  {
    trade_date: "2026-10-20",
    code: "600715.SH",
    name: "文投控股",
    preset: "pool1,breakout,volume_surge",
    presets: ["pool1", "breakout", "volume_surge"],
    close: 2.53,
    pct_chg: 9.9,
    roe: 8.1,
  },
  {
    trade_date: "2026-10-20",
    code: "000999.SZ",
    name: "*ST演示",
    preset: "all_market",
    presets: ["all_market"],
    close: 4.2,
    pct_chg: -2.0,
    roe: -5.0,
  },
];

afterEach(() => {
  resetRememberedWatch();
  window.localStorage.clear();
  window.sessionStorage.clear();
  vi.unstubAllGlobals();
  vi.unstubAllEnvs();
});

it("deduplicates duplicate stock rows (e.g. three 文投控股) and merges source labels on fallback", async () => {
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = new URL(input instanceof Request ? input.url : String(input), "http://localhost");
      const method = (
        init?.method ?? (input instanceof Request ? input.method : "GET")
      ).toUpperCase();
      const path = url.pathname;

      if (method === "POST" && path.endsWith("/screener/run")) {
        return json({ error: { code: "UNAVAILABLE", message: "fallback test" } }, 500);
      }
      if (method === "GET" && path.endsWith("/screen")) {
        return json({
          data: {
            trade_date: "2026-10-20",
            presets: ["pool1", "breakout", "volume_surge"],
            rows: [
              {
                trade_date: "2026-10-20",
                code: "600715.SH",
                name: "文投控股",
                preset: "pool1",
                close: 2.53,
                pct_chg: 9.9,
              },
              {
                trade_date: "2026-10-20",
                code: "600715.SH",
                name: "文投控股",
                preset: "breakout",
                close: 2.53,
                pct_chg: 9.9,
              },
              {
                trade_date: "2026-10-20",
                code: "600715.SH",
                name: "文投控股",
                preset: "volume_surge",
                close: 2.53,
                pct_chg: 9.9,
              },
            ],
          },
          serving,
        });
      }
      if (method === "GET" && path.endsWith("/watchlist")) {
        return json({ data: { items: [] }, serving });
      }
      if (method === "GET" && path.endsWith("/pools")) {
        return json({ data: { trade_date: "2026-10-20", pools: [] }, serving });
      }
      if (method === "GET" && path.endsWith("/conditions")) {
        return json({ data: { runs: [] }, serving });
      }
      if (method === "GET" && path.endsWith("/meta")) {
        return json({ data: { app: "rQuant", version: "t", generation: serving }, serving });
      }
      return json({ data: {}, serving });
    }),
  );

  renderApp("/screener");
  const cells = await screen.findAllByText("文投控股");
  expect(cells).toHaveLength(1);
  expect(screen.getByText("N字初筛 / 突破 / 放量")).toBeInTheDocument();
});

it("renders Mode-B Pool Canvas with trackpad pinch-zoom, node drag, inline filter creation, and NL -> ★ 我的积木 sync", async () => {
  const runRequests: ScreenerRunRequest[] = [];
  const savedCustomBlocks: CustomBlockItem[] = [];

  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = new URL(input instanceof Request ? input.url : String(input), "http://localhost");
      const method = (
        init?.method ?? (input instanceof Request ? input.method : "GET")
      ).toUpperCase();
      const path = url.pathname;
      const rawBody =
        typeof init?.body === "string"
          ? init.body
          : input instanceof Request && method !== "GET"
            ? await input.clone().text()
            : "";

      if (method === "GET" && path.endsWith("/screener/catalog")) {
        return json({
          data: {
            trade_date: "2026-10-20",
            all_market_count: 3,
            presets: ["pool1", "breakout"],
            upstream_options: [],
            rules: [
              {
                name: "not_st",
                title: "排除 ST / *ST",
                category: "filter",
                category_label: "基础过滤",
                description: "剔除风险警示股",
                examples: ["排除ST"],
                params: [],
              },
              {
                name: "not_bj",
                title: "排除北交所",
                category: "filter",
                category_label: "基础过滤",
                description: "剔除北交所标的",
                examples: ["排除北交所"],
                params: [],
              },
              {
                name: "above_ma",
                title: "站上均线",
                category: "ma",
                category_label: "均线趋势",
                description: "收盘高于均线",
                examples: ["站上20日线"],
                params: [{ name: "period", label: "周期", kind: "int", default: 20 }],
              },
            ],
            field_groups: [
              { group: "财务", items: [{ field: "roe", label: "ROE %", is_factor: false }] },
            ],
            custom_blocks: savedCustomBlocks,
          },
          serving,
        });
      }

      if (method === "POST" && path.endsWith("/screener/run")) {
        const req = JSON.parse(rawBody || "{}") as ScreenerRunRequest;
        runRequests.push(req);
        const up = req.upstream ?? { base: "presets" };
        let rows =
          up.base === "codes"
            ? MARKET_ROWS.filter((r) => (up.codes ?? []).includes(r.code))
            : up.base === "all_market"
              ? [...MARKET_ROWS]
              : MARKET_ROWS.filter((r) => r.code !== "000999.SZ");
        const startCount = rows.length;
        for (const b of req.blocks ?? []) {
          if (b.enabled === false) continue;
          if (b.rule_name === "not_st") {
            rows = rows.filter((r) => !r.name.includes("ST"));
          }
        }
        return json({
          data: {
            trade_date: "2026-10-20",
            presets: ["pool1", "breakout"],
            pool_refs: ["all_market"],
            active_step: 1,
            steps: [
              {
                step: 0,
                block_id: null,
                label: up.base_label || "输入",
                remaining: startCount,
                dropped: 0,
                dropped_pct: 0,
              },
              {
                step: 1,
                block_id: "b1",
                label: "筛选后",
                remaining: rows.length,
                dropped: startCount - rows.length,
                dropped_pct: 0,
              },
            ],
            rows,
          },
          serving,
        });
      }

      if (method === "POST" && path.endsWith("/screener/from-text")) {
        const body = JSON.parse(rawBody || "{}") as { text?: string };
        return json({
          mode: "local",
          summary: `拆解：${body.text ?? ""}`,
          expression: "not_st()",
          blocks: [
            {
              id: "nl-canvas-1",
              type: "rule",
              enabled: true,
              rule_name: "not_st",
              label: "排除 ST / *ST",
              args: {},
              op: ">=",
              value: 0,
            },
          ],
        });
      }

      if (method === "PUT" && path.includes("/screener/blocks/")) {
        const blockId = decodeURIComponent(path.split("/screener/blocks/")[1] ?? "cb-nl");
        const body = JSON.parse(rawBody || "{}") as {
          name: string;
          description?: string;
          kind: "tdx" | "rule" | "field" | "composite";
          spec?: Record<string, unknown>;
        };
        const saved: CustomBlockItem = {
          block_id: blockId,
          name: body.name,
          description: body.description ?? "",
          kind: body.kind,
          spec: body.spec ?? {},
          created_at: "2026-10-20T07:10:00Z",
        };
        savedCustomBlocks.push(saved);
        return json(saved);
      }

      if (method === "GET" && path.endsWith("/ui-preferences")) {
        return json({ data: { preferences: {} }, serving });
      }
      if (method === "PUT" && path.endsWith("/ui-preferences")) {
        return json({ data: { key: "screener.canvas.v1", value: {} }, serving });
      }
      if (method === "GET" && path.endsWith("/watchlist")) {
        return json({ data: { items: [] }, serving });
      }
      if (method === "GET" && path.endsWith("/pools")) {
        return json({ data: { trade_date: "2026-10-20", pools: [] }, serving });
      }
      if (method === "GET" && path.endsWith("/meta")) {
        return json({ data: { app: "rQuant", version: "t", generation: serving }, serving });
      }
      return json({ data: { trade_date: "2026-10-20", presets: [], rows: MARKET_ROWS }, serving });
    }),
  );

  renderApp("/screener?view=canvas");

  // 1. Default DAG nodes render and evaluate cascade via base="codes"
  expect(await screen.findByText("Pool 1 · 基础初筛池")).toBeInTheDocument();
  expect(screen.getByText("Pool 2-1 · 趋势精选池")).toBeInTheDocument();
  await waitFor(() => expect(runRequests.some((r) => r.upstream?.base === "codes")).toBe(true));

  // 2. MacBook Trackpad two-finger pinch gesture (ctrlKey wheel) updates canvas zoom %
  const stage = screen.getByTestId("pool-canvas-stage");
  expect(screen.getByRole("button", { name: "重置缩放 100%" })).toHaveTextContent("100%");
  fireEvent.wheel(stage, { deltaY: -25, ctrlKey: true, clientX: 200, clientY: 160 });
  await waitFor(() =>
    expect(screen.getByRole("button", { name: "重置缩放 100%" })).not.toHaveTextContent("100%"),
  );
  fireEvent.click(screen.getByRole("button", { name: "重置缩放 100%" }));
  expect(screen.getByRole("button", { name: "重置缩放 100%" })).toHaveTextContent("100%");

  // 3. Node card is draggable on the canvas
  const pool1Card = stage.querySelector('[data-canvas-node="pool_1"]') as HTMLElement;
  expect(pool1Card).toBeTruthy();
  const initialLeft = pool1Card.style.left;
  fireEvent.pointerDown(pool1Card, { pointerId: 1, clientX: 100, clientY: 100 });
  fireEvent.pointerMove(window, { pointerId: 1, clientX: 180, clientY: 140 });
  fireEvent.pointerUp(window, { pointerId: 1, clientX: 180, clientY: 140 });
  expect(pool1Card.style.left).not.toBe(initialLeft);

  // 4. NL condition in right inspector drawer auto-syncs into selectable ★ 我的积木
  const inspector = screen.getByRole("complementary", { name: "节点配置抽屉" });
  const nlInput = within(inspector).getByLabelText("一句话条件");
  fireEvent.change(nlInput, { target: { value: "排除ST龙头" } });
  fireEvent.click(within(inspector).getByRole("button", { name: "一句话生成" }));

  await waitFor(() => expect(savedCustomBlocks.length).toBeGreaterThan(0));
  expect(savedCustomBlocks[0]?.name).toContain("NL · 排除ST龙头");

  // Open "+ 添加条件积木" inside the inspector and confirm the new NL block is immediately selectable!
  fireEvent.click(within(inspector).getByRole("button", { name: "+ 添加条件积木" }));
  const picker = await screen.findByRole("dialog", { name: "选股积木库" });
  expect(await within(picker).findByText(/★ NL · 排除ST龙头/)).toBeInTheDocument();
  fireEvent.click(within(inspector).getByRole("button", { name: "+ 添加条件积木" }));

  // 5. Create a new downstream node with inline filter condition editing directly in the creation drawer
  fireEvent.click(screen.getByRole("button", { name: "+ 新建下游节点" }));
  const drawer = await screen.findByRole("dialog", { name: "新建下游池子节点" });
  fireEvent.change(within(drawer).getByLabelText("节点名称"), {
    target: { value: "Pool 3 · 尾盘精选" },
  });
  fireEvent.click(within(drawer).getByRole("button", { name: "+ 添加条件积木" }));
  const drawerPicker = within(drawer).getByRole("dialog", { name: "选股积木库" });
  fireEvent.click(within(drawerPicker).getByRole("button", { name: /排除北交所/ }));
  fireEvent.click(within(drawer).getByRole("button", { name: "创建下游节点并连线" }));

  expect(await screen.findByText("Pool 3 · 尾盘精选")).toBeInTheDocument();

  // 6. Create a new initial (root) node
  fireEvent.click(screen.getByRole("button", { name: "+ 新建初始节点" }));
  const rootDrawer = await screen.findByRole("dialog", { name: "新建初始节点" });
  fireEvent.change(within(rootDrawer).getByLabelText("节点名称"), {
    target: { value: "自选观察源" },
  });
  fireEvent.click(within(rootDrawer).getByRole("button", { name: "创建初始节点" }));
  expect(await screen.findByText("自选观察源")).toBeInTheDocument();
});
