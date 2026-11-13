import { fireEvent, screen, waitFor, within } from "@testing-library/react";
import type { CustomBlockItem, ScreenerRunRequest } from "@/api/endpoints";
import { renderApp } from "@/test/render";
import { resetRememberedWatch } from "./index";

const serving = { state: "ready", generation_id: "g-wb", generated_at: "2026-10-20T07:00:00Z" };

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

const ALL_ROWS = [
  {
    trade_date: "2026-10-20",
    code: "600519.SH",
    name: "贵州茅台",
    preset: "breakout",
    close: 1620,
    pct_chg: 2.1,
    roe: 24.5,
  },
  {
    trade_date: "2026-10-20",
    code: "600036.SH",
    name: "招商银行",
    preset: "all_market",
    close: 36.8,
    pct_chg: 1.4,
    roe: 16.2,
  },
  {
    trade_date: "2026-10-20",
    code: "000858.SZ",
    name: "五粮液",
    preset: "all_market",
    close: 142.0,
    pct_chg: -0.6,
    roe: 19.8,
  },
];

afterEach(() => {
  resetRememberedWatch();
  window.sessionStorage.clear();
  vi.unstubAllGlobals();
  vi.unstubAllEnvs();
});

function stubWorkbench() {
  const runRequests: ScreenerRunRequest[] = [];
  const savedCustomBlocks: CustomBlockItem[] = [];
  const nlPrompts: string[] = [];

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
            presets: ["breakout", "pullback"],
            upstream_options: [
              { ref: "preset:breakout", kind: "preset", label: "预设 · 突破", count: 1 },
              { ref: "pool:黑名单池", kind: "pool", label: "池子 · 黑名单池", count: 1 },
              { ref: "watchlist", kind: "watchlist", label: "自选股", count: 1 },
            ],
            rules: [
              {
                name: "exclude_st",
                title: "排除 ST / *ST",
                category: "filter",
                category_label: "基础过滤",
                description: "剔除风险警示股",
                example: "exclude_st()",
                params: [],
              },
              {
                name: "above_ma",
                title: "收盘站上均线",
                category: "ma",
                category_label: "均线趋势",
                description: "收盘价高于指定周期均线",
                example: "above_ma(period=20)",
                params: [
                  {
                    name: "period",
                    label: "均线周期",
                    kind: "int",
                    default: 20,
                    min: 3,
                    max: 250,
                    step: 1,
                  },
                ],
              },
              {
                name: "first_seal",
                title: "首板涨停",
                category: "seal",
                category_label: "涨跌停与连板",
                description: "当日或昨日首次封板",
                example: "first_seal(offset=1)",
                params: [
                  {
                    name: "offset",
                    label: "回溯日",
                    kind: "offset",
                    default: 0,
                    min: 0,
                    max: 5,
                  },
                ],
              },
            ],
            field_groups: [
              {
                group: "财务",
                items: [{ field: "roe", label: "ROE %", is_factor: false }],
              },
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
        let baseRows =
          up.base === "all_market"
            ? [...ALL_ROWS]
            : ALL_ROWS.filter((r) => r.preset === "breakout");
        if ((up.exclude_pools ?? []).includes("pool:黑名单池")) {
          baseRows = baseRows.filter((r) => r.code !== "000858.SZ");
        }
        const steps: Array<{
          step: number;
          block_id: string | null;
          label: string;
          remaining: number;
          dropped: number;
          dropped_pct: number;
        }> = [
          {
            step: 0,
            block_id: null,
            label: up.base === "all_market" ? "全市场 A 股" : "今日候选池",
            remaining: baseRows.length,
            dropped: 0,
            dropped_pct: 0,
          },
        ];
        const snapshots = [baseRows];
        let cur = baseRows;
        for (const [idx, b] of (req.blocks ?? []).entries()) {
          if (b.enabled === false) continue;
          const before = cur.length;
          if (b.type === "rule" && b.rule_name === "above_ma") {
            const p = Number(b.args?.period ?? 20);
            cur = p >= 60 ? cur.slice(0, 1) : cur.slice(0, 2);
          } else {
            cur = cur.slice(0, Math.max(1, cur.length - 1));
          }
          const dropped = before - cur.length;
          steps.push({
            step: idx + 1,
            block_id: b.id ?? `b-${idx}`,
            label: b.label || b.rule_name || b.tdx || b.field || "条件",
            remaining: cur.length,
            dropped,
            dropped_pct: before > 0 ? Math.round((dropped / before) * 100) : 0,
          });
          snapshots.push(cur);
        }
        const lastIdx = steps.length - 1;
        const activeStep =
          req.preview_step != null && req.preview_step >= 0 && req.preview_step <= lastIdx
            ? req.preview_step
            : lastIdx;
        return json({
          data: {
            trade_date: "2026-10-20",
            presets: ["breakout", "pullback"],
            pool_refs: up.base === "all_market" ? ["all_market"] : ["breakout"],
            active_step: activeStep,
            steps,
            rows: snapshots[activeStep] ?? cur,
          },
          serving,
        });
      }

      if (method === "POST" && path.endsWith("/screener/from-text")) {
        const body = JSON.parse(rawBody || "{}") as { text?: string };
        nlPrompts.push(body.text ?? "");
        return json({
          text: body.text ?? "",
          mode: "local",
          summary: "白盒拆解为 2 条积木",
          expression: "exclude_st() & first_seal(offset=1)",
          blocks: [
            {
              id: "nl-1",
              type: "rule",
              enabled: true,
              rule_name: "exclude_st",
              label: "排除 ST / *ST",
              args: {},
              op: ">=",
              value: 0,
            },
            {
              id: "nl-2",
              type: "rule",
              enabled: true,
              rule_name: "first_seal",
              label: "昨日首板",
              args: { offset: 1 },
              op: ">=",
              value: 0,
            },
          ],
        });
      }

      if (method === "PUT" && path.includes("/screener/blocks/")) {
        const blockId = decodeURIComponent(path.split("/screener/blocks/")[1] ?? "cb-1");
        const body = JSON.parse(rawBody || "{}") as {
          name: string;
          description?: string;
          kind: "tdx" | "rule" | "field" | "composite";
          spec?: Record<string, unknown>;
        };
        const item: CustomBlockItem = {
          block_id: blockId,
          name: body.name,
          description: body.description ?? "",
          kind: body.kind,
          spec: body.spec ?? {},
          created_at: "2026-10-20T07:05:00Z",
        };
        savedCustomBlocks.push(item);
        return json(item);
      }

      if (method === "GET" && path.endsWith("/screener/blocks")) {
        return json({ data: { blocks: savedCustomBlocks }, serving });
      }

      if (method === "GET" && path.endsWith("/pools")) {
        return json({
          data: {
            trade_date: "2026-10-20",
            pools: [{ name: "黑名单池", kind: "manual", members: [{ code: "000858.SZ" }] }],
          },
          serving,
        });
      }

      if (method === "GET" && path.endsWith("/watchlist")) {
        return json({ data: { items: [] }, serving });
      }

      if (method === "GET" && path.endsWith("/conditions")) {
        return json({ data: { runs: [] }, serving });
      }

      if (method === "GET" && path.endsWith("/meta")) {
        return json({
          data: { app: "rQuant", version: "t", generation: serving },
          serving,
        });
      }

      return json({
        data: { trade_date: "2026-10-20", presets: ["breakout"], rows: [ALL_ROWS[0]] },
        serving,
      });
    }),
  );

  return { runRequests, savedCustomBlocks, nlPrompts };
}

it("switches cascading upstream to all-market and applies set exclusion", async () => {
  const { runRequests } = stubWorkbench();
  renderApp("/screener");

  expect(await screen.findByText("贵州茅台")).toBeInTheDocument();
  expect(screen.queryByText("招商银行")).not.toBeInTheDocument();

  const allMarketBtn = await screen.findByRole("button", { name: /全市场 A 股/ });
  fireEvent.click(allMarketBtn);

  expect(await screen.findByText("招商银行")).toBeInTheDocument();
  expect(await screen.findByText("五粮液")).toBeInTheDocument();

  fireEvent.change(screen.getByLabelText("集合运算类型"), { target: { value: "exclude" } });
  fireEvent.change(screen.getByLabelText("集合目标池子"), { target: { value: "pool:黑名单池" } });
  fireEvent.click(screen.getByRole("button", { name: "加入范围" }));

  await waitFor(() => expect(screen.queryByText("五粮液")).not.toBeInTheDocument());
  expect(screen.getByText("招商银行")).toBeInTheDocument();
  expect(runRequests.at(-1)?.upstream).toMatchObject({
    base: "all_market",
    exclude_pools: ["pool:黑名单池"],
  });
});

it("picks a built-in rule block, edits its parameter inline, and previews intermediate funnel steps", async () => {
  const { runRequests } = stubWorkbench();
  renderApp("/screener");

  fireEvent.click(await screen.findByRole("button", { name: /全市场 A 股/ }));
  expect(await screen.findByText("五粮液")).toBeInTheDocument();

  fireEvent.click(screen.getByRole("button", { name: "+ 添加条件积木" }));
  const dialog = await screen.findByRole("dialog", { name: "选股积木库" });
  fireEvent.click(within(dialog).getByRole("button", { name: /收盘站上均线/ }));

  const periodInput = await screen.findByLabelText("收盘站上均线-均线周期");
  expect(periodInput).toHaveValue(20);
  await waitFor(() => expect(screen.queryByText("五粮液")).not.toBeInTheDocument());
  expect(screen.getByText("招商银行")).toBeInTheDocument();

  fireEvent.change(periodInput, { target: { value: "60" } });
  await waitFor(() => expect(screen.queryByText("招商银行")).not.toBeInTheDocument());
  expect(runRequests.at(-1)?.blocks?.[0]?.args).toMatchObject({ period: 60 });

  // Click step 0 in the funnel to preview upstream stocks before filtering
  const funnel = screen.getByRole("list", { name: "逐条命中" });
  fireEvent.click(within(funnel).getByRole("button", { name: /全市场 A 股/ }));

  expect(await screen.findByText("五粮液")).toBeInTheDocument();
  expect(screen.getByText(/正在预览第 0 层/)).toBeInTheDocument();

  fireEvent.click(screen.getByRole("button", { name: /返回最终结果/ }));
  await waitFor(() => expect(screen.queryByText("五粮液")).not.toBeInTheDocument());
});

it("compiles white-box natural language into blocks and saves them to ★ 我的积木", async () => {
  const { nlPrompts, savedCustomBlocks } = stubWorkbench();
  renderApp("/screener");

  const nlInput = await screen.findByLabelText("一句话条件");
  fireEvent.change(nlInput, { target: { value: "排除ST且昨日首板" } });
  fireEvent.click(screen.getByRole("button", { name: "一句话生成" }));

  await waitFor(() => expect(nlPrompts).toEqual(["排除ST且昨日首板"]));
  expect(await screen.findByLabelText("生成结果")).toHaveTextContent(
    "已加入 2 个白盒积木 · 校验通过",
  );

  // Save the active combination as a reusable custom block
  fireEvent.click(screen.getByRole("button", { name: "★ 存为我的积木" }));
  const savePanel = await screen.findByRole("region", { name: "保存自定义积木面板" });
  fireEvent.change(within(savePanel).getByLabelText("自定义积木名称"), {
    target: { value: "昨日首板滤网" },
  });
  fireEvent.click(within(savePanel).getByRole("button", { name: "保存积木" }));

  await waitFor(() => expect(savedCustomBlocks).toHaveLength(1));
  expect(savedCustomBlocks[0]).toMatchObject({
    name: "昨日首板滤网",
    kind: "composite",
  });

  // Open picker and verify the saved block appears under ★ 我的积木
  fireEvent.click(screen.getByRole("button", { name: "+ 添加条件积木" }));
  const dialog = await screen.findByRole("dialog", { name: "选股积木库" });
  expect(await within(dialog).findByText("★ 昨日首板滤网")).toBeInTheDocument();
});
