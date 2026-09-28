import { act, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import type { Schemas } from "@/api/client";
import { metaEnvelope } from "@/test/fixtures";
import { findJargon } from "@/test/jargon";
import { renderApp } from "@/test/render";
import { server } from "@/test/server";

vi.mock("@/charts/PriceChart", () => ({
  PriceChart: ({
    label,
    marks,
    referenceLine,
  }: {
    label: string;
    marks?: readonly { time: string; label: string }[];
    referenceLine?: { price: number; label: string };
  }) => (
    <div
      role="img"
      aria-label={label}
      data-marks={JSON.stringify(marks ?? [])}
      data-reference={JSON.stringify(referenceLine ?? null)}
    />
  ),
}));

const serving = metaEnvelope().serving;
const base: Schemas["PoolsData"] = {
  state: "ready",
  latest_trade_date: "2026-09-23",
  definitions_available: true,
  rules_available: false,
  canvases: [
    {
      name: "观察画布",
      description: "日终观察",
      pool_keys: ["n-shape-pool1", "deleted-pool"],
      refs_truncated: false,
    },
  ],
  canvases_truncated: false,
  pools: [
    {
      key: "n-shape-pool1",
      name: "N 字一池",
      state: "current",
      trade_date: "2026-09-23",
      member_count: 121,
      gain_verified_count: 0,
      gain_sample_avg_pct: null,
      steps: [{ step_index: 0, label: "最终命中", count: 121 }],
      steps_truncated: false,
      members: [
        {
          code: "600001.SH",
          name: "样本01",
          close: 11,
          pct_chg: 1.2,
          entry_trade_date: null,
          entry_close: null,
          gain_pct: null,
          gain_through_date: null,
          entry_line_price: null,
        },
      ],
      members_truncated: true,
      result: {
        state: "unverified",
        status_label: "结果版本待确认",
        trade_date: null,
        hit_count: null,
      },
    },
    {
      key: "deleted-pool",
      name: "选股池",
      state: "unpublished",
      trade_date: null,
      member_count: null,
      gain_verified_count: 0,
      gain_sample_avg_pct: null,
      steps: [],
      steps_truncated: false,
      members: [],
      members_truncated: false,
      result: {
        state: "not_run",
        status_label: "尚无选股结果",
        trade_date: null,
        hit_count: null,
      },
    },
  ],
  pools_truncated: false,
};

const firstRule: Schemas["PoolDefinitionView"] = {
  name: "N 形态一池",
  state: "available",
  status_label: "已发布",
  reason_label: null,
  source_label: "内置规则",
  description: "昨首板与安全过滤",
  depends_on: null,
  delay_label: null,
  rules: [{ label: "排除 ST", parameters: [] }],
};
const secondRule: Schemas["PoolDefinitionView"] = {
  ...firstRule,
  name: "N 形态二池",
  depends_on: "n-shape-pool1",
  delay_label: "使用父池前 2 个交易日内的成员",
  rules: [
    {
      label: "明显下影线",
      parameters: [
        { label: "最小振幅（%）", value: "2%" },
        { label: "相对日期", value: "所选交易日" },
      ],
    },
  ],
};

function respond(data: Schemas["PoolsData"] = base) {
  server.use(http.get("*/api/v1/pools", () => HttpResponse.json({ data, serving })));
}

function respondFormula(
  latestResult: Schemas["FormulaPoolLatestResult"] | null,
  generation = serving.generation_id,
) {
  server.use(
    http.get("*/api/v1/pools/formula", () =>
      HttpResponse.json({
        data: {
          availability: "ready",
          available_at: "2026-09-24T07:31:00Z",
          message: "",
          pools: [
            {
              pool_name: "user/趋势池",
              display_name: "趋势池",
              formula: "CLOSE>MA(CLOSE,2)",
              syntax_version: "tdx-v1",
              created_at: "2026-09-24T07:31:00Z",
              status_label: latestResult ? "已有结果" : "尚未运行",
              latest_result: latestResult,
              version: "f".repeat(64),
            },
          ],
        },
        serving: { ...serving, generation_id: generation },
      }),
    ),
  );
}

const formulaResult: Schemas["FormulaPoolLatestResult"] = {
  trade_date: "2026-09-24",
  market_total: 100,
  match_count: 51,
  no_match_count: 40,
  unknown_count: 9,
  unknown_reasons: [{ reason: "missing", label: "行情字段缺失", count: 9 }],
};

it("在全部池子显示公式定义、真实运行日和同代分页成员，且不打开规则编辑", async () => {
  respond({ ...base, canvases: [], pools: [] });
  respondFormula(formulaResult);
  const cursors: (string | null)[] = [];
  server.use(
    http.get("*/api/v1/pools/formula/*/members", ({ request }) => {
      const cursor = new URL(request.url).searchParams.get("cursor");
      cursors.push(cursor);
      return HttpResponse.json({
        data: {
          pool_name: "user/趋势池",
          trade_date: "2026-09-24",
          total: 51,
          offset: cursor ? 50 : 0,
          match_codes: [cursor ? "600051.SH" : "600001.SH"],
          next_cursor: cursor ? null : "page-2",
        },
        serving,
      });
    }),
    http.get("*/api/v1/stocks/600001.SH/summary", () =>
      HttpResponse.json({
        data: { ts_code: "600001.SH", name: "样本01", price: 11, as_of: null, pools: [] },
        serving,
      }),
    ),
    http.get("*/api/v1/panorama/stocks/600001.SH/daily", () =>
      HttpResponse.json({
        data: { ts_code: "600001.SH", name: "样本01", bars: [] },
        serving,
      }),
    ),
  );
  const user = userEvent.setup();
  const { container } = renderApp("/pools");
  const list = await screen.findByRole("group", { name: "池子列表" });
  const formulaButton = await within(list).findByRole("button", { name: "查看 趋势池公式池" });
  act(() => formulaButton.focus());
  await user.keyboard("{Enter}");
  const detail = screen.getByRole("region", { name: "池子详情" });
  expect(within(detail).getByText("CLOSE>MA(CLOSE,2)")).toBeVisible();
  expect(within(detail).getByText(/选股日期 · 2026-09-24/)).toBeVisible();
  expect(within(detail).getByText("行情字段缺失")).toBeVisible();
  expect(within(detail).queryByRole("button", { name: "编辑规则" })).toBeNull();
  await user.click(await within(detail).findByRole("button", { name: /600001.SH/ }));
  expect(await screen.findByRole("dialog")).toHaveTextContent("样本01");
  await user.click(within(screen.getByRole("dialog")).getByRole("button", { name: "关闭" }));
  await user.click(within(detail).getByRole("button", { name: "下一页" }));
  expect(await within(detail).findByRole("button", { name: /600051.SH/ })).toBeVisible();
  expect(cursors).toEqual([null, "page-2"]);
  expect(findJargon(container.textContent ?? "")).toEqual([]);
});

it("公式池区分尚未运行、可信零命中和旧数据代", async () => {
  respond({ ...base, canvases: [], pools: [] });
  respondFormula(null);
  const view = renderApp("/pools");
  expect((await screen.findAllByText("尚未运行")).length).toBeGreaterThan(0);
  expect(screen.queryByRole("region", { name: "公式池成员" })).toBeNull();
  view.unmount();

  respondFormula({ ...formulaResult, match_count: 0, no_match_count: 91 });
  const zero = renderApp("/pools");
  expect(await screen.findByText("本次没有命中股票")).toBeVisible();
  expect(screen.queryByRole("region", { name: "公式池成员" })).toBeNull();
  zero.unmount();

  respondFormula(formulaResult, "new-generation");
  renderApp("/pools");
  expect(await screen.findByText("公式池数据正在更新，稍后查看。")).toBeVisible();
  expect(screen.queryByRole("button", { name: "查看 趋势池公式池" })).toBeNull();
});

it("规则池接口暂不可用时仍能查看同代公式池定义", async () => {
  server.use(
    http.get("*/api/v1/pools", () => HttpResponse.json({ detail: "暂不可用" }, { status: 503 })),
  );
  respondFormula(null);
  renderApp("/pools");
  const button = await screen.findByRole("button", { name: "查看 趋势池公式池" });
  await userEvent.setup().click(button);
  expect(screen.getByRole("region", { name: "公式条件" })).toHaveTextContent("CLOSE>MA(CLOSE,2)");
});

it("坏成员不进入列表，游标失效后从第一页重新读取", async () => {
  respond({ ...base, canvases: [], pools: [] });
  respondFormula(formulaResult);
  let firstReads = 0;
  server.use(
    http.get("*/api/v1/pools/formula/*/members", ({ request }) => {
      const cursor = new URL(request.url).searchParams.get("cursor");
      if (cursor) return HttpResponse.json({ detail: "结果已更新" }, { status: 409 });
      firstReads += 1;
      return HttpResponse.json({
        data: {
          pool_name: "user/趋势池",
          trade_date: "2026-09-24",
          total: 51,
          offset: 0,
          match_codes: [firstReads === 1 ? "bad-code" : "600001.SH"],
          next_cursor: "next-page",
        },
        serving,
      });
    }),
  );
  const user = userEvent.setup();
  renderApp("/pools");
  const detail = await screen.findByRole("region", { name: "池子详情" });
  expect(await within(detail).findByText("成员暂时无法读取")).toBeVisible();
  expect(within(detail).queryByRole("button", { name: /bad-code/ })).toBeNull();
  await user.click(within(detail).getByRole("button", { name: "重试读取" }));
  expect(await within(detail).findByRole("button", { name: /600001.SH/ })).toBeVisible();
  await user.click(within(detail).getByRole("button", { name: "下一页" }));
  expect(await within(detail).findByText("结果已更新")).toBeVisible();
  await user.click(within(detail).getByRole("button", { name: "从第一页重看" }));
  expect(await within(detail).findByRole("button", { name: /600001.SH/ })).toBeVisible();
  expect(firstReads).toBe(3);
});

it("selects a published pool by keyboard and opens a member's stock drawer", async () => {
  respond();
  server.use(
    http.get("*/api/v1/stocks/600001.SH/summary", () =>
      HttpResponse.json({
        data: { ts_code: "600001.SH", name: "样本01", price: 11, as_of: null, pools: [] },
        serving,
      }),
    ),
    http.get("*/api/v1/panorama/stocks/600001.SH/daily", () =>
      HttpResponse.json({ data: { ts_code: "600001.SH", name: "样本01", bars: [] }, serving }),
    ),
  );
  const user = userEvent.setup();
  const { container } = renderApp("/pools");
  const list = await screen.findByRole("group", { name: "池子列表" });
  const selected = within(list).getByRole("button", { name: /N 字一池/ });
  act(() => selected.focus());
  await user.keyboard("{Enter}");
  expect(
    within(screen.getByRole("region", { name: "池子详情" })).getAllByText("121 只").length,
  ).toBeGreaterThan(0);
  expect(screen.getByText("最终命中")).toBeInTheDocument();
  expect(screen.getByText(/仅显示前 1 只/)).toBeInTheDocument();
  expect(container.querySelectorAll(".flow-graph-edge")).toHaveLength(0);
  expect(findJargon(container.textContent ?? "")).toEqual([]);
  await user.click(screen.getByRole("row", { name: /样本01/ }));
  expect(await screen.findByRole("dialog")).toHaveTextContent("样本01");
});

it.each([
  {
    name: "same generation with the entry day",
    generation: serving.generation_id,
    day: "2026-09-22",
    marked: true,
  },
  {
    name: "another generation",
    generation: "another-generation",
    day: "2026-09-22",
    marked: false,
  },
  {
    name: "daily bars without the entry day",
    generation: serving.generation_id,
    day: "2026-09-23",
    marked: false,
  },
])("shows an entry marker only for $name", async ({ generation, day, marked }) => {
  const first = base.pools[0];
  const member = first?.members[0];
  if (!first || !member) throw new Error("pool fixture is incomplete");
  respond({
    ...base,
    pools: [
      {
        ...first,
        member_count: 1,
        members: [{ ...member, entry_trade_date: "2026-09-22", entry_close: null }],
        result: {
          state: "current_rules",
          status_label: "结果已按当前规则更新",
          trade_date: "2026-09-23",
          hit_count: 1,
        },
      },
    ],
  });
  server.use(
    http.get("*/api/v1/stocks/600001.SH/summary", () =>
      HttpResponse.json({
        data: { ts_code: "600001.SH", name: "样本01", price: 11, as_of: null, pools: [] },
        serving,
      }),
    ),
    http.get("*/api/v1/panorama/stocks/600001.SH/daily", () =>
      HttpResponse.json({
        data: {
          ts_code: "600001.SH",
          name: "样本01",
          bars: [
            {
              date: day,
              open: 10,
              high: 11,
              low: 9.8,
              close: 10.5,
              volume: 1000,
              ma5: null,
              ma10: null,
              ma20: null,
              provisional: false,
            },
          ],
        },
        serving: { ...serving, generation_id: generation },
      }),
    ),
  );
  const user = userEvent.setup();
  const { container } = renderApp("/pools");
  const table = await screen.findByRole("table", { name: "池子成员" });
  expect(within(table).getByRole("columnheader", { name: "入池日" })).toBeInTheDocument();
  expect(within(table).getByRole("columnheader", { name: "入池日收盘价" })).toBeInTheDocument();
  expect(within(table).getByRole("row", { name: /样本01/ })).toHaveTextContent("09-22");
  await user.click(within(table).getByRole("row", { name: /样本01/ }));
  const drawer = await screen.findByRole("dialog");
  const chart = await within(drawer).findByRole("img", { name: "样本01 日 K" });
  expect(chart).toHaveAttribute(
    "data-marks",
    JSON.stringify(marked ? [{ time: "2026-09-22", label: "入池" }] : []),
  );
  expect(within(drawer).queryByText("入池 · 2026-09-22") !== null).toBe(marked);
  expect(findJargon(container.textContent ?? "")).toEqual([]);
});

it.each([
  {
    name: "same factor",
    line: 10.25,
    provisional: false,
    differentGeneration: false,
    expectedLine: true,
  },
  {
    name: "factor changed",
    line: null,
    provisional: false,
    differentGeneration: false,
    expectedLine: false,
  },
  {
    name: "current bar is provisional",
    line: 10.25,
    provisional: true,
    differentGeneration: false,
    expectedLine: false,
  },
  {
    name: "another generation",
    line: 10.25,
    provisional: false,
    differentGeneration: true,
    expectedLine: false,
  },
])(
  "shows verified return and safe entry line for $name",
  async ({ line, provisional, differentGeneration, expectedLine }) => {
    const first = base.pools[0];
    const member = first?.members[0];
    if (!first || !member) throw new Error("pool fixture is incomplete");
    respond({
      ...base,
      pools: [
        {
          ...first,
          member_count: 1,
          gain_verified_count: 1,
          gain_sample_avg_pct: 12.5,
          members: [
            {
              ...member,
              entry_trade_date: "2026-09-22",
              entry_close: 10.25,
              gain_pct: 12.5,
              gain_through_date: "2026-09-23",
              entry_line_price: line,
            },
          ],
          result: {
            state: "current_rules",
            status_label: "结果已按当前规则更新",
            trade_date: "2026-09-23",
            hit_count: 1,
          },
        },
      ],
    });
    server.use(
      http.get("*/api/v1/stocks/600001.SH/summary", () =>
        HttpResponse.json({
          data: { ts_code: "600001.SH", name: "样本01", price: 11, as_of: null, pools: [] },
          serving,
        }),
      ),
      http.get("*/api/v1/panorama/stocks/600001.SH/daily", () =>
        HttpResponse.json({
          data: {
            ts_code: "600001.SH",
            name: "样本01",
            bars: [
              {
                date: "2026-09-22",
                open: 10,
                high: 11,
                low: 9.8,
                close: 10.25,
                volume: 1000,
                ma5: null,
                ma10: null,
                ma20: null,
                provisional: false,
              },
              {
                date: "2026-09-23",
                open: 10.3,
                high: 11,
                low: 10,
                close: 11,
                volume: 1000,
                ma5: null,
                ma10: null,
                ma20: null,
                provisional,
              },
            ],
          },
          serving: differentGeneration
            ? { ...serving, generation_id: "another-generation" }
            : serving,
        }),
      ),
    );
    const user = userEvent.setup();
    const { container } = renderApp("/pools");
    const table = await screen.findByRole("table", { name: "池子成员" });
    expect(within(table).getByRole("columnheader", { name: "入池后复权涨幅" })).toBeInTheDocument();
    expect(within(table).getByRole("row", { name: /样本01/ })).toHaveTextContent("12.50%");
    expect(screen.getByText("已核验样本平均")).toBeInTheDocument();
    await user.click(within(table).getByRole("row", { name: /样本01/ }));
    const drawer = await screen.findByRole("dialog");
    const chart = await within(drawer).findByRole("img", { name: "样本01 日 K" });
    expect(chart).toHaveAttribute(
      "data-reference",
      JSON.stringify(expectedLine ? { price: 10.25, label: "入池日收盘价" } : null),
    );
    if (line === null) expect(within(drawer).getByText("价格口径不同")).toBeInTheDocument();
    expect(findJargon(container.textContent ?? "")).toEqual([]);
  },
);

it("shows independent published-rule and verified-result states in graph, list, and detail", async () => {
  const first = base.pools[0];
  if (!first) throw new Error("pool fixture is incomplete");
  respond({
    ...base,
    rules_available: true,
    pools: [
      {
        ...first,
        definition: firstRule,
        result: {
          state: "current_rules",
          status_label: "结果已按当前规则更新",
          trade_date: "2026-09-23",
          hit_count: 121,
        },
      },
    ],
  });
  const { container } = renderApp("/pools");
  const list = await screen.findByRole("group", { name: "池子列表" });
  expect(within(list).getByRole("button", { name: "查看 N 字一池成员" })).toHaveTextContent(
    "结果已按当前规则更新",
  );
  const graph = screen.getByRole("group", { name: "已发布规则与池子" });
  expect(graph.querySelector('[data-id="n-shape-pool1"]')).toHaveTextContent(
    "结果已按当前规则更新",
  );
  expect(graph.querySelector('[data-id="condition:n-shape-pool1"]')).toHaveTextContent(
    "规则已发布",
  );
  const detail = screen.getByRole("region", { name: "池子详情" });
  expect(within(detail).getByRole("region", { name: "规则详情" })).toHaveTextContent("已发布");
  expect(within(detail).getByRole("region", { name: "上次选股结果" })).toHaveTextContent(
    "结果已按当前规则更新",
  );
  expect(findJargon(container.textContent ?? "")).toEqual([]);
});

it("shows trusted zero hits without a fabricated step or old member", async () => {
  const first = base.pools[0];
  if (!first) throw new Error("pool fixture is incomplete");
  respond({
    ...base,
    rules_available: true,
    pools: [
      {
        ...first,
        definition: firstRule,
        member_count: 0,
        members: [],
        steps: [],
        result: {
          state: "current_rules",
          status_label: "结果已按当前规则更新",
          trade_date: "2026-09-23",
          hit_count: 0,
          zero_hit_label: "该交易日没有符合条件的股票",
        },
      },
    ],
  });
  renderApp("/pools");
  const detail = await screen.findByRole("region", { name: "上次选股结果" });
  expect(detail).toHaveTextContent("该交易日没有符合条件的股票");
  expect(detail).toHaveTextContent("0 只");
  expect(detail).not.toHaveTextContent("命中步骤");
  expect(screen.queryByText("样本01")).not.toBeInTheDocument();
});

it("keeps old members visible after a rule change and dates another pool's earlier run", async () => {
  const first = base.pools[0];
  if (!first) throw new Error("pool fixture is incomplete");
  respond({
    ...base,
    rules_available: true,
    canvases: [],
    pools: [
      {
        ...first,
        definition: firstRule,
        result: {
          state: "rules_changed",
          status_label: "规则已更新，等待下次选股",
          trade_date: "2026-09-23",
          hit_count: 121,
        },
      },
      {
        ...first,
        key: "user/旧日池",
        name: "旧日池",
        state: "older",
        trade_date: "2026-09-22",
        member_count: null,
        members: [],
        definition: { ...firstRule, name: "旧日池" },
        result: {
          state: "older_rules",
          status_label: "上次结果与当前规则一致",
          trade_date: "2026-09-22",
          hit_count: 2,
        },
      },
    ],
  });
  const user = userEvent.setup();
  renderApp("/pools");
  const detail = await screen.findByRole("region", { name: "池子详情" });
  expect(detail).toHaveTextContent("规则已更新，等待下次选股");
  expect(detail).toHaveTextContent("样本01");
  await user.click(screen.getByRole("button", { name: /查看 旧日池成员/ }));
  expect(detail).toHaveTextContent("上次结果与当前规则一致");
  expect(detail).toHaveTextContent("2026");
  expect(detail).toHaveTextContent("最近交易日未运行");
  expect(detail).not.toHaveTextContent("样本01");
});

it("does not call unpublished references invalid or reuse older counts", async () => {
  const current = base.pools[0];
  const unpublished = base.pools[1];
  if (!current || !unpublished) throw new Error("pool fixture is incomplete");
  respond({
    ...base,
    pools: [
      {
        ...current,
        state: "older",
        trade_date: "2026-09-22",
        member_count: null,
        members: [],
        steps: [],
      },
      unpublished,
    ],
  });
  const user = userEvent.setup();
  renderApp("/pools");
  expect((await screen.findAllByText(/不是最新交易日/)).length).toBeGreaterThan(0);
  await user.click(screen.getByRole("button", { name: /选股池/ }));
  expect(screen.getAllByText(/尚无已发布结果/).length).toBeGreaterThan(0);
  expect(screen.queryByText(/已失效/)).not.toBeInTheDocument();
});

it("selects a graph node's details with Enter and Space", async () => {
  const first = base.pools[0];
  const canvas = base.canvases[0];
  if (!first || !canvas) throw new Error("pool fixture is incomplete");
  respond({
    ...base,
    canvases: [{ ...canvas, pool_keys: ["n-shape-pool1", "n-shape-pool2"] }],
    pools: [first, { ...first, key: "n-shape-pool2", name: "N 字二池", member_count: 2 }],
  });
  const user = userEvent.setup();
  renderApp("/pools");
  const graph = await screen.findByRole("group", { name: "已发布池子；关系尚未发布" });
  const secondNode = graph.querySelector<HTMLElement>('[data-id="n-shape-pool2"]');
  const firstNode = graph.querySelector<HTMLElement>('[data-id="n-shape-pool1"]');
  expect(secondNode).toBeTruthy();
  expect(firstNode).toBeTruthy();
  act(() => secondNode?.focus());
  await user.keyboard("{Enter}");
  expect(
    within(screen.getByRole("region", { name: "池子详情" })).getByText("2 只"),
  ).toBeInTheDocument();
  act(() => firstNode?.focus());
  await user.keyboard("{Space}");
  expect(
    within(screen.getByRole("region", { name: "池子详情" })).getAllByText("121 只").length,
  ).toBeGreaterThan(0);
});

it("shows a truthful unavailable state without stale members", async () => {
  respond({ ...base, state: "unavailable", latest_trade_date: null, pools: [] });
  renderApp("/pools");
  expect(await screen.findByText(/池子结果暂不可用/)).toBeInTheDocument();
  expect(screen.queryByText("样本01")).not.toBeInTheDocument();
});

it("does not show cached pool members from a different generation", async () => {
  const first = base.pools[0];
  if (!first) throw new Error("pool fixture is incomplete");
  server.use(
    http.get("*/api/v1/pools", () =>
      HttpResponse.json({
        data: {
          ...base,
          pools: [
            {
              ...first,
              result: {
                state: "current_rules",
                status_label: "结果已按当前规则更新",
                trade_date: "2026-09-23",
                hit_count: 121,
              },
            },
          ],
        },
        serving: { ...serving, generation_id: "another-generation" },
      }),
    ),
  );
  renderApp("/pools");
  expect(await screen.findByText("池子数据正在更新")).toBeInTheDocument();
  expect(screen.queryByText("样本01")).not.toBeInTheDocument();
  expect(screen.queryByText("结果已按当前规则更新")).not.toBeInTheDocument();
});

it("explains when no pools have been published", async () => {
  respond({ ...base, state: "no_data", latest_trade_date: null, canvases: [], pools: [] });
  renderApp("/pools");
  expect(await screen.findByText("还没有已发布的池子")).toBeInTheDocument();
});

it("draws published dependencies through condition nodes and opens rules by keyboard", async () => {
  const first = base.pools[0];
  const canvas = base.canvases[0];
  if (!first || !canvas) throw new Error("pool fixture is incomplete");
  respond({
    ...base,
    rules_available: true,
    canvases: [{ ...canvas, pool_keys: ["n-shape-pool1", "n-shape-pool2"] }],
    pools: [
      { ...first, name: "N 形态一池", definition: firstRule },
      {
        ...first,
        key: "n-shape-pool2",
        name: "N 形态二池",
        member_count: 2,
        definition: secondRule,
      },
    ],
  });
  const user = userEvent.setup();
  const { container } = renderApp("/pools");
  const graph = await screen.findByRole("group", { name: "已发布规则与池子" });
  expect(graph.querySelectorAll(".flow-graph-node")).toHaveLength(4);
  expect(graph.querySelector('[data-id="n-shape-pool1"]')).toHaveAttribute("aria-pressed", "true");
  const condition = graph.querySelector<HTMLElement>('[data-id="condition:n-shape-pool2"]');
  expect(condition).toBeTruthy();
  act(() => condition?.focus());
  await user.keyboard("{Enter}");
  expect(condition).toHaveAttribute("aria-pressed", "true");
  expect(screen.getByRole("region", { name: "规则详情" })).toHaveTextContent("明显下影线");
  expect(screen.getByRole("region", { name: "规则详情" })).toHaveTextContent("最小振幅（%）2%");
  expect(screen.getByRole("region", { name: "规则详情" })).toHaveTextContent("N 形态一池");
  expect(screen.getByRole("region", { name: "上次选股结果" })).toHaveTextContent("2 只");
  expect(screen.getByRole("button", { name: "查看 N 形态二池条件" })).toHaveAttribute(
    "aria-pressed",
    "true",
  );
  expect(container.querySelector("main")?.textContent).not.toContain("n-shape-pool");
  expect(findJargon(container.textContent ?? "")).toEqual([]);
});

it("keeps published rules visible when the member source is unavailable", async () => {
  const first = base.pools[0];
  if (!first) throw new Error("pool fixture is incomplete");
  respond({
    ...base,
    state: "unavailable",
    latest_trade_date: null,
    rules_available: true,
    pools: [
      {
        ...first,
        name: "N 形态一池",
        state: "unavailable",
        trade_date: null,
        member_count: null,
        members: [],
        definition: firstRule,
      },
    ],
  });
  renderApp("/pools");
  expect(await screen.findByRole("region", { name: "规则详情" })).toHaveTextContent("排除 ST");
  expect(screen.getByRole("region", { name: "上次选股结果" })).toHaveTextContent("结果暂不可用");
  expect(screen.queryByText("样本01")).not.toBeInTheDocument();
});

it("does not invent a parent line when the parent is hidden by the list limit", async () => {
  const first = base.pools[0];
  if (!first) throw new Error("pool fixture is incomplete");
  respond({
    ...base,
    rules_available: true,
    pools_truncated: true,
    canvases: [],
    pools: [
      {
        ...first,
        key: "user/观察池",
        name: "观察池",
        definition: { ...secondRule, name: "观察池", depends_on: "user/未展示" },
      },
    ],
  });
  const user = userEvent.setup();
  const { container } = renderApp("/pools");
  const graph = await screen.findByRole("group", { name: "已发布规则与池子" });
  expect(graph.querySelectorAll(".flow-graph-node")).toHaveLength(2);
  await user.click(screen.getByRole("button", { name: "查看 观察池条件" }));
  expect(screen.getByRole("region", { name: "规则详情" })).toHaveTextContent(
    "父池未显示，列表已达上限",
  );
  expect(container.querySelector("main")?.textContent).not.toContain("user/未展示");
});
