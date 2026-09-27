import { act, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import type { Schemas } from "@/api/client";
import { metaEnvelope } from "@/test/fixtures";
import { findJargon } from "@/test/jargon";
import { renderApp } from "@/test/render";
import { server } from "@/test/server";

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
      steps: [{ step_index: 0, label: "最终命中", count: 121 }],
      steps_truncated: false,
      members: [{ code: "600001.SH", name: "样本01", close: 11, pct_chg: 1.2 }],
      members_truncated: true,
    },
    {
      key: "deleted-pool",
      name: "选股池",
      state: "unpublished",
      trade_date: null,
      member_count: null,
      steps: [],
      steps_truncated: false,
      members: [],
      members_truncated: false,
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
  expect(container.querySelectorAll(".react-flow__edge")).toHaveLength(0);
  expect(findJargon(container.textContent ?? "")).toEqual([]);
  await user.click(screen.getByRole("row", { name: /样本01/ }));
  expect(await screen.findByRole("dialog")).toHaveTextContent("样本01");
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
  server.use(
    http.get("*/api/v1/pools", () =>
      HttpResponse.json({
        data: base,
        serving: { ...serving, generation_id: "another-generation" },
      }),
    ),
  );
  renderApp("/pools");
  expect(await screen.findByText("池子数据正在更新")).toBeInTheDocument();
  expect(screen.queryByText("样本01")).not.toBeInTheDocument();
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
  expect(graph.querySelectorAll(".react-flow__node")).toHaveLength(4);
  const condition = graph.querySelector<HTMLElement>('[data-id="condition:n-shape-pool2"]');
  expect(condition).toBeTruthy();
  act(() => condition?.focus());
  await user.keyboard("{Enter}");
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
  expect(graph.querySelectorAll(".react-flow__node")).toHaveLength(2);
  await user.click(screen.getByRole("button", { name: "查看 观察池条件" }));
  expect(screen.getByRole("region", { name: "规则详情" })).toHaveTextContent(
    "父池未显示，列表已达上限",
  );
  expect(container.querySelector("main")?.textContent).not.toContain("user/未展示");
});
