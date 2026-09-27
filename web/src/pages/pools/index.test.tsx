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
      state: "missing",
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

it("explains missing references, older results, and empty data", async () => {
  const current = base.pools[0];
  const missing = base.pools[1];
  if (!current || !missing) throw new Error("pool fixture is incomplete");
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
      missing,
    ],
  });
  const user = userEvent.setup();
  renderApp("/pools");
  expect((await screen.findAllByText(/不是最新交易日/)).length).toBeGreaterThan(0);
  await user.click(screen.getByRole("button", { name: /选股池/ }));
  expect(screen.getAllByText(/引用的池子已失效/).length).toBeGreaterThan(0);
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
