import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import { vi } from "vitest";
import { META_QUERY_KEY } from "@/api/useMeta";
import { metaEnvelope } from "@/test/fixtures";
import { findJargon } from "@/test/jargon";
import { renderApp } from "@/test/render";
import { server } from "@/test/server";
import { FactorDailyFeatures } from "./FactorDailyFeatures";
import { dailyCapability, dailyResearch } from "./factorDailyFields.fixture";
import { diagnosticFactor, diagnosticResult } from "./factorDiagnostics.fixture";
import {
  mixedStockCapability,
  mixedStockResearch,
  stockCapability,
  stockResearch,
} from "./factorStockFeature.fixture";
import { technicalResearch } from "./factorTechnicalHistory.fixture";

vi.mock("@/charts/EChart", () => ({
  EChart: ({ label }: { label: string }) => <div role="img" aria-label={label} />,
}));

const originalMedia = window.matchMedia;
beforeEach(() => {
  window.matchMedia = (query) => ({ ...originalMedia(query), matches: false });
});
afterEach(() => {
  window.matchMedia = originalMedia;
});

async function basis() {
  const anchor = screen.getByText("日线字段口径").closest(".tip-anchor");
  if (!anchor) throw new Error("Source explanation anchor is required");
  fireEvent.focus(anchor);
  return { anchor, tip: await screen.findByRole("tooltip") };
}

function publish(
  capability = mixedStockCapability,
  research = mixedStockResearch,
  generationId?: string,
) {
  const serving = metaEnvelope({ generationId }).serving;
  const current = diagnosticResult();
  server.use(
    http.get("*/api/v1/factors/definitions", () =>
      HttpResponse.json({
        data: {
          availability: "populated",
          can_save: true,
          can_archive: true,
          available_at: serving.built_at,
          definitions: [diagnosticFactor],
        },
        serving,
      }),
    ),
    http.get("*/api/v1/factors/capabilities", () =>
      HttpResponse.json({ data: capability, serving }),
    ),
    http.get("*/api/v1/factors/results", () =>
      HttpResponse.json({
        data: { availability: "populated", available_at: serving.built_at, results: [current] },
        serving,
      }),
    ),
    http.get("*/api/v1/factors/results/:jobId", () =>
      HttpResponse.json({
        data: { availability: "ready", available_at: serving.built_at, result: current, research },
        serving,
      }),
    ),
  );
}

it("独立选股结果显示自己的观察窗口，说明不套技术指标初始化", async () => {
  render(<FactorDailyFeatures research={stockResearch} />);
  const { tip } = await basis();
  const summary = stockResearch.daily_features?.stock_features;
  expect(tip).toHaveTextContent("选股派生");
  expect(tip).toHaveTextContent("90 / 120 / 250 个实际观察");
  expect(tip).toHaveTextContent("价格窗口含参考日");
  expect(tip).toHaveTextContent("吸筹窗口取参考日前 20 个观察");
  expect(tip).toHaveTextContent("均线排列需 60 个观察，收盘百分位需 250 个观察");
  expect(tip).toHaveTextContent(`历史起点 ${summary?.source_history_start}`);
  expect(tip).not.toHaveTextContent("库存原值");
  expect(tip).not.toHaveTextContent("初始化");
});

it("混合选股结果独立显示技术初始化、选股窗口和库存原值", async () => {
  render(<FactorDailyFeatures research={mixedStockResearch} />);
  const { tip } = await basis();
  expect(tip).toHaveTextContent("选股派生");
  expect(tip).toHaveTextContent("历史推导：5日均线");
  expect(tip).toHaveTextContent("库存原值：换手率");
  expect(tip).toHaveTextContent("从首个有效历史观察初始化");
  expect(tip).toHaveTextContent("历史断裂后不重新初始化");
  expect(tip).not.toHaveTextContent("指标价格基准与初始化未核验");
});

it.each([
  ["missing_reference_factor", "缺少参考日复权因子"],
  ["non_finite_reference_factor", "参考日复权因子无效"],
  ["non_positive_reference_factor", "参考日复权因子非正数"],
  ["missing_required_factor", "缺少窗口复权因子"],
  ["non_finite_required_factor", "窗口复权因子无效"],
  ["non_positive_required_factor", "窗口复权因子非正数"],
  ["insufficient_history", "观察数不足"],
  ["missing_daily_data", "缺少日线记录"],
  ["undefined_statistic", "统计量无值"],
] as const)("选股覆盖的 %s 原因可聚焦查看，服务端计数不重算", async (reason, label) => {
  const day = stockResearch.daily_feature_coverage_days?.[0];
  if (!day) throw new Error("Stock coverage day is required");
  render(
    <FactorDailyFeatures
      research={{
        ...stockResearch,
        daily_feature_coverage_days: [
          {
            ...day,
            counts: [
              {
                column: "price_position_90d_pct",
                valid: 7,
                missing: 0,
                null: 5,
                non_finite: 0,
                stock_reasons: [{ reason, count: 5 }],
              },
            ],
          },
        ],
      }}
    />,
  );
  await userEvent.click(screen.getByText("查看字段覆盖"));
  await userEvent.selectOptions(
    screen.getByRole("combobox", { name: "覆盖字段" }),
    "price_position_90d_pct",
  );
  const table = screen.getByRole("table", { name: "日线字段覆盖" });
  const count = within(table).getByText("7 / 12");
  fireEvent.focus(count.closest(".tip-anchor") ?? count);
  expect(await screen.findByRole("tooltip")).toHaveTextContent(`${label}：5`);
  expect(table).not.toHaveTextContent("12 / 12");
});

it("有效观察计数保留原覆盖，窗口不可用解释与有效值分别展示", async () => {
  render(<FactorDailyFeatures research={stockResearch} />);
  await userEvent.click(screen.getByText("查看字段覆盖"));
  await userEvent.selectOptions(
    screen.getByRole("combobox", { name: "覆盖字段" }),
    "price_window_days_90d",
  );
  const table = screen.getByRole("table", { name: "日线字段覆盖" });
  expect(within(table).getAllByText("11 / 12")).toHaveLength(3);
  const explain = screen.getByText("覆盖说明");
  fireEvent.focus(explain.closest(".tip-anchor") ?? explain);
  const tip = await screen.findByRole("tooltip");
  expect(tip).toHaveTextContent("实际观察数");
  expect(tip).toHaveTextContent("计数有效不代表窗口可用");
});

it("缺覆盖字段用横杠，刷新和切换来源不借当前能力填值", async () => {
  const app = render(<FactorDailyFeatures research={mixedStockResearch} />);
  await userEvent.click(screen.getByText("查看字段覆盖"));
  await userEvent.selectOptions(screen.getByRole("combobox", { name: "覆盖字段" }), "ma5");
  app.rerender(<FactorDailyFeatures research={stockResearch} />);
  expect(screen.getByRole("combobox", { name: "覆盖字段" })).not.toHaveValue("ma5");
  const day = stockResearch.daily_feature_coverage_days?.[0];
  if (!day) throw new Error("Coverage day is required");
  app.rerender(
    <FactorDailyFeatures
      research={{ ...stockResearch, daily_feature_coverage_days: [{ ...day, counts: [] }] }}
    />,
  );
  expect(
    within(screen.getByRole("table", { name: "日线字段覆盖" })).getByText("— / 12"),
  ).toBeInTheDocument();
  app.rerender(
    <FactorDailyFeatures research={{ ...stockResearch, daily_feature_coverage_days: null }} />,
  );
  expect(screen.getByText("当前结果未提供字段覆盖记录。")).toBeInTheDocument();
  app.rerender(<FactorDailyFeatures research={dailyResearch} />);
  const stored = await basis();
  expect(stored.tip).toHaveTextContent("已存日线原值");
  expect(stored.tip).not.toHaveTextContent("选股派生");
  fireEvent.blur(stored.anchor);
  app.rerender(<FactorDailyFeatures research={technicalResearch} />);
  const technical = await basis();
  expect(technical.tip).toHaveTextContent("5日均线（历史推导）");
  expect(technical.tip).not.toHaveTextContent("选股派生");
  fireEvent.blur(technical.anchor);
  app.rerender(<FactorDailyFeatures research={null} />);
  expect(screen.queryByRole("region", { name: "日线字段来源" })).toBeNull();
});

it.each([stockCapability, mixedStockCapability])(
  "真实目录展示全部字段，观察数与0/1说明可插入",
  async (capability) => {
    publish(capability);
    renderApp("/factors");
    const create = await screen.findByRole("button", { name: "新建因子" });
    await waitFor(() => expect(create).toBeEnabled());
    await userEvent.click(create);
    const dialog = screen.getByRole("dialog", { name: "新建因子" });
    await userEvent.click(within(dialog).getByRole("button", { name: "全部字段" }));
    expect(within(dialog).getAllByRole("button", { name: /^插入/ })).toHaveLength(
      capability.fields.length,
    );
    const search = within(dialog).getByRole("searchbox", { name: "搜索日线字段" });
    const expression = within(dialog).getByRole("textbox", { name: "表达式" });
    await userEvent.type(search, "90日实际观察数");
    fireEvent.focus(within(dialog).getByRole("button", { name: "90日实际观察数说明" }));
    expect(await screen.findByRole("tooltip")).toHaveTextContent("最近最多90个日线观察");
    expect(screen.getByRole("tooltip")).toHaveTextContent("单位：观察数");
    await userEvent.click(within(dialog).getByRole("button", { name: "插入90日实际观察数" }));
    expect(expression).toHaveValue("price_window_days_90d");
    await userEvent.clear(search);
    await userEvent.type(search, "均线多头排列");
    fireEvent.focus(within(dialog).getByRole("button", { name: "均线多头排列说明" }));
    expect(await screen.findByRole("tooltip")).toHaveTextContent("时为1，否则为0");
    expect(screen.getByRole("tooltip")).toHaveTextContent("单位：0 / 1");
    await userEvent.clear(search);
    await userEvent.type(search, "250日收盘百分位");
    fireEvent.focus(within(dialog).getByRole("button", { name: "250日收盘百分位说明" }));
    expect(await screen.findByRole("tooltip")).toHaveTextContent("取值0–1");
    expect(expression).toHaveValue("price_window_days_90d");
  },
);

it("来源缺失或能力错配只保留表达式，历史结果仍按自己的来源显示", async () => {
  publish();
  const app = renderApp("/factors");
  const create = await screen.findByRole("button", { name: "新建因子" });
  await waitFor(() => expect(create).toBeEnabled());
  await userEvent.click(create);
  const dialog = screen.getByRole("dialog", { name: "新建因子" });
  const expression = within(dialog).getByRole("textbox", { name: "表达式" });
  await userEvent.type(expression, "ma_alignment + price_percentile_250d");
  const generation = metaEnvelope().serving.generation_id;
  act(() =>
    app.queryClient.setQueryData(["factors", "capabilities", generation, "tester", 0], {
      data: { ...dailyCapability, fields: dailyCapability.fields.slice(0, 6) },
      serving: metaEnvelope({ generationId: "d".repeat(64) }).serving,
    }),
  );
  await waitFor(() => expect(within(dialog).queryByRole("button", { name: /^插入/ })).toBeNull());
  expect(expression).toHaveValue("ma_alignment + price_percentile_250d");
  publish(
    { ...dailyCapability, fields: dailyCapability.fields.slice(0, 6) },
    mixedStockResearch,
    "e".repeat(64),
  );
  act(() =>
    app.queryClient.setQueryData(META_QUERY_KEY, metaEnvelope({ generationId: "e".repeat(64) })),
  );
  const close = within(dialog)
    .getAllByRole("button", { name: /^关闭$/ })
    .at(-1);
  if (!close) throw new Error("Editor close button is required");
  await userEvent.click(close);
  await screen.findByText("日线字段口径");
  const actual = await basis();
  expect(actual.tip).toHaveTextContent("选股派生");
  fireEvent.blur(actual.anchor);
  const walker = document.createTreeWalker(app.container, NodeFilter.SHOW_TEXT);
  const pieces: string[] = [];
  while (walker.nextNode()) pieces.push(walker.currentNode.textContent ?? "");
  expect(findJargon(pieces.join(" "))).toEqual([]);
  expect(app.container.textContent).not.toMatch(
    /stock_features_derived|algorithm_version|implementation_sha256/,
  );
});
