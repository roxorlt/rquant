import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import { useState } from "react";
import { vi } from "vitest";
import type { Schemas } from "@/api/client";
import { metaEnvelope } from "@/test/fixtures";
import { findJargon } from "@/test/jargon";
import { renderApp } from "@/test/render";
import { server } from "@/test/server";
import { FactorDailyFeatures } from "./FactorDailyFeatures";
import { FactorEditor, type FactorEditorDraft } from "./FactorEditor";
import { dailyResearch } from "./factorDailyFields.fixture";
import {
  marketTemperatureCapability,
  marketTemperatureFactor,
  marketTemperatureResearch,
  mixedMarketTemperatureCapability,
  mixedMarketTemperatureResearch,
  nullMarketTemperatureResearch,
} from "./factorMarketTemperature.fixture";
import { trackingKey, trackingPanel, trackingResult } from "./factorTracking.fixture";

vi.mock("@/charts/EChart", () => ({
  EChart: ({ label }: { label: string }) => <div role="img" aria-label={label} />,
}));

const originalMedia = window.matchMedia;
beforeEach(() => {
  window.matchMedia = (query) => ({ ...originalMedia(query), matches: false });
  localStorage.clear();
  Object.defineProperty(navigator, "locks", {
    configurable: true,
    value: {
      request: async (
        _name: string,
        _options: unknown,
        action: (lock: { name: string }) => unknown,
      ) => action({ name: "rquant.factor.command" }),
    },
  });
});
afterEach(() => {
  window.matchMedia = originalMedia;
  Object.defineProperty(navigator, "locks", { configurable: true, value: undefined });
});

async function focusTip(label: string, text: string) {
  const anchor = screen.getByText(label, { exact: true }).closest<HTMLElement>(".tip-anchor");
  if (!anchor) throw new Error("Tip anchor required");
  act(() => anchor.focus());
  const tip = await waitFor(() => {
    const target = screen.getAllByRole("tooltip").find((item) => item.textContent?.includes(text));
    expect(target).toBeDefined();
    if (!target) throw new Error("Target tooltip required");
    return target;
  });
  return { anchor, tip };
}

it("市场温度独立展示实际日值，前一完整开市日和回顾来源放提示", async () => {
  const { container } = render(<FactorDailyFeatures research={marketTemperatureResearch} />);
  expect(screen.getByRole("heading", { name: "市场温度" })).toBeInTheDocument();
  expect(screen.queryByText("日线字段口径")).toBeNull();
  expect(screen.queryByText("分钟字段口径")).toBeNull();
  expect(container.textContent).not.toMatch(/SSE|09:25|历史回顾|market_temperature|12 \/ 12/);
  expect(findJargon(container.textContent ?? "")).toEqual([]);
  const source = await focusTip("市场温度口径", "全市场日值");
  expect(source.tip).toHaveTextContent("全市场日值");
  expect(source.tip).toHaveTextContent("不按所选股票池重算");
  expect(source.tip).toHaveTextContent("前一完整SSE开市日");
  expect(source.tip).toHaveTextContent("09:25");
  expect(source.tip).toHaveTextContent("仅用于历史回顾，不代表当时已知");
  act(() => source.anchor.blur());
  await userEvent.click(screen.getByText("查看市场温度"));
  const table = screen.getByRole("table", { name: "市场温度日值" });
  expect(table).toHaveTextContent("60日新高占比");
  expect(table).toHaveTextContent("20日均线上方占比");
  expect(table).toHaveTextContent("20.00%");
  expect(table).toHaveTextContent("98.00%");
  expect(table).not.toHaveTextContent("12 / 12");
  const value = await focusTip("20.00%", "原值：20%");
  expect(value.tip).toHaveTextContent("字段日期：2026-07-03");
  expect(value.tip).toHaveTextContent("原值：20%");
});

it("0和100为合法原百分比，不能乘100或填成缺值", async () => {
  const day = marketTemperatureResearch.daily_feature_coverage_days?.[0];
  if (!day) throw new Error("Captured market day required");
  render(
    <FactorDailyFeatures
      research={{
        ...marketTemperatureResearch,
        daily_feature_coverage_days: [
          {
            ...day,
            market_temperature_values: [
              { column: "market_high_60d_ratio_pct", status: "valid", value: 0 },
              { column: "market_above_ma20_ratio_pct", status: "valid", value: 100 },
            ],
          },
        ],
      }}
    />,
  );
  await userEvent.click(screen.getByText("查看市场温度"));
  const table = screen.getByRole("table", { name: "市场温度日值" });
  expect(table).toHaveTextContent("0.00%");
  expect(table).toHaveTextContent("100.00%");
  expect(table).not.toHaveTextContent("10,000");
  expect(within(table).queryByText("—")).toBeNull();
});

it.each([
  ["missing", "missing_market_temperature", "缺少市场温度记录"],
  ["null", "market_temperature_null", "市场温度为空"],
  ["non_finite", "market_temperature_non_finite", "市场温度数值无效"],
  ["null", "invalid_market_percentage", "市场温度超出0–100%"],
] as const)("市场%s保留—和%s原因，不借单股计数补值", async (status, reason, label) => {
  const day = marketTemperatureResearch.daily_feature_coverage_days?.[0];
  if (!day) throw new Error("Captured market day required");
  render(
    <FactorDailyFeatures
      research={{
        ...marketTemperatureResearch,
        daily_feature_coverage_days: [
          {
            ...day,
            market_temperature_values: [
              {
                column: "market_high_60d_ratio_pct",
                status,
                value: null,
                reason,
                ...(status === "non_finite" ? { non_finite_value: "NaN" as const } : {}),
              },
            ],
          },
        ],
      }}
    />,
  );
  await userEvent.click(screen.getByText("查看市场温度"));
  const table = screen.getByRole("table", { name: "市场温度日值" });
  const value = within(table).getAllByText("—").at(-1);
  if (!value) throw new Error("Missing value required");
  fireEvent.focus(value.closest(".tip-anchor") ?? value);
  expect(await screen.findByRole("tooltip")).toHaveTextContent(label);
  expect(table).not.toHaveTextContent("0.00%");
});

it("实际空值结果和未提供日值分别显示，切换旧来源后不残留温度", async () => {
  const app = render(<FactorDailyFeatures research={nullMarketTemperatureResearch} />);
  await userEvent.click(screen.getByText("查看市场温度"));
  const table = screen.getByRole("table", { name: "市场温度日值" });
  const missing = within(table).getByText("—");
  fireEvent.focus(missing.closest(".tip-anchor") ?? missing);
  expect(await screen.findByRole("tooltip")).toHaveTextContent("市场温度为空");
  fireEvent.blur(missing.closest(".tip-anchor") ?? missing);
  const day = marketTemperatureResearch.daily_feature_coverage_days?.[0];
  if (!day) throw new Error("Captured market day required");
  app.rerender(
    <FactorDailyFeatures
      research={{
        ...marketTemperatureResearch,
        daily_feature_coverage_days: [{ ...day, market_temperature_values: null }],
      }}
    />,
  );
  expect(within(table).getAllByText("—")).toHaveLength(2);
  app.rerender(<FactorDailyFeatures research={dailyResearch} />);
  expect(screen.queryByText("市场温度口径")).toBeNull();
  expect(screen.queryByText("查看市场温度")).toBeNull();
  expect(screen.getByText("日线字段口径")).toBeInTheDocument();
});

it("混合来源中市场日值独立于50项单股和分钟覆盖", async () => {
  render(<FactorDailyFeatures research={mixedMarketTemperatureResearch} />);
  await userEvent.click(screen.getByText("查看字段覆盖"));
  const field = screen.getByRole("combobox", { name: "覆盖字段" });
  expect(within(field).getAllByRole("option")).toHaveLength(50);
  expect(within(field).queryByRole("option", { name: "60日新高占比" })).toBeNull();
  const source = await focusTip("日线字段口径", "历史推导：12项");
  expect(source.tip).not.toHaveTextContent("市场温度");
  expect(source.tip).not.toHaveTextContent("60日新高占比");
  act(() => source.anchor.blur());
  const minute = await focusTip("分钟字段口径", "前一交易日精确15:00");
  expect(minute.tip).not.toHaveTextContent("市场温度");
  act(() => minute.anchor.blur());
  await userEvent.click(screen.getByText("查看市场温度"));
  expect(screen.getByRole("table", { name: "市场温度日值" })).toHaveTextContent("10.00%");
});

function Editor({ mixed }: { mixed: boolean }) {
  const [draft, setDraft] = useState<FactorEditorDraft>({
    generation_id: "a".repeat(64),
    mode: "create",
    factor_id: null,
    expected_head: null,
    name_zh: "市场温度",
    category: "technical",
    category_label: "技术",
    direction: "higher_is_better",
    expression: "close * ",
  });
  return (
    <FactorEditor
      draft={draft}
      open
      capabilities={mixed ? mixedMarketTemperatureCapability : marketTemperatureCapability}
      catalog={undefined}
      currentDefinition={null}
      currentGeneration={draft.generation_id}
      canSave
      storageReady
      stale={false}
      canRebase={false}
      busy={false}
      onChange={setDraft}
      onClose={() => {}}
      onRebase={() => {}}
      onSubmit={() => {}}
    />
  );
}

it.each([false, true])(
  "可信市场字段可按中文搜索并插入，独立分组/百分比说明正确 (%s)",
  async (mixed) => {
    const { container } = render(<Editor mixed={mixed} />);
    const dialog = screen.getByRole("dialog", { name: "新建因子" });
    const search = within(dialog).getByRole("searchbox", { name: "搜索字段" });
    await userEvent.type(search, "新高");
    const group = within(dialog).getByRole("group", { name: "市场温度" });
    expect(within(group).getByRole("button", { name: "插入60日新高占比" })).toBeEnabled();
    expect(findJargon(container.textContent ?? "")).toEqual([]);
    expect(container.textContent).not.toMatch(/market_temperature_stored|09:25|SSE/);
    const info = within(group).getByRole("button", { name: "60日新高占比说明" });
    fireEvent.focus(info);
    expect(await screen.findByRole("tooltip")).toHaveTextContent("百分比原值");
    expect(screen.getByRole("tooltip")).toHaveTextContent("单位：百分比（%）");
    fireEvent.blur(info);
    const expression = within(dialog).getByRole("textbox", {
      name: "表达式",
    }) as HTMLTextAreaElement;
    expression.setSelectionRange(expression.value.length, expression.value.length);
    await userEvent.click(within(group).getByRole("button", { name: "插入60日新高占比" }));
    expect(expression).toHaveValue("close * market_high_60d_ratio_pct");
    await userEvent.clear(search);
    await userEvent.type(search, "均线上方");
    expect(within(dialog).getByRole("button", { name: "插入20日均线上方占比" })).toBeEnabled();
  },
);

it("手机点按打开市场温度说明", async () => {
  window.matchMedia = (query) => ({ ...originalMedia(query), matches: query === "(hover: none)" });
  render(<FactorDailyFeatures research={marketTemperatureResearch} />);
  await userEvent.click(screen.getByText("市场温度口径"));
  expect(await screen.findByRole("tooltip")).toHaveTextContent("不按所选股票池重算");
});

it("市场能力可加入跟踪，能力收缩后取消保留原定义请求", async () => {
  const metadata = metaEnvelope();
  let panel = trackingPanel({ factor_id: marketTemperatureFactor.factor_id });
  const seen: Schemas["FactorTrackingRequest"][] = [];
  server.use(
    http.get("*/api/v1/meta", () => HttpResponse.json(metadata)),
    http.get("*/api/v1/factors/definitions", () =>
      HttpResponse.json({
        data: {
          availability: "populated",
          available_at: metadata.serving.built_at,
          can_save: true,
          can_archive: true,
          definitions: [marketTemperatureFactor],
        },
        serving: metadata.serving,
      }),
    ),
    http.get("*/api/v1/factors/capabilities", () =>
      HttpResponse.json({
        data: marketTemperatureCapability,
        serving: metadata.serving,
      }),
    ),
    http.get("*/api/v1/factors/:factorId/tracking", () =>
      HttpResponse.json({ data: panel, serving: metadata.serving }),
    ),
    http.post("*/api/v1/factors/tracking/commands", async ({ request }) => {
      const body = (await request.json()) as Schemas["FactorTrackingRequest"];
      seen.push(body);
      if (body.tracked)
        panel = trackingPanel({
          factor_id: body.factor_id,
          availability: "tracked",
          status: "active",
          tracked: true,
          tracking_generation: "b".repeat(32),
          definition_head: body.expected_head,
        });
      return HttpResponse.json({ data: trackingResult(body), serving: metadata.serving });
    }),
  );
  const app = renderApp("/factors");
  await screen.findByRole("button", { name: "加入跟踪" });
  const dependencies = (await screen.findByText("使用字段", { selector: "dt" })).nextElementSibling;
  expect(dependencies).toHaveTextContent("2 项");
  expect(dependencies).not.toHaveTextContent("market_high_60d_ratio_pct");
  await waitFor(() =>
    expect(
      app.queryClient.getQueryData([
        "factors",
        "capabilities",
        metadata.serving.generation_id,
        "tester",
        0,
      ]),
    ).toMatchObject({
      data: { version: "daily_market_temperature_v1" },
      serving: metadata.serving,
    }),
  );
  await waitFor(() => expect(screen.getByRole("button", { name: "加入跟踪" })).toBeEnabled());
  await userEvent.click(screen.getByRole("button", { name: "加入跟踪" }));
  const confirmation = await screen.findByRole("dialog", { name: "加入因子跟踪" });
  await userEvent.click(within(confirmation).getByRole("button", { name: "确认加入" }));
  await waitFor(() => expect(seen).toHaveLength(1));
  expect(seen[0]?.tracked).toBe(true);
  expect(seen[0]?.expected_head).toEqual({
    version: marketTemperatureFactor.version,
    content_sha256: marketTemperatureFactor.content_sha256,
  });
  await userEvent.click(await screen.findByRole("button", { name: "继续查看跟踪" }));
  act(() =>
    app.queryClient.setQueryData(
      ["factors", "capabilities", metadata.serving.generation_id, "tester", 0],
      {
        data: {
          ...marketTemperatureCapability,
          fields: marketTemperatureCapability.fields.slice(0, 6),
        },
        serving: metadata.serving,
      },
    ),
  );
  await screen.findByRole("button", { name: "取消跟踪" });
  expect(dependencies).toHaveTextContent("2 项");
  expect(dependencies).not.toHaveTextContent("market_high_60d_ratio_pct");
  await waitFor(() => expect(screen.getByRole("button", { name: "取消跟踪" })).toBeEnabled());
  await userEvent.click(screen.getByRole("button", { name: "取消跟踪" }));
  await waitFor(() => expect(seen).toHaveLength(2));
  expect(seen[1]?.tracked).toBe(false);
  expect(seen[1]?.expected_head).toEqual(seen[0]?.expected_head);
  expect(JSON.parse(localStorage.getItem(trackingKey) ?? "null").request).toEqual(seen[1]);
});
