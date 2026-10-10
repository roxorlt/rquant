import { fireEvent, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import { vi } from "vitest";
import type { Schemas } from "@/api/client";
import { metaEnvelope } from "@/test/fixtures";
import { findJargon } from "@/test/jargon";
import { renderApp } from "@/test/render";
import { server } from "@/test/server";
import { dailyCapability, dailyResearch } from "./factorDailyFields.fixture";
import { diagnosticFactor, diagnosticResult } from "./factorDiagnostics.fixture";
import { technicalCapability, technicalResearch } from "./factorTechnicalHistory.fixture";

vi.mock("@/charts/EChart", () => ({
  EChart: ({ label }: { label: string }) => <div role="img" aria-label={label} />,
}));

function publish(
  research: Schemas["FactorStreamResearchDisplay"] = technicalResearch,
  capability: Schemas["FactorCapabilitiesData"] = technicalCapability,
  historical?: Schemas["FactorStreamResearchDisplay"],
) {
  const serving = metaEnvelope().serving;
  const current = diagnosticResult();
  const old = diagnosticResult({
    job_id: "1".repeat(32),
    factor_version: 1,
    definition_status: "historical_unavailable",
    updated_at: "2026-09-20T07:31:00Z",
  });
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
        data: {
          availability: "populated",
          available_at: serving.built_at,
          results: historical ? [current, old] : [current],
        },
        serving,
      }),
    ),
    http.get("*/api/v1/factors/results/:jobId", ({ params }) =>
      HttpResponse.json({
        data: {
          availability: "ready",
          available_at: serving.built_at,
          result: params.jobId === old.job_id ? old : current,
          research: params.jobId === old.job_id ? historical : research,
        },
        serving,
      }),
    ),
  );
}

async function openBasis() {
  const text = await screen.findByText("日线字段口径");
  const anchor = text.closest(".tip-anchor") ?? text;
  await userEvent.click(text);
  return { anchor, tip: await screen.findByRole("tooltip") };
}

it("混合结果按自身字段区分历史推导和库存，显示实际初始化边界而非当前目录", async () => {
  publish(technicalResearch, { ...dailyCapability, fields: dailyCapability.fields.slice(0, 6) });
  const { container } = renderApp("/factors");
  const { tip, anchor } = await openBasis();
  expect(tip).toHaveTextContent("5日均线（历史推导）");
  expect(tip).toHaveTextContent("换手率（库存原值）");
  expect(tip).toHaveTextContent("按观察日复权因子计算、当日因子还原价格");
  expect(tip).toHaveTextContent("从首个有效历史观察初始化");
  expect(tip).toHaveTextContent("历史断裂后不重新初始化");
  expect(tip).toHaveTextContent("历史起点 2026-07-01");
  expect(tip).toHaveTextContent("已初始化 10，未初始化 2，历史断裂 1");
  expect(tip).toHaveTextContent("0.5387表示0.5387%");
  expect(tip).not.toHaveTextContent("指标价格基准与初始化未核验");
  await userEvent.unhover(anchor);
  await userEvent.tab();
  await waitFor(() => expect(screen.queryByRole("tooltip")).toBeNull(), { timeout: 2000 });
  const text = document.createTreeWalker(container, NodeFilter.SHOW_TEXT);
  const pieces: string[] = [];
  while (text.nextNode()) pieces.push(text.currentNode.textContent ?? "");
  expect(findJargon(pieces.join(" "))).toEqual([]);
  expect(container.textContent).not.toMatch(/rquant-ta|history_derived|implementation_sha256/);
});

it.each([
  ["insufficient_window", "窗口不足"],
  ["no_initialization", "缺少初始化历史"],
  ["history_break", "历史断裂"],
  ["missing_observation", "缺少观察记录"],
  ["derived_non_finite", "推导值无效"],
] as const)("字段覆盖保留服务端%s数量，键盘可查看缺因且不补值", async (reason, label) => {
  const day = technicalResearch.daily_feature_coverage_days[0];
  if (!day) throw new Error("Captured coverage is required");
  const research: Schemas["FactorStreamResearchDisplay"] = {
    ...technicalResearch,
    daily_feature_coverage_days: [
      {
        ...day,
        counts: [
          {
            column: "ma5",
            valid: 9,
            missing: reason === "missing_observation" ? 3 : 0,
            null: reason !== "missing_observation" && reason !== "derived_non_finite" ? 3 : 0,
            non_finite: reason === "derived_non_finite" ? 3 : 0,
            reasons: [{ reason, count: 3 }],
          },
        ],
      },
    ],
  };
  publish(research);
  renderApp("/factors");
  await userEvent.click(await screen.findByText("查看字段覆盖"));
  const table = screen.getByRole("table", { name: "日线字段覆盖" });
  const count = within(table).getByText("9 / 12");
  fireEvent.focus(count.closest(".tip-anchor") ?? count);
  expect(await screen.findByRole("tooltip")).toHaveTextContent(`${label}：3`);
  expect(table).toHaveTextContent("2026-08-31");
  expect(table).not.toHaveTextContent("12 / 12");
});

it("基本事实单独结果沿库存原值，不套未消费技术来源的重算或未核验声明", async () => {
  const source = dailyResearch.daily_features;
  if (!source) throw new Error("Stored fixture source is required");
  publish({
    ...dailyResearch,
    daily_features: {
      ...source,
      fields: source.fields.filter((field) => field.table === "daily_basic"),
    },
  });
  renderApp("/factors");
  const { tip } = await openBasis();
  expect(tip).toHaveTextContent("已存日线原值，未重新计算");
  expect(tip).toHaveTextContent("换手率（库存原值）");
  expect(tip).not.toHaveTextContent("历史推导");
  expect(tip).not.toHaveTextContent("初始化未核验");
});

it("切换历史库存结果与换代重载仍读取各结果自身来源", async () => {
  publish(technicalResearch, technicalCapability, dailyResearch);
  const app = renderApp("/factors");
  let basis = await openBasis();
  expect(basis.tip).toHaveTextContent("5日均线（历史推导）");
  fireEvent.blur(basis.anchor);
  await userEvent.click(
    within(screen.getByRole("table", { name: "最近检验" })).getByRole("cell", { name: "第 1 版" }),
  );
  basis = await openBasis();
  expect(basis.tip).toHaveTextContent("已存20日均线，价格基准未核验。");
  expect(basis.tip).toHaveTextContent("指标价格基准与初始化未核验");
  expect(basis.tip).not.toHaveTextContent("历史推导");
  app.unmount();
  publish(technicalResearch, dailyCapability, dailyResearch);
  renderApp("/factors");
  basis = await openBasis();
  expect(basis.tip).toHaveTextContent("5日均线（历史推导）");
  expect(basis.tip).not.toHaveTextContent("已存20日均线");
});

it("中文字段说明取当前可信API，技术和基本事实单位不由前端改写", async () => {
  publish();
  renderApp("/factors");
  const create = await screen.findByRole("button", { name: "新建因子" });
  await waitFor(() => expect(create).toBeEnabled());
  await userEvent.click(create);
  const dialog = screen.getByRole("dialog", { name: "新建因子" });
  await userEvent.type(within(dialog).getByRole("searchbox", { name: "搜索日线字段" }), "均线");
  const info = within(dialog).getByRole("button", { name: "5日均线说明" });
  fireEvent.focus(info);
  expect(await screen.findByRole("tooltip")).toHaveTextContent(
    "从首个有效历史观察推导，按观察日复权因子计算、当日因子还原价格。",
  );
  expect(within(dialog).getByRole("textbox", { name: "表达式" })).toHaveValue("");
});
