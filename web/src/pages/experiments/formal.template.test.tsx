import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import type { Schemas } from "@/api/client";
import { metaEnvelope } from "@/test/fixtures";
import { findJargon } from "@/test/jargon";
import { renderApp } from "@/test/render";
import { metaHandler, server } from "@/test/server";
import { experimentFixture } from "./formal.fixture";
import { experimentTemplateFixture as template } from "./template.fixture";

vi.mock("@/charts/EChart", () => ({
  EChart: ({ label }: { label: string }) => <div role="img" aria-label={label} />,
}));

vi.mock("@/ui/theme", async (original) => {
  const theme = await original<typeof import("@/ui/theme")>();
  return {
    ...theme,
    antdThemeFor: (...args: Parameters<typeof theme.antdThemeFor>) => {
      const value = theme.antdThemeFor(...args);
      return { ...value, token: { ...value.token, motion: false } };
    },
  };
});

function ready() {
  server.use(
    metaHandler(metaEnvelope({ viewer: "alice", generationId: experimentFixture.generation_id })),
    http.get("*/api/v1/experiments/capabilities", () => HttpResponse.json(template.capabilities)),
    http.get("*/api/v1/experiments/mine", () => HttpResponse.json(template.mine)),
    http.get("*/api/v1/experiments/families/:family", () => HttpResponse.json(template.family)),
    http.get("*/api/v1/strategy-templates", () => HttpResponse.json(template.catalog)),
    http.get("*/api/v1/strategy-templates/sources", () => HttpResponse.json(template.sources)),
    http.get("*/api/v1/strategy-templates/:strategy/versions", () =>
      HttpResponse.json(template.versions),
    ),
    http.get("*/api/v1/strategy-templates/:strategy", ({ request }) =>
      HttpResponse.json(
        new URL(request.url).searchParams.get("version") === "1"
          ? template.detail
          : template.latest,
      ),
    ),
  );
}

it("实验选择原模板精确版本，保留完整规则且只提交有型身份", async () => {
  ready();
  const bodies: Schemas["ExperimentSearchWrite"][] = [];
  server.use(
    http.post("*/api/v1/experiments/commands", async ({ request }) => {
      const body = (await request.json()) as Schemas["ExperimentSearchWrite"];
      bodies.push(body);
      return HttpResponse.json({
        command_id: body.command_id,
        status: "registered",
        message: "实验已登记。",
        family_id: null,
        job_ids: [],
        planned_count: 4,
        version: null,
      } satisfies Schemas["ExperimentWriteReceipt"]);
    }),
  );
  renderApp("/experiments");
  await userEvent.click(await screen.findByRole("button", { name: "新建实验" }));
  await userEvent.selectOptions(screen.getByLabelText("实验执行方式"), "template");
  expect(screen.getByRole("button", { name: "开始搜索" })).toBeDisabled();
  await userEvent.selectOptions(
    await screen.findByLabelText("实验策略"),
    template.detail.data.strategy_id,
  );
  await waitFor(() => expect(screen.getByRole("region", { name: "完整策略规则" })).toBeVisible());
  expect(screen.getByText("6 个交易日")).toBeVisible();
  await userEvent.selectOptions(screen.getByLabelText("实验策略版本"), "1");
  expect(await screen.findByText("5 个交易日")).toBeVisible();
  await userEvent.type(screen.getByLabelText("实验名称"), "规则参数研究");
  await userEvent.click(screen.getByRole("button", { name: "开始搜索" }));
  await waitFor(() => expect(bodies).toHaveLength(1));
  const request = bodies[0]?.request;
  if (!request || !("base_config" in request)) {
    throw new Error("期望组合策略实验请求");
  }
  expect(request.template).toEqual({
    strategy_id: template.detail.data.strategy_id,
    head: template.detail.data.head,
  });
  expect(request.base_config.weight_rule).toEqual(template.detail.data.rules.weight_rule);
  expect(request.base_config.rebalance_rule).toEqual(template.detail.data.rules.rebalance_rule);
  expect(JSON.stringify(bodies[0])).not.toContain("owner_id");
  expect(JSON.stringify(bodies[0])).not.toContain("metadata_identity");
});

it("准备失败保留全部计划项，取消可用，不能当成已运行或显示统计", async () => {
  ready();
  // UI-only edge states reuse the original selected rules and all actual slot parameters.
  server.use(
    http.get("*/api/v1/experiments/families/:family", () =>
      HttpResponse.json({
        ...template.family,
        data: {
          ...template.family.data,
          preparations: template.family.data.preparations?.map((slot) => ({
            ...slot,
            strategy_name: template.detail.data.name,
            strategy_version: template.detail.data.head.version,
            rules: {
              ...template.detail.data.rules,
              weight_rule: slot.configuration.weight_rule,
              rebalance_rule: slot.configuration.rebalance_rule,
            },
            metrics:
              experimentFixture.family.data.items[0]?.metrics?.map((metric) => ({
                ...metric,
                value: null,
              })) ?? [],
          })),
        },
      }),
    ),
  );
  const bodies: Schemas["ExperimentCancelWrite"][] = [];
  server.use(
    http.post("*/api/v1/experiments/commands", async ({ request }) => {
      const body = (await request.json()) as Schemas["ExperimentCancelWrite"];
      bodies.push(body);
      return HttpResponse.json({
        command_id: body.command_id,
        status: "cancelled",
        message: "准备已取消。",
        family_id: body.family_id,
        job_ids: [],
        planned_count: 4,
        version: null,
      } satisfies Schemas["ExperimentWriteReceipt"]);
    }),
  );
  renderApp("/experiments");
  const pending = await screen.findByRole("table", { name: "准备中的实验" });
  expect(within(pending).getByText("计划 4 次")).toBeVisible();
  expect(within(pending).getByText("已保存 2 · 已准备 1")).toBeVisible();
  await userEvent.click(within(pending).getByRole("button", { name: "退出规则实验" }));
  const plans = await screen.findByRole("table", { name: "完整准备清单" });
  expect(within(plans).getAllByRole("row")).toHaveLength(5);
  expect(within(plans).getByText("容量不足")).toBeVisible();
  expect(
    within(plans).getAllByText(
      `${template.detail.data.name} · 第 ${template.detail.data.head.version} 版`,
    ),
  ).toHaveLength(4);
  for (const metric of experimentFixture.family.data.items[0]?.metrics ?? []) {
    expect(within(plans).getByRole("columnheader", { name: metric.label })).toBeVisible();
  }
  const first = within(plans).getAllByRole("row")[1];
  if (!first) throw new Error("complete planned preparation row is missing");
  await userEvent.click(within(first).getByText("全部指标"));
  expect(within(first).getByText("净收益").nextElementSibling?.textContent).toBe("—");
  await userEvent.click(within(first).getByText("全部参数"));
  expect(within(first).getAllByText("单股上限")).toHaveLength(2);
  for (const label of within(first).getAllByText("单股上限")) expect(label).toBeVisible();
  expect(screen.queryByRole("img", { name: "实验与基准净值" })).not.toBeInTheDocument();
  expect(screen.queryByText("过拟合检查")).not.toBeInTheDocument();
  expect(findJargon(document.body.textContent ?? "")).toEqual([]);
  await userEvent.click(screen.getByRole("button", { name: "取消未完成项" }));
  await waitFor(() => expect(bodies).toHaveLength(1));
  expect(bodies[0]?.family_id).toBe(template.family.data.family_id);
});

it("原模板版本发生混代时撤下规则并阻止提交", async () => {
  ready();
  server.use(
    http.get("*/api/v1/strategy-templates/:strategy/versions", () =>
      HttpResponse.json({}, { status: 409 }),
    ),
  );
  renderApp("/experiments");
  await userEvent.click(await screen.findByRole("button", { name: "新建实验" }));
  await userEvent.selectOptions(screen.getByLabelText("实验执行方式"), "template");
  await userEvent.selectOptions(
    await screen.findByLabelText("实验策略"),
    template.detail.data.strategy_id,
  );
  expect(await screen.findByText("该版本暂时无法核对")).toBeVisible();
  expect(screen.queryByRole("region", { name: "完整策略规则" })).not.toBeInTheDocument();
  expect(screen.getByRole("button", { name: "开始搜索" })).toBeDisabled();
});
