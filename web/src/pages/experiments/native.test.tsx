import { act, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import type { Schemas } from "@/api/client";
import { metaEnvelope } from "@/test/fixtures";
import { findJargon } from "@/test/jargon";
import { renderApp } from "@/test/render";
import { metaHandler, server } from "@/test/server";
import { nativeExperimentFixture as fixture } from "./native.fixture";

const drawerMotion = vi.hoisted((): { holdOpen: boolean; completeOpen: (() => void) | null } => ({
  holdOpen: false,
  completeOpen: null,
}));

vi.mock("@/ui/Drawer", async (original) => {
  const drawers = await original<typeof import("@/ui/Drawer")>();
  return {
    ...drawers,
    SideDrawer: (props: Parameters<typeof drawers.SideDrawer>[0]) => (
      <drawers.SideDrawer
        {...props}
        afterOpenChange={(open) => {
          if (open && drawerMotion.holdOpen) {
            drawerMotion.completeOpen = () => props.afterOpenChange?.(true);
          } else props.afterOpenChange?.(open);
        }}
      />
    ),
  };
});

afterEach(() => {
  drawerMotion.holdOpen = false;
  drawerMotion.completeOpen = null;
});

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

function ready(
  native: Schemas["ExperimentNativeResultIdentity"] | null = fixture.result.data.native,
) {
  server.use(
    metaHandler(metaEnvelope({ viewer: "alice", generationId: fixture.generation_id })),
    http.get("*/api/v1/experiments/capabilities", () => HttpResponse.json(fixture.capabilities)),
    http.get("*/api/v1/experiments/mine", () => HttpResponse.json(fixture.mine)),
    http.get("*/api/v1/experiments/families/:family", () => HttpResponse.json(fixture.family)),
    http.get("*/api/v1/experiments/results/:experiment", () =>
      HttpResponse.json({ ...fixture.result, data: { ...fixture.result.data, native } }),
    ),
    http.get("*/api/v1/experiments/results/:experiment/statistics", () =>
      HttpResponse.json({}, { status: 503 }),
    ),
    http.get("*/api/v1/experiments/families/:family/heatmap", () =>
      HttpResponse.json({}, { status: 503 }),
    ),
  );
  renderApp("/experiments");
  return userEvent.setup();
}

describe("分钟策略实验配置", () => {
  it("NATIVE-EXP-01 原生配置展示实际策略和区间，缺参数明细不造组合权重", async () => {
    const user = ready();
    const mine = await screen.findByRole("table", { name: "我的实验" });
    await user.click(within(mine).getByText("全部参数"));
    const configuration = within(mine).getByRole("region", { name: "分钟策略配置" });
    expect(within(configuration).getByText("N 形突破 · 第 1 版")).toBeVisible();
    expect(within(configuration).getByText("2026-01-01 — 2026-01-02")).toBeVisible();
    expect(within(configuration).getByText("参数明细").nextElementSibling?.textContent).toBe("—");
    expect(within(configuration).queryByText("最多持仓")).not.toBeInTheDocument();
    expect(within(configuration).queryByText("调仓")).not.toBeInTheDocument();
    expect(document.body.textContent).not.toContain("n_shape");
    expect(document.body.textContent).not.toContain(fixture.configuration.selection.profile_hash);
    expect(findJargon(document.body.textContent ?? "")).toEqual([]);
  });

  it.each([
    ["captured", "原始记录"],
    ["reconstructed", "历史重建"],
  ] as const)("NATIVE-EXP-02 完整结果保留%s来源和原费用", async (kind, label) => {
    const user = ready({ ...fixture.result.data.native, source_kind: kind });
    const mine = await screen.findByRole("table", { name: "我的实验" });
    await user.click(within(mine).getByRole("button", { name: "分钟策略实验 · 1" }));
    await screen.findByRole("img", { name: "实验与基准净值" });
    const configurations = screen.getAllByRole("region", { name: "分钟策略配置" });
    const complete = configurations.at(-1);
    if (!complete) throw new Error("complete native configuration is missing");
    expect(within(complete).getByText(label)).toBeVisible();
    const fees = within(complete).getByRole("button", { name: "查看分钟策略费用" });
    expect(fees).toBeVisible();
    expect(within(complete).queryByText("现金保留")).not.toBeInTheDocument();
    const originalCostId =
      fixture.capabilities.data.default_config?.execution_cost_spec.cost_spec_id;
    if (!originalCostId) throw new Error("original cost identity is missing");
    act(() => fees.focus());
    await waitFor(() =>
      expect(
        screen.getAllByRole("tooltip").some((tip) => tip.textContent?.includes(originalCostId)),
      ).toBe(true),
    );
  });

  it("NATIVE-EXP-03 只有原配置时费用和采集说明保持缺失", async () => {
    const user = ready(null);
    const mine = await screen.findByRole("table", { name: "我的实验" });
    await user.click(within(mine).getByRole("button", { name: "分钟策略实验 · 1" }));
    await screen.findByRole("img", { name: "实验与基准净值" });
    const complete = screen.getAllByRole("region", { name: "分钟策略配置" }).at(-1);
    if (!complete) throw new Error("complete native configuration is missing");
    expect(within(complete).getByText("费用").nextElementSibling?.textContent).toBe("—");
    expect(within(complete).getByText("采集方式").nextElementSibling?.textContent).toBe("—");
  });

  it("NATIVE-EXP-04 键盘聚焦可读原来源，内部标识只在Tip", async () => {
    const user = ready();
    const mine = await screen.findByRole("table", { name: "我的实验" });
    await user.click(within(mine).getByText("全部参数"));
    const configuration = within(mine).getByRole("region", { name: "分钟策略配置" });
    const source = within(configuration).getByRole("button", { name: "查看分钟数据来源" });
    expect(document.body.textContent).not.toContain("original-minute");
    act(() => source.focus());
    expect(source).toHaveFocus();
    await waitFor(() =>
      expect(screen.getAllByRole("tooltip").map((tip) => tip.textContent)).toContain(
        "来源 original-minute；第 1 版",
      ),
    );
  });

  it("NATIVE-EXP-05 同条结果原参数文字与执行资金、模拟数量完整展示", async () => {
    const user = ready();
    const mine = await screen.findByRole("table", { name: "我的实验" });
    await user.click(within(mine).getByRole("button", { name: "分钟策略实验 · 1" }));
    await screen.findByRole("img", { name: "实验与基准净值" });
    const complete = screen.getAllByRole("region", { name: "分钟策略配置" }).at(-1);
    if (!complete) throw new Error("complete native configuration is missing");
    expect(within(complete).getByText("突破高点比例")).toBeVisible();
    expect(within(complete).getByText("1.002 倍")).toBeVisible();
    expect(within(complete).getByText("0.998 倍")).toBeVisible();
    expect(within(complete).getByText("90 秒")).toBeVisible();
    expect(within(complete).getByText("100,000.00 元")).toBeVisible();
    expect(within(complete).getByText("买入意向 · 100 股")).toBeVisible();
    expect(within(complete).getByText("卖出意向 · 100 股")).toBeVisible();
    expect(within(complete).getByText("5 秒")).toBeVisible();
    expect(document.body.textContent).not.toContain("break_high_ratio");
    expect(document.body.textContent).not.toContain("synthetic-paper-alice");
    expect(document.body.textContent).not.toContain("synthetic-minute-profile");
    expect(findJargon(complete.textContent ?? "")).toEqual([]);
    const profile = within(complete).getByRole("button", { name: "查看分钟执行配置" });
    act(() => profile.focus());
    await waitFor(() =>
      expect(
        screen
          .getAllByRole("tooltip")
          .some((tip) =>
            tip.textContent?.includes(JSON.stringify(fixture.result.data.native.execution_profile)),
          ),
      ).toBe(true),
    );
  });

  it("NATIVE-EXP-06 缺中文参数或完整执行配置时不解释原数字或造资金", async () => {
    const user = ready({
      ...fixture.result.data.native,
      parameters: [{ name: "break_high_ratio", value: 1.002 }],
      execution_profile: null,
    });
    const mine = await screen.findByRole("table", { name: "我的实验" });
    await user.click(within(mine).getByRole("button", { name: "分钟策略实验 · 1" }));
    await screen.findByRole("img", { name: "实验与基准净值" });
    const complete = screen.getAllByRole("region", { name: "分钟策略配置" }).at(-1);
    if (!complete) throw new Error("complete native configuration is missing");
    expect(within(complete).getByText("初始资金").nextElementSibling?.textContent).toBe("—");
    expect(within(complete).getByText("模拟数量").nextElementSibling?.textContent).toBe("—");
    expect(within(complete).getByText("执行延迟").nextElementSibling?.textContent).toBe("—");
    expect(within(complete).getByText("参数").nextElementSibling?.textContent).toContain("—");
    expect(document.body.textContent).not.toContain("break_high_ratio");
    expect(within(complete).queryByText("1.002 倍")).not.toBeInTheDocument();
  });

  it("NATIVE-EXP-07 详情滑入期间不接受来源焦点，稳定后保留原Tip与关闭焦点", async () => {
    drawerMotion.holdOpen = true;
    const user = ready();
    const mine = await screen.findByRole("table", { name: "我的实验" });
    const trigger = within(mine).getByRole("button", { name: "分钟策略实验 · 1" });
    await user.click(trigger);
    const dialog = await screen.findByRole("dialog", { name: "分钟策略实验" });
    await screen.findByRole("img", { name: "实验与基准净值" });
    const configuration = within(dialog).getAllByRole("region", { name: "分钟策略配置" }).at(-1);
    if (!configuration) throw new Error("complete native configuration is missing");
    const source = within(configuration).getByRole("button", { name: "查看分钟数据来源" });
    expect(source).toBeDisabled();
    for (const button of within(dialog).getAllByRole("button", { name: "查看分钟数据来源" }))
      expect(button).toBeDisabled();
    act(() => source.focus());
    expect(source).not.toHaveFocus();
    expect(screen.queryByRole("tooltip", { name: "来源 original-minute；第 1 版" })).toBeNull();
    await waitFor(() => expect(drawerMotion.completeOpen).not.toBeNull());
    act(() => {
      if (!drawerMotion.completeOpen) throw new Error("original drawer open receipt is missing");
      drawerMotion.completeOpen();
    });
    await waitFor(() => expect(source).toBeEnabled());
    act(() => source.focus());
    expect(source).toHaveFocus();
    await screen.findByRole("tooltip", { name: "来源 original-minute；第 1 版" });
    await user.keyboard("{Escape}");
    await waitFor(() => expect(dialog).not.toBeInTheDocument());
    await waitFor(() =>
      expect(screen.getByRole("button", { name: "分钟策略实验 · 1" })).toHaveFocus(),
    );
  });
});
