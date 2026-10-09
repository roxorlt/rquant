import { act, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { findJargon } from "@/test/jargon";
import { ThemeProvider } from "@/theme/ThemeProvider";
import { UiProvider } from "@/ui";
import {
  MinuteStudyViews,
  type MinuteStudyViewsProps,
  type StudyHeatmapView,
} from "./MinuteStudyViews";

// Presentation fixtures only; the formal caller must supply verified sealed results.
const heatmap: StudyHeatmapView = {
  resultKey: "synthetic-heatmap-a",
  axes: [
    { key: "max_hold_days", label: "持仓天数", format: "count" },
    { key: "paper.stop_loss_pct", label: "止损比例", format: "ratio-percent" },
    { key: "volume_profile.enabled", label: "价量过滤", format: "boolean" },
  ],
  xAxis: { key: "max_hold_days", values: [2, 3] },
  yAxis: { key: "paper.stop_loss_pct", values: [0.01, 0.02] },
  cells: [
    {
      xIndex: 0,
      yIndex: 0,
      studyId: "a".repeat(64),
      isCurrent: true,
      status: "available",
      score: 4.25,
      trades: 8,
      neighborhoodMinimum: -1.75,
      neighborhoodReason: null,
      detail: "完整训练资料。",
    },
    {
      xIndex: 1,
      yIndex: 0,
      studyId: "b".repeat(64),
      isCurrent: false,
      status: "available",
      score: -1.75,
      trades: 8,
      neighborhoodMinimum: 0,
      neighborhoodReason: null,
      detail: null,
    },
    {
      xIndex: 0,
      yIndex: 1,
      studyId: "c".repeat(64),
      isCurrent: false,
      status: "available",
      score: 0,
      trades: 8,
      neighborhoodMinimum: -1.75,
      neighborhoodReason: null,
      detail: null,
    },
    {
      xIndex: 1,
      yIndex: 1,
      studyId: "d".repeat(64),
      isCurrent: false,
      status: "missing_trial",
      score: null,
      trades: null,
      neighborhoodMinimum: null,
      neighborhoodReason: "缺少相邻试验。",
      detail: "尚无此组参数的结果。",
    },
  ],
};

const props: MinuteStudyViewsProps = {
  state: "completed",
  scopeKey: "synthetic-owner-publication-a",
  heatmap,
  ablations: [
    {
      key: "baseline",
      label: "基线",
      status: "available",
      score: 1.2,
      returnPercent: 2.5,
      trades: 32,
      detail: null,
    },
    {
      key: "no-large",
      label: "去大单",
      status: "available",
      score: 0,
      returnPercent: 0,
      trades: 0,
      detail: null,
    },
    {
      key: "no-outer",
      label: "去内外盘",
      status: "available",
      score: -1.3,
      returnPercent: -2,
      trades: 18,
      detail: null,
    },
    {
      key: "no-board",
      label: "去板块",
      status: "unavailable",
      score: null,
      returnPercent: null,
      trades: null,
      detail: "训练资料不足。",
    },
    {
      key: "no-fresh",
      label: "去首爆",
      status: "available",
      score: 0.8,
      returnPercent: 1.1,
      trades: 25,
      detail: null,
    },
  ],
  folds: [
    {
      key: "first-fold",
      label: "第一窗",
      status: "available",
      trainStart: "2026-01-05",
      trainEnd: "2026-02-27",
      validationStart: null,
      validationEnd: null,
      testStart: "2026-03-02",
      testEnd: "2026-03-31",
      trainingScore: 2.75,
      validationScore: null,
      testScore: -0.75,
      testReturnPercent: -1.25,
      testTrades: 9,
      detail: "测试窗口未参与参数选择。",
    },
    {
      key: "second-fold",
      label: "第二窗",
      status: "unavailable",
      trainStart: "2026-01-05",
      trainEnd: "2026-03-31",
      validationStart: null,
      validationEnd: null,
      testStart: "2026-04-01",
      testEnd: "2026-04-30",
      trainingScore: 3.5,
      validationScore: null,
      testScore: null,
      testReturnPercent: null,
      testTrades: null,
      detail: "缺少完整测试资料。",
    },
  ],
};

function View(value: MinuteStudyViewsProps) {
  return (
    <ThemeProvider>
      <UiProvider>
        <MinuteStudyViews {...value} />
      </UiProvider>
    </ThemeProvider>
  );
}

it("shows the provided scores, current parameters and owner neighborhood without recalculation", () => {
  const values = {
    ...props,
    heatmap: {
      ...heatmap,
      cells: heatmap.cells.map((cell) =>
        cell.isCurrent ? { ...cell, neighborhoodMinimum: -9.8765 } : cell,
      ),
    },
  };
  const { container } = render(View(values));
  const current = screen.getByRole("button", {
    name: /持仓天数 2，止损比例 1.00%，评分 4.2500，当前参数/,
  });
  expect(current).toHaveAttribute("aria-current", "true");
  expect(screen.getByLabelText("当前参数邻域最低分")).toHaveTextContent("-9.8765");
  expect(screen.getByLabelText("当前参数评分")).toHaveTextContent("4.2500");
  expect(findJargon(container.textContent ?? "")).toEqual([]);
  expect(container).not.toHaveTextContent("a".repeat(64));
});

it("distinguishes negative, zero and missing scores in text and color", () => {
  render(View(props));
  const negative = screen.getByRole("button", { name: /评分 -1.7500/ });
  const zero = screen.getByRole("button", { name: /评分 0.0000/ });
  const missing = screen.getByRole("button", { name: /持仓天数 3，止损比例 2.00%，无结果/ });
  expect(negative).toHaveClass("down");
  expect(zero).not.toHaveClass("down", "up", "study-cell-missing");
  expect(missing).toHaveClass("study-cell-missing");
  expect(missing).toHaveTextContent("—");
});

it("keeps technical identity in a keyboard-focusable tip and exposes missing reasons", async () => {
  const user = userEvent.setup();
  render(View(props));
  const current = screen.getByRole("button", { name: /评分 4.2500，当前参数/ });
  act(() => current.focus());
  expect(await screen.findByRole("tooltip")).toHaveTextContent("a".repeat(64));
  const missing = screen.getByRole("button", { name: /持仓天数 3，止损比例 2.00%，无结果/ });
  act(() => current.blur());
  expect(screen.queryByText("尚无此组参数的结果。")).not.toBeInTheDocument();
  await user.hover(missing);
  const reason = await screen.findByText("尚无此组参数的结果。");
  expect(reason.closest('[role="tooltip"]')).not.toBeNull();
  expect(missing).toHaveAttribute("aria-describedby");
  await user.click(missing);
  expect(screen.getByLabelText("所看参数邻域最低分")).toHaveTextContent("—");
});

it("opens full score details by touch and uses a compact matrix", async () => {
  const user = userEvent.setup();
  const original = window.matchMedia;
  window.matchMedia = (query) => ({
    ...original(query),
    matches: query === "(hover: none)" || query === "(max-width: 760px)",
  });
  try {
    render(View(props));
    await user.click(screen.getByRole("button", { name: /评分 4.2500，当前参数/ }));
    expect(await screen.findByRole("tooltip")).toHaveTextContent("完整训练资料。");
    expect(screen.getByLabelText("参数热图")).toHaveAttribute("data-columns", "2");
  } finally {
    window.matchMedia = original;
  }
});

it("requests exact axis keys and clears the old grid until new backend data arrives", async () => {
  const user = userEvent.setup();
  const onAxesChange = vi.fn();
  const { rerender } = render(View({ ...props, onAxesChange }));
  await user.selectOptions(screen.getByLabelText("横轴参数"), "volume_profile.enabled");
  expect(onAxesChange).toHaveBeenCalledExactlyOnceWith(
    "volume_profile.enabled",
    "paper.stop_loss_pct",
  );
  expect(screen.queryByRole("button", { name: /评分 4.2500，当前参数/ })).not.toBeInTheDocument();
  expect(screen.getByText("正在加载所选参数")).toBeVisible();
  const replacement: StudyHeatmapView = {
    ...heatmap,
    resultKey: "synthetic-heatmap-b",
    xAxis: { key: "volume_profile.enabled", values: [false, true] },
  };
  rerender(View({ ...props, heatmap: replacement, onAxesChange }));
  expect(
    screen.getByRole("button", { name: /价量过滤 关闭，止损比例 1.00%，评分 4.2500/ }),
  ).toBeVisible();
  expect(screen.queryByText("正在加载所选参数")).not.toBeInTheDocument();
});

it("prevents identical axes and disables an unconnected axis selector", () => {
  render(View(props));
  expect(screen.getByLabelText("横轴参数")).toBeDisabled();
  const vertical = screen.getByLabelText("纵轴参数");
  expect(within(vertical).getByRole("option", { name: "持仓天数" })).toBeDisabled();
});

it("keeps current score and minimum visible off page and locates the actual current cell", async () => {
  const user = userEvent.setup();
  const first = heatmap.cells[0];
  if (!first) throw new Error("complete local heatmap fixture required");
  const wide: StudyHeatmapView = {
    ...heatmap,
    xAxis: { key: "max_hold_days", values: Array.from({ length: 10 }, (_, i) => i + 1) },
    cells: Array.from({ length: 20 }, (_, i) => ({
      ...first,
      xIndex: i % 10,
      yIndex: Math.floor(i / 10),
      studyId: `synthetic-cell-${i}`,
      isCurrent: i === 0,
      score: i + 0.25,
      neighborhoodMinimum: -0.25,
    })),
  };
  render(View({ ...props, heatmap: wide }));
  await user.click(screen.getByRole("button", { name: "下一组横轴" }));
  expect(screen.queryByRole("button", { name: /评分 0.2500，当前参数/ })).not.toBeInTheDocument();
  expect(screen.getByLabelText("当前参数评分")).toHaveTextContent("0.2500");
  expect(screen.getByLabelText("当前参数邻域最低分")).toHaveTextContent("-0.2500");
  await user.click(screen.getByRole("button", { name: "当前参数" }));
  const current = await screen.findByRole("button", { name: /评分 0.2500，当前参数/ });
  await waitFor(() => expect(current).toHaveFocus());
});

it("moves between actual matrix cells with arrow keys and keeps the original current marker", async () => {
  const user = userEvent.setup();
  render(View(props));
  const current = screen.getByRole("button", { name: /评分 4.2500，当前参数/ });
  act(() => current.focus());
  await user.keyboard("{ArrowRight}");
  const next = screen.getByRole("button", { name: /评分 -1.7500/ });
  await waitFor(() => expect(next).toHaveFocus());
  expect(next).not.toHaveAttribute("aria-current");
  expect(current).toHaveAttribute("aria-current", "true");
});

it("shows low-trade and illegal combinations without making a numeric minimum", () => {
  const modified: StudyHeatmapView = {
    ...heatmap,
    cells: heatmap.cells.map((cell, i) =>
      i === 0
        ? {
            ...cell,
            status: "insufficient_trades",
            score: null,
            trades: 4,
            neighborhoodMinimum: null,
            neighborhoodReason: "交易笔数不足。",
            detail: "最少需 5 笔交易。",
          }
        : i === 3
          ? { ...cell, status: "invalid_parameters", detail: "原参数范围不允许此组合。" }
          : cell,
    ),
  };
  render(View({ ...props, heatmap: modified }));
  expect(screen.getByRole("button", { name: /交易不足，当前参数/ })).toHaveTextContent("—");
  expect(screen.getByRole("button", { name: /不适用/ })).toHaveClass("study-cell-missing");
  expect(screen.getByLabelText("当前参数邻域最低分")).toHaveTextContent("—");
});

it("shows all five provided ablations, with exact zero and unavailable facts", () => {
  render(View(props));
  const table = screen.getByRole("table", { name: "五组消融对照" });
  expect(within(table).getAllByRole("row")).toHaveLength(6);
  const zero = within(table).getByRole("row", { name: /去大单/ });
  expect(zero).toHaveTextContent("0.0000");
  expect(zero).toHaveTextContent("0.00%");
  const missing = within(table).getByRole("row", { name: /去板块/ });
  expect(missing).toHaveTextContent("—");
  expect(missing).not.toHaveTextContent("0.0000");
});

it("separates training from independent test values without aggregating incomplete folds", async () => {
  const user = userEvent.setup();
  render(View(props));
  const table = screen.getByRole("table", { name: "滚动分窗结果" });
  expect(within(table).getByRole("columnheader", { name: "训练评分" })).toBeVisible();
  expect(within(table).getByRole("columnheader", { name: "最终测试" })).toBeVisible();
  const first = within(table).getByRole("row", { name: /第一窗/ });
  expect(first).toHaveTextContent("2.7500");
  expect(first).toHaveTextContent("-0.7500");
  expect(first).toHaveTextContent("−1.25%");
  const second = within(table).getByRole("row", { name: /第二窗/ });
  expect(second).toHaveTextContent("—");
  await user.hover(within(first).getByRole("button", { name: "第一窗详情" }));
  expect(await screen.findByRole("tooltip")).toHaveTextContent("2026-03-02 至 2026-03-31");
});

it("shows all three supplied intervals and validation scores without inventing a missing segment", async () => {
  const user = userEvent.setup();
  render(
    View({
      ...props,
      folds: props.folds.map((fold, index) =>
        index === 0
          ? {
              ...fold,
              validationStart: "2026-03-02",
              validationEnd: "2026-03-31",
              validationScore: 0,
              testStart: "2026-04-01",
              testEnd: "2026-04-30",
            }
          : fold,
      ),
    }),
  );
  const table = screen.getByRole("table", { name: "滚动分窗结果" });
  expect(within(table).getByRole("columnheader", { name: "验证评分" })).toBeVisible();
  expect(within(table).getByRole("columnheader", { name: "最终测试" })).toBeVisible();
  const first = within(table).getByRole("row", { name: /第一窗/ });
  expect(first).toHaveTextContent("0.0000");
  await user.hover(within(first).getByRole("button", { name: "第一窗详情" }));
  expect(await screen.findByText("训练：2026-01-05 至 2026-02-27")).toBeInTheDocument();
  expect(screen.getByText("验证：2026-03-02 至 2026-03-31")).toBeInTheDocument();
  expect(screen.getByText("最终测试：2026-04-01 至 2026-04-30")).toBeInTheDocument();
  const second = within(table).getByRole("row", { name: /第二窗/ });
  expect(second).not.toHaveTextContent("0.0000");
  await user.hover(within(second).getByRole("button", { name: "第二窗详情" }));
  expect(await screen.findByText("验证：—")).toBeInTheDocument();
});

it.each([
  ["loading", "正在加载研究结果"],
  ["pending", "研究正在进行"],
  ["empty", "暂无研究结果"],
  ["unavailable", "研究资料暂不可用"],
  ["failed", "研究失败"],
] as const)(
  "%s state clears old private results and keeps technical error details hidden",
  (state, text) => {
    const { container } = render(
      View({ ...props, state, detail: "generation_id private synthetic failure" }),
    );
    if (state === "loading") expect(screen.getByRole("status", { name: text })).toBeVisible();
    else expect(screen.getByText(text)).toBeVisible();
    expect(screen.queryByRole("table", { name: "五组消融对照" })).not.toBeInTheDocument();
    expect(container).not.toHaveTextContent("generation_id");
  },
);

it("resets inspected cells and open private tips when the result scope changes", async () => {
  const user = userEvent.setup();
  const { rerender } = render(View(props));
  await user.hover(screen.getByRole("button", { name: /评分 -1.7500/ }));
  expect(await screen.findByRole("tooltip")).toHaveTextContent("b".repeat(64));
  rerender(View({ ...props, scopeKey: "synthetic-owner-publication-new", state: "empty" }));
  await waitFor(() => expect(screen.queryByRole("tooltip")).not.toBeInTheDocument());
  expect(screen.queryByText("b".repeat(64))).not.toBeInTheDocument();
});
