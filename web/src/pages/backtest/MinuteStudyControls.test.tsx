import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import type { ComponentType } from "react";
import { findJargon } from "@/test/jargon";
import { ThemeProvider } from "@/theme/ThemeProvider";
import { UiProvider } from "@/ui";
import type { MinuteStudyControlsProps, MinuteStudyDraft } from "./MinuteStudyControls";

const modules = import.meta.glob<{
  MinuteStudyControls: ComponentType<MinuteStudyControlsProps>;
}>("./MinuteStudyControls.tsx", { eager: true });

// Caller-provided presentation inputs. These are not public DTOs or execution proof.
const draft: MinuteStudyDraft = {
  mode: "grid",
  scoreProfile: "owner-score-01",
  topN: "3",
  minTrades: "8",
  randomTrials: "12",
  seed: "137",
  axes: [],
  windows: { folds: "4", minTrainingDates: "20", validationDates: "5" },
};
const props: MinuteStudyControlsProps = {
  scopeKey: "explicit-ui-owner:source-a:version-a",
  draftKey: "new",
  status: "ready",
  defaultDraft: draft,
  modes: [
    { key: "grid", label: "网格搜索", available: true, detail: "逐一运行声明的有限取值。" },
    { key: "random", label: "随机搜索", available: true, detail: "沿原随机种子抽取有限方案。" },
    { key: "ablation", label: "固定消融", available: true, detail: "完整保留原五组对照。" },
    { key: "walk_forward", label: "滚动验证", available: true, detail: "沿原日期规则分窗。" },
  ],
  scores: Array.from({ length: 11 }, (_, index) => ({
    key: `owner-score-${String(index + 1).padStart(2, "0")}`,
    label: `原评分方案 ${index + 1}`,
    detail: "只用训练段评分；验证与最终测试不参与选择。",
  })),
  fields: [
    {
      key: "max_hold_days",
      label: "最长持仓天数",
      kind: "number",
      available: true,
      inputModes: ["values", "range"],
      detail: "原参数字段，组合合法性由研究服务核验。",
    },
    {
      key: "paper.stop_loss_pct",
      label: "止损比例",
      kind: "number",
      available: true,
      inputModes: ["values"],
      detail: "输入原比例值。",
    },
    {
      key: "volume_profile.enabled",
      label: "价量过滤",
      kind: "choice",
      available: true,
      inputModes: ["values"],
      choices: [
        { value: "true", label: "开启" },
        { value: "false", label: "关闭" },
      ],
      detail: "取值由原能力提供。",
    },
  ],
  ablations: [
    { key: "original", label: "原始组合", detail: "保留完整原参数。" },
    { key: "owner-a", label: "关闭过滤一", detail: "仅改变原指定字段。" },
    { key: "owner-b", label: "关闭过滤二", detail: "仅改变原指定字段。" },
    { key: "owner-c", label: "关闭过滤三", detail: "仅改变原指定字段。" },
    { key: "owner-d", label: "关闭过滤四", detail: "仅改变原指定字段。" },
  ],
  bounds: {
    topN: { min: 1, max: 20000 },
    minTrades: { min: 1, max: 20000 },
    randomTrials: { min: 1, max: 20000 },
    seed: { min: 0, max: Number.MAX_SAFE_INTEGER },
    folds: { min: 1, max: 366 },
    minTrainingDates: { min: 1, max: 366 },
    validationDates: { min: 1, max: 366 },
    maxSearchFields: 8,
  },
  onDraftChange: vi.fn(),
};

function Form(value: MinuteStudyControlsProps = props) {
  const module = Object.values(modules)[0];
  expect(module, "independent study controls are missing").toBeDefined();
  if (!module) throw new Error("independent study controls are missing");
  return (
    <ThemeProvider>
      <UiProvider>
        <module.MinuteStudyControls {...value} />
      </UiProvider>
    </ThemeProvider>
  );
}

function change(label: string, value: string) {
  fireEvent.change(screen.getByLabelText(label), { target: { value } });
}

it("uses the caller's four modes and all eleven scores without exposing technical keys", async () => {
  const onDraftChange = vi.fn();
  const { container } = render(Form({ ...props, onDraftChange }));
  expect(within(screen.getByLabelText("研究方式")).getAllByRole("option")).toHaveLength(4);
  expect(within(screen.getByLabelText("评分方式")).getAllByRole("option")).toHaveLength(11);
  change("评分方式", "owner-score-11");
  change("每时点最多入选", "17");
  await waitFor(() =>
    expect(onDraftChange.mock.lastCall?.[0]).toMatchObject({
      scoreProfile: "owner-score-11",
      topN: "17",
      minTrades: "8",
      seed: "137",
    }),
  );
  expect(container).not.toHaveTextContent("owner-score-");
  expect(findJargon(container.textContent ?? "")).toEqual([]);
});

it("keeps explicit numeric values and nested field identities without making combinations", async () => {
  const onDraftChange = vi.fn();
  render(Form({ ...props, onDraftChange }));
  fireEvent.click(screen.getByRole("button", { name: "添加参数" }));
  change("参数取值 1", "3, 5, 13");
  fireEvent.click(screen.getByRole("button", { name: "添加参数" }));
  change("参数字段 2", "paper.stop_loss_pct");
  change("参数取值 2", "0, 0.000123456789, 0.07");
  await waitFor(() => expect(onDraftChange.mock.lastCall?.[1]).toBe(true));
  expect(onDraftChange.mock.lastCall?.[0].axes).toEqual([
    {
      fieldKey: "max_hold_days",
      inputMode: "values",
      valuesText: "3, 5, 13",
      selectedValues: [],
      minimum: "",
      maximum: "",
      step: "",
    },
    {
      fieldKey: "paper.stop_loss_pct",
      inputMode: "values",
      valuesText: "0, 0.000123456789, 0.07",
      selectedValues: [],
      minimum: "",
      maximum: "",
      step: "",
    },
  ]);
  expect(screen.queryByText(/预计.*方案/)).not.toBeInTheDocument();
  expect(
    within(screen.getByLabelText("参数字段 2")).getByRole("option", { name: "最长持仓天数" }),
  ).toBeDisabled();
});

it("keeps range boundaries as a raw draft only when the owner offers range input", async () => {
  const onDraftChange = vi.fn();
  render(Form({ ...props, onDraftChange }));
  fireEvent.click(screen.getByRole("button", { name: "添加参数" }));
  change("取值方式 1", "range");
  change("最小值 1", "3");
  change("最大值 1", "13");
  change("步长 1", "2");
  await waitFor(() => expect(onDraftChange.mock.lastCall?.[1]).toBe(true));
  expect(onDraftChange.mock.lastCall?.[0].axes[0]).toMatchObject({
    minimum: "3",
    maximum: "13",
    step: "2",
    valuesText: "",
    inputMode: "range",
  });
  expect(screen.queryByLabelText("参数取值 1")).not.toBeInTheDocument();
  change("参数字段 1", "paper.stop_loss_pct");
  expect(screen.getByLabelText("取值方式 1")).toHaveValue("values");
  expect(
    within(screen.getByLabelText("取值方式 1")).queryByRole("option", { name: "范围" }),
  ).not.toBeInTheDocument();
});

it("uses owner-provided boolean choices and removes a field without filling missing values", async () => {
  const onDraftChange = vi.fn();
  render(Form({ ...props, onDraftChange }));
  fireEvent.click(screen.getByRole("button", { name: "添加参数" }));
  change("参数字段 1", "volume_profile.enabled");
  expect(screen.getByRole("checkbox", { name: "价量过滤：开启" })).not.toBeChecked();
  fireEvent.click(screen.getByRole("checkbox", { name: "价量过滤：关闭" }));
  await waitFor(() => expect(onDraftChange.mock.lastCall?.[1]).toBe(true));
  expect(onDraftChange.mock.lastCall?.[0].axes[0].selectedValues).toEqual(["false"]);
  fireEvent.click(screen.getByRole("button", { name: "移除参数 1" }));
  await waitFor(() => expect(onDraftChange.mock.lastCall?.[0].axes).toEqual([]));
  expect(onDraftChange.mock.lastCall?.[1]).toBe(false);
});

it("retains invalid count input and the exact random seed instead of clamping or sampling", async () => {
  const onDraftChange = vi.fn();
  render(Form({ ...props, onDraftChange }));
  change("研究方式", "random");
  fireEvent.click(screen.getByRole("button", { name: "添加参数" }));
  change("参数取值 1", "3, 5, 13");
  change("随机方案数量", "2");
  change("研究随机种子", "0");
  await waitFor(() => expect(onDraftChange.mock.lastCall?.[1]).toBe(true));
  expect(onDraftChange.mock.lastCall?.[0]).toMatchObject({ randomTrials: "2", seed: "0" });
  change("最少闭环交易", "0");
  await waitFor(() => expect(onDraftChange.mock.lastCall?.[1]).toBe(false));
  expect(screen.getByLabelText("最少闭环交易")).toHaveValue(0);
  expect(onDraftChange.mock.lastCall?.[0].minTrades).toBe("0");
});

it("shows exactly the supplied fixed ablations and keeps unavailable modes disabled", async () => {
  const { rerender } = render(Form());
  change("研究方式", "ablation");
  expect(screen.getByRole("group", { name: "五组固定对照" })).toHaveTextContent("原始组合");
  expect(
    within(screen.getByRole("group", { name: "五组固定对照" })).getAllByRole("listitem"),
  ).toHaveLength(5);
  expect(screen.queryByRole("button", { name: "添加参数" })).not.toBeInTheDocument();
  rerender(
    Form({
      ...props,
      scopeKey: "other-capability",
      modes: props.modes.map((mode) => ({ ...mode, available: mode.key !== "ablation" })),
    }),
  );
  expect(
    within(screen.getByLabelText("研究方式")).getByRole("option", { name: "固定消融" }),
  ).toBeDisabled();
});

it("reuses caller date controls and only records declared rolling window counts", async () => {
  const onDraftChange = vi.fn();
  render(
    Form({
      ...props,
      onDraftChange,
      context: (
        <label>
          训练开始
          <input aria-label="原训练开始" defaultValue="2026-01-05" />
        </label>
      ),
    }),
  );
  change("研究方式", "walk_forward");
  change("滚动窗口数量", "6");
  change("最少训练交易日", "31");
  change("验证交易日", "7");
  await waitFor(() => expect(onDraftChange.mock.lastCall?.[1]).toBe(true));
  expect(onDraftChange.mock.lastCall?.[0].windows).toEqual({
    folds: "6",
    minTrainingDates: "31",
    validationDates: "7",
  });
  expect(screen.getByLabelText("原训练开始")).toHaveValue("2026-01-05");
  expect(screen.queryByText(/第.*窗.*2026/)).not.toBeInTheDocument();
});

it("resets local drafts when actor or source scope changes and preserves edits in the same scope", async () => {
  const onDraftChange = vi.fn();
  const value = { ...props, onDraftChange };
  const { rerender } = render(Form(value));
  change("每时点最多入选", "19");
  rerender(Form({ ...value, message: "已保留草稿" }));
  expect(screen.getByLabelText("每时点最多入选")).toHaveValue(19);
  rerender(Form({ ...value, scopeKey: "foreign-owner:source-b:version-b" }));
  expect(screen.getByLabelText("每时点最多入选")).toHaveValue(3);
  await waitFor(() => expect(onDraftChange.mock.lastCall?.[0].topN).toBe("3"));
});

it("keeps actor scope and restored request keys separate when their text contains separators", () => {
  const { rerender } = render(Form({ ...props, scopeKey: "owner:source", draftKey: "request" }));
  change("每时点最多入选", "19");
  rerender(Form({ ...props, scopeKey: "owner", draftKey: "source:request" }));
  expect(screen.getByLabelText("每时点最多入选")).toHaveValue(3);
});

it("keeps pending and failed drafts, disables missing identity, and never sends a request", async () => {
  const onDraftChange = vi.fn();
  const fetch = vi.spyOn(globalThis, "fetch");
  const { rerender } = render(
    Form({
      ...props,
      onDraftChange,
      status: "pending",
      initialDraft: { ...draft, seed: "197", topN: "23" },
    }),
  );
  expect(screen.getByLabelText("每时点最多入选")).toHaveValue(23);
  expect(screen.getByLabelText("每时点最多入选")).toBeDisabled();
  expect(screen.getByRole("status")).toHaveTextContent("任务待确认");
  rerender(Form({ ...props, onDraftChange, status: "failed", message: "原服务暂时无法处理。" }));
  expect(screen.getByLabelText("每时点最多入选")).toHaveValue(23);
  expect(screen.getByRole("status")).toHaveTextContent("原服务暂时无法处理。");
  expect(screen.getByLabelText("每时点最多入选")).toBeEnabled();
  rerender(Form({ ...props, onDraftChange, scopeKey: null }));
  expect(screen.getByLabelText("每时点最多入选")).toBeDisabled();
  await waitFor(() => expect(onDraftChange.mock.lastCall?.[1]).toBe(false));
  expect(fetch).not.toHaveBeenCalled();
  fetch.mockRestore();
});

it("keeps capability changes unavailable and exposes the original score explanation on keyboard focus", async () => {
  const onDraftChange = vi.fn();
  const { rerender } = render(Form({ ...props, onDraftChange }));
  const user = userEvent.setup();
  const explanation = screen.getByRole("button", { name: "评分说明" });
  act(() => explanation.focus());
  expect(explanation).toHaveFocus();
  await user.keyboard(" ");
  expect(await screen.findByRole("tooltip")).toHaveTextContent(
    "只用训练段评分；验证与最终测试不参与选择。",
  );
  expect(explanation).toHaveFocus();
  rerender(Form({ ...props, onDraftChange, scores: [] }));
  expect(screen.getByLabelText("评分方式")).toBeDisabled();
  await waitFor(() => expect(onDraftChange.mock.lastCall?.[1]).toBe(false));
  rerender(Form({ ...props, onDraftChange, scopeKey: "new-owner:new-source" }));
  await waitFor(() => expect(screen.queryByRole("tooltip")).not.toBeInTheDocument());
});
