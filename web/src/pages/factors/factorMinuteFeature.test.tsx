import { fireEvent, render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useState } from "react";
import { FactorDailyFeatures } from "./FactorDailyFeatures";
import { FactorEditor, type FactorEditorDraft } from "./FactorEditor";
import { dailyResearch } from "./factorDailyFields.fixture";
import {
  minuteCapability,
  minuteResearch,
  mixedMinuteCapability,
  mixedMinuteResearch,
} from "./factorMinuteFeature.fixture";
import { stockResearch } from "./factorStockFeature.fixture";

async function showTip(label: string) {
  const anchor = screen.getByText(label, { exact: true }).closest(".tip-anchor");
  if (!anchor) throw new Error("Tip anchor required");
  fireEvent.focus(anchor);
  return { anchor, tip: await screen.findByRole("tooltip") };
}

it("独立分钟结果展示固定精确时点与历史回顾，不能宣称日线库存", async () => {
  render(<FactorDailyFeatures research={minuteResearch} />);
  expect(screen.getByRole("heading", { name: "分钟字段" })).toBeInTheDocument();
  const { tip } = await showTip("分钟字段口径");
  expect(tip).toHaveTextContent("前一交易日精确15:00");
  expect(tip).toHaveTextContent("下一交易日09:25");
  expect(tip).toHaveTextContent("不以14:59替代");
  expect(tip).toHaveTextContent("最多20个实际观察日");
  expect(tip).toHaveTextContent("仅用于历史回顾");
  expect(tip).toHaveTextContent("不代表当时已知");
  expect(tip).not.toHaveTextContent("库存原值");
  expect(tip).not.toHaveTextContent("初始化");
});

it("组合结果保留完整50项，日线与分钟口径独立，旧字段仍可选择", async () => {
  render(<FactorDailyFeatures research={mixedMinuteResearch} />);
  const daily = await showTip("日线字段口径");
  expect(daily.tip).toHaveTextContent("历史推导：12项");
  expect(daily.tip).toHaveTextContent("库存原值：4项");
  expect(daily.tip).not.toHaveTextContent("库存原值：15项");
  fireEvent.blur(daily.anchor);
  await userEvent.click(screen.getByText("查看字段覆盖"));
  const field = screen.getByRole("combobox", { name: "覆盖字段" });
  expect(within(field).getAllByRole("option")).toHaveLength(50);
  for (const column of ["ma5", "price_position_90d_pct", "signal_minute_amount"]) {
    await userEvent.selectOptions(field, column);
    expect(field).toHaveValue(column);
  }
  const { tip } = await showTip("覆盖说明");
  expect(tip).toHaveTextContent("15:00分钟成交额（分钟派生）");
  expect(tip).toHaveTextContent("单位：元");
});

it("消费独立分钟缺因，区分无目标、无历史、零基准、不适用与无加速历史", async () => {
  const day = minuteResearch.daily_feature_coverage_days?.[0];
  const count = day?.counts[0];
  if (!day || !count) throw new Error("Actual minute coverage required");
  render(
    <FactorDailyFeatures
      research={{
        ...minuteResearch,
        daily_feature_coverage_days: [
          {
            ...day,
            counts: [
              {
                ...count,
                valid: 0,
                null: 12,
                reasons: [],
                stock_reasons: [],
                minute_reasons: [
                  { reason: "missing_target_minute", count: 1 },
                  { reason: "missing_history", count: 1 },
                  { reason: "missing_same_minute_history", count: 1 },
                  { reason: "zero_same_minute_baseline", count: 1 },
                  { reason: "zero_cumulative_baseline", count: 1 },
                  { reason: "not_applicable", count: 1 },
                  { reason: "no_acceleration_history", count: 1 },
                  { reason: "undefined_statistic", count: 5 },
                ],
              },
            ],
          },
        ],
      }}
    />,
  );
  await userEvent.click(screen.getByText("查看字段覆盖"));
  const { tip } = await showTip("0 / 12");
  for (const reason of [
    "缺少15:00分钟",
    "无历史分钟记录",
    "无历史同分钟记录",
    "同分钟基准为零",
    "累计基准为零",
    "此口径不适用",
    "无可用加速历史",
    "统计量无值",
  ]) {
    expect(tip).toHaveTextContent(reason);
  }
  expect(tip).not.toHaveTextContent("初始化");
});

it("实际开盘标记和历史日数保持有效，开盘金额保留不适用与无目标缺因", async () => {
  render(<FactorDailyFeatures research={minuteResearch} />);
  await userEvent.click(screen.getByText("查看字段覆盖"));
  const field = screen.getByRole("combobox", { name: "覆盖字段" });
  const table = screen.getByRole("table", { name: "字段覆盖" });
  for (const column of ["hist_intraday_days_20d", "signal_opening_segment"]) {
    await userEvent.selectOptions(field, column);
    expect(within(table).getAllByText("10 / 12")).toHaveLength(3);
    expect(within(table).getAllByText("0", { exact: true })).not.toHaveLength(0);
  }
  await userEvent.selectOptions(field, "signal_opening_segment_amount");
  const first = within(table).getAllByText("0 / 12")[0];
  if (!first) throw new Error("Coverage value required");
  fireEvent.focus(first.closest(".tip-anchor") ?? first);
  const tip = await screen.findByRole("tooltip");
  expect(tip).toHaveTextContent("此口径不适用：10");
  expect(tip).toHaveTextContent("缺少15:00分钟：2");
});

it("缺少口径或计数用—，换结果只显示本结果来源", async () => {
  const source = minuteResearch.daily_features;
  const day = minuteResearch.daily_feature_coverage_days?.[0];
  if (!source || !day) throw new Error("Minute source required");
  const app = render(
    <FactorDailyFeatures
      research={{
        ...minuteResearch,
        daily_features: { ...source, minute_features: null },
        daily_feature_coverage_days: [{ ...day, counts: [] }],
      }}
    />,
  );
  const missing = await showTip("分钟字段口径");
  expect(missing.tip).toHaveTextContent("本次结果未提供分钟口径");
  expect(missing.tip).not.toHaveTextContent("精确15:00");
  fireEvent.blur(missing.anchor);
  await userEvent.click(screen.getByText("查看字段覆盖"));
  expect(screen.getByText("— / 12")).toBeInTheDocument();
  app.rerender(<FactorDailyFeatures research={stockResearch} />);
  expect(screen.queryByText("分钟字段口径")).toBeNull();
  const stock = await showTip("日线字段口径");
  expect(stock.tip).toHaveTextContent("选股派生");
  fireEvent.blur(stock.anchor);
  app.rerender(<FactorDailyFeatures research={dailyResearch} />);
  expect(screen.queryByText("分钟字段口径")).toBeNull();
  app.rerender(<FactorDailyFeatures research={null} />);
  expect(screen.queryByText("查看字段覆盖")).toBeNull();
});

function Editor({ mixed }: { mixed: boolean }) {
  const [draft, setDraft] = useState<FactorEditorDraft>({
    generation_id: "a".repeat(64),
    mode: "create",
    factor_id: null,
    expected_head: null,
    name_zh: "合成分钟因子",
    category: "technical",
    category_label: "技术",
    direction: "higher_is_better",
    expression: "close + ",
  });
  return (
    <FactorEditor
      draft={draft}
      open
      capabilities={mixed ? mixedMinuteCapability : minuteCapability}
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
  "分钟真实目录可搜索与插入，金额/倍数/观察日/0标记各自说明 (%s)",
  async (mixed) => {
    render(<Editor mixed={mixed} />);
    const dialog = screen.getByRole("dialog", { name: "新建因子" });
    await userEvent.click(within(dialog).getByRole("button", { name: "全部字段" }));
    expect(within(dialog).getAllByRole("button", { name: /^插入/ })).toHaveLength(mixed ? 56 : 17);
    const search = within(dialog).getByRole("searchbox", { name: "搜索字段" });
    for (const [name, unit, words] of [
      ["15:00分钟成交额", "元", "精确15:00"],
      ["累计相对成交额", "倍数", "零基准无值"],
      ["历史分钟观察日数", "观察日数", "0有效"],
      ["开盘段标记", "0 / 1", "缺目标分钟无值"],
      ["近5次成交额加速", "倍数", "不保证连续5分钟"],
    ]) {
      await userEvent.clear(search);
      await userEvent.type(search, name ?? "");
      const info = within(dialog).getByRole("button", { name: `${name}说明` });
      fireEvent.focus(info);
      const tip = await screen.findByRole("tooltip");
      expect(tip).toHaveTextContent(`单位：${unit}`);
      expect(tip).toHaveTextContent(words ?? "");
      expect(tip).not.toHaveTextContent("原核");
      fireEvent.blur(info);
    }
    const expression = within(dialog).getByRole("textbox", {
      name: "表达式",
    }) as HTMLTextAreaElement;
    expression.setSelectionRange(expression.value.length, expression.value.length);
    await userEvent.click(within(dialog).getByRole("button", { name: "插入近5次成交额加速" }));
    expect(within(dialog).getByRole("textbox", { name: "表达式" })).toHaveValue(
      "close + signal_amount_accel_5m",
    );
  },
);
