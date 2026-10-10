import { fireEvent, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useState } from "react";
import type { Schemas } from "@/api/client";
import * as parameterControls from "./MinuteParameterControls";
import { MinuteParameterControls } from "./MinuteParameterControls";
import {
  auctionParameters,
  auctionV2Recipe,
  growthParameters,
  nShapeParameters,
  parameterSet,
} from "./MinuteParameterControls.fixture";

function supported(value: Schemas["MinuteParameterSet"]) {
  const params = value.parameters;
  return [
    ...Object.keys(params),
    ...Object.keys(params.paper ?? {}).map((key) => `paper.${key}`),
    ...(params.family === "n_shape"
      ? Object.keys(params.volume_profile ?? {}).map((key) => `volume_profile.${key}`)
      : []),
  ];
}

function Harness({
  initial,
  enabled,
}: {
  initial: Schemas["MinuteParameterSet"];
  enabled?: readonly string[];
}) {
  const [value, setValue] = useState(initial);
  const [valid, setValid] = useState(true);
  return (
    <>
      <MinuteParameterControls
        value={value}
        frequency={value.parameters.freq}
        supported={enabled ?? supported(value)}
        onChange={setValue}
        onValidityChange={setValid}
      />
      <button type="button" disabled={!valid}>
        保存参数
      </button>
      <output data-testid="recipe">{JSON.stringify(value)}</output>
    </>
  );
}

it("changes a holding term while retaining the complete N recipe and original zero", () => {
  const initial = parameterSet({ ...nShapeParameters, max_hold_days: 7 });
  render(<Harness initial={initial} />);
  fireEvent.change(screen.getByLabelText("最长持仓（交易日）"), { target: { value: "13" } });
  expect(JSON.parse(screen.getByTestId("recipe").textContent ?? "")).toEqual({
    ...initial,
    parameters: { ...initial.parameters, max_hold_days: 13 },
  });
  expect(screen.getByLabelText("量价均线缓冲（%）")).toHaveValue(0);
});

it("keeps an empty required number invalid rather than silently using zero or its old value", () => {
  render(<Harness initial={parameterSet(nShapeParameters)} />);
  fireEvent.change(screen.getByLabelText("最长持仓（交易日）"), { target: { value: "" } });
  expect(screen.getByRole("button", { name: "保存参数" })).toBeDisabled();
  expect(screen.getByLabelText("最长持仓（交易日）")).toHaveValue(null);
  fireEvent.change(screen.getByLabelText("最长持仓（交易日）"), { target: { value: "4" } });
  expect(screen.getByRole("button", { name: "保存参数" })).toBeEnabled();
});

it("only enables source-supported fields and its actual sample frequency", () => {
  render(<Harness initial={parameterSet(nShapeParameters)} enabled={["max_hold_days"]} />);
  expect(screen.getByLabelText("最长持仓（交易日）")).toBeEnabled();
  expect(screen.getByLabelText("采样频率")).toBeDisabled();
  expect(screen.getByRole("option", { name: "5 分钟" })).toBeDisabled();
  expect(screen.getByLabelText("量价均线缓冲（%）")).toBeDisabled();
});

it("edits the original nullable auction fields without turning null into a zero requirement", async () => {
  const initial = parameterSet({
    ...auctionParameters,
    factor_score_threshold: null,
    seal_hold_min_fd_to_circ_pct: 0.75,
  });
  render(<Harness initial={initial} />);
  await userEvent.setup().click(screen.getByText("竞价与退出"));
  expect(screen.getByLabelText("最低因子评分")).toHaveValue(null);
  expect(screen.getByLabelText("最低封单流通比（%）")).toHaveValue(0.75);
  fireEvent.change(screen.getByLabelText("最低因子评分"), { target: { value: "0" } });
  let saved: unknown = JSON.parse(screen.getByTestId("recipe").textContent ?? "");
  expect(saved).toMatchObject({ parameters: { factor_score_threshold: 0 } });
  fireEvent.change(screen.getByLabelText("最低因子评分"), { target: { value: "" } });
  saved = JSON.parse(screen.getByTestId("recipe").textContent ?? "");
  expect(saved).toMatchObject({ parameters: { factor_score_threshold: null } });
  expect(saved).toMatchObject({ parameters: { start_date: "2026-01-05", end_date: "2026-08-03" } });
});

it("retains the fixed v2 auction policy through edits without offering a future-price filter", () => {
  const initial = structuredClone(auctionV2Recipe);
  render(<Harness initial={initial} />);
  fireEvent.change(screen.getByLabelText("最长持仓（交易日）"), { target: { value: "7" } });
  expect(JSON.parse(screen.getByTestId("recipe").textContent ?? "")).toEqual({
    ...initial,
    parameters: { ...initial.parameters, max_hold_days: 7 },
  });
  expect(auctionV2Recipe).toEqual(initial);
  expect(
    parameterControls
      .minuteParameterStudyFields(initial)
      .some((field) => field.key === "next_day_price_policy"),
  ).toBe(false);
  expect(screen.queryByRole("switch", { name: /次日|未来/ })).not.toBeInTheDocument();
});

it("edits growth switches by keyboard and retains its complete paper recipe", async () => {
  const initial = parameterSet(growthParameters);
  render(<Harness initial={initial} />);
  const user = userEvent.setup();
  await user.click(screen.getByText("放量与筛选"));
  const control = screen.getByRole("switch", { name: "要求同刻放量" });
  control.focus();
  await user.keyboard(" ");
  expect(control).toHaveAttribute("aria-checked", "false");
  expect(JSON.parse(screen.getByTestId("recipe").textContent ?? "")).toEqual({
    ...initial,
    parameters: { ...initial.parameters, use_same_minute_surge: false },
  });
});

it("retains missing nested facts and does not create a paper or profile default", () => {
  const { paper: _paper, volume_profile: _profile, ...partial } = nShapeParameters;
  const initial = parameterSet(partial);
  render(<Harness initial={initial} />);
  expect(screen.getByText("模拟盘参数未提供。"));
  expect(screen.getByText("价量分布参数未提供。"));
  expect(JSON.parse(screen.getByTestId("recipe").textContent ?? "")).toEqual(initial);
});

it("keeps exclusive original numeric boundaries invalid and recovers after a legal value", () => {
  render(<Harness initial={parameterSet(nShapeParameters)} />);
  fireEvent.change(screen.getByLabelText("分钟放量倍数"), { target: { value: "1" } });
  expect(screen.getByRole("button", { name: "保存参数" })).toBeDisabled();
  fireEvent.change(screen.getByLabelText("分钟放量倍数"), { target: { value: "1.01" } });
  expect(screen.getByRole("button", { name: "保存参数" })).toBeEnabled();
});

function ReadOnlyRecipe({ rate, disabled = true }: { rate: number; disabled?: boolean }) {
  const value = parameterSet({
    ...nShapeParameters,
    paper: { ...nShapeParameters.paper, stop_loss_pct: rate },
  });
  return (
    <MinuteParameterControls
      value={value}
      frequency="1min"
      supported={supported(value)}
      disabled={disabled}
      onChange={() => undefined}
      onValidityChange={() => undefined}
    />
  );
}

it.each([
  [0.07, "7"],
  [2.5e-9, "0.00000025"],
  [0.0012345678901234567, "0.12345678901234567"],
] as const)(
  "readonly percentage %s retains all source digits without binary display noise",
  (rate, text) => {
    render(<ReadOnlyRecipe rate={rate} />);
    expect(screen.getByLabelText<HTMLInputElement>("止损（%）").value).toBe(text);
  },
);

it("readonly percentage updates from the supplied fact while null and zero stay distinct", () => {
  const { rerender } = render(<ReadOnlyRecipe rate={0.03} />);
  rerender(<ReadOnlyRecipe rate={0.07} />);
  expect(screen.getByLabelText<HTMLInputElement>("止损（%）").value).toBe("7");
  expect(screen.getByLabelText("量价均线缓冲（%）")).toHaveValue(0);
  expect(screen.getByLabelText("分桶相对宽度（%）")).toHaveValue(null);
});

it("readonly formatting leaves the editable draft and source number unchanged across locking", () => {
  const { rerender } = render(<ReadOnlyRecipe rate={0.07} disabled={false} />);
  const field = screen.getByLabelText<HTMLInputElement>("止损（%）");
  expect(field.value).toBe(String(0.07 * 100));
  fireEvent.change(field, { target: { value: "7.123456789" } });
  expect(field.value).toBe("7.123456789");
  rerender(<ReadOnlyRecipe rate={0.07} />);
  expect(field.value).toBe("7");
  rerender(<ReadOnlyRecipe rate={0.07} disabled={false} />);
  expect(field.value).toBe("7.123456789");
});

it.each([
  [nShapeParameters, "carry_low_ratio", "低点承接倍数"],
  [auctionParameters, "min_auction_vol_ratio_5d", "竞价量比下限"],
  [growthParameters, "min_cum_amount_ratio", "累计放量倍数"],
] as const)(
  "exports the original study display metadata for %s without changing its recipe",
  (params, field, label) => {
    expect(parameterControls.minuteParameterStudyFields).toBeTypeOf("function");
    const recipe = parameterSet(params);
    const original = structuredClone(recipe);
    const fields = parameterControls.minuteParameterStudyFields(recipe);
    expect(fields.find((item) => item.key === field)).toMatchObject({ label, kind: "number" });
    expect(fields.find((item) => item.key === "max_hold_days")).toMatchObject({
      label: "最长持仓（交易日）",
      kind: "number",
      integer: true,
    });
    expect(fields.find((item) => item.key === "paper.stop_loss_pct")).toMatchObject({
      label: "止损（%）",
      kind: "number",
      percent: true,
    });
    expect(
      fields.some((item) =>
        ["family", "freq", "schema_version", "paper.candidate_id"].includes(item.key),
      ),
    ).toBe(false);
    expect(new Set(fields.map((item) => item.key)).size).toBe(fields.length);
    expect(recipe).toEqual(original);
  },
);

it("exposes the existing boolean and complete lookback-list display facts without fabricating missing nested fields", () => {
  expect(parameterControls.minuteParameterStudyFields).toBeTypeOf("function");
  const fields = parameterControls.minuteParameterStudyFields(parameterSet(nShapeParameters));
  expect(fields.find((item) => item.key === "volume_profile.enabled")).toMatchObject({
    label: "启用价量分布",
    kind: "boolean",
  });
  expect(fields.find((item) => item.key === "volume_profile.lookback_days")).toMatchObject({
    label: "分布回看（交易日）",
    kind: "integer-list",
  });
  const { paper: _paper, volume_profile: _profile, ...partial } = nShapeParameters;
  expect(
    parameterControls
      .minuteParameterStudyFields(parameterSet(partial))
      .some((item) => item.key.startsWith("paper.") || item.key.startsWith("volume_profile.")),
  ).toBe(false);
});
