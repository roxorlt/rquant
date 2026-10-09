import { useEffect, useState } from "react";
import type { Schemas } from "@/api/client";
import { Switch, Tip } from "@/ui";

type Parameters = Schemas["MinuteParameterSet"];
type NumericKey<T> = { [K in keyof T]-?: NonNullable<T[K]> extends number ? K : never }[keyof T];
type BooleanKey<T> = { [K in keyof T]-?: T[K] extends boolean ? K : never }[keyof T];
type NumberSpec<T> = {
  key: NumericKey<T>;
  label: string;
  min?: number;
  max?: number;
  exclusiveMin?: boolean;
  exclusiveMax?: boolean;
  integer?: boolean;
  percent?: boolean;
  nullable?: boolean;
  hint?: string;
};
type BooleanSpec<T> = { key: BooleanKey<T>; label: string };

export type MinuteParameterStudyField = {
  key: string;
  label: string;
  kind: "number" | "boolean" | "integer-list";
  percent?: boolean;
  integer?: boolean;
  nullable?: boolean;
  hint?: string;
};

const maxHoldingLabel = "最长持仓（交易日）";
const profileLookbackLabel = "分布回看（交易日）";

const frequencyOptions = [
  { value: "1min", label: "1 分钟" },
  { value: "5min", label: "5 分钟" },
  { value: "15min", label: "15 分钟" },
  { value: "30min", label: "30 分钟" },
  { value: "60min", label: "60 分钟" },
] as const;

function readonlyNumberText(value: number | null | undefined, scale: 1 | 100): string {
  if (value == null) return "";
  if (scale === 1 || !Number.isFinite(value)) return String(value);
  // Shift the source decimal spelling without adding binary multiplication noise.
  const [coefficient = "0", exponent = "0"] = String(value).split("e");
  const negative = coefficient.startsWith("-");
  const [whole = "0", fraction = ""] = (negative ? coefficient.slice(1) : coefficient).split(".");
  const digits = whole + fraction;
  const point = whole.length + Number(exponent) + 2;
  const shifted =
    point <= 0
      ? `0.${"0".repeat(-point)}${digits}`
      : point >= digits.length
        ? digits + "0".repeat(point - digits.length)
        : `${digits.slice(0, point)}.${digits.slice(point)}`;
  const [integer = "0", decimal = ""] = shifted.split(".");
  const tail = decimal.replace(/0+$/, "");
  return `${negative ? "-" : ""}${integer.replace(/^0+(?=\d)/, "")}${tail === "" ? "" : `.${tail}`}`;
}

function NumberControl({
  value,
  label,
  disabled,
  min,
  max,
  exclusiveMin,
  exclusiveMax,
  integer,
  percent,
  nullable,
  hint,
  onChange,
  onValid,
}: {
  value: number | null | undefined;
  label: string;
  disabled: boolean;
  min?: number;
  max?: number;
  exclusiveMin?: boolean;
  exclusiveMax?: boolean;
  integer?: boolean;
  percent?: boolean;
  nullable?: boolean;
  hint?: string;
  onChange: (value: number | null) => void;
  onValid: (valid: boolean) => void;
}) {
  const scale = percent ? 100 : 1;
  const [draft, setDraft] = useState(value == null ? "" : String(value * scale));
  useEffect(() => setDraft(value == null ? "" : String(value * scale)), [value, scale]);
  return (
    <label className="bt-parameter-field">
      <span>
        {label}
        {hint ? (
          <Tip content={hint}>
            <span className="bt-context-tip" role="img" aria-label={`${label}说明`}>
              ⓘ
            </span>
          </Tip>
        ) : null}
      </span>
      <input
        className="inp num"
        aria-label={label}
        type="number"
        inputMode={integer ? "numeric" : "decimal"}
        required={!nullable}
        disabled={disabled}
        step={integer ? 1 : "any"}
        min={min === undefined ? undefined : min * scale}
        max={max === undefined ? undefined : max * scale}
        placeholder={nullable ? "不设" : "—"}
        value={disabled ? readonlyNumberText(value, scale) : draft}
        onChange={(event) => {
          const text = event.target.value;
          setDraft(text);
          if (text === "") {
            onValid(Boolean(nullable));
            if (nullable) onChange(null);
          } else {
            const number = Number(text) / scale;
            const valid =
              Number.isFinite(number) &&
              (!integer || Number.isInteger(number)) &&
              (min === undefined || (exclusiveMin ? number > min : number >= min)) &&
              (max === undefined || (exclusiveMax ? number < max : number <= max));
            onValid(valid);
            if (valid) onChange(number);
          }
        }}
      />
    </label>
  );
}

function ChoiceControl<T extends string>({
  label,
  value,
  options,
  disabled,
  onChange,
}: {
  label: string;
  value: T;
  options: readonly { value: T; label: string; disabled?: boolean }[];
  disabled: boolean;
  onChange: (value: T) => void;
}) {
  return (
    <label className="bt-parameter-field">
      <span>{label}</span>
      <select
        className="inp"
        aria-label={label}
        value={value}
        disabled={disabled}
        onChange={(event) => {
          const next = options.find(
            (option) => option.value === event.target.value && !option.disabled,
          );
          if (next) onChange(next.value);
        }}
      >
        {options.map((option) => (
          <option key={option.value} value={option.value} disabled={option.disabled}>
            {option.label}
          </option>
        ))}
      </select>
    </label>
  );
}

const nNumbers: readonly NumberSpec<Schemas["MinuteNShapeParameters"]>[] = [
  { key: "carry_low_ratio", label: "低点承接倍数", min: 0, exclusiveMin: true },
  { key: "carry_close_ratio", label: "收盘承接倍数", min: 0, exclusiveMin: true },
  { key: "break_high_ratio", label: "前高突破倍数", min: 0, exclusiveMin: true },
  {
    key: "retest_tolerance_pct",
    label: "回踩容差（%）",
    min: 0,
    max: 0.05,
    percent: true,
    exclusiveMax: true,
  },
  {
    key: "vwap_buffer_pct",
    label: "量价均线缓冲（%）",
    min: 0,
    max: 0.02,
    percent: true,
    exclusiveMax: true,
  },
  { key: "amount_surge_lookback", label: "放量回看（分钟）", min: 1, max: 30, integer: true },
  {
    key: "amount_surge_min_prior_minutes",
    label: "最少先前分钟数",
    min: 1,
    max: 30,
    integer: true,
  },
  { key: "amount_surge_ratio", label: "分钟放量倍数", min: 1, max: 20, exclusiveMin: true },
  { key: "factor_score_threshold", label: "因子评分门槛", min: 0 },
  {
    key: "price_discontinuity_pct",
    label: "价格跳变容差（%）",
    min: 0,
    max: 1,
    percent: true,
    exclusiveMin: true,
    exclusiveMax: true,
  },
];
const auctionNumbers: readonly NumberSpec<Schemas["MinuteAuctionGapParameters"]>[] = [
  { key: "min_auction_vol_ratio_5d", label: "竞价量比下限", min: 0 },
  { key: "max_auction_vol_ratio_5d", label: "竞价量比上限", min: 0 },
  {
    key: "entry_pullback_tolerance_pct",
    label: "入场回踩容差（%）",
    min: 0,
    max: 0.1,
    percent: true,
    exclusiveMax: true,
  },
  {
    key: "entry_vwap_buffer_pct",
    label: "入场均线缓冲（%）",
    min: 0,
    max: 0.05,
    percent: true,
    exclusiveMax: true,
  },
  { key: "min_limit_progress_pct", label: "最少涨停进度（%）", min: 0, max: 1, percent: true },
  {
    key: "next_auction_weak_gap_pct",
    label: "次日弱竞价门槛（%）",
    min: -0.2,
    max: 0.2,
    percent: true,
    exclusiveMin: true,
    exclusiveMax: true,
  },
  {
    key: "strong_seal_min_close_minutes",
    label: "强封板最少分钟",
    min: 1,
    max: 240,
    integer: true,
  },
  {
    key: "strong_seal_weak_gap_pct",
    label: "强封板弱开门槛（%）",
    min: -0.2,
    max: 0.2,
    percent: true,
    exclusiveMin: true,
    exclusiveMax: true,
  },
  {
    key: "next_morning_vwap_break_buffer_pct",
    label: "次日均线跌破缓冲（%）",
    min: 0,
    max: 0.05,
    percent: true,
    exclusiveMax: true,
  },
  { key: "price_tol", label: "价格容差", min: 0 },
  { key: "seal_hold_max_days", label: "封板延长持有（交易日）", min: 1, max: 10, integer: true },
  { key: "seal_hold_max_open_times", label: "最多开板次数", min: 0, max: 50, integer: true },
  {
    key: "seal_hold_min_fd_to_circ_pct",
    label: "最低封单流通比（%）",
    min: 0,
    nullable: true,
  },
  { key: "factor_score_threshold", label: "最低因子评分", min: 0, nullable: true },
];
const growthNumbers: readonly NumberSpec<Schemas["MinuteGrowthParameters"]>[] = [
  { key: "lookback_days", label: "放量历史窗口（交易日）", min: 1, max: 90, integer: true },
  { key: "min_hist_days", label: "最少历史交易日", min: 1, max: 90, integer: true },
  { key: "min_cum_amount_ratio", label: "累计放量倍数", min: 0, exclusiveMin: true },
  { key: "min_same_minute_amount_ratio", label: "同刻放量倍数", min: 0, exclusiveMin: true },
  { key: "min_amount_accel_5m", label: "五分钟加速倍数", min: 0, exclusiveMin: true },
  {
    key: "vwap_buffer_pct",
    label: "量价均线缓冲（%）",
    min: 0,
    max: 0.05,
    percent: true,
    exclusiveMax: true,
  },
  { key: "max_inner_outer_ratio", label: "内外盘比上限", min: 0, exclusiveMin: true },
  { key: "min_large_net_vol", label: "最低大单净量", hint: "按原指标单位，保持原数值。" },
  { key: "fresh_lookback_days", label: "首次放量回看（交易日）", min: 1, max: 20, integer: true },
  { key: "fresh_max_prior_volume_ratio", label: "此前放量比上限", min: 0, exclusiveMin: true },
  { key: "min_listing_trading_days", label: "最少上市交易日", min: 0, integer: true },
  { key: "min_board_gap_up_ratio", label: "板块高开占比（%）", min: 0, max: 1, percent: true },
  { key: "min_board_auction_amount_ratio", label: "板块竞价金额倍数", min: 0, exclusiveMin: true },
  { key: "board_hist_days", label: "板块历史窗口（交易日）", min: 1, max: 90, integer: true },
  { key: "factor_score_threshold", label: "因子评分门槛", min: 0 },
  { key: "price_tol", label: "价格容差", min: 0, max: 1, exclusiveMin: true, exclusiveMax: true },
];
const paperNumbers: readonly NumberSpec<Schemas["MinutePaperParameters"]>[] = [
  {
    key: "stop_loss_pct",
    label: "止损（%）",
    min: 0,
    max: 1,
    percent: true,
    exclusiveMin: true,
    exclusiveMax: true,
  },
  {
    key: "entry_buffer_pct",
    label: "买入缓冲（%）",
    min: 0,
    max: 1,
    percent: true,
    exclusiveMin: true,
    exclusiveMax: true,
  },
  {
    key: "entry_slippage_pct",
    label: "买入滑点（%）",
    min: 0,
    max: 0.05,
    percent: true,
    exclusiveMax: true,
  },
  {
    key: "take_profit_pct",
    label: "止盈（%）",
    min: 0,
    max: 1,
    percent: true,
    exclusiveMin: true,
    exclusiveMax: true,
  },
  {
    key: "trailing_stop_pct",
    label: "移动止损（%）",
    min: 0,
    max: 1,
    percent: true,
    exclusiveMin: true,
    exclusiveMax: true,
  },
];
const profileNumbers: readonly NumberSpec<Schemas["MinuteVolumeProfileParameters"]>[] = [
  { key: "min_reclaimed_poc_count", label: "收复筹码峰数量", min: 1, max: 1, integer: true },
  { key: "min_reward_risk", label: "最低盈亏比", min: 0, exclusiveMin: true },
  {
    key: "max_stop_distance_pct",
    label: "最大止损距离（%）",
    min: 0,
    max: 0.2,
    percent: true,
    exclusiveMin: true,
    exclusiveMax: true,
  },
  {
    key: "min_take_profit_pct",
    label: "最低止盈幅度（%）",
    min: 0,
    max: 0.2,
    percent: true,
    exclusiveMin: true,
    exclusiveMax: true,
  },
  {
    key: "fallback_take_profit_pct",
    label: "备用止盈幅度（%）",
    min: 0,
    max: 0.3,
    percent: true,
    exclusiveMin: true,
    exclusiveMax: true,
  },
  {
    key: "support_buffer_pct",
    label: "支撑缓冲（%）",
    min: 0,
    max: 0.05,
    percent: true,
    exclusiveMax: true,
  },
  {
    key: "resistance_buffer_pct",
    label: "阻力缓冲（%）",
    min: 0,
    max: 0.05,
    percent: true,
    exclusiveMax: true,
  },
  {
    key: "trailing_stop_pct",
    label: "分布移动止损（%）",
    min: 0,
    max: 0.2,
    percent: true,
    exclusiveMin: true,
    exclusiveMax: true,
  },
  {
    key: "bin_ratio",
    label: "分桶相对宽度（%）",
    min: 0,
    max: 0.1,
    percent: true,
    nullable: true,
    hint: "留空或零沿用原历史分桶口径。",
    exclusiveMax: true,
  },
];

const auctionToggles: readonly BooleanSpec<Schemas["MinuteAuctionGapParameters"]>[] = [
  { key: "seal_hold_enabled", label: "启用封板延长持有" },
];
const growthToggles: readonly BooleanSpec<Schemas["MinuteGrowthParameters"]>[] = [
  { key: "use_same_minute_surge", label: "要求同刻放量" },
  { key: "use_accel_surge", label: "要求五分钟加速" },
  { key: "require_vwap_strength", label: "要求量价均线强势" },
  { key: "require_inner_outer", label: "限制内外盘比" },
  { key: "require_large_net_vol", label: "要求大单净量" },
  { key: "require_fresh_surge", label: "要求首次放量" },
  { key: "require_board_favor", label: "要求板块竞价强势" },
  { key: "enable_factor_confirm", label: "启用因子评分确认" },
];
const profileToggles: readonly BooleanSpec<Schemas["MinuteVolumeProfileParameters"]>[] = [
  { key: "enabled", label: "启用价量分布" },
  { key: "filter_entry", label: "参与入场过滤" },
  { key: "require_profile", label: "要求完整分布" },
];

/** Display metadata only; the caller still intersects actual backend capabilities. */
export function minuteParameterStudyFields(
  value: Parameters,
): readonly MinuteParameterStudyField[] {
  const fields: MinuteParameterStudyField[] = [
    { key: "max_hold_days", label: maxHoldingLabel, kind: "number", integer: true },
  ];
  function numbers<T>(specs: readonly NumberSpec<T>[], prefix = "") {
    fields.push(
      ...specs.map(({ key, label, percent, integer, nullable, hint }) => ({
        key: `${prefix}${String(key)}`,
        label,
        kind: "number" as const,
        percent,
        integer,
        nullable,
        hint,
      })),
    );
  }
  function booleans<T>(specs: readonly BooleanSpec<T>[], prefix = "") {
    fields.push(
      ...specs.map(({ key, label }) => ({
        key: `${prefix}${String(key)}`,
        label,
        kind: "boolean" as const,
      })),
    );
  }
  const params = value.parameters;
  if (params.family === "n_shape") numbers(nNumbers);
  else if (params.family === "auction_gap") {
    numbers(auctionNumbers);
    booleans(auctionToggles);
  } else {
    numbers(growthNumbers);
    booleans(growthToggles);
  }
  if (params.paper) numbers(paperNumbers, "paper.");
  if (params.family === "n_shape" && params.volume_profile) {
    numbers(profileNumbers, "volume_profile.");
    booleans(profileToggles, "volume_profile.");
    fields.push({
      key: "volume_profile.lookback_days",
      label: profileLookbackLabel,
      kind: "integer-list",
    });
  }
  return fields;
}

export function MinuteParameterControls({
  value,
  frequency,
  supported,
  disabled = false,
  onChange,
  onValidityChange,
}: {
  value: Parameters;
  frequency: Schemas["MinuteParameterFactSourceOption"]["frequency"];
  supported: readonly string[];
  disabled?: boolean;
  onChange: (value: Parameters) => void;
  onValidityChange: (valid: boolean) => void;
}) {
  const [invalid, setInvalid] = useState<ReadonlySet<string>>(new Set());
  const params = value.parameters;
  const can = (name: string) => !disabled && supported.includes(name);
  const setValid = (key: string, valid: boolean) =>
    setInvalid((previous) => {
      if (previous.has(key) === !valid) return previous;
      const next = new Set(previous);
      if (valid) next.delete(key);
      else next.add(key);
      return next;
    });
  useEffect(() => onValidityChange(invalid.size === 0), [invalid, onValidityChange]);
  function numbers<T extends object>(
    values: T,
    specs: readonly NumberSpec<T>[],
    update: (next: T) => void,
    prefix = "",
  ) {
    return specs.map((spec) => {
      const { key: fieldKey, ...props } = spec;
      const raw = values[fieldKey];
      const key = `${prefix}${String(fieldKey)}`;
      return (
        <NumberControl
          key={key}
          {...props}
          value={typeof raw === "number" ? raw : raw === null ? null : undefined}
          disabled={!can(key)}
          onValid={(valid) => setValid(key, valid)}
          onChange={(next) => update({ ...values, [fieldKey]: next })}
        />
      );
    });
  }
  function toggles<T extends object>(
    values: T,
    specs: readonly BooleanSpec<T>[],
    update: (next: T) => void,
    prefix = "",
  ) {
    return specs.map((spec) => (
      <div className="bt-parameter-switch" key={String(spec.key)}>
        <span>{spec.label}</span>
        <Switch
          label={spec.label}
          checked={values[spec.key] === true}
          disabled={!can(`${prefix}${String(spec.key)}`)}
          onChange={(next) => update({ ...values, [spec.key]: next })}
        />
      </div>
    ));
  }
  const time = (label: string, key: string, raw: string, update: (next: string) => void) => (
    <label className="bt-parameter-field">
      <span>{label}</span>
      <input
        className="inp"
        aria-label={label}
        type="time"
        step={1}
        required
        disabled={!can(key)}
        value={raw}
        onChange={(event) => {
          setValid(key, event.target.value !== "");
          update(event.target.value);
        }}
      />
    </label>
  );
  const paper = params.paper;
  const profile = params.family === "n_shape" ? params.volume_profile : undefined;
  return (
    <fieldset className="bt-parameter-controls" aria-label="完整策略参数" disabled={disabled}>
      <legend className="sr-only">完整策略参数</legend>
      <div className="bt-parameter-grid">
        <ChoiceControl
          label="采样频率"
          value={params.freq}
          options={frequencyOptions.map((option) => ({
            ...option,
            disabled: option.value !== frequency,
          }))}
          disabled={!can("freq")}
          onChange={(freq) => onChange({ ...value, parameters: { ...params, freq } })}
        />
        <NumberControl
          label={maxHoldingLabel}
          value={params.max_hold_days}
          integer
          min={1}
          max={params.family === "n_shape" ? 20 : 10}
          disabled={!can("max_hold_days")}
          onValid={(valid) => setValid("max_hold_days", valid)}
          onChange={(max_hold_days) => {
            if (max_hold_days !== null)
              onChange({ ...value, parameters: { ...params, max_hold_days } });
          }}
        />
      </div>
      <p className="bt-runtime-note">可修改项由所选来源提供；频率需有对应的完整分钟资料。</p>
      {params.family === "n_shape" ? (
        <details className="bt-parameter-section" open>
          <summary>入场条件</summary>
          <div className="bt-parameter-grid">
            <ChoiceControl
              label="观察池"
              value={params.preset_name}
              options={[
                { value: "n-shape-pool1", label: "一池" },
                { value: "n-shape-pool2", label: "二池" },
                { value: "n-shape-combined", label: "一池与二池" },
              ]}
              disabled={!can("preset_name")}
              onChange={(preset_name) =>
                onChange({ ...value, parameters: { ...params, preset_name } })
              }
            />
            <ChoiceControl
              label="入场方式"
              value={params.entry_mode}
              options={[
                { value: "first_break", label: "首次突破" },
                { value: "break_retest", label: "突破回踩" },
                { value: "late_confirm", label: "稍晚确认" },
                { value: "vwap_confirm", label: "量价均线确认" },
                { value: "amount_surge", label: "分钟放量确认" },
                { value: "factor_confirm", label: "因子评分确认" },
              ]}
              disabled={!can("entry_mode")}
              onChange={(entry_mode) =>
                onChange({ ...value, parameters: { ...params, entry_mode } })
              }
            />
            {time("稍晚确认时刻", "late_confirm_at", params.late_confirm_at, (late_confirm_at) =>
              onChange({ ...value, parameters: { ...params, late_confirm_at } }),
            )}
            {numbers(params, nNumbers, (next) => onChange({ ...value, parameters: next }))}
          </div>
        </details>
      ) : null}
      {params.family === "auction_gap" ? (
        <details className="bt-parameter-section">
          <summary>竞价与退出</summary>
          <div className="bt-parameter-grid">
            <ChoiceControl
              label="缺口依据"
              value={params.gap_mode}
              options={[
                { value: "close", label: "前收盘" },
                { value: "strict_high", label: "前最高价" },
              ]}
              disabled={!can("gap_mode")}
              onChange={(gap_mode) => onChange({ ...value, parameters: { ...params, gap_mode } })}
            />
            <ChoiceControl
              label="风险警示股过滤"
              value={params.st_filter}
              options={[
                { value: "case_insensitive", label: "不区分大小写" },
                { value: "literal_lower", label: "只按小写标记" },
                { value: "none", label: "不过滤" },
              ]}
              disabled={!can("st_filter")}
              onChange={(st_filter) => onChange({ ...value, parameters: { ...params, st_filter } })}
            />
            <p className="bt-runtime-note">入场方式：量价均线推进。</p>
            {time("最早入场时刻", "entry_start_time", params.entry_start_time, (entry_start_time) =>
              onChange({ ...value, parameters: { ...params, entry_start_time } }),
            )}
            {time(
              "次日上午退出截止",
              "next_morning_exit_until",
              params.next_morning_exit_until,
              (next_morning_exit_until) =>
                onChange({ ...value, parameters: { ...params, next_morning_exit_until } }),
            )}
            {(
              [
                ["start_date", "竞价资料开始"],
                ["end_date", "竞价资料结束"],
              ] as const
            ).map(([key, label]) => (
              <label className="bt-parameter-field" key={key}>
                {label}
                <input
                  className="inp"
                  type="date"
                  aria-label={label}
                  required
                  disabled={!can(key)}
                  value={params[key]}
                  onChange={(event) => {
                    setValid(key, event.target.value !== "");
                    onChange({ ...value, parameters: { ...params, [key]: event.target.value } });
                  }}
                />
              </label>
            ))}
            {toggles(params, auctionToggles, (next) => onChange({ ...value, parameters: next }))}
            {numbers(params, auctionNumbers, (next) => onChange({ ...value, parameters: next }))}
          </div>
        </details>
      ) : null}
      {params.family === "growth_board_surge" ? (
        <details className="bt-parameter-section">
          <summary>放量与筛选</summary>
          <div className="bt-parameter-grid">
            {time("最早信号时刻", "min_signal_time", params.min_signal_time, (min_signal_time) =>
              onChange({ ...value, parameters: { ...params, min_signal_time } }),
            )}
            {toggles(params, growthToggles, (next) => onChange({ ...value, parameters: next }))}
            {numbers(params, growthNumbers, (next) => onChange({ ...value, parameters: next }))}
          </div>
        </details>
      ) : null}
      {paper ? (
        <details className="bt-parameter-section">
          <summary>模拟盘与风控</summary>
          <div className="bt-parameter-grid">
            {numbers(
              paper,
              paperNumbers,
              (next) => onChange({ ...value, parameters: { ...params, paper: next } }),
              "paper.",
            )}
          </div>
          <Tip content={paper.candidate_id}>
            <span className="bt-context-tip">查看配置依据</span>
          </Tip>
        </details>
      ) : (
        <p className="bt-runtime-note">模拟盘参数未提供。</p>
      )}
      {params.family === "n_shape" && profile ? (
        <details className="bt-parameter-section">
          <summary>价量分布</summary>
          <div className="bt-parameter-grid">
            {toggles(
              profile,
              profileToggles,
              (next) => onChange({ ...value, parameters: { ...params, volume_profile: next } }),
              "volume_profile.",
            )}
            <ProfileLookback
              value={profile.lookback_days}
              disabled={!can("volume_profile.lookback_days")}
              onChange={(lookback_days) =>
                onChange({
                  ...value,
                  parameters: { ...params, volume_profile: { ...profile, lookback_days } },
                })
              }
              onValid={(valid) => setValid("volume_profile.lookback_days", valid)}
            />
            {numbers(
              profile,
              profileNumbers,
              (next) => onChange({ ...value, parameters: { ...params, volume_profile: next } }),
              "volume_profile.",
            )}
          </div>
        </details>
      ) : params.family === "n_shape" ? (
        <p className="bt-runtime-note">价量分布参数未提供。</p>
      ) : null}
    </fieldset>
  );
}

function ProfileLookback({
  value,
  disabled,
  onChange,
  onValid,
}: {
  value: readonly number[];
  disabled: boolean;
  onChange: (value: number[]) => void;
  onValid: (valid: boolean) => void;
}) {
  const [draft, setDraft] = useState(value.join("、"));
  useEffect(() => setDraft(value.join("、")), [value]);
  return (
    <label className="bt-parameter-field">
      <span>
        {profileLookbackLabel}
        <Tip content="多个窗口用逗号分隔，保持原顺序。">
          <span className="bt-context-tip">ⓘ</span>
        </Tip>
      </span>
      <input
        className="inp num"
        aria-label={profileLookbackLabel}
        inputMode="numeric"
        required
        disabled={disabled}
        value={draft}
        onChange={(event) => {
          const raw = event.target.value;
          setDraft(raw);
          const entries = raw.split(/[,，、\s]+/).filter(Boolean);
          const next = entries.map(Number);
          const valid =
            entries.length > 0 &&
            next.every((entry) => Number.isSafeInteger(entry) && entry > 0) &&
            new Set(next).size === next.length;
          onValid(valid);
          if (valid) onChange(next);
        }}
      />
    </label>
  );
}
