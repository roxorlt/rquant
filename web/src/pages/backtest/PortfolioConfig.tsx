import { cloneElement, type ReactElement, useId } from "react";
import type { PortfolioConfig, PortfolioSource } from "@/api/backtests";
import { Tip } from "@/ui";

function Field({
  label,
  tip,
  children,
}: {
  label: string;
  tip?: string;
  children: ReactElement<{ id?: string }>;
}) {
  const id = useId();
  return (
    <div className="pb-field">
      <label htmlFor={id}>{label}</label>
      {cloneElement(children, { id })}
      {tip ? (
        <Tip content={tip}>
          <span className="pb-help" role="img" aria-label={`${label}说明`}>
            ⓘ
          </span>
        </Tip>
      ) : null}
    </div>
  );
}

const percent = (value: string | number | null | undefined) =>
  value == null ? "" : String(Number(value) * 100);
const fraction = (value: string) => (value === "" ? "" : String(Number(value) / 100));
const benchmarks = [
  ["000300.SH", "沪深 300"],
  ["000905.SH", "中证 500"],
  ["000852.SH", "中证 1000"],
  ["000001.SH", "上证指数"],
  ["399001.SZ", "深证成指"],
  ["399006.SZ", "创业板指"],
] as const;

export function PortfolioConfiguration({
  value,
  sources,
  locked,
  onChange,
}: {
  value: PortfolioConfig;
  sources: PortfolioSource[];
  locked: boolean;
  onChange: (value: PortfolioConfig) => void;
}) {
  const source = sources.find(
    (item) => item.key === value.source_key && item.version === value.source_version,
  );
  function weight(update: Partial<PortfolioConfig["weight_rule"]>) {
    onChange({ ...value, weight_rule: { ...value.weight_rule, ...update } });
  }
  function cost(update: Partial<PortfolioConfig["execution_cost_spec"]>) {
    onChange({
      ...value,
      execution_cost_spec: { ...value.execution_cost_spec, ...update, cost_spec_id: null },
    });
  }
  function fee(
    kind: "commission_rules" | "transfer_fee_rules" | "stamp_duty_rules",
    field: "rate_bps" | "minimum_amount",
    amount: string,
  ) {
    cost({ [kind]: value.execution_cost_spec[kind].map((rule) => ({ ...rule, [field]: amount })) });
  }
  return (
    <fieldset className="pb-config" disabled={locked}>
      <legend className="sr-only">回测配置</legend>
      <h3>范围与资金</h3>
      <div className="pb-fields">
        <Field
          label="候选来源"
          tip="只使用已经核验的候选与行情版本。历史回顾假设可在结果说明中查看。"
        >
          <select
            value={`${value.source_key}:${value.source_version}`}
            onChange={(event) => {
              const selected = sources.find(
                (item) => `${item.key}:${item.version}` === event.target.value,
              );
              if (selected)
                onChange({ ...value, source_key: selected.key, source_version: selected.version });
            }}
          >
            {source ? null : (
              <option value={`${value.source_key}:${value.source_version}`}>
                原始候选来源（当前不可用）
              </option>
            )}
            {sources.map((item) => (
              <option key={`${item.key}:${item.version}`} value={`${item.key}:${item.version}`}>
                {item.label} · 第 {item.version} 版
              </option>
            ))}
          </select>
        </Field>
        <Field label="比较基准">
          <select
            value={value.benchmark_code}
            onChange={(event) => onChange({ ...value, benchmark_code: event.target.value })}
          >
            {benchmarks.map(([code, label]) => (
              <option value={code} key={code}>
                {label}
              </option>
            ))}
          </select>
        </Field>
        <Field label="开始日期">
          <input
            type="date"
            required
            min={source?.start_date}
            max={value.end_date}
            value={value.start_date}
            onChange={(event) => onChange({ ...value, start_date: event.target.value })}
          />
        </Field>
        <Field label="结束日期">
          <input
            type="date"
            required
            min={value.start_date}
            max={source?.end_date}
            value={value.end_date}
            onChange={(event) => onChange({ ...value, end_date: event.target.value })}
          />
        </Field>
        <Field label="初始资金（元）">
          <input
            type="number"
            inputMode="decimal"
            min="0.01"
            max="1000000000000"
            step="0.01"
            required
            value={value.initial_cash}
            onChange={(event) => onChange({ ...value, initial_cash: event.target.value })}
          />
        </Field>
        <Field label="调仓频率">
          <select
            value={value.rebalance_rule.kind}
            onChange={(event) => {
              const kind = event.target.value;
              if (kind === "daily" || kind === "weekly" || kind === "monthly" || kind === "every_n")
                onChange({
                  ...value,
                  rebalance_rule: { kind, every_n_days: kind === "every_n" ? 5 : null },
                });
            }}
          >
            <option value="daily">每日</option>
            <option value="weekly">每周</option>
            <option value="monthly">每月</option>
            <option value="every_n">每隔若干交易日</option>
          </select>
        </Field>
        {value.rebalance_rule.kind === "every_n" ? (
          <Field label="交易日间隔">
            <input
              type="number"
              min={1}
              max={500}
              required
              value={value.rebalance_rule.every_n_days ?? 5}
              onChange={(event) =>
                onChange({
                  ...value,
                  rebalance_rule: { kind: "every_n", every_n_days: Number(event.target.value) },
                })
              }
            />
          </Field>
        ) : null}
      </div>
      <h3>仓位分配</h3>
      <div className="pb-fields">
        <Field
          label="权重方式"
          tip={
            source?.ranking_available
              ? "按前一日已经确定的候选排序分配。"
              : "这份来源尚未提供完整排序，只能使用等权。"
          }
        >
          <select
            value={value.weight_rule.method}
            onChange={(event) =>
              weight({ method: event.target.value === "rank_score" ? "rank_score" : "equal" })
            }
          >
            <option value="equal">等权</option>
            <option value="rank_score" disabled={!source?.ranking_available}>
              按排序分数
            </option>
          </select>
        </Field>
        <Field
          label="最多持仓数"
          tip="取候选排序前若干只；缺少排序时，候选数量必须不超过这个上限。"
        >
          <input
            type="number"
            min={1}
            max={500}
            required
            value={value.weight_rule.max_positions}
            onChange={(event) => weight({ max_positions: Number(event.target.value) })}
          />
        </Field>
        <Field label="单股上限（%）">
          <input
            type="number"
            min={0.01}
            max={100}
            step="0.01"
            required
            value={percent(value.weight_rule.max_stock_weight)}
            onChange={(event) => weight({ max_stock_weight: fraction(event.target.value) })}
          />
        </Field>
        <Field label="保留现金（%）">
          <input
            type="number"
            min={0}
            max={100}
            step="0.01"
            required
            value={percent(value.weight_rule.cash_reserve)}
            onChange={(event) => weight({ cash_reserve: fraction(event.target.value) })}
          />
        </Field>
        <Field
          label="行业上限（%）"
          tip={
            source?.industry_available ? "留空表示不限制。" : "这份来源缺少行业资料，暂不可设置。"
          }
        >
          <input
            type="number"
            min={0.01}
            max={100}
            step="0.01"
            disabled={!source?.industry_available}
            value={percent(value.weight_rule.max_industry_weight)}
            onChange={(event) =>
              weight({
                max_industry_weight:
                  event.target.value === "" ? null : fraction(event.target.value),
              })
            }
          />
        </Field>
        <Field label="最小目标金额（元）" tip="低于这个金额的目标不提交订单。">
          <input
            type="number"
            min={0}
            step="0.01"
            required
            value={value.weight_rule.min_target_amount}
            onChange={(event) => weight({ min_target_amount: event.target.value })}
          />
        </Field>
      </div>
      <h3>交易成本</h3>
      <div className="pb-fields">
        <Field label="佣金费率（%）">
          <input
            type="number"
            min={0}
            max={100}
            step="0.0001"
            required
            value={Number(value.execution_cost_spec.commission_rules[0]?.rate_bps ?? 0) / 100}
            onChange={(event) =>
              fee("commission_rules", "rate_bps", String(Number(event.target.value) * 100))
            }
          />
        </Field>
        <Field label="最低佣金（元）">
          <input
            type="number"
            min={0}
            step="0.01"
            required
            value={value.execution_cost_spec.commission_rules[0]?.minimum_amount ?? "0"}
            onChange={(event) => fee("commission_rules", "minimum_amount", event.target.value)}
          />
        </Field>
        <Field label="过户费（%）">
          <input
            type="number"
            min={0}
            max={100}
            step="0.0001"
            required
            value={Number(value.execution_cost_spec.transfer_fee_rules[0]?.rate_bps ?? 0) / 100}
            onChange={(event) =>
              fee("transfer_fee_rules", "rate_bps", String(Number(event.target.value) * 100))
            }
          />
        </Field>
        <Field label="卖出印花税（%）">
          <input
            type="number"
            min={0}
            max={100}
            step="0.0001"
            required
            value={Number(value.execution_cost_spec.stamp_duty_rules[0]?.rate_bps ?? 0) / 100}
            onChange={(event) =>
              fee("stamp_duty_rules", "rate_bps", String(Number(event.target.value) * 100))
            }
          />
        </Field>
        <Field label="买入滑点（基点）">
          <input
            type="number"
            min={0}
            max={1000}
            step="0.01"
            required
            value={value.execution_cost_spec.slippage.buy_bps}
            onChange={(event) =>
              cost({
                slippage: { ...value.execution_cost_spec.slippage, buy_bps: event.target.value },
              })
            }
          />
        </Field>
        <Field label="卖出滑点（基点）">
          <input
            type="number"
            min={0}
            max={1000}
            step="0.01"
            required
            value={value.execution_cost_spec.slippage.sell_bps}
            onChange={(event) =>
              cost({
                slippage: { ...value.execution_cost_spec.slippage, sell_bps: event.target.value },
              })
            }
          />
        </Field>
      </div>
      <h3>回撤限制</h3>
      <div className="pb-check">
        <label>
          <input
            type="checkbox"
            checked={value.drawdown_rule != null}
            onChange={(event) =>
              onChange({
                ...value,
                drawdown_rule: event.target.checked
                  ? {
                      action: "block_new_positions",
                      trigger_drawdown: "0.1",
                      release_drawdown: "0.05",
                      total_risk_weight_cap: null,
                    }
                  : null,
              })
            }
          />
          启用回撤限制
        </label>
        <Tip content="只使用前一日已知净值，限制下一交易日目标；回撤恢复后解除限制。">
          <span className="pb-help" role="img" aria-label="回撤限制说明">
            ⓘ
          </span>
        </Tip>
      </div>
      {value.drawdown_rule ? (
        <div className="pb-fields">
          <Field label="触发回撤（%）">
            <input
              type="number"
              min={0.01}
              max={100}
              step="0.01"
              required
              value={percent(value.drawdown_rule.trigger_drawdown)}
              onChange={(event) =>
                onChange({
                  ...value,
                  drawdown_rule: {
                    ...value.drawdown_rule,
                    action: value.drawdown_rule?.action ?? "block_new_positions",
                    trigger_drawdown: fraction(event.target.value),
                    release_drawdown: value.drawdown_rule?.release_drawdown ?? "0",
                  },
                })
              }
            />
          </Field>
          <Field label="恢复回撤（%）">
            <input
              type="number"
              min={0}
              max={Number(value.drawdown_rule.trigger_drawdown) * 100}
              step="0.01"
              required
              value={percent(value.drawdown_rule.release_drawdown)}
              onChange={(event) =>
                onChange({
                  ...value,
                  drawdown_rule: {
                    ...value.drawdown_rule,
                    action: value.drawdown_rule?.action ?? "block_new_positions",
                    trigger_drawdown: value.drawdown_rule?.trigger_drawdown ?? "0.1",
                    release_drawdown: fraction(event.target.value),
                  },
                })
              }
            />
          </Field>
          <Field label="限制方式">
            <select
              value={value.drawdown_rule.action}
              onChange={(event) =>
                onChange({
                  ...value,
                  drawdown_rule: {
                    ...value.drawdown_rule,
                    action:
                      event.target.value === "cap_total_risk_weight"
                        ? "cap_total_risk_weight"
                        : "block_new_positions",
                    trigger_drawdown: value.drawdown_rule?.trigger_drawdown ?? "0.1",
                    release_drawdown: value.drawdown_rule?.release_drawdown ?? "0",
                    total_risk_weight_cap:
                      event.target.value === "cap_total_risk_weight" ? "0.5" : null,
                  },
                })
              }
            >
              <option value="block_new_positions">暂停开新仓</option>
              <option value="cap_total_risk_weight">限制总仓位</option>
            </select>
          </Field>
          {value.drawdown_rule.action === "cap_total_risk_weight" ? (
            <Field label="总仓位上限（%）">
              <input
                type="number"
                min={0}
                max={100}
                step="0.01"
                required
                value={percent(value.drawdown_rule.total_risk_weight_cap)}
                onChange={(event) =>
                  onChange({
                    ...value,
                    drawdown_rule: {
                      ...value.drawdown_rule,
                      action: "cap_total_risk_weight",
                      trigger_drawdown: value.drawdown_rule?.trigger_drawdown ?? "0.1",
                      release_drawdown: value.drawdown_rule?.release_drawdown ?? "0",
                      total_risk_weight_cap: fraction(event.target.value),
                    },
                  })
                }
              />
            </Field>
          ) : null}
        </div>
      ) : null}
    </fieldset>
  );
}
