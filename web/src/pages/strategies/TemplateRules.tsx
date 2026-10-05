import { Tip } from "@/ui";
import type { TemplateRules, TemplateSources } from "./templateApi";
import { rateValue } from "./templateValues";

export const ACTION_COPY = {
  watch: "观察",
  b_intent: "买入意向",
  b_confirm: "买入确认",
  reduce: "减仓",
  s_intent: "卖出意向",
  s_confirm: "卖出确认",
} as const;
export const REBALANCE_COPY = {
  daily: "每天",
  weekly: "每周",
  monthly: "每月",
  every_n: "每隔指定交易日",
} as const;
export function percent(value: string | number | null | undefined): string {
  return value == null ? "—" : `${rateValue(value)}%`;
}

export function parameterCopy(
  value: unknown,
  parameter: TemplateSources["conditions"][number]["block"]["parameters"][number],
): string {
  if (Array.isArray(value))
    return value
      .map(
        (item) => parameter.options?.find((option) => option.value === item)?.label ?? "其他选项",
      )
      .join("、");
  if (typeof value === "string") {
    const selected = parameter.options?.find((option) => option.value === value);
    if (selected) return selected.label;
    const field = /^([A-Z][A-Z0-9_]*)\[(\d+)\]$/.exec(value);
    if (field) {
      const base = parameter.options?.find((option) => option.value === `${field[1]}[0]`);
      return base
        ? `${base.label}${field[2] === "0" ? "" : `（前 ${field[2]} 日）`}`
        : "其他数据项";
    }
    if (!Number.isFinite(Number(value))) return "其他选项";
  }
  if (typeof value === "number" || typeof value === "string")
    return String(Number(value) * (parameter.scale ?? 1));
  return "—";
}

export function TemplateRulesSummary({
  rules,
  sources,
}: {
  rules: TemplateRules;
  sources: TemplateSources | undefined;
}) {
  const entry = rules.entry;
  const pool =
    entry.kind === "pool"
      ? sources?.pools.find(
          (item) =>
            item.pool_key === entry.pool_key &&
            item.version === entry.version &&
            item.body_hash === entry.body_hash,
        )
      : undefined;
  const signal =
    entry.kind === "signal"
      ? sources?.signals.find(
          (item) =>
            item.strategy_id === entry.strategy_id &&
            item.version === entry.version &&
            item.source_hash === entry.source_hash,
        )
      : undefined;
  const exits = rules.exit;
  const weight = rules.weight_rule;
  return (
    <section className="template-rules" aria-label="完整策略规则">
      <section>
        <h3>入场条件</h3>
        {entry.kind === "pool" ? (
          <p>
            {pool?.name ?? "原股票池"} · 第 {entry.version} 版
          </p>
        ) : entry.kind === "signal" ? (
          <p>
            {signal?.name ?? "原策略信号"} · 第 {entry.version} 版 · {ACTION_COPY[entry.action]}
          </p>
        ) : (
          <ul className="template-rule-list">
            {entry.conditions.map((condition, index) => {
              const block = sources?.conditions.find((item) => item.key === condition.key)?.block;
              return (
                // biome-ignore lint/suspicious/noArrayIndexKey: Frozen ordered rules may repeat and the summary has no row state.
                <li key={`${condition.key}-${index}`}>
                  <Tip content={block?.hint}>
                    <strong>{block?.label ?? "原筛选条件"}</strong>
                  </Tip>
                  {block?.parameters.map((parameter) => (
                    <span key={parameter.key}>
                      <Tip
                        content={
                          parameter.hint ||
                          (parameter.minimum == null && parameter.maximum == null
                            ? ""
                            : `范围：${parameter.minimum ?? "无下限"}–${parameter.maximum ?? "无上限"}`)
                        }
                      >
                        {parameter.label}{" "}
                        {parameterCopy(condition.args?.[parameter.key], parameter)}
                      </Tip>
                    </span>
                  ))}
                </li>
              );
            })}
          </ul>
        )}
      </section>
      <section>
        <h3>退出条件</h3>
        <dl className="template-values">
          <dt>止损</dt>
          <dd>{exits?.stop_loss == null ? "未启用" : percent(exits.stop_loss)}</dd>
          <dt>止盈</dt>
          <dd>{exits?.take_profit == null ? "未启用" : percent(exits.take_profit)}</dd>
          <dt>移动止盈</dt>
          <dd>{exits?.trailing_profit == null ? "未启用" : percent(exits.trailing_profit)}</dd>
          <dt>持有上限</dt>
          <dd>
            {exits?.max_holding_days == null ? "未启用" : `${exits.max_holding_days} 个交易日`}
          </dd>
          <dt>
            <Tip content="需要所选分钟的真实价格、状态和观测时间。缺材料时不能运行。">定时退出</Tip>
          </dt>
          <dd>{exits?.exit_time ?? "未启用"}</dd>
        </dl>
      </section>
      <section>
        <h3>仓位与调仓</h3>
        <dl className="template-values">
          <dt>分配方式</dt>
          <dd>{weight.method === "rank_score" ? "按排名得分" : "等权"}</dd>
          <dt>
            <Tip content="1–500 只">持仓上限</Tip>
          </dt>
          <dd>{weight.max_positions} 只</dd>
          <dt>单股上限</dt>
          <dd>{percent(weight.max_stock_weight ?? 1)}</dd>
          <dt>行业上限</dt>
          <dd>
            {weight.max_industry_weight == null ? "不限" : percent(weight.max_industry_weight)}
          </dd>
          <dt>现金保留</dt>
          <dd>{percent(weight.cash_reserve ?? 0)}</dd>
          <dt>最小目标金额</dt>
          <dd className="num">
            {Number(weight.min_target_amount ?? 0).toLocaleString("zh-CN", {
              minimumFractionDigits: 2,
              maximumFractionDigits: 2,
            })}{" "}
            元
          </dd>
          <dt>调仓</dt>
          <dd>
            {rules.rebalance_rule.kind === "every_n"
              ? `每 ${rules.rebalance_rule.every_n_days} 个交易日`
              : REBALANCE_COPY[rules.rebalance_rule.kind]}
          </dd>
          <dt>指数过滤</dt>
          <dd>
            {rules.index_filter == null
              ? "未启用"
              : `${rules.index_filter.benchmark_code} · ${rules.index_filter.direction === "above" ? "高于" : "低于"} ${rules.index_filter.ma_days} 日均线`}
          </dd>
        </dl>
      </section>
    </section>
  );
}
