import { type FormEvent, useState } from "react";
import { Button, Tip } from "@/ui";
import type { PaperConfiguration, PaperSave } from "./paperPortfolioApi";

export function PaperRulesEditor({
  configuration,
  generation,
  locked,
  onSave,
}: {
  configuration: PaperConfiguration;
  generation: string;
  locked: boolean;
  onSave: (body: PaperSave) => void;
}) {
  const original = configuration.weight_rule;
  const drawdown = configuration.drawdown_rule;
  const [method, setMethod] = useState(original.method ?? "equal");
  const [positions, setPositions] = useState(String(original.max_positions));
  const [stock, setStock] = useState(String(Number(original.max_stock_weight ?? 1) * 100));
  const [industryEnabled, setIndustryEnabled] = useState(original.max_industry_weight != null);
  const [industry, setIndustry] = useState(String(Number(original.max_industry_weight ?? 1) * 100));
  const [cash, setCash] = useState(String(Number(original.cash_reserve ?? 0) * 100));
  const [minimum, setMinimum] = useState(String(original.min_target_amount ?? "0"));
  const [riskEnabled, setRiskEnabled] = useState(drawdown != null);
  const [trigger, setTrigger] = useState(String(Number(drawdown?.trigger_drawdown ?? "0.1") * 100));
  const [release, setRelease] = useState(
    String(Number(drawdown?.release_drawdown ?? "0.02") * 100),
  );
  const [action, setAction] = useState(drawdown?.action ?? "block_new_positions");
  const [cap, setCap] = useState(String(Number(drawdown?.total_risk_weight_cap ?? "0.5") * 100));
  const [error, setError] = useState<string | null>(null);
  function fraction(value: string): string {
    return String(Number(value) / 100);
  }
  function submit(event: FormEvent<HTMLFormElement>): void {
    event.preventDefault();
    const percent = (value: string, zero: boolean, full: boolean) =>
      /^\d+(\.\d+)?$/.test(value) &&
      Number(value) >= (zero ? 0 : Number.MIN_VALUE) &&
      Number(value) <= (full ? 100 : 100 - Number.EPSILON * 100);
    if (
      !/^\d+$/.test(positions) ||
      Number(positions) < 1 ||
      Number(positions) > 500 ||
      !percent(stock, false, true) ||
      !percent(cash, true, true) ||
      (industryEnabled && !percent(industry, false, true)) ||
      !/^\d+(\.\d{1,2})?$/.test(minimum) ||
      Number(minimum) > 1e12 ||
      (riskEnabled &&
        (!percent(trigger, false, false) ||
          !percent(release, true, false) ||
          Number(release) >= Number(trigger) ||
          (action === "cap_total_risk_weight" && !percent(cap, true, false))))
    ) {
      setError("请检查规则。恢复回撤须小于触发回撤，回撤和降仓上限须小于 100%。");
      return;
    }
    setError(null);
    onSave({
      kind: "save_paper_portfolio_configuration",
      command_id: crypto.randomUUID(),
      requested_at: new Date().toISOString(),
      generation_id: generation,
      account_id: configuration.account_id,
      expected_configuration_fingerprint: configuration.fingerprint,
      weight_rule: {
        method,
        max_positions: Number(positions),
        max_stock_weight: fraction(stock),
        max_industry_weight: industryEnabled ? fraction(industry) : null,
        cash_reserve: fraction(cash),
        min_target_amount: minimum,
      },
      drawdown_rule: riskEnabled
        ? {
            trigger_drawdown: fraction(trigger),
            release_drawdown: fraction(release),
            action,
            total_risk_weight_cap: action === "cap_total_risk_weight" ? fraction(cap) : null,
          }
        : null,
    });
  }
  const number = (
    label: string,
    value: string,
    change: (value: string) => void,
    max: number,
    step = "any",
  ) => (
    <label className="field">
      <span className="lbl">{label}</span>
      <input
        className="inp num"
        type="number"
        inputMode="decimal"
        value={value}
        min="0"
        max={max}
        step={step}
        onChange={(event) => change(event.target.value)}
      />
    </label>
  );
  return (
    <form className="paper-rules" onSubmit={submit} noValidate>
      <fieldset disabled={locked}>
        <label className="field">
          <span className="lbl">分配方式</span>
          <select
            className="inp"
            value={method}
            onChange={(event) =>
              setMethod(event.target.value === "rank_score" ? "rank_score" : "equal")
            }
          >
            <option value="equal">等权</option>
            <option value="rank_score">按排名分</option>
          </select>
        </label>
        {number("最多持股数", positions, setPositions, 500, "1")}
        {number("单票上限（%）", stock, setStock, 100)}
        {number("现金保留（%）", cash, setCash, 100)}
        {number("最小目标金额（元）", minimum, setMinimum, 1e12, "0.01")}
        <div className="paper-rule-toggle">
          <label>
            <input
              type="checkbox"
              checked={industryEnabled}
              onChange={(event) => setIndustryEnabled(event.target.checked)}
            />
            行业上限
          </label>
          <Tip content="需要当时可用的申万一级行业。缺行业时本次新入场会拒绝。" interactive>
            <button type="button" className="screen-help" aria-label="行业上限说明">
              ?
            </button>
          </Tip>
        </div>
        {industryEnabled ? number("行业上限（%）", industry, setIndustry, 100) : null}
        <div className="paper-rule-toggle">
          <label>
            <input
              type="checkbox"
              checked={riskEnabled}
              onChange={(event) => setRiskEnabled(event.target.checked)}
            />
            回撤限制
          </label>
          <Tip content="新规则开始新的净值序列。退出仍按原交易限制执行。" interactive>
            <button type="button" className="screen-help" aria-label="回撤限制说明">
              ?
            </button>
          </Tip>
        </div>
        {riskEnabled ? (
          <>
            {number("触发回撤（%）", trigger, setTrigger, 100)}
            {number("恢复回撤（%）", release, setRelease, 100)}
            <label className="field">
              <span className="lbl">触发动作</span>
              <select
                className="inp"
                value={action}
                onChange={(event) =>
                  setAction(
                    event.target.value === "cap_total_risk_weight"
                      ? "cap_total_risk_weight"
                      : "block_new_positions",
                  )
                }
              >
                <option value="block_new_positions">停止开新仓</option>
                <option value="cap_total_risk_weight">降低总仓位</option>
              </select>
            </label>
            {action === "cap_total_risk_weight" ? number("降仓上限（%）", cap, setCap, 100) : null}
          </>
        ) : null}
      </fieldset>
      {error ? (
        <p role="alert" className="crit-text">
          {error}
        </p>
      ) : null}
      <Button type="submit" variant="primary" disabled={locked}>
        保存规则
      </Button>
    </form>
  );
}
