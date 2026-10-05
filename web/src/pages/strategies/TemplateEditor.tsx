import { useRef, useState } from "react";
import { Button, Tip } from "@/ui";
import type { ParameterValue } from "@/ui/ParamControl";
import { TemplateParamControl } from "./TemplateParamControl";
import { ACTION_COPY, REBALANCE_COPY, TemplateRulesSummary } from "./TemplateRules";
import type { SaveTemplate, TemplateDetail, TemplateRules, TemplateSources } from "./templateApi";
import { decimalRate, positiveRate, rateValue } from "./templateValues";

const STEPS = ["入场", "退出", "仓位与调仓", "确认"];
type Condition = Extract<TemplateRules["entry"], { kind: "conditions" }>["conditions"][number];
type ExitKey = "stop_loss" | "take_profit" | "trailing_profit";

function initialCondition(sources: TemplateSources, key?: string): Condition {
  const choice = sources.conditions.find((item) => item.key === key) ?? sources.conditions[0];
  return {
    key: choice?.key ?? "not_st",
    args: Object.fromEntries(
      choice?.block.parameters.map((parameter) => [
        parameter.key,
        parameter.key === "period" && parameter.initial != null
          ? Number(parameter.initial)
          : parameter.initial,
      ]) ?? [],
    ),
  };
}

function defaultRules(sources: TemplateSources): TemplateRules {
  return {
    template_contract: "strategy-template/v1",
    entry: { kind: "conditions", conditions: [initialCondition(sources)] },
    exit: {},
    weight_rule: {
      method: "equal",
      max_positions: 10,
      max_stock_weight: "0.1",
      max_industry_weight: null,
      cash_reserve: "0",
      min_target_amount: "0",
    },
    rebalance_rule: { kind: "daily", every_n_days: null },
    index_filter: null,
  };
}

function controlValue(value: unknown): ParameterValue {
  return typeof value === "string" || typeof value === "number"
    ? value
    : Array.isArray(value) && value.every((item) => typeof item === "string")
      ? value
      : null;
}

export function TemplateEditor({
  sources,
  initial,
  generation,
  locked,
  onSave,
}: {
  sources: TemplateSources;
  initial?: TemplateDetail;
  generation: string;
  locked: boolean;
  onSave: (body: SaveTemplate) => void;
}) {
  const [step, setStep] = useState(0);
  const [name, setName] = useState(initial?.name ?? "");
  const [note, setNote] = useState("");
  const [rules, setRules] = useState<TemplateRules>(() => initial?.rules ?? defaultRules(sources));
  const [error, setError] = useState<string | null>(null);
  const form = useRef<HTMLFormElement>(null);
  const entry = rules.entry;
  const exit = rules.exit ?? {};
  const weight = rules.weight_rule;
  const indexFilter = rules.index_filter;
  const sourceReady =
    entry.kind === "pool"
      ? sources.pools.some(
          (item) =>
            item.pool_key === entry.pool_key &&
            item.version === entry.version &&
            item.body_hash === entry.body_hash,
        )
      : entry.kind === "signal"
        ? sources.signals.some(
            (item) =>
              item.strategy_id === entry.strategy_id &&
              item.version === entry.version &&
              item.source_hash === entry.source_hash &&
              item.actions.includes(entry.action),
          )
        : entry.conditions.length > 0 &&
          entry.conditions.every((condition) =>
            sources.conditions.some((item) => item.key === condition.key),
          );

  function changeEntry(kind: TemplateRules["entry"]["kind"]): void {
    const pool = sources.pools[0];
    const signal = sources.signals[0];
    if (kind === "conditions")
      setRules({ ...rules, entry: { kind, conditions: [initialCondition(sources)] } });
    if (kind === "pool" && pool)
      setRules({
        ...rules,
        entry: { kind, pool_key: pool.pool_key, version: pool.version, body_hash: pool.body_hash },
      });
    if (kind === "signal" && signal)
      setRules({
        ...rules,
        entry: {
          kind,
          strategy_id: signal.strategy_id,
          version: signal.version,
          source_hash: signal.source_hash,
          action: signal.actions[0] as Extract<
            TemplateRules["entry"],
            { kind: "signal" }
          >["action"],
        },
      });
  }

  function updateCondition(index: number, next: Condition): void {
    if (entry.kind === "conditions")
      setRules({
        ...rules,
        entry: {
          ...entry,
          conditions: entry.conditions.map((condition, at) => (at === index ? next : condition)),
        },
      });
  }

  function validateStep(): boolean {
    if (
      !form.current?.reportValidity() ||
      !name.trim() ||
      (initial && !note.trim()) ||
      !sourceReady
    ) {
      setError("请补全当前设置。");
      return false;
    }
    if (
      (["stop_loss", "take_profit", "trailing_profit"] as const).some(
        (key) => exit[key] != null && !positiveRate(exit[key]),
      ) ||
      !positiveRate(weight.max_stock_weight ?? 1) ||
      (weight.max_industry_weight != null && !positiveRate(weight.max_industry_weight))
    ) {
      setError("启用的比例须大于零。");
      return false;
    }
    setError(null);
    return true;
  }

  function save(): void {
    if (!validateStep() || locked) return;
    const body: SaveTemplate = {
      kind: "save_strategy_template",
      command_id: crypto.randomUUID(),
      requested_at: new Date().toISOString(),
      generation_id: generation,
      strategy_id: initial?.strategy_id ?? null,
      expected_head: initial?.current_head ?? null,
      name: name.trim(),
      change_note: note.trim(),
      rules,
    };
    if (new TextEncoder().encode(JSON.stringify(body)).length > 32768) {
      setError("策略内容过长，请减少条件。");
      return;
    }
    onSave(body);
  }

  return (
    <form
      ref={form}
      className="template-editor"
      onSubmit={(event) => {
        event.preventDefault();
        if (step === 3) save();
        else if (validateStep()) setStep(step + 1);
      }}
    >
      <ol className="template-steps" aria-label="新建步骤">
        {STEPS.map((label, at) => (
          <li key={label} aria-current={step === at ? "step" : undefined}>
            <span>{at + 1}</span>
            {label}
          </li>
        ))}
      </ol>
      <fieldset disabled={locked} className="template-fields">
        {step === 0 ? (
          <>
            <label className="field">
              <span className="lbl">策略名称</span>
              <input
                className="inp"
                value={name}
                required
                maxLength={80}
                onChange={(event) => setName(event.target.value)}
              />
            </label>
            {initial ? (
              <label className="field">
                <span className="lbl">改动说明</span>
                <input
                  className="inp"
                  value={note}
                  required
                  maxLength={1024}
                  onChange={(event) => setNote(event.target.value)}
                />
              </label>
            ) : null}
            <label className="field">
              <span className="lbl">入场方式</span>
              <select
                className="inp"
                value={entry.kind}
                onChange={(event) =>
                  changeEntry(event.target.value as TemplateRules["entry"]["kind"])
                }
              >
                <option value="conditions">筛选条件</option>
                <option value="pool" disabled={!sources.pools.length}>
                  股票池{!sources.pools.length ? "（无可用来源）" : ""}
                </option>
                <option value="signal" disabled={!sources.signals.length}>
                  策略信号{!sources.signals.length ? "（无可用来源）" : ""}
                </option>
              </select>
            </label>
            <h3>
              入场条件{" "}
              <Tip content="所有筛选条件须同时满足。池子和信号使用所选精确版本。">
                <span className="screen-help" role="img" aria-label="入场条件说明">
                  ?
                </span>
              </Tip>
            </h3>
            {entry.kind === "pool" ? (
              <label className="field">
                <span className="lbl">股票池</span>
                <select
                  className="inp"
                  value={`${entry.pool_key}:${entry.version}`}
                  onChange={(event) => {
                    const selected = sources.pools.find(
                      (item) => `${item.pool_key}:${item.version}` === event.target.value,
                    );
                    if (selected)
                      setRules({
                        ...rules,
                        entry: {
                          kind: "pool",
                          pool_key: selected.pool_key,
                          version: selected.version,
                          body_hash: selected.body_hash,
                        },
                      });
                  }}
                >
                  {sources.pools.map((item) => (
                    <option
                      key={`${item.pool_key}:${item.version}`}
                      value={`${item.pool_key}:${item.version}`}
                    >
                      {item.name} · 第 {item.version} 版
                    </option>
                  ))}
                </select>
              </label>
            ) : null}
            {entry.kind === "signal" ? (
              <>
                <label className="field">
                  <span className="lbl">策略信号</span>
                  <select
                    className="inp"
                    value={`${entry.strategy_id}:${entry.version}`}
                    onChange={(event) => {
                      const selected = sources.signals.find(
                        (item) => `${item.strategy_id}:${item.version}` === event.target.value,
                      );
                      if (selected)
                        setRules({
                          ...rules,
                          entry: {
                            kind: "signal",
                            strategy_id: selected.strategy_id,
                            version: selected.version,
                            source_hash: selected.source_hash,
                            action: selected.actions[0] as typeof entry.action,
                          },
                        });
                    }}
                  >
                    {sources.signals.map((item) => (
                      <option
                        key={`${item.strategy_id}:${item.version}`}
                        value={`${item.strategy_id}:${item.version}`}
                      >
                        {item.name} · 第 {item.version} 版
                      </option>
                    ))}
                  </select>
                </label>
                <label className="field">
                  <span className="lbl">信号动作</span>
                  <select
                    className="inp"
                    value={entry.action}
                    onChange={(event) =>
                      setRules({
                        ...rules,
                        entry: { ...entry, action: event.target.value as typeof entry.action },
                      })
                    }
                  >
                    {sources.signals
                      .find(
                        (item) =>
                          item.strategy_id === entry.strategy_id && item.version === entry.version,
                      )
                      ?.actions.map((action) => (
                        <option key={action} value={action}>
                          {ACTION_COPY[action as keyof typeof ACTION_COPY] ?? "其他动作"}
                        </option>
                      ))}
                  </select>
                </label>
              </>
            ) : null}
            {entry.kind === "conditions" ? (
              <>
                {entry.conditions.map((condition, index) => {
                  const block = sources.conditions.find(
                    (item) => item.key === condition.key,
                  )?.block;
                  return (
                    // biome-ignore lint/suspicious/noArrayIndexKey: Repeated conditions are valid; these controls have no local row state.
                    <section className="template-condition" key={index}>
                      <div className="template-condition-head">
                        <label className="field">
                          <span className="lbl">条件 {index + 1}</span>
                          <select
                            className="inp"
                            value={condition.key}
                            onChange={(event) =>
                              updateCondition(index, initialCondition(sources, event.target.value))
                            }
                          >
                            {sources.conditions.map((item) => (
                              <option key={item.key} value={item.key}>
                                {item.label}
                              </option>
                            ))}
                          </select>
                        </label>
                        <Button
                          size="sm"
                          disabledReason={
                            entry.conditions.length === 1 ? "至少保留一个条件" : undefined
                          }
                          onClick={() =>
                            setRules({
                              ...rules,
                              entry: {
                                ...entry,
                                conditions: entry.conditions.filter((_, at) => at !== index),
                              },
                            })
                          }
                        >
                          移除条件 {index + 1}
                        </Button>
                      </div>
                      <div className="template-param-grid">
                        {block?.parameters.map((parameter) => (
                          <TemplateParamControl
                            key={parameter.key}
                            parameter={parameter}
                            value={controlValue(condition.args?.[parameter.key])}
                            onChange={(value) =>
                              updateCondition(index, {
                                ...condition,
                                args: {
                                  ...condition.args,
                                  [parameter.key]:
                                    parameter.key === "period" && value !== null
                                      ? Number(value)
                                      : value,
                                },
                              })
                            }
                          />
                        ))}
                      </div>
                    </section>
                  );
                })}
                <Button
                  size="sm"
                  disabledReason={entry.conditions.length >= 26 ? "最多 26 个条件" : undefined}
                  onClick={() =>
                    setRules({
                      ...rules,
                      entry: {
                        ...entry,
                        conditions: [...entry.conditions, initialCondition(sources)],
                      },
                    })
                  }
                >
                  添加条件
                </Button>
              </>
            ) : null}
            {!sourceReady ? (
              <p className="crit-text" role="alert">
                原入场来源不可用，请重新选择。
              </p>
            ) : null}
          </>
        ) : null}
        {step === 1 ? (
          <>
            <h3>退出条件</h3>
            {(
              [
                ["stop_loss", "止损", "0.1", 100],
                ["take_profit", "止盈", "0.2", 1000],
                ["trailing_profit", "移动止盈", "0.1", 100],
              ] as const
            ).map(([key, label, initialRate, maximum]) => (
              <div className="template-exit" key={key}>
                <label className="template-check">
                  <input
                    type="checkbox"
                    checked={exit[key] != null}
                    onChange={(event) =>
                      setRules({
                        ...rules,
                        exit: { ...exit, [key]: event.target.checked ? initialRate : null },
                      })
                    }
                  />
                  {label}
                </label>
                {exit[key] != null ? (
                  <label className="field">
                    <span className="lbl">{label}幅度（%）</span>
                    <input
                      className="inp num"
                      type="number"
                      min="0"
                      max={maximum}
                      step="any"
                      required
                      value={rateValue(exit[key])}
                      onChange={(event) =>
                        setRules({
                          ...rules,
                          exit: { ...exit, [key as ExitKey]: decimalRate(event.target.value) },
                        })
                      }
                    />
                  </label>
                ) : null}
              </div>
            ))}
            <div className="template-exit">
              <label className="template-check">
                <input
                  type="checkbox"
                  checked={exit.max_holding_days != null}
                  onChange={(event) =>
                    setRules({
                      ...rules,
                      exit: { ...exit, max_holding_days: event.target.checked ? 5 : null },
                    })
                  }
                />
                持有上限
              </label>
              {exit.max_holding_days != null ? (
                <label className="field">
                  <span className="lbl">持有上限（交易日）</span>
                  <input
                    className="inp num"
                    type="number"
                    min={1}
                    max={2520}
                    step={1}
                    required
                    value={exit.max_holding_days}
                    onChange={(event) =>
                      setRules({
                        ...rules,
                        exit: { ...exit, max_holding_days: Number(event.target.value) },
                      })
                    }
                  />
                </label>
              ) : null}
            </div>
            <div className="template-exit">
              <div className="template-check">
                <label className="template-check">
                  <input
                    type="checkbox"
                    checked={exit.exit_time != null}
                    onChange={(event) =>
                      setRules({
                        ...rules,
                        exit: { ...exit, exit_time: event.target.checked ? "14:50" : null },
                      })
                    }
                  />
                  定时退出
                </label>
                <Tip content="使用真实分钟价格。缺少价格、交易状态或观测时间时不能运行。">
                  <span role="img" aria-label="定时退出说明" className="screen-help">
                    ?
                  </span>
                </Tip>
              </div>
              {exit.exit_time != null ? (
                <label className="field">
                  <span className="lbl">退出时间</span>
                  <input
                    className="inp num"
                    type="time"
                    step={60}
                    min="09:31"
                    max="15:00"
                    required
                    value={exit.exit_time}
                    onChange={(event) =>
                      setRules({ ...rules, exit: { ...exit, exit_time: event.target.value } })
                    }
                  />
                </label>
              ) : null}
            </div>
          </>
        ) : null}
        {step === 2 ? (
          <>
            <h3>仓位与调仓</h3>
            <div className="template-param-grid">
              <label className="field">
                <span className="lbl">分配方式</span>
                <select
                  className="inp"
                  value={weight.method ?? "equal"}
                  onChange={(event) =>
                    setRules({
                      ...rules,
                      weight_rule: {
                        ...weight,
                        method: event.target.value as "equal" | "rank_score",
                      },
                    })
                  }
                >
                  <option value="equal">等权</option>
                  <option value="rank_score">按排名得分</option>
                </select>
              </label>
              <label className="field">
                <span className="lbl">持仓上限（只）</span>
                <input
                  className="inp num"
                  type="number"
                  min={1}
                  max={500}
                  step={1}
                  required
                  value={weight.max_positions}
                  onChange={(event) =>
                    setRules({
                      ...rules,
                      weight_rule: { ...weight, max_positions: Number(event.target.value) },
                    })
                  }
                />
              </label>
              <label className="field">
                <span className="lbl">单股上限（%）</span>
                <input
                  className="inp num"
                  type="number"
                  min="0"
                  max={100}
                  step="any"
                  required
                  value={rateValue(weight.max_stock_weight ?? 1)}
                  onChange={(event) =>
                    setRules({
                      ...rules,
                      weight_rule: { ...weight, max_stock_weight: decimalRate(event.target.value) },
                    })
                  }
                />
              </label>
              <label className="field">
                <span className="lbl">现金保留（%）</span>
                <input
                  className="inp num"
                  type="number"
                  min={0}
                  max={100}
                  step="any"
                  required
                  value={rateValue(weight.cash_reserve ?? 0)}
                  onChange={(event) =>
                    setRules({
                      ...rules,
                      weight_rule: { ...weight, cash_reserve: decimalRate(event.target.value) },
                    })
                  }
                />
              </label>
              <label className="field">
                <span className="lbl">行业上限（%，空为不限）</span>
                <input
                  className="inp num"
                  type="number"
                  min="0"
                  max={100}
                  step="any"
                  value={rateValue(weight.max_industry_weight)}
                  onChange={(event) =>
                    setRules({
                      ...rules,
                      weight_rule: {
                        ...weight,
                        max_industry_weight:
                          event.target.value === "" ? null : decimalRate(event.target.value),
                      },
                    })
                  }
                />
              </label>
              <label className="field">
                <span className="lbl">最小目标金额（元）</span>
                <input
                  className="inp num"
                  type="number"
                  min={0}
                  max="1000000000000"
                  step="0.01"
                  required
                  value={weight.min_target_amount ?? "0"}
                  onChange={(event) =>
                    setRules({
                      ...rules,
                      weight_rule: { ...weight, min_target_amount: event.target.value },
                    })
                  }
                />
              </label>
              <label className="field">
                <span className="lbl">调仓频率</span>
                <select
                  className="inp"
                  value={rules.rebalance_rule.kind}
                  onChange={(event) => {
                    const kind = event.target.value as TemplateRules["rebalance_rule"]["kind"];
                    setRules({
                      ...rules,
                      rebalance_rule: { kind, every_n_days: kind === "every_n" ? 5 : null },
                    });
                  }}
                >
                  {Object.entries(REBALANCE_COPY).map(([value, label]) => (
                    <option value={value} key={value}>
                      {label}
                    </option>
                  ))}
                </select>
              </label>
              {rules.rebalance_rule.kind === "every_n" ? (
                <label className="field">
                  <span className="lbl">调仓间隔（交易日）</span>
                  <input
                    className="inp num"
                    type="number"
                    min={1}
                    max={252}
                    step={1}
                    required
                    value={rules.rebalance_rule.every_n_days ?? 5}
                    onChange={(event) =>
                      setRules({
                        ...rules,
                        rebalance_rule: {
                          kind: "every_n",
                          every_n_days: Number(event.target.value),
                        },
                      })
                    }
                  />
                </label>
              ) : null}
            </div>
            <div className="template-check">
              <label className="template-check">
                <input
                  type="checkbox"
                  checked={indexFilter != null}
                  onChange={(event) =>
                    setRules({
                      ...rules,
                      index_filter: event.target.checked
                        ? { benchmark_code: "000300.SH", ma_days: 20, direction: "above" }
                        : null,
                    })
                  }
                />
                指数过滤
              </label>
              <Tip content="使用决策前已观测的指数收盘价与交易日均线。">
                <span className="screen-help" role="img" aria-label="指数过滤说明">
                  ?
                </span>
              </Tip>
            </div>
            {indexFilter ? (
              <div className="template-param-grid">
                <label className="field">
                  <span className="lbl">指数代码</span>
                  <select
                    className="inp mono"
                    value={indexFilter.benchmark_code}
                    onChange={(event) =>
                      setRules({
                        ...rules,
                        index_filter: {
                          ...indexFilter,
                          benchmark_code: event.target.value,
                        },
                      })
                    }
                  >
                    {[
                      "000300.SH",
                      "000905.SH",
                      "000852.SH",
                      "000001.SH",
                      "399001.SZ",
                      "399006.SZ",
                    ].map((code) => (
                      <option value={code} key={code}>
                        {code}
                      </option>
                    ))}
                  </select>
                </label>
                <label className="field">
                  <span className="lbl">均线周期（交易日）</span>
                  <input
                    className="inp num"
                    type="number"
                    min={1}
                    max={250}
                    step={1}
                    required
                    value={indexFilter.ma_days}
                    onChange={(event) =>
                      setRules({
                        ...rules,
                        index_filter: {
                          ...indexFilter,
                          ma_days: Number(event.target.value),
                        },
                      })
                    }
                  />
                </label>
                <label className="field">
                  <span className="lbl">指数方向</span>
                  <select
                    className="inp"
                    value={indexFilter.direction}
                    onChange={(event) =>
                      setRules({
                        ...rules,
                        index_filter: {
                          ...indexFilter,
                          direction: event.target.value as "above" | "below",
                        },
                      })
                    }
                  >
                    <option value="above">高于均线</option>
                    <option value="below">低于均线</option>
                  </select>
                </label>
              </div>
            ) : null}
          </>
        ) : null}
        {step === 3 ? (
          <>
            <h3>保存前确认</h3>
            <p>
              <strong>{name}</strong>
              {initial ? ` · 新增第 ${initial.current_head.version + 1} 版` : " · 第 1 版"}
            </p>
            {initial ? <p>{note}</p> : null}
            <TemplateRulesSummary rules={rules} sources={sources} />
          </>
        ) : null}
      </fieldset>
      {error ? (
        <p className="crit-text" role="alert">
          {error}
        </p>
      ) : null}
      <div className="template-form-actions">
        <Button
          disabled={locked || step === 0}
          onClick={() => {
            setError(null);
            setStep(step - 1);
          }}
        >
          上一步
        </Button>
        <Button
          type="submit"
          variant="primary"
          disabledReason={
            locked ? "先查看这次操作的结果" : !sourceReady ? "请重新选择入场来源" : undefined
          }
        >
          {step === 3 ? (initial ? "保存新版本" : "保存策略") : "下一步"}
        </Button>
      </div>
    </form>
  );
}
