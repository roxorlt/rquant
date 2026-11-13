import { useEffect, useMemo, useRef, useState } from "react";
import {
  type CustomBlockItem,
  type CustomBlockSaveRequest,
  type PoolItem,
  type RuleCatalogItem,
  type RuleParamSpec,
  type ScreenerBlock,
  type ScreenerFunnelStep,
  type ScreenerUpstream,
  type UpstreamOptionItem,
  useDeleteCustomBlock,
  useSaveCustomBlock,
  useScreenerFromText,
  useTranslateTdx,
} from "@/api/endpoints";
import { presetLabel } from "@/copy/publicText";
import { Button, FailureNotice, useToast } from "@/ui";
import { failureCopy } from "@/ui/failure";
import { defaultBlockForRule, formatClientBlockLabel, newBlockId } from "./Builder";
import { FieldPicker } from "./FieldPicker";

function makeSlug(name: string): string {
  const ascii = name
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "");
  const suffix = Math.random().toString(36).slice(2, 7);
  return ascii ? `${ascii.slice(0, 28)}-${suffix}` : `blk-${Date.now().toString(36)}-${suffix}`;
}

function refDisplayLabel(ref: string, options: readonly UpstreamOptionItem[]): string {
  const found = options.find((o) => o.ref === ref);
  if (found) return found.label;
  if (ref === "watchlist") return "自选股";
  if (ref.startsWith("preset:")) return `预设 · ${presetLabel(ref.slice(7))}`;
  if (ref.startsWith("pool:")) return `池子 · ${ref.slice(5)}`;
  return ref;
}

export function UpstreamScopeBar({
  upstream,
  onChange,
  presets,
  pools,
  upstreamOptions,
  allMarketCount,
}: {
  upstream: ScreenerUpstream;
  onChange: (next: ScreenerUpstream) => void;
  presets: readonly string[];
  pools: readonly PoolItem[];
  upstreamOptions: readonly UpstreamOptionItem[];
  allMarketCount: number;
}) {
  const userPools = useMemo(() => pools.filter((p) => p.name !== "自选股"), [pools]);
  const setChoices = useMemo<UpstreamOptionItem[]>(() => {
    if (upstreamOptions.length > 0) return [...upstreamOptions];
    const out: UpstreamOptionItem[] = presets.map((p) => ({
      ref: `preset:${p}`,
      kind: "preset",
      name: p,
      label: `预设 · ${presetLabel(p)}`,
      count: 0,
    }));
    for (const p of userPools) {
      out.push({
        ref: `pool:${p.name}`,
        kind: "pool",
        name: p.name,
        label: `池子 · ${p.name}`,
        count: p.members.length,
      });
    }
    out.push({ ref: "watchlist", kind: "watchlist", name: "自选股", label: "自选股", count: 0 });
    return out;
  }, [upstreamOptions, presets, userPools]);

  const [setOp, setSetOp] = useState<"include" | "intersect" | "exclude">("exclude");
  const [targetRef, setTargetRef] = useState<string>("watchlist");
  useEffect(() => {
    if (setChoices.length > 0 && !setChoices.some((c) => c.ref === targetRef)) {
      setTargetRef(setChoices[0]?.ref ?? "watchlist");
    }
  }, [setChoices, targetRef]);

  const includeList = upstream.include_pools ?? [];
  const intersectList = upstream.intersect_pools ?? [];
  const excludeList = upstream.exclude_pools ?? [];

  const selectBase = (base: ScreenerUpstream["base"], sub?: string) => {
    if (base === "preset") {
      onChange({ ...upstream, base: "preset", preset: sub ?? presets[0] ?? null, pool: null });
    } else if (base === "pool") {
      onChange({
        ...upstream,
        base: "pool",
        pool: sub ?? upstream.pool ?? userPools[0]?.name ?? null,
        preset: null,
      });
    } else {
      onChange({ ...upstream, base, preset: null, pool: null });
    }
  };

  const addSetModifier = () => {
    if (!targetRef) return;
    if (setOp === "include" && !includeList.includes(targetRef)) {
      onChange({ ...upstream, include_pools: [...includeList, targetRef] });
    } else if (setOp === "intersect" && !intersectList.includes(targetRef)) {
      onChange({ ...upstream, intersect_pools: [...intersectList, targetRef] });
    } else if (setOp === "exclude" && !excludeList.includes(targetRef)) {
      onChange({ ...upstream, exclude_pools: [...excludeList, targetRef] });
    }
  };

  return (
    <section className="wb-upstream" aria-label="输入股票范围">
      <div className="wb-upstream-main">
        <span className="wb-upstream-k">输入范围</span>
        <fieldset className="wb-pills" aria-label="选股范围">
          <button
            type="button"
            className={`wb-pill${upstream.base === "presets" ? " on" : ""}`}
            aria-pressed={upstream.base === "presets"}
            onClick={() => selectBase("presets")}
          >
            今日候选池
          </button>
          <button
            type="button"
            className={`wb-pill${upstream.base === "all_market" ? " on" : ""}`}
            aria-pressed={upstream.base === "all_market"}
            onClick={() => selectBase("all_market")}
          >
            全市场 A 股{allMarketCount > 0 ? ` (${allMarketCount})` : ""}
          </button>
          {presets.map((p) => {
            const active = upstream.base === "preset" && upstream.preset === p;
            return (
              <button
                key={p}
                type="button"
                className={`wb-pill${active ? " on" : ""}`}
                aria-pressed={active}
                onClick={() => selectBase("preset", p)}
              >
                {presetLabel(p)}
              </button>
            );
          })}
          {userPools.length > 0 ? (
            <button
              type="button"
              className={`wb-pill${upstream.base === "pool" ? " on" : ""}`}
              aria-pressed={upstream.base === "pool"}
              onClick={() => selectBase("pool")}
            >
              我的池子
            </button>
          ) : null}
          <button
            type="button"
            className={`wb-pill${upstream.base === "watchlist" ? " on" : ""}`}
            aria-pressed={upstream.base === "watchlist"}
            onClick={() => selectBase("watchlist")}
          >
            自选股
          </button>
        </fieldset>
        {upstream.base === "pool" && userPools.length > 0 ? (
          <select
            className="select wb-pool-select"
            aria-label="选择输入池子"
            value={upstream.pool ?? userPools[0]?.name ?? ""}
            onChange={(e) => selectBase("pool", e.target.value)}
          >
            {userPools.map((p) => (
              <option key={p.name} value={p.name}>
                {p.name} ({p.members.length})
              </option>
            ))}
          </select>
        ) : null}
      </div>

      <div className="wb-upstream-ops">
        <span className="hint">级联集合</span>
        {includeList.map((ref) => (
          <span key={`inc-${ref}`} className="wb-set-chip inc">
            + 并集 · {refDisplayLabel(ref, setChoices)}
            <button
              type="button"
              aria-label={`移除并集 ${ref}`}
              onClick={() =>
                onChange({ ...upstream, include_pools: includeList.filter((r) => r !== ref) })
              }
            >
              ×
            </button>
          </span>
        ))}
        {intersectList.map((ref) => (
          <span key={`int-${ref}`} className="wb-set-chip int">
            ∩ 交集 · {refDisplayLabel(ref, setChoices)}
            <button
              type="button"
              aria-label={`移除交集 ${ref}`}
              onClick={() =>
                onChange({ ...upstream, intersect_pools: intersectList.filter((r) => r !== ref) })
              }
            >
              ×
            </button>
          </span>
        ))}
        {excludeList.map((ref) => (
          <span key={`exc-${ref}`} className="wb-set-chip exc">
            − 排除 · {refDisplayLabel(ref, setChoices)}
            <button
              type="button"
              aria-label={`移除排除 ${ref}`}
              onClick={() =>
                onChange({ ...upstream, exclude_pools: excludeList.filter((r) => r !== ref) })
              }
            >
              ×
            </button>
          </span>
        ))}
        {setChoices.length > 0 ? (
          <span className="wb-set-adder">
            <select
              className="select wb-sm-select"
              aria-label="集合运算类型"
              value={setOp}
              onChange={(e) => setSetOp(e.target.value as "include" | "intersect" | "exclude")}
            >
              <option value="include">+ 并集叠加</option>
              <option value="intersect">∩ 交集限定</option>
              <option value="exclude">− 排除剔除</option>
            </select>
            <select
              className="select wb-sm-select"
              aria-label="集合目标池子"
              value={targetRef}
              onChange={(e) => setTargetRef(e.target.value)}
            >
              {setChoices.map((c) => (
                <option key={c.ref} value={c.ref}>
                  {c.label}
                  {c.count > 0 ? ` (${c.count})` : ""}
                </option>
              ))}
            </select>
            <Button size="sm" variant="ghost" onClick={addSetModifier}>
              加入范围
            </Button>
          </span>
        ) : null}
      </div>
    </section>
  );
}

export function NlWhiteBoxBar({
  onAppendBlocks,
}: {
  onAppendBlocks: (blocks: ScreenerBlock[]) => void;
}) {
  const [text, setText] = useState("");
  const ai = useScreenerFromText();

  const submit = () => {
    const trimmed = text.trim();
    if (trimmed.length < 2 || ai.isPending) return;
    ai.mutate(trimmed, {
      onSuccess: (res) => {
        if (res.blocks && res.blocks.length > 0) {
          onAppendBlocks(
            res.blocks.map((b) => ({
              ...b,
              id: b.id || newBlockId(),
              enabled: true,
            })),
          );
        }
      },
    });
  };

  const compiledCount = ai.data?.blocks?.length ?? 0;
  const ok = compiledCount > 0 || Boolean(ai.data?.expression);

  return (
    <div className="wb-nl">
      <div className="formula-ai">
        <input
          aria-label="一句话条件"
          placeholder="一句话白盒生成积木，如：排除ST和北交所，昨日首板且流通市值小于80亿 / 站上20日线且放量一倍"
          value={text}
          onChange={(e) => setText(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter") submit();
          }}
        />
        <Button variant="quiet" disabled={ai.isPending || text.trim().length < 2} onClick={submit}>
          {ai.isPending ? "生成中…" : "一句话生成"}
        </Button>
      </div>
      {ai.error ? (
        <FailureNotice copy={failureCopy("条件", "生成", ai.error)} onRetry={submit} />
      ) : null}
      {ai.data ? (
        <output className="formula-result" aria-label="生成结果">
          <div className="formula-result-h">
            <span className="formula-result-label">生成结果</span>
            <span className={ok ? "st ok" : "st warn"}>
              {ok
                ? compiledCount > 0
                  ? `已加入 ${compiledCount} 个白盒积木 · 校验通过`
                  : "校验通过"
                : "未能识别"}
            </span>
            {ai.data.summary ? <span className="hint">{ai.data.summary}</span> : null}
          </div>
          <div className="formula-result-expr">
            {ai.data.expression ?? ai.data.error ?? ai.data.summary}
          </div>
        </output>
      ) : null}
    </div>
  );
}

function OffsetSelect({
  value,
  onChange,
  label,
}: {
  value: number;
  onChange: (n: number) => void;
  label: string;
}) {
  return (
    <select
      className="select wb-param-select"
      aria-label={label}
      value={String(value)}
      onChange={(e) => onChange(Number(e.target.value))}
    >
      <option value="0">今日 (T)</option>
      <option value="1">昨日 (T-1)</option>
      <option value="2">前日 (T-2)</option>
      <option value="3">T-3</option>
      <option value="5">T-5</option>
    </select>
  );
}

function RuleParamInputs({
  rule,
  args,
  onChange,
}: {
  rule: RuleCatalogItem;
  args: Record<string, unknown>;
  onChange: (nextArgs: Record<string, unknown>) => void;
}) {
  const params = rule.params ?? [];
  if (params.length === 0) return null;

  const setArg = (key: string, val: unknown) => onChange({ ...args, [key]: val });

  return (
    <span className="wb-rule-params">
      {params.map((p: RuleParamSpec) => {
        const cur = args[p.name] ?? p.default;
        if (p.name === "offset") {
          return (
            <OffsetSelect
              key={p.name}
              label={`${rule.title}-${p.label}`}
              value={typeof cur === "number" ? cur : 0}
              onChange={(n) => setArg(p.name, n)}
            />
          );
        }
        if (p.kind === "boards") {
          const selected = Array.isArray(cur) ? (cur as string[]) : ["main", "gem"];
          const opts = p.options ?? [
            { value: "main", label: "主板" },
            { value: "gem", label: "创业板" },
            { value: "star", label: "科创板" },
            { value: "bj", label: "北交所" },
          ];
          return (
            <span key={p.name} className="wb-board-pills">
              {opts.map((opt) => {
                const val = opt.value ?? "";
                const on = selected.includes(val);
                return (
                  <button
                    key={val}
                    type="button"
                    className={`wb-mini-pill${on ? " on" : ""}`}
                    aria-pressed={on}
                    onClick={() => {
                      const next = on ? selected.filter((x) => x !== val) : [...selected, val];
                      if (next.length > 0) setArg(p.name, next);
                    }}
                  >
                    {opt.label}
                  </button>
                );
              })}
            </span>
          );
        }
        if ((p.kind === "ma_line" || p.kind === "operand") && (p.options?.length ?? 0) > 0) {
          const strVal = String(cur ?? p.options?.[0]?.value ?? "");
          return (
            <label key={p.name} className="wb-param-lbl">
              <span>{p.label}</span>
              <select
                className="select wb-param-select"
                aria-label={`${rule.title}-${p.label}`}
                value={strVal}
                onChange={(e) => setArg(p.name, e.target.value)}
              >
                {(p.options ?? []).map((opt) => (
                  <option key={opt.value} value={opt.value}>
                    {opt.label}
                  </option>
                ))}
              </select>
            </label>
          );
        }
        return (
          <label key={p.name} className="wb-param-lbl">
            <span>{p.label}</span>
            <input
              aria-label={`${rule.title}-${p.label}`}
              type="number"
              min={p.min ?? undefined}
              max={p.max ?? undefined}
              step={p.step ?? (p.kind === "int" ? 1 : "any")}
              value={typeof cur === "number" ? cur : Number(cur ?? 0)}
              onChange={(e) =>
                setArg(
                  p.name,
                  p.kind === "int" ? Math.round(Number(e.target.value)) : Number(e.target.value),
                )
              }
            />
          </label>
        );
      })}
    </span>
  );
}

function TdxBlockControls({
  formula,
  onChange,
}: {
  formula: string;
  onChange: (next: string) => void;
}) {
  const translate = useTranslateTdx();
  return (
    <span className="wb-tdx-inline">
      <input
        className="mono wb-tdx-in"
        aria-label="积木通达信公式"
        placeholder="例如 C>MA(C,20) AND V>MA(V,5)*2"
        value={formula}
        onChange={(e) => {
          onChange(e.target.value);
          if (translate.data) translate.reset();
        }}
      />
      <Button
        size="sm"
        variant="ghost"
        onClick={() => translate.mutate(formula)}
        disabled={!formula.trim() || translate.isPending}
      >
        校验
      </Button>
      {translate.data ? (
        <span className={translate.data.expression ? "st ok" : "st warn"}>
          {translate.data.expression ? `✓ ${translate.data.expression}` : translate.data.error}
        </span>
      ) : null}
    </span>
  );
}

export function BlockListWorkbench({
  blocks,
  onChange,
  fields,
  rules,
  customBlocks,
}: {
  blocks: ScreenerBlock[];
  onChange: (next: ScreenerBlock[]) => void;
  fields: Record<string, string>;
  rules: readonly RuleCatalogItem[];
  customBlocks: readonly CustomBlockItem[];
}) {
  const toast = useToast();
  const saveBlock = useSaveCustomBlock();
  const deleteBlock = useDeleteCustomBlock();

  const [pickerOpen, setPickerOpen] = useState(false);
  const [pickerCat, setPickerCat] = useState<string>("all");
  const [pickerQuery, setPickerQuery] = useState("");
  const pickerRef = useRef<HTMLDivElement>(null);

  const [saveTarget, setSaveTarget] = useState<{
    mode: "single" | "composite";
    block?: ScreenerBlock;
  } | null>(null);
  const [customName, setCustomName] = useState("");
  const [customDesc, setCustomDesc] = useState("");
  const [saveError, setSaveError] = useState<string | null>(null);

  useEffect(() => {
    if (!pickerOpen) return;
    const close = (e: PointerEvent) => {
      if (!pickerRef.current?.contains(e.target as Node)) setPickerOpen(false);
    };
    document.addEventListener("pointerdown", close);
    return () => document.removeEventListener("pointerdown", close);
  }, [pickerOpen]);

  const ruleMap = useMemo(() => new Map(rules.map((r) => [r.name, r])), [rules]);
  const customMap = useMemo(
    () => new Map(customBlocks.map((b) => [b.block_id, b])),
    [customBlocks],
  );

  const categories = useMemo(() => {
    const seen = new Map<string, string>();
    for (const r of rules) {
      if (!seen.has(r.category)) seen.set(r.category, r.category_label);
    }
    return [...seen.entries()];
  }, [rules]);

  const filteredRules = useMemo(() => {
    const needle = pickerQuery.trim().toLowerCase();
    return rules.filter((r) => {
      if (pickerCat !== "all" && pickerCat !== "custom" && r.category !== pickerCat) return false;
      if (!needle) return true;
      return `${r.name} ${r.title} ${r.description} ${(r.examples ?? []).join(" ")}`
        .toLowerCase()
        .includes(needle);
    });
  }, [rules, pickerCat, pickerQuery]);

  const filteredCustom = useMemo(() => {
    if (pickerCat !== "all" && pickerCat !== "custom") return [];
    const needle = pickerQuery.trim().toLowerCase();
    return customBlocks.filter(
      (c) => !needle || `${c.name} ${c.description} ${c.kind}`.toLowerCase().includes(needle),
    );
  }, [customBlocks, pickerCat, pickerQuery]);

  const updateBlock = (idx: number, patch: Partial<ScreenerBlock>) =>
    onChange(blocks.map((b, j) => (j === idx ? { ...b, ...patch } : b)));

  const appendBlock = (blk: ScreenerBlock) => {
    onChange([...blocks, blk]);
    setPickerOpen(false);
    setPickerQuery("");
  };

  const openSaveSingle = (blk: ScreenerBlock) => {
    setSaveTarget({ mode: "single", block: blk });
    setCustomName(formatClientBlockLabel(blk, fields, customMap, ruleMap).replace(/^★\s*/, ""));
    setCustomDesc("");
    setSaveError(null);
  };

  const openSaveComposite = () => {
    setSaveTarget({ mode: "composite" });
    setCustomName("");
    setCustomDesc(`${blocks.length} 条组合条件`);
    setSaveError(null);
  };

  const confirmSaveCustom = () => {
    const trimmed = customName.trim();
    if (!trimmed || !saveTarget) return;
    let body: CustomBlockSaveRequest;
    if (saveTarget.mode === "single" && saveTarget.block) {
      const b = saveTarget.block;
      if (b.type === "tdx") {
        body = { name: trimmed, description: customDesc, kind: "tdx", spec: { tdx: b.tdx ?? "" } };
      } else if (b.type === "rule") {
        body = {
          name: trimmed,
          description: customDesc,
          kind: "rule",
          spec: { rule_name: b.rule_name, args: b.args ?? {} },
        };
      } else if (b.type === "field") {
        body = {
          name: trimmed,
          description: customDesc,
          kind: "field",
          spec: { field: b.field, op: b.op, value: b.value, value2: b.value2 },
        };
      } else {
        body = {
          name: trimmed,
          description: customDesc,
          kind: "composite",
          spec: { blocks: [b] },
        };
      }
    } else {
      body = {
        name: trimmed,
        description: customDesc,
        kind: "composite",
        spec: { blocks },
      };
    }

    saveBlock.mutate(
      { blockId: makeSlug(trimmed), body },
      {
        onSuccess: () => {
          setSaveTarget(null);
          setCustomName("");
          setSaveError(null);
          toast(`已存入『★ 我的积木』：${trimmed}`);
        },
        onError: (err) => {
          setSaveError(err instanceof Error ? err.message : "保存积木失败，请检查公式或参数");
        },
      },
    );
  };

  return (
    <div className="wb-blocks">
      {blocks.length === 0 ? (
        <p className="cond-none">
          无门槛，输入范围内全部股票进入排名（可点击下方添加积木或用上方一句话生成）
        </p>
      ) : null}

      <div className="wb-block-list">
        {blocks.map((b, i) => {
          const enabled = b.enabled !== false;
          const ruleSpec = b.type === "rule" ? ruleMap.get(b.rule_name ?? "") : undefined;
          const customSpec = b.type === "custom" ? customMap.get(b.custom_id ?? "") : undefined;
          return (
            <div key={b.id || `pos-${i}`} className={`wb-block-row${enabled ? "" : " muted"}`}>
              <input
                type="checkbox"
                className="wb-check"
                aria-label={`启用条件 ${i + 1}`}
                checked={enabled}
                onChange={(e) => updateBlock(i, { enabled: e.target.checked })}
              />
              <span className="mono wb-idx">{i + 1}</span>

              <div className="wb-block-body">
                {b.type === "rule" ? (
                  <>
                    <span className="wb-badge rule">规则</span>
                    <strong className="wb-rule-title">{ruleSpec?.title ?? b.rule_name}</strong>
                    {ruleSpec ? (
                      <RuleParamInputs
                        rule={ruleSpec}
                        args={(b.args ?? {}) as Record<string, unknown>}
                        onChange={(nextArgs) => updateBlock(i, { args: nextArgs })}
                      />
                    ) : null}
                  </>
                ) : b.type === "field" ? (
                  <span className="wb-field-controls">
                    <span className="wb-badge field">指标</span>
                    <FieldPicker
                      label="字段"
                      value={b.field ?? "pct_chg"}
                      fields={fields}
                      onChange={(fld) => updateBlock(i, { field: fld })}
                    />
                    <select
                      className="select wb-op-select"
                      aria-label="比较"
                      value={b.op ?? ">="}
                      onChange={(e) =>
                        updateBlock(i, { op: e.target.value as ScreenerBlock["op"] })
                      }
                    >
                      <option value=">=">≥</option>
                      <option value="<=">≤</option>
                      <option value="between">区间</option>
                      <option value="top_pct">前 N%</option>
                      <option value="bottom_pct">后 N%</option>
                    </select>
                    <input
                      aria-label="阈值"
                      type="number"
                      step="any"
                      value={b.value ?? 0}
                      onChange={(e) => updateBlock(i, { value: Number(e.target.value) })}
                    />
                    {b.op === "between" ? (
                      <>
                        <span className="hint">~</span>
                        <input
                          aria-label="区间上限"
                          type="number"
                          step="any"
                          value={b.value2 ?? b.value ?? 0}
                          onChange={(e) => updateBlock(i, { value2: Number(e.target.value) })}
                        />
                      </>
                    ) : null}
                  </span>
                ) : b.type === "tdx" ? (
                  <>
                    <span className="wb-badge tdx">TDX</span>
                    <TdxBlockControls
                      formula={b.tdx ?? ""}
                      onChange={(next) => updateBlock(i, { tdx: next })}
                    />
                  </>
                ) : (
                  <>
                    <span className="wb-badge custom">★ 我的积木</span>
                    <strong className="wb-rule-title">
                      {customSpec?.name ?? b.custom_id ?? "自定义积木"}
                    </strong>
                    {customSpec?.description ? (
                      <span className="hint">{customSpec.description}</span>
                    ) : null}
                  </>
                )}
              </div>

              <div className="wb-row-actions">
                {b.type !== "custom" ? (
                  <button
                    type="button"
                    className="wb-icon-btn star"
                    title="存为我的积木"
                    aria-label={`存为我的积木 ${i + 1}`}
                    onClick={() => openSaveSingle(b)}
                  >
                    ★
                  </button>
                ) : null}
                <Button
                  size="sm"
                  variant="ghost"
                  aria-label={`删除条件 ${i + 1}`}
                  onClick={() => onChange(blocks.filter((_, j) => j !== i))}
                >
                  ×
                </Button>
              </div>
            </div>
          );
        })}
      </div>

      <div className="wb-toolbar" ref={pickerRef}>
        <Button
          size="sm"
          variant="ghost"
          aria-expanded={pickerOpen}
          onClick={() => setPickerOpen((v) => !v)}
        >
          + 添加条件积木
        </Button>
        <Button
          size="sm"
          variant="ghost"
          onClick={() =>
            appendBlock({
              id: newBlockId(),
              type: "field",
              enabled: true,
              field: "pct_chg",
              op: ">=",
              value: 0,
            })
          }
        >
          + 添加条件
        </Button>
        <Button
          size="sm"
          variant="ghost"
          onClick={() =>
            appendBlock({
              id: newBlockId(),
              type: "tdx",
              enabled: true,
              tdx: "C>MA(C,20)",
              op: ">=",
              value: 0,
            })
          }
        >
          + TDX 公式
        </Button>
        {blocks.length > 0 ? (
          <Button size="sm" variant="quiet" onClick={openSaveComposite}>
            ★ 存为我的积木
          </Button>
        ) : null}

        {pickerOpen ? (
          <div className="wb-picker-pop" role="dialog" aria-label="选股积木库">
            <div className="wb-picker-top">
              <input
                // biome-ignore lint/a11y/noAutofocus: quick search inside popover
                autoFocus
                aria-label="搜索积木"
                placeholder="搜索 26 个内置规则 / 我的积木 / 示例（如：首板、均线、北交、跌停）…"
                value={pickerQuery}
                onChange={(e) => setPickerQuery(e.target.value)}
              />
            </div>
            <div className="wb-picker-cats">
              <button
                type="button"
                className={`wb-mini-pill${pickerCat === "all" ? " on" : ""}`}
                onClick={() => setPickerCat("all")}
              >
                全部 ({rules.length + customBlocks.length})
              </button>
              <button
                type="button"
                className={`wb-mini-pill${pickerCat === "custom" ? " on" : ""}`}
                onClick={() => setPickerCat("custom")}
              >
                ★ 我的积木 ({customBlocks.length})
              </button>
              {categories.map(([cat, lbl]) => (
                <button
                  key={cat}
                  type="button"
                  className={`wb-mini-pill${pickerCat === cat ? " on" : ""}`}
                  onClick={() => setPickerCat(cat)}
                >
                  {lbl}
                </button>
              ))}
            </div>

            <div className="wb-picker-list">
              {filteredCustom.length > 0 ? (
                <div className="wb-picker-sec">
                  <div className="fpick-g">★ 我的积木</div>
                  {filteredCustom.map((cb) => (
                    <div key={cb.block_id} className="wb-picker-custom-item">
                      <button
                        type="button"
                        className="wb-picker-card"
                        onClick={() =>
                          appendBlock({
                            id: newBlockId(),
                            type: "custom",
                            enabled: true,
                            custom_id: cb.block_id,
                            op: ">=",
                            value: 0,
                          })
                        }
                      >
                        <strong>★ {cb.name}</strong>
                        <span className="hint">
                          {cb.description ||
                            (cb.kind === "tdx" ? String(cb.spec?.tdx ?? "") : cb.kind)}
                        </span>
                      </button>
                      <button
                        type="button"
                        className="wb-del-custom"
                        aria-label={`删除积木 ${cb.name}`}
                        onClick={() => deleteBlock.mutate(cb.block_id)}
                      >
                        ×
                      </button>
                    </div>
                  ))}
                </div>
              ) : pickerCat === "custom" ? (
                <p className="hint" style={{ padding: "8px" }}>
                  还没有保存自定义积木。可在任意条件右侧点「★」或点「★
                  存为我的积木」沉淀常用公式/组合。
                </p>
              ) : null}

              {pickerCat !== "custom" ? (
                <div className="wb-picker-sec">
                  <div className="fpick-g">内置规则积木 ({filteredRules.length})</div>
                  <div className="wb-picker-grid">
                    {filteredRules.map((r) => (
                      <button
                        key={r.name}
                        type="button"
                        className="wb-picker-card"
                        onClick={() => appendBlock(defaultBlockForRule(r))}
                      >
                        <div className="wb-picker-card-h">
                          <strong>{r.title}</strong>
                          <span className="wb-cat-tag">{r.category_label}</span>
                        </div>
                        <span className="hint">{r.description}</span>
                      </button>
                    ))}
                  </div>
                </div>
              ) : null}
            </div>
          </div>
        ) : null}
      </div>

      {saveTarget ? (
        <section className="wb-save-custom" aria-label="保存自定义积木面板">
          <span className="wb-badge custom">
            {saveTarget.mode === "single"
              ? "★ 单条存为积木"
              : `★ 组合存为积木 (${blocks.length} 条)`}
          </span>
          <input
            aria-label="自定义积木名称"
            placeholder="积木名称，如：昨日首板滤网 / 站上20日放量"
            value={customName}
            onChange={(e) => setCustomName(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter") confirmSaveCustom();
            }}
          />
          <input
            aria-label="自定义积木说明"
            placeholder="简短备注（可选）"
            value={customDesc}
            onChange={(e) => setCustomDesc(e.target.value)}
          />
          <Button
            size="sm"
            variant="primary"
            disabled={!customName.trim() || saveBlock.isPending}
            onClick={confirmSaveCustom}
          >
            保存积木
          </Button>
          <Button size="sm" variant="ghost" onClick={() => setSaveTarget(null)}>
            取消
          </Button>
          {saveError ? <span className="st warn">{saveError}</span> : null}
        </section>
      ) : null}
    </div>
  );
}

export function FunnelStrip({
  steps,
  activeStep,
  previewStep,
  onSelectStep,
}: {
  steps: readonly ScreenerFunnelStep[];
  activeStep: number;
  previewStep: number | null;
  onSelectStep: (step: number | null) => void;
}) {
  const lastIdx = Math.max(0, steps.length - 1);
  const isPreviewingIntermediate = previewStep !== null && previewStep < lastIdx;

  return (
    <div className="wb-funnel-wrap">
      <ol className="funnel-inline" aria-label="逐条命中">
        {steps.map((s) => {
          const isSelected = s.step === activeStep;
          const isFinal = s.step === lastIdx;
          return (
            <li
              key={`${s.step}-${s.block_id ?? "root"}`}
              className={`${isSelected ? "selected" : ""}${isFinal ? " final" : ""}`}
            >
              <button
                type="button"
                className="funnel-step-btn"
                aria-pressed={isSelected}
                title="点击预览该层剩余股票"
                onClick={() => onSelectStep(s.step === lastIdx ? null : s.step)}
              >
                <span className="fl">{s.label}</span>
                <span className="fv num">{s.remaining}</span>
                {s.step > 0 && (s.dropped ?? 0) > 0 ? (
                  <span className="fd num">-{s.dropped_pct ?? 0}%</span>
                ) : null}
                {s.error ? (
                  <span className="fe" title={s.error}>
                    异常
                  </span>
                ) : null}
              </button>
            </li>
          );
        })}
      </ol>
      {isPreviewingIntermediate ? (
        <div className="wb-funnel-preview-banner">
          <span>
            正在预览第 {activeStep} 层「{steps[activeStep]?.label}」剩余的{" "}
            <strong>{steps[activeStep]?.remaining ?? 0}</strong> 只股票
          </span>
          <Button size="sm" variant="quiet" onClick={() => onSelectStep(null)}>
            返回最终结果 ({steps[lastIdx]?.remaining ?? 0})
          </Button>
        </div>
      ) : null}
    </div>
  );
}
