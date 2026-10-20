import { type FormEvent, useState } from "react";
import type { Schemas } from "@/api/client";
import { useAlertRules, useSaveAlertRule } from "@/api/endpoints";
import { useReadOnlyReason } from "@/api/useMeta";
import { DataTable } from "@/table/DataTable";
import { AppDrawer, Button, FailureNotice, Panel, Pill, useToast } from "@/ui";
import { type FailureCopy, failureCopy } from "@/ui/failure";
import { QueryView } from "../shared";

type Rule = Schemas["AlertRuleBody"];
const POOLS = ["pool1", "pool2", "manual"] as const;
const POOL_LABEL: Record<string, string> = { pool1: "一号池", pool2: "二号池", manual: "自选" };
const LEVELS = ["L1", "L2", "L3"] as const;

export function AlertRules() {
  const query = useAlertRules();
  const save = useSaveAlertRule();
  const toast = useToast();
  const readOnly = useReadOnlyReason();
  const [ruleFail, setRuleFail] = useState<{ copy: FailureCopy; rule: Rule } | null>(null);
  const [open, setOpen] = useState(false);
  const [draft, setDraft] = useState<Rule>({
    rule_id: "",
    title: "",
    enabled: true,
    pools: [],
    levels: [],
    cooldown_minutes: 30,
  });
  const submit = (rule: Rule) => {
    if (readOnly) return;
    const ruleId = rule.rule_id || `r${Date.now().toString(36)}`.slice(0, 40);
    save.mutate(
      { ...rule, rule_id: ruleId },
      {
        onSuccess: () => {
          setRuleFail(null);
          setOpen(false);
          toast(`已保存规则『${rule.title || ruleId}』`);
        },
        onError: (error) =>
          setRuleFail({
            copy: failureCopy(`规则『${rule.title || "未命名"}』`, "保存", error),
            rule,
          }),
      },
    );
  };
  const onSubmit = (event: FormEvent) => {
    event.preventDefault();
    submit(draft);
  };
  return (
    <>
      <QueryView query={query}>
        {(data) => (
          <Panel
            title={`告警规则 · ${data.rules.length}`}
            sub="没有规则时全部推送；有规则后只推送命中启用规则的告警"
            actions={
              <Button
                size="sm"
                variant="primary"
                ariaDisabled={!!readOnly}
                aria-describedby={readOnly ? "readonly-notice" : undefined}
                onClick={() => setOpen(true)}
              >
                新建规则
              </Button>
            }
            flush
          >
            <DataTable
              label="告警规则"
              rows={data.rules}
              rowKey={(row) => row.rule_id}
              emptyText="还没有规则，全部告警都会推送"
              columns={[
                { id: "title", header: "规则", value: (row) => row.title },
                {
                  id: "pools",
                  header: "范围",
                  value: (row) => (row.pools ?? []).join(","),
                  cell: (row) =>
                    row.pools?.length
                      ? row.pools.map((p) => POOL_LABEL[p] ?? p).join("、")
                      : "全部",
                },
                {
                  id: "levels",
                  header: "档位",
                  value: (row) => (row.levels ?? []).join(","),
                  cell: (row) => (row.levels?.length ? row.levels.join("、") : "全部"),
                },
                {
                  id: "cool",
                  header: "冷却(分)",
                  numeric: true,
                  value: (row) => row.cooldown_minutes ?? 0,
                },
                {
                  id: "on",
                  header: "状态",
                  value: (row) => String(row.enabled),
                  cell: (row) => (
                    <Button
                      size="sm"
                      variant="ghost"
                      onClick={() => submit({ ...row, enabled: !row.enabled })}
                      aria-describedby={readOnly ? "readonly-notice" : undefined}
                      ariaDisabled={!!readOnly}
                    >
                      {row.enabled ? <Pill kind="ok">启用</Pill> : <Pill kind="warn">停用</Pill>}
                    </Button>
                  ),
                },
              ]}
            />
            {ruleFail && !open ? (
              <FailureNotice
                copy={ruleFail.copy}
                busy={save.isPending}
                onRetry={() => submit(ruleFail.rule)}
              />
            ) : null}
          </Panel>
        )}
      </QueryView>
      <AppDrawer
        open={open}
        onClose={() => setOpen(false)}
        title="新建规则"
        label="新建规则"
        footer={
          <div className="rule-acts">
            <Button type="button" onClick={() => setOpen(false)}>
              取消
            </Button>
            <Button
              type="submit"
              form="rule-form"
              variant="primary"
              disabled={!draft.title.trim()}
              ariaDisabled={!!readOnly}
              aria-describedby={readOnly ? "readonly-notice" : undefined}
            >
              保存规则
            </Button>
          </div>
        }
      >
        <form id="rule-form" className="rule-fields" onSubmit={onSubmit} aria-label="新建规则">
          <label className="fld">
            名称
            <input
              aria-label="规则名称"
              value={draft.title}
              onChange={(e) => setDraft({ ...draft, title: e.target.value })}
            />
          </label>
          <fieldset>
            <legend>池子</legend>
            {POOLS.map((p) => (
              <label key={p} className="rule-check">
                <input
                  type="checkbox"
                  checked={draft.pools?.includes(p) ?? false}
                  onChange={(e) =>
                    setDraft({
                      ...draft,
                      pools: e.target.checked
                        ? [...(draft.pools ?? []), p]
                        : (draft.pools ?? []).filter((x) => x !== p),
                    })
                  }
                />
                {POOL_LABEL[p]}
              </label>
            ))}
          </fieldset>
          <fieldset>
            <legend>档位</legend>
            <div className="seg">
              {LEVELS.map((level) => (
                <button
                  key={level}
                  type="button"
                  aria-pressed={draft.levels?.includes(level) ?? false}
                  onClick={() =>
                    setDraft({
                      ...draft,
                      levels: draft.levels?.includes(level)
                        ? draft.levels.filter((item) => item !== level)
                        : [...(draft.levels ?? []), level],
                    })
                  }
                >
                  {level}
                </button>
              ))}
            </div>
          </fieldset>
          <label className="fld">
            冷却
            <span className="suffix-field">
              <input
                aria-label="冷却分钟"
                type="number"
                min={0}
                max={1440}
                value={draft.cooldown_minutes ?? 30}
                onChange={(e) => setDraft({ ...draft, cooldown_minutes: Number(e.target.value) })}
              />
              <span className="suffix">分钟</span>
            </span>
          </label>
          {ruleFail ? (
            <FailureNotice
              copy={ruleFail.copy}
              busy={save.isPending}
              onRetry={() => submit(ruleFail.rule)}
            />
          ) : null}
        </form>
      </AppDrawer>
    </>
  );
}
