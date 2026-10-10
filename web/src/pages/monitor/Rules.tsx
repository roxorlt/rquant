import { type FormEvent, useState } from "react";
import type { Schemas } from "@/api/client";
import { useAlertRules, useSaveAlertRule } from "@/api/endpoints";
import { DataTable } from "@/table/DataTable";
import { Button, Panel, Pill, useToast } from "@/ui";
import { QueryView } from "../shared";

type Rule = Schemas["AlertRuleBody"];
const POOLS = ["pool1", "pool2", "manual"] as const;
const POOL_LABEL: Record<string, string> = { pool1: "一号池", pool2: "二号池", manual: "自选" };

export function AlertRules() {
  const query = useAlertRules();
  const save = useSaveAlertRule();
  const toast = useToast();
  const [draft, setDraft] = useState<Rule>({
    rule_id: "",
    title: "",
    enabled: true,
    pools: [],
    levels: [],
    cooldown_minutes: 30,
  });
  const submit = (rule: Rule) =>
    save.mutate(rule, {
      onSuccess: (r) => toast(`规则已提交（${r.status}），数据刷新后生效`),
      onError: (e) => toast(`保存失败：${e.message}`),
    });
  const onSubmit = (event: FormEvent) => {
    event.preventDefault();
    submit(draft);
  };
  return (
    <QueryView query={query}>
      {(data) => (
        <Panel
          title={`告警规则 · ${data.rules.length}`}
          sub="没有规则时全部推送；有规则后只推送命中启用规则的告警（同票冷却）"
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
                  row.pools?.length ? row.pools.map((p) => POOL_LABEL[p] ?? p).join("、") : "全部",
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
                  >
                    {row.enabled ? <Pill kind="ok">启用</Pill> : <Pill kind="warn">停用</Pill>}
                  </Button>
                ),
              },
            ]}
          />
          <form className="rule-form" onSubmit={onSubmit} aria-label="新建告警规则">
            <input
              aria-label="规则 ID"
              placeholder="规则 ID（小写字母/数字）"
              value={draft.rule_id}
              onChange={(e) => setDraft({ ...draft, rule_id: e.target.value })}
            />
            <input
              aria-label="规则名称"
              placeholder="名称"
              value={draft.title}
              onChange={(e) => setDraft({ ...draft, title: e.target.value })}
            />
            {POOLS.map((p) => (
              <label key={p}>
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
            <input
              aria-label="档位"
              placeholder="档位，如 L1,L2（空=全部）"
              value={(draft.levels ?? []).join(",")}
              onChange={(e) =>
                setDraft({
                  ...draft,
                  levels: e.target.value
                    .split(",")
                    .map((x) => x.trim())
                    .filter(Boolean),
                })
              }
            />
            <input
              aria-label="冷却分钟"
              type="number"
              min={0}
              max={1440}
              value={draft.cooldown_minutes ?? 0}
              onChange={(e) => setDraft({ ...draft, cooldown_minutes: Number(e.target.value) })}
            />
            <Button type="submit" size="sm" disabled={!draft.rule_id || !draft.title}>
              保存规则
            </Button>
          </form>
        </Panel>
      )}
    </QueryView>
  );
}
