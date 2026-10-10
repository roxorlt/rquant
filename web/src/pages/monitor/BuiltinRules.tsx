import type { Schemas } from "@/api/client";
import { formatCount } from "@/format/number";
import { formatShanghaiDateTime } from "@/format/time";
import { Button, EmptyState, Panel, RelativeTime, SkeletonKpis, StatusBadge, Tip } from "@/ui";

type Choice = Schemas["MonitorBuiltinControlView"];
type Rule = Schemas["MonitorBuiltinStatus"];

const SOURCE_TONE = {
  ready: "ok",
  waiting: "waiting",
  disabled: "idle",
  stale: "warn",
  disconnected: "warn",
  unknown: "warn",
} as const;

function stateLabel(rule: Rule): string {
  if (rule.state === "ready") return "正常";
  if (rule.state === "disabled") return "未运行";
  if (rule.state === "waiting") return rule.state_label === "已收盘" ? "已收盘" : "等待开盘";
  return "注意";
}

export function BuiltinRules({
  data,
  loading = false,
  choices = [],
  busyIds = [],
  blockedIds = [],
  onToggle,
}: {
  data: Schemas["MonitorRuntimeData"] | undefined;
  loading?: boolean;
  choices?: readonly Choice[];
  busyIds?: readonly string[];
  blockedIds?: readonly string[];
  onToggle?: (choice: Choice) => void;
}) {
  return (
    <section className="monitor-builtins" aria-label="内置规则">
      <Panel title="内置规则" sub="沿用原提醒条件">
        {loading ? (
          <SkeletonKpis count={4} />
        ) : data?.state !== "ready" || !data.builtins?.length ? (
          <EmptyState title="内置规则尚未就绪" hint="配置与来源核对后会显示。" />
        ) : (
          <div className="monitor-builtin-grid">
            {data.builtins.map((rule) => {
              const choice = choices.find((item) => item.builtin_id === rule.builtin_id);
              const syncing = choice !== undefined && choice.enabled !== rule.enabled;
              const detail = [
                rule.state_label,
                rule.source_note,
                `核对时间：${formatShanghaiDateTime(rule.evaluated_at)}`,
                `来源时间：${rule.observed_at === null ? "—" : formatShanghaiDateTime(rule.observed_at)}`,
                `有效截至：${rule.source_valid_until === null ? "—" : formatShanghaiDateTime(rule.source_valid_until)}`,
              ].join("；");
              return (
                <article
                  key={rule.builtin_id}
                  className="monitor-builtin-card"
                  aria-label={rule.label}
                >
                  <header>
                    <strong>{rule.label}</strong>
                    <StatusBadge
                      state={SOURCE_TONE[rule.state]}
                      label={stateLabel(rule)}
                      reason={rule.source_note}
                    />
                  </header>
                  <dl className="monitor-builtin-facts">
                    <div>
                      <dt>本次触发</dt>
                      <dd className="num">{formatCount(rule.matched_count)}</dd>
                    </div>
                    <div>
                      <dt>最近触发</dt>
                      <dd>
                        <RelativeTime at={rule.last_triggered_at} />
                      </dd>
                    </div>
                  </dl>
                  <footer>
                    <Button
                      size="sm"
                      variant="ghost"
                      aria-label={`${rule.enabled ? "暂停" : "启用"}${rule.label}`}
                      disabled={
                        !onToggle ||
                        !choice?.can_request ||
                        syncing ||
                        busyIds.includes(rule.builtin_id) ||
                        blockedIds.includes(rule.builtin_id)
                      }
                      onClick={() => {
                        if (choice) onToggle?.(choice);
                      }}
                    >
                      {syncing ? "正在同步" : rule.enabled ? "暂停" : "启用"}
                    </Button>
                    <Tip content={detail} interactive>
                      <Button size="sm" variant="ghost" aria-label={`${rule.label}来源说明`}>
                        ?
                      </Button>
                    </Tip>
                  </footer>
                </article>
              );
            })}
          </div>
        )}
      </Panel>
    </section>
  );
}
