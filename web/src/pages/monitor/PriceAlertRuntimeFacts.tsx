import { useRef } from "react";
import { ApiError, apiClient, type Schemas } from "@/api/client";
import { useServingQuery } from "@/api/useServingQuery";
import { formatShanghaiDateTime } from "@/format/time";
import { Button, EmptyState, PageSkeleton, Panel, RelativeTime, StatusBadge, Tip } from "@/ui";
import "./priceAlertRuntimeFacts.css";

type RuntimeData = Schemas["PriceAlertRuntimeData"];
type EventsData = Schemas["PriceAlertRecentEventsData"];
type RuntimeItem = Schemas["PriceAlertRuntimeItem"];
type Notification = Schemas["PriceAlertNotificationFact"];

/** Round the original bounded decimal text; a JavaScript number would lose precision. */
export function priceText(value: string): string {
  if (!/^\d{1,64}(?:\.\d{1,64})?$/.test(value)) return "—";
  const [integer = "0", fraction = ""] = value.split(".");
  let cents = BigInt(integer) * 100n + BigInt(fraction.padEnd(2, "0").slice(0, 2));
  if ((fraction[2] ?? "0") >= "5") cents += 1n;
  const whole = (cents / 100n).toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
  return `${whole}.${(cents % 100n).toString().padStart(2, "0")}`;
}

export function usePriceAlertRuntimeFacts(owner: string | null, generation: string | null) {
  const identity = useRef({ owner, generation });
  identity.current = { owner, generation };
  const enabled = owner !== null && generation !== null;
  const runtime = useServingQuery<RuntimeData>(
    ["private-price-runtime", owner, generation],
    async () => {
      const { data, response } = await apiClient().GET("/api/v1/monitor/price-rules/runtime");
      if (data === undefined) throw new ApiError(response.status, "运行状态暂不可用，请稍后重试。");
      if (
        identity.current.owner !== owner ||
        identity.current.generation !== generation ||
        data.serving.state !== "ready" ||
        data.serving.generation_id !== generation ||
        data.data.generation_id !== generation
      )
        throw new ApiError(409, "设置已更新，等待运行端同步。");
      return data;
    },
    { enabled, staleTime: 0, refetchInterval: enabled ? 5_000 : false },
  );
  const events = useServingQuery<EventsData>(
    ["private-price-events", owner, generation],
    async () => {
      const { data, response } = await apiClient().GET("/api/v1/monitor/price-rules/events");
      if (data === undefined) throw new ApiError(response.status, "最近提醒暂不可用，请稍后重试。");
      if (
        identity.current.owner !== owner ||
        identity.current.generation !== generation ||
        data.serving.state !== "ready" ||
        data.serving.generation_id !== generation ||
        data.data.generation_id !== generation
      )
        throw new ApiError(409, "设置已更新，等待运行端同步。");
      return data;
    },
    { enabled, staleTime: 0, refetchInterval: enabled ? 5_000 : false },
  );
  const conflict = [runtime.error, events.error].some(
    (error) => error instanceof ApiError && error.status === 409,
  );
  const currentRuntime = enabled && !conflict && !runtime.error ? runtime.data : undefined;
  const currentEvents = enabled && !conflict && !events.error ? events.data : undefined;
  return {
    runtime: currentRuntime,
    events: currentEvents,
    loading: enabled && (runtime.isLoading || events.isLoading),
    message: conflict
      ? "设置已更新，等待运行端同步。"
      : runtime.error || events.error
        ? "运行信息暂不可用，请稍后重试。"
        : "",
    refresh: () => {
      runtime.refetch();
      events.refetch();
    },
  };
}

type Facts = ReturnType<typeof usePriceAlertRuntimeFacts>;

function statusState(item: Pick<RuntimeItem, "status" | "status_label">) {
  if (["等待开盘", "已收盘", "午间休市"].includes(item.status_label)) return "waiting" as const;
  switch (item.status) {
    case "normal":
      return "ok" as const;
    case "attention":
      return "warn" as const;
    case "error":
      return "crit" as const;
    default:
      return "idle" as const;
  }
}

export function PriceAlertRuntimeStatus({
  facts,
  rule,
}: {
  facts: Facts;
  rule: Schemas["PriceAlertRuleItem"];
}) {
  const item = facts.runtime?.items.find(
    (entry) =>
      entry.rule_id === rule.rule_id &&
      entry.version === rule.version &&
      entry.membership_version === rule.membership_version,
  );
  if (!item)
    return (
      <StatusBadge
        state="idle"
        label="未运行"
        reason={facts.message || facts.runtime?.message || "尚无当前规则的运行记录。"}
      />
    );
  return (
    <div className="price-runtime-status">
      <StatusBadge state={statusState(item)} label={item.status_label} reason={item.message} />
      {item.evaluated_at ? <RelativeTime at={item.evaluated_at} suffix="检查" /> : null}
      {item.last_triggered_at ? (
        <Tip content={`上次触发 ${formatShanghaiDateTime(item.last_triggered_at)}`}>
          <span className="muted">
            上次 <RelativeTime at={item.last_triggered_at} />
          </span>
        </Tip>
      ) : null}
      {item.next_allowed_at && Date.parse(item.next_allowed_at) > Date.now() ? (
        <Tip content={`下次可提醒：${formatShanghaiDateTime(item.next_allowed_at)}`}>
          <span className="muted">提醒间隔中</span>
        </Tip>
      ) : null}
    </div>
  );
}

function NotificationBadge({ notification }: { notification: Notification }) {
  const state =
    notification.state === "accepted"
      ? "ok"
      : ["unknown", "admitted", "rejected", "unavailable"].includes(notification.state)
        ? "warn"
        : "idle";
  return (
    <StatusBadge
      state={state}
      label={notification.label}
      reason={`${notification.channel === "pushdeer" ? "通知推送" : "消息推送"}：${notification.message}`}
    />
  );
}

export function PriceAlertRuntimeFacts({ facts }: { facts: Facts }) {
  const runtime = facts.runtime;
  const events = facts.events;
  return (
    <Panel
      title="最近到价提醒"
      sub="最近 20 条"
      label="最近到价提醒"
      actions={
        <>
          {runtime ? (
            <StatusBadge
              state={statusState(runtime)}
              label={runtime.status_label}
              reason={runtime.message}
            />
          ) : null}
          <Button size="sm" variant="ghost" onClick={facts.refresh}>
            刷新提醒
          </Button>
        </>
      }
    >
      {runtime?.quote_updated_at ? (
        <div className="price-runtime-updated muted">
          报价 <RelativeTime at={runtime.quote_updated_at} suffix="更新" />
        </div>
      ) : null}
      {facts.loading ? (
        <PageSkeleton label="运行信息加载中" />
      ) : facts.message ? (
        <EmptyState title={facts.message} />
      ) : events?.availability !== "ready" ? (
        <EmptyState
          title={events?.message || runtime?.message || "尚无运行记录，启用规则后等待检查。"}
        />
      ) : events.items.length === 0 ? (
        <EmptyState title="暂未触发到价提醒" hint={events.message || "满足规则后会显示在这里。"} />
      ) : (
        <ul className="price-runtime-events" aria-label="到价提醒记录">
          {events.items.map((event) => (
            <li key={event.event_id}>
              <div className="price-runtime-event-title">
                <strong>{event.rule_name}</strong>
                <span className="mono">{event.ts_code}</span>
                <RelativeTime at={event.triggered_at} />
              </div>
              <div className="price-runtime-event-price">
                <Tip content={`完整报价 ${event.price}`}>
                  <span className="num">{priceText(event.price)}</span>
                </Tip>
                <Tip content={`完整阈值 ${event.threshold}`}>
                  <span className="muted">
                    {event.comparison === "gte" ? "不低于" : "不高于"} {priceText(event.threshold)}
                  </span>
                </Tip>
              </div>
              <div className="price-runtime-notifications">
                {event.notifications.map((notification, index) => (
                  <NotificationBadge
                    // biome-ignore lint/suspicious/noArrayIndexKey: Up to two immutable targets retain their original stable receipt order.
                    key={`${notification.channel}-${index}`}
                    notification={notification}
                  />
                ))}
                {event.route_message ? <span className="muted">{event.route_message}</span> : null}
              </div>
            </li>
          ))}
        </ul>
      )}
    </Panel>
  );
}
