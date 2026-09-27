import type { Schemas } from "@/api/client";
import { formatCount } from "@/format/number";
import { formatShanghaiDateTime } from "@/format/time";
import { type Kpi, RelativeTime, StatusBadge } from "@/ui";

type Acknowledgment = Schemas["AlertAcknowledgmentView"];
type Summary = Schemas["UnacknowledgedSummary"];

const STATE = {
  confirmed: { state: "ok", label: "已确认" },
  unconfirmed: { state: "warn", label: "待确认" },
  historical: { state: "idle", label: "历史告警" },
  unavailable: { state: "idle", label: "暂不可用" },
} as const;

function acknowledgmentReason(acknowledgment: Acknowledgment | undefined): string {
  if (!acknowledgment) return "确认信息尚未发布，请稍后查看。";
  const note =
    acknowledgment.note ??
    {
      confirmed: "这条告警已人工确认。",
      unconfirmed: "这条告警尚未人工确认；通知投递状态另列。",
      historical: "这条告警早于确认功能启用时间。",
      unavailable: "这条告警的确认状态暂时无法核对。",
    }[acknowledgment.state];
  return acknowledgment.confirmed_at
    ? `${note} 确认于 ${formatShanghaiDateTime(acknowledgment.confirmed_at)}`
    : note;
}

/** Display the read model only. The confirmation command is not available yet. */
export function AlertAcknowledgment({ acknowledgment }: { acknowledgment?: Acknowledgment }) {
  const status = STATE[acknowledgment?.state ?? "unavailable"];
  return (
    <StatusBadge
      state={status.state}
      label={status.label}
      reason={acknowledgmentReason(acknowledgment)}
    />
  );
}

export function unacknowledgedKpi(summary?: Summary): Kpi {
  const count = summary?.state === "ready" && summary.count_as_of ? summary.count : null;
  const ready = count !== null && count !== undefined;
  return {
    key: "unacknowledged",
    label: "待确认",
    value: formatCount(ready ? count : null),
    unit: ready ? "条" : undefined,
    sub: ready ? (
      <>
        截至 <RelativeTime at={summary?.count_as_of} />
      </>
    ) : (
      "数量未知"
    ),
    tip: summary?.note ?? "确认信息尚未发布，请稍后查看。",
    tone: ready && count > 0 ? "warn" : undefined,
  };
}
