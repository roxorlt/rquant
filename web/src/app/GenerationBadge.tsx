import type { MetaEnvelope } from "@/api/client";
import { formatAge, formatShanghaiDateTime } from "@/format/time";

const STATE_LABELS = {
  ready: "正常",
  stale: "已过期",
  degraded: "降级",
  unavailable: "不可用",
} as const;

/** The Serving generation marker: first 8 characters · age · state. */
export function GenerationBadge({
  meta,
  failed,
}: {
  meta: MetaEnvelope | undefined;
  failed: boolean;
}) {
  if (meta === undefined) {
    return (
      <span className="gen-tag" data-state={failed ? "unavailable" : "loading"}>
        数据代 {failed ? "未连接" : "读取中"}
      </span>
    );
  }
  const { serving, data } = meta;
  const generation = data.generation;
  if (generation === null) {
    return (
      <span className="gen-tag" data-state="unavailable" title={serving.detail}>
        数据代 不可用
      </span>
    );
  }
  const title = [
    `数据代 ${generation.generation_id}`,
    `生成于 ${formatShanghaiDateTime(generation.built_at)}`,
    `提交 ${generation.producer_commit.slice(0, 12)}`,
    serving.detail,
  ].join("\n");
  return (
    <span className="gen-tag" data-state={serving.state} title={title}>
      <span className="mono">{generation.generation_id.slice(0, 8)}</span>
      <span>· {formatAge(generation.age_seconds)}</span>
      <span>· {STATE_LABELS[serving.state]}</span>
    </span>
  );
}
