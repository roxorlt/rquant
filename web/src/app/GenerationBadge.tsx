import type { MetaEnvelope } from "@/api/client";
import { formatAge, formatShanghaiDateTime } from "@/format/time";
import { Tip, useNow } from "@/ui";

const STATE_WORDS = {
  ready: "正常",
  stale: "没有按时更新",
  degraded: "显示的是上一批",
  unavailable: "读不到",
} as const;

function ChipTip({ meta }: { meta: MetaEnvelope }) {
  const { serving, data } = meta;
  const generation = data.generation;
  return (
    <div className="chip-tip">
      <dl className="kv">
        <dt>状态</dt>
        <dd>{STATE_WORDS[serving.state]}</dd>
        {generation ? (
          <>
            <dt>更新于</dt>
            <dd className="num">{formatShanghaiDateTime(generation.built_at)}</dd>
            <dt>数据版本</dt>
            <dd className="mono">{generation.generation_id.slice(0, 12)}</dd>
          </>
        ) : null}
      </dl>
      {data.datasets.length ? (
        <ul className="chip-datasets">
          {data.datasets.map((item) => (
            <li key={item.dataset_id} data-state={item.user_status.state}>
              <span>{item.name}</span>
              <span>{item.user_status.label}</span>
            </li>
          ))}
        </ul>
      ) : null}
    </div>
  );
}

/** The page-data chip: "数据 1 分钟前更新"; version, time and datasets in the tooltip. */
export function GenerationBadge({
  meta,
  failed,
  receivedAt,
}: {
  meta: MetaEnvelope | undefined;
  failed: boolean;
  /** When the browser received `meta` (ms), so the age keeps counting between polls. */
  receivedAt: number;
}) {
  const now = useNow(15_000);
  if (meta === undefined) {
    return (
      <span className="gen-tag" data-state={failed ? "unavailable" : "loading"}>
        <span className="gdot" aria-hidden="true" />
        {failed ? "数据 未连接" : "数据 读取中"}
      </span>
    );
  }
  const generation = meta.data.generation;
  const state = meta.serving.state;
  let text = "数据 读不到";
  if (generation !== null) {
    const age = generation.age_seconds + Math.max((now - receivedAt) / 1000, 0);
    text = age < 60 ? "数据 刚刚更新" : `数据 ${formatAge(age)}更新`;
  }
  return (
    <Tip content={<ChipTip meta={meta} />} placement="bottom">
      <span
        className="gen-tag"
        data-state={state}
        data-generation={generation?.generation_id.slice(0, 12)}
      >
        <span className="gdot" aria-hidden="true" />
        {text}
      </span>
    </Tip>
  );
}
