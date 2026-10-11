import { useState } from "react";
import type { Schemas } from "@/api/client";
import { useChips } from "@/api/endpoints";
import { Panel } from "@/ui";

const LETTERS = "ABCDEFGHIJKLMNOP";
/** server default, chosen by scripts/tpo_study.py */
const TPO_DEFAULT = 30;

/** Market profile (TPO): one letter per 30-minute period a price traded in, from 5-minute bars. */
export function TpoPanel({ code }: { code: string }) {
  const [period, setPeriod] = useState<number | undefined>(undefined);
  const chips = useChips(code, period);
  const cur = period ?? TPO_DEFAULT;
  const n = 240 / cur;
  const days: readonly Schemas["TpoDay"][] = chips.data?.tpo ?? [];
  const [pick, setPick] = useState<string | null>(null);
  if (days.length === 0) return null;
  const d = days.find((x) => x.date === pick) ?? days[days.length - 1];
  if (!d) return null;
  return (
    <Panel
      title="TPO 市场轮廓"
      sub={`5 分钟线聚合为 ${cur} 分钟时段 A–${LETTERS[n - 1]}（A 09:30，${LETTERS[n / 2]} 13:00）；高亮 = 70% 价值区，◆ = POC`}
      actions={
        <span className="qc-ctl">
          <span className="seg" title="TPO 时段长度（默认值由历史检验选定）">
            {[15, 30, 60].map((p) => (
              <button key={p} type="button" aria-pressed={p === cur} onClick={() => setPeriod(p)}>
                {p}分
              </button>
            ))}
          </span>
          <span className="seg">
            {days.map((x) => (
              <button
                key={x.date}
                type="button"
                aria-pressed={x.date === d.date}
                onClick={() => setPick(x.date)}
              >
                {x.date.slice(5)}
              </button>
            ))}
          </span>
        </span>
      }
    >
      <p className="chip-stats">
        <span>
          开 <b>{d.open.toFixed(2)}</b> 收 <b>{d.close.toFixed(2)}</b>
        </span>
        <span>
          POC <b>{d.poc.toFixed(2)}</b>
        </span>
        <span>
          价值区{" "}
          <b>
            {d.val.toFixed(2)}–{d.vah.toFixed(2)}
          </b>
        </span>
        <span>
          格距 <b>{d.step}</b>
        </span>
      </p>
      <div className="tpo">
        {d.rows.map((r) => {
          const inVa = r.price >= d.val - 1e-9 && r.price <= d.vah + 1e-9;
          const isPoc = Math.abs(r.price - d.poc) < 1e-6;
          return (
            <div key={r.price} className={`tpo-row${inVa ? " va" : ""}${isPoc ? " poc" : ""}`}>
              <span className="tpo-px">{r.price.toFixed(2)}</span>
              <span className="tpo-l">
                {r.letters}
                {isPoc ? " ◆" : ""}
              </span>
            </div>
          );
        })}
      </div>
    </Panel>
  );
}
