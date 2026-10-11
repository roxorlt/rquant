import { useState } from "react";
import type { Schemas } from "@/api/client";
import { useChips } from "@/api/endpoints";
import { Panel } from "@/ui";

/** Market profile (TPO): one letter per 30-minute period a price traded in, from 5-minute bars. */
export function TpoPanel({ code }: { code: string }) {
  const chips = useChips(code);
  const days: readonly Schemas["TpoDay"][] = chips.data?.tpo ?? [];
  const [pick, setPick] = useState<string | null>(null);
  if (days.length === 0) return null;
  const d = days.find((x) => x.date === pick) ?? days[days.length - 1];
  if (!d) return null;
  return (
    <Panel
      title="TPO 市场轮廓"
      sub="5 分钟线聚合为 30 分钟时段 A–H（A 09:30、D 11:00、E 13:00、H 14:30）；高亮 = 70% 价值区，◆ = POC"
      actions={
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
