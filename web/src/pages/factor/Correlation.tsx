import { useState } from "react";
import { useFactorCorrelation } from "@/api/endpoints";
import { formatFixed } from "@/format/number";
import { Panel, Segmented } from "@/ui";

type Kind = "value" | "ic";

function bg(v: number | null | undefined): string | undefined {
  if (v == null) return undefined;
  const c = v >= 0 ? "var(--accent)" : "var(--warn)";
  return `color-mix(in srgb, ${c} ${Math.round(Math.abs(v) * 70)}%, transparent)`;
}

/** Factor × factor correlation: latest cross-section (Spearman) or daily IC series. */
export function CorrelationPanel() {
  const query = useFactorCorrelation();
  const [kind, setKind] = useState<Kind>("value");
  const d = query.data;
  if (!d?.names || d.names.length < 2) return null;
  const m = kind === "value" ? d.value_corr : d.ic_corr;
  return (
    <Panel
      title="因子相关性"
      sub="|ρ| > 0.7 的因子信息高度重叠，组合时只留一个"
      actions={
        <Segmented<Kind>
          label="相关性口径"
          value={kind}
          onChange={setKind}
          options={[
            { value: "value", label: "截面值" },
            { value: "ic", label: "IC 序列" },
          ]}
        />
      }
      flush
    >
      <div className="tbl-wrap scroll">
        <table className="tbl corr" aria-label="因子相关性">
          <thead>
            <tr>
              <th />
              {d.names.map((n) => (
                <th key={n} className="num">
                  {n}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {d.names.map((n, i) => (
              <tr key={n}>
                <td>{n}</td>
                {(m[i] ?? []).map((v, j) => (
                  <td
                    key={d.names[j]}
                    className="num"
                    style={{ background: i === j ? undefined : bg(v) }}
                  >
                    {v == null ? "—" : formatFixed(v, 2)}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </Panel>
  );
}
