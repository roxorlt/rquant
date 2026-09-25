import type { ReactNode } from "react";
import { Tip } from "./Tip";

export interface Kpi {
  key: string;
  label: string;
  value: ReactNode;
  unit?: string;
  sub?: ReactNode;
  /** What the number means, on hover / tap of the label. */
  tip?: ReactNode;
  /** Colours the value (e.g. 异常 count in red). */
  tone?: "warn" | "crit" | "ok";
}

/** A row of key numbers drawn as one joined strip. */
export function KpiStrip({ items, label }: { items: readonly Kpi[]; label: string }) {
  return (
    <section className="kpis" aria-label={label}>
      {items.map((item) => (
        <div className="kpi" key={item.key} data-kpi={item.key}>
          {item.tip ? (
            <Tip content={item.tip} className="lbl has-tip">
              {item.label}
            </Tip>
          ) : (
            <span className="lbl">{item.label}</span>
          )}
          <span className="val" data-tone={item.tone}>
            {item.value}
            {item.unit ? <small>{item.unit}</small> : null}
          </span>
          {item.sub ? <span className="sub">{item.sub}</span> : null}
        </div>
      ))}
    </section>
  );
}
