import type { ReactNode } from "react";

export interface Kpi {
  key: string;
  label: string;
  value: ReactNode;
  unit?: string;
  sub?: ReactNode;
}

/** A row of key numbers drawn as one joined strip. */
export function KpiStrip({ items, label }: { items: readonly Kpi[]; label: string }) {
  return (
    <section className="kpis" aria-label={label}>
      {items.map((item) => (
        <div className="kpi" key={item.key}>
          <span className="lbl">{item.label}</span>
          <span className="val">
            {item.value}
            {item.unit ? <small>{item.unit}</small> : null}
          </span>
          {item.sub ? <span className="sub">{item.sub}</span> : null}
        </div>
      ))}
    </section>
  );
}
