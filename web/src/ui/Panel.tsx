import type { ReactNode } from "react";

export interface PanelProps {
  title?: ReactNode;
  sub?: ReactNode;
  actions?: ReactNode;
  /** No body padding (tables that run edge to edge). */
  flush?: boolean;
  label?: string;
  children: ReactNode;
}

export function Panel({ title, sub, actions, flush, label, children }: PanelProps) {
  return (
    <section className="panel" aria-label={label}>
      {title || sub || actions ? (
        <div className="panel-h">
          {title ? <h2>{title}</h2> : null}
          {sub ? <span className="sub">{sub}</span> : null}
          {actions ? <div className="row">{actions}</div> : null}
        </div>
      ) : null}
      <div className={flush ? "panel-b flush" : "panel-b"}>{children}</div>
    </section>
  );
}
