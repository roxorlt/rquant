import type { ReactNode } from "react";

export interface PageHeaderProps {
  eyebrow: string;
  title: string;
  note?: ReactNode;
  actions?: ReactNode;
}

export function PageHeader({ eyebrow, title, note, actions }: PageHeaderProps) {
  return (
    <header className="ph">
      <div className="ph-main">
        <p className="ph-eyebrow">{eyebrow}</p>
        <h1>{title}</h1>
        {note ? <p className="ph-note">{note}</p> : null}
      </div>
      {actions ? <div className="ph-actions">{actions}</div> : null}
    </header>
  );
}
