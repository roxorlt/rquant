import type { ReactNode } from "react";

export interface PageHeaderProps {
  /** Group label above the title. Nav-group labels are not shown. */
  eyebrow?: string;
  title: ReactNode;
  note?: ReactNode;
  actions?: ReactNode;
}

const GROUP_LABELS = new Set(["概览", "宏观", "数据", "研究", "策略与验证", "跟踪与告警", "运维"]);

function shownNote(note: ReactNode): ReactNode {
  if (typeof note === "string" && /^\d{4}-\d{2}-\d{2}$/.test(note.trim())) return null;
  return note ?? null;
}

export function PageHeader({ eyebrow, title, note, actions }: PageHeaderProps) {
  const group = eyebrow !== undefined && GROUP_LABELS.has(eyebrow);
  const meta = group ? null : (eyebrow ?? null);
  const line = shownNote(note);
  return (
    <header className="ph">
      <div className="ph-main">
        {meta ? <p className="ph-eyebrow">{meta}</p> : null}
        <h1>{title}</h1>
        {line ? <p className="ph-note">{line}</p> : null}
      </div>
      {actions ? <div className="ph-actions">{actions}</div> : null}
    </header>
  );
}
