import type { ReactNode } from "react";

/** Says why there is nothing here and what happens next, in one or two short lines. */
export function EmptyState({ title, hint }: { title: string; hint?: ReactNode }) {
  return (
    <div className="empty-state">
      <p className="empty-title">{title}</p>
      {hint ? <p className="empty-hint">{hint}</p> : null}
    </div>
  );
}
