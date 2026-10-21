import type { ReactElement } from "react";
import { ALERT_POOL_TIP_PARAGRAPHS } from "@/copy/publicText";

/** The scope explanation: two paragraphs, wording unchanged. */
export function AlertPoolTip(): ReactElement {
  return (
    <div className="scope-tip">
      {ALERT_POOL_TIP_PARAGRAPHS.map((paragraph) => (
        <p key={paragraph}>{paragraph}</p>
      ))}
    </div>
  );
}
