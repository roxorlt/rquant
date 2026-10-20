import type { ReactNode } from "react";
import { Popover } from "./Popover";

/** A 「?」 that opens its explanation on click. */
export function HelpMark({ label = "说明", children }: { label?: string; children: ReactNode }) {
  return (
    <Popover content={<div className="help-body">{children}</div>} placement="bottomLeft">
      <button type="button" className="help-mark" aria-label={label}>
        ?
      </button>
    </Popover>
  );
}
