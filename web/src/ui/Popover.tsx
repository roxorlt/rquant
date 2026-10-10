import { Popover as AntPopover } from "antd";
import type { ReactNode } from "react";

export interface PopoverProps {
  title?: ReactNode;
  content: ReactNode;
  /** The trigger, usually a button. */
  children: ReactNode;
  placement?: "bottom" | "bottomLeft" | "bottomRight" | "top";
}

/** A click-opened panel anchored to its trigger (antd Popover). */
export function Popover({ title, content, children, placement = "bottomRight" }: PopoverProps) {
  return (
    <AntPopover
      title={title}
      content={content}
      trigger="click"
      placement={placement}
      destroyOnHidden
      classNames={{ root: "rq-popover" }}
    >
      {children}
    </AntPopover>
  );
}
