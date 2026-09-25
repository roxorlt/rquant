import { Drawer as AntDrawer } from "antd";
import type { ReactNode } from "react";

export interface SideDrawerProps {
  open: boolean;
  onClose: () => void;
  title: ReactNode;
  /** 520 px instead of 440 px (log viewer, backtest config). */
  wide?: boolean;
  extra?: ReactNode;
  footer?: ReactNode;
  children: ReactNode;
}

/** Right-hand drawer (stock detail, logs, config). Full width on phones. */
export function SideDrawer({
  open,
  onClose,
  title,
  wide,
  extra,
  footer,
  children,
}: SideDrawerProps) {
  return (
    <AntDrawer
      open={open}
      onClose={onClose}
      title={title}
      extra={extra}
      footer={footer}
      placement="right"
      size={wide ? "min(520px, 100vw)" : "min(440px, 100vw)"}
      destroyOnHidden
      rootClassName="rq-drawer"
    >
      {children}
    </AntDrawer>
  );
}

export interface BottomSheetProps {
  open: boolean;
  onClose: () => void;
  title: ReactNode;
  closeLabel?: string;
  children: ReactNode;
}

/** Bottom sheet used on phones, e.g. for the page navigation. */
export function BottomSheet({
  open,
  onClose,
  title,
  closeLabel = "关闭",
  children,
}: BottomSheetProps) {
  return (
    <AntDrawer
      open={open}
      onClose={onClose}
      title={title}
      placement="bottom"
      size="auto"
      closable={false}
      extra={
        <button className="btn sm" type="button" onClick={onClose}>
          {closeLabel}
        </button>
      }
      rootClassName="rq-sheet"
      styles={{
        section: { maxHeight: "82vh", borderRadius: "12px 12px 0 0" },
        body: { padding: "4px 8px calc(12px + env(safe-area-inset-bottom, 0px))" },
      }}
    >
      {children}
    </AntDrawer>
  );
}
