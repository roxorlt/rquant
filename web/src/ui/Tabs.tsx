import { Tabs as AntTabs } from "antd";
import type { ReactNode } from "react";

export interface TabItem {
  key: string;
  label: ReactNode;
  children?: ReactNode;
  disabled?: boolean;
}

export interface TabsProps {
  items: readonly TabItem[];
  activeKey?: string;
  defaultActiveKey?: string;
  onChange?: (key: string) => void;
}

/** Underlined page tabs (the prototype's .tabs). */
export function Tabs({ items, activeKey, defaultActiveKey, onChange }: TabsProps) {
  return (
    <AntTabs
      className="rq-tabs"
      items={items.map((item) => ({ ...item }))}
      activeKey={activeKey}
      defaultActiveKey={defaultActiveKey}
      onChange={onChange}
    />
  );
}
