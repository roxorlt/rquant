import { Dropdown, type MenuProps } from "antd";
import type { ReactNode } from "react";

export type DropdownMenuItem =
  | {
      key: string;
      label: ReactNode;
      onSelect?: () => void;
      disabled?: boolean;
      /** Shown as a ✓ for the chosen option of a radio-like group. */
      checked?: boolean;
    }
  | { key: string; type: "divider" }
  | { key: string; type: "group"; label: ReactNode; children: DropdownMenuItem[] };

function toAntItems(items: readonly DropdownMenuItem[]): NonNullable<MenuProps["items"]> {
  return items.map((item) => {
    if ("type" in item && item.type === "divider") {
      return { key: item.key, type: "divider" as const };
    }
    if ("type" in item && item.type === "group") {
      return {
        key: item.key,
        type: "group" as const,
        label: item.label,
        children: toAntItems(item.children),
      };
    }
    const entry = item as Extract<DropdownMenuItem, { onSelect?: () => void }>;
    return {
      key: entry.key,
      label: entry.label,
      disabled: entry.disabled,
      extra: entry.checked ? "✓" : undefined,
      onClick: entry.onSelect,
    };
  });
}

export interface DropdownMenuProps {
  items: readonly DropdownMenuItem[];
  /** The trigger, usually a button. */
  children: ReactNode;
}

/** A click-opened menu anchored to its trigger (antd Dropdown). */
export function DropdownMenu({ items, children }: DropdownMenuProps) {
  return (
    <Dropdown menu={{ items: toAntItems(items) }} trigger={["click"]} placement="bottomRight">
      {children}
    </Dropdown>
  );
}
