import { useNavigate } from "react-router";
import { THEME_LABELS, THEME_MODES, useTheme } from "@/theme/ThemeProvider";
import { DropdownMenu, type DropdownMenuItem } from "@/ui";
import { UserIcon } from "./icons";

/** 我的: current user, theme, reports, operation log, open-source licences. */
export function UserMenu({ viewer }: { viewer: string | null | undefined }) {
  const navigate = useNavigate();
  const { mode, setMode } = useTheme();
  const items: DropdownMenuItem[] = [
    {
      key: "user",
      label: viewer ? `当前用户：${viewer}` : "当前用户：未识别",
      disabled: true,
    },
    { key: "d1", type: "divider" },
    {
      key: "theme",
      type: "group",
      label: "主题",
      children: THEME_MODES.map((item) => ({
        key: `theme-${item}`,
        label: THEME_LABELS[item],
        checked: item === mode,
        onSelect: () => setMode(item),
      })),
    },
    { key: "d2", type: "divider" },
    { key: "reports", label: "报告", onSelect: () => navigate("/reports") },
    { key: "audit", label: "操作记录 · 即将上线", disabled: true },
    { key: "licenses", label: "开源许可", onSelect: () => navigate("/licenses") },
  ];
  return (
    <DropdownMenu items={items}>
      <button className="icon-btn" type="button" aria-label="我的" aria-haspopup="menu">
        <UserIcon />
      </button>
    </DropdownMenu>
  );
}
