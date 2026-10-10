import { useNavigate } from "react-router";
import { type CollaborationMe, ROLE_LABELS } from "@/api/collaboration";
import { THEME_LABELS, THEME_MODES, useTheme } from "@/theme/ThemeProvider";
import { DropdownMenu, type DropdownMenuItem } from "@/ui";
import { UserIcon } from "./icons";

/** 我的: current user, theme, reports, operation log, open-source licences. */
export function UserMenu({
  viewer,
  onAiUsage,
  collaboration,
}: {
  viewer: string | null | undefined;
  onAiUsage?: () => void;
  collaboration?: CollaborationMe;
}) {
  const navigate = useNavigate();
  const { mode, setMode } = useTheme();
  const items: DropdownMenuItem[] = [
    {
      key: "user",
      label: viewer
        ? `当前用户：${viewer}${collaboration?.role ? ` · ${ROLE_LABELS[collaboration.role]}` : ""}`
        : "当前用户：未识别",
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
    { key: "ai-usage", label: "AI 用量", onSelect: onAiUsage, disabled: !viewer },
    { key: "reports", label: "报告", onSelect: () => navigate("/reports") },
    ...(collaboration?.available && collaboration.username === viewer
      ? [
          ...(collaboration.can_manage_users
            ? [{ key: "users", label: "用户与权限", onSelect: () => navigate("/users") }]
            : []),
          ...(collaboration.can_read_audit
            ? [{ key: "audit", label: "操作记录", onSelect: () => navigate("/audit") }]
            : []),
        ]
      : []),
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
