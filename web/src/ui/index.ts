/**
 * The only entry point pages use for UI building blocks. antd stays behind
 * these wrappers (biome.json forbids importing it from pages and the shell),
 * so a component can be replaced in one place.
 */
export { Button, type ButtonProps } from "./Button";
export { ChangeText } from "./ChangeText";
export { ConfirmDialog, type ConfirmDialogProps, type ConfirmLevel } from "./ConfirmDialog";
export { BottomSheet, type BottomSheetProps, SideDrawer, type SideDrawerProps } from "./Drawer";
export { DropdownMenu, type DropdownMenuItem } from "./DropdownMenu";
export { type Kpi, KpiStrip } from "./KpiStrip";
export { PageHeader, type PageHeaderProps } from "./PageHeader";
export { PagePlaceholder, type PagePlaceholderProps } from "./PagePlaceholder";
export { Panel, type PanelProps } from "./Panel";
export { Pill, type PillKind } from "./Pill";
export { Segmented, type SegmentedOption } from "./Segmented";
export { ServingBanner, type ServingState, servingBannerMessage } from "./ServingBanner";
export { Switch } from "./Switch";
export { type TabItem, Tabs } from "./Tabs";
export { useToast } from "./Toast";
export { UiProvider } from "./UiProvider";
