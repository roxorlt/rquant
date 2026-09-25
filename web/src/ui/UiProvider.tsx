import { App as AntApp, ConfigProvider } from "antd";
import zhCN from "antd/locale/zh_CN";
import dayjs from "dayjs";
import "dayjs/locale/zh-cn";
import { type ReactNode, useMemo } from "react";
import { useTheme } from "@/theme/ThemeProvider";
import { antdThemeFor } from "./theme";

dayjs.locale("zh-cn");

/**
 * antd for the whole app: Chinese locale (antd and dayjs) and the rQuant palette
 * for the current theme. Pages never import antd directly; they use this
 * directory's wrappers.
 */
export function UiProvider({ children }: { children: ReactNode }) {
  const { mode, resolved } = useTheme();
  // biome-ignore lint/correctness/useExhaustiveDependencies: mode/resolved change the CSS variables antdThemeFor() reads.
  const themeConfig = useMemo(() => antdThemeFor(resolved), [mode, resolved]);
  return (
    <ConfigProvider locale={zhCN} theme={themeConfig}>
      <AntApp component={false}>{children}</AntApp>
    </ConfigProvider>
  );
}
