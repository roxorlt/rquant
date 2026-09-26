import { useEffect, useRef } from "react";
import { useTheme } from "@/theme/ThemeProvider";
import { type EChartOption, type ECharts, init } from "./echarts";
import { type ChartColors, chartColors } from "./tokens";

export interface EChartProps {
  /** Builds the option from the theme colours; rebuilt when the theme changes. */
  build: (colors: ChartColors) => EChartOption;
  label: string;
  /** Size class from components.css: "chart", "chart sm" or "chart lg". */
  className?: string;
}

/**
 * The one ECharts wrapper: init, resize, theme switch and dispose (the
 * prototype's defChart / rethemeCharts). Pages never import echarts.
 */
export function EChart({ build, label, className = "chart" }: EChartProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<ECharts | null>(null);
  const { mode, resolved } = useTheme();

  useEffect(() => {
    const container = containerRef.current;
    if (container === null) {
      return undefined;
    }
    const chart = init(container, null, { renderer: "canvas" });
    chartRef.current = chart;
    const observer = new ResizeObserver(() => chart.resize());
    observer.observe(container);
    return () => {
      observer.disconnect();
      chart.dispose();
      chartRef.current = null;
    };
  }, []);

  // biome-ignore lint/correctness/useExhaustiveDependencies: mode/resolved change the CSS variables chartColors() reads.
  useEffect(() => {
    chartRef.current?.setOption(build(chartColors()), true);
  }, [build, mode, resolved]);

  return <div ref={containerRef} className={className} role="img" aria-label={label} />;
}
