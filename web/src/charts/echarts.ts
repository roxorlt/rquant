/**
 * ECharts, imported on demand: only the chart types and components the
 * prototype's pages use are registered, instead of the full bundle.
 * Add a type here (and to EChartOption) before using it in a page.
 */
import {
  BarChart,
  type BarSeriesOption,
  CandlestickChart,
  type CandlestickSeriesOption,
  HeatmapChart,
  type HeatmapSeriesOption,
  LineChart,
  type LineSeriesOption,
} from "echarts/charts";
import {
  DataZoomComponent,
  type DataZoomComponentOption,
  GridComponent,
  type GridComponentOption,
  LegendComponent,
  type LegendComponentOption,
  MarkLineComponent,
  type MarkLineComponentOption,
  TooltipComponent,
  type TooltipComponentOption,
  VisualMapComponent,
  type VisualMapComponentOption,
} from "echarts/components";
import { type ComposeOption, init as initECharts, use } from "echarts/core";
import { CanvasRenderer } from "echarts/renderers";

let registered = false;

/**
 * Registers the chart types and components. Called by `init` below rather than at module
 * load: the package is declared side-effect free (`"sideEffects": ["*.css"]`), and the
 * production bundle dropped a bare top-level `use([...])`, so the first real chart failed
 * with "… is not a constructor".
 */
export function registerECharts(): void {
  if (registered) {
    return;
  }
  use([
    BarChart,
    CandlestickChart,
    HeatmapChart,
    LineChart,
    DataZoomComponent,
    GridComponent,
    LegendComponent,
    MarkLineComponent,
    TooltipComponent,
    VisualMapComponent,
    CanvasRenderer,
  ]);
  registered = true;
}

export type EChartOption = ComposeOption<
  | BarSeriesOption
  | CandlestickSeriesOption
  | HeatmapSeriesOption
  | LineSeriesOption
  | DataZoomComponentOption
  | GridComponentOption
  | LegendComponentOption
  | MarkLineComponentOption
  | TooltipComponentOption
  | VisualMapComponentOption
>;

export type { ECharts } from "echarts/core";

/** echarts.init after the registration above. */
export function init(...args: Parameters<typeof initECharts>): ReturnType<typeof initECharts> {
  registerECharts();
  return initECharts(...args);
}
