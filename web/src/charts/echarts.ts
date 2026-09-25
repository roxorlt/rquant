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
import { type ComposeOption, init, use } from "echarts/core";
import { CanvasRenderer } from "echarts/renderers";

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
export { init };
