import {
  CandlestickSeries,
  type ChartOptions,
  ColorType,
  createChart,
  type DeepPartial,
  HistogramSeries,
  type IChartApi,
  type ISeriesApi,
  LineSeries,
} from "lightweight-charts";
import { useEffect, useRef } from "react";
import { useTheme } from "@/theme/ThemeProvider";
import { type ChartColors, chartColors, withAlpha } from "./tokens";

/**
 * The three chart modes of the stock drawer and the panorama. M0 fixes the
 * interface and draws daily candles; M1 adds the 240-slot intraday axis, the
 * average-price line and surge markers for the two minute modes.
 */
export type PriceChartMode = "intraday" | "five-day" | "daily";

export interface DailyBar {
  /** Trading day, "YYYY-MM-DD". */
  time: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume?: number;
}

export interface MinuteBar {
  /** Bar start as a Unix timestamp in seconds. */
  time: number;
  price: number;
  volume?: number;
}

export type PriceChartProps =
  | { mode: "daily"; bars: readonly DailyBar[]; label: string; className?: string }
  | {
      mode: "intraday" | "five-day";
      bars: readonly MinuteBar[];
      label: string;
      className?: string;
    };

export function priceChartOptions(colors: ChartColors): DeepPartial<ChartOptions> {
  return {
    layout: {
      background: { type: ColorType.Solid, color: colors.surface },
      textColor: colors.muted,
      fontFamily: colors.fontMono,
      fontSize: 11,
      // Keeps the TradingView attribution the lightweight-charts licence requires.
      attributionLogo: true,
    },
    grid: {
      vertLines: { color: colors.grid },
      horzLines: { color: colors.grid },
    },
    rightPriceScale: { borderColor: colors.rule },
    timeScale: { borderColor: colors.rule },
    localization: { locale: "zh-CN" },
  };
}

/** Red candles for a rise, green for a fall. */
export function candleSeriesColors(colors: ChartColors) {
  return {
    upColor: colors.up,
    downColor: colors.down,
    borderUpColor: colors.up,
    borderDownColor: colors.down,
    wickUpColor: colors.up,
    wickDownColor: colors.down,
  };
}

export function dailyVolumeBars(bars: readonly DailyBar[], colors: ChartColors) {
  return bars
    .filter((bar) => bar.volume !== undefined)
    .map((bar) => ({
      time: bar.time,
      value: bar.volume ?? 0,
      color: withAlpha(bar.close >= bar.open ? colors.up : colors.down, 0.45),
    }));
}

/** The one lightweight-charts wrapper. Pages never import lightweight-charts. */
export function PriceChart(props: PriceChartProps) {
  const { mode, label, className = "chart" } = props;
  const containerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const priceRef = useRef<ISeriesApi<"Candlestick"> | ISeriesApi<"Line"> | null>(null);
  const volumeRef = useRef<ISeriesApi<"Histogram"> | null>(null);
  const { mode: themeMode, resolved } = useTheme();

  useEffect(() => {
    const container = containerRef.current;
    if (container === null) {
      return undefined;
    }
    const chart = createChart(container, { autoSize: true });
    priceRef.current =
      mode === "daily" ? chart.addSeries(CandlestickSeries, {}) : chart.addSeries(LineSeries, {});
    volumeRef.current = chart.addSeries(HistogramSeries, {
      priceScaleId: "volume",
      priceFormat: { type: "volume" },
    });
    chart.priceScale("volume").applyOptions({ scaleMargins: { top: 0.8, bottom: 0 } });
    chartRef.current = chart;
    return () => {
      chart.remove();
      chartRef.current = null;
      priceRef.current = null;
      volumeRef.current = null;
    };
  }, [mode]);

  // biome-ignore lint/correctness/useExhaustiveDependencies: themeMode/resolved change the CSS variables chartColors() reads.
  useEffect(() => {
    const colors = chartColors();
    chartRef.current?.applyOptions(priceChartOptions(colors));
    if (props.mode === "daily") {
      const series = priceRef.current as ISeriesApi<"Candlestick"> | null;
      series?.applyOptions(candleSeriesColors(colors));
      series?.setData(
        props.bars.map(({ time, open, high, low, close }) => ({ time, open, high, low, close })),
      );
      volumeRef.current?.setData(dailyVolumeBars(props.bars, colors));
    } else {
      const series = priceRef.current as ISeriesApi<"Line"> | null;
      series?.applyOptions({ color: colors.accent, lineWidth: 2 });
      series?.setData(props.bars.map((bar) => ({ time: bar.time as never, value: bar.price })));
      volumeRef.current?.setData(
        props.bars
          .filter((bar) => bar.volume !== undefined)
          .map((bar) => ({ time: bar.time as never, value: bar.volume ?? 0 })),
      );
    }
    chartRef.current?.timeScale().fitContent();
  }, [props.bars, props.mode, themeMode, resolved]);

  return <div ref={containerRef} className={className} role="img" aria-label={label} />;
}
