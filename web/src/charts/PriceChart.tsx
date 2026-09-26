import {
  CandlestickSeries,
  type ChartOptions,
  ColorType,
  createChart,
  createSeriesMarkers,
  type DeepPartial,
  HistogramSeries,
  type IChartApi,
  type ISeriesApi,
  type ISeriesMarkersPluginApi,
  LineSeries,
  LineStyle,
  type MouseEventParams,
  type Time,
} from "lightweight-charts";
import { useEffect, useRef, useState } from "react";
import { useTheme } from "@/theme/ThemeProvider";
import { SLOTS_PER_DAY, sessionPosition, sessionTime, slotLabel } from "./sessionAxis";
import { type ChartColors, chartColors, withAlpha } from "./tokens";

/** The three chart modes of the panorama and the stock views. */
export type PriceChartMode = "intraday" | "five-day" | "daily";

export interface DailyBar {
  /** Trading day, "YYYY-MM-DD". */
  time: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume?: number | null;
  ma5?: number | null;
  ma10?: number | null;
  ma20?: number | null;
  /** Today's bar made up from the live snapshot. */
  provisional?: boolean;
}

export interface SessionBar {
  /** Trading day, "YYYY-MM-DD". */
  day: string;
  /** "HH:MM". */
  t: string;
  /** 0–240 on the session axis (see sessionAxis.ts). */
  slot: number;
  price: number;
  avg?: number | null;
  volume?: number | null;
  direction: "up" | "down" | "flat";
}

export interface SessionMark {
  day: string;
  slot: number;
  label: string;
}

export type PriceChartProps =
  | { mode: "daily"; bars: readonly DailyBar[]; label: string; className?: string }
  | {
      mode: "intraday" | "five-day";
      bars: readonly SessionBar[];
      /** The trading days on the axis, oldest first. */
      days: readonly string[];
      marks?: readonly SessionMark[];
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
    crosshair: { horzLine: { labelVisible: true }, vertLine: { labelVisible: true } },
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
    .filter((bar) => bar.volume !== undefined && bar.volume !== null)
    .map((bar) => ({
      time: bar.time,
      value: bar.volume ?? 0,
      color: withAlpha(bar.close >= bar.open ? colors.up : colors.down, 0.45),
    }));
}

/** MA lines drop their empty head (fewer than N bars so far). */
export function movingAverage(bars: readonly DailyBar[], key: "ma5" | "ma10" | "ma20") {
  return bars.flatMap((bar) => {
    const value = bar[key];
    return value === null || value === undefined ? [] : [{ time: bar.time, value }];
  });
}

const DIRECTION_COLOR = {
  up: (colors: ChartColors) => withAlpha(colors.up, 0.55),
  down: (colors: ChartColors) => withAlpha(colors.down, 0.55),
  flat: (colors: ChartColors) => withAlpha(colors.muted, 0.45),
} as const;

export interface SessionSeries {
  price: ({ time: number; value: number } | { time: number })[];
  avg: { time: number; value: number }[];
  volume: { time: number; value: number; color: string }[];
}

/**
 * Price, average and volume points on the session axis. One day is padded with empty
 * points to all 241 slots, so a morning's data stops where the morning stops.
 */
export function sessionSeries(
  bars: readonly SessionBar[],
  days: readonly string[],
  colors: ChartColors,
  pad: boolean,
): SessionSeries {
  const dayIndex = new Map(days.map((day, index) => [day, index]));
  const byTime = new Map<number, SessionBar>();
  for (const bar of bars) {
    const index = dayIndex.get(bar.day);
    if (index !== undefined) {
      byTime.set(sessionTime(index, bar.slot), bar);
    }
  }
  const times = [...byTime.keys()].sort((a, b) => a - b);
  const price: SessionSeries["price"] = times.map((time) => ({
    time,
    value: (byTime.get(time) as SessionBar).price,
  }));
  if (pad && days.length === 1) {
    const taken = new Set(times);
    for (let slot = 0; slot < SLOTS_PER_DAY; slot += 1) {
      const time = sessionTime(0, slot);
      if (!taken.has(time)) {
        price.push({ time });
      }
    }
    price.sort((a, b) => a.time - b.time);
  }
  const avg = times.flatMap((time) => {
    const value = (byTime.get(time) as SessionBar).avg;
    return value === null || value === undefined ? [] : [{ time, value }];
  });
  const volume = times.flatMap((time) => {
    const bar = byTime.get(time) as SessionBar;
    return bar.volume === null || bar.volume === undefined
      ? []
      : [{ time, value: bar.volume, color: DIRECTION_COLOR[bar.direction](colors) }];
  });
  return { price, avg, volume };
}

/** What the legend shows for the point under the cursor (or the last one). */
export interface ChartReadout {
  when: string;
  values: { key: string; label: string; value: number | null; color?: string }[];
  /** e.g. the 爆量确认 at this minute. */
  note?: string;
}

function sessionReadout(
  bar: SessionBar | undefined,
  colors: ChartColors,
  note?: string,
): ChartReadout | null {
  if (bar === undefined) {
    return null;
  }
  return {
    note,
    when: `${bar.day.slice(5)} ${bar.t}`,
    values: [
      { key: "price", label: "价", value: bar.price, color: colors.accent },
      { key: "avg", label: "均", value: bar.avg ?? null, color: colors.warn },
      { key: "volume", label: "量", value: bar.volume ?? null },
    ],
  };
}

function dailyReadout(bar: DailyBar | undefined, colors: ChartColors): ChartReadout | null {
  if (bar === undefined) {
    return null;
  }
  return {
    when: bar.provisional ? `${bar.time} 盘中` : bar.time,
    values: [
      { key: "open", label: "开", value: bar.open },
      { key: "high", label: "高", value: bar.high },
      { key: "low", label: "低", value: bar.low },
      { key: "close", label: "收", value: bar.close },
      { key: "ma5", label: "MA5", value: bar.ma5 ?? null, color: colors.series[1] },
      { key: "ma10", label: "MA10", value: bar.ma10 ?? null, color: colors.series[0] },
      { key: "ma20", label: "MA20", value: bar.ma20 ?? null, color: colors.series[2] },
    ],
  };
}

function formatValue(key: string, value: number | null): string {
  if (value === null || !Number.isFinite(value)) {
    return "—";
  }
  if (key === "volume") {
    return value >= 1e4 ? `${(value / 1e4).toFixed(1)}万` : value.toFixed(0);
  }
  return value.toFixed(2);
}

/**
 * The one lightweight-charts wrapper (pages never import lightweight-charts): daily
 * candles with MA5/10/20, or the intraday / 5-day line on the session axis with the
 * average line, volume coloured by minute direction and 爆量 markers. A readout above
 * the chart follows the cursor.
 */
export function PriceChart(props: PriceChartProps) {
  const { mode, label, className = "chart" } = props;
  const containerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const priceRef = useRef<ISeriesApi<"Candlestick"> | ISeriesApi<"Line"> | null>(null);
  const avgRef = useRef<ISeriesApi<"Line"> | null>(null);
  const maRefs = useRef<ISeriesApi<"Line">[]>([]);
  const volumeRef = useRef<ISeriesApi<"Histogram"> | null>(null);
  const markersRef = useRef<ISeriesMarkersPluginApi<Time> | null>(null);
  const readoutAt = useRef<(time: number | string | null) => ChartReadout | null>(() => null);
  const markTimesRef = useRef<{ time: number; label: string }[]>([]);
  const [readout, setReadout] = useState<ChartReadout | null>(null);
  const [markLines, setMarkLines] = useState<{ time: number; x: number; label: string }[]>([]);
  const { mode: themeMode, resolved } = useTheme();
  const updateMarkLines = () => {
    const chart = chartRef.current;
    const width = containerRef.current?.clientWidth ?? 0;
    if (chart === null) {
      return;
    }
    const next = markTimesRef.current.flatMap(({ time, label }) => {
      const x = chart.timeScale().timeToCoordinate(time as Time);
      return x === null || x < 0 || x > width ? [] : [{ time, x, label }];
    });
    setMarkLines((current) =>
      current.length === next.length &&
      current.every((line, index) => line.time === next[index]?.time && line.x === next[index]?.x)
        ? current
        : next,
    );
  };
  const updateMarkLinesRef = useRef(updateMarkLines);
  updateMarkLinesRef.current = updateMarkLines;
  const scheduleMarkLinesRef = useRef<() => void>(() => {});

  useEffect(() => {
    const container = containerRef.current;
    if (container === null) {
      return undefined;
    }
    const chart = createChart(container, { autoSize: true });
    if (mode === "daily") {
      priceRef.current = chart.addSeries(CandlestickSeries, {});
      maRefs.current = [0, 1, 2].map(() =>
        chart.addSeries(LineSeries, {
          lineWidth: 1,
          priceLineVisible: false,
          lastValueVisible: false,
          crosshairMarkerVisible: false,
        }),
      );
    } else {
      priceRef.current = chart.addSeries(LineSeries, { lineWidth: 2 });
      avgRef.current = chart.addSeries(LineSeries, {
        lineWidth: 1,
        lineStyle: LineStyle.Dashed,
        priceLineVisible: false,
        lastValueVisible: false,
        crosshairMarkerVisible: false,
      });
    }
    volumeRef.current = chart.addSeries(HistogramSeries, {
      priceScaleId: "volume",
      priceFormat: { type: "volume" },
      priceLineVisible: false,
      lastValueVisible: false,
    });
    chart.priceScale("volume").applyOptions({ scaleMargins: { top: 0.78, bottom: 0 } });
    chart.priceScale("right").applyOptions({ scaleMargins: { top: 0.08, bottom: 0.26 } });
    const onMove = (param: MouseEventParams<Time>) => {
      const time = param.time === undefined ? null : (param.time as number | string);
      setReadout(readoutAt.current(time));
    };
    chart.subscribeCrosshairMove(onMove);
    chartRef.current = chart;
    let frame: number | null = null;
    const scheduleMarkLines = () => {
      if (frame !== null) {
        cancelAnimationFrame(frame);
      }
      frame = requestAnimationFrame(() => {
        frame = null;
        updateMarkLinesRef.current();
      });
    };
    scheduleMarkLinesRef.current = scheduleMarkLines;
    chart.timeScale().subscribeVisibleLogicalRangeChange(scheduleMarkLines);
    const observer = new ResizeObserver(scheduleMarkLines);
    observer.observe(container);
    return () => {
      chart.timeScale().unsubscribeVisibleLogicalRangeChange(scheduleMarkLines);
      observer.disconnect();
      if (frame !== null) {
        cancelAnimationFrame(frame);
      }
      scheduleMarkLinesRef.current = () => {};
      chart.unsubscribeCrosshairMove(onMove);
      chart.remove();
      chartRef.current = null;
      priceRef.current = null;
      avgRef.current = null;
      maRefs.current = [];
      volumeRef.current = null;
      markersRef.current = null;
    };
  }, [mode]);

  // biome-ignore lint/correctness/useExhaustiveDependencies: themeMode/resolved change the CSS variables chartColors() reads.
  useEffect(() => {
    const chart = chartRef.current;
    if (chart === null) {
      return;
    }
    const colors = chartColors();
    chart.applyOptions(priceChartOptions(colors));
    if (props.mode === "daily") {
      markTimesRef.current = [];
      setMarkLines([]);
      const series = priceRef.current as ISeriesApi<"Candlestick"> | null;
      series?.applyOptions(candleSeriesColors(colors));
      series?.setData(
        props.bars.map(({ time, open, high, low, close }) => ({ time, open, high, low, close })),
      );
      const maKeys = ["ma5", "ma10", "ma20"] as const;
      const maColors = [colors.series[1], colors.series[0], colors.series[2]];
      maRefs.current.forEach((line, index) => {
        line.applyOptions({ color: maColors[index] });
        line.setData(movingAverage(props.bars, maKeys[index] as "ma5"));
      });
      volumeRef.current?.setData(dailyVolumeBars(props.bars, colors));
      const provisional = props.bars.filter((bar) => bar.provisional);
      if (series) {
        markersRef.current ??= createSeriesMarkers(series as ISeriesApi<"Candlestick", Time>, []);
        markersRef.current.setMarkers(
          provisional.map((bar) => ({
            time: bar.time as Time,
            position: "aboveBar" as const,
            color: colors.muted,
            shape: "circle" as const,
            text: "盘中",
          })),
        );
      }
      chart.applyOptions({ timeScale: { tickMarkFormatter: undefined } });
      const byTime = new Map(props.bars.map((bar) => [bar.time, bar]));
      const last = props.bars[props.bars.length - 1];
      readoutAt.current = (time) =>
        dailyReadout(time === null ? last : (byTime.get(String(time)) ?? last), colors);
      setReadout(dailyReadout(last, colors));
      chart.timeScale().fitContent();
      return;
    }
    const { bars, days } = props;
    const pad = props.mode === "intraday";
    const data = sessionSeries(bars, days, colors, pad);
    const line = priceRef.current as ISeriesApi<"Line"> | null;
    line?.applyOptions({ color: colors.accent });
    line?.setData(data.price as never);
    avgRef.current?.applyOptions({ color: colors.warn });
    avgRef.current?.setData(data.avg as never);
    volumeRef.current?.setData(data.volume as never);
    const dayIndex = new Map(days.map((day, index) => [day, index]));
    markTimesRef.current = (props.marks ?? []).flatMap((mark) => {
      const index = dayIndex.get(mark.day);
      return index === undefined
        ? []
        : [{ time: sessionTime(index, mark.slot), label: mark.label }];
    });
    if (line) {
      markersRef.current ??= createSeriesMarkers(line as ISeriesApi<"Line", Time>, []);
      markersRef.current.setMarkers(
        (props.marks ?? []).flatMap((mark) => {
          const index = dayIndex.get(mark.day);
          return index === undefined
            ? []
            : [
                {
                  time: sessionTime(index, mark.slot) as Time,
                  position: "aboveBar" as const,
                  color: colors.series[1],
                  shape: "circle" as const,
                  text: days.length > 1 ? "爆量" : mark.label.slice(0, 5),
                },
              ];
        }),
      );
    }
    const label = (time: number) => {
      const { dayIndex: index, slot } = sessionPosition(time);
      if (days.length > 1) {
        return slot === 0 ? (days[index] ?? "").slice(5) : slotLabel(slot);
      }
      return slotLabel(slot);
    };
    chart.applyOptions({
      timeScale: {
        tickMarkFormatter: (time: Time) => (typeof time === "number" ? label(time) : null),
        fixLeftEdge: true,
        fixRightEdge: true,
      },
      localization: {
        locale: "zh-CN",
        timeFormatter: (time: Time) => {
          if (typeof time !== "number") {
            return String(time);
          }
          const { dayIndex: index, slot } = sessionPosition(time);
          return `${(days[index] ?? "").slice(5)} ${slotLabel(slot)}`;
        },
      },
    });
    const byTime = new Map<number, SessionBar>();
    for (const bar of bars) {
      const index = dayIndex.get(bar.day);
      if (index !== undefined) {
        byTime.set(sessionTime(index, bar.slot), bar);
      }
    }
    const notes = new Map<number, string>();
    for (const mark of props.marks ?? []) {
      const index = dayIndex.get(mark.day);
      if (index !== undefined) {
        notes.set(sessionTime(index, mark.slot), mark.label);
      }
    }
    const last = bars[bars.length - 1];
    readoutAt.current = (time) => {
      const bar = time === null ? last : (byTime.get(Number(time)) ?? last);
      return sessionReadout(bar, colors, time === null ? undefined : notes.get(Number(time)));
    };
    setReadout(sessionReadout(last, colors));
    chart.timeScale().fitContent();
    updateMarkLinesRef.current();
    scheduleMarkLinesRef.current();
  }, [props, themeMode, resolved]);

  return (
    <div className="price-chart">
      <div className="chart-readout" aria-live="off">
        {readout ? (
          <>
            <span className="num when">{readout.when}</span>
            {readout.values.map((item) => (
              <span key={item.key} className="num">
                <span style={item.color ? { color: item.color } : undefined}>{item.label}</span>{" "}
                {formatValue(item.key, item.value)}
              </span>
            ))}
            {readout.note ? <span className="readout-note">{readout.note}</span> : null}
          </>
        ) : null}
      </div>
      <div className="price-chart-stage">
        <div ref={containerRef} className={className} role="img" aria-label={label} />
        {markLines.map((line) => (
          <span
            key={line.time}
            className="surge-mark-line"
            style={{ left: line.x }}
            title={line.label}
            aria-hidden="true"
          />
        ))}
      </div>
    </div>
  );
}
