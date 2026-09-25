import { render } from "@testing-library/react";
import tokensCss from "@/styles/tokens.css?raw";
import { ThemeProvider } from "@/theme/ThemeProvider";
import { layoutFlow } from "./flowLayout";
import { candlestickItemStyle } from "./options";
import { candleSeriesColors, dailyVolumeBars, PriceChart } from "./PriceChart";
import { chartColors } from "./tokens";

const chart = vi.hoisted(() => {
  const series = { applyOptions: vi.fn(), setData: vi.fn() };
  const api = {
    addSeries: vi.fn(() => series),
    applyOptions: vi.fn(),
    priceScale: vi.fn(() => ({ applyOptions: vi.fn() })),
    timeScale: vi.fn(() => ({ fitContent: vi.fn() })),
    remove: vi.fn(),
  };
  return { api, series, createChart: vi.fn(() => api) };
});

vi.mock("lightweight-charts", () => ({
  CandlestickSeries: "Candlestick",
  HistogramSeries: "Histogram",
  LineSeries: "Line",
  ColorType: { Solid: "solid" },
  createChart: chart.createChart,
}));

function withTokens(): () => void {
  const style = document.createElement("style");
  style.textContent = tokensCss;
  document.head.append(style);
  return () => style.remove();
}

describe("price chart", () => {
  it("draws red rising and green falling candles from the theme", () => {
    const cleanup = withTokens();
    const bars = [
      { time: "2026-09-23", open: 10, high: 11, low: 9.8, close: 10.8, volume: 1000 },
      { time: "2026-09-24", open: 10.8, high: 10.9, low: 10, close: 10.1, volume: 800 },
    ];
    const { unmount } = render(
      <ThemeProvider>
        <PriceChart mode="daily" bars={bars} label="日 K" />
      </ThemeProvider>,
    );
    expect(chart.createChart).toHaveBeenCalledOnce();
    expect(chart.series.applyOptions).toHaveBeenCalledWith(
      expect.objectContaining({ upColor: "#d6333b", downColor: "#17975a" }),
    );
    expect(chart.series.setData).toHaveBeenCalledWith([
      { time: "2026-09-23", open: 10, high: 11, low: 9.8, close: 10.8 },
      { time: "2026-09-24", open: 10.8, high: 10.9, low: 10, close: 10.1 },
    ]);
    unmount();
    expect(chart.api.remove).toHaveBeenCalledOnce();
    cleanup();
  });

  it("colours volume by the bar's direction", () => {
    const cleanup = withTokens();
    const colors = chartColors();
    expect(candleSeriesColors(colors).wickUpColor).toBe("#d6333b");
    const volume = dailyVolumeBars(
      [
        { time: "a", open: 1, high: 2, low: 1, close: 2, volume: 5 },
        { time: "b", open: 2, high: 2, low: 1, close: 1, volume: 6 },
        { time: "c", open: 2, high: 2, low: 1, close: 1 },
      ],
      colors,
    );
    expect(volume.map((bar) => bar.color)).toEqual([
      "rgba(214, 51, 59, 0.45)",
      "rgba(23, 151, 90, 0.45)",
    ]);
    expect(candlestickItemStyle(colors)).toEqual({
      color: "#d6333b",
      color0: "#17975a",
      borderColor: "#d6333b",
      borderColor0: "#17975a",
    });
    cleanup();
  });
});

describe("pool graph layout", () => {
  it("lays dependencies out left to right without overlaps", () => {
    const positions = layoutFlow(
      [{ id: "universe" }, { id: "pool1" }, { id: "pool2" }, { id: "watch" }],
      [
        { source: "universe", target: "pool1" },
        { source: "universe", target: "pool2" },
        { source: "pool1", target: "watch" },
      ],
    );
    const at = (id: string) => positions.get(id) ?? { x: Number.NaN, y: Number.NaN };
    expect(at("universe").x).toBeLessThan(at("pool1").x);
    expect(at("pool1").x).toBeLessThan(at("watch").x);
    expect(at("pool1").x).toBe(at("pool2").x);
    expect(Math.abs(at("pool1").y - at("pool2").y)).toBeGreaterThanOrEqual(64);
  });
});
