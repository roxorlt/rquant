import { render, waitFor } from "@testing-library/react";
import tokensCss from "@/styles/tokens.css?raw";
import { ThemeProvider } from "@/theme/ThemeProvider";
import { layoutFlow } from "./flowLayout";
import { candlestickItemStyle } from "./options";
import {
  candleSeriesColors,
  dailyVolumeBars,
  movingAverage,
  PriceChart,
  sessionSeries,
} from "./PriceChart";
import { SLOTS_PER_DAY, sessionPosition, sessionTime, slotLabel } from "./sessionAxis";
import { chartColors } from "./tokens";

const chart = vi.hoisted(() => {
  const series = { applyOptions: vi.fn(), setData: vi.fn() };
  const markers = { setMarkers: vi.fn() };
  const scale = {
    fitContent: vi.fn(),
    timeToCoordinate: vi.fn(() => 42),
    subscribeVisibleLogicalRangeChange: vi.fn(),
    unsubscribeVisibleLogicalRangeChange: vi.fn(),
  };
  const api = {
    addSeries: vi.fn(() => series),
    applyOptions: vi.fn(),
    priceScale: vi.fn(() => ({ applyOptions: vi.fn() })),
    timeScale: vi.fn(() => scale),
    subscribeCrosshairMove: vi.fn(),
    unsubscribeCrosshairMove: vi.fn(),
    remove: vi.fn(),
  };
  return { api, series, markers, scale, createChart: vi.fn(() => api) };
});

vi.mock("lightweight-charts", () => ({
  CandlestickSeries: "Candlestick",
  HistogramSeries: "Histogram",
  LineSeries: "Line",
  LineStyle: { Dashed: 2 },
  ColorType: { Solid: "solid" },
  createChart: chart.createChart,
  createSeriesMarkers: vi.fn(() => chart.markers),
}));

function withTokens(): () => void {
  const style = document.createElement("style");
  style.textContent = tokensCss;
  document.head.append(style);
  return () => style.remove();
}

describe("price chart", () => {
  it("aligns surge lines with chart coordinates and updates after horizontal movement", async () => {
    const width = vi.spyOn(HTMLElement.prototype, "clientWidth", "get").mockReturnValue(300);
    const { container, unmount } = render(
      <ThemeProvider>
        <PriceChart
          mode="intraday"
          bars={[
            {
              day: "2026-09-24",
              t: "09:47",
              slot: 17,
              price: 10,
              direction: "up",
            },
          ]}
          days={["2026-09-24"]}
          marks={[{ day: "2026-09-24", slot: 17, label: "09:47 爆量确认" }]}
          label="分时"
        />
      </ThemeProvider>,
    );
    const line = container.querySelector<HTMLElement>(".surge-mark-line");
    expect(line).toHaveStyle({ left: "42px" });
    chart.scale.timeToCoordinate.mockReturnValue(72);
    const moved = chart.scale.subscribeVisibleLogicalRangeChange.mock.calls.at(-1)?.[0];
    moved?.();
    await waitFor(() => expect(line).toHaveStyle({ left: "72px" }));
    unmount();
    expect(chart.scale.unsubscribeVisibleLogicalRangeChange).toHaveBeenCalledWith(moved);
    width.mockRestore();
  });

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

describe("session axis", () => {
  it("labels the 241 points like the old panorama and round-trips positions", () => {
    expect(SLOTS_PER_DAY).toBe(241);
    expect([0, 60, 120, 180, 240].map(slotLabel)).toEqual([
      "09:30",
      "10:30",
      "11:30/13:00",
      "14:00",
      "15:00",
    ]);
    expect(slotLabel(121)).toBe("13:01");
    expect(sessionPosition(sessionTime(3, 17))).toEqual({ dayIndex: 3, slot: 17 });
    // Whole hours land on slots 0, 60, … so the chart's own ticks read 09:30, 10:30 …
    expect(sessionTime(0, 0) % 3600).toBe(0);
    expect(sessionTime(0, 60) % 3600).toBe(0);
  });

  it("pads one day to the full session and colours volume by minute direction", () => {
    const cleanup = withTokens();
    const colors = chartColors();
    const bars = [
      { day: "2026-09-24", t: "09:30", slot: 0, price: 10, avg: 10, volume: 5, direction: "flat" },
      {
        day: "2026-09-24",
        t: "09:31",
        slot: 1,
        price: 10.2,
        avg: null,
        volume: 7,
        direction: "up",
      },
      { day: "2026-09-24", t: "09:32", slot: 2, price: 10.1, volume: 3, direction: "down" },
    ] as const;
    const series = sessionSeries(bars, ["2026-09-24"], colors, true);
    expect(series.price).toHaveLength(241);
    expect(series.price.filter((point) => "value" in point)).toHaveLength(3);
    expect(series.avg).toHaveLength(1);
    expect(series.volume.map((point) => point.color)).toEqual([
      "rgba(91, 100, 116, 0.45)",
      "rgba(214, 51, 59, 0.55)",
      "rgba(23, 151, 90, 0.55)",
    ]);
    // Five days are not padded: the axis spans the data.
    expect(sessionSeries(bars, ["2026-09-24"], colors, false).price).toHaveLength(3);
    cleanup();
  });

  it("drops the empty head of a moving average", () => {
    expect(
      movingAverage(
        [
          { time: "a", open: 1, high: 1, low: 1, close: 1, ma5: null },
          { time: "b", open: 1, high: 1, low: 1, close: 1, ma5: 1.5 },
        ],
        "ma5",
      ),
    ).toEqual([{ time: "b", value: 1.5 }]);
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
