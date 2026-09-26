import type { ChartColors } from "./tokens";

/**
 * Shared ECharts option fragments (the prototype's baseOpt and candlestick
 * colours) as plain data, testable without a canvas.
 */
export function baseOption(colors: ChartColors, animate = true) {
  return {
    animation: animate,
    animationDuration: 380,
    textStyle: { fontFamily: colors.fontSans, color: colors.muted, fontSize: 12 },
    tooltip: {
      trigger: "axis" as const,
      confine: true,
      backgroundColor: colors.surface,
      borderColor: colors.rule,
      borderWidth: 1,
      textStyle: { color: colors.text, fontSize: 12, fontFamily: colors.fontSans },
      axisPointer: { type: "line" as const, lineStyle: { color: colors.ruleStrong, width: 1 } },
    },
  };
}

/** Red body for a rise, green for a fall (ECharts calls the fall colour "0"). */
export function candlestickItemStyle(colors: ChartColors) {
  return {
    color: colors.up,
    color0: colors.down,
    borderColor: colors.up,
    borderColor0: colors.down,
  };
}
