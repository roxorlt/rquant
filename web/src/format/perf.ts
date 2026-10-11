/** Client-side report statistics derived from a NAV series (fractions, not %). */

export interface DrawdownPeriod {
  start: number;
  trough: number;
  /** Index where NAV regained the old peak, or null if still under water. */
  end: number | null;
  depth: number;
}

/** The deepest non-overlapping drawdown periods, deepest first. */
export function topDrawdowns(nav: readonly number[], count = 5): DrawdownPeriod[] {
  const out: DrawdownPeriod[] = [];
  let peak = 0;
  let cur: DrawdownPeriod | null = null;
  nav.forEach((v, i) => {
    const pv = nav[peak] ?? v;
    if (v >= pv) {
      if (cur) {
        cur.end = i;
        out.push(cur);
        cur = null;
      }
      peak = i;
      return;
    }
    const depth = v / pv - 1;
    if (!cur) cur = { start: peak, trough: i, end: null, depth };
    else if (depth < cur.depth) {
      cur.trough = i;
      cur.depth = depth;
    }
  });
  if (cur) out.push(cur);
  return out.sort((a, b) => a.depth - b.depth).slice(0, count);
}

/** Rolling annualised Sharpe of daily returns (rf = 0); null until the window fills. */
export function rollingSharpe(nav: readonly number[], window: number): (number | null)[] {
  const rets = nav.map((v, i) => (i === 0 ? 0 : v / (nav[i - 1] ?? v) - 1));
  return rets.map((_, i) => {
    if (i < window) return null;
    const w = rets.slice(i - window + 1, i + 1);
    const mean = w.reduce((s, x) => s + x, 0) / w.length;
    const sd = Math.sqrt(w.reduce((s, x) => s + (x - mean) ** 2, 0) / (w.length - 1));
    return sd > 0 ? (mean / sd) * Math.sqrt(252) : null;
  });
}

/** Calendar-year returns from dated NAV points. */
export function yearlyReturns(points: readonly { date: string; nav: number }[]) {
  const byYear = new Map<number, { first: number; last: number }>();
  let prevLast: number | null = null;
  for (const p of points) {
    const y = Number(p.date.slice(0, 4));
    const e = byYear.get(y);
    if (!e) {
      byYear.set(y, { first: prevLast ?? 1, last: p.nav });
    } else e.last = p.nav;
    prevLast = p.nav;
  }
  return [...byYear.entries()].map(([year, e]) => ({ year, ret: e.last / e.first - 1 }));
}
