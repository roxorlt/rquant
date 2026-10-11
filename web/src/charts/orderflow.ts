/** Order-flow estimates from OHLCV bars (no tick data: all of these are approximations). */
export interface OBar {
  open: number;
  high: number;
  low: number;
  close: number;
  vol: number;
}

/** Session VWAP on the typical price with volume-weighted σ. */
export function sessionVwap(bars: readonly OBar[]) {
  let sv = 0;
  let spv = 0;
  let sp2v = 0;
  return bars.map((b) => {
    const p = (b.high + b.low + b.close) / 3;
    sv += b.vol;
    spv += p * b.vol;
    sp2v += p * p * b.vol;
    const m = sv > 0 ? spv / sv : p;
    return { vwap: m, sd: sv > 0 ? Math.sqrt(Math.max(sp2v / sv - m * m, 0)) : 0 };
  });
}

/** Standard normal CDF (Abramowitz–Stegun 7.1.26). */
export function phi(x: number): number {
  const t = 1 / (1 + 0.3275911 * (Math.abs(x) / Math.SQRT2));
  const y =
    1 -
    ((((1.061405429 * t - 1.453152027) * t + 1.421413741) * t - 0.284496736) * t + 0.254829592) *
      t *
      Math.exp(-(x * x) / 2);
  return x >= 0 ? (1 + y) / 2 : (1 - y) / 2;
}

/**
 * Signed volume per bar. ``tick``: sign of the close change (ties split);
 * ``bvc``: bulk volume classification, buy share = Φ(Δp / σ(Δp)) (Easley, López de Prado, O'Hara).
 */
export function deltas(bars: readonly OBar[], prevClose: number, mode: "tick" | "bvc"): number[] {
  const dp = bars.map((b, i) => b.close - (i === 0 ? prevClose : (bars[i - 1]?.close ?? b.close)));
  if (mode === "tick") return bars.map((b, i) => Math.sign(dp[i] ?? 0) * b.vol);
  const mean = dp.reduce((a, b) => a + b, 0) / (dp.length || 1);
  const sd = Math.sqrt(dp.reduce((a, b) => a + (b - mean) ** 2, 0) / (dp.length || 1)) || 1e-9;
  return bars.map((b, i) => b.vol * (2 * phi((dp[i] ?? 0) / sd) - 1));
}

/** Volume spread uniformly over each bar's [low, high]; POC and the 70% value area. */
export function profile(bars: readonly OBar[], lo: number, hi: number, bins = 40) {
  const dist = new Array<number>(bins).fill(0);
  const w = (hi - lo) / bins || 1;
  for (const b of bars) {
    const a = Math.max(0, Math.floor((b.low - lo) / w));
    const z = Math.min(bins - 1, Math.floor((b.high - lo) / w));
    for (let k = a; k <= z; k++) dist[k] = (dist[k] ?? 0) + b.vol / (z - a + 1);
  }
  let poc = 0;
  dist.forEach((v, k) => {
    if (v > (dist[poc] ?? 0)) poc = k;
  });
  const total = dist.reduce((s, v) => s + v, 0);
  let a = poc;
  let b = poc;
  let acc = dist[poc] ?? 0;
  while (acc < total * 0.7 && (a > 0 || b < bins - 1)) {
    const up = b < bins - 1 ? (dist[b + 1] ?? 0) : -1;
    const dn = a > 0 ? (dist[a - 1] ?? 0) : -1;
    if (up >= dn) acc += dist[++b] ?? 0;
    else acc += dist[--a] ?? 0;
  }
  const px = (k: number) => lo + (k + 0.5) * w;
  return { dist, w, poc: px(poc), val: px(a), vah: px(b) };
}

/** Minutes since 09:30 on the continuous 240-minute session axis. */
export function sessionMinute(t: string): number {
  const [h, m] = t.split(":").map(Number);
  const x = (h ?? 0) * 60 + (m ?? 0);
  return x <= 690 ? x - 570 : 120 + (x - 780);
}
