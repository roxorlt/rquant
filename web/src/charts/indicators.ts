/** Classic technical indicators over full series (null during warm-up). */
type S = (number | null)[];

export function ema(v: readonly number[], n: number): number[] {
  const k = 2 / (n + 1);
  const out: number[] = [];
  v.forEach((x, i) => {
    out.push(i === 0 ? x : x * k + (out[i - 1] ?? x) * (1 - k));
  });
  return out;
}

export function sma(v: readonly number[], n: number): S {
  let sum = 0;
  return v.map((x, i) => {
    sum += x;
    if (i >= n) sum -= v[i - n] ?? 0;
    return i >= n - 1 ? sum / n : null;
  });
}

export function macd(close: readonly number[]) {
  const e12 = ema(close, 12);
  const e26 = ema(close, 26);
  const dif = close.map((_, i) => (e12[i] ?? 0) - (e26[i] ?? 0));
  const dea = ema(dif, 9);
  return { dif, dea, hist: dif.map((d, i) => 2 * (d - (dea[i] ?? 0))) };
}

export function kdj(
  high: readonly number[],
  low: readonly number[],
  close: readonly number[],
  n = 9,
) {
  const k: number[] = [];
  const d: number[] = [];
  close.forEach((c, i) => {
    const from = Math.max(0, i - n + 1);
    const h = Math.max(...high.slice(from, i + 1));
    const l = Math.min(...low.slice(from, i + 1));
    const rsv = h === l ? 50 : ((c - l) / (h - l)) * 100;
    const kk = ((k[i - 1] ?? 50) * 2) / 3 + rsv / 3;
    k.push(kk);
    d.push(((d[i - 1] ?? 50) * 2) / 3 + kk / 3);
  });
  return { k, d, j: k.map((x, i) => 3 * x - 2 * (d[i] ?? 0)) };
}

/** Wilder RSI. */
export function rsi(close: readonly number[], n: number): S {
  let up = 0;
  let dn = 0;
  return close.map((c, i) => {
    if (i === 0) return null;
    const ch = c - (close[i - 1] ?? c);
    const u = Math.max(ch, 0);
    const d = Math.max(-ch, 0);
    if (i <= n) {
      up += u / n;
      dn += d / n;
      if (i < n) return null;
    } else {
      up = (up * (n - 1) + u) / n;
      dn = (dn * (n - 1) + d) / n;
    }
    return dn === 0 ? 100 : 100 - 100 / (1 + up / dn);
  });
}

export function boll(close: readonly number[], n = 20, k = 2) {
  const mid = sma(close, n);
  const up: S = [];
  const dn: S = [];
  close.forEach((_, i) => {
    const m = mid[i];
    if (m == null) {
      up.push(null);
      dn.push(null);
      return;
    }
    const w = close.slice(i - n + 1, i + 1);
    const sd = Math.sqrt(w.reduce((s, x) => s + (x - m) ** 2, 0) / n);
    up.push(m + k * sd);
    dn.push(m - k * sd);
  });
  return { mid, up, dn };
}

export function obv(close: readonly number[], vol: readonly number[]): number[] {
  let acc = 0;
  return close.map((c, i) => {
    const p = close[i - 1];
    if (p != null) acc += c > p ? (vol[i] ?? 0) : c < p ? -(vol[i] ?? 0) : 0;
    return acc;
  });
}
