export interface Rect {
  x: number;
  y: number;
  w: number;
  h: number;
}

/** Squarified treemap (Bruls et al.): items sorted by weight desc → rectangles in the same order. */
export function squarify(weights: readonly number[], box: Rect): Rect[] {
  const total = weights.reduce((s, v) => s + v, 0);
  if (total <= 0 || weights.length === 0)
    return weights.map(() => ({ x: box.x, y: box.y, w: 0, h: 0 }));
  const scale = (box.w * box.h) / total;
  const areas = weights.map((v) => v * scale);
  const out: Rect[] = [];
  let rest = { ...box };
  let i = 0;
  const worst = (row: number[], side: number) => {
    const s = row.reduce((a, b) => a + b, 0);
    const mx = Math.max(...row);
    const mn = Math.min(...row);
    return Math.max((side * side * mx) / (s * s), (s * s) / (side * side * mn));
  };
  while (i < areas.length) {
    const side = Math.min(rest.w, rest.h);
    const row: number[] = [areas[i] ?? 0];
    let j = i + 1;
    while (j < areas.length) {
      const next = [...row, areas[j] ?? 0];
      if (worst(next, side) > worst(row, side)) break;
      row.push(areas[j] ?? 0);
      j += 1;
    }
    const s = row.reduce((a, b) => a + b, 0);
    if (rest.w >= rest.h) {
      const cw = s / rest.h;
      let y = rest.y;
      for (const a of row) {
        const h = a / cw;
        out.push({ x: rest.x, y, w: cw, h });
        y += h;
      }
      rest = { x: rest.x + cw, y: rest.y, w: rest.w - cw, h: rest.h };
    } else {
      const rh = s / rest.w;
      let x = rest.x;
      for (const a of row) {
        const w = a / rh;
        out.push({ x, y: rest.y, w, h: rh });
        x += w;
      }
      rest = { x: rest.x, y: rest.y + rh, w: rest.w, h: rest.h - rh };
    }
    i = j;
  }
  return out;
}
