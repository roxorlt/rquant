/** "600519" / "600519.sh" → "600519.SH"; anything not code-like → null. */
export function fullCode(q: string): string | null {
  const t = q.trim().toUpperCase();
  if (/^\d{6}\.(SH|SZ|BJ)$/.test(t)) return t;
  if (!/^\d{6}$/.test(t)) return null;
  const head = t[0];
  return `${t}.${head === "6" || head === "9" ? "SH" : head === "4" || head === "8" ? "BJ" : "SZ"}`;
}
