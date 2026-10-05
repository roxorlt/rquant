export function portfolioMoney(value: string | null | undefined): string {
  if (value == null) return "—";
  const [integer = "0", fraction = ""] = value.split(".");
  return `${integer.replace(/\B(?=(\d{3})+(?!\d))/g, ",")}.${fraction.padEnd(2, "0")}`;
}

export function portfolioPrice(value: string | null | undefined): string {
  if (value == null) return "—";
  const match = /^(\d+)(?:\.(\d*))?$/.exec(value);
  if (match === null) return "—";
  const integer = match[1] ?? "0";
  const fraction = match[2] ?? "";
  let cents = BigInt(integer) * 100n + BigInt(fraction.slice(0, 2).padEnd(2, "0"));
  if ((fraction[2] ?? "0") >= "5") cents += 1n;
  return portfolioMoney(`${cents / 100n}.${String(cents % 100n).padStart(2, "0")}`);
}

export function portfolioPercent(value: number | string | null | undefined): string {
  if (value == null) return "—";
  const number = Number(value);
  return Number.isFinite(number) ? `${(number * 100).toFixed(2)}%` : "—";
}

export function portfolioRatio(value: number | null | undefined): string {
  return value == null || !Number.isFinite(value) ? "—" : value.toFixed(2);
}
