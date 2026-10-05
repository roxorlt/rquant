/** Shift a decimal string for percent controls without rounding stored rules. */
function shiftDecimal(value: string, places: number): string {
  const match = /^([+-]?)(\d*)(?:\.(\d*))?(?:[eE]([+-]?\d+))?$/.exec(value);
  if (!match || !(match[2] || match[3])) return value;
  const sign = match[1] === "-" ? "-" : "";
  const fraction = match[3] ?? "";
  const digits = `${match[2] ?? ""}${fraction}`.replace(/^0+/, "") || "0";
  if (digits === "0") return "0";
  const exponent = Number(match[4] ?? 0) + places - fraction.length;
  const point = digits.length + exponent;
  if (point < -6 || point > 22 || !Number.isFinite(point))
    return `${sign}${digits[0]}${digits.length > 1 ? `.${digits.slice(1)}` : ""}e${point - 1}`;
  const shifted =
    point <= 0
      ? `0.${"0".repeat(-point)}${digits}`
      : point >= digits.length
        ? `${digits}${"0".repeat(point - digits.length)}`
        : `${digits.slice(0, point)}.${digits.slice(point)}`;
  return `${sign}${shifted.includes(".") ? shifted.replace(/0+$/, "").replace(/\.$/, "") : shifted}`;
}

export function rateValue(value: string | number | null | undefined): string {
  return value == null ? "" : shiftDecimal(String(value), 2);
}
export function decimalRate(value: string): string {
  return value === "" ? "" : shiftDecimal(value, -2);
}

export function positiveRate(value: string | number | null | undefined): boolean {
  return (
    value != null &&
    !String(value).startsWith("-") &&
    /[1-9]/.test(String(value).split(/[eE]/)[0] ?? "")
  );
}
