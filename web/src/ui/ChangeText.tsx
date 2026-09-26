import { toneClass, toneOf } from "@/format/color";
import { formatSignedPercent } from "@/format/number";

/** A signed percentage coloured red when up and green when down. */
export function ChangeText({ value, digits = 2 }: { value: number | null; digits?: number }) {
  const tone = toneOf(value);
  const className = ["num", toneClass(tone)].filter(Boolean).join(" ");
  return (
    <span className={className} data-tone={tone}>
      {formatSignedPercent(value, digits)}
    </span>
  );
}
