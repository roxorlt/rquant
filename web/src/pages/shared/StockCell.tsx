import { Tip } from "@/ui";

/** Name over code; a stock without a known name shows its code alone. */
export function StockCell({ code, name }: { code: string; name: string | null | undefined }) {
  if (!name) {
    return <span className="mono">{code}</span>;
  }
  return (
    <div className="cell2">
      <span className="nm">{name}</span>
      <span className="s mono">{code}</span>
    </div>
  );
}

/** A plain name with its technical key on hover (service ids, dataset ids). */
export function NamedKey({ name, techKey }: { name: string; techKey: string }) {
  return (
    <Tip content={<span className="tip-detail">{techKey}</span>}>
      <span className="nm">{name}</span>
    </Tip>
  );
}
