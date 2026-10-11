import { Link } from "react-router";

/** A stock code that opens its profile (F9-lite). Clicks don't select the table row. */
export function StockLink({ code }: { code: string }) {
  return (
    <Link
      className="stock-link mono"
      to={`/stock/${encodeURIComponent(code)}`}
      onClick={(e) => e.stopPropagation()}
    >
      {code}
    </Link>
  );
}
