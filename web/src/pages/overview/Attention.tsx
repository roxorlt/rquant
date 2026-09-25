import { Link } from "react-router";
import type { Schemas } from "@/api/client";
import { EmptyState, Pill } from "@/ui";

type Item = Schemas["AttentionItem"];

/** What needs a look, worst first, each with the one place to go. */
export function Attention({ items }: { items: readonly Item[] }) {
  if (items.length === 0) {
    return <EmptyState title="一切正常" hint="没有需要处理的事" />;
  }
  return (
    <ul className="attn">
      {items.map((item) => (
        <li key={`${item.level}-${item.title}`}>
          <Pill kind={item.level === "crit" ? "crit" : "warn"}>
            {item.level === "crit" ? "需处理" : "注意"}
          </Pill>
          <span className="txt cell2">
            <span className="nm">{item.title}</span>
            <span className="s">{item.reason}</span>
          </span>
          <Link className="btn sm" to={item.to}>
            {item.action}
          </Link>
        </li>
      ))}
    </ul>
  );
}
