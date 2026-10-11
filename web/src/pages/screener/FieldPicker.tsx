import { useEffect, useRef, useState } from "react";
import { groupsFor } from "./Builder";

/** Grouped, searchable field chooser (replaces a long flat <select>). */
export function FieldPicker({
  value,
  fields,
  onChange,
  label,
}: {
  value: string;
  fields: Record<string, string>;
  onChange: (v: string) => void;
  label: string;
}) {
  const [open, setOpen] = useState(false);
  const [q, setQ] = useState("");
  const ref = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (!open) return;
    const close = (e: MouseEvent) => {
      if (!ref.current?.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener("mousedown", close);
    return () => document.removeEventListener("mousedown", close);
  }, [open]);
  const needle = q.trim().toLowerCase();
  const groups = groupsFor(fields)
    .map(
      ([g, items]) =>
        [
          g,
          items.filter(([k, v]) => !needle || `${k} ${v} ${g}`.toLowerCase().includes(needle)),
        ] as const,
    )
    .filter(([, items]) => items.length > 0);
  const pick = (k: string) => {
    onChange(k);
    setOpen(false);
    setQ("");
  };
  return (
    <div className="fpick" ref={ref}>
      <button
        type="button"
        className="fpick-btn"
        aria-label={label}
        aria-expanded={open}
        onClick={() => setOpen(!open)}
      >
        {fields[value] ?? value}
        <span aria-hidden="true">▾</span>
      </button>
      {open ? (
        <div className="fpick-pop">
          <input
            // biome-ignore lint/a11y/noAutofocus: the popover exists to type a search
            autoFocus
            placeholder="搜索字段…"
            value={q}
            onChange={(e) => setQ(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Escape") setOpen(false);
              const first = groups[0]?.[1][0];
              if (e.key === "Enter" && first) pick(first[0]);
            }}
          />
          <div className="fpick-list">
            {groups.map(([g, items]) => (
              <div key={g}>
                <div className="fpick-g">
                  {g} <span className="hint">{items.length}</span>
                </div>
                {items.map(([k, v]) => (
                  <button
                    key={k}
                    type="button"
                    className={`fpick-item${k === value ? " on" : ""}`}
                    onClick={() => pick(k)}
                  >
                    {v.replace(/^因子·/, "")}
                  </button>
                ))}
              </div>
            ))}
            {groups.length === 0 ? <p className="hint">没有匹配的字段</p> : null}
          </div>
        </div>
      ) : null}
    </div>
  );
}
