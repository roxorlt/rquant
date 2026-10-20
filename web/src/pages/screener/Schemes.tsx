import { useState } from "react";
import { readPreference, writePreference } from "@/theme/storage";
import { Button, FailureNotice } from "@/ui";
import type { FailureCopy } from "@/ui/failure";
import type { Filter, Rank } from "./Builder";

export interface Scheme {
  name: string;
  filters: Filter[];
  ranks: Rank[];
}

const KEY = "screener.schemes";

export function loadSchemes(): Scheme[] {
  try {
    const raw = JSON.parse(readPreference(KEY) ?? "[]");
    return Array.isArray(raw) ? (raw as Scheme[]) : [];
  } catch {
    return [];
  }
}

function store(list: Scheme[]): boolean {
  writePreference(KEY, list.length ? JSON.stringify(list) : null);
  return list.length === 0 || readPreference(KEY) != null;
}

/** Named condition schemes kept in this browser: save the current set, load or delete one. */
export function Schemes({
  filters,
  ranks,
  onLoad,
}: {
  filters: Filter[];
  ranks: Rank[];
  onLoad: (s: Scheme) => void;
}) {
  const [list, setList] = useState<Scheme[]>(loadSchemes);
  const [name, setName] = useState("");
  const [fail, setFail] = useState<FailureCopy | null>(null);
  const save = () => {
    const n = name.trim();
    if (!n) return;
    const next = [...list.filter((s) => s.name !== n), { name: n, filters, ranks }];
    if (!store(next)) {
      setFail({
        text: `方案『${n}』没保存上：浏览器存不下这些条件。删掉几条旧方案后再试`,
        retry: true,
        health: false,
      });
      return;
    }
    setList(next);
    setName("");
    setFail(null);
  };
  const remove = (n: string) => {
    const next = list.filter((s) => s.name !== n);
    store(next);
    setList(next);
  };
  return (
    <div className="schemes">
      <span className="hint">方案</span>
      {list.length === 0 ? <span className="hint">（还没有保存的方案）</span> : null}
      {list.map((s) => (
        <span key={s.name} className="scheme-chip">
          <button type="button" onClick={() => onLoad(s)} title="载入此方案">
            {s.name}
          </button>
          <button type="button" aria-label={`删除方案 ${s.name}`} onClick={() => remove(s.name)}>
            ×
          </button>
        </span>
      ))}
      <input
        aria-label="方案名"
        placeholder="命名当前条件…"
        value={name}
        onChange={(e) => setName(e.target.value)}
        onKeyDown={(e) => {
          if (e.key === "Enter") save();
        }}
      />
      <Button size="sm" onClick={save} disabled={!name.trim()}>
        保存方案
      </Button>
      {fail ? <FailureNotice copy={fail} onRetry={save} /> : null}
    </div>
  );
}
