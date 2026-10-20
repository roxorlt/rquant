import { useState } from "react";
import {
  useDeleteScreenerScheme,
  useSaveScreenerScheme,
  useScreenerSchemes,
} from "@/api/endpoints";
import { useReadOnlyReason } from "@/api/useMeta";
import { readPreference, writePreference } from "@/theme/storage";
import { Button, FailureNotice, useToast } from "@/ui";
import { type FailureCopy, failureCopy } from "@/ui/failure";
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

function storeLocal(list: Scheme[]): boolean {
  writePreference(KEY, list.length ? JSON.stringify(list) : null);
  return list.length === 0 || readPreference(KEY) != null;
}

/** Named condition schemes. The server is the copy that follows the account; this browser is only a fallback. */
export function Schemes({
  filters,
  ranks,
  onLoad,
}: {
  filters: Filter[];
  ranks: Rank[];
  onLoad: (s: Scheme) => void;
}) {
  const remote = useScreenerSchemes();
  const saveScheme = useSaveScreenerScheme();
  const removeScheme = useDeleteScreenerScheme();
  const readOnly = useReadOnlyReason();
  const toast = useToast();
  const [local, setLocal] = useState<Scheme[]>(loadSchemes);
  const [name, setName] = useState("");
  const [fail, setFail] = useState<FailureCopy | null>(null);
  const remoteSchemes = remote.data?.schemes;
  const list: Scheme[] = Array.isArray(remoteSchemes)
    ? remoteSchemes.map((s) => ({
        name: s.name,
        filters: s.filters as unknown as Filter[],
        ranks: s.ranks as unknown as Rank[],
      }))
    : local;
  const save = () => {
    const n = name.trim();
    if (!n || readOnly) return;
    const scheme = { name: n, filters, ranks };
    const body = {
      name: n,
      filters: filters as unknown as Record<string, unknown>[],
      ranks: ranks as unknown as Record<string, unknown>[],
    };
    saveScheme.mutate(body, {
      onSuccess: () => {
        setName("");
        setFail(null);
        toast(`已保存方案『${n}』`);
      },
      onError: (error) => {
        const next = [...local.filter((s) => s.name !== n), scheme];
        if (storeLocal(next)) setLocal(next);
        setFail(failureCopy(`方案『${n}』`, "保存", error));
      },
    });
  };
  const remove = (n: string) => {
    if (readOnly) return;
    removeScheme.mutate(n, {
      onError: () => {
        const next = local.filter((s) => s.name !== n);
        storeLocal(next);
        setLocal(next);
      },
    });
    if (!remote.data) {
      const next = local.filter((s) => s.name !== n);
      storeLocal(next);
      setLocal(next);
    }
  };
  return (
    <div className="schemes">
      <span className="hint">方案</span>
      {list.length === 0 ? <span className="hint">还没有保存的方案</span> : null}
      {list.map((s) => (
        <span key={s.name} className="scheme-chip">
          <button type="button" onClick={() => onLoad(s)} title="载入此方案">
            {s.name}
          </button>
          <button
            type="button"
            aria-label={`删除方案 ${s.name}`}
            aria-disabled={readOnly ? true : undefined}
            onClick={() => remove(s.name)}
          >
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
      <Button
        size="sm"
        onClick={save}
        disabled={!readOnly && !name.trim()}
        ariaDisabled={!!readOnly}
        aria-describedby={readOnly ? "readonly-notice" : undefined}
      >
        保存方案
      </Button>
      {fail ? <FailureNotice copy={fail} onRetry={save} /> : null}
    </div>
  );
}
