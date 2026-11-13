import { useState } from "react";
import {
  type ScreenerBlock,
  type ScreenerUpstream,
  useDeleteScreenerScheme,
  useSaveScreenerScheme,
  useScreenerSchemes,
} from "@/api/endpoints";
import { screenerSchemesEnabled } from "@/features";
import { readPreference, writePreference } from "@/theme/storage";
import { Button, FailureNotice, useToast } from "@/ui";
import { type FailureCopy, failureCopy } from "@/ui/failure";
import {
  blocksToLegacyFilters,
  DEFAULT_UPSTREAM,
  type Filter,
  filtersToBlocks,
  type Rank,
} from "./Builder";

export interface Scheme {
  name: string;
  filters: Filter[];
  ranks: Rank[];
  upstream?: ScreenerUpstream;
  blocks?: ScreenerBlock[];
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

function normalizeUpstream(raw: unknown): ScreenerUpstream | undefined {
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) return undefined;
  const obj = raw as Record<string, unknown>;
  if (typeof obj.base !== "string") return undefined;
  return {
    base: obj.base as ScreenerUpstream["base"],
    preset: typeof obj.preset === "string" ? obj.preset : null,
    pool: typeof obj.pool === "string" ? obj.pool : null,
    include_pools: Array.isArray(obj.include_pools) ? (obj.include_pools as string[]) : [],
    intersect_pools: Array.isArray(obj.intersect_pools) ? (obj.intersect_pools as string[]) : [],
    exclude_pools: Array.isArray(obj.exclude_pools) ? (obj.exclude_pools as string[]) : [],
  };
}

export function Schemes(props: {
  blocks: ScreenerBlock[];
  upstream: ScreenerUpstream;
  ranks: Rank[];
  onLoad: (s: Scheme) => void;
}) {
  if (!screenerSchemesEnabled()) return null;
  return <SchemeEditor {...props} />;
}

/** Named condition schemes (persists upstream + blocks + ranks in SQLite Web Store). */
function SchemeEditor({
  blocks,
  upstream,
  ranks,
  onLoad,
}: {
  blocks: ScreenerBlock[];
  upstream: ScreenerUpstream;
  ranks: Rank[];
  onLoad: (s: Scheme) => void;
}) {
  const remote = useScreenerSchemes();
  const saveScheme = useSaveScreenerScheme();
  const removeScheme = useDeleteScreenerScheme();
  const toast = useToast();
  const [local, setLocal] = useState<Scheme[]>(loadSchemes);
  const [name, setName] = useState("");
  const [fail, setFail] = useState<FailureCopy | null>(null);
  const remoteSchemes = remote.data?.schemes;
  const list: Scheme[] = Array.isArray(remoteSchemes)
    ? remoteSchemes.map((s) => {
        const legacyFilters = (s.filters ?? []) as unknown as Filter[];
        const rawBlocks = Array.isArray(s.blocks) ? (s.blocks as unknown as ScreenerBlock[]) : [];
        return {
          name: s.name,
          filters: legacyFilters,
          ranks: (s.ranks ?? []) as unknown as Rank[],
          upstream: normalizeUpstream(s.upstream),
          blocks: rawBlocks.length > 0 ? rawBlocks : filtersToBlocks(legacyFilters),
        };
      })
    : local;
  const save = () => {
    const n = name.trim();
    if (!n) return;
    const legacyFilters = blocksToLegacyFilters(blocks);
    const scheme: Scheme = {
      name: n,
      filters: legacyFilters,
      ranks,
      upstream,
      blocks,
    };
    const body = {
      name: n,
      filters: legacyFilters as unknown as Record<string, unknown>[],
      ranks: ranks as unknown as Record<string, unknown>[],
      upstream: upstream as unknown as Record<string, unknown>,
      blocks: blocks as unknown as Record<string, unknown>[],
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
    const scheme = list.find((s) => s.name === n);
    const snapshot: Scheme | null = scheme
      ? {
          name: scheme.name,
          filters: scheme.filters.map((f) => ({ ...f })),
          ranks: scheme.ranks.map((r) => ({ ...r })),
          upstream: scheme.upstream ? { ...scheme.upstream } : undefined,
          blocks: scheme.blocks ? scheme.blocks.map((b) => ({ ...b })) : undefined,
        }
      : null;
    removeScheme.mutate(n, {
      onSuccess: () => {
        if (!snapshot) return;
        toast({
          text: `已删除方案『${snapshot.name}』`,
          duration: 6000,
          action: {
            label: "撤销",
            onClick: () => {
              saveScheme.mutate({
                name: snapshot.name,
                filters: snapshot.filters as unknown as Record<string, unknown>[],
                ranks: snapshot.ranks as unknown as Record<string, unknown>[],
                upstream: (snapshot.upstream ?? DEFAULT_UPSTREAM) as unknown as Record<
                  string,
                  unknown
                >,
                blocks: (snapshot.blocks ?? []) as unknown as Record<string, unknown>[],
              });
              const next = [...loadSchemes().filter((s) => s.name !== snapshot.name), snapshot];
              if (storeLocal(next)) setLocal(next);
            },
          },
        });
      },
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
          <button type="button" aria-label={`删除方案 ${s.name}`} onClick={() => remove(s.name)}>
            ×
          </button>
        </span>
      ))}
      <input
        aria-label="方案名"
        placeholder="命名当前范围与积木…"
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
