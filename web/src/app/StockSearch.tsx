import { useEffect, useState } from "react";
import { type StockSearchRow, useStockSearch } from "@/api/endpoints";
import { Button, SkeletonRows } from "@/ui";
import { SearchIcon } from "./icons";
import { StockDrawer } from "./StockDrawer";

const OPTIONS_ID = "stock-search-options";

export function StockSearch() {
  const [draft, setDraft] = useState("");
  const [query, setQuery] = useState("");
  const [open, setOpen] = useState(false);
  const [active, setActive] = useState(-1);
  const [selectedCode, setSelectedCode] = useState<string | null>(null);
  const search = useStockSearch(query);
  const trimmed = draft.trim();
  const current = query === trimmed;
  const rows =
    current && !search.isLoading && !search.error && search.data?.available !== false
      ? (search.data?.rows ?? [])
      : [];
  const showing = open && trimmed.length > 0;

  useEffect(() => {
    if (!trimmed) {
      setQuery("");
      return;
    }
    const timer = window.setTimeout(() => setQuery(trimmed), 250);
    return () => window.clearTimeout(timer);
  }, [trimmed]);

  const choose = (row: StockSearchRow) => {
    setSelectedCode(row.ts_code);
    setDraft("");
    setQuery("");
    setOpen(false);
    setActive(-1);
  };

  return (
    <>
      {/* biome-ignore lint/a11y/useSemanticElements: <search> is newer than the Safari 15 build target. */}
      <form
        className="search"
        role="search"
        onSubmit={(event) => event.preventDefault()}
        onBlur={(event) => {
          if (!event.currentTarget.contains(event.relatedTarget)) {
            setOpen(false);
          }
        }}
      >
        <SearchIcon />
        <input
          type="search"
          role="combobox"
          aria-label="搜索股票"
          aria-autocomplete="list"
          aria-expanded={showing}
          aria-controls={showing ? OPTIONS_ID : undefined}
          aria-activedescendant={showing && rows[active] ? `stock-option-${active}` : undefined}
          autoComplete="off"
          maxLength={32}
          placeholder="代码 / 名称"
          value={draft}
          onFocus={() => setOpen(true)}
          onChange={(event) => {
            setDraft(event.target.value);
            setActive(-1);
            setOpen(true);
          }}
          onKeyDown={(event) => {
            if (event.key === "Escape") {
              setOpen(false);
              return;
            }
            if (!showing || !rows.length) {
              return;
            }
            if (event.key === "ArrowDown") {
              event.preventDefault();
              setActive((index) => (index + 1) % rows.length);
            } else if (event.key === "ArrowUp") {
              event.preventDefault();
              setActive((index) => (index <= 0 ? rows.length - 1 : index - 1));
            } else if (event.key === "Enter") {
              event.preventDefault();
              const row = rows[active >= 0 ? active : 0];
              if (row) {
                choose(row);
              }
            }
          }}
        />
        {showing ? (
          <div
            className="stock-search-pop"
            id={OPTIONS_ID}
            role="listbox"
            aria-label="股票搜索结果"
          >
            {!current || search.isLoading ? (
              <div className="stock-search-state" role="status" aria-label="正在搜索股票">
                <SkeletonRows rows={2} />
              </div>
            ) : search.error || search.data?.available === false ? (
              <div className="stock-search-state" role="alert" aria-label="搜索暂时无法加载">
                <span>搜索暂时无法加载</span>
                <Button size="sm" onClick={search.refetch}>
                  重试
                </Button>
              </div>
            ) : rows.length ? (
              <>
                {rows.map((row, index) => (
                  <button
                    type="button"
                    role="option"
                    id={`stock-option-${index}`}
                    aria-selected={index === active}
                    className="stock-search-option"
                    key={row.ts_code}
                    onClick={() => choose(row)}
                    onMouseEnter={() => setActive(index)}
                  >
                    <span>{row.name}</span>
                    <span className="mono">{row.ts_code}</span>
                  </button>
                ))}
                {search.data?.truncated ? (
                  <p className="stock-search-hint">只显示前 20 条，可继续输入缩小范围</p>
                ) : null}
              </>
            ) : (
              <p className="stock-search-state" role="status">
                没有找到股票
              </p>
            )}
          </div>
        ) : null}
      </form>
      <StockDrawer tsCode={selectedCode} onClose={() => setSelectedCode(null)} />
    </>
  );
}
