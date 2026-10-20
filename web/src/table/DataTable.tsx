import {
  type ColumnDef,
  flexRender,
  getCoreRowModel,
  getSortedRowModel,
  type Row,
  type SortingState,
  useReactTable,
} from "@tanstack/react-table";
import { useVirtualizer } from "@tanstack/react-virtual";
import { type KeyboardEvent, type ReactNode, useEffect, useMemo, useRef, useState } from "react";
import { readPreference, writePreference } from "@/theme/storage";
import { DropdownMenu } from "@/ui/DropdownMenu";

export type CellValue = string | number | null;

export interface DataColumn<T> {
  id: string;
  header: string;
  /** Value used for sorting and, without `cell`, for display. */
  value: (row: T) => CellValue;
  cell?: (row: T) => ReactNode;
  /** Right-aligned number column. */
  numeric?: boolean;
  /** Long text that wraps instead of widening the table. */
  wrap?: boolean;
  sortable?: boolean;
  /** Secondary columns are hidden on phones (≤ 599 px) to keep rows one-hand readable. */
  secondary?: boolean;
  /** Hidden until the viewer turns the column on. Ignored once a preference is saved. */
  hiddenByDefault?: boolean;
}

export interface DataTableProps<T> {
  rows: readonly T[];
  columns: readonly DataColumn<T>[];
  rowKey: (row: T) => string;
  /** Accessible name of the table. */
  label: string;
  initialSort?: { id: string; desc: boolean };
  /** Enables single-row selection (click, Enter or Space; arrows move). */
  onSelect?: (row: T) => void;
  selectedKey?: string | null;
  emptyText?: ReactNode;
  /**
   * Scroll height in px. With a height the header sticks and, above
   * `virtualizeFrom` rows, only the visible rows are rendered.
   */
  height?: number;
  virtualizeFrom?: number;
  estimatedRowHeight?: number;
  /**
   * A small toolbar: show/hide columns (remembered per table label) and CSV
   * export of the visible columns in the current sort order. A string names the file.
   */
  tools?: boolean | string;
  /** Keep the first and last columns in view while the rest scroll sideways. */
  pinEnds?: boolean;
  /** Phone and coarse pointers sort from a select; the header buttons stay for a mouse. */
  phoneSort?: boolean;
}

/** CSV text (with BOM so Excel reads UTF-8) of the given columns and rows. */
export function toCsv<T>(columns: readonly DataColumn<T>[], rows: readonly T[]): string {
  const esc = (v: CellValue) => {
    if (v === null) return "";
    const s = String(v);
    return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
  };
  const lines = [columns.map((c) => esc(c.header)).join(",")];
  for (const row of rows) lines.push(columns.map((c) => esc(c.value(row))).join(","));
  return `\uFEFF${lines.join("\n")}\n`;
}

function download(name: string, text: string) {
  const url = URL.createObjectURL(new Blob([text], { type: "text/csv;charset=utf-8" }));
  const a = document.createElement("a");
  a.href = url;
  a.download = name.endsWith(".csv") ? name : `${name}.csv`;
  a.click();
  URL.revokeObjectURL(url);
}

/** Nulls always sort after real values, whatever the direction. */
export function compareCells(a: CellValue, b: CellValue): number {
  if (a === b) {
    return 0;
  }
  if (a === null) {
    return 1;
  }
  if (b === null) {
    return -1;
  }
  if (typeof a === "number" && typeof b === "number") {
    return a - b;
  }
  return String(a).localeCompare(String(b), "zh-CN");
}

const ARIA_SORT = { asc: "ascending", desc: "descending" } as const;

/**
 * The one table: the prototype's .tbl look over TanStack Table (sorting,
 * single selection, keyboard) and TanStack Virtual for long lists. Pages never
 * use TanStack directly.
 */
export function DataTable<T>({
  rows,
  columns: allColumns,
  rowKey,
  label,
  initialSort,
  onSelect,
  selectedKey = null,
  emptyText = "暂无数据",
  height,
  virtualizeFrom = 200,
  estimatedRowHeight = 37,
  tools,
  pinEnds = false,
  phoneSort = false,
}: DataTableProps<T>) {
  const prefKey = `cols.${label}`;
  const [hidden, setHidden] = useState<Set<string>>(() => {
    const saved = tools ? readPreference(prefKey) : null;
    if (saved !== null) return new Set(saved.split(",").filter(Boolean));
    return new Set(
      allColumns.filter((column) => column.hiddenByDefault).map((column) => column.id),
    );
  });
  const toggleColumn = (id: string) =>
    setHidden((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      writePreference(prefKey, next.size ? [...next].join(",") : null);
      return next;
    });
  const columns = useMemo(() => allColumns.filter((c) => !hidden.has(c.id)), [allColumns, hidden]);
  const [sorting, setSorting] = useState<SortingState>(initialSort ? [initialSort] : []);
  const scrollRef = useRef<HTMLDivElement>(null);
  const [overflowing, setOverflowing] = useState(false);
  // Column count changes scrollWidth without resizing the wrap, so it is a dependency.
  // biome-ignore lint/correctness/useExhaustiveDependencies: remeasure when columns or rows change
  useEffect(() => {
    const el = scrollRef.current;
    if (!el || !pinEnds) return;
    const measure = () => {
      const first = el.querySelector("tbody td");
      if (first) el.style.setProperty("--pin-1", `${first.getBoundingClientRect().width}px`);
      setOverflowing(el.scrollWidth > el.clientWidth + 1);
    };
    measure();
    const observer = new ResizeObserver(measure);
    observer.observe(el);
    return () => observer.disconnect();
  }, [pinEnds, rows.length, columns.length]);
  const bodyRef = useRef<HTMLTableSectionElement>(null);

  const columnDefs = useMemo<ColumnDef<T>[]>(
    () =>
      columns.map((column) => ({
        id: column.id,
        header: column.header,
        // null → undefined so TanStack's sortUndefined keeps empty cells last both ways.
        accessorFn: (row: T) => column.value(row) ?? undefined,
        cell: (context) =>
          column.cell
            ? column.cell(context.row.original)
            : (column.value(context.row.original) ?? "—"),
        // Numbers are what quant users rank by: numeric columns sort unless opted out.
        enableSorting: column.sortable ?? column.numeric ?? false,
        sortDescFirst: false,
        sortUndefined: "last" as const,
        sortingFn: (left: Row<T>, right: Row<T>) =>
          compareCells(column.value(left.original), column.value(right.original)),
      })),
    [columns],
  );

  const table = useReactTable<T>({
    data: rows as T[],
    columns: columnDefs,
    state: { sorting },
    onSortingChange: setSorting,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
    getRowId: (row) => rowKey(row),
  });

  const byId = useMemo(() => new Map(columns.map((column) => [column.id, column])), [columns]);
  const cellClass = (id: string): string | undefined => {
    const column = byId.get(id);
    const classes = [
      column?.numeric ? "num" : column?.wrap ? "wrap" : "",
      column?.secondary ? "col-secondary" : "",
    ].filter(Boolean);
    return classes.length ? classes.join(" ") : undefined;
  };

  const sortedRows = table.getRowModel().rows;
  const virtual = height !== undefined && sortedRows.length > virtualizeFrom;
  const virtualizer = useVirtualizer({
    count: virtual ? sortedRows.length : 0,
    getScrollElement: () => scrollRef.current,
    estimateSize: () => estimatedRowHeight,
    overscan: 12,
  });
  const items = virtual ? virtualizer.getVirtualItems() : [];
  const visible = virtual
    ? items.flatMap((item) => {
        const row = sortedRows[item.index];
        return row ? [{ row, index: item.index }] : [];
      })
    : sortedRows.map((row, index) => ({ row, index }));
  const padTop = virtual ? (items[0]?.start ?? 0) : 0;
  const padBottom = virtual ? virtualizer.getTotalSize() - (items[items.length - 1]?.end ?? 0) : 0;

  const rowElement = (index: number) =>
    bodyRef.current?.querySelector<HTMLTableRowElement>(`tr[data-index="${index}"]`) ?? null;

  const focusRow = (index: number) => {
    const clamped = Math.max(0, Math.min(index, sortedRows.length - 1));
    const rendered = rowElement(clamped);
    if (rendered !== null) {
      rendered.focus();
      return;
    }
    // Virtualized and off screen: scroll it into the window first.
    virtualizer.scrollToIndex(clamped);
    window.requestAnimationFrame(() => rowElement(clamped)?.focus());
  };

  const onRowKeyDown = (event: KeyboardEvent<HTMLTableRowElement>, index: number, row: T) => {
    if (event.key === "ArrowDown") {
      event.preventDefault();
      focusRow(index + 1);
    } else if (event.key === "ArrowUp") {
      event.preventDefault();
      focusRow(index - 1);
    } else if (event.key === "Enter" || event.key === " ") {
      event.preventDefault();
      onSelect?.(row);
    }
  };

  const selectable = onSelect !== undefined;

  const sortChoices = columns.filter(
    (column) => (column.sortable ?? column.numeric ?? false) && column.header,
  );
  const sortValue = sorting[0] ? `${sorting[0].id}:${sorting[0].desc ? "desc" : "asc"}` : "";
  const phoneSortSelect =
    phoneSort && sortChoices.length ? (
      <select
        className="select tbl-sort"
        aria-label="排序"
        value={sortValue}
        onChange={(event) => {
          const [id, dir] = event.target.value.split(":");
          if (id) setSorting([{ id, desc: dir === "desc" }]);
        }}
      >
        {sortChoices.flatMap((column) => [
          <option key={`${column.id}:desc`} value={`${column.id}:desc`}>
            {column.header} 降序
          </option>,
          <option key={`${column.id}:asc`} value={`${column.id}:asc`}>
            {column.header} 升序
          </option>,
        ])}
      </select>
    ) : null;

  const toolbar = tools ? (
    <div className="tbl-tools">
      <span className="tbl-count">{sortedRows.length} 行</span>
      <DropdownMenu
        items={allColumns
          .filter((c) => c.header)
          .map((c) => ({
            key: c.id,
            label: c.header,
            checked: !hidden.has(c.id),
            onSelect: () => toggleColumn(c.id),
          }))}
      >
        <button type="button" className="tbl-tool">
          列{hidden.size ? `（隐藏 ${hidden.size}）` : ""} ▾
        </button>
      </DropdownMenu>
      <button
        type="button"
        className="tbl-tool"
        onClick={() =>
          download(
            typeof tools === "string" ? tools : label,
            toCsv(
              columns.filter((c) => c.header),
              sortedRows.map((r) => r.original),
            ),
          )
        }
      >
        导出 CSV
      </button>
    </div>
  ) : null;

  return (
    <>
      {toolbar}
      {phoneSortSelect}
      <div
        ref={scrollRef}
        className={`tbl-wrap${height === undefined ? "" : " scroll"}${pinEnds ? " pin-cols" : ""}`}
        style={height === undefined ? undefined : { maxHeight: height }}
      >
        <table className="tbl" aria-label={label} aria-rowcount={sortedRows.length + 1}>
          <thead>
            {table.getHeaderGroups().map((group) => (
              <tr key={group.id}>
                {group.headers.map((header) => {
                  const sorted = header.column.getIsSorted();
                  return (
                    <th
                      key={header.id}
                      scope="col"
                      className={cellClass(header.column.id)}
                      aria-sort={sorted ? ARIA_SORT[sorted] : undefined}
                    >
                      {header.column.getCanSort() ? (
                        <button
                          type="button"
                          className="sort"
                          onClick={header.column.getToggleSortingHandler()}
                        >
                          {flexRender(header.column.columnDef.header, header.getContext())}
                          <span aria-hidden="true">
                            {sorted === "asc" ? "↑" : sorted === "desc" ? "↓" : "↕"}
                          </span>
                        </button>
                      ) : (
                        flexRender(header.column.columnDef.header, header.getContext())
                      )}
                    </th>
                  );
                })}
              </tr>
            ))}
          </thead>
          <tbody ref={bodyRef}>
            {sortedRows.length === 0 ? (
              <tr>
                <td colSpan={columns.length} className="tbl-empty">
                  {emptyText}
                </td>
              </tr>
            ) : null}
            {padTop > 0 ? (
              <tr className="pad">
                <td colSpan={columns.length} style={{ height: padTop }} />
              </tr>
            ) : null}
            {visible.map(({ row, index }) => (
              <tr
                key={row.id}
                data-index={index}
                aria-rowindex={index + 2}
                className={selectable ? "click" : undefined}
                aria-selected={selectable ? row.id === selectedKey : undefined}
                tabIndex={selectable ? 0 : undefined}
                data-row-key={row.id}
                onClick={
                  selectable
                    ? (event) => {
                        event.currentTarget.focus();
                        onSelect(row.original);
                      }
                    : undefined
                }
                onKeyDown={
                  selectable ? (event) => onRowKeyDown(event, index, row.original) : undefined
                }
              >
                {row.getVisibleCells().map((cell) => (
                  <td key={cell.id} className={cellClass(cell.column.id)}>
                    {flexRender(cell.column.columnDef.cell, cell.getContext())}
                  </td>
                ))}
              </tr>
            ))}
            {padBottom > 0 ? (
              <tr className="pad">
                <td colSpan={columns.length} style={{ height: padBottom }} />
              </tr>
            ) : null}
          </tbody>
        </table>
      </div>
      {pinEnds && overflowing ? <p className="tbl-scroll-hint">左右滑动查看全部列</p> : null}
    </>
  );
}
