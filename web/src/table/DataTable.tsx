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
import { type KeyboardEvent, type ReactNode, useMemo, useRef, useState } from "react";

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
  emptyText?: string;
  /**
   * Scroll height in px. With a height the header sticks and, above
   * `virtualizeFrom` rows, only the visible rows are rendered.
   */
  height?: number;
  virtualizeFrom?: number;
  estimatedRowHeight?: number;
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
  columns,
  rowKey,
  label,
  initialSort,
  onSelect,
  selectedKey = null,
  emptyText = "暂无数据",
  height,
  virtualizeFrom = 200,
  estimatedRowHeight = 37,
}: DataTableProps<T>) {
  const [sorting, setSorting] = useState<SortingState>(initialSort ? [initialSort] : []);
  const scrollRef = useRef<HTMLDivElement>(null);
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
        enableSorting: column.sortable ?? false,
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
    if (column?.numeric) {
      return "num";
    }
    return column?.wrap ? "wrap" : undefined;
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

  return (
    <div
      ref={scrollRef}
      className={height === undefined ? "tbl-wrap" : "tbl-wrap scroll"}
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
                    className={byId.get(header.column.id)?.numeric ? "num" : undefined}
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
              <td colSpan={columns.length} className="muted">
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
              onClick={selectable ? () => onSelect(row.original) : undefined}
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
  );
}
