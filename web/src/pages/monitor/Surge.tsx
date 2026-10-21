import { useMemo, useState } from "react";
import { type SurgeEvent, useSurge } from "@/api/endpoints";
import { formatAmount, formatNumber, formatSignedPercent } from "@/format/number";
import { type DataColumn, DataTable, downloadCsv, toCsv } from "@/table/DataTable";
import { readPreference, writePreference } from "@/theme/storage";
import { DropdownMenu, HelpMark, Panel, Pill, Tip } from "@/ui";
import { StockLink } from "@/ui/StockLink";
import { PHONE_QUERY, useMedia } from "../panorama/useMedia";
import { QueryView } from "../shared";
import { type SurgeRules, surgeHelp, surgeSubtitle } from "./surgeCopy";

function rulesOf(data: { rules?: SurgeRules }): SurgeRules | null {
  return data.rules ?? null;
}

const COLUMNS: readonly DataColumn<SurgeEvent>[] = [
  { id: "time", header: "时间", sortable: true, value: (row) => row.time },
  {
    id: "code",
    header: "代码",
    sortable: true,
    value: (row) => row.code,
    cell: (row) => <StockLink code={row.code} />,
  },
  { id: "name", header: "名称", value: (row) => row.name ?? null },
  {
    id: "kind",
    header: "类型",
    value: (row) => row.kind,
    cell: (row) => <Pill kind={row.kind === "ratio" ? "warn" : "idle"}>{kindLabel(row.kind)}</Pill>,
  },
  {
    id: "ratio",
    header: "量比",
    numeric: true,
    sortable: true,
    value: (row) => row.ratio ?? null,
    cell: (row) => formatNumber(row.ratio, 1),
  },
  {
    id: "acc5",
    header: "5分加速",
    numeric: true,
    sortable: true,
    value: (row) => row.acc5 ?? null,
    cell: (row) => formatNumber(row.acc5, 1),
  },
  {
    id: "pct",
    header: "涨跌幅",
    numeric: true,
    sortable: true,
    value: (row) => row.pct ?? null,
    cell: (row) => formatSignedPercent(row.pct == null ? null : row.pct * 100),
  },
  {
    id: "amount",
    header: "成交额",
    numeric: true,
    sortable: true,
    value: (row) => row.amount ?? null,
    cell: (row) => formatAmount(row.amount),
  },
];

function kindLabel(kind: string): string {
  return kind === "ratio" ? "量比" : "加速";
}

function hiddenColumns(label: string): Set<string> {
  const saved = readPreference(`cols.${label}`);
  if (saved !== null) return new Set(saved.split(",").filter(Boolean));
  return new Set();
}

/** Column menu and CSV, without the row count. The phone list keeps this pair. */
function SurgeTools({ rows }: { rows: readonly SurgeEvent[] }) {
  const [hidden, setHidden] = useState(() => hiddenColumns("量能异动"));
  const visible = useMemo(() => COLUMNS.filter((column) => !hidden.has(column.id)), [hidden]);
  const toggle = (id: string) =>
    setHidden((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      writePreference("cols.量能异动", [...next].join(","));
      return next;
    });
  const sorted = [...rows].sort((a, b) => b.time.localeCompare(a.time, "zh-CN"));
  return (
    <>
      <DropdownMenu
        items={COLUMNS.map((column) => ({
          key: column.id,
          label: column.header,
          checked: !hidden.has(column.id),
          onSelect: () => toggle(column.id),
        }))}
      >
        <button type="button" className="tbl-tool">
          列{hidden.size ? `（隐藏 ${hidden.size}）` : ""} ▾
        </button>
      </DropdownMenu>
      <button
        type="button"
        className="tbl-tool"
        onClick={() => downloadCsv("异动", toCsv(visible, sorted))}
      >
        导出 CSV
      </button>
    </>
  );
}

function SurgeList({ rows }: { rows: readonly SurgeEvent[] }) {
  if (!rows.length) {
    return (
      <p className="hint" style={{ padding: "12px 16px" }}>
        今天还没有异动事件。
      </p>
    );
  }
  const sorted = [...rows].sort((a, b) => b.time.localeCompare(a.time, "zh-CN"));
  return (
    <ul className="li2-list" aria-label="量能异动">
      {sorted.map((row) => (
        <li key={`${row.code}-${row.kind}-${row.time}`} className="li2">
          <span className="t">
            <span className="name">{row.name ?? "—"}</span>
            <span className="code">
              <StockLink code={row.code} />
            </span>
          </span>
          <span className="meta">
            {row.time} · {kindLabel(row.kind)}
          </span>
          <span className="v">{formatNumber(row.ratio, 1)}</span>
        </li>
      ))}
    </ul>
  );
}

/** 量能异动: events from the snapshot collector's surge scanner (today, newest first). */
export function SurgePanel() {
  const query = useSurge();
  const phone = useMedia(PHONE_QUERY);
  return (
    <QueryView query={query}>
      {(data) => {
        const rules = rulesOf(data);
        const title = phone ? (
          <>
            <Tip content={rules ? surgeSubtitle(rules) : ""} className="has-tip">
              量能异动
            </Tip>
            {` · ${data.date ?? "暂无"} · ${data.items.length} 条`}
          </>
        ) : (
          `量能异动 · ${data.date ?? "暂无"} · ${data.items.length} 条`
        );
        return (
          <Panel
            title={title}
            sub={
              !phone && rules ? (
                <span className="surge-sub">
                  {surgeSubtitle(rules)}
                  <HelpMark label="量能异动规则">{surgeHelp(rules)}</HelpMark>
                </span>
              ) : null
            }
            actions={phone ? <SurgeTools rows={data.items} /> : undefined}
            flush
          >
            {phone ? (
              <SurgeList rows={data.items} />
            ) : (
              <DataTable<SurgeEvent>
                label="量能异动"
                tools="异动"
                rows={data.items}
                initialSort={{ id: "time", desc: true }}
                rowKey={(row) => `${row.code}-${row.kind}-${row.time}`}
                height={420}
                emptyText="今天还没有异动事件。"
                columns={COLUMNS}
              />
            )}
          </Panel>
        );
      }}
    </QueryView>
  );
}
