import { useEffect, useMemo, useState } from "react";
import { type SurgeRow, useSurge, useSurgeSearch } from "@/api/endpoints";
import { formatPrice } from "@/format/number";
import { formatTradeDate } from "@/format/time";
import { type DataColumn, DataTable } from "@/table/DataTable";
import {
  ChangeText,
  DatePicker,
  EmptyState,
  Panel,
  Pill,
  SearchInput,
  SkeletonRows,
  Tip,
} from "@/ui";
import { StockCell } from "../shared/StockCell";
import { fixed, yi } from "./format";
import { LoadError } from "./LoadError";
import { SessionChart } from "./StockChart";

function columns(withDate: boolean): DataColumn<SurgeRow>[] {
  const list: (DataColumn<SurgeRow> | false)[] = [
    withDate && {
      id: "date",
      header: "日期",
      value: (row) => row.trade_date,
      cell: (row) => <span className="num">{formatTradeDate(row.trade_date)}</span>,
      sortable: true,
    },
    {
      id: "time",
      header: "时间",
      value: (row) => row.confirmed_at,
      cell: (row) => <span className="num">{row.confirmed_at}</span>,
      sortable: true,
    },
    {
      id: "stock",
      header: "股票",
      value: (row) => row.name ?? row.ts_code,
      cell: (row) => <StockCell code={row.ts_code} name={row.name} />,
    },
    { id: "theme", header: "题材", value: (row) => row.theme, secondary: true },
    {
      id: "price",
      header: "推送价",
      value: (row) => row.price,
      cell: (row) => formatPrice(row.price),
      numeric: true,
      secondary: true,
    },
    {
      id: "pct",
      header: "涨幅",
      value: (row) => row.pct_chg,
      cell: (row) => <ChangeText value={row.pct_chg} />,
      numeric: true,
      sortable: true,
    },
    {
      id: "rel",
      header: "累计放量",
      value: (row) => row.rel_cum,
      cell: (row) => (row.rel_cum === null ? "—" : `${fixed(row.rel_cum)}×`),
      numeric: true,
      sortable: true,
    },
    {
      id: "cum",
      header: "累计额（亿）",
      value: (row) => row.cum_amount,
      cell: (row) => yi(row.cum_amount),
      numeric: true,
      secondary: true,
    },
    {
      id: "room",
      header: "距涨停",
      value: (row) => row.room_to_limit_pct,
      cell: (row) => (row.room_to_limit_pct === null ? "—" : `${fixed(row.room_to_limit_pct, 1)}%`),
      numeric: true,
      secondary: true,
    },
    {
      id: "status",
      header: "状态",
      value: (row) => row.status_label,
      cell: (row) =>
        row.status === "unbuyable" ? (
          <Pill kind="crit">{row.status_label}</Pill>
        ) : (
          <Tip content="观察提示，不是买入信号">
            <Pill kind="acc">{row.status_label}</Pill>
          </Tip>
        ),
    },
  ];
  return list.filter((column): column is DataColumn<SurgeRow> => column !== false);
}

const rowKey = (row: SurgeRow) => `${row.trade_date}-${row.ts_code}-${row.confirmed_at}`;

export function SurgeTab({ today }: { today: string | null }) {
  const [query, setQuery] = useState("");
  const [date, setDate] = useState<string | null>(null);
  const [selected, setSelected] = useState<SurgeRow | null>(null);
  const day = useSurge(date);
  const search = useSurgeSearch(query);
  const searching = query.length > 0;
  const rows = (searching ? search.data?.rows : day.data?.rows) ?? [];
  const loading = searching ? search.isLoading : day.isLoading;
  const activeError = searching ? search.error : day.error;
  const retry = searching ? search.refetch : day.refetch;
  const shownDate = day.data?.trade_date ?? date;
  const tableColumns = useMemo(() => columns(searching), [searching]);
  // biome-ignore lint/correctness/useExhaustiveDependencies: a new list clears the selection.
  useEffect(() => setSelected(null), [query, date]);

  let empty = <EmptyState title="没有爆量记录" />;
  if (searching) {
    empty = <EmptyState title={`没有找到「${query}」的爆量记录`} />;
  } else if (shownDate && shownDate === today) {
    empty = <EmptyState title="今天还没有爆量记录" hint="盘中识别到后自动出现" />;
  } else if (shownDate) {
    empty = <EmptyState title={`${formatTradeDate(shownDate)} 没有爆量记录`} />;
  }

  const config = day.data?.config;
  return (
    <div className="stack">
      <Panel
        title="爆量记录"
        sub={
          searching && !search.error && !search.isLoading
            ? `跨日找到 ${rows.length} 条${search.data?.truncated ? "（只显示最近的）" : ""}`
            : undefined
        }
        actions={
          <div className="panel-tools">
            <SearchInput
              value={query}
              onSearch={setQuery}
              placeholder="代码或名称，跨天找"
              label="按代码或名称搜索爆量记录"
            />
            {searching ? null : (
              <DatePicker
                value={shownDate}
                onChange={setDate}
                allowed={day.data?.dates ?? []}
                label="选择日期"
              />
            )}
          </div>
        }
        flush
      >
        {loading ? (
          <div className="panel-b">
            <SkeletonRows rows={6} />
          </div>
        ) : activeError ? (
          <LoadError label={searching ? "搜索结果" : "爆量记录"} onRetry={retry} />
        ) : (
          <DataTable
            label="爆量记录"
            rows={rows}
            columns={tableColumns}
            rowKey={rowKey}
            onSelect={setSelected}
            selectedKey={selected ? rowKey(selected) : null}
            height={360}
            emptyText={empty}
          />
        )}
        <p className="surge-footnote">
          统计口径：盘中累计放量，仅供观察。{" "}
          <Tip content={config?.summary ?? "检测详情暂不可用"}>
            <span className="has-tip">查看详情</span>
          </Tip>
          {day.data && day.data.dates.length === 0 ? " · 日期暂不可选，请稍后重试" : null}
        </p>
      </Panel>
      {selected ? (
        <Panel
          title={`${selected.name ?? selected.ts_code} · ${formatTradeDate(selected.trade_date)}`}
          sub="当天完整分钟走势，橙点和竖线为每一次爆量确认"
        >
          <SessionChart
            tsCode={selected.ts_code}
            days={1}
            date={selected.trade_date}
            label={`${selected.name ?? selected.ts_code} ${selected.trade_date} 分时`}
          />
        </Panel>
      ) : rows.length ? (
        <p className="hint pick-hint">点一条记录，查看当天的分钟走势和全部爆量确认点</p>
      ) : null}
    </div>
  );
}
