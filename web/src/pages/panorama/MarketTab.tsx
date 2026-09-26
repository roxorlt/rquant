import { useEffect, useMemo, useState } from "react";
import { type BoardRow, type MemberRow, useBoards, useMembers, usePulse } from "@/api/endpoints";
import { toneClass, toneOf } from "@/format/color";
import { formatCount, formatPercent, formatPrice } from "@/format/number";
import { type DataColumn, DataTable } from "@/table/DataTable";
import { readPreference, writePreference } from "@/theme/storage";
import {
  ChangeText,
  EmptyState,
  Panel,
  Pill,
  Segmented,
  SkeletonKpis,
  SkeletonRows,
  Tip,
} from "@/ui";
import { StockCell } from "../shared/StockCell";
import { fixed, yi } from "./format";
import { LoadError } from "./LoadError";
import { PulseRow } from "./PulseRow";
import { StockChart } from "./StockChart";

const SYSTEM_KEY = "panorama.system";
const DEFAULT_SYSTEM = "开盘啦题材";

function signedYi(value: number | null) {
  return <span className={`num ${toneClass(toneOf(value))}`}>{yi(value)}</span>;
}

function boardColumns(hasFlow: boolean): DataColumn<BoardRow>[] {
  const columns: (DataColumn<BoardRow> | false)[] = [
    {
      id: "name",
      header: "板块",
      value: (row) => row.board_name,
      cell: (row) => <span className="nm">{row.board_name}</span>,
    },
    {
      id: "amount",
      header: "成交额（亿）",
      value: (row) => row.amount,
      cell: (row) => yi(row.amount),
      numeric: true,
      sortable: true,
    },
    hasFlow && {
      id: "flow",
      header: "净流入（亿）",
      value: (row) => row.main_net_amount,
      cell: (row) => signedYi(row.main_net_amount),
      numeric: true,
      sortable: true,
    },
    hasFlow && {
      id: "flow_rate",
      header: "净流率",
      value: (row) => row.main_net_rate,
      cell: (row) => <ChangeText value={row.main_net_rate} />,
      numeric: true,
      sortable: true,
      secondary: true,
    },
    {
      id: "median",
      header: "涨跌中位",
      value: (row) => row.pct_chg_median,
      cell: (row) => <ChangeText value={row.pct_chg_median} />,
      numeric: true,
      sortable: true,
    },
    {
      id: "limit_up",
      header: "涨停",
      value: (row) => row.limit_up_count,
      cell: (row) => (row.limit_up_count === null ? "—" : formatCount(row.limit_up_count)),
      numeric: true,
      sortable: true,
    },
    {
      id: "broken",
      header: "炸板",
      value: (row) => row.broken_count,
      cell: (row) => (row.broken_count === null ? "—" : formatCount(row.broken_count)),
      numeric: true,
      sortable: true,
      secondary: true,
    },
    {
      id: "ratio",
      header: "涨停占比",
      value: (row) => row.limit_up_ratio_pct,
      cell: (row) => formatPercent(row.limit_up_ratio_pct, 1),
      numeric: true,
      sortable: true,
      secondary: true,
    },
    {
      id: "count",
      header: "成分",
      value: (row) => row.stock_count,
      cell: (row) => (row.stock_count === null ? "—" : formatCount(row.stock_count)),
      numeric: true,
      sortable: true,
      secondary: true,
    },
    hasFlow && {
      id: "leader",
      header: "主力流入最多",
      value: (row) => row.leading_stock,
      secondary: true,
    },
  ];
  return columns.filter((column): column is DataColumn<BoardRow> => column !== false);
}

const MEMBER_COLUMNS: DataColumn<MemberRow>[] = [
  {
    id: "stock",
    header: "股票",
    value: (row) => row.name ?? row.ts_code,
    cell: (row) => <StockCell code={row.ts_code} name={row.name} />,
  },
  {
    id: "marks",
    header: "标记",
    value: (row) => (row.is_limit_up ? 1 : 0) + row.pools.length,
    cell: (row) => (
      <span className="tags">
        {row.is_limit_up ? <Pill kind="crit">涨停</Pill> : null}
        {row.pools.map((pool) => (
          <Pill key={pool} kind="acc">
            {pool}
          </Pill>
        ))}
      </span>
    ),
  },
  {
    id: "price",
    header: "现价",
    value: (row) => row.price,
    cell: (row) => formatPrice(row.price),
    numeric: true,
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
    id: "amount",
    header: "成交额（亿）",
    value: (row) => row.amount,
    cell: (row) => yi(row.amount),
    numeric: true,
    sortable: true,
    secondary: true,
  },
  {
    id: "strength",
    header: "强度",
    value: (row) => row.strength,
    cell: (row) => fixed(row.strength, 1),
    numeric: true,
    sortable: true,
  },
  {
    id: "turnover",
    header: "换手强度",
    value: (row) => row.turnover_pct,
    cell: (row) => fixed(row.turnover_pct),
    numeric: true,
    sortable: true,
    secondary: true,
  },
  {
    id: "relvol",
    header: "相对放量",
    value: (row) => row.rel_volume_5d,
    cell: (row) => fixed(row.rel_volume_5d),
    numeric: true,
    sortable: true,
    secondary: true,
  },
];

function BoardPanel({
  system,
  onSystem,
  selected,
  onSelect,
}: {
  system: string;
  onSystem: (system: string) => void;
  selected: string | null;
  onSelect: (row: BoardRow | null) => void;
}) {
  const boards = useBoards(system);
  const rows = boards.data?.rows ?? [];
  const systems = boards.data?.systems ?? ["东财行业", "东财概念", DEFAULT_SYSTEM];
  const columns = useMemo(
    () => boardColumns(boards.data?.has_flow ?? false),
    [boards.data?.has_flow],
  );
  // The first board (most limit-ups) is selected until the reader picks one.
  useEffect(() => {
    if (rows.length && !rows.some((row) => row.board_code === selected)) {
      onSelect(rows[0] ?? null);
    }
    if (!rows.length && !boards.isLoading && selected !== null) {
      onSelect(null);
    }
  }, [rows, selected, onSelect, boards.isLoading]);
  return (
    <Panel
      title="板块"
      sub={boards.error ? undefined : rows.length ? `${rows.length} 个 · 按涨停数排序` : undefined}
      actions={
        <Segmented
          label="板块体系"
          options={systems.map((item) => ({ value: item, label: item }))}
          value={system}
          onChange={onSystem}
        />
      }
      flush
    >
      {boards.isLoading ? (
        <div className="panel-b">
          <SkeletonRows rows={8} />
        </div>
      ) : boards.error ? (
        <LoadError label="板块" onRetry={boards.refetch} />
      ) : (
        <DataTable
          label="板块"
          rows={rows}
          columns={columns}
          rowKey={(row) => row.board_code}
          onSelect={onSelect}
          selectedKey={selected}
          height={520}
          emptyText={
            <EmptyState title="暂时没有板块数据" hint="盘中每分钟更新，收盘后显示当天收盘数据" />
          }
        />
      )}
    </Panel>
  );
}

function MembersPanel({
  board,
  selected,
  onSelect,
}: {
  board: BoardRow | null;
  selected: string | null;
  onSelect: (row: MemberRow | null) => void;
}) {
  const members = useMembers(board?.board_code ?? null);
  const rows = members.data?.rows ?? [];
  useEffect(() => {
    if (board === null) {
      onSelect(null);
      return;
    }
    if (rows.length && !rows.some((row) => row.ts_code === selected)) {
      onSelect(rows[0] ?? null);
    }
  }, [board, rows, selected, onSelect]);
  return (
    <Panel
      title={board ? `「${board.board_name}」成分` : "成分"}
      sub={
        rows.length && !members.error ? (
          <Tip content="强度：板块内按涨幅、换手、相对放量和离涨停的距离综合排名，0–100">
            <span className="has-tip">按强度排序</span>
          </Tip>
        ) : undefined
      }
      flush
    >
      {board === null ? (
        <EmptyState title="先在左边选一个板块" />
      ) : members.isLoading ? (
        <div className="panel-b">
          <SkeletonRows rows={5} />
        </div>
      ) : members.error ? (
        <LoadError label="板块成分" onRetry={members.refetch} />
      ) : (
        <DataTable
          label="板块成分"
          rows={rows}
          columns={MEMBER_COLUMNS}
          rowKey={(row) => row.ts_code}
          onSelect={onSelect}
          selectedKey={selected}
          height={300}
          emptyText={<EmptyState title="这个板块的成分暂时没有行情" />}
        />
      )}
    </Panel>
  );
}

export function MarketTab() {
  const pulse = usePulse();
  const [system, setSystem] = useState(() => readPreference(SYSTEM_KEY) ?? DEFAULT_SYSTEM);
  const [board, setBoard] = useState<BoardRow | null>(null);
  const [stock, setStock] = useState<MemberRow | null>(null);
  const changeSystem = (next: string) => {
    writePreference(SYSTEM_KEY, next);
    setSystem(next);
    setBoard(null);
    setStock(null);
  };
  const selectBoard = (row: BoardRow | null) => {
    setBoard(row);
    setStock(null);
  };
  return (
    <div className="pano">
      {pulse.error ? (
        <section aria-label="市场脉搏">
          <LoadError label="市场脉搏" onRetry={pulse.refetch} />
        </section>
      ) : pulse.data ? (
        <PulseRow pulse={pulse.data} />
      ) : (
        <SkeletonKpis count={5} />
      )}
      <div className="pano-grid">
        <BoardPanel
          system={system}
          onSystem={changeSystem}
          selected={board?.board_code ?? null}
          onSelect={selectBoard}
        />
        <div className="stack">
          <MembersPanel board={board} selected={stock?.ts_code ?? null} onSelect={setStock} />
          <StockChart tsCode={stock?.ts_code ?? null} name={stock?.name ?? null} />
        </div>
      </div>
    </div>
  );
}
