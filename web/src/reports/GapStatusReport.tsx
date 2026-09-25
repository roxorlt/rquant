import { useMemo, useState } from "react";
import { type DataColumn, DataTable } from "@/table/DataTable";
import { PageHeader, Panel, Pill, type PillKind, Segmented } from "@/ui";
import { GAP_STATUS, type GapModule, type GapStatus } from "./reports";

const STATUS_KIND: Record<GapStatus, PillKind> = {
  已有: "ok",
  部分: "warn",
  缺: "crit",
  不做: "idle",
};

type StatusFilter = "全部" | GapStatus;
const STATUS_FILTERS: readonly StatusFilter[] = ["全部", "已有", "部分", "缺", "不做"];
type TierFilter = "全部" | "必需" | "建议" | "可选";
const TIER_FILTERS: readonly TierFilter[] = ["全部", "必需", "建议", "可选"];

function tierMatches(tier: string, filter: TierFilter): boolean {
  return filter === "全部" || tier.includes(filter);
}

const COLUMNS: readonly DataColumn<GapModule>[] = [
  {
    id: "id",
    header: "模块",
    value: (row) => Number(row.id.slice(1)),
    cell: (row) => (
      <span className="cell2">
        <span className="nm">{row.name}</span>
        <span className="s code">{row.id}</span>
      </span>
    ),
    sortable: true,
  },
  { id: "tier", header: "分级", value: (row) => row.tier, sortable: true },
  {
    id: "status",
    header: "现状",
    value: (row) => row.status,
    cell: (row) => (
      <span className="row">
        <Pill kind={STATUS_KIND[row.status]}>{row.status}</Pill>
        {row.statusNote ? <Pill kind="warn">{row.statusNote}</Pill> : null}
      </span>
    ),
    sortable: true,
  },
  { id: "has", header: "rQuant 已有", value: (row) => row.has, wrap: true },
  { id: "lacks", header: "还缺什么", value: (row) => row.lacks, wrap: true },
  {
    id: "plan",
    header: "能力 · 工作流",
    value: (row) => row.capabilities.join(" "),
    cell: (row) =>
      row.capabilities.length ? (
        <span className="cell2">
          <span className="code">{row.capabilities.join(" ")}</span>
          <span className="s">{row.workstreams.join(" · ")}</span>
        </span>
      ) : (
        <span className="muted">不做</span>
      ),
    wrap: true,
  },
  {
    id: "done",
    header: "完成版本",
    value: (row) => row.doneVersion,
    cell: (row) => <span className="mono">{row.doneVersion ?? "—"}</span>,
  },
];

export function GapStatusReport() {
  const [status, setStatus] = useState<StatusFilter>("全部");
  const [tier, setTier] = useState<TierFilter>("全部");
  const rows = useMemo(
    () =>
      GAP_STATUS.modules.filter(
        (module) =>
          (status === "全部" || module.status === status) && tierMatches(module.tier, tier),
      ),
    [status, tier],
  );
  const updated = GAP_STATUS.latestVersion
    ? `最近更新 · ${GAP_STATUS.latestVersion}`
    : `最近更新 · ${GAP_STATUS.updated}`;
  return (
    <>
      <PageHeader eyebrow="报告" title={GAP_STATUS.title} note={`${GAP_STATUS.source}${updated}`} />
      <Panel
        title="16 个模块"
        sub={`显示 ${rows.length} 个`}
        actions={
          <div className="filters">
            <span>
              <span className="fl">现状</span>
              <Segmented
                options={STATUS_FILTERS.map((value) => ({ value, label: value }))}
                value={status}
                onChange={setStatus}
                label="按现状筛选"
              />
            </span>
            <span>
              <span className="fl">分级</span>
              <Segmented
                options={TIER_FILTERS.map((value) => ({ value, label: value }))}
                value={tier}
                onChange={setTier}
                label="按分级筛选"
              />
            </span>
          </div>
        }
        flush
      >
        <DataTable
          rows={rows}
          columns={COLUMNS}
          rowKey={(row) => row.id}
          label="差距总览"
          initialSort={{ id: "id", desc: false }}
          emptyText="没有符合筛选条件的模块"
        />
      </Panel>
      <Panel title="跨模块的结论">
        <ol className="concl">
          {GAP_STATUS.conclusions.map((item) => (
            <li key={item}>{item}</li>
          ))}
        </ol>
      </Panel>
    </>
  );
}
