import type { Schemas } from "@/api/client";
import { toneClass, toneOf } from "@/format/color";
import { formatPercent, formatSignedPercent } from "@/format/number";
import { formatShanghaiDateTime } from "@/format/time";
import { type DataColumn, DataTable } from "@/table/DataTable";
import { EmptyState, Panel, Tip } from "@/ui";
import type { PaperDetail } from "./paperPortfolioApi";

type Exposure = Schemas["ExposureSlice"];
type Attribution = Schemas["AttributionSlice"];
const exposureColumns: DataColumn<Exposure>[] = [
  {
    id: "industry",
    header: "行业",
    value: (row) => row.industry_l1 ?? "",
    cell: (row) =>
      row.kind === "cash" ? "现金" : row.kind === "unknown" ? "缺行业" : row.industry_l1,
  },
  {
    id: "weight",
    header: "账户",
    value: (row) => Number(row.portfolio_weight),
    cell: (row) => (
      <span className="paper-exposure-value">
        <span className="num">{formatPercent(Number(row.portfolio_weight) * 100)}</span>
        <span className="paper-exposure-bar" aria-hidden="true">
          <span style={{ width: `${Number(row.portfolio_weight) * 100}%` }} />
        </span>
      </span>
    ),
    numeric: true,
  },
  {
    id: "benchmark",
    header: "基准",
    value: (row) => Number(row.benchmark_weight),
    cell: (row) => formatPercent(Number(row.benchmark_weight) * 100),
    numeric: true,
  },
  {
    id: "deviation",
    header: "偏离",
    value: (row) => Number(row.deviation),
    cell: (row) => (
      <span className={`num ${toneClass(toneOf(Number(row.deviation)))}`}>
        {formatSignedPercent(Number(row.deviation) * 100)}
      </span>
    ),
    numeric: true,
  },
];
const attributionColumns: DataColumn<Attribution>[] = [
  {
    id: "industry",
    header: "行业",
    value: (row) => row.industry_l1 ?? "",
    cell: (row) => (row.kind === "cash" ? "现金" : row.industry_l1),
  },
  {
    id: "allocation",
    header: "配置贡献",
    value: (row) => Number(row.allocation),
    cell: (row) => formatSignedPercent(Number(row.allocation) * 100),
    numeric: true,
  },
  {
    id: "selection",
    header: "选股贡献",
    value: (row) => Number(row.selection_and_interaction),
    cell: (row) => formatSignedPercent(Number(row.selection_and_interaction) * 100),
    numeric: true,
  },
  {
    id: "total",
    header: "合计",
    value: (row) => Number(row.total),
    cell: (row) => (
      <span className={`num ${toneClass(toneOf(Number(row.total)))}`}>
        {formatSignedPercent(Number(row.total) * 100)}
      </span>
    ),
    numeric: true,
  },
];
export function PaperExposureView({ detail }: { detail: PaperDetail }) {
  const period = detail.attribution;
  return (
    <div className="paper-analysis-grid">
      <Panel
        title="行业暴露"
        sub={
          <Tip content="申万一级行业，与同刻可用的基准权重对照。现金单列，缺行业不作估计。">
            当前持仓
          </Tip>
        }
      >
        {detail.exposure ? (
          <DataTable
            rows={detail.exposure.rows}
            columns={exposureColumns}
            rowKey={(row) => `${row.kind}:${row.industry_l1 ?? ""}`}
            label="行业暴露"
          />
        ) : (
          <EmptyState
            title="行业对照暂不可用"
            hint={detail.exposure_reason ?? "行业与基准权重发布后会显示。"}
          />
        )}
      </Panel>
      <Panel
        title="收益归因"
        sub={
          period ? (
            <span className="num">
              {formatShanghaiDateTime(period.start_at).slice(0, 10)} 至{" "}
              {formatShanghaiDateTime(period.end_at).slice(0, 10)}
            </span>
          ) : undefined
        }
      >
        {period?.status === "complete" && period.result ? (
          <>
            <div className="paper-nav-caption">
              <span>
                超额收益{" "}
                <strong className="num">
                  {formatSignedPercent(Number(period.result.active_return) * 100)}
                </strong>
              </span>
              <Tip
                content="使用期间起点的行业权重。BF 归因只含配置与选股贡献，现金单列。"
                interactive
              >
                <button type="button" className="screen-help" aria-label="收益归因说明">
                  ?
                </button>
              </Tip>
            </div>
            <DataTable
              rows={period.result.rows}
              columns={attributionColumns}
              rowKey={(row) => `${row.kind}:${row.industry_l1 ?? ""}`}
              label="行业收益归因"
            />
          </>
        ) : (
          <EmptyState
            title="期间收益尚未完整发布"
            hint={period?.reason ?? "行业期间收益发布后会显示。"}
          />
        )}
      </Panel>
    </div>
  );
}
