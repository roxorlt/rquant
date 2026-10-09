import {
  type CSSProperties,
  type KeyboardEvent,
  useMemo,
  useRef,
  useState,
  useSyncExternalStore,
} from "react";
import { toneClass, toneOf } from "@/format/color";
import { formatCount, formatNumber, formatPercent, formatSignedPercent } from "@/format/number";
import { type DataColumn, DataTable } from "@/table/DataTable";
import { Button, EmptyState, PageSkeleton, Panel, Tip } from "@/ui";
import "./MinuteStudyViews.css";

// Local presentation props. The caller maps verified generated DTOs after wire freeze.
export type StudyViewState =
  | "loading"
  | "pending"
  | "empty"
  | "unavailable"
  | "failed"
  | "completed";
export type StudyAxisValue = number | boolean;
export interface StudyAxisOption {
  key: string;
  label: string;
  /** percent is already in percentage points; ratio-percent formats a raw ratio. */
  format: "number" | "count" | "percent" | "ratio-percent" | "boolean";
}
export interface StudyHeatmapCell {
  xIndex: number;
  yIndex: number;
  studyId: string | null;
  isCurrent: boolean;
  status: "available" | "insufficient_trades" | "missing_trial" | "invalid_parameters";
  score: number | null;
  trades: number | null;
  neighborhoodMinimum: number | null;
  neighborhoodReason: string | null;
  detail: string | null;
}
export interface StudyHeatmapView {
  resultKey: string;
  axes: readonly StudyAxisOption[];
  xAxis: { key: string; values: readonly StudyAxisValue[] };
  yAxis: { key: string; values: readonly StudyAxisValue[] };
  cells: readonly StudyHeatmapCell[];
}
export interface StudyAblationView {
  key: string;
  label: string;
  status: "available" | "unavailable";
  score: number | null;
  /** Backend value in percentage points; no return calculation takes place here. */
  returnPercent: number | null;
  trades: number | null;
  detail: string | null;
}
export interface StudyFoldView {
  key: string;
  label: string;
  status: "available" | "unavailable";
  trainStart: string;
  trainEnd: string;
  validationStart: string | null;
  validationEnd: string | null;
  testStart: string;
  testEnd: string;
  trainingScore: number | null;
  validationScore: number | null;
  testScore: number | null;
  testReturnPercent: number | null;
  testTrades: number | null;
  detail: string | null;
}
export interface MinuteStudyViewsProps {
  state: StudyViewState;
  /** Include actor/data version/result identity; a changed key resets local inspection. */
  scopeKey: string;
  heatmap: StudyHeatmapView | null;
  ablations: readonly StudyAblationView[];
  folds: readonly StudyFoldView[];
  detail?: string | null;
  /** The caller names the actual owner metric; it may be an average trade return. */
  ablationReturnLabel?: string;
  foldReturnLabel?: string;
  onAxesChange?: (xParameter: string, yParameter: string) => void;
}

const COMPACT_QUERY = "(max-width: 760px)";
const ROWS_PER_PAGE = 5;

function subscribeCompact(onChange: () => void) {
  const media = window.matchMedia(COMPACT_QUERY);
  media.addEventListener("change", onChange);
  return () => media.removeEventListener("change", onChange);
}

function isCompact() {
  return window.matchMedia(COMPACT_QUERY).matches;
}

function axisValue(value: StudyAxisValue | undefined, option: StudyAxisOption | undefined) {
  if (typeof value === "boolean") return value ? "开启" : "关闭";
  if (option?.format === "ratio-percent") return formatPercent(value == null ? null : value * 100);
  if (option?.format === "percent") return formatPercent(value);
  if (option?.format === "count") return formatCount(value);
  return formatNumber(value);
}

function cellScore(cell: StudyHeatmapCell | undefined) {
  return cell?.status === "available" ? cell.score : null;
}

function cellState(cell: StudyHeatmapCell | undefined) {
  if (cell?.status === "available") return `评分 ${formatNumber(cell.score, 4)}`;
  if (cell?.status === "insufficient_trades") return "交易不足";
  if (cell?.status === "invalid_parameters") return "不适用";
  return "无结果";
}

function CellDetails({ cell, label }: { cell: StudyHeatmapCell; label: string }) {
  return (
    <div className="study-tip">
      <strong>{label}</strong>
      <div>训练评分：{formatNumber(cellScore(cell), 4)}</div>
      <div>交易笔数：{formatCount(cell.trades)}</div>
      <div>邻域最低分：{formatNumber(cell.neighborhoodMinimum, 4)}</div>
      {cell.neighborhoodReason ? <div>{cell.neighborhoodReason}</div> : null}
      {cell.detail ? <div>{cell.detail}</div> : null}
      {cell.studyId ? (
        <div>
          结果标识：<span className="mono">{cell.studyId}</span>
        </div>
      ) : null}
    </div>
  );
}

function NeighborhoodValue({ cell, label }: { cell: StudyHeatmapCell | undefined; label: string }) {
  return (
    <Tip content={cell?.neighborhoodReason}>
      <output aria-label={label} className="num">
        {formatNumber(cell?.neighborhoodMinimum, 4)}
      </output>
    </Tip>
  );
}

function Heatmap({
  data,
  onAxesChange,
}: {
  data: StudyHeatmapView;
  onAxesChange: MinuteStudyViewsProps["onAxesChange"];
}) {
  const compact = useSyncExternalStore(subscribeCompact, isCompact, () => true);
  const columnsPerPage = compact ? 4 : 8;
  const current = data.cells.find((cell) => cell.isCurrent);
  const [xPage, setXPage] = useState(0);
  const [yPage, setYPage] = useState(0);
  const [inspected, setInspected] = useState<StudyHeatmapCell | undefined>(current);
  const [requestedAxes, setRequestedAxes] = useState<{ x: string; y: string } | null>(null);
  const matrix = useRef<HTMLFieldSetElement>(null);
  const xOption = data.axes.find((axis) => axis.key === data.xAxis.key);
  const yOption = data.axes.find((axis) => axis.key === data.yAxis.key);
  const xLabel = xOption?.label ?? "横轴";
  const yLabel = yOption?.label ?? "纵轴";
  const byCoordinate = new Map(data.cells.map((cell) => [`${cell.xIndex}:${cell.yIndex}`, cell]));
  const xPageCount = Math.max(1, Math.ceil(data.xAxis.values.length / columnsPerPage));
  const yPageCount = Math.max(1, Math.ceil(data.yAxis.values.length / ROWS_PER_PAGE));
  const xOffset = Math.min(xPage, xPageCount - 1) * columnsPerPage;
  const yOffset = Math.min(yPage, yPageCount - 1) * ROWS_PER_PAGE;
  const xValues = data.xAxis.values.slice(xOffset, xOffset + columnsPerPage);
  const yValues = data.yAxis.values.slice(yOffset, yOffset + ROWS_PER_PAGE);
  // Normalization affects paint only. Scores, current point and neighborhood come from the caller.
  const extent = data.cells.reduce((max, cell) => {
    const score = cellScore(cell);
    return score == null || !Number.isFinite(score) ? max : Math.max(max, Math.abs(score));
  }, 0);
  const describe = (cell: StudyHeatmapCell) =>
    `${xLabel} ${axisValue(data.xAxis.values[cell.xIndex], xOption)}，${yLabel} ${axisValue(data.yAxis.values[cell.yIndex], yOption)}`;

  const locate = (cell: StudyHeatmapCell) => {
    setXPage(Math.floor(cell.xIndex / columnsPerPage));
    setYPage(Math.floor(cell.yIndex / ROWS_PER_PAGE));
    window.requestAnimationFrame(() => {
      matrix.current
        ?.querySelector<HTMLButtonElement>(`[data-cell="${cell.xIndex}:${cell.yIndex}"]`)
        ?.focus();
    });
  };

  const navigate = (event: KeyboardEvent<HTMLButtonElement>, cell: StudyHeatmapCell) => {
    const delta: Record<string, readonly [number, number]> = {
      ArrowLeft: [-1, 0],
      ArrowRight: [1, 0],
      ArrowUp: [0, -1],
      ArrowDown: [0, 1],
    };
    const step = delta[event.key];
    if (!step) return;
    event.preventDefault();
    const next = byCoordinate.get(`${cell.xIndex + step[0]}:${cell.yIndex + step[1]}`);
    if (next) locate(next);
  };

  const changeAxis = (axis: "x" | "y", key: string) => {
    if (!onAxesChange || requestedAxes) return;
    const selection = {
      x: axis === "x" ? key : data.xAxis.key,
      y: axis === "y" ? key : data.yAxis.key,
    };
    if (selection.x === selection.y) return;
    setRequestedAxes(selection);
    onAxesChange(selection.x, selection.y);
  };

  return (
    <Panel
      title="参数热图"
      label="参数热图结果"
      actions={
        <Button
          size="sm"
          disabled={!current || requestedAxes !== null}
          onClick={() => current && locate(current)}
        >
          当前参数
        </Button>
      }
    >
      <div className="study-axis-controls">
        {(["x", "y"] as const).map((axis) => {
          const value = axis === "x" ? data.xAxis.key : data.yAxis.key;
          const opposite = axis === "x" ? data.yAxis.key : data.xAxis.key;
          return (
            <label key={axis}>
              <span>{axis === "x" ? "横轴参数" : "纵轴参数"}</span>
              <select
                className="inp"
                value={requestedAxes?.[axis] ?? value}
                disabled={!onAxesChange || requestedAxes !== null}
                onChange={(event) => changeAxis(axis, event.target.value)}
              >
                {data.axes.map((option) => (
                  <option key={option.key} value={option.key} disabled={option.key === opposite}>
                    {option.label}
                  </option>
                ))}
              </select>
            </label>
          );
        })}
      </div>
      {requestedAxes ? (
        <EmptyState title="正在加载所选参数" />
      ) : (
        <>
          <dl className="study-current-summary">
            <div>
              <dt>当前参数</dt>
              <dd className="study-current-values">{current ? describe(current) : "—"}</dd>
            </div>
            <div>
              <dt>当前评分</dt>
              <dd>
                <output aria-label="当前参数评分" className="num">
                  {formatNumber(cellScore(current), 4)}
                </output>
              </dd>
            </div>
            <div>
              <dt>邻域最低分</dt>
              <dd>
                <NeighborhoodValue cell={current} label="当前参数邻域最低分" />
              </dd>
            </div>
          </dl>
          <div className="study-axis-caption">
            <span>{yLabel} ↓</span>
            <span>{xLabel} →</span>
          </div>
          <fieldset
            ref={matrix}
            className="study-matrix"
            aria-label="参数热图"
            data-columns={xValues.length}
            style={{
              gridTemplateColumns: `minmax(38px, 0.65fr) repeat(${xValues.length}, minmax(0, 1fr))`,
            }}
          >
            <span aria-hidden="true" />
            {xValues.map((value) => (
              <span className="study-axis-tick num" key={String(value)}>
                {axisValue(value, xOption)}
              </span>
            ))}
            {yValues.map((value, visibleY) => {
              const yIndex = yOffset + visibleY;
              return (
                <div className="study-matrix-row" key={String(value)}>
                  <span className="study-axis-tick num">{axisValue(value, yOption)}</span>
                  {xValues.map((xValue, visibleX) => {
                    const xIndex = xOffset + visibleX;
                    const cell = byCoordinate.get(`${xIndex}:${yIndex}`);
                    if (!cell)
                      return (
                        <span key={String(xValue)} className="study-cell study-cell-missing">
                          —
                        </span>
                      );
                    const score = cellScore(cell);
                    const missing = score == null || !Number.isFinite(score);
                    const tone = toneClass(toneOf(missing ? null : score));
                    const paint: CSSProperties & { "--study-fill": string } = {
                      "--study-fill": `${!missing && extent > 0 && score !== null ? 10 + (Math.abs(score) / extent) * 25 : 0}%`,
                    };
                    const description = describe(cell);
                    return (
                      <Tip
                        key={String(xValue)}
                        interactive
                        placement="topLeft"
                        content={<CellDetails cell={cell} label={description} />}
                      >
                        <button
                          type="button"
                          data-cell={`${xIndex}:${yIndex}`}
                          style={paint}
                          className={`study-cell ${tone} ${missing ? "study-cell-missing" : ""}`}
                          aria-label={`${description}，${cellState(cell)}${cell.isCurrent ? "，当前参数" : ""}`}
                          aria-current={cell.isCurrent ? "true" : undefined}
                          onClick={() => setInspected(cell)}
                          onKeyDown={(event) => navigate(event, cell)}
                        >
                          <span className="num">{formatNumber(score, 4)}</span>
                          {cell.isCurrent ? (
                            <span className="study-current-marker" aria-hidden="true">
                              当前
                            </span>
                          ) : null}
                        </button>
                      </Tip>
                    );
                  })}
                </div>
              );
            })}
          </fieldset>
          <div className="study-grid-navigation">
            {xPageCount > 1 ? (
              <div className="study-page-control">
                <Button
                  size="sm"
                  aria-label="上一组横轴"
                  disabled={xOffset === 0}
                  onClick={() => setXPage(Math.max(0, xPage - 1))}
                >
                  ←
                </Button>
                <span className="num">
                  {Math.floor(xOffset / columnsPerPage) + 1} / {xPageCount}
                </span>
                <Button
                  size="sm"
                  aria-label="下一组横轴"
                  disabled={xOffset + columnsPerPage >= data.xAxis.values.length}
                  onClick={() => setXPage(Math.min(xPageCount - 1, xPage + 1))}
                >
                  →
                </Button>
              </div>
            ) : null}
            {yPageCount > 1 ? (
              <div className="study-page-control">
                <Button
                  size="sm"
                  aria-label="上一组纵轴"
                  disabled={yOffset === 0}
                  onClick={() => setYPage(Math.max(0, yPage - 1))}
                >
                  ↑
                </Button>
                <span className="num">
                  {Math.floor(yOffset / ROWS_PER_PAGE) + 1} / {yPageCount}
                </span>
                <Button
                  size="sm"
                  aria-label="下一组纵轴"
                  disabled={yOffset + ROWS_PER_PAGE >= data.yAxis.values.length}
                  onClick={() => setYPage(Math.min(yPageCount - 1, yPage + 1))}
                >
                  ↓
                </Button>
              </div>
            ) : null}
          </div>
          {inspected ? (
            <section className="study-inspected" aria-label="所看参数">
              <span>{describe(inspected)}</span>
              <span>
                邻域最低分 <NeighborhoodValue cell={inspected} label="所看参数邻域最低分" />
              </span>
            </section>
          ) : null}
        </>
      )}
    </Panel>
  );
}

function ablationColumns(returnLabel = "收益"): readonly DataColumn<StudyAblationView>[] {
  return [
    {
      id: "label",
      header: "方案",
      value: (row) => row.label,
      wrap: true,
      cell: (row) => (
        <Tip
          content={
            <div className="study-tip">
              <div>评分：{formatNumber(row.score, 4)}</div>
              <div>
                {returnLabel}：{formatSignedPercent(row.returnPercent)}
              </div>
              <div>交易：{formatCount(row.trades)}</div>
              {row.detail ? <div>{row.detail}</div> : null}
            </div>
          }
        >
          <span>{row.label}</span>
        </Tip>
      ),
    },
    {
      id: "score",
      header: "评分",
      value: (row) => row.score,
      numeric: true,
      cell: (row) => <span className="num">{formatNumber(row.score, 4)}</span>,
    },
    {
      id: "return",
      header: returnLabel,
      value: (row) => row.returnPercent,
      numeric: true,
      cell: (row) => (
        <span className={`num ${toneClass(toneOf(row.returnPercent))}`}>
          {formatSignedPercent(row.returnPercent)}
        </span>
      ),
    },
    {
      id: "trades",
      header: "交易",
      value: (row) => row.trades,
      numeric: true,
      cell: (row) => formatCount(row.trades),
    },
  ];
}

function interval(start: string | null, end: string | null) {
  return start === null && end === null ? "—" : `${start ?? "—"} 至 ${end ?? "—"}`;
}

function FoldDetails({ row, returnLabel }: { row: StudyFoldView; returnLabel: string }) {
  return (
    <div className="study-tip">
      <div>训练：{interval(row.trainStart, row.trainEnd)}</div>
      <div>验证：{interval(row.validationStart, row.validationEnd)}</div>
      <div>最终测试：{interval(row.testStart, row.testEnd)}</div>
      <div>训练评分：{formatNumber(row.trainingScore, 4)}</div>
      <div>验证评分：{formatNumber(row.validationScore, 4)}</div>
      <div>最终测试评分：{formatNumber(row.testScore, 4)}</div>
      <div>
        {returnLabel}：{formatSignedPercent(row.testReturnPercent)}
      </div>
      <div>测试交易：{formatCount(row.testTrades)}</div>
      {row.detail ? <div>{row.detail}</div> : null}
    </div>
  );
}

function foldColumns(returnLabel = "测试收益"): readonly DataColumn<StudyFoldView>[] {
  return [
    {
      id: "label",
      header: "窗口",
      value: (row) => row.label,
      wrap: true,
      cell: (row) => (
        <Tip interactive content={<FoldDetails row={row} returnLabel={returnLabel} />}>
          <button type="button" className="study-detail-button" aria-label={`${row.label}详情`}>
            {row.label}
          </button>
        </Tip>
      ),
    },
    {
      id: "training",
      header: "训练评分",
      value: (row) => row.trainingScore,
      numeric: true,
      cell: (row) => <span className="num">{formatNumber(row.trainingScore, 4)}</span>,
    },
    {
      id: "validation",
      header: "验证评分",
      value: (row) => row.validationScore,
      numeric: true,
      cell: (row) => <span className="num">{formatNumber(row.validationScore, 4)}</span>,
    },
    {
      id: "test",
      header: "最终测试",
      value: (row) => row.testScore,
      numeric: true,
      cell: (row) => <span className="num">{formatNumber(row.testScore, 4)}</span>,
    },
    {
      id: "return",
      header: returnLabel,
      value: (row) => row.testReturnPercent,
      numeric: true,
      cell: (row) => (
        <span className={`num ${toneClass(toneOf(row.testReturnPercent))}`}>
          {formatSignedPercent(row.testReturnPercent)}
        </span>
      ),
    },
    {
      id: "trades",
      header: "交易",
      value: (row) => row.testTrades,
      numeric: true,
      secondary: true,
      cell: (row) => formatCount(row.testTrades),
    },
  ];
}

export function MinuteStudyViews(props: MinuteStudyViewsProps) {
  const ablationTableColumns = useMemo(
    () => ablationColumns(props.ablationReturnLabel),
    [props.ablationReturnLabel],
  );
  const foldTableColumns = useMemo(
    () => foldColumns(props.foldReturnLabel),
    [props.foldReturnLabel],
  );
  if (props.state === "loading") return <PageSkeleton label="正在加载研究结果" />;
  if (props.state !== "completed") {
    const title = {
      pending: "研究正在进行",
      empty: "暂无研究结果",
      unavailable: "研究资料暂不可用",
      failed: "研究失败",
    }[props.state];
    return (
      <EmptyState
        title={title}
        hint={
          props.detail ? (
            <Tip content={props.detail}>
              <span>详情</span>
            </Tip>
          ) : undefined
        }
      />
    );
  }
  return (
    <div className="minute-study-views" key={props.scopeKey}>
      {props.heatmap ? (
        <Heatmap
          data={props.heatmap}
          onAxesChange={props.onAxesChange}
          key={`${props.scopeKey}:${props.heatmap.resultKey}:${props.heatmap.xAxis.key}:${props.heatmap.yAxis.key}`}
        />
      ) : (
        <Panel title="参数热图">
          <EmptyState title="暂无参数对照" />
        </Panel>
      )}
      <div className="study-table">
        <Panel title="五组消融" flush>
          <DataTable
            label="五组消融对照"
            rows={props.ablations}
            columns={ablationTableColumns}
            rowKey={(row) => row.key}
            emptyText="暂无消融结果"
          />
        </Panel>
      </div>
      <div className="study-table">
        <Panel title="滚动分窗" flush>
          <DataTable
            label="滚动分窗结果"
            rows={props.folds}
            columns={foldTableColumns}
            rowKey={(row) => row.key}
            emptyText="暂无分窗结果"
          />
        </Panel>
      </div>
    </div>
  );
}
