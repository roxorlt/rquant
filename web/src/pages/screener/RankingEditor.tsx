import type { ScreenOption } from "@/api/screen";
import { Button, EmptyState, Panel, Tip } from "@/ui";

export type RankingDraft = {
  id: number;
  metric: string;
  ascending: boolean;
  weight: string;
};

export function RankingEditor({
  metrics,
  rows,
  topN,
  totalWeight,
  error,
  onAdd,
  onUpdate,
  onRemove,
  onTopN,
}: {
  metrics: ScreenOption[];
  rows: RankingDraft[];
  topN: string;
  totalWeight: number;
  error: string | null;
  onAdd: () => void;
  onUpdate: (id: number, change: Partial<RankingDraft>) => void;
  onRemove: (id: number) => void;
  onTopN: (value: string) => void;
}) {
  const selected = new Set(rows.map((row) => row.metric));
  return (
    <Panel
      title="排名条件（权重）"
      sub="对留下的股票加权打分"
      actions={
        <Button onClick={onAdd} disabled={metrics.length === 0 || selected.size >= metrics.length}>
          添加排名
        </Button>
      }
    >
      {metrics.length === 0 ? (
        <EmptyState title="暂无可用排名指标" hint="相关数据发布后即可设置排名。" />
      ) : null}
      {rows.length === 0 && metrics.length > 0 ? (
        <p className="hint screen-rank-empty">添加排名后，可按分数选出前 N 只。</p>
      ) : null}
      <div className="screen-rank-list">
        {rows.map((row, index) => (
          <div className="screen-rank-row" key={row.id}>
            <span className="screen-index num">{index + 1}</span>
            <label className="field screen-rank-metric">
              <span className="lbl">指标</span>
              <select
                className="inp"
                aria-label={`第 ${index + 1} 项指标`}
                value={row.metric}
                onChange={(event) =>
                  onUpdate(row.id, {
                    metric: event.target.value,
                    ascending: event.target.value === "CIRC_MV[0]",
                  })
                }
              >
                {metrics.map((metric) => (
                  <option
                    key={metric.value}
                    value={metric.value}
                    disabled={metric.value !== row.metric && selected.has(metric.value)}
                  >
                    {metric.label}
                  </option>
                ))}
              </select>
            </label>
            <label className="field screen-rank-direction">
              <span className="lbl">优先方向</span>
              <select
                className="inp"
                aria-label={`第 ${index + 1} 项方向`}
                value={row.ascending ? "asc" : "desc"}
                onChange={(event) => onUpdate(row.id, { ascending: event.target.value === "asc" })}
              >
                <option value="desc">数值高优先</option>
                <option value="asc">数值低优先</option>
              </select>
            </label>
            <label className="field screen-rank-weight">
              <span className="lbl">权重</span>
              <input
                className="inp num"
                type="number"
                min="0"
                max="100"
                step="0.1"
                inputMode="decimal"
                aria-label={`第 ${index + 1} 项权重`}
                value={row.weight}
                onChange={(event) => onUpdate(row.id, { weight: event.target.value })}
              />
            </label>
            <Button
              size="sm"
              variant="ghost"
              aria-label={`删除第 ${index + 1} 项排名`}
              onClick={() => onRemove(row.id)}
            >
              删除
            </Button>
          </div>
        ))}
      </div>
      {rows.length > 0 ? (
        <div className="screen-rank-footer">
          <p className="screen-rank-summary">
            权重合计 {Number.isFinite(totalWeight) ? totalWeight.toLocaleString("zh-CN") : "—"}% ·
            运行时按比例折算为 100%
            <Tip content="每项按同日股票的相对位置计分，再按权重合成 0–100 分；缺值靠后。">
              <span className="screen-help" role="img" aria-label="排名说明">
                ⓘ
              </span>
            </Tip>
          </p>
          <label className="field screen-rank-top">
            <span className="lbl">取前 N 只</span>
            <input
              className="inp num"
              type="number"
              min="1"
              max="100"
              step="1"
              inputMode="numeric"
              value={topN}
              onChange={(event) => onTopN(event.target.value)}
            />
          </label>
        </div>
      ) : null}
      {error ? (
        <p className="screen-rank-error" role="alert">
          {error}
        </p>
      ) : null}
    </Panel>
  );
}
