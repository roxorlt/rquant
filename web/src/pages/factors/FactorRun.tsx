import { Button, ConfirmDialog, Panel, Segmented, Tip } from "@/ui";
import { isNeutralizationMode, type RunDraft } from "./factorRunState";
import { neutralizationExplanation } from "./runNeutralization";
import { outlierCaption, outlierExplanation } from "./runOutliers";
import type { useFactorRun } from "./useFactorRun";

type Run = ReturnType<typeof useFactorRun>;

export function FactorRunParameters({ run }: { run: Run }) {
  const { params, availability } = run;
  const change = <K extends keyof RunDraft>(key: K, value: RunDraft[K]) =>
    run.update({ ...params, [key]: value });
  const pool = availability.data?.pools.find((item) => item.selection === params.selection);
  return (
    <Panel title="检验参数" label="检验参数">
      <form
        className="factor-run-form"
        onSubmit={(event) => {
          event.preventDefault();
          run.open();
        }}
      >
        <label className="field">
          <span className="lbl">股票池</span>
          <select
            className="inp"
            value={params.selection}
            onChange={(event) => change("selection", event.target.value as RunDraft["selection"])}
          >
            {availability.data?.pools.map((item) => (
              <option key={item.selection} value={item.selection} disabled={!item.available}>
                {item.label}
              </option>
            ))}
          </select>
        </label>
        {availability.data?.pools.some((item) => !item.available) ? (
          <Tip
            content={availability.data.pools
              .filter((item) => !item.available)
              .map((item) => `${item.label}：${item.reason ?? "暂不可用"}`)
              .join("；")}
          >
            股票池可用范围
          </Tip>
        ) : null}
        <div className="factor-run-fields">
          <label className="field">
            <span className="lbl">开始日期</span>
            <input
              type="date"
              className="inp num"
              value={params.start_date}
              onChange={(event) => change("start_date", event.target.value)}
            />
          </label>
          <label className="field">
            <span className="lbl">结束日期</span>
            <input
              type="date"
              className="inp num"
              value={params.end_date}
              onChange={(event) => change("end_date", event.target.value)}
            />
          </label>
          <label className="field">
            <span className="lbl">调仓周期</span>
            <select
              className="inp"
              value={params.holding_sessions}
              onChange={(event) =>
                change(
                  "holding_sessions",
                  Number(event.target.value) as RunDraft["holding_sessions"],
                )
              }
            >
              {[1, 5, 10, 20].map((days) => (
                <option key={days} value={days}>
                  {days} 日
                </option>
              ))}
            </select>
          </label>
          <label className="field">
            <span className="lbl">分组数</span>
            <select
              className="inp"
              value={params.group_count}
              onChange={(event) =>
                change("group_count", Number(event.target.value) as RunDraft["group_count"])
              }
            >
              {[3, 5, 10].map((count) => (
                <option key={count} value={count}>
                  {count} 组
                </option>
              ))}
            </select>
          </label>
        </div>
        <label className="field">
          <span className="lbl">离群值处理</span>
          <select
            className="inp"
            value={params.mad_multiple == null ? "none" : "mad"}
            disabled={!run.availabilityVerified || run.permissionDenied}
            onChange={(event) => change("mad_multiple", event.target.value === "mad" ? 3 : null)}
          >
            <option value="none">不处理</option>
            <option value="mad">MAD</option>
          </select>
        </label>
        {params.mad_multiple != null ? (
          <label className="field">
            <span className="lbl">MAD 倍数</span>
            <input
              className="inp num"
              type="number"
              min="0"
              step="any"
              value={params.mad_multiple}
              disabled={!run.availabilityVerified || run.permissionDenied}
              onChange={(event) => change("mad_multiple", Number(event.target.value))}
            />
          </label>
        ) : null}
        <Tip content={outlierExplanation}>离群值处理说明</Tip>
        <label className="field">
          <span className="lbl">中性化</span>
          <select
            className="inp"
            value={params.neutralization}
            disabled={!run.availabilityVerified || run.permissionDenied}
            onChange={(event) => {
              if (isNeutralizationMode(event.target.value))
                change("neutralization", event.target.value);
            }}
          >
            {run.neutralizations.map((option) => (
              <option
                key={option.neutralization}
                value={option.neutralization}
                disabled={!option.available}
              >
                {option.label}
                {option.available ? "" : "（暂不可用）"}
              </option>
            ))}
          </select>
        </label>
        <Tip content={neutralizationExplanation(run.neutralizations)}>中性化说明</Tip>
        <div className="field">
          <span className="lbl">IC 算法</span>
          <Segmented
            label="检验 IC 算法"
            value={params.ic_method ?? "rank"}
            onChange={(value) => change("ic_method", value)}
            options={[
              { value: "rank", label: "RankIC" },
              { value: "normal", label: "NormalIC" },
            ]}
          />
        </div>
        {run.blockedReason ? (
          <p className="hint" role={!run.storageReady ? "alert" : "status"}>
            {run.blockedReason}
          </p>
        ) : null}
        {pool?.available && availability.data?.enabled && run.storageReady ? (
          <Button
            variant="primary"
            type="submit"
            disabledReason={!run.canStart ? (run.blockedReason ?? "请先完成本次检验。") : undefined}
          >
            运行检验
          </Button>
        ) : null}
      </form>
    </Panel>
  );
}

export function FactorRunStatus({ run }: { run: Run }) {
  const record = run.operation;
  if (record === null) return null;
  const p = record.request.parameters;
  return (
    <Panel title="本次检验" label="本次检验">
      <div className="factor-run-summary">
        <span>
          {record.factorName} · 第 {p.expected_head.version} 版
        </span>
        <span>
          {record.poolLabel} · {p.start_date} 至 {p.end_date}
        </span>
        <span>
          {p.holding_sessions} 日 · {p.group_count} 组 ·{" "}
          {p.ic_method === "rank" ? "RankIC" : "NormalIC"}
          {" · "}
          {run.describeNeutralization(record)}
          {" · "}
          {outlierCaption(p.mad_multiple)}
        </span>
      </div>
      <div
        className="factor-command-state"
        role={record.result?.status === "rejected" ? "alert" : "status"}
      >
        <p>{run.status}</p>
        {record.result?.status === "rejected" || run.failed ? (
          <Button size="sm" disabled={run.busy || !run.sameActor} onClick={() => void run.finish()}>
            修改检验参数
          </Button>
        ) : run.completed ? (
          <Button size="sm" disabled={run.busy} onClick={() => void run.finish()}>
            继续查看结果
          </Button>
        ) : (
          <div className="factor-command-actions">
            <Button
              size="sm"
              disabled={run.busy || !run.canContinue}
              onClick={() => void run.refresh()}
            >
              刷新检验状态
            </Button>
            {record.result?.status === "uncertain" || record.result === null ? (
              <Button
                size="sm"
                disabled={run.busy || !run.canContinue}
                onClick={() => void run.continueRun("retry")}
              >
                用原请求重试检验
              </Button>
            ) : null}
          </div>
        )}
      </div>
      {run.blockedReason ? <p className="hint">{run.blockedReason}</p> : null}
    </Panel>
  );
}

export function FactorRunConfirmation({ run }: { run: Run }) {
  const record = run.confirmation;
  const p = record?.request.parameters;
  return (
    <ConfirmDialog
      open={record !== null}
      level="heavy"
      title="运行因子检验"
      confirmLabel="确认运行"
      busy={run.busy}
      disabled={!run.confirmationCurrent}
      description={
        record && p ? (
          <>
            <p>
              {record.factorName} · 第 {p.expected_head.version} 版
            </p>
            <p>
              {record.poolLabel} · {p.start_date} 至 {p.end_date}
            </p>
            <p>
              {p.holding_sessions} 日调仓 · {p.group_count} 组 ·{" "}
              {p.ic_method === "rank" ? "RankIC" : "NormalIC"} ·{" "}
              {run.describeNeutralization(record)}
              {" · "}
              {outlierCaption(p.mad_multiple)}
            </p>
            <p className="hint">确认后提交检验，结果更新后可查看。</p>
            {!run.confirmationCurrent ? (
              <p role="alert">因子或检验条件已变化，请关闭后重新确认。</p>
            ) : null}
          </>
        ) : null
      }
      onConfirm={() => void run.submit()}
      onCancel={run.cancel}
    />
  );
}
