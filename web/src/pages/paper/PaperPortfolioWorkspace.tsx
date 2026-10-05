import { useEffect, useRef, useState } from "react";
import { ApiError, type Schemas } from "@/api/client";
import { toneClass, toneOf } from "@/format/color";
import {
  formatCount,
  formatNumber,
  formatPercent,
  formatPrice,
  formatSignedNumber,
  formatSignedPercent,
} from "@/format/number";
import { type DataColumn, DataTable } from "@/table/DataTable";
import {
  Button,
  ConfirmDialog,
  EmptyState,
  type Kpi,
  KpiStrip,
  PageHeader,
  PageSkeleton,
  Panel,
  RelativeTime,
  SideDrawer,
  StatusBadge,
  Tip,
} from "@/ui";
import { StockCell } from "../shared/StockCell";
import { type SelectedTask, TaskProgressDrawer } from "../tasks/TaskProgressDrawer";
import { PaperExposureView } from "./PaperExposureView";
import { PaperFullHistory } from "./PaperFullHistory";
import { PaperNavView } from "./PaperNavView";
import { PaperRulesEditor } from "./PaperRulesEditor";
import {
  downloadPaperResearch,
  type PaperDetail,
  type PaperItem,
  type PaperPreparation,
  type PaperRun,
  preparePaperPause,
  usePaperPortfolio,
  usePaperPortfolios,
  usePaperResearch,
} from "./paperPortfolioApi";
import { usePaperCommands } from "./paperPortfolioCommands";
import "./portfolio.css";

function CommandNotice({
  commands,
  onRefresh,
}: {
  commands: ReturnType<typeof usePaperCommands>;
  onRefresh: () => void;
}) {
  if (!commands.pending && !commands.result) return null;
  return (
    <div className="paper-command-notice" role="status">
      <span>{commands.result?.message ?? "原请求结果待确认，请继续查看。"}</span>
      {commands.pending ? (
        <div className="row">
          <Button
            size="sm"
            disabled={commands.busy}
            onClick={() => void commands.recover().then(onRefresh)}
          >
            继续查看
          </Button>
          {commands.result?.status === "uncertain" || commands.result === null ? (
            <Button
              size="sm"
              disabled={commands.busy}
              onClick={() => void commands.retry().then(onRefresh)}
            >
              重试原操作
            </Button>
          ) : null}
        </div>
      ) : null}
    </div>
  );
}

type Holding = Schemas["PaperHolding"];
const holdingColumns: DataColumn<Holding>[] = [
  {
    id: "stock",
    header: "股票",
    value: (row) => row.code,
    cell: (row) => <StockCell code={row.code} name={null} />,
  },
  {
    id: "quantity",
    header: "持仓 / 可卖",
    value: (row) => row.quantity,
    cell: (row) => (
      <span className="paper-quantity">
        <strong className="num">{formatCount(row.quantity)}</strong>
        <span className="num muted">可卖 {formatCount(row.available_quantity)}</span>
      </span>
    ),
    numeric: true,
  },
  {
    id: "cost",
    header: "成本价",
    value: (row) => Number(row.average_cost),
    cell: (row) => formatPrice(Number(row.average_cost)),
    numeric: true,
    secondary: true,
  },
  {
    id: "price",
    header: "估值价",
    value: (row) => Number(row.market_price),
    cell: (row) => formatPrice(Number(row.market_price)),
    numeric: true,
  },
];
function itemState(row: PaperItem) {
  return row.status === "unavailable" || row.operator.status === "unavailable"
    ? { state: "warn" as const, label: "注意", reason: row.reason ?? row.operator.reason }
    : row.operator.status === "waiting"
      ? {
          state: "idle" as const,
          label: "未运行",
          reason: row.operator.reason ?? "控制等待角色应用。",
        }
      : {
          state: "ok" as const,
          label: "正常",
          reason: row.operator.paused ? "已暂停新入场。退出继续执行。" : "新入场按当前规则运行。",
        };
}
function controlLabel(row: PaperItem): string {
  return row.operator.status !== "applied" ? "等待应用" : row.operator.paused ? "已暂停" : "运行中";
}
function metrics(detail: PaperDetail): Kpi[] {
  const account = detail.account;
  return [
    { key: "nav", label: "总资产", value: formatNumber(account ? Number(account.nav) : null) },
    {
      key: "cash",
      label: "可用现金",
      value: formatNumber(account ? Number(account.available_cash) : null),
    },
    {
      key: "return",
      label: "累计收益",
      value: formatSignedPercent(
        detail.metrics.total_return == null ? null : detail.metrics.total_return * 100,
      ),
      tip: detail.metrics.reason ?? "来自同一完整收盘净值序列。",
    },
    {
      key: "drawdown",
      label: "最大回撤",
      value: formatPercent(
        detail.metrics.max_drawdown == null ? null : detail.metrics.max_drawdown * 100,
      ),
      tip: detail.metrics.reason ?? undefined,
    },
    {
      key: "days",
      label: "运行天数",
      value: formatCount(detail.metrics.running_days),
      tip: `已核实 ${detail.metrics.verified_days} 天收盘净值。`,
    },
    {
      key: "pnl",
      label: "浮动盈亏",
      value: (
        <span
          className={`num ${toneClass(toneOf(account ? Number(account.unrealized_pnl) : null))}`}
        >
          {formatSignedNumber(account ? Number(account.unrealized_pnl) : null)}
        </span>
      ),
    },
  ];
}

function ResearchResult({
  viewer,
  generation,
  account,
  job,
  onClose,
}: {
  viewer: string;
  generation: string;
  account: string;
  job: string | null;
  onClose: () => void;
}) {
  const result = usePaperResearch(viewer, generation, account, job);
  const [downloadError, setDownloadError] = useState<string | null>(null);
  const [downloading, setDownloading] = useState(false);
  const valid = !result.error && result.serving?.generation_id === generation ? result.data : null;
  const sealed = valid?.sealed;
  async function download(): Promise<void> {
    if (!sealed || !job || downloading) return;
    setDownloading(true);
    setDownloadError(null);
    try {
      await downloadPaperResearch(account, job);
    } catch {
      setDownloadError("结果暂时无法下载，请稍后重试。");
    } finally {
      setDownloading(false);
    }
  }
  return (
    <SideDrawer open={job !== null} onClose={onClose} wide title="模拟盘研究结果">
      {result.isLoading ? (
        <PageSkeleton label="研究结果加载中" />
      ) : result.error ? (
        <EmptyState
          title="研究结果暂时无法加载"
          hint={<Button onClick={result.refetch}>重试</Button>}
        />
      ) : sealed ? (
        <>
          <p>
            第 {sealed.configuration_version} 版 ·{" "}
            <RelativeTime at={sealed.completed_at} suffix="完成" />
          </p>
          {sealed.reconcile ? (
            <Panel title="只读对账">
              <p>
                {sealed.reconcile.status === "consistent"
                  ? "对账完成，无差异。"
                  : `已发现 ${formatCount(sealed.reconcile.difference_count)} 项差异。`}
              </p>
              {sealed.reconcile.differences.length ? (
                <ul className="paper-differences">
                  {sealed.reconcile.differences.map((difference) => (
                    <li key={difference.path}>
                      <Tip content={difference.path}>
                        <strong>核对项</strong>
                      </Tip>
                      <span>
                        原记录 <span className="num">{difference.expected}</span>
                      </span>
                      <span>
                        核对值 <span className="num">{difference.actual}</span>
                      </span>
                    </li>
                  ))}
                </ul>
              ) : null}
              {sealed.reconcile.truncated ? (
                <p className="muted">仅展示部分差异。完整内容可下载。</p>
              ) : null}
            </Panel>
          ) : (
            <Panel title="回测区间">
              <p>区间已计算，共 {formatCount(sealed.band?.dates.length)} 个交易日。</p>
              <Tip content="固定 seed、有放回日收益抽样。此结果用于对照，不能证明未来收益。">
                <span className="muted">查看计算说明</span>
              </Tip>
            </Panel>
          )}
          <Button variant="primary" onClick={() => void download()} disabled={downloading}>
            下载结果
          </Button>
          {downloadError ? <p role="alert">{downloadError}</p> : null}
        </>
      ) : valid ? (
        <EmptyState title="结果等待完成" hint={valid.reason ?? "封存后可查看和下载。"} />
      ) : null}
    </SideDrawer>
  );
}

function AccountDetail({
  detail,
  viewer,
  generation,
  commands,
  onBack,
  onRefresh,
  onEditorChange,
}: {
  detail: PaperDetail;
  viewer: string;
  generation: string;
  commands: ReturnType<typeof usePaperCommands>;
  onBack: () => void;
  onRefresh: () => void;
  onEditorChange: (open: boolean) => void;
}) {
  const [mode, setMode] = useState<"rules" | "band" | null>(null);
  const [preparation, setPreparation] = useState<PaperPreparation | null>(null);
  const [preparing, setPreparing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [backtest, setBacktest] = useState(detail.backtests[0]?.job_id ?? "");
  const [resultJob, setResultJob] = useState<string | null>(null);
  const [task, setTask] = useState<SelectedTask | null>(null);
  const [focusRequested, setFocusRequested] = useState(false);
  const trigger = useRef<HTMLElement | null>(null);
  const active = useRef(true);
  useEffect(() => {
    active.current = true;
    return () => {
      active.current = false;
    };
  }, []);
  useEffect(() => {
    onEditorChange(mode !== null);
    return () => onEditorChange(false);
  }, [mode, onEditorChange]);
  const locked = commands.busy || commands.pending !== null;
  useEffect(() => {
    if (
      !focusRequested ||
      locked ||
      mode !== null ||
      preparation !== null ||
      task !== null ||
      resultJob !== null
    )
      return;
    const frame = requestAnimationFrame(() => {
      if (trigger.current?.isConnected) trigger.current.focus();
      setFocusRequested(false);
    });
    return () => cancelAnimationFrame(frame);
  }, [focusRequested, locked, mode, preparation, task, resultJob]);
  const configuration = detail.configuration;
  function remember(): void {
    setFocusRequested(false);
    trigger.current = document.activeElement instanceof HTMLElement ? document.activeElement : null;
  }
  function returnFocus(): void {
    setFocusRequested(true);
  }
  function close(): void {
    setMode(null);
    setPreparation(null);
    returnFocus();
  }
  async function pause(): Promise<void> {
    if (locked || preparing) return;
    remember();
    setPreparing(true);
    setError(null);
    try {
      const value = await preparePaperPause({
        kind: "set_paper_account_paused",
        command_id: crypto.randomUUID(),
        requested_at: new Date().toISOString(),
        generation_id: generation,
        account_id: configuration.account_id,
        configuration_fingerprint: configuration.fingerprint,
        expected_sequence: detail.operator.sequence,
        expected_paused: detail.operator.paused,
        paused: !detail.operator.paused,
      });
      if (active.current) setPreparation(value);
    } catch (failure) {
      if (active.current)
        setError(failure instanceof ApiError ? failure.message : "确认内容暂时无法加载，请重试。");
    } finally {
      if (active.current) setPreparing(false);
    }
  }
  function run(taskName: PaperRun["task_name"], job?: string): void {
    if (locked) return;
    void commands.submit({
      kind: "run_paper_portfolio_research",
      command_id: crypto.randomUUID(),
      requested_at: new Date().toISOString(),
      generation_id: generation,
      account_id: configuration.account_id,
      configuration_fingerprint: configuration.fingerprint,
      task_name: taskName,
      backtest_job_id: job ?? null,
    });
  }
  const receipt = commands.result?.account_id === configuration.account_id ? commands.result : null;
  const latestJob = receipt?.status === "submitted" ? receipt.job_id : null;
  const pauseName = detail.operator.paused ? "恢复新入场" : "暂停新入场";
  return (
    <div className="paper-portfolio-detail">
      <div className="paper-detail-top">
        <Button variant="ghost" size="sm" onClick={onBack}>
          账户列表
        </Button>
        <span>
          <strong>{configuration.strategy_name}</strong> · 第 {configuration.strategy_version} 版
        </span>
        <StatusBadge {...itemState(detail)} />
        <span className="muted">{controlLabel(detail)}</span>
      </div>
      <div className="paper-detail-actions">
        <Button
          onClick={() => {
            remember();
            setMode("rules");
          }}
          disabled={locked}
          disabledReason={!detail.can_configure ? "当前账户暂不能修改规则。" : undefined}
        >
          设置仓位
        </Button>
        <Button
          onClick={() => void pause()}
          disabled={locked || preparing}
          disabledReason={!detail.can_pause ? "当前控制等待应用，暂不能继续切换。" : undefined}
        >
          {pauseName}
        </Button>
        <Button
          onClick={() => run("paper_reconcile")}
          disabled={locked}
          disabledReason={!detail.can_reconcile ? "缺少当前可核实的账户材料。" : undefined}
        >
          只读对账
        </Button>
        <Button
          onClick={() => {
            remember();
            setMode("band");
          }}
          disabled={locked}
          disabledReason={!detail.can_band ? "需要同版回测与完整收盘净值。" : undefined}
        >
          计算回测区间
        </Button>
      </div>
      {error ? (
        <p role="alert" className="paper-notice">
          {error}
        </p>
      ) : null}
      {latestJob ? (
        <Button
          size="sm"
          onClick={() => {
            remember();
            setTask({
              jobId: latestJob,
              name: receipt?.message ?? "模拟盘研究",
              viewer,
              generationId: generation,
            });
          }}
        >
          查看进展
        </Button>
      ) : null}
      <KpiStrip items={metrics(detail)} label="账户资产" />
      <div className="paper-rule-summary">
        <Tip content="保存规则形成新版本，待角色应用和账户数据更新后生效。">
          <span>规则第 {configuration.version} 版</span>
        </Tip>
        <span>{configuration.weight_rule.method === "rank_score" ? "按排名分" : "等权"}</span>
        <span>最多 {configuration.weight_rule.max_positions} 只</span>
        <span>单票 ≤{formatPercent(Number(configuration.weight_rule.max_stock_weight) * 100)}</span>
        <span>现金 ≥{formatPercent(Number(configuration.weight_rule.cash_reserve) * 100)}</span>
        <span>
          {configuration.drawdown_rule
            ? `回撤 ${formatPercent(Number(configuration.drawdown_rule.trigger_drawdown) * 100)} 触发`
            : "未设回撤限制"}
        </span>
      </div>
      {detail.risk?.decision ? (
        <Panel title="回撤状态">
          <div className="paper-risk-summary">
            <span>
              当前回撤{" "}
              <strong className="num">
                {formatPercent(Number(detail.risk.decision.drawdown) * 100)}
              </strong>
            </span>
            <span>{detail.risk.decision.allow_new_positions ? "允许新入场" : "新入场已关闭"}</span>
            {detail.reduction && detail.reduction.status !== "not_required" ? (
              <Tip content={detail.reduction.reason ?? "实际仓位来自当前账本。"}>
                <span>
                  {detail.reduction.status === "complete"
                    ? "降仓已完成"
                    : detail.reduction.status === "waiting"
                      ? "降仓等待处理"
                      : "降仓未完成"}
                </span>
              </Tip>
            ) : null}
          </div>
        </Panel>
      ) : null}
      <PaperNavView detail={detail} />
      <PaperExposureView detail={detail} />
      <Panel
        title="持仓明细"
        sub={detail.account ? `${detail.account.holdings.length} 只` : undefined}
      >
        {detail.account ? (
          detail.account.holdings.length ? (
            <DataTable
              rows={detail.account.holdings}
              columns={holdingColumns}
              rowKey={(row) => row.code}
              label="模拟账户持仓"
            />
          ) : (
            <EmptyState title="当前账户没有持仓" />
          )
        ) : (
          <EmptyState
            title="账户估值暂不可用"
            hint={detail.reason ?? "可核实的估值发布后会显示。"}
          />
        )}
      </Panel>
      <PaperFullHistory
        key={`${configuration.account_id}:${configuration.fingerprint}`}
        viewer={viewer}
        generation={generation}
        account={configuration.account_id}
        asOf={detail.account?.as_of_time ?? configuration.configured_at}
        available={detail.history_available}
      />
      <Panel title="研究记录">
        <div className="paper-research-list">
          {detail.recent_research.length ? (
            detail.recent_research.map((item) => (
              <div key={item.job_id} className="paper-research-row">
                <div>
                  <strong>{item.task_name === "paper_reconcile" ? "只读对账" : "回测区间"}</strong>
                  <span className="muted">
                    规则第 {item.configuration_version} 版 · <RelativeTime at={item.accepted_at} />
                  </span>
                </div>
                <span>{item.sealed ? "已完成" : "等待完成"}</span>
                <Button
                  size="sm"
                  onClick={() => {
                    remember();
                    setResultJob(item.job_id);
                  }}
                >
                  看结果
                </Button>
              </div>
            ))
          ) : (
            <EmptyState title="还没有研究记录" hint="提交后会显示处理进展。" />
          )}
        </div>
      </Panel>
      <SideDrawer
        open={mode !== null}
        onClose={close}
        wide
        title={mode === "rules" ? "仓位与回撤" : "计算回测区间"}
        footer={<CommandNotice commands={commands} onRefresh={onRefresh} />}
      >
        {mode === "rules" ? (
          <PaperRulesEditor
            configuration={configuration}
            generation={generation}
            locked={locked}
            onSave={(body) => void commands.submit(body)}
          />
        ) : mode === "band" ? (
          <div className="paper-rules">
            <label className="field">
              <span className="lbl">同版回测</span>
              <select
                className="inp"
                disabled={locked}
                value={backtest}
                onChange={(event) => setBacktest(event.target.value)}
              >
                {detail.backtests.map((item) => (
                  <option key={item.job_id} value={item.job_id}>
                    {item.name}
                  </option>
                ))}
              </select>
            </label>
            <Button
              variant="primary"
              disabled={locked || !backtest}
              onClick={() => run("paper_backtest_band", backtest)}
            >
              提交计算
            </Button>
          </div>
        ) : null}
      </SideDrawer>
      <ConfirmDialog
        open={preparation !== null}
        level="high"
        title={preparation?.command.paused ? "暂停新入场" : "恢复新入场"}
        description={
          preparation?.command.paused
            ? "关闭新的买入。已提交订单继续恢复，减仓和退出继续执行。"
            : "恢复后按当前规则接收新信号，已拒绝的买入不会补买。"
        }
        confirmName={configuration.strategy_name}
        expiresAt={preparation ? new Date(preparation.expires_at) : undefined}
        busy={commands.busy}
        disabled={locked}
        onCancel={close}
        onConfirm={() => {
          if (!preparation || locked) return;
          const original = preparation;
          setPreparation(null);
          void commands.submit(original.command, original.confirmation_id).then(() => {
            if (active.current) {
              returnFocus();
              onRefresh();
            }
          });
        }}
      />
      <ResearchResult
        viewer={viewer}
        generation={generation}
        account={configuration.account_id}
        job={resultJob}
        onClose={() => {
          setResultJob(null);
          returnFocus();
        }}
      />
      <TaskProgressDrawer
        selected={task}
        onClose={() => {
          setTask(null);
          returnFocus();
        }}
        onInvalidated={() => setTask(null)}
      />
    </div>
  );
}

export function PaperPortfolioWorkspace({
  viewer,
  generation,
  onLegacy,
}: {
  viewer: string;
  generation: string;
  onLegacy: () => React.ReactNode;
}) {
  const catalog = usePaperPortfolios(viewer, generation);
  const [selected, setSelected] = useState<string | null>(null);
  const selectedCard = useRef<string | null>(null);
  function back(): void {
    setSelected(null);
    requestAnimationFrame(() => {
      if (selectedCard.current) document.getElementById(selectedCard.current)?.focus();
    });
  }
  const detail = usePaperPortfolio(viewer, generation, selected);
  const commands = usePaperCommands(viewer);
  const [editing, setEditing] = useState(false);
  const changed = catalog.serving != null && catalog.serving.generation_id !== generation;
  const detailChanged = detail.serving != null && detail.serving.generation_id !== generation;
  const refresh = () => {
    catalog.refetch();
    if (selected !== null) detail.refetch();
  };
  const ownDetail = selected !== null && !detail.error && !detailChanged ? detail.data : null;
  if (!catalog.error && !changed && catalog.data?.availability === "unavailable") return onLegacy();
  return (
    <>
      <PageHeader
        eyebrow="跟踪与告警"
        title="模拟盘"
        note="账户、仓位与风控"
        actions={
          <Button
            size="sm"
            variant="ghost"
            disabled={catalog.isFetching || detail.isFetching}
            onClick={refresh}
          >
            刷新
          </Button>
        }
      />
      {!editing ? <CommandNotice commands={commands} onRefresh={refresh} /> : null}
      {catalog.isLoading ? (
        <PageSkeleton label="模拟账户加载中" />
      ) : changed ? (
        <Panel>
          <EmptyState title="账户数据更新中" hint={<Button onClick={refresh}>重试</Button>} />
        </Panel>
      ) : catalog.error ? (
        <Panel>
          <EmptyState
            title="模拟账户暂时无法加载"
            hint={<Button onClick={catalog.refetch}>重试</Button>}
          />
        </Panel>
      ) : selected !== null ? (
        detail.isLoading ? (
          <PageSkeleton label="账户详情加载中" />
        ) : detail.error || detailChanged ? (
          <Panel>
            <EmptyState
              title={detailChanged ? "账户数据更新中" : "账户详情暂时无法加载"}
              hint={
                <>
                  <Button onClick={back}>账户列表</Button>
                  <Button onClick={detail.refetch}>重试</Button>
                </>
              }
            />
          </Panel>
        ) : ownDetail ? (
          <AccountDetail
            key={`${selected}:${ownDetail.configuration.fingerprint}`}
            detail={ownDetail}
            viewer={viewer}
            generation={generation}
            commands={commands}
            onBack={back}
            onRefresh={refresh}
            onEditorChange={setEditing}
          />
        ) : null
      ) : catalog.data?.accounts.length ? (
        <div className="paper-account-grid">
          {catalog.data.accounts.map((item, index) => (
            <button
              id={`paper-account-${index}`}
              key={item.configuration.account_id}
              type="button"
              className="paper-account-card"
              aria-label={`查看模拟账户 ${index + 1}`}
              onClick={(event) => {
                selectedCard.current = event.currentTarget.id;
                setSelected(item.configuration.account_id);
              }}
            >
              <div className="paper-account-card-head">
                <strong>{item.configuration.strategy_name}</strong>
                <StatusBadge state={itemState(item).state} label={itemState(item).label} />
              </div>
              <p className="muted">
                第 {item.configuration.strategy_version} 版 · {controlLabel(item)}
              </p>
              <div className="paper-account-card-metrics">
                <span>
                  总资产
                  <strong className="num">
                    {formatNumber(item.account ? Number(item.account.nav) : null)}
                  </strong>
                </span>
                <span>
                  累计收益
                  <strong className="num">
                    {formatSignedPercent(
                      item.metrics.total_return == null ? null : item.metrics.total_return * 100,
                    )}
                  </strong>
                </span>
              </div>
              <span className="paper-card-link">查看账户 →</span>
            </button>
          ))}
        </div>
      ) : (
        <Panel>
          <EmptyState title="还没有模拟账户" hint="账户发布后会显示，请稍后刷新。" />
        </Panel>
      )}
    </>
  );
}
