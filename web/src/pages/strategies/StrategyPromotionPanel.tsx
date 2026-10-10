import { useEffect, useRef, useState } from "react";
import type { Schemas } from "@/api/client";
import { useCollaboration } from "@/api/collaboration";
import {
  type PromotionData,
  type PromotionPreparation,
  type PromotionReview,
  type PromotionTarget,
  samePromotionValue,
  usePromotionData,
} from "@/api/strategyPromotion";
import { useCurrentMeta } from "@/api/useMeta";
import { type DataColumn, DataTable } from "@/table/DataTable";
import { Button, ConfirmDialog, EmptyState, Panel, RelativeTime, SkeletonRows, Tip } from "@/ui";
import { usePromotionCommands, usePromotionPrivateCleanup } from "./promotionCommands";
import "./templates.css";

type Stage = Schemas["PromotionStage"];
type Head = Schemas["StrategyTemplateHead"];
type Gate = Schemas["PromotionGate-Output"];
type Candidate = Schemas["StrategyPromotionCandidateReference"];
function candidateIdentity(candidate: Candidate): string {
  return JSON.stringify(candidate);
}
const STAGES: Stage[] = ["exploratory", "comparable", "paper_candidate", "monitor_approved"];
const STAGE_LABELS: Record<Stage, string> = {
  exploratory: "探索",
  comparable: "可比",
  paper_candidate: "模拟候选",
  monitor_approved: "监控批准",
};
const GATE_STATUS: Record<Gate["status"], string> = {
  satisfied: "已满足",
  failed: "未通过",
  missing: "待补证据",
};
const GATE_LABELS: Record<string, string> = {
  preregistered: "预登记",
  fixed_validation: "固定区间",
  validation_trades: "验证交易",
  full_costs: "完整成本",
  manual_comparable: "可比阶段批准",
  validation_sharpe: "验证夏普",
  full_parent_bh: "全搜索校正",
  unique_outer: "样本外收益",
  six_folds: "六折验证",
  manual_paper: "模拟候选批准",
  forward_open_days: "完整开市日",
  original_band: "原收益区间",
  original_reconciliation: "账本对账",
};
function displayValue(gate: Gate): string {
  if (gate.value === null || gate.value === undefined) return "—";
  const number = Number(gate.value);
  if (!Number.isFinite(number)) return "—";
  if (gate.key === "unique_outer") return `${(number * 100).toFixed(2)}%`;
  if (["validation_trades", "forward_open_days", "six_folds"].includes(gate.key))
    return number.toLocaleString("zh-CN", { maximumFractionDigits: 0 });
  return number.toLocaleString("zh-CN", {
    maximumFractionDigits: gate.key === "full_parent_bh" ? 6 : 2,
  });
}
const gateColumns: DataColumn<Gate>[] = [
  {
    id: "gate",
    header: "证据",
    value: (row) => GATE_LABELS[row.key] ?? "来源证据",
    cell: (row) => <Tip content={row.message}>{GATE_LABELS[row.key] ?? "来源证据"}</Tip>,
  },
  {
    id: "value",
    header: "原结果",
    value: displayValue,
    cell: (row) => <span className="num">{displayValue(row)}</span>,
  },
  {
    id: "status",
    header: "评估",
    value: (row) => GATE_STATUS[row.status],
    cell: (row) => (
      <span className={`promotion-gate-${row.status}`}>{GATE_STATUS[row.status]}</span>
    ),
  },
];
function belongs(
  review: PromotionReview | undefined | null,
  target: PromotionTarget | undefined,
  viewer: string,
): review is PromotionReview {
  return Boolean(
    review && target && review.actor_id === viewer && samePromotionValue(review.target, target),
  );
}
function approvalMatches(
  preparation: PromotionPreparation,
  review: PromotionReview,
  role: Schemas["CollaborationMe"],
  viewer: string,
): boolean {
  const now = Date.now();
  const issued = Date.parse(preparation.issued_at),
    expires = Date.parse(preparation.expires_at);
  return Boolean(
    preparation.actor_id === viewer &&
      preparation.role_revision === role.revision &&
      preparation.role_state_hash === role.state_sha256 &&
      /^[a-f0-9]{64}$/.test(preparation.issuance_proof) &&
      samePromotionValue(preparation.review, review) &&
      Number.isFinite(issued) &&
      Number.isFinite(expires) &&
      issued <= now &&
      expires > now &&
      expires - issued <= 120_000 &&
      review.gates.length > 0 &&
      review.gates.every((gate) => gate.status === "satisfied"),
  );
}
export function StrategyPromotionPanel({
  viewer,
  generation,
  ready,
  sourceKind,
  strategyId,
  head,
  version,
}: {
  viewer: string;
  generation: string | null;
  ready: boolean;
  sourceKind: PromotionTarget["source_kind"];
  strategyId: string;
  head?: Head;
  version?: number;
}) {
  const role = useCollaboration();
  const meta = useCurrentMeta();
  usePromotionPrivateCleanup(viewer, role.me?.state_sha256);
  const identity = `${viewer}:${role.me?.state_sha256 ?? ""}:${generation ?? ""}:${sourceKind}:${strategyId}:${JSON.stringify(head ?? version)}`;
  const [selected, setSelected] = useState<{
    identity: string;
    candidateKey: string;
  } | null>(null);
  const [offset, setOffset] = useState<{ identity: string; value: number }>({ identity, value: 0 });
  const [evidence, setEvidence] = useState<{
    identity: string;
    selection: Schemas["PromotionEvidenceSelection"];
  } | null>(null);
  const [evaluated, setEvaluated] = useState<{ view: string; review: PromotionReview } | null>(
    null,
  );
  const [preparation, setPreparation] = useState<{
    view: string;
    value: PromotionPreparation;
  } | null>(null);
  const [notice, setNotice] = useState<{ view: string; message: string } | null>(null);
  const origin = useRef<HTMLSpanElement>(null);
  const lookupOnce = useRef<string | null>(null);
  const authorized =
    ready && role.current && role.viewer === viewer && role.generation === generation;
  const query = usePromotionData(
    viewer,
    role.me?.state_sha256,
    generation,
    sourceKind,
    strategyId,
    head?.version ?? version,
    authorized,
    offset.identity === identity ? offset.value : 0,
  );
  const matches =
    authorized &&
    !query.error &&
    query.serving?.state === "ready" &&
    query.serving.generation_id === generation &&
    query.data?.strategy_id === strategyId &&
    query.data.source_kind === sourceKind;
  const source: PromotionData | undefined = matches ? query.data : undefined;
  const choices =
    source?.candidates.filter(
      (item) =>
        item.target.owner_id === viewer &&
        item.target.source_kind === sourceKind &&
        (sourceKind === "template"
          ? (item.template_parent?.strategy_id === strategyId &&
              (!head || samePromotionValue(item.template_parent.head, head))) ||
            (item.target.strategy_id === strategyId &&
              (!head || samePromotionValue(item.target.head, head)))
          : item.target.strategy_id === strategyId &&
            (version === undefined || item.target.head.version === version)),
    ) ?? [];
  const chosen =
    choices.find(
      (item) =>
        selected?.identity === identity && candidateIdentity(item) === selected.candidateKey,
    ) ?? choices[0];
  const target = chosen?.target;
  const selectionIdentity = `${identity}:${chosen ? candidateIdentity(chosen) : ""}`;
  const fact = source?.states.find(
    (item) => item.owner_id === viewer && samePromotionValue(item.state.target, target),
  );
  const stage = fact?.state.stage ?? "exploratory";
  const revision = fact?.state.revision ?? 0;
  const view = `${identity}:${JSON.stringify(chosen ?? null)}:${stage}:${revision}`;
  const currentView = useRef(view);
  currentView.current = view;
  const commands = usePromotionCommands(viewer, selectionIdentity);
  const result = commands.result;
  const folds =
    source?.walk_forward.filter(
      (item) =>
        item.family_id === chosen?.selection.family_id &&
        item.experiment_id === chosen?.selection.experiment_id &&
        (!fact || item.target_key === fact.target_key),
    ) ?? [];
  const papers =
    source?.paper_accounts.filter((item) => item.target_key === fact?.target_key) ?? [];
  const selectedEvidence = evidence?.identity === selectionIdentity ? evidence.selection : null;
  const paperId = selectedEvidence?.paper_account_id ?? papers[0]?.account_id ?? null;
  const paper = papers.find((item) => item.account_id === paperId);
  const bandId = selectedEvidence?.band_job_id ?? paper?.band_jobs[0] ?? null;
  const wfId =
    selectedEvidence?.walk_forward_id ??
    folds.find((item) => item.fold_count === 6 && item.submitted)?.command_id ??
    null;
  const selection: Schemas["PromotionEvidenceSelection"] | null = chosen
    ? { ...chosen.selection, walk_forward_id: wfId, paper_account_id: paperId, band_job_id: bandId }
    : null;
  const reviews =
    source?.reviews.filter(
      (item) =>
        belongs(item, target, viewer) &&
        item.selection.family_id === chosen?.selection.family_id &&
        item.selection.experiment_id === chosen?.selection.experiment_id,
    ) ?? [];
  const returnedReview = result?.review ?? result?.preparation?.review;
  const review =
    belongs(returnedReview, target, viewer) &&
    samePromotionValue(returnedReview.selection, selection)
      ? returnedReview
      : evaluated?.view === view
        ? evaluated.review
        : reviews[0];
  const currentReview =
    belongs(review, target, viewer) &&
    review.expected_revision === revision &&
    review.from_stage === stage &&
    review.to_stage === STAGES[revision + 1] &&
    samePromotionValue(review.selection, selection);
  const eligible =
    currentReview &&
    Boolean(review?.review_id) &&
    Boolean(review?.gates.length) &&
    review.gates.every((gate) => gate.status === "satisfied");
  const locked = commands.busy || commands.pending !== null;
  const canEvaluate = Boolean(
    authorized &&
      source?.can_evaluate &&
      role.me?.can_research &&
      chosen?.is_current &&
      stage !== "monitor_approved",
  );
  const canApprove = Boolean(
    canEvaluate && source?.can_prepare_approval && role.me?.role === "admin" && eligible,
  );
  const prepared =
    preparation?.view === view &&
    canApprove &&
    role.me &&
    review &&
    samePromotionValue(preparation.value.review, review)
      ? preparation.value
      : null;
  // biome-ignore lint/correctness/useExhaustiveDependencies: a new actor, role or generation clears private evaluation and confirmation views.
  useEffect(() => {
    setPreparation(null);
    setNotice(null);
    setEvaluated(null);
  }, [identity]);
  useEffect(() => {
    if (!prepared && preparation) setPreparation(null);
  }, [prepared, preparation]);
  useEffect(() => {
    const pending = commands.pending;
    const key = pending ? `${selectionIdentity}:${pending.command_id}` : null;
    if (key && commands.busy) lookupOnce.current = key;
    if (authorized && chosen && key && !commands.busy && lookupOnce.current !== key) {
      lookupOnce.current = key;
      void commands.lookup();
    }
  }, [authorized, chosen, selectionIdentity, commands.pending, commands.busy, commands.lookup]);
  function base() {
    if (!target || !generation) return null;
    return {
      target,
      generation_id: generation,
      command_id: crypto.randomUUID(),
      requested_at: new Date().toISOString(),
    };
  }
  async function evaluate(): Promise<void> {
    const command = base();
    if (!command || !selection || !canEvaluate || locked) return;
    setPreparation(null);
    setNotice(null);
    const started = view;
    const value = await commands.submit({
      ...command,
      kind: "request_promotion_review",
      expected_revision: revision,
      selection,
    });
    if (currentView.current === started && belongs(value?.review, target, viewer))
      setEvaluated({ view: started, review: value.review });
  }
  async function prepare(): Promise<void> {
    const command = base();
    if (!command || !canApprove || locked || !review?.review_id || !role.me) return;
    const started = view;
    setEvaluated({ view: started, review });
    const value = await commands.submit({
      ...command,
      kind: "prepare_promotion_approval",
      review_id: review.review_id,
    });
    if (currentView.current !== started) return;
    if (!value?.preparation || !approvalMatches(value.preparation, review, role.me, viewer)) {
      setNotice({ view: started, message: "确认已过期或证据已变化，请重新评估。" });
      return;
    }
    setPreparation({ view: started, value: value.preparation });
  }
  async function recover(resume: boolean): Promise<void> {
    if (!authorized || (resume && !role.me?.can_research)) return;
    const value = await (resume ? commands.resume() : commands.lookup());
    if (value?.status === "published") {
      void meta.refetch();
      query.refetch();
    }
  }
  async function approve(): Promise<void> {
    const command = base();
    if (
      !command ||
      !prepared ||
      !review ||
      !role.me ||
      locked ||
      !canApprove ||
      !approvalMatches(prepared, review, role.me, viewer)
    )
      return;
    setPreparation(null);
    const value = await commands.submit({
      ...command,
      kind: "approve_promotion",
      preparation: prepared,
      entered_name: prepared.review.target.name,
    });
    if (value?.status === "published" || value?.status === "completed_waiting_publication") {
      void meta.refetch();
      query.refetch();
    }
  }
  function changeEvidence(update: Partial<Schemas["PromotionEvidenceSelection"]>): void {
    if (!selection) return;
    setEvidence({ identity: selectionIdentity, selection: { ...selection, ...update } });
    setEvaluated(null);
    setPreparation(null);
    setNotice(null);
  }
  const confirmError = notice?.view === view ? notice.message : null;
  return (
    <Panel
      title="阶段评估"
      label="阶段评估"
      sub={
        <Tip content="各阶段都需完整原证据和人工批准。批准只改变研究阶段，不启动实盘或自动调参。">
          <span className="template-small-help">晋级规则</span>
        </Tip>
      }
    >
      <div className="promotion-panel">
        <ol className="promotion-stages" aria-label="研究阶段">
          {STAGES.map((item) => (
            <li key={item} aria-current={item === stage ? "step" : undefined}>
              {STAGE_LABELS[item]}
            </li>
          ))}
        </ol>
        {role.isLoading || (authorized && query.isLoading) ? (
          <SkeletonRows rows={3} />
        ) : role.error ? (
          <EmptyState title="权限暂不可用，请刷新。" />
        ) : !authorized ? (
          <EmptyState title="人工晋级暂不可用。" />
        ) : query.error || !source ? (
          <div role="alert" className="template-message">
            <p>{query.error?.message ?? "数据已更新，请重新查看。"}</p>
            <Button
              size="sm"
              onClick={() => {
                void meta.refetch();
                query.refetch();
              }}
            >
              重新查看评估
            </Button>
          </div>
        ) : source.availability === "unavailable" || !chosen ? (
          <EmptyState title={source.reason || "还没有同版本的完整验证结果。"} />
        ) : (
          <>
            <div className="promotion-heading">
              <label className="field">
                <span className="lbl">验证版本</span>
                <select
                  className="inp"
                  value={candidateIdentity(chosen)}
                  disabled={locked}
                  onChange={(event) => {
                    setSelected({ identity, candidateKey: event.target.value });
                    setEvidence(null);
                    setEvaluated(null);
                    setPreparation(null);
                    setOffset({ identity, value: 0 });
                  }}
                >
                  {choices.map((item, index) => (
                    <option key={candidateIdentity(item)} value={candidateIdentity(item)}>
                      方案 {index + 1} · 第 {item.target.head.version} 版
                    </option>
                  ))}
                </select>
              </label>
              <output className="promotion-current" aria-label="当前阶段">
                当前：{STAGE_LABELS[stage]}
              </output>
            </div>
            <dl className="promotion-source">
              <dt>验证来源</dt>
              <dd>
                {chosen.family_name} · 完整 {chosen.parent_count.toLocaleString("zh-CN")} 组
              </dd>
              <dt>训练区间</dt>
              <dd className="num">
                {chosen.train_window.start_date} — {chosen.train_window.end_date}
              </dd>
              <dt>验证区间</dt>
              <dd className="num">
                {chosen.validation_window.start_date} — {chosen.validation_window.end_date}
              </dd>
              <dt>结果</dt>
              <dd>
                <Tip
                  content={`原任务：${chosen.job_id}\n输入：${chosen.input_hash}\n配置：${chosen.spec_hash}\n清单：${chosen.manifest_hash ?? "缺失"}\n结果：${chosen.result_hash ?? "缺失"}`}
                >
                  {chosen.has_sealed_reference ? "完整封存" : "等待完整结果"}
                </Tip>
              </dd>
            </dl>
            {source.reason ? <p className="muted promotion-note">{source.reason}</p> : null}
            {stage === "comparable" ? (
              <div className="promotion-evidence-selects">
                <label className="field">
                  <span className="lbl">验证结果</span>
                  <select
                    className="inp"
                    value={wfId ?? ""}
                    disabled={locked}
                    onChange={(event) =>
                      changeEvidence({ walk_forward_id: event.target.value || null })
                    }
                  >
                    <option value="">尚无完整六折验证</option>
                    {folds.map((item, index) => (
                      <option key={item.command_id} value={item.command_id}>
                        {item.fold_count} 折 · {item.submitted ? "已提交" : "待确认"} · 第{" "}
                        {index + 1} 次
                      </option>
                    ))}
                  </select>
                </label>
                <Tip content="1至5折可用于研究。晋级需要原完整六折结果，至少四折收益为正。">
                  <span className="template-small-help">验证要求</span>
                </Tip>
              </div>
            ) : null}
            {stage === "paper_candidate" ? (
              <div className="promotion-evidence-selects">
                <label className="field">
                  <span className="lbl">模拟账户</span>
                  <select
                    className="inp"
                    value={paperId ?? ""}
                    disabled={locked}
                    onChange={(event) =>
                      changeEvidence({
                        paper_account_id: event.target.value || null,
                        band_job_id: null,
                      })
                    }
                  >
                    <option value="">等待同版本模拟账户</option>
                    {papers.map((item, index) => (
                      <option key={item.account_id} value={item.account_id}>
                        模拟账户 {index + 1}
                      </option>
                    ))}
                  </select>
                </label>
                <label className="field">
                  <span className="lbl">收益区间</span>
                  <select
                    className="inp"
                    value={bandId ?? ""}
                    disabled={locked}
                    onChange={(event) =>
                      changeEvidence({ band_job_id: event.target.value || null })
                    }
                  >
                    <option value="">等待完整收益区间</option>
                    {paper?.band_jobs.map((job, index) => (
                      <option key={job} value={job}>
                        原完整区间 {index + 1}
                      </option>
                    ))}
                  </select>
                </label>
                <Tip content="模拟候选批准后，检查20个完整开市日、原5%至95%收益区间和完整账本对账。">
                  <span className="template-small-help">前向验证要求</span>
                </Tip>
              </div>
            ) : null}
            <div className="promotion-actions">
              <Button
                size="sm"
                variant="primary"
                disabledReason={
                  locked
                    ? "先查看原操作"
                    : !canEvaluate
                      ? stage === "monitor_approved"
                        ? "已达到监控批准"
                        : "当前版本或角色不能评估"
                      : undefined
                }
                onClick={() => void evaluate()}
              >
                评估下一阶段
              </Button>
              <span ref={origin}>
                <Button
                  size="sm"
                  disabledReason={
                    locked ? "先查看原操作" : !canApprove ? "评估通过后由管理员批准" : undefined
                  }
                  onClick={() => void prepare()}
                >
                  批准晋级
                </Button>
              </span>
              {stage === "comparable" ? (
                <Button
                  size="sm"
                  disabledReason={
                    locked
                      ? "先查看原操作"
                      : !source.can_run_walk_forward || !role.me?.can_research || !chosen.is_current
                        ? "当前版本不能运行验证"
                        : undefined
                  }
                  onClick={() => {
                    const command = base();
                    if (
                      command &&
                      selection &&
                      !locked &&
                      source.can_run_walk_forward &&
                      role.me?.can_research
                    )
                      void commands.submit({
                        ...command,
                        kind: "run_strategy_walk_forward",
                        selection,
                        fold_count: 6,
                      });
                  }}
                >
                  运行六折验证
                </Button>
              ) : null}
              <Button
                size="sm"
                onClick={() => {
                  void meta.refetch();
                  query.refetch();
                }}
              >
                刷新证据
              </Button>
            </div>
            {commands.error || result || commands.pending ? (
              <div className="promotion-receipt" role="status">
                <p>{commands.error ?? result?.message ?? "有待确认的操作，请查看原操作。"}</p>
                {commands.pending ? (
                  <>
                    <Button size="sm" disabled={commands.busy} onClick={() => void recover(false)}>
                      查看原操作
                    </Button>
                    <Button
                      size="sm"
                      disabledReason={
                        commands.busy
                          ? "正在查看原操作"
                          : !authorized ||
                              !role.me?.can_research ||
                              result?.status === "not_registered"
                            ? "当前只能查询原操作"
                            : undefined
                      }
                      onClick={() => {
                        if (result?.status !== "not_registered") void recover(true);
                      }}
                    >
                      恢复原操作
                    </Button>
                  </>
                ) : null}
                {result?.preparation && !commands.pending ? (
                  <Button
                    size="sm"
                    disabledReason={!canApprove ? "重新评估后可继续" : undefined}
                    onClick={() => {
                      if (
                        review &&
                        role.me &&
                        result.preparation &&
                        approvalMatches(result.preparation, review, role.me, viewer)
                      )
                        setPreparation({ view, value: result.preparation });
                      else setNotice({ view, message: "确认已过期或证据已变化，请重新评估。" });
                    }}
                  >
                    核对原确认
                  </Button>
                ) : null}
              </div>
            ) : null}
            {confirmError ? (
              <p role="alert" className="crit-text">
                {confirmError}
              </p>
            ) : null}
            {review ? (
              <section className="promotion-review">
                <div className="promotion-review-heading">
                  <h3>
                    {STAGE_LABELS[review.from_stage]} → {STAGE_LABELS[review.to_stage]}
                  </h3>
                  <RelativeTime at={review.observed_at} />
                </div>
                {!currentReview ? (
                  <p className="muted promotion-note">这是旧阶段或旧证据记录，重新评估后可继续。</p>
                ) : null}
                <DataTable
                  rows={review.gates}
                  columns={gateColumns}
                  rowKey={(row) => row.key}
                  label="阶段证据"
                />
              </section>
            ) : null}
            {reviews.length ? (
              <section className="promotion-history">
                <h3>评估记录</h3>
                <ul>
                  {reviews.map((item) => (
                    <li key={item.review_id ?? item.command_id}>
                      <span>
                        {STAGE_LABELS[item.from_stage]} → {STAGE_LABELS[item.to_stage]}
                      </span>
                      <span>
                        {item.gates.length > 0 &&
                        item.gates.every((gate) => gate.status === "satisfied")
                          ? "评估通过"
                          : "需补证据"}
                      </span>
                      <RelativeTime at={item.observed_at} />
                    </li>
                  ))}
                </ul>
              </section>
            ) : null}
            {source.next_offset != null || offset.value > 0 ? (
              <div className="promotion-actions">
                {offset.identity === identity && offset.value > 0 ? (
                  <Button size="sm" onClick={() => setOffset({ identity, value: 0 })}>
                    最近评估
                  </Button>
                ) : null}
                {source.next_offset != null ? (
                  <Button
                    size="sm"
                    onClick={() => setOffset({ identity, value: source.next_offset ?? 0 })}
                  >
                    更早评估
                  </Button>
                ) : null}
              </div>
            ) : null}
          </>
        )}
        {prepared ? (
          <ConfirmDialog
            open
            level="high"
            title="批准晋级"
            confirmName={prepared.review.target.name}
            expiresAt={new Date(prepared.expires_at)}
            confirmLabel="确认晋级"
            busy={commands.busy}
            disabled={locked || !canApprove}
            description={
              prepared ? (
                <>
                  <strong>{prepared.review.target.name}</strong>
                  <p>
                    {STAGE_LABELS[prepared.review.from_stage]} →{" "}
                    {STAGE_LABELS[prepared.review.to_stage]}
                  </p>
                  <p>批准只更新这一验证版本的研究阶段。</p>
                </>
              ) : null
            }
            onCancel={() => {
              setPreparation(null);
              queueMicrotask(() =>
                origin.current?.querySelector<HTMLButtonElement>("button")?.focus(),
              );
            }}
            afterClose={() => origin.current?.querySelector<HTMLButtonElement>("button")?.focus()}
            onConfirm={() => void approve()}
          />
        ) : null}
      </div>
    </Panel>
  );
}
