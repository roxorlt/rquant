import { useEffect, useMemo, useRef, useState, useSyncExternalStore } from "react";
import { usePools } from "@/api/endpoints";
import {
  isPoolRankingMetric,
  type PoolRankingPlan,
  submitPoolEditorCommand,
  usePoolEditor,
} from "@/api/poolEditor";
import {
  isFundamentalScreenField,
  type ScreenBlock,
  type ScreenOption,
  type ScreenQueryReadData,
  type ScreenRunRequest,
} from "@/api/screen";
import { useCurrentMeta } from "@/api/useMeta";
import { Button, SideDrawer, Tip } from "@/ui";
import { describeConditions } from "./ScreenNaturalLanguage";
import {
  type ScreenPoolSaveCommand,
  ScreenPoolSaveSession,
  screenPoolSaveStorage,
} from "./screenPoolSaveSession";

const SAFE_NAME = /^[\w\u4e00-\u9fff-]{1,80}$/u;

function nextCommandId(): string {
  return `web-${Array.from(crypto.getRandomValues(new Uint8Array(16)), (item) => item.toString(16).padStart(2, "0")).join("")}`;
}

function publicationStatus(
  journal: ReturnType<ScreenPoolSaveSession["snapshot"]>["journal"],
  editor: ReturnType<typeof usePoolEditor>,
  pools: ReturnType<typeof usePools>,
  generation: string | null | undefined,
): "request" | "published" | "result" {
  if (journal?.status !== "succeeded" || journal.version === null) return "request";
  if (
    generation == null ||
    editor.serving?.generation_id !== generation ||
    pools.serving?.generation_id !== generation ||
    editor.data?.state !== "ready" ||
    pools.data?.state !== "ready"
  )
    return "request";
  const key = `user/${journal.body.base_name}`;
  const editable = editor.data.pools.find((pool) => pool.key === key);
  const published = pools.data.pools.find((pool) => pool.key === key);
  if (editable?.version !== journal.version || published?.definition?.state !== "available")
    return "request";
  return published.result.state === "current_rules" ? "result" : "published";
}

function saveableRanking(plan: ScreenRunRequest["ranking"]): PoolRankingPlan | null {
  if (!plan) return null;
  const conditions: PoolRankingPlan["conditions"] = [];
  for (const row of plan.conditions) {
    if (!isPoolRankingMetric(row.metric)) return null;
    conditions.push({
      metric: row.metric,
      ascending: row.ascending,
      weight: row.weight,
    });
  }
  return { top_n: plan.top_n, conditions };
}

function dailyLimitReason(candidate: ScreenRunRequest, writerReady = false): string | null {
  if (writerReady) return null;
  for (const condition of candidate.conditions) {
    const values = Object.values(condition.args ?? {});
    if (values.some((value) => isFundamentalScreenField(value)))
      return "基本面条件暂不能保存为每日池子。";
    if (
      (condition.key === "rsi_oversold" || condition.key === "rsi_overbought") &&
      ![6, 14].includes(Number(condition.args?.period))
    )
      return "自定义 RSI 暂不能保存为每日池子。";
    if (condition.key === "above_ma" && ![5, 10, 20, 60].includes(Number(condition.args?.period)))
      return "自定义均线暂不能保存为每日池子。";
    for (const value of values) {
      if (typeof value !== "string") continue;
      const rsi = /^RSI(\d+)\[\d+\]$/.exec(value);
      if (rsi && ![6, 14].includes(Number(rsi[1]))) return "自定义 RSI 暂不能保存为每日池子。";
      const ma = /^MA(\d+)(?:\[\d+\])?$/.exec(value);
      if (ma && ![5, 10, 20, 60].includes(Number(ma[1]))) return "自定义均线暂不能保存为每日池子。";
    }
  }
  return null;
}

function saveBody(
  candidate: ScreenRunRequest,
  name: string,
  ranking: PoolRankingPlan | null,
): ScreenPoolSaveCommand {
  const common = {
    command_id: nextCommandId(),
    requested_at: new Date().toISOString(),
    base_name: name,
    display_name: name,
    description: "",
    depends_on: null,
    delay_days: 0,
    rule_calls: candidate.conditions.map((condition) => ({
      name: condition.key,
      args: condition.args ?? {},
    })),
    include_columns: [],
    expected_version: null,
  };
  return { ...common, kind: "save_user_pool_v3", ranking };
}

function ScreenPoolPublication({
  journal,
  evidence,
  onCheck,
}: {
  journal: NonNullable<ReturnType<ScreenPoolSaveSession["snapshot"]>["journal"]>;
  evidence: ScreenQueryReadData["daily_run_evidence"];
  onCheck?: () => void;
}) {
  const meta = useCurrentMeta();
  const editor = usePoolEditor();
  const pools = usePools();
  const stage = publicationStatus(journal, editor, pools, meta.data?.serving.generation_id);
  const proof = evidence?.find(
    (item) =>
      item.preset_name === `user/${journal.body.base_name}` &&
      item.definition_version === journal.version,
  );
  const published = pools.data?.pools.find((item) => item.key === `user/${journal.body.base_name}`);
  const confirmed =
    stage === "result" &&
    proof != null &&
    proof.unknown_count === 0 &&
    published?.result.trade_date === proof.trade_date &&
    published.result.hit_count === proof.hit_count;
  return (
    <div className="screen-save-publication">
      <p>{stage === "request" ? "等待规则发布" : "规则已发布"}</p>
      <p>{confirmed ? "结果已按新规则更新" : "等待日终结果确认"}</p>
      {proof ? (
        <div>
          <p>
            {proof.trade_date} · 命中 {proof.hit_count} 只 · 待确认 {proof.unknown_count} 只
          </p>
          <Tip
            content={`输入 ${proof.content_digest}；结果 ${proof.result_version}；排名 ${proof.member_rank_digest}`}
          >
            <Button size="sm" variant="ghost">
              结果出处
            </Button>
          </Tip>
        </div>
      ) : null}
      {!confirmed ? (
        <Button
          size="sm"
          onClick={() => {
            void meta.refetch();
            editor.refetch();
            pools.refetch();
            onCheck?.();
          }}
        >
          检查更新
        </Button>
      ) : null}
    </div>
  );
}

export function ScreenPoolSave({
  candidate,
  blockedReason,
  blocks,
  rankingMetrics,
  dailyWriterCapability = null,
  dailyRunEvidence = [],
  onCheckEvidence,
}: {
  candidate: ScreenRunRequest | null;
  blockedReason: string | null;
  blocks: ScreenBlock[];
  rankingMetrics: ScreenOption[];
  dailyWriterCapability?: ScreenQueryReadData["daily_writer_capability"];
  dailyRunEvidence?: ScreenQueryReadData["daily_run_evidence"];
  onCheckEvidence?: () => void;
}) {
  const meta = useCurrentMeta();
  const viewer = meta.data?.data.viewer ?? null;
  const session = useMemo(
    () => new ScreenPoolSaveSession(viewer, screenPoolSaveStorage(), submitPoolEditorCommand),
    [viewer],
  );
  const snapshot = useSyncExternalStore(session.subscribe, session.snapshot, session.snapshot);
  const [open, setOpen] = useState(false);
  const [name, setName] = useState("");
  const inputRef = useRef<HTMLInputElement>(null);
  const autoRetry = useRef({ commandId: "", attempts: 0 });
  const journal = snapshot.journal;
  const validName = SAFE_NAME.test(name.trim());
  const descriptions = candidate
    ? describeConditions(candidate.conditions, blocks)?.descriptions
    : null;
  const rankDescriptions = candidate?.ranking?.conditions.map((row) => {
    const metric = rankingMetrics.find((item) => item.value === row.metric);
    return metric
      ? `${metric.label} · ${row.ascending ? "低值优先" : "高值优先"} · 权重 ${row.weight.toLocaleString("zh-CN")}%`
      : null;
  });
  const ranking = saveableRanking(candidate?.ranking);
  const writerReady =
    dailyWriterCapability?.contract === "daily-screen-writer/v1" &&
    dailyWriterCapability.serving_generation_id === meta.data?.serving.generation_id;
  const dailyReason = candidate ? dailyLimitReason(candidate, writerReady) : null;
  const rankingReason =
    candidate?.ranking && ranking === null ? "这项排名暂不能保存为每日池子。" : null;
  const readable =
    descriptions !== null && rankDescriptions?.every((item) => item !== null) !== false;
  const eligible =
    candidate !== null &&
    blockedReason === null &&
    dailyReason === null &&
    rankingReason === null &&
    readable &&
    (!candidate.ranking || ranking !== null);

  useEffect(() => {
    if (!open || journal) return;
    const frame = window.requestAnimationFrame(() => inputRef.current?.focus());
    return () => window.cancelAnimationFrame(frame);
  }, [open, journal]);

  useEffect(() => {
    if (!journal || snapshot.busy || !["pending", "processing", "unknown"].includes(journal.status))
      return;
    if (autoRetry.current.commandId !== journal.body.command_id)
      autoRetry.current = { commandId: journal.body.command_id, attempts: 0 };
    if (autoRetry.current.attempts >= 3) return;
    const timer = window.setTimeout(() => {
      autoRetry.current.attempts += 1;
      void session.advance();
    }, 1800);
    return () => window.clearTimeout(timer);
  }, [journal, session, snapshot.busy]);

  function save(): void {
    if (
      !eligible ||
      !validName ||
      viewer === null ||
      journal !== null ||
      !snapshot.storageAvailable
    )
      return;
    void session.start(saveBody(candidate, name.trim(), ranking));
  }

  return (
    <>
      <div className="screen-save-bar">
        <Button
          onClick={() => setOpen(true)}
          disabledReason={
            journal
              ? "请先核对上次保存。"
              : !viewer
                ? "请先登录，再保存池子。"
                : !snapshot.storageAvailable
                  ? "浏览器存储不可用，无法安全提交。"
                  : (blockedReason ??
                    dailyReason ??
                    rankingReason ??
                    (candidate === null
                      ? "先运行筛选，查看结果后再保存。"
                      : !readable
                        ? "条件目录已更新，请重新运行筛选。"
                        : undefined))
          }
        >
          保存为池子
        </Button>
        {journal ? (
          <Button size="sm" onClick={() => setOpen(true)}>
            查看保存进度
          </Button>
        ) : null}
        {dailyReason || rankingReason ? (
          <span className="screen-save-hint">{dailyReason ?? rankingReason}</span>
        ) : null}
        {journal?.status === "succeeded" ? (
          <span className="screen-save-hint" role="status">
            保存请求已完成，查看发布进度
          </span>
        ) : journal ? (
          <span className="screen-save-hint" role="status">
            保存状态待确认，点开继续核对
          </span>
        ) : null}
      </div>
      <SideDrawer
        open={open}
        onClose={() => setOpen(false)}
        title="保存为池子"
        footer={
          <div className="screen-save-footer">
            <Button onClick={() => setOpen(false)}>返回选股</Button>
            {journal === null ? (
              <Button
                variant="primary"
                onClick={save}
                disabledReason={
                  !eligible
                    ? (blockedReason ?? dailyReason ?? rankingReason ?? "请先运行筛选。")
                    : !validName
                      ? "池子名称请输入 1–80 个汉字、字母、数字、横线或下划线。"
                      : undefined
                }
              >
                保存池子
              </Button>
            ) : ["pending", "processing", "ambiguous", "unknown"].includes(journal.status) ? (
              <Button
                variant="primary"
                disabled={snapshot.busy}
                onClick={() => void session.advance()}
              >
                {snapshot.busy ? "正在核对…" : "继续核对"}
              </Button>
            ) : (
              <Button
                onClick={() => {
                  if (session.clear()) setName("");
                }}
              >
                {journal.status === "failed" ? "重新填写" : "保存另一只"}
              </Button>
            )}
          </div>
        }
      >
        <div className="screen-save-drawer">
          {journal ? (
            <div className="screen-save-state" role="status">
              <strong>
                {journal.status === "succeeded"
                  ? "保存请求已完成"
                  : journal.status === "failed"
                    ? "保存失败"
                    : "保存状态待确认"}
              </strong>
              <span>{journal.body.display_name}</span>
              {journal.message ? <p>{journal.message}</p> : null}
              {journal.status === "succeeded" ? (
                <ScreenPoolPublication
                  journal={journal}
                  evidence={dailyRunEvidence}
                  onCheck={onCheckEvidence}
                />
              ) : null}
            </div>
          ) : (
            <>
              <label className="field">
                <span className="lbl">池子名称</span>
                <input
                  ref={inputRef}
                  className="inp"
                  maxLength={80}
                  value={name}
                  onChange={(event) => setName(event.target.value)}
                  autoComplete="off"
                />
              </label>
              {candidate && descriptions ? (
                <section className="screen-save-preview" aria-label="将保存的规则">
                  <div className="screen-save-preview-head">
                    <strong>每日按这些条件筛选</strong>
                    <span className="num">{candidate.trade_date}</span>
                  </div>
                  <ol>
                    {descriptions.map((item, index) => (
                      // biome-ignore lint/suspicious/noArrayIndexKey: Equal rules can repeat and this preview has no stateful rows.
                      <li key={`${item.label}-${index}`}>
                        <b>{item.label}</b>
                        {item.parameters.length ? <span>{item.parameters.join(" · ")}</span> : null}
                      </li>
                    ))}
                  </ol>
                  {candidate.ranking && rankDescriptions ? (
                    <div className="screen-save-rank">
                      <strong>
                        排名后取前 {candidate.ranking.top_n.toLocaleString("zh-CN")} 只
                      </strong>
                      {rankDescriptions.map((item, index) => (
                        <span key={candidate.ranking?.conditions[index]?.metric}>{item}</span>
                      ))}
                    </div>
                  ) : null}
                  <p>保存的是每日运行规则，成员会在下次选股后更新。</p>
                </section>
              ) : (
                <p className="screen-save-hint">请先重新运行筛选，再保存规则。</p>
              )}
            </>
          )}
          {!snapshot.storageAvailable ? (
            <p className="screen-save-error" role="alert">
              {snapshot.message}
            </p>
          ) : null}
        </div>
      </SideDrawer>
    </>
  );
}
