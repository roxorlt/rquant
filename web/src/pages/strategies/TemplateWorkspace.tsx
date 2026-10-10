import { type FormEvent, useEffect, useRef, useState } from "react";
import { apiBaseUrl, type Schemas } from "@/api/client";
import { useCollaboration } from "@/api/collaboration";
import { type DataColumn, DataTable } from "@/table/DataTable";
import {
  Button,
  ConfirmDialog,
  EmptyState,
  Panel,
  RelativeTime,
  SideDrawer,
  SkeletonRows,
  Tip,
} from "@/ui";
import { type SelectedTask, TaskProgressDrawer } from "../tasks/TaskProgressDrawer";
import { StrategyPromotionPanel } from "./StrategyPromotionPanel";
import { TemplateEditor } from "./TemplateEditor";
import { TemplateRulesSummary } from "./TemplateRules";
import {
  type RunTemplate,
  type TemplateDetail,
  type TemplateHead,
  useTemplateCatalog,
  useTemplateDetail,
  useTemplateSources,
  useTemplateVersions,
} from "./templateApi";
import { useTemplateCommands } from "./templateCommands";
import "./templates.css";

type TemplateItem = Schemas["StrategyTemplateItem"];
type RecentRun = Schemas["StrategyTemplateRecentRun"];
type Mode = "detail" | "create" | "edit" | "run";

function sameHead(left: TemplateHead, right: TemplateHead): boolean {
  return (
    left.version === right.version &&
    left.registration_fingerprint === right.registration_fingerprint &&
    left.record_hash === right.record_hash &&
    left.spec_fingerprint === right.spec_fingerprint
  );
}
function exactRun(
  value: RecentRun | null | undefined,
  viewer: string,
  strategy: string,
  head: TemplateHead,
): RecentRun | null {
  return value?.owner_id === viewer && value.strategy_id === strategy && sameHead(value.head, head)
    ? value
    : null;
}

function RunEditor({
  detail,
  generation,
  locked,
  onSubmit,
}: {
  detail: TemplateDetail;
  generation: string;
  locked: boolean;
  onSubmit: (body: RunTemplate) => void;
}) {
  const [start, setStart] = useState("");
  const [end, setEnd] = useState("");
  const [cash, setCash] = useState("100000.00");
  const [error, setError] = useState<string | null>(null);
  function submit(event: FormEvent<HTMLFormElement>): void {
    event.preventDefault();
    const days = (Date.parse(end) - Date.parse(start)) / 86400000 + 1;
    if (
      !Number.isFinite(days) ||
      days < 1 ||
      days > 5 * 366 ||
      !/^\d+(\.\d{1,2})?$/.test(cash) ||
      Number(cash) <= 0 ||
      Number(cash) > 1e12
    ) {
      setError("请检查日期和初始资金。");
      return;
    }
    setError(null);
    onSubmit({
      kind: "run_strategy_template",
      command_id: crypto.randomUUID(),
      requested_at: new Date().toISOString(),
      generation_id: generation,
      strategy_id: detail.strategy_id,
      head: detail.head,
      expected_head: detail.current_head,
      start_date: start,
      end_date: end,
      initial_cash: cash,
    });
  }
  return (
    <form className="template-editor" onSubmit={submit}>
      <p>
        <strong>{detail.name}</strong> · 第 {detail.head.version} 版
      </p>
      <fieldset className="template-fields" disabled={locked}>
        <label className="field">
          <span className="lbl">开始日期</span>
          <input
            className="inp num"
            type="date"
            required
            value={start}
            max={end || undefined}
            onChange={(event) => setStart(event.target.value)}
          />
        </label>
        <label className="field">
          <span className="lbl">结束日期</span>
          <input
            className="inp num"
            type="date"
            required
            value={end}
            min={start || undefined}
            onChange={(event) => setEnd(event.target.value)}
          />
        </label>
        <label className="field">
          <span className="lbl">初始资金（元）</span>
          <input
            className="inp num"
            type="number"
            min="0.01"
            max="1000000000000"
            step="0.01"
            required
            value={cash}
            onChange={(event) => setCash(event.target.value)}
          />
        </label>
      </fieldset>
      <Tip content="按所选版本和受信历史材料运行。日历、成本与交易状态由服务提供；缺材料时拒绝。">
        <span className="template-small-help">回测范围说明</span>
      </Tip>
      {error ? (
        <p role="alert" className="crit-text">
          {error}
        </p>
      ) : null}
      <div className="template-form-actions">
        <Button
          type="submit"
          variant="primary"
          disabledReason={
            locked ? "先查看这次操作的结果" : !detail.can_run ? "回测暂时不可用" : undefined
          }
        >
          提交回测
        </Button>
      </div>
    </form>
  );
}

export function TemplateWorkspace({
  viewer,
  generation,
  ready,
}: {
  viewer: string;
  generation: string | null;
  ready: boolean;
}) {
  const catalog = useTemplateCatalog(viewer, generation);
  const collaboration = useCollaboration();
  const sources = useTemplateSources(viewer, generation);
  const [selected, setSelected] = useState<string | null>(null);
  const [version, setVersion] = useState<number | null>(null);
  const [before, setBefore] = useState<number | null>(null);
  const [mode, setMode] = useState<Mode>("detail");
  const [open, setOpen] = useState(false);
  const [archive, setArchive] = useState(false);
  const [progress, setProgress] = useState<SelectedTask | null>(null);
  const origin = useRef<HTMLElement | null>(null);
  const createOrigin = useRef<HTMLSpanElement>(null);
  const fromCreate = useRef(false);
  const detail = useTemplateDetail(viewer, generation, selected, version);
  const history = useTemplateVersions(viewer, generation, selected, before);
  const commands = useTemplateCommands(viewer);
  const lastPublished = useRef<string | null>(null);
  const locked = commands.busy || commands.pending !== null;
  const matches =
    ready && catalog.serving?.state === "ready" && catalog.serving.generation_id === generation;
  const sourceMatches =
    ready && sources.serving?.state === "ready" && sources.serving.generation_id === generation;
  const detailMatches =
    ready &&
    detail.serving?.state === "ready" &&
    detail.serving.generation_id === generation &&
    detail.data?.strategy_id === selected &&
    (version === null || detail.data.head.version === version);
  const current = detailMatches && !detail.error ? detail.data : undefined;
  const canCreate =
    matches && sourceMatches && catalog.data?.can_create && sources.data?.can_create;
  const rows = matches && !catalog.error ? (catalog.data?.templates ?? []) : [];

  useEffect(() => {
    if (
      commands.result?.status === "published" &&
      lastPublished.current !== commands.result.command_id
    ) {
      lastPublished.current = commands.result.command_id;
      catalog.refetch();
      if (selected !== null) {
        detail.refetch();
        history.refetch();
      }
      setArchive(false);
    }
  }, [commands.result, catalog.refetch, detail.refetch, history.refetch, selected]);

  function enter(next: Mode, strategyId: string | null = selected): void {
    if (!open) {
      origin.current =
        document.activeElement instanceof HTMLElement ? document.activeElement : null;
      fromCreate.current = next === "create";
    }
    setMode(next);
    setSelected(strategyId);
    setVersion(null);
    setBefore(null);
    setOpen(true);
  }
  function close(): void {
    setOpen(false);
    setArchive(false);
    setMode("detail");
    window.requestAnimationFrame(() => {
      if (fromCreate.current) {
        const button = createOrigin.current?.querySelector("button");
        const target = button?.disabled
          ? createOrigin.current?.querySelector<HTMLElement>(".tip-anchor")
          : button;
        target?.focus();
      } else if (origin.current?.isConnected) origin.current.focus();
    });
  }
  function showProgress(jobId: string, name: string): void {
    if (generation !== null) setProgress({ jobId, name, generationId: generation, viewer });
  }

  const columns: DataColumn<TemplateItem>[] = [
    {
      id: "name",
      header: "策略",
      value: (row) => row.name,
      cell: (row) => (
        <span className="template-name">
          {row.name}
          {row.archived ? <span className="template-archived">已归档</span> : null}
        </span>
      ),
    },
    { id: "phase", header: "阶段", value: (row) => row.phase },
    {
      id: "version",
      header: "版本",
      value: (row) => row.head.version,
      cell: (row) => `第 ${row.head.version} 版`,
    },
    {
      id: "recent",
      header: "最近回测",
      value: (row) =>
        exactRun(row.latest_run, viewer, row.strategy_id, row.head)?.completed_at ?? "",
      cell: (row) => {
        const run = exactRun(row.latest_run, viewer, row.strategy_id, row.head);
        return run ? <RelativeTime at={run.completed_at} /> : "暂无回测";
      },
    },
    {
      id: "saved",
      header: "保存时间",
      value: (row) => row.saved_at,
      secondary: true,
      cell: (row) => <RelativeTime at={row.saved_at} />,
    },
  ];

  return (
    <div className="template-workspace">
      <Panel
        title="我的策略"
        actions={
          <span ref={createOrigin}>
            <Button
              variant="primary"
              onClick={() => enter("create", null)}
              disabledReason={
                locked ? "先查看这次操作的结果" : !canCreate ? "策略编辑暂时不可用" : undefined
              }
            >
              新建策略
            </Button>
          </span>
        }
        flush
      >
        {generation === null || catalog.data?.availability === "unavailable" ? (
          <EmptyState title="策略模板暂不可用" hint="数据恢复后可继续查看。" />
        ) : catalog.isLoading ? (
          <div role="status" aria-label="正在加载我的策略">
            <SkeletonRows rows={3} />
          </div>
        ) : catalog.error || !matches ? (
          <div className="strategy-state" role="alert">
            <p>{catalog.error?.message ?? "数据已更新，请重新查看策略。"}</p>
            <Button size="sm" onClick={catalog.refetch}>
              刷新模板
            </Button>
          </div>
        ) : (
          <DataTable
            rows={rows}
            columns={columns}
            rowKey={(row) => row.strategy_id}
            label="我的策略"
            onSelect={(row) => enter("detail", row.strategy_id)}
            emptyText="还没有策略，点新建开始"
          />
        )}
      </Panel>
      {commands.result || commands.pending ? (
        <Panel label="操作结果">
          <div className="template-receipt" role="status">
            <p>{commands.result?.message ?? "上次操作待确认，请继续查看。"}</p>
            {commands.pending ? (
              <Button size="sm" disabled={commands.busy} onClick={() => void commands.resume()}>
                继续查看结果
              </Button>
            ) : null}
            {commands.result?.status === "submitted" &&
            "job_id" in commands.result &&
            commands.result.job_id ? (
              <Button
                size="sm"
                onClick={() => {
                  const jobId =
                    commands.result && "job_id" in commands.result ? commands.result.job_id : null;
                  if (jobId) showProgress(jobId, "策略回测");
                }}
              >
                查看回测进展
              </Button>
            ) : null}
          </div>
        </Panel>
      ) : null}
      <SideDrawer
        open={open}
        onClose={close}
        wide
        title={
          mode === "create"
            ? "新建策略"
            : mode === "edit"
              ? "保存新版本"
              : mode === "run"
                ? "运行回测"
                : (current?.name ?? "策略详情")
        }
      >
        {mode === "create" ? (
          sourceMatches && sources.data && generation ? (
            <TemplateEditor
              sources={sources.data}
              generation={generation}
              locked={locked}
              onSave={(body) => void commands.submit(body)}
            />
          ) : (
            <EmptyState title="入场来源暂不可用" hint="请关闭并刷新后重试。" />
          )
        ) : detail.isLoading ? (
          <div role="status" aria-label="正在加载策略详情">
            <SkeletonRows rows={4} />
          </div>
        ) : !current ? (
          <div role="alert" className="template-message">
            <p>{detail.error?.message ?? "策略已变化，请重新查看。"}</p>
            <Button size="sm" onClick={detail.refetch}>
              重新查看
            </Button>
          </div>
        ) : mode === "edit" ? (
          sourceMatches && sources.data && generation && current.can_save ? (
            <TemplateEditor
              key={`${current.strategy_id}:${current.head.version}`}
              sources={sources.data}
              initial={current}
              generation={generation}
              locked={locked}
              onSave={(body) => void commands.submit(body)}
            />
          ) : (
            <EmptyState title="当前策略不能编辑" />
          )
        ) : mode === "run" ? (
          generation ? (
            <RunEditor
              detail={current}
              generation={generation}
              locked={locked}
              onSubmit={(body) => void commands.submit(body)}
            />
          ) : null
        ) : (
          <div className="template-detail">
            <div className="template-detail-heading">
              <p>
                第 {current.head.version} 版
                {current.head.version === current.current_head.version
                  ? " · 当前版本"
                  : " · 历史版本"}
                {current.archived ? " · 已归档" : ""}
              </p>
              <RelativeTime at={current.saved_at} />
            </div>
            {current.change_note ? (
              <p className="template-change-note">{current.change_note}</p>
            ) : null}
            <div className="template-actions">
              <Button
                size="sm"
                onClick={() => {
                  setMode("edit");
                }}
                disabledReason={
                  locked
                    ? "先查看这次操作的结果"
                    : !current.can_save
                      ? current.archived
                        ? "策略已归档"
                        : "当前账号不能编辑"
                      : !sourceMatches
                        ? "入场来源暂不可用"
                        : undefined
                }
              >
                保存新版本
              </Button>
              <Button
                size="sm"
                variant="primary"
                onClick={() => setMode("run")}
                disabledReason={
                  locked
                    ? "先查看这次操作的结果"
                    : !current.can_run
                      ? current.archived
                        ? "策略已归档"
                        : "回测暂时不可用"
                      : undefined
                }
              >
                运行回测
              </Button>
              <Button
                size="sm"
                onClick={() => setArchive(true)}
                disabledReason={
                  locked
                    ? "先查看这次操作的结果"
                    : !current.can_archive
                      ? current.archived
                        ? "策略已归档"
                        : "当前账号不能归档"
                      : undefined
                }
              >
                归档策略
              </Button>
            </div>
            <TemplateRulesSummary
              rules={current.rules}
              sources={sourceMatches ? sources.data : undefined}
            />
            <StrategyPromotionPanel
              key={`${viewer}:${generation}:${current.strategy_id}:${current.head.version}`}
              viewer={viewer}
              generation={generation}
              ready={ready && detailMatches}
              sourceKind="template"
              strategyId={current.strategy_id}
              head={current.head}
            />
            <section>
              <h3>最近回测</h3>
              {(() => {
                const run = exactRun(current.latest_run, viewer, current.strategy_id, current.head);
                return run ? (
                  <div className="template-actions">
                    <RelativeTime at={run.completed_at} />
                    <Button size="sm" onClick={() => showProgress(run.job_id, current.name)}>
                      查看回测进展
                    </Button>
                    {collaboration.current &&
                    collaboration.viewer === viewer &&
                    collaboration.generation === generation &&
                    /^[a-f0-9]{64}$/.test(run.complete_result_hash) ? (
                      <Tip content="下载这一版本的完整封存结果，可离线查看。" interactive>
                        <a
                          className="btn sm"
                          href={`${apiBaseUrl()}/api/v1/experiments/template-results/${encodeURIComponent(run.job_id)}/report.html?result_hash=${encodeURIComponent(run.complete_result_hash)}`}
                          download
                        >
                          导出只读页面
                        </a>
                      </Tip>
                    ) : null}
                  </div>
                ) : (
                  <p className="muted">暂无回测</p>
                );
              })()}
            </section>
            <section>
              <h3>版本历史</h3>
              {history.isLoading ? (
                <SkeletonRows rows={2} />
              ) : history.error || history.serving?.generation_id !== generation ? (
                <div className="template-message" role="alert">
                  <p>版本历史暂不可用。</p>
                  <Button size="sm" onClick={history.refetch}>
                    刷新版本
                  </Button>
                </div>
              ) : (
                <>
                  <ul className="template-history">
                    {history.data?.versions.map((item) => (
                      <li key={item.head.version}>
                        <div>
                          <Button
                            size="sm"
                            variant="ghost"
                            onClick={() => {
                              setVersion(item.head.version);
                            }}
                          >
                            第 {item.head.version} 版{item.is_head ? " · 当前" : ""}
                          </Button>
                          <RelativeTime at={item.saved_at} />
                        </div>
                        <p>{item.change_note || "首次保存"}</p>
                        {exactRun(item.latest_run, viewer, current.strategy_id, item.head) ? (
                          <Button
                            size="sm"
                            variant="ghost"
                            onClick={() => {
                              const run = exactRun(
                                item.latest_run,
                                viewer,
                                current.strategy_id,
                                item.head,
                              );
                              if (run) showProgress(run.job_id, current.name);
                            }}
                          >
                            查看本版回测
                          </Button>
                        ) : (
                          <span className="muted">暂无回测</span>
                        )}
                      </li>
                    ))}
                  </ul>
                  <div className="template-actions">
                    {before !== null ? (
                      <Button size="sm" onClick={() => setBefore(null)}>
                        返回最近版本
                      </Button>
                    ) : null}
                    {history.data?.next_before_version != null ? (
                      <Button
                        size="sm"
                        onClick={() => setBefore(history.data?.next_before_version ?? null)}
                      >
                        更早版本
                      </Button>
                    ) : null}
                  </div>
                </>
              )}
            </section>
          </div>
        )}
      </SideDrawer>
      <ConfirmDialog
        open={archive && current !== undefined}
        level="heavy"
        title="归档策略"
        description={
          <>
            <strong>{current?.name}</strong>
            <p>归档后关闭新编辑和新回测，历史版本仍可查看。</p>
          </>
        }
        confirmLabel="确认归档"
        busy={commands.busy}
        disabled={locked || !current?.can_archive}
        onCancel={() => setArchive(false)}
        onConfirm={() => {
          if (!current || !generation || locked) return;
          void commands.submit({
            kind: "archive_strategy_template",
            command_id: crypto.randomUUID(),
            requested_at: new Date().toISOString(),
            generation_id: generation,
            strategy_id: current.strategy_id,
            expected_head: current.current_head,
          });
        }}
      />
      <TaskProgressDrawer
        selected={progress}
        onClose={() => setProgress(null)}
        onInvalidated={() => setProgress(null)}
      />
    </div>
  );
}
