import { useEffect, useRef, useState } from "react";
import { ApiError } from "@/api/client";
import {
  type FactorArchiveCommandData,
  type FactorArchiveCommandRequest,
  type FactorDefinitionItem,
  type FactorSaveCommandData,
  type FactorSaveDraft,
  postFactorArchive,
  postFactorSave,
  useFactorCapabilities,
  useFactorCatalog,
} from "@/api/factors";
import { useCurrentMeta } from "@/api/useMeta";
import { type DataColumn, DataTable } from "@/table/DataTable";
import {
  Button,
  ConfirmDialog,
  EmptyState,
  PageHeader,
  PageSkeleton,
  Panel,
  RelativeTime,
  Tip,
} from "@/ui";
import { FactorEditor, type FactorEditorDraft } from "./FactorEditor";
import { FactorResults } from "./FactorResults";
import { FactorRunConfirmation, FactorRunParameters, FactorRunStatus } from "./FactorRun";
import { hasDefinitionCommand, hasRun, withFactorCommandLock } from "./factorRunState";
import {
  clearSaveCommand,
  persistEditorDraft,
  persistSaveCommand,
  persistSaveRejection,
  readEditorDraft,
  readSaveCommand,
  readSaveRejection,
  SAVE_DRAFT_KEY,
  storageWritable,
} from "./factorSaveState";
import { useFactorRun } from "./useFactorRun";
import "./factors.css";

const ARCHIVE_STORAGE_KEY = "rquant.factor.archive-command.v1";

type StoredArchive = { factorId: string; command: FactorArchiveCommandRequest };

function sameArchive(current: StoredArchive | null, candidate: StoredArchive): boolean {
  return (
    current !== null &&
    current.factorId === candidate.factorId &&
    current.command.command_id === candidate.command.command_id &&
    current.command.generation_id === candidate.command.generation_id &&
    current.command.requested_at === candidate.command.requested_at
  );
}

function sameSave(current: FactorSaveDraft | null, candidate: FactorSaveDraft): boolean {
  return (
    current !== null &&
    current.command_id === candidate.command_id &&
    current.requested_at === candidate.requested_at &&
    current.generation_id === candidate.generation_id
  );
}

function storedArchive(): StoredArchive | null {
  try {
    const raw = window.localStorage.getItem(ARCHIVE_STORAGE_KEY);
    if (raw === null) return null;
    const parsed: unknown = JSON.parse(raw);
    if (typeof parsed !== "object" || parsed === null) return null;
    const record = parsed as Partial<StoredArchive>;
    if (
      typeof record.factorId !== "string" ||
      typeof record.command?.command_id !== "string" ||
      typeof record.command.requested_at !== "string" ||
      typeof record.command.generation_id !== "string" ||
      typeof record.command.expected_head?.version !== "number" ||
      typeof record.command.expected_head.content_sha256 !== "string"
    ) {
      return null;
    }
    return record as StoredArchive;
  } catch {
    return null;
  }
}

const columns: DataColumn<FactorDefinitionItem>[] = [
  {
    id: "name",
    header: "因子",
    value: (row) => row.name_zh,
    cell: (row) => (
      <span className="factor-name-cell">
        <span>{row.name_zh}</span>
        {row.archived ? <span className="factor-archive">已归档</span> : null}
      </span>
    ),
    sortable: true,
  },
  { id: "category", header: "分类", value: (row) => row.category_label, secondary: true },
  {
    id: "version",
    header: "版本",
    value: (row) => row.version,
    cell: (row) => `第 ${row.version} 版`,
    secondary: true,
  },
];

export default function FactorsPage() {
  const meta = useCurrentMeta();
  const currentGeneration =
    meta.data === undefined ? undefined : (meta.data.data.generation?.generation_id ?? null);
  const viewer = meta.data?.data.viewer;
  const [permissionRevision, setPermissionRevision] = useState(0);
  const catalog = useFactorCatalog(currentGeneration, viewer, permissionRevision);
  const capabilities = useFactorCapabilities(
    currentGeneration,
    catalog.data?.can_save === true && catalog.serving?.generation_id === currentGeneration,
    viewer,
    permissionRevision,
  );
  const [selection, setSelection] = useState<{ generationId: string; factorId: string } | null>(
    null,
  );
  const [archiveCommand, setArchiveCommand] = useState<StoredArchive | null>(storedArchive);
  const [editorDraft, setEditorDraft] = useState<FactorEditorDraft | null>(readEditorDraft);
  const [editorOpen, setEditorOpen] = useState(false);
  const [storageReady, setStorageReady] = useState(storageWritable);
  const [saveCommand, setSaveCommand] = useState<FactorSaveDraft | null>(readSaveCommand);
  const [saveResult, setSaveResult] = useState<FactorSaveCommandData | null>(
    () => readSaveRejection(saveCommand)?.result ?? null,
  );
  const [deniedViewer, setDeniedViewer] = useState<string | null>(
    () => readSaveRejection(saveCommand)?.deniedViewer ?? null,
  );
  const [saveError, setSaveError] = useState<string | null>(null);
  const [saveBusy, setSaveBusy] = useState(false);
  const saveCommandRef = useRef(saveCommand);
  const saveBusyRef = useRef(false);
  const restoredSave = useRef(false);
  const [archiveResult, setArchiveResult] = useState<FactorArchiveCommandData | null>(null);
  const [archiveError, setArchiveError] = useState<string | null>(null);
  const [archiveBusy, setArchiveBusy] = useState(false);
  const [confirmArchive, setConfirmArchive] = useState(false);
  const archiveCommandRef = useRef(archiveCommand);
  const archiveBusyRef = useRef(false);
  const restoredCommand = useRef(false);
  const selectedId =
    selection !== null && selection.generationId === currentGeneration ? selection.factorId : null;
  const changed =
    currentGeneration !== undefined &&
    catalog.serving !== undefined &&
    currentGeneration !== catalog.serving.generation_id;
  const rows = changed ? [] : (catalog.data?.definitions ?? []);
  const selected = rows.find((row) => row.factor_id === selectedId) ?? rows[0] ?? null;
  const catalogVerified =
    typeof currentGeneration === "string" &&
    catalog.serving?.generation_id === currentGeneration &&
    catalog.serving.state === "ready" &&
    catalog.data !== undefined &&
    catalog.data.availability !== "unavailable" &&
    !meta.isError &&
    !catalog.error &&
    !catalog.isFetching &&
    !changed;
  const run = useFactorRun({
    generationId: currentGeneration,
    viewer,
    permissionRevision,
    selected,
    catalogVerified,
    definitionBusy:
      saveCommand !== null ||
      archiveCommand !== null ||
      saveBusy ||
      archiveBusy ||
      (typeof viewer === "string" && viewer === deniedViewer),
  });
  const runBlocksWrite =
    run.occupied ||
    run.busy ||
    run.permissionDenied ||
    (typeof viewer === "string" && viewer === deniedViewer);
  const saveAvailable =
    typeof viewer === "string" &&
    viewer !== deniedViewer &&
    !run.permissionDenied &&
    catalogVerified &&
    catalog.data?.can_save === true &&
    capabilities.data?.can_save === true &&
    capabilities.serving?.generation_id === currentGeneration &&
    capabilities.serving.state === "ready" &&
    !capabilities.error &&
    !capabilities.isFetching &&
    meta.data?.serving.state === "ready";
  const draftCurrentRow =
    editorDraft?.factor_id === null
      ? null
      : (rows.find((row) => row.factor_id === editorDraft?.factor_id) ?? null);
  const draftStale =
    editorDraft !== null &&
    (editorDraft.generation_id !== currentGeneration ||
      (editorDraft.mode === "edit" &&
        (draftCurrentRow === null ||
          draftCurrentRow.archived ||
          draftCurrentRow.version !== editorDraft.expected_head?.version ||
          draftCurrentRow.content_sha256 !== editorDraft.expected_head.content_sha256)));
  const canRebaseDraft =
    saveAvailable &&
    (editorDraft?.mode === "create" || (draftCurrentRow !== null && !draftCurrentRow.archived));
  const canFinishSave =
    saveCommand !== null &&
    saveResult?.status === "published" &&
    typeof currentGeneration === "string" &&
    currentGeneration !== saveCommand.generation_id &&
    catalogVerified &&
    catalog.data?.definitions.some(
      (row) =>
        row.factor_id === saveResult.factor_id &&
        ((!row.archived &&
          row.version === saveResult.version &&
          row.content_sha256 === saveResult.content_sha256) ||
          (saveResult.current_head_updated &&
            saveResult.version !== null &&
            saveResult.version !== undefined &&
            row.version > saveResult.version)),
    ) === true;
  const canFinishArchive =
    archiveCommand !== null &&
    archiveResult?.status === "published" &&
    typeof currentGeneration === "string" &&
    currentGeneration !== archiveCommand.command.generation_id &&
    catalog.serving?.generation_id === currentGeneration &&
    catalog.data?.availability !== "unavailable" &&
    !catalog.isFetching;
  const runArchive = async (record: StoredArchive, resume: boolean) => {
    if (!sameArchive(archiveCommandRef.current, record)) return;
    archiveBusyRef.current = true;
    setArchiveBusy(true);
    setArchiveError(null);
    try {
      const result = await postFactorArchive(record.factorId, record.command, resume);
      if (!sameArchive(archiveCommandRef.current, record)) return;
      setArchiveResult(result);
      if (result.status === "published") {
        await meta.refetch();
      }
    } catch (error) {
      if (sameArchive(archiveCommandRef.current, record)) {
        setArchiveError(
          error instanceof Error ? error.message : "归档状态暂不可用，请用原命令继续查看。",
        );
      }
    } finally {
      if (sameArchive(archiveCommandRef.current, record)) {
        archiveBusyRef.current = false;
        setArchiveBusy(false);
      }
    }
  };

  const runSave = async (record: FactorSaveDraft, action: "save" | "resume" | "retry") => {
    if (!sameSave(saveCommandRef.current, record) || saveBusyRef.current) return;
    saveBusyRef.current = true;
    setSaveBusy(true);
    setSaveError(null);
    try {
      const result = await postFactorSave(record, action);
      if (!sameSave(saveCommandRef.current, record)) return;
      if (result.command_id !== record.command_id) {
        setSaveError("保存结果尚未确认，请保留这次操作。");
        return;
      }
      if (result.status === "rejected" && !persistSaveRejection(record, result))
        setStorageReady(false);
      setSaveResult(result);
      if (result.status === "published") void meta.refetch();
    } catch (error) {
      if (!sameSave(saveCommandRef.current, record)) return;
      const permissionDenied =
        error instanceof ApiError && (error.status === 401 || error.status === 403);
      if (permissionDenied) {
        setDeniedViewer(viewer ?? null);
        void meta.refetch();
      }
      if (
        action === "save" &&
        error instanceof ApiError &&
        [401, 403, 409, 422].includes(error.status)
      ) {
        const rejection: FactorSaveCommandData = {
          status: "rejected",
          command_id: record.command_id,
          message: error.message,
          current_head_updated: false,
        };
        if (!persistSaveRejection(record, rejection, permissionDenied ? (viewer ?? null) : null))
          setStorageReady(false);
        setSaveResult(rejection);
      } else {
        setSaveError("保存结果尚未确认，请保留这次操作。");
      }
    } finally {
      if (sameSave(saveCommandRef.current, record)) {
        saveBusyRef.current = false;
        setSaveBusy(false);
      }
    }
  };

  useEffect(() => {
    if (restoredSave.current) return;
    restoredSave.current = true;
    const record = saveCommandRef.current;
    if (record !== null && saveResult?.status !== "rejected") void runSave(record, "resume");
  });

  const updateEditorDraft = (draft: FactorEditorDraft) => {
    setEditorDraft(draft);
    if (!persistEditorDraft(draft)) setStorageReady(false);
  };

  const beginCreate = () => {
    if (
      !saveAvailable ||
      runBlocksWrite ||
      hasRun() ||
      hasDefinitionCommand() ||
      saveCommandRef.current !== null ||
      archiveCommandRef.current !== null ||
      typeof currentGeneration !== "string"
    )
      return;
    updateEditorDraft({
      generation_id: currentGeneration,
      mode: "create",
      factor_id: null,
      expected_head: null,
      name_zh: "",
      category: "技术",
      category_label: "技术",
      direction: "higher_is_better",
      expression: "",
    });
    setEditorOpen(true);
  };

  const beginEdit = () => {
    if (
      !saveAvailable ||
      runBlocksWrite ||
      hasRun() ||
      hasDefinitionCommand() ||
      saveCommandRef.current !== null ||
      archiveCommandRef.current !== null ||
      selected === null ||
      selected.archived ||
      typeof currentGeneration !== "string"
    )
      return;
    if (editorDraft?.mode === "edit" && editorDraft.factor_id === selected.factor_id) {
      setEditorOpen(true);
      return;
    }
    updateEditorDraft({
      generation_id: currentGeneration,
      mode: "edit",
      factor_id: selected.factor_id,
      expected_head: { version: selected.version, content_sha256: selected.content_sha256 },
      name_zh: selected.name_zh,
      category: selected.category,
      category_label: selected.category_label,
      direction: selected.direction,
      expression: selected.expression,
    });
    setEditorOpen(true);
  };

  const rebaseDraft = () => {
    if (!canRebaseDraft || editorDraft === null || typeof currentGeneration !== "string") return;
    if (editorDraft.mode === "edit" && draftCurrentRow === null) return;
    updateEditorDraft({
      ...editorDraft,
      generation_id: currentGeneration,
      expected_head:
        editorDraft.mode === "create" || draftCurrentRow === null
          ? null
          : {
              version: draftCurrentRow.version,
              content_sha256: draftCurrentRow.content_sha256,
            },
    });
  };

  const submitSave = async () => {
    if (
      editorDraft === null ||
      !storageReady ||
      !saveAvailable ||
      runBlocksWrite ||
      hasRun() ||
      draftStale ||
      saveCommandRef.current !== null ||
      archiveCommandRef.current !== null
    )
      return;
    const record: FactorSaveDraft = {
      generation_id: editorDraft.generation_id,
      command_id: crypto.randomUUID(),
      requested_at: new Date().toISOString(),
      mode: editorDraft.mode,
      factor_id: editorDraft.factor_id,
      expected_head: editorDraft.expected_head,
      name_zh: editorDraft.name_zh.trim(),
      category: editorDraft.category,
      direction: editorDraft.direction,
      expression: editorDraft.expression,
    };
    await withFactorCommandLock(() => {
      if (
        hasRun() ||
        hasDefinitionCommand() ||
        saveCommandRef.current !== null ||
        archiveCommandRef.current !== null
      ) {
        run.sync();
        return;
      }
      if (!persistEditorDraft(editorDraft) || !persistSaveCommand(record)) {
        setStorageReady(false);
        return;
      }
      saveCommandRef.current = record;
      setSaveCommand(record);
      setSaveResult(null);
      setSaveError(null);
      setEditorOpen(false);
      void runSave(record, "save");
    });
  };

  const finishSave = () => {
    if (saveResult?.status !== "rejected" && !canFinishSave) return;
    if (!clearSaveCommand()) {
      setStorageReady(false);
      return;
    }
    saveCommandRef.current = null;
    saveBusyRef.current = false;
    setSaveCommand(null);
    setSaveResult(null);
    setSaveError(null);
    setSaveBusy(false);
    if (canFinishSave) {
      try {
        window.localStorage.removeItem(SAVE_DRAFT_KEY);
      } catch {
        setStorageReady(false);
      }
      setEditorDraft(null);
      void refreshDefinitions(false);
    } else {
      setEditorOpen(true);
    }
  };

  useEffect(() => {
    if (restoredCommand.current) return;
    restoredCommand.current = true;
    const record = archiveCommandRef.current;
    if (record !== null) void runArchive(record, true);
  });

  const submitArchive = async () => {
    if (
      selected === null ||
      selected.archived ||
      runBlocksWrite ||
      hasRun() ||
      !storageReady ||
      !catalogVerified ||
      typeof viewer !== "string" ||
      typeof currentGeneration !== "string" ||
      saveCommandRef.current !== null ||
      archiveCommandRef.current !== null
    )
      return;
    const record: StoredArchive = {
      factorId: selected.factor_id,
      command: {
        generation_id: currentGeneration,
        command_id: crypto.randomUUID(),
        requested_at: new Date().toISOString(),
        expected_head: {
          version: selected.version,
          content_sha256: selected.content_sha256,
        },
      },
    };
    await withFactorCommandLock(() => {
      if (
        hasRun() ||
        hasDefinitionCommand() ||
        saveCommandRef.current !== null ||
        archiveCommandRef.current !== null
      ) {
        run.sync();
        return;
      }
      try {
        const serialized = JSON.stringify(record);
        window.localStorage.setItem(ARCHIVE_STORAGE_KEY, serialized);
        if (window.localStorage.getItem(ARCHIVE_STORAGE_KEY) !== serialized) {
          setStorageReady(false);
          return;
        }
      } catch {
        setStorageReady(false);
        return;
      }
      archiveCommandRef.current = record;
      setArchiveCommand(record);
      setArchiveResult(null);
      setConfirmArchive(false);
      void runArchive(record, false);
    });
  };

  const clearRejectedArchive = () => {
    archiveCommandRef.current = null;
    archiveBusyRef.current = false;
    window.localStorage.removeItem(ARCHIVE_STORAGE_KEY);
    setArchiveCommand(null);
    setArchiveResult(null);
    setArchiveError(null);
    setArchiveBusy(false);
    void refreshDefinitions(false);
  };

  const finishArchive = () => {
    if (!canFinishArchive) return;
    archiveCommandRef.current = null;
    archiveBusyRef.current = false;
    window.localStorage.removeItem(ARCHIVE_STORAGE_KEY);
    setArchiveCommand(null);
    setArchiveResult(null);
    setArchiveError(null);
    setArchiveBusy(false);
  };

  const refreshDefinitions = async (resumeArchive = true) => {
    run.sync();
    const refreshed = await meta.refetch();
    const activeCommand = archiveCommandRef.current;
    if (resumeArchive && activeCommand !== null && !archiveBusyRef.current)
      void runArchive(activeCommand, true);
    if (refreshed.isError || refreshed.data === undefined) return;
    setDeniedViewer(null);
    setPermissionRevision((revision) => revision + 1);
    const nextGeneration = refreshed.data.data.generation?.generation_id ?? null;
    if (nextGeneration !== currentGeneration) {
      setSelection(null);
    }
  };

  useEffect(() => {
    if (run.completed && run.operation !== null && typeof currentGeneration === "string") {
      setSelection({
        generationId: currentGeneration,
        factorId: run.operation.request.parameters.factor_id,
      });
    }
  }, [run.completed, run.operation, currentGeneration]);

  return (
    <>
      <PageHeader
        eyebrow="研究"
        title="因子研究"
        note="查看已发布因子与历史检验"
        actions={
          <>
            {run.availability.data?.enabled && run.storageReady ? (
              <Button
                size="sm"
                variant="primary"
                disabledReason={
                  !run.canStart ? (run.blockedReason ?? "请先完成本次检验。") : undefined
                }
                onClick={run.open}
              >
                运行检验
              </Button>
            ) : null}
            {saveAvailable && !runBlocksWrite && saveCommand === null && archiveCommand === null ? (
              <Button
                size="sm"
                variant="primary"
                onClick={editorDraft === null ? beginCreate : () => setEditorOpen(true)}
              >
                {editorDraft === null ? "新建因子" : "继续编辑草稿"}
              </Button>
            ) : null}
            <Button
              size="sm"
              onClick={() => void refreshDefinitions()}
              disabled={
                currentGeneration === null || currentGeneration === undefined || catalog.isFetching
              }
            >
              刷新
            </Button>
          </>
        }
      />
      <FactorRunStatus run={run} />
      {saveCommand !== null ? (
        <Panel>
          <div
            className="factor-command-state"
            role={saveError || saveResult?.status === "rejected" ? "alert" : "status"}
          >
            <p>
              {saveResult?.status === "rejected"
                ? saveResult.message
                : canFinishSave
                  ? saveResult?.current_head_updated
                    ? "已保存，当前已有新版本。"
                    : "已保存。"
                  : (saveError ??
                    (saveResult?.status === "pending"
                      ? "正在保存，请稍后查看。"
                      : saveResult?.status === "succeeded_waiting_publication" ||
                          saveResult?.status === "published"
                        ? "已提交，等待更新。"
                        : "保存结果尚未确认，请保留这次操作。"))}
            </p>
            {saveResult?.status === "rejected" ? (
              <Button size="sm" onClick={finishSave}>
                修改草稿
              </Button>
            ) : canFinishSave ? (
              <Button size="sm" onClick={finishSave}>
                继续查看因子
              </Button>
            ) : (
              <div className="factor-command-actions">
                <Button
                  size="sm"
                  disabled={saveBusy}
                  onClick={() => void runSave(saveCommand, "resume")}
                >
                  刷新状态
                </Button>
                {saveError !== null || saveResult?.status === "uncertain" ? (
                  <Button
                    size="sm"
                    disabled={saveBusy}
                    onClick={() => void runSave(saveCommand, "retry")}
                  >
                    用原请求重试
                  </Button>
                ) : null}
              </div>
            )}
          </div>
        </Panel>
      ) : null}
      {archiveCommand !== null ? (
        <Panel>
          <div className="factor-command-state" role={archiveError ? "alert" : "status"}>
            <p>{archiveError ?? archiveResult?.message ?? "正在核对归档状态，请稍后查看。"}</p>
            {archiveResult?.status === "rejected" ? (
              <Button size="sm" onClick={clearRejectedArchive}>
                刷新当前版本
              </Button>
            ) : archiveResult?.status === "published" ? (
              <Button size="sm" disabled={!canFinishArchive} onClick={finishArchive}>
                继续查看因子
              </Button>
            ) : (
              <Button
                size="sm"
                disabled={archiveBusy}
                onClick={() => void runArchive(archiveCommand, true)}
              >
                刷新状态
              </Button>
            )}
          </div>
        </Panel>
      ) : null}
      {meta.isError ? (
        <Panel>
          <div className="factor-state" role="alert">
            <p>因子库暂时无法核对，请稍后重试。</p>
            <Button size="sm" onClick={() => void meta.refetch()}>
              重新加载
            </Button>
          </div>
        </Panel>
      ) : meta.data === undefined || (currentGeneration !== null && catalog.isLoading) ? (
        <PageSkeleton label="正在加载因子库" />
      ) : currentGeneration === null ? (
        <Panel>
          <EmptyState title="因子库暂时无法查看" hint="数据恢复后会显示，请稍后刷新。" />
        </Panel>
      ) : catalog.error || changed ? (
        <Panel>
          <div className="factor-state" role="alert">
            <p>{changed ? "数据已更新，请重新查看因子。" : catalog.error?.message}</p>
            <Button size="sm" onClick={() => void refreshDefinitions()}>
              重新加载
            </Button>
          </div>
        </Panel>
      ) : catalog.data?.availability === "unavailable" ? (
        <Panel>
          <EmptyState title="因子库暂时无法查看" hint="数据恢复后会显示，请稍后刷新。" />
        </Panel>
      ) : catalog.data?.availability === "empty" ? (
        <Panel>
          <EmptyState title="还没有因子" hint="保存因子后会显示在这里。" />
        </Panel>
      ) : (
        <div className="factor-layout">
          <div className="factor-sidebar">
            <FactorRunParameters run={run} />
            <Panel
              title="因子列表"
              sub={
                catalog.data?.available_at ? (
                  <span>
                    {rows.length} 个因子 · <RelativeTime at={catalog.data.available_at} />
                    更新
                  </span>
                ) : undefined
              }
              flush
            >
              <DataTable
                rows={rows}
                columns={columns}
                rowKey={(row) => row.factor_id}
                label="因子列表"
                selectedKey={selected?.factor_id}
                onSelect={
                  typeof currentGeneration === "string"
                    ? (row) =>
                        setSelection({ generationId: currentGeneration, factorId: row.factor_id })
                    : undefined
                }
                emptyText="还没有因子"
              />
            </Panel>
          </div>
          {selected ? (
            <div className="factor-main">
              <Panel
                title={selected.name_zh}
                label="因子详情"
                sub={
                  <span>
                    第 {selected.version} 版{selected.archived ? " · 已归档" : ""}
                  </span>
                }
              >
                <div className="factor-detail">
                  <div className="factor-detail-lead">
                    <span className="factor-detail-category">{selected.category_label}</span>
                    <span>{selected.direction_label}</span>
                  </div>
                  <dl className="factor-facts">
                    <div>
                      <dt>记录起日</dt>
                      <dd>
                        {selected.earliest_available_date === null ? (
                          <Tip content="保存公式后，运行检验时核对实际数据起日">待检验</Tip>
                        ) : (
                          selected.earliest_available_date
                        )}
                      </dd>
                    </div>
                    <div>
                      <dt>历史窗口</dt>
                      <dd>{selected.max_history_window} 个交易日</dd>
                    </div>
                    <div>
                      <dt>使用字段</dt>
                      <dd>{selected.dependency_columns.join("、") || "无"}</dd>
                    </div>
                  </dl>
                  <div className="factor-expression">
                    <div className="factor-expression-head">
                      <h3>计算表达式</h3>
                      <Tip content={`因子标识：${selected.factor_id}`}>查看标识</Tip>
                    </div>
                    <code>{selected.expression}</code>
                  </div>
                  {!runBlocksWrite &&
                  ((saveAvailable &&
                    !selected.archived &&
                    saveCommand === null &&
                    archiveCommand === null) ||
                    (catalog.data?.can_archive &&
                      !selected.archived &&
                      archiveCommand === null &&
                      saveCommand === null)) ? (
                    <div className="factor-detail-actions">
                      {saveAvailable &&
                      !selected.archived &&
                      saveCommand === null &&
                      archiveCommand === null ? (
                        <Button size="sm" onClick={beginEdit}>
                          编辑
                        </Button>
                      ) : null}
                      {catalog.data?.can_archive &&
                      !selected.archived &&
                      archiveCommand === null &&
                      saveCommand === null ? (
                        <Button
                          size="sm"
                          disabled={archiveBusy}
                          onClick={() => setConfirmArchive(true)}
                        >
                          归档
                        </Button>
                      ) : null}
                    </div>
                  ) : null}
                </div>
              </Panel>
              {typeof currentGeneration === "string" ? (
                <FactorResults
                  factor={selected}
                  generationId={currentGeneration}
                  preferred={
                    run.preferred?.request.parameters.factor_id === selected.factor_id
                      ? run.preferred
                      : null
                  }
                  onRefresh={() => void refreshDefinitions()}
                />
              ) : null}
            </div>
          ) : null}
        </div>
      )}
      <ConfirmDialog
        open={confirmArchive}
        level="heavy"
        title="归档因子"
        description="归档当前定义，历史记录仍会保留。"
        confirmLabel="确认归档"
        busy={archiveBusy}
        disabled={runBlocksWrite || !catalogVerified || typeof viewer !== "string"}
        onConfirm={() => void submitArchive()}
        onCancel={() => setConfirmArchive(false)}
      />
      <FactorRunConfirmation run={run} />
      <FactorEditor
        draft={editorDraft}
        open={editorOpen}
        capabilities={capabilities.data}
        catalog={catalog.data}
        currentDefinition={draftCurrentRow}
        currentGeneration={currentGeneration}
        canSave={saveAvailable}
        storageReady={storageReady}
        stale={draftStale}
        canRebase={canRebaseDraft}
        busy={saveBusy || saveCommand !== null || runBlocksWrite}
        onChange={updateEditorDraft}
        onClose={() => setEditorOpen(false)}
        onRebase={rebaseDraft}
        onSubmit={() => void submitSave()}
      />
    </>
  );
}
