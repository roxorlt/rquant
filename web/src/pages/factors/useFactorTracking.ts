import { useCallback, useEffect, useRef, useState } from "react";
import { ApiError } from "@/api/client";
import {
  type FactorDefinitionItem,
  postFactorTracking,
  useFactorCapabilities,
  useFactorTrackingPanel,
} from "@/api/factors";
import { useCurrentMeta } from "@/api/useMeta";
import { hasDefinitionCommand, hasRun, hasTracking, withFactorCommandLock } from "./factorRunState";
import { storageWritable } from "./factorSaveState";
import {
  clearTracking,
  matchesTrackingPanel,
  persistTracking,
  readTracking,
  type StoredTracking,
  sameTrackingHead,
  sameTrackingReceipt,
  sameTrackingRequest,
  validTrackingPanel,
  validTrackingResult,
} from "./factorTrackingState";

const uncertain = "跟踪状态暂未确认，请保留本次操作。";
const invalidPanel = "跟踪数据暂时无法核对，请刷新后查看。";

export function useFactorTracking({
  generationId,
  viewer,
  permissionRevision,
  selected,
  catalogVerified,
  otherBusy,
  onPermissionDenied,
}: {
  generationId: string | null | undefined;
  viewer: string | null | undefined;
  permissionRevision: number;
  selected: FactorDefinitionItem | null;
  catalogVerified: boolean;
  otherBusy: boolean;
  onPermissionDenied: (viewer: string) => void;
}) {
  const meta = useCurrentMeta();
  const [operation, setOperation] = useState<StoredTracking | null>(readTracking);
  const [occupied, setOccupied] = useState(hasTracking);
  const [storageReady, setStorageReady] = useState(storageWritable);
  const [confirmation, setConfirmation] = useState<StoredTracking | null>(null);
  const [busy, setBusy] = useState(false);
  const [notice, setNotice] = useState<string | null>(null);
  const [deniedActor, setDeniedActor] = useState<string | null>(null);
  const operationRef = useRef(operation);
  const busyRef = useRef(false);
  const actorRef = useRef(viewer);
  actorRef.current = viewer;
  const restored = useRef<string | null>(null);
  const revisionRef = useRef(permissionRevision);
  const panelQuery = useFactorTrackingPanel(
    generationId,
    selected?.factor_id ?? null,
    viewer,
    permissionRevision,
  );
  const originalQuery = useFactorTrackingPanel(
    generationId,
    operation?.request.factor_id ?? null,
    viewer,
    permissionRevision,
  );
  const capabilities = useFactorCapabilities(
    generationId,
    catalogVerified && panelQuery.data?.can_set_tracked === true,
    viewer,
    permissionRevision,
  );
  const deniedQuery = [panelQuery, originalQuery].some(
    (query) => query.error instanceof ApiError && [401, 403].includes(query.error.status),
  );
  const permissionDenied =
    deniedActor === viewer ||
    deniedQuery ||
    (operation !== null && operation.viewer === viewer && operation.denied);
  const metaVerified =
    typeof viewer === "string" &&
    !meta.isError &&
    !meta.isFetching &&
    meta.data?.data.viewer === viewer;
  const panelValid =
    selected !== null &&
    validTrackingPanel(panelQuery.data, selected.factor_id) &&
    (panelQuery.data.status === "paused" ||
      panelQuery.data.availability !== "tracked" ||
      sameTrackingHead(panelQuery.data.definition_head, {
        version: selected.version,
        content_sha256: selected.content_sha256,
      }));
  const panelVerified =
    panelValid &&
    metaVerified &&
    catalogVerified &&
    typeof generationId === "string" &&
    panelQuery.serving?.generation_id === generationId &&
    panelQuery.serving.state === "ready" &&
    !panelQuery.error &&
    !panelQuery.isFetching;
  const panel = panelVerified ? (panelQuery.data ?? null) : null;
  const originalVerified =
    operation !== null &&
    metaVerified &&
    typeof generationId === "string" &&
    originalQuery.serving?.generation_id === generationId &&
    originalQuery.serving.state === "ready" &&
    !originalQuery.error &&
    !originalQuery.isFetching &&
    validTrackingPanel(originalQuery.data, operation.request.factor_id);
  const completed =
    operation?.viewer === viewer &&
    originalVerified &&
    originalQuery.data !== undefined &&
    operation !== null &&
    matchesTrackingPanel(originalQuery.data, operation);
  const sameActor = operation?.viewer === viewer;
  const canContinue =
    operation !== null &&
    sameActor &&
    metaVerified &&
    !permissionDenied &&
    storageReady &&
    navigator.locks !== undefined &&
    !hasRun() &&
    !hasDefinitionCommand() &&
    !otherBusy;
  const canStart =
    panelVerified &&
    panel?.can_set_tracked === true &&
    panel.availability !== "unavailable" &&
    !permissionDenied &&
    operation === null &&
    !occupied &&
    !hasTracking() &&
    !otherBusy &&
    !hasRun() &&
    !hasDefinitionCommand() &&
    storageReady &&
    navigator.locks !== undefined;
  const capabilitiesVerified =
    metaVerified &&
    catalogVerified &&
    capabilities.serving?.generation_id === generationId &&
    capabilities.serving?.state === "ready" &&
    capabilities.data !== undefined &&
    !capabilities.error &&
    !capabilities.isFetching;
  const unsupported = selected?.dependency_columns.map((column) =>
    capabilities.data?.fields.find((field) => field.column === column),
  );
  const joinBlockedReason = !capabilitiesVerified
    ? "字段能力暂时无法核对，请刷新后再加入跟踪。"
    : unsupported?.some((field) => field === undefined)
      ? "当前数据缺少该因子需要的字段，请核对后再加入跟踪。"
      : (unsupported?.find((field) => field?.tracking_supported === false)
          ?.tracking_unavailable_reason_zh ??
        (unsupported?.some((field) => field?.tracking_supported === false)
          ? "该因子的字段暂不支持持续跟踪。"
          : null));
  const canJoin = canStart && selected !== null && !selected.archived && joinBlockedReason === null;
  const canCancel = canStart && panel?.tracked === true;
  const confirmationCurrent =
    confirmation !== null &&
    canJoin &&
    selected !== null &&
    confirmation.viewer === viewer &&
    confirmation.request.factor_id === selected.factor_id &&
    confirmation.request.serving_generation_id === generationId &&
    sameTrackingHead(confirmation.request.expected_head, {
      version: selected.version,
      content_sha256: selected.content_sha256,
    }) &&
    confirmation.request.expected_tracking_generation === (panel?.tracking_generation ?? null);
  const live = useRef({
    canJoin,
    canCancel,
    confirmationCurrent,
    confirmation,
    selected,
    panel,
    generationId,
  });
  live.current = {
    canJoin,
    canCancel,
    confirmationCurrent,
    confirmation,
    selected,
    panel,
    generationId,
  };
  const completedRef = useRef(completed);
  completedRef.current = completed;

  const sync = useCallback(() => {
    const next = readTracking();
    operationRef.current = next;
    setOperation(next);
    setOccupied(hasTracking());
    setNotice(null);
  }, []);
  useEffect(() => {
    const receive = (event: StorageEvent) => {
      if (event.key === null || event.key.startsWith("rquant.factor.")) sync();
    };
    window.addEventListener("storage", receive);
    window.addEventListener("focus", sync);
    return () => {
      window.removeEventListener("storage", receive);
      window.removeEventListener("focus", sync);
    };
  }, [sync]);
  useEffect(() => {
    if (!deniedQuery || typeof viewer !== "string") return;
    setDeniedActor(viewer);
    onPermissionDenied(viewer);
  }, [deniedQuery, viewer, onPermissionDenied]);
  useEffect(() => {
    if (revisionRef.current === permissionRevision) return;
    revisionRef.current = permissionRevision;
    setDeniedActor(null);
    const current = operationRef.current;
    if (current === null || current.viewer !== viewer || !current.denied) return;
    void withFactorCommandLock(() => {
      const saved = readTracking();
      if (
        actorRef.current !== current.viewer ||
        saved?.viewer !== current.viewer ||
        !sameTrackingRequest(saved.request, current.request)
      )
        return;
      const next = { ...saved, denied: false };
      if (!persistTracking(next)) {
        setStorageReady(false);
        return;
      }
      operationRef.current = next;
      setOperation(next);
    }, true);
  }, [permissionRevision, viewer]);

  const currentOperation = (record: StoredTracking) => {
    const saved = readTracking();
    const current = operationRef.current;
    return (
      actorRef.current === record.viewer &&
      saved?.viewer === record.viewer &&
      current?.viewer === record.viewer &&
      sameTrackingRequest(saved.request, record.request) &&
      sameTrackingRequest(current.request, record.request)
    );
  };
  const execute = async (
    record: StoredTracking,
    action: "set" | "resume" | "retry",
    locked = false,
  ) => {
    const send = async () => {
      if (!currentOperation(record) || hasRun() || hasDefinitionCommand()) return;
      const previous = readTracking();
      if (previous?.denied || previous?.result?.status === "rejected") return;
      setBusy(true);
      setNotice(null);
      try {
        const response = await postFactorTracking(record.request, action);
        if (!currentOperation(record)) return;
        const latest = readTracking() ?? record;
        if (
          !validTrackingResult(response, record.request) ||
          (latest.result?.status === "rejected" && response.status !== "rejected") ||
          (latest.result?.receipt != null &&
            (response.receipt == null ||
              !sameTrackingReceipt(latest.result.receipt, response.receipt)))
        ) {
          setNotice(uncertain);
          return;
        }
        const next = { ...latest, result: response };
        if (!persistTracking(next)) {
          setStorageReady(false);
          return;
        }
        operationRef.current = next;
        setOperation(next);
        if (response.status === "applied") {
          void meta.refetch();
          if (operation?.request.factor_id === record.request.factor_id) originalQuery.refetch();
          if (selected !== null) panelQuery.refetch();
        }
      } catch (error) {
        if (!currentOperation(record)) return;
        const latest = readTracking() ?? record;
        const denied = error instanceof ApiError && [401, 403].includes(error.status);
        if (denied) {
          setDeniedActor(record.viewer);
          onPermissionDenied(record.viewer);
        }
        const next: StoredTracking = {
          ...latest,
          denied: latest.denied || denied,
          result: latest.result?.receipt
            ? latest.result
            : {
                original_request: record.request,
                status: "uncertain",
                receipt: null,
                reason: null,
              },
        };
        if (!persistTracking(next)) setStorageReady(false);
        operationRef.current = next;
        setOperation(next);
        setNotice(uncertain);
        if (denied) void meta.refetch();
      } finally {
        setBusy(false);
      }
    };
    if (locked) await send();
    else if (!(await withFactorCommandLock(send, true))) {
      sync();
      setNotice("其他页面正在处理因子操作，请稍后刷新。");
    }
  };
  const continueTracking = async (action: "resume" | "retry") => {
    const record = operationRef.current;
    if (record === null || busyRef.current || !canContinue) return;
    busyRef.current = true;
    try {
      await execute(record, action);
    } finally {
      busyRef.current = false;
    }
  };
  useEffect(() => {
    if (
      !canContinue ||
      operation === null ||
      operation.result?.status === "rejected" ||
      busyRef.current ||
      restored.current === operation.request.command_id
    )
      return;
    restored.current = operation.request.command_id;
    void continueTracking("resume");
  });

  const capture = (tracked: boolean): StoredTracking | null => {
    if (
      typeof viewer !== "string" ||
      typeof generationId !== "string" ||
      selected === null ||
      panel === null
    )
      return null;
    return {
      viewer,
      factorName: selected.name_zh,
      result: null,
      denied: false,
      request: {
        command_id: crypto.randomUUID(),
        requested_at: new Date()
          .toISOString()
          .replace(/\.000Z$/, "Z")
          .replace(/\.(\d{3})Z$/, (_match, digits: string) => `.${digits}000Z`),
        serving_generation_id: generationId,
        factor_id: selected.factor_id,
        tracked,
        expected_head: { version: selected.version, content_sha256: selected.content_sha256 },
        expected_tracking_generation: panel.tracking_generation ?? null,
      },
    };
  };
  const open = () => {
    if (!canJoin || hasTracking() || hasRun() || hasDefinitionCommand()) return;
    setConfirmation(capture(true));
  };
  const submit = async (cancel = false) => {
    const record = cancel ? (canCancel ? capture(false) : null) : confirmation;
    if (record === null || busyRef.current || (!cancel && !confirmationCurrent)) return;
    busyRef.current = true;
    try {
      const acquired = await withFactorCommandLock(async () => {
        const current = live.current;
        if (
          (cancel
            ? !current.canCancel
            : !current.canJoin ||
              !current.confirmationCurrent ||
              current.confirmation?.request.command_id !== record.request.command_id) ||
          actorRef.current !== record.viewer ||
          current.generationId !== record.request.serving_generation_id ||
          current.selected?.factor_id !== record.request.factor_id ||
          !sameTrackingHead(
            record.request.expected_head,
            current.selected === null
              ? null
              : {
                  version: current.selected.version,
                  content_sha256: current.selected.content_sha256,
                },
          ) ||
          record.request.expected_tracking_generation !==
            (current.panel?.tracking_generation ?? null) ||
          hasTracking() ||
          hasRun() ||
          hasDefinitionCommand()
        ) {
          sync();
          return;
        }
        if (!persistTracking(record)) {
          setStorageReady(false);
          return;
        }
        restored.current = record.request.command_id;
        operationRef.current = record;
        setOperation(record);
        setOccupied(true);
        setConfirmation(null);
        await execute(record, "set", true);
      }, true);
      if (!acquired) {
        sync();
        setNotice("其他页面正在处理因子操作，请稍后刷新。");
      }
    } finally {
      busyRef.current = false;
    }
  };
  const finish = async () => {
    const record = operationRef.current;
    if (
      record === null ||
      busyRef.current ||
      record.viewer !== viewer ||
      (record.result?.status !== "rejected" && !completed)
    )
      return;
    await withFactorCommandLock(() => {
      if (!currentOperation(record)) return;
      const latest = readTracking();
      if (latest === null) return;
      if (
        latest.result?.status !== "rejected" &&
        (!completedRef.current ||
          latest.result?.receipt == null ||
          record.result?.receipt == null ||
          !sameTrackingReceipt(latest.result.receipt, record.result.receipt))
      ) {
        sync();
        return;
      }
      if (!clearTracking(latest)) {
        setStorageReady(false);
        return;
      }
      operationRef.current = null;
      setOperation(null);
      setOccupied(false);
      setNotice(null);
    }, true);
  };
  const refresh = async () => {
    await continueTracking("resume");
    await meta.refetch();
    panelQuery.refetch();
    originalQuery.refetch();
  };
  const blockedReason = !storageReady
    ? "无法保留本次操作，请检查浏览器存储后重新加载。"
    : operation === null && occupied
      ? "本机有未能恢复的跟踪操作，请保留浏览器记录后重新加载。"
      : operation !== null && !sameActor
        ? "请切回提交本次跟踪的账号继续查看。"
        : permissionDenied
          ? "当前账号不能修改跟踪，请刷新后核对权限。"
          : navigator.locks === undefined
            ? "当前浏览器无法协调因子操作，请换用较新的浏览器。"
            : typeof viewer !== "string"
              ? "登录后可加入跟踪。"
              : otherBusy || hasRun() || hasDefinitionCommand()
                ? "请先完成其他因子操作。"
                : selected === null
                  ? "选择一个已保存因子后加入跟踪。"
                  : ((panelQuery.error
                      ? panelQuery.error instanceof ApiError
                        ? panelQuery.error.message
                        : invalidPanel
                      : null) ??
                    (panelQuery.isLoading || panelQuery.isFetching || !catalogVerified
                      ? "正在核对跟踪条件…"
                      : !panelVerified
                        ? invalidPanel
                        : panel?.availability === "unavailable"
                          ? (panel.reason ?? "跟踪数据尚未发布。")
                          : panel?.can_set_tracked !== true
                            ? "当前账号暂时不能修改跟踪。"
                            : selected.archived
                              ? "已归档因子不能重新加入跟踪。"
                              : null));
  const status = completed
    ? operation?.request.tracked
      ? "已加入跟踪。"
      : "已取消跟踪。"
    : (notice ??
      (operation?.result?.status === "rejected"
        ? (operation.result.reason ?? "本次跟踪未被接受。")
        : operation?.result?.status === "applied"
          ? "已保存，等待同步。"
          : operation?.result?.status === "pending" || busy
            ? "正在核对跟踪状态。"
            : uncertain));
  const awaitingSync =
    operation !== null &&
    operation.request.factor_id === selected?.factor_id &&
    operation.result?.status !== "rejected" &&
    !completed &&
    panelVerified &&
    panel?.availability !== "unavailable";
  return {
    selected,
    panel: awaitingSync ? null : panel,
    panelNotice: awaitingSync ? "本次跟踪尚未同步，请刷新状态。" : null,
    panelQuery,
    panelVerified,
    operation,
    occupied: occupied || operation !== null,
    storageReady,
    busy,
    permissionDenied,
    sameActor,
    canContinue,
    canJoin,
    canCancel,
    joinBlockedReason,
    blockedReason,
    status,
    completed,
    confirmation,
    confirmationCurrent,
    open,
    submit,
    continueTracking,
    refresh,
    finish,
    cancelConfirmation: () => setConfirmation(null),
    sync,
  };
}
