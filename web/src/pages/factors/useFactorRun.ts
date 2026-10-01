import { useCallback, useEffect, useRef, useState } from "react";
import { ApiError } from "@/api/client";
import {
  type FactorDefinitionItem,
  postFactorRun,
  useFactorResultDetail,
  useFactorResults,
  useFactorRunAvailability,
} from "@/api/factors";
import { useCurrentMeta } from "@/api/useMeta";
import {
  clearRun,
  hasDefinitionCommand,
  hasRun,
  matchesRunResult,
  persistRun,
  persistRunDraft,
  type RunDraft,
  readRun,
  readRunDraft,
  type StoredRun,
  sameRunRequest,
  validRunResult,
  withFactorCommandLock,
} from "./factorRunState";
import { storageWritable } from "./factorSaveState";

const defaults: RunDraft = {
  selection: "all",
  start_date: "",
  end_date: "",
  holding_sessions: 5,
  group_count: 5,
  ic_method: "rank",
  neutralization: "none",
};

export function useFactorRun({
  generationId,
  viewer,
  permissionRevision,
  selected,
  catalogVerified,
  definitionBusy,
}: {
  generationId: string | null | undefined;
  viewer: string | null | undefined;
  permissionRevision: number;
  selected: FactorDefinitionItem | null;
  catalogVerified: boolean;
  definitionBusy: boolean;
}) {
  const meta = useCurrentMeta();
  const availability = useFactorRunAvailability(generationId, viewer, permissionRevision);
  const [operation, setOperation] = useState<StoredRun | null>(readRun);
  const [params, setParams] = useState<RunDraft>(
    () => readRunDraft() ?? (operation === null ? defaults : draftFrom(operation)),
  );
  const [confirmation, setConfirmation] = useState<StoredRun | null>(null);
  const [storageReady, setStorageReady] = useState(storageWritable);
  const [busy, setBusy] = useState(false);
  const [notice, setNotice] = useState<string | null>(null);
  const [deniedActor, setDeniedActor] = useState<string | null>(() =>
    operation?.denied ? operation.viewer : null,
  );
  const [occupied, setOccupied] = useState(() => hasRun() || hasDefinitionCommand());
  const [preferred, setPreferred] = useState<StoredRun | null>(null);
  const operationRef = useRef(operation);
  const busyRef = useRef(false);
  const actorRef = useRef(viewer);
  actorRef.current = viewer;
  const verifiedRef = useRef(false);
  const restored = useRef<string | null>(null);
  const revisionRef = useRef(permissionRevision);
  const availabilityVerified =
    typeof viewer === "string" &&
    typeof generationId === "string" &&
    availability.serving?.generation_id === generationId &&
    availability.serving.state === "ready" &&
    availability.data !== undefined &&
    !availability.error &&
    !availability.isFetching &&
    !meta.isError &&
    !meta.isFetching &&
    meta.data?.serving.state === "ready";
  const denied =
    deniedActor === viewer || (operation?.denied === true && operation.viewer === viewer);
  const canContinue =
    typeof viewer === "string" &&
    operation?.viewer === viewer &&
    !denied &&
    !meta.isError &&
    !meta.isFetching &&
    meta.data !== undefined &&
    !(availability.error instanceof ApiError && [401, 403].includes(availability.error.status)) &&
    !availability.isFetching;
  const selectedPool = availability.data?.pools.find((pool) => pool.selection === params.selection);
  const validDates =
    params.start_date !== "" && params.end_date !== "" && params.start_date <= params.end_date;
  const canStart =
    availabilityVerified &&
    catalogVerified &&
    availability.data?.enabled === true &&
    selected !== null &&
    !selected.archived &&
    !denied &&
    !definitionBusy &&
    !occupied &&
    operation === null &&
    storageReady &&
    navigator.locks !== undefined &&
    selectedPool?.available === true &&
    validDates;
  verifiedRef.current = canStart;
  const currentHeadRef = useRef(selected);
  currentHeadRef.current = selected;
  const generationRef = useRef(generationId);
  generationRef.current = generationId;
  const confirmationCurrent =
    confirmation !== null &&
    selected !== null &&
    canStart &&
    confirmation.viewer === viewer &&
    confirmation.request.serving_generation_id === generationId &&
    confirmation.request.parameters.factor_id === selected?.factor_id &&
    confirmation.request.parameters.expected_head.version === selected.version &&
    confirmation.request.parameters.expected_head.content_sha256 === selected.content_sha256;

  const results = useFactorResults(operation?.result?.job_id ? generationId : null);
  const published =
    results.serving?.generation_id === generationId &&
    results.serving?.state === "ready" &&
    results.data?.availability !== "unavailable" &&
    !results.error &&
    !results.isFetching &&
    typeof generationId === "string" &&
    generationId !== operation?.request.serving_generation_id;
  const matched =
    operation !== null && published
      ? (results.data?.results.find((item) => matchesRunResult(item, operation)) ?? null)
      : null;
  const detail = useFactorResultDetail(
    generationId,
    matched?.status === "succeeded" && matched.display_status === "available"
      ? matched.job_id
      : null,
  );
  const completed =
    operation !== null &&
    operation.viewer === viewer &&
    matched?.status === "succeeded" &&
    matched.display_status === "available" &&
    detail.serving?.generation_id === generationId &&
    detail.serving?.state === "ready" &&
    !detail.error &&
    !detail.isFetching &&
    detail.data?.availability === "ready" &&
    detail.data.research != null &&
    matchesRunResult(detail.data.result, operation) &&
    detail.data.result?.status === "succeeded" &&
    detail.data.result.display_status === "available" &&
    detail.data.result.updated_at === matched.updated_at &&
    detail.data.result.as_of_time === matched.as_of_time;
  const failed = operation?.viewer === viewer && matched?.status === "failed";

  useEffect(() => {
    const start = availability.data?.start_date;
    const end = availability.data?.end_date;
    if (!availabilityVerified || params.start_date !== "" || !start || !end) return;
    const initial = { ...params, start_date: start, end_date: end };
    setParams(initial);
    if (!persistRunDraft(initial)) setStorageReady(false);
  }, [availabilityVerified, availability.data, params]);

  const sync = useCallback(() => {
    const next = readRun();
    operationRef.current = next;
    setOperation(next);
    setOccupied(hasRun() || hasDefinitionCommand());
  }, []);

  useEffect(() => {
    const receive = (event: StorageEvent) => {
      if (event.key === null || event.key.startsWith("rquant.factor.")) sync();
    };
    const focus = () => sync();
    window.addEventListener("storage", receive);
    window.addEventListener("focus", focus);
    return () => {
      window.removeEventListener("storage", receive);
      window.removeEventListener("focus", focus);
    };
  }, [sync]);

  useEffect(() => {
    setOccupied(definitionBusy || hasRun() || hasDefinitionCommand());
  }, [definitionBusy]);

  useEffect(() => {
    if (revisionRef.current === permissionRevision) return;
    revisionRef.current = permissionRevision;
    setDeniedActor(null);
    const current = operationRef.current;
    if (current !== null && current.viewer === viewer && current.denied) {
      void withFactorCommandLock(() => {
        const stored = readRun();
        if (
          stored === null ||
          stored.viewer !== current.viewer ||
          !sameRunRequest(stored.request, current.request)
        )
          return;
        const next = { ...stored, denied: false };
        if (!persistRun(next)) {
          setStorageReady(false);
          return;
        }
        operationRef.current = next;
        setOperation(next);
      }, true);
    }
  }, [permissionRevision, viewer]);

  const currentOperation = (record: StoredRun) => {
    const current = readRun();
    return (
      actorRef.current === record.viewer &&
      current?.viewer === record.viewer &&
      operationRef.current?.viewer === record.viewer &&
      sameRunRequest(current.request, record.request) &&
      sameRunRequest(operationRef.current.request, record.request)
    );
  };

  const execute = async (
    record: StoredRun,
    action: "run" | "resume" | "retry",
    alreadyLocked = false,
  ) => {
    const send = async () => {
      if (!currentOperation(record)) return;
      const latest = readRun();
      if (
        latest?.denied ||
        latest?.result?.status === "rejected" ||
        (action === "retry" && latest?.result?.status === "submitted")
      )
        return;
      setBusy(true);
      setNotice(null);
      try {
        const response = await postFactorRun(record.request, action);
        if (!currentOperation(record)) return;
        const previous = readRun() ?? record;
        // A receipt must bind every original field, and cannot replace a known job.
        if (
          !validRunResult(response, record.request) ||
          (previous.result?.job_id != null &&
            (response.job_id !== previous.result.job_id ||
              response.spec_sha256 !== previous.result.spec_sha256))
        ) {
          setNotice("检验结果暂未确认，请保留本次操作。");
          return;
        }
        const next: StoredRun = { ...previous, result: response };
        if (!persistRun(next)) {
          setStorageReady(false);
          return;
        }
        operationRef.current = next;
        setOperation(next);
        if (response.status === "submitted") void meta.refetch();
      } catch (error) {
        if (!currentOperation(record)) return;
        const previous = readRun() ?? record;
        const permissionDenied = error instanceof ApiError && [401, 403].includes(error.status);
        if (permissionDenied) setDeniedActor(record.viewer);
        const next: StoredRun = {
          ...previous,
          denied: previous.denied || permissionDenied,
          result: previous.result?.job_id
            ? previous.result
            : {
                original_request: record.request,
                status: "uncertain",
                reason: null,
                job_id: null,
                spec_sha256: null,
              },
        };
        if (!persistRun(next)) setStorageReady(false);
        operationRef.current = next;
        setOperation(next);
        setNotice("检验结果暂未确认，请保留本次操作。");
        if (permissionDenied) void meta.refetch();
      } finally {
        setBusy(false);
      }
    };
    if (alreadyLocked) await send();
    else if (!(await withFactorCommandLock(send, true))) {
      setNotice("其他页面正在处理因子操作，请稍后刷新。");
      sync();
    }
  };

  const continueRun = async (action: "resume" | "retry") => {
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
    void continueRun("resume");
  });

  const update = (value: RunDraft) => {
    setParams(value);
    if (!persistRunDraft(value)) setStorageReady(false);
  };

  const open = () => {
    sync();
    if (
      !canStart ||
      typeof viewer !== "string" ||
      typeof generationId !== "string" ||
      selected === null ||
      selectedPool === undefined ||
      hasRun() ||
      hasDefinitionCommand()
    )
      return;
    setConfirmation({
      viewer,
      factorName: selected.name_zh,
      poolLabel: selectedPool.label,
      result: null,
      denied: false,
      request: {
        command_id: crypto.randomUUID(),
        // Match the HTTP datetime serializer before capturing the immutable request.
        requested_at: new Date()
          .toISOString()
          .replace(/\.000Z$/, "Z")
          .replace(/\.(\d{3})Z$/, (_match, digits: string) => `.${digits}000Z`),
        serving_generation_id: generationId,
        parameters: {
          ...params,
          factor_id: selected.factor_id,
          expected_head: { version: selected.version, content_sha256: selected.content_sha256 },
        },
      },
    });
  };

  const submit = async () => {
    if (confirmation === null || !confirmationCurrent || busyRef.current) return;
    busyRef.current = true;
    try {
      const acquired = await withFactorCommandLock(async () => {
        const head = currentHeadRef.current;
        if (
          !verifiedRef.current ||
          actorRef.current !== confirmation.viewer ||
          generationRef.current !== confirmation.request.serving_generation_id ||
          head?.factor_id !== confirmation.request.parameters.factor_id ||
          head.version !== confirmation.request.parameters.expected_head.version ||
          head.content_sha256 !== confirmation.request.parameters.expected_head.content_sha256 ||
          hasRun() ||
          hasDefinitionCommand()
        ) {
          sync();
          return;
        }
        if (!persistRunDraft(draftFrom(confirmation)) || !persistRun(confirmation)) {
          setStorageReady(false);
          return;
        }
        restored.current = confirmation.request.command_id;
        operationRef.current = confirmation;
        setOperation(confirmation);
        setOccupied(true);
        setConfirmation(null);
        await execute(confirmation, "run", true);
      }, true);
      if (!acquired) {
        setNotice("其他页面正在处理因子操作，请稍后刷新。");
        sync();
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
      (record.result?.status !== "rejected" && !completed && !failed)
    )
      return;
    await withFactorCommandLock(() => {
      if (!currentOperation(record)) return;
      if (!persistRunDraft(draftFrom(record)) || !clearRun(record)) {
        setStorageReady(false);
        return;
      }
      setParams(draftFrom(record));
      if (completed) setPreferred(record);
      operationRef.current = null;
      setOperation(null);
      setOccupied(hasDefinitionCommand());
      setNotice(null);
    }, true);
  };

  const refresh = async () => {
    await continueRun("resume");
    await meta.refetch();
    if (operation?.result?.job_id && typeof generationId === "string") results.refetch();
    if (matched?.status === "succeeded" && matched.display_status === "available") detail.refetch();
  };

  const blockedReason = !storageReady
    ? "无法保留本次操作，请检查浏览器存储后重新加载。"
    : operation === null && hasRun()
      ? "本机有未能恢复的检验操作，请保留浏览器记录后重新加载。"
      : operation !== null && operation.viewer !== viewer
        ? "请切回提交本次检验的账号继续查看。"
        : denied
          ? "当前账号不能运行检验，请刷新后重新核对权限。"
          : navigator.locks === undefined
            ? "当前浏览器无法安全保留并协调检验操作，请换用较新的浏览器。"
            : typeof viewer !== "string"
              ? "登录后可运行检验。"
              : (availability.error?.message ??
                (!availabilityVerified
                  ? "正在核对检验条件…"
                  : availability.data?.enabled === false
                    ? (availability.data.reason ?? "暂时无法运行检验。")
                    : definitionBusy || hasDefinitionCommand()
                      ? "请先完成保存或归档操作。"
                      : selected?.archived
                        ? "归档因子仅可查看历史检验。"
                        : selected === null
                          ? "选择一个因子后运行检验。"
                          : selectedPool?.available === false
                            ? (selectedPool.reason ?? "这个股票池暂不可用。")
                            : !validDates
                              ? "请选择有效的起止日期。"
                              : null));
  const status = completed
    ? "检验完成。"
    : failed
      ? (matched?.failure_message ?? "本次检验未完成。")
      : (notice ??
        (operation?.result?.status === "rejected"
          ? (operation.result.reason ?? "本次检验未被接受。")
          : operation?.result?.status === "submitted"
            ? "已提交，等待更新。"
            : operation?.result?.status === "processing" || matched?.status === "running"
              ? "检验中。"
              : operation?.result?.status === "pending" || busy
                ? "等待检验。"
                : "检验结果暂未确认，请保留本次操作。"));
  return {
    availability,
    params,
    update,
    operation,
    occupied: occupied || operation !== null,
    busy,
    storageReady,
    permissionDenied: denied,
    sameActor: operation?.viewer === viewer,
    canStart,
    canContinue,
    blockedReason,
    status,
    completed,
    failed,
    preferred: operation ?? preferred,
    confirmation,
    confirmationCurrent,
    open,
    submit,
    cancel: () => setConfirmation(null),
    continueRun,
    refresh,
    finish,
    sync,
  };
}

function draftFrom(operation: StoredRun): RunDraft {
  const p = operation.request.parameters;
  return {
    selection: p.selection,
    start_date: p.start_date,
    end_date: p.end_date,
    holding_sessions: p.holding_sessions,
    group_count: p.group_count,
    ic_method: p.ic_method,
    neutralization: p.neutralization,
  };
}
