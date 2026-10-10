import { useEffect, useRef, useState } from "react";
import {
  type AIGenerateRequest,
  type AIRequestView,
  generateAI,
  lookupAI,
} from "@/api/aiAssistance";
import { ApiError } from "@/api/client";

const PREFIX = "rquant.ai.original.v1:";
const key = (viewer: string, slot: string) =>
  `${PREFIX}${encodeURIComponent(viewer)}:${encodeURIComponent(slot)}`;
export function readOriginal(viewer: string, slot: string): AIGenerateRequest | null {
  try {
    const raw = sessionStorage.getItem(key(viewer, slot));
    if (!raw || new TextEncoder().encode(raw).length > 65536) return null;
    const body: unknown = JSON.parse(raw);
    if (
      typeof body !== "object" ||
      body === null ||
      !("purpose" in body) ||
      !("request_id" in body) ||
      typeof body.request_id !== "string" ||
      !/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/.test(body.request_id) ||
      !["screen", "pool_edit", "interpretation", "news_digest"].includes(String(body.purpose))
    )
      return null;
    return Object.freeze(body) as AIGenerateRequest;
  } catch {
    return null;
  }
}
export function saveOriginal(viewer: string, slot: string, body: AIGenerateRequest): void {
  const old = readOriginal(viewer, slot);
  const raw = JSON.stringify(body);
  if (new TextEncoder().encode(raw).length > 65536) throw new Error("描述过长，请删减后再试。");
  if (old && JSON.stringify(old) !== raw) throw new Error("请先查看原请求，再新建建议。");
  sessionStorage.setItem(key(viewer, slot), raw);
}
export function forgetOriginal(viewer: string, slot: string): void {
  sessionStorage.removeItem(key(viewer, slot));
}

export function useAiGeneration(viewer: string | null, slot: string) {
  const scope = `${viewer}:${slot}`;
  const current = useRef(scope);
  current.current = scope;
  const controller = useRef<AbortController | null>(null);
  const [state, setState] = useState<{
    scope: string;
    original: AIGenerateRequest | null;
    view: AIRequestView | null;
    busy: boolean;
    error: string | null;
    errorStatus: number | null;
    absent: boolean;
  }>(() => ({
    scope,
    original: viewer ? readOriginal(viewer, slot) : null,
    view: null,
    busy: false,
    error: null,
    errorStatus: null,
    absent: false,
  }));
  useEffect(() => {
    controller.current?.abort();
    setState({
      scope,
      original: viewer ? readOriginal(viewer, slot) : null,
      view: null,
      busy: false,
      error: null,
      errorStatus: null,
      absent: false,
    });
    return () => controller.current?.abort();
  }, [scope, viewer, slot]);
  const visible =
    state.scope === scope
      ? state
      : {
          scope,
          original: null,
          view: null,
          busy: false,
          error: null,
          errorStatus: null,
          absent: false,
        };
  async function run(body: AIGenerateRequest, lookup: boolean) {
    if (!viewer || visible.busy) return;
    let request: AIGenerateRequest;
    try {
      saveOriginal(viewer, slot, body);
      request = readOriginal(viewer, slot) ?? body;
    } catch {
      setState({
        scope,
        original: visible.original,
        view: visible.view,
        busy: false,
        error: "无法保存原请求，请检查浏览器存储。",
        errorStatus: null,
        absent: false,
      });
      return;
    }
    controller.current?.abort();
    const abort = new AbortController();
    controller.current = abort;
    setState({
      scope,
      original: request,
      view: visible.view,
      busy: true,
      error: null,
      errorStatus: null,
      absent: false,
    });
    try {
      const view = await (lookup
        ? lookupAI(request, abort.signal)
        : generateAI(request, abort.signal));
      if (current.current !== scope || abort.signal.aborted) return;
      if (view.request_id !== request.request_id || view.purpose !== request.purpose)
        throw new Error("原请求回执不匹配，请继续查看。");
      setState({
        scope,
        original: request,
        view,
        busy: false,
        error: view.message ?? null,
        errorStatus: null,
        absent: view.state === "not_dispatched",
      });
    } catch (error) {
      if (current.current !== scope || abort.signal.aborted) return;
      setState({
        scope,
        original: request,
        view: null,
        busy: false,
        error:
          error instanceof ApiError && error.status === 404
            ? "暂未查到原请求，请继续原请求。"
            : error instanceof ApiError
              ? error.message
              : "调用结果未知，请继续查看原请求。",
        errorStatus: error instanceof ApiError ? error.status : null,
        absent: false,
      });
    }
  }
  return {
    ...visible,
    generate: (body: AIGenerateRequest) => run(body, false),
    lookup: () => (visible.original ? run(visible.original, true) : Promise.resolve()),
    reset: () => {
      if (
        !viewer ||
        visible.busy ||
        (visible.original && !visible.absent && visible.view?.state !== "completed")
      )
        return;
      forgetOriginal(viewer, slot);
      setState({
        scope,
        original: null,
        view: null,
        busy: false,
        error: null,
        errorStatus: null,
        absent: false,
      });
    },
  };
}
