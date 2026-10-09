import { useEffect, useRef, useState } from "react";
import { ApiError } from "@/api/client";
import {
  type PromotionCommand,
  type PromotionMode,
  type PromotionResult,
  postPromotionCommand,
  usePromotionCacheIdentity,
} from "@/api/strategyPromotion";

export const PROMOTION_PENDING_KEY = "rquant.strategy-promotion.pending.v1";
const CHANGED = "rquant-strategy-promotion-original-changed";
function readOriginal(viewer: string | null | undefined): PromotionCommand | null {
  if (viewer === undefined) return null;
  try {
    if (viewer === null) {
      sessionStorage.removeItem(PROMOTION_PENDING_KEY);
      return null;
    }
    const text = sessionStorage.getItem(PROMOTION_PENDING_KEY);
    if (!text || new TextEncoder().encode(text).length > 32768 + 512) return null;
    const value: unknown = JSON.parse(text);
    if (typeof value !== "object" || value === null || !("viewer" in value) || !("body" in value))
      return null;
    if (value.viewer !== viewer) {
      sessionStorage.removeItem(PROMOTION_PENDING_KEY);
      return null;
    }
    const body = value.body;
    if (new TextEncoder().encode(JSON.stringify(body)).length > 32768) return null;
    if (
      typeof body !== "object" ||
      body === null ||
      !("kind" in body) ||
      !("command_id" in body) ||
      !("requested_at" in body) ||
      !("generation_id" in body) ||
      !("target" in body)
    )
      return null;
    if (
      ![
        "request_promotion_review",
        "prepare_promotion_approval",
        "approve_promotion",
        "run_strategy_walk_forward",
      ].includes(String(body.kind)) ||
      typeof body.command_id !== "string" ||
      !/^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$/.test(body.command_id) ||
      typeof body.requested_at !== "string" ||
      !Number.isFinite(Date.parse(body.requested_at)) ||
      typeof body.generation_id !== "string" ||
      typeof body.target !== "object" ||
      body.target === null ||
      !("owner_id" in body.target) ||
      body.target.owner_id !== viewer
    )
      return null;
    return body as PromotionCommand;
  } catch {
    return null;
  }
}
function storeOriginal(viewer: string, body: PromotionCommand | null): void {
  try {
    if (body) sessionStorage.setItem(PROMOTION_PENDING_KEY, JSON.stringify({ viewer, body }));
    else sessionStorage.removeItem(PROMOTION_PENDING_KEY);
  } catch {
    /* The same in-memory body remains available when storage is blocked. */
  }
  window.dispatchEvent(new Event(CHANGED));
}
export function usePromotionPrivateCleanup(
  viewer: string | null | undefined,
  roleHash?: string | null,
): void {
  usePromotionCacheIdentity(viewer ?? null, roleHash);
  useEffect(() => {
    readOriginal(viewer);
  }, [viewer]);
}
export function usePromotionCommands(viewer: string, identity: string) {
  const [pending, setPending] = useState<PromotionCommand | null>(() => readOriginal(viewer));
  const [result, setResult] = useState<{ identity: string; value: PromotionResult } | null>(null);
  const [error, setError] = useState<{ identity: string; message: string } | null>(null);
  const [busy, setBusy] = useState(false);
  const active = useRef(true);
  const current = useRef(identity);
  const inFlight = useRef(false);
  const original = useRef(pending);
  current.current = identity;
  useEffect(() => {
    active.current = true;
    return () => {
      active.current = false;
    };
  }, []);
  useEffect(() => {
    const stored = readOriginal(viewer);
    original.current = stored;
    setPending(stored);
    setResult((previous) => (previous?.identity === identity ? previous : null));
    setError((previous) => (previous?.identity === identity ? previous : null));
    setBusy(false);
    function changed() {
      original.current = readOriginal(viewer);
      setPending(original.current);
    }
    window.addEventListener(CHANGED, changed);
    window.addEventListener("storage", changed);
    return () => {
      window.removeEventListener(CHANGED, changed);
      window.removeEventListener("storage", changed);
    };
  }, [viewer, identity]);
  async function send(
    body: PromotionCommand,
    mode: PromotionMode,
  ): Promise<PromotionResult | null> {
    const stored = readOriginal(viewer) ?? original.current;
    if (
      inFlight.current ||
      body.target.owner_id !== viewer ||
      (mode === "submit" && stored !== null)
    )
      return null;
    inFlight.current = true;
    const startedIdentity = identity;
    original.current = body;
    setPending(body);
    setBusy(true);
    setError(null);
    storeOriginal(viewer, body);
    try {
      const value = await postPromotionCommand(body, mode);
      if (!active.current || current.current !== startedIdentity) return null;
      setResult({ identity: startedIdentity, value });
      if (["completed", "published", "rejected"].includes(value.status)) {
        original.current = null;
        setPending(null);
        storeOriginal(viewer, null);
      }
      return value;
    } catch (cause) {
      if (!active.current || current.current !== startedIdentity) return null;
      if (cause instanceof ApiError && cause.status === 422) {
        original.current = null;
        setPending(null);
        storeOriginal(viewer, null);
        setError({ identity: startedIdentity, message: "请求内容有误，请重新核对。" });
      } else setError({ identity: startedIdentity, message: "结果待确认，请查看原操作。" });
      return null;
    } finally {
      inFlight.current = false;
      if (active.current && current.current === startedIdentity) setBusy(false);
    }
  }
  return {
    pending: pending?.target.owner_id === viewer ? pending : null,
    result: result?.identity === identity ? result.value : null,
    error: error?.identity === identity ? error.message : null,
    busy,
    submit: (body: PromotionCommand) => send(body, "submit"),
    lookup: () => {
      const body = readOriginal(viewer) ?? original.current;
      return body ? send(body, "lookup") : Promise.resolve(null);
    },
    resume: () => {
      const body = readOriginal(viewer) ?? original.current;
      return body ? send(body, "resume") : Promise.resolve(null);
    },
  };
}
