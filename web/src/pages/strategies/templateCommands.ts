import { useEffect, useRef, useState } from "react";
import { ApiError } from "@/api/client";
import { postTemplateOperation, type TemplateOperation, type TemplateResult } from "./templateApi";

function storageKey(viewer: string): string {
  return `rquant.strategy-template.pending.${encodeURIComponent(viewer)}`;
}

function readPending(viewer: string): TemplateOperation | null {
  try {
    const text = sessionStorage.getItem(storageKey(viewer));
    if (text === null || new TextEncoder().encode(text).length > 32768) return null;
    const value: unknown = JSON.parse(text);
    if (
      typeof value !== "object" ||
      value === null ||
      !("kind" in value) ||
      !("command_id" in value) ||
      !("requested_at" in value) ||
      !("generation_id" in value)
    )
      return null;
    if (
      !["save_strategy_template", "archive_strategy_template", "run_strategy_template"].includes(
        String(value.kind),
      ) ||
      typeof value.command_id !== "string" ||
      !/^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$/.test(value.command_id) ||
      typeof value.requested_at !== "string" ||
      typeof value.generation_id !== "string"
    )
      return null;
    return value as TemplateOperation;
  } catch {
    return null;
  }
}

export function useTemplateCommands(viewer: string) {
  const [pending, setPending] = useState<TemplateOperation | null>(() => readPending(viewer));
  const [result, setResult] = useState<TemplateResult | null>(null);
  const [busy, setBusy] = useState(false);
  const active = useRef(true);
  const inFlight = useRef(false);
  const original = useRef(pending);
  useEffect(() => {
    active.current = true;
    return () => {
      active.current = false;
    };
  }, []);

  function clearOriginal(): void {
    original.current = null;
    setPending(null);
    try {
      sessionStorage.removeItem(storageKey(viewer));
    } catch {
      /* A definite server result stays final when browser storage is blocked. */
    }
  }

  async function send(body: TemplateOperation, resume: boolean): Promise<void> {
    if (inFlight.current || (!resume && original.current !== null)) return;
    inFlight.current = true;
    original.current = body;
    setPending(body);
    setBusy(true);
    try {
      sessionStorage.setItem(storageKey(viewer), JSON.stringify(body));
    } catch {
      /* The in-memory original still supports receipt recovery. */
    }
    try {
      const receipt = await postTemplateOperation(body, resume);
      if (!active.current) return;
      setResult(receipt);
      if (["published", "submitted", "rejected"].includes(receipt.status)) {
        clearOriginal();
      }
    } catch (error) {
      if (!active.current) return;
      if (error instanceof ApiError && error.status === 422) {
        clearOriginal();
        setResult({ command_id: body.command_id, status: "rejected", message: error.message });
      } else {
        setResult({
          command_id: body.command_id,
          status: "uncertain",
          message: "结果待确认，请继续查看。",
        });
      }
    } finally {
      inFlight.current = false;
      if (active.current) setBusy(false);
    }
  }
  return {
    pending,
    result,
    busy,
    submit: (body: TemplateOperation) => send(body, false),
    resume: () => (original.current === null ? Promise.resolve() : send(original.current, true)),
  };
}
