import { useEffect, useRef, useState } from "react";
import { ApiError } from "@/api/client";
import { type PaperOperation, type PaperReceipt, postPaperOperation } from "./paperPortfolioApi";

function key(viewer: string): string {
  return `rquant.paper-portfolio.pending.${encodeURIComponent(viewer)}`;
}
interface StoredOperation {
  command: PaperOperation;
  confirmationId?: string;
}
function read(viewer: string): StoredOperation | null {
  try {
    const raw = sessionStorage.getItem(key(viewer));
    if (!raw || new TextEncoder().encode(raw).length > 16384) return null;
    const stored: unknown = JSON.parse(raw);
    const body: unknown =
      typeof stored === "object" && stored !== null && "command" in stored
        ? stored.command
        : stored;
    if (
      typeof body !== "object" ||
      body === null ||
      !("kind" in body) ||
      !("command_id" in body) ||
      !("account_id" in body) ||
      !("generation_id" in body) ||
      !("requested_at" in body) ||
      ![
        "set_paper_account_paused",
        "save_paper_portfolio_configuration",
        "run_paper_portfolio_research",
      ].includes(String(body.kind)) ||
      !/^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$/.test(String(body.command_id)) ||
      typeof body.account_id !== "string" ||
      typeof body.generation_id !== "string" ||
      typeof body.requested_at !== "string"
    )
      return null;
    const confirmationId =
      typeof stored === "object" &&
      stored !== null &&
      "confirmationId" in stored &&
      typeof stored.confirmationId === "string" &&
      stored.confirmationId.length <= 64
        ? stored.confirmationId
        : undefined;
    return { command: body as PaperOperation, confirmationId };
  } catch {
    return null;
  }
}

export function usePaperCommands(viewer: string) {
  const initial = useRef(read(viewer));
  const [pending, setPending] = useState<PaperOperation | null>(initial.current?.command ?? null);
  const [result, setResult] = useState<PaperReceipt | null>(null);
  const [busy, setBusy] = useState(false);
  const original = useRef(pending);
  const confirmation = useRef(initial.current?.confirmationId);
  const active = useRef(true);
  const flight = useRef(false);
  useEffect(() => {
    active.current = true;
    return () => {
      active.current = false;
    };
  }, []);
  function clear(): void {
    original.current = null;
    confirmation.current = undefined;
    setPending(null);
    try {
      sessionStorage.removeItem(key(viewer));
    } catch {
      /* Receipt stays final. */
    }
  }
  async function send(
    body: PaperOperation,
    resume: boolean,
    confirmationId?: string,
    retry = false,
  ): Promise<void> {
    if (flight.current || (!resume && !retry && original.current !== null)) return;
    flight.current = true;
    original.current = body;
    if (!resume) confirmation.current = confirmationId;
    setPending(body);
    setBusy(true);
    try {
      sessionStorage.setItem(
        key(viewer),
        JSON.stringify({ command: body, confirmationId: confirmation.current }),
      );
    } catch {
      /* Keep the in-memory original. */
    }
    try {
      const receipt = await postPaperOperation(body, resume, confirmationId);
      if (!active.current) return;
      setResult(receipt);
      if (["rejected", "published", "applied", "submitted"].includes(receipt.status)) clear();
    } catch (error) {
      if (!active.current) return;
      if (error instanceof ApiError && error.status === 422) {
        clear();
        setResult({
          command_id: body.command_id,
          account_id: body.account_id,
          status: "rejected",
          message: error.message,
        });
      } else {
        setResult({
          command_id: body.command_id,
          account_id: body.account_id,
          status: "uncertain",
          message: "结果待确认，请继续查看。",
        });
      }
    } finally {
      flight.current = false;
      if (active.current) setBusy(false);
    }
  }
  return {
    pending,
    result,
    busy,
    submit: (body: PaperOperation, confirmationId?: string) => send(body, false, confirmationId),
    recover: () => (original.current === null ? Promise.resolve() : send(original.current, true)),
    retry: () =>
      original.current === null
        ? Promise.resolve()
        : send(original.current, false, confirmation.current, true),
  };
}
