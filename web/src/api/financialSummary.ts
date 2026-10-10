import { useCallback, useEffect, useRef, useState } from "react";
import { ApiError, apiClient, type Schemas } from "./client";

export type FinancialSummary = Schemas["FundamentalSummaryData"];

type FinancialState =
  | { phase: "loading"; data: null }
  | { phase: "ready"; data: FinancialSummary }
  | { phase: "error"; data: null };

/** The finance replica has its own identity and does not follow Serving /meta. */
export function useFinancialSummary() {
  const sourceIdentity = useRef<string | null>(null);
  const sequence = useRef(0);
  const pending = useRef<AbortController | null>(null);
  const [state, setState] = useState<FinancialState>({ phase: "loading", data: null });

  const refresh = useCallback(async () => {
    const current = ++sequence.current;
    pending.current?.abort();
    const controller = new AbortController();
    pending.current = controller;
    setState({ phase: "loading", data: null });
    const read = (identity: string | null) =>
      apiClient().GET("/api/v1/data/fundamentals/summary", {
        params: { query: { expected_identity: identity ?? undefined } },
        signal: controller.signal,
      });

    try {
      const expected = sourceIdentity.current;
      let result = await read(expected);
      if (current !== sequence.current || controller.signal.aborted) return;
      if (result.response.status === 409 && expected !== null) {
        sourceIdentity.current = null;
        result = await read(null);
      }
      if (current !== sequence.current || controller.signal.aborted) return;
      if (result.data === undefined) {
        throw new ApiError(result.response.status, "财务数据暂时无法读取");
      }
      sourceIdentity.current = result.data.source?.identity ?? null;
      setState({ phase: "ready", data: result.data });
    } catch {
      if (current === sequence.current && !controller.signal.aborted) {
        setState({ phase: "error", data: null });
      }
    } finally {
      if (current === sequence.current) pending.current = null;
    }
  }, []);

  useEffect(() => {
    void refresh();
    return () => {
      sequence.current += 1;
      pending.current?.abort();
      pending.current = null;
    };
  }, [refresh]);

  return { ...state, refresh };
}
