import { act, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import type { Schemas } from "@/api/client";
import { META_QUERY_KEY } from "@/api/useMeta";
import { AppProviders } from "@/app/App";
import { readOriginal, saveOriginal } from "@/app/aiAssistanceSession";
import { metaEnvelope } from "@/test/fixtures";
import { testQueryClient } from "@/test/queryClient";
import { server } from "@/test/server";
import { AiInterpretation } from "./AiInterpretation";

it("reads four sealed sections with precise numeric evidence and clears private text on account change", async () => {
  const binding: Schemas["AISealedResultBinding"] = {
    owner_uid: "alice",
    source_kind: "portfolio",
    job_id: "e28878d8-2d82-4c92-93ab-6c1e963f8530",
    spec_sha256: "a".repeat(64),
    manifest_sha256: "b".repeat(64),
    result_sha256: "c".repeat(64),
  };
  const fact: Schemas["AISealedFact"] = {
    fact_id: "performance.total_return",
    label: "总收益",
    kind: "number",
    value: "0.125",
    unit: "%",
    decimals: 2,
    source_path: "result.performance.total_return",
    source_sha256: binding.result_sha256,
  };
  const content: Schemas["ValidatedInterpretation"] = {
    binding: {
      result: binding,
      facts_sha256: "d".repeat(64),
      model_id: "original-model",
      template_version: "ai-assistance/v1",
    },
    sections: ["overview", "annual", "risk", "suggestions"].map((key) => ({
      key: key as "overview" | "annual" | "risk" | "suggestions",
      paragraphs: [
        {
          text: key === "overview" ? "封存总收益 12.50%。" : "依照本次完整结果继续核对。",
          citations: [fact.fact_id],
        },
      ],
    })),
    metrics: [],
  };
  let calls = 0;
  let reads = 0;
  server.use(
    http.post("*/api/v1/ai/interpretations/read", () => {
      reads++;
      return HttpResponse.json({
        serving: metaEnvelope().serving,
        data: { binding, content, facts: [fact], cache_key: "e".repeat(64) },
      });
    }),
    http.get("*/api/v1/ai/capabilities", () =>
      HttpResponse.json({
        serving: metaEnvelope().serving,
        data: { available: true, can_generate: false },
      }),
    ),
    http.post("*/api/v1/ai/requests", () => {
      calls++;
      return HttpResponse.error();
    }),
  );
  const client = testQueryClient();
  const user = userEvent.setup();
  const { rerender } = render(
    <AppProviders queryClient={client}>
      <AiInterpretation
        viewer="alice"
        sourceKind="portfolio"
        jobId={binding.job_id}
        resultHash={binding.result_sha256}
      />
    </AppProviders>,
  );
  const overview = await screen.findByRole("region", { name: "概要" });
  for (const name of ["分年表现", "风险", "下一步建议"])
    expect(screen.getByRole("region", { name })).toBeInTheDocument();
  expect(overview).toHaveTextContent("封存总收益 12.50%。");
  await user.click(within(overview).getByRole("button", { name: "查看总收益依据" }));
  expect(await screen.findByText(/封存原值 0.125/)).toBeInTheDocument();
  expect(screen.getByText(/result.performance.total_return/)).toHaveTextContent(
    binding.result_sha256,
  );
  expect(calls).toBe(0);
  await user.click(screen.getByRole("button", { name: "刷新解读" }));
  await waitFor(() => expect(reads).toBe(2));
  expect(screen.queryByRole("button", { name: "生成解读" })).not.toBeInTheDocument();
  rerender(
    <AppProviders queryClient={client}>
      <AiInterpretation
        viewer="bob"
        sourceKind="portfolio"
        jobId={binding.job_id}
        resultHash={binding.result_sha256}
      />
    </AppProviders>,
  );
  await waitFor(() => expect(screen.queryByText("封存总收益 12.50%。")).not.toBeInTheDocument());
});
it("keeps missing sealed context explicit and never generates on mount", async () => {
  let calls = 0;
  server.use(
    http.post("*/api/v1/ai/interpretations/read", () =>
      HttpResponse.json({ detail: "回测结果尚未封存。" }, { status: 409 }),
    ),
    http.post("*/api/v1/ai/requests", () => {
      calls++;
      return HttpResponse.error();
    }),
    http.get("*/api/v1/ai/capabilities", () =>
      HttpResponse.json({
        serving: metaEnvelope().serving,
        data: { available: true, can_generate: true, can_prepare_backtest: false },
      }),
    ),
  );
  render(
    <AppProviders queryClient={testQueryClient()}>
      <AiInterpretation
        viewer="alice"
        sourceKind="portfolio"
        jobId="e28878d8-2d82-4c92-93ab-6c1e963f8530"
        resultHash={"f".repeat(64)}
      />
    </AppProviders>,
  );
  expect(await screen.findByText("回测结果尚未封存。")).toBeInTheDocument();
  expect(calls).toBe(0);
  expect(screen.getByRole("button", { name: "生成解读" })).toBeDisabled();
});

const savedBinding: Schemas["AISealedResultBinding"] = {
  owner_uid: "alice",
  source_kind: "portfolio",
  job_id: "e28878d8-2d82-4c92-93ab-6c1e963f8530",
  spec_sha256: "a".repeat(64),
  manifest_sha256: "b".repeat(64),
  result_sha256: "c".repeat(64),
};
const original: Schemas["AIInterpretationRequest"] = {
  purpose: "interpretation",
  request_id: "685cf99f-e774-4bb9-a21b-dfe01b859ac3",
  source_kind: savedBinding.source_kind,
  job_id: savedBinding.job_id,
  spec_sha256: savedBinding.spec_sha256,
  manifest_sha256: savedBinding.manifest_sha256,
  result_sha256: savedBinding.result_sha256,
};
function pending() {
  let release = () => {};
  const promise = new Promise<void>((resolve) => {
    release = resolve;
  });
  return { promise, release };
}
function savedView(content = false): Schemas["AIInterpretationView"] {
  return {
    binding: savedBinding,
    cache_key: "e".repeat(64),
    facts: [],
    content: content
      ? {
          binding: {
            result: savedBinding,
            facts_sha256: "d".repeat(64),
            model_id: "original-model",
            template_version: "ai-assistance/v1",
          },
          sections: ["overview", "annual", "risk", "suggestions"].map((key) => ({
            key: key as "overview" | "annual" | "risk" | "suggestions",
            paragraphs: [{ text: "上一代私有解读。", citations: [] }],
          })),
          metrics: [],
        }
      : null,
    message: "尚无保存的解读。",
  };
}
function interpretation() {
  return (
    <AiInterpretation
      viewer="alice"
      sourceKind="portfolio"
      jobId={savedBinding.job_id}
      resultHash={savedBinding.result_sha256}
    />
  );
}
const slot = `interpretation:portfolio:${savedBinding.job_id}:${savedBinding.result_sha256}`;

it.each([false, true])(
  "offers only read and refresh for cached interpretation when paid generation is %s",
  async (paid) => {
    let calls = 0;
    let reads = 0;
    server.use(
      http.get("*/api/v1/ai/capabilities", () =>
        HttpResponse.json({
          serving: metaEnvelope().serving,
          data: { available: true, can_generate: paid },
        }),
      ),
      http.post("*/api/v1/ai/interpretations/read", () => {
        reads++;
        return HttpResponse.json({ serving: metaEnvelope().serving, data: savedView(true) });
      }),
      http.post("*/api/v1/ai/requests", () => {
        calls++;
        return HttpResponse.error();
      }),
    );
    const user = userEvent.setup();
    render(<AppProviders queryClient={testQueryClient()}>{interpretation()}</AppProviders>);
    await screen.findByRole("region", { name: "概要" });
    expect(screen.queryByRole("button", { name: "生成解读" })).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "新建解读" })).not.toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "刷新解读" }));
    await waitFor(() => expect(reads).toBe(2));
    expect(calls).toBe(0);
  },
);

it("continues only a proved reserved original interpretation and then only looks up its unknown result", async () => {
  saveOriginal("alice", slot, original);
  const generated: unknown[] = [];
  const looked: unknown[] = [];
  server.use(
    http.get("*/api/v1/ai/capabilities", () =>
      HttpResponse.json({
        serving: metaEnvelope().serving,
        data: { available: true, can_generate: false, remaining_calls: 0 },
      }),
    ),
    http.post("*/api/v1/ai/interpretations/read", () =>
      HttpResponse.json({ serving: metaEnvelope().serving, data: savedView() }),
    ),
    http.post("*/api/v1/ai/requests/lookup", async ({ request }) => {
      looked.push(await request.json());
      return HttpResponse.json({
        serving: metaEnvelope().serving,
        data: {
          request_id: original.request_id,
          purpose: "interpretation",
          state: generated.length ? "unknown" : "reserved",
          created_at: "2026-10-06T00:00:00Z",
          result: null,
        },
      });
    }),
    http.post("*/api/v1/ai/requests", async ({ request }) => {
      generated.push(await request.json());
      return HttpResponse.json({
        serving: metaEnvelope().serving,
        data: {
          request_id: original.request_id,
          purpose: "interpretation",
          state: "unknown",
          created_at: "2026-10-06T00:00:00Z",
          result: null,
        },
      });
    }),
  );
  const user = userEvent.setup();
  render(<AppProviders queryClient={testQueryClient()}>{interpretation()}</AppProviders>);
  await screen.findByText("尚无保存的解读。");
  expect(generated).toEqual([]);
  await user.click(screen.getByRole("button", { name: "继续查看原请求" }));
  await user.click(await screen.findByRole("button", { name: "继续生成原请求" }));
  await waitFor(() => expect(generated).toEqual([original]));
  await user.click(await screen.findByRole("button", { name: "继续查看原请求" }));
  await waitFor(() => expect(looked).toEqual([original, original]));
  expect(generated).toEqual([original]);
});

it.each(["not_dispatched", "completed"] as const)(
  "FCR-001 interpretation preserves a missing original until confirmed %s",
  async (terminal) => {
    saveOriginal("alice", slot, original);
    const generated: unknown[] = [];
    const looked: unknown[] = [];
    server.use(
      http.get("*/api/v1/ai/capabilities", () =>
        HttpResponse.json({
          serving: metaEnvelope().serving,
          data: { available: true, can_generate: false, remaining_calls: 0 },
        }),
      ),
      http.post("*/api/v1/ai/interpretations/read", () =>
        HttpResponse.json({ serving: metaEnvelope().serving, data: savedView() }),
      ),
      http.post("*/api/v1/ai/requests/lookup", async ({ request }) => {
        looked.push(await request.json());
        if (looked.length === 1) {
          return HttpResponse.json({ detail: "找不到原请求。" }, { status: 404 });
        }
        return HttpResponse.json({
          serving: metaEnvelope().serving,
          data: {
            request_id: original.request_id,
            purpose: "interpretation",
            state: terminal,
            created_at: "2026-10-06T00:00:00Z",
            result: null,
          },
        });
      }),
      http.post("*/api/v1/ai/requests", async ({ request }) => {
        generated.push(await request.json());
        return HttpResponse.json({
          serving: metaEnvelope().serving,
          data: {
            request_id: original.request_id,
            purpose: "interpretation",
            state: "unknown",
            created_at: "2026-10-06T00:00:00Z",
            result: null,
          },
        });
      }),
    );
    const user = userEvent.setup();
    render(<AppProviders queryClient={testQueryClient()}>{interpretation()}</AppProviders>);
    await screen.findByText("尚无保存的解读。");
    await user.click(screen.getByRole("button", { name: "继续查看原请求" }));
    await screen.findByText("暂未查到原请求，请继续原请求。");
    expect(screen.queryByRole("button", { name: "新建解读" })).not.toBeInTheDocument();
    expect(screen.queryByText("调用未发出。")).not.toBeInTheDocument();
    expect(readOriginal("alice", slot)).toEqual(original);
    expect(generated).toEqual([]);
    await user.click(screen.getByRole("button", { name: "继续生成原请求" }));
    await screen.findByRole("button", { name: "继续查看原请求" });
    expect(generated).toEqual([original]);
    expect(readOriginal("alice", slot)).toEqual(original);
    expect(screen.queryByRole("button", { name: "新建解读" })).not.toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "继续查看原请求" }));
    await user.click(await screen.findByRole("button", { name: "新建解读" }));
    expect(looked).toEqual([original, original]);
    expect(readOriginal("alice", slot)).toBeNull();
    expect(screen.getByRole("button", { name: "生成解读" })).toBeInTheDocument();
    expect(generated).toEqual([original]);
  },
);

it("does not read or refresh before capabilities complete or while AI is unavailable", async () => {
  const capability = pending();
  let capabilityCalls = 0;
  let reads = 0;
  let generations = 0;
  server.use(
    http.get("*/api/v1/ai/capabilities", async () => {
      capabilityCalls++;
      await capability.promise;
      return HttpResponse.json({
        serving: metaEnvelope().serving,
        data: { available: false, can_generate: false, message: "助手尚未配置。" },
      });
    }),
    http.post("*/api/v1/ai/interpretations/read", () => {
      reads++;
      return HttpResponse.json({ detail: "当前账号不能使用助手。" }, { status: 403 });
    }),
    http.post("*/api/v1/ai/requests", () => {
      generations++;
      return HttpResponse.error();
    }),
  );
  const user = userEvent.setup();
  render(<AppProviders queryClient={testQueryClient()}>{interpretation()}</AppProviders>);
  try {
    await waitFor(() => expect(capabilityCalls).toBe(1));
    expect(reads).toBe(0);
    expect(screen.getByRole("button", { name: "刷新解读" })).toBeDisabled();
    await user.click(screen.getByRole("button", { name: "刷新解读" }));
    capability.release();
    expect(await screen.findByText("AI 解读尚未配置")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "刷新解读" }));
    expect(reads).toBe(0);
    expect(generations).toBe(0);
    expect(screen.queryByText("当前账号不能使用助手。")).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "生成解读" })).toBeDisabled();
  } finally {
    capability.release();
  }
});

it("does not refetch sealed history after an original lookup when capabilities become unavailable", async () => {
  saveOriginal("alice", slot, original);
  const lookup = pending();
  let available = true;
  let reads = 0;
  let lookups = 0;
  let lookupBody: unknown;
  server.use(
    http.get("*/api/v1/ai/capabilities", () =>
      HttpResponse.json({
        serving: metaEnvelope().serving,
        data: { available, can_generate: false },
      }),
    ),
    http.post("*/api/v1/ai/interpretations/read", () => {
      reads++;
      return HttpResponse.json({ serving: metaEnvelope().serving, data: savedView() });
    }),
    http.post("*/api/v1/ai/requests/lookup", async ({ request }) => {
      lookups++;
      lookupBody = await request.json();
      await lookup.promise;
      return HttpResponse.json({
        serving: metaEnvelope().serving,
        data: {
          request_id: original.request_id,
          purpose: "interpretation",
          state: "completed",
          created_at: "2026-10-06T00:00:00Z",
          result: null,
        },
      });
    }),
  );
  const client = testQueryClient();
  const user = userEvent.setup();
  render(<AppProviders queryClient={client}>{interpretation()}</AppProviders>);
  try {
    await screen.findByText("尚无保存的解读。");
    await user.click(screen.getByRole("button", { name: "继续查看原请求" }));
    await waitFor(() => expect(lookups).toBe(1));
    available = false;
    await act(() => client.invalidateQueries({ queryKey: ["ai", "capabilities", "alice"] }));
    expect(await screen.findByText("AI 解读尚未配置")).toBeInTheDocument();
    lookup.release();
    await screen.findByRole("button", { name: "新建解读" });
    expect(reads).toBe(1);
    expect(lookupBody).toEqual(original);
    expect(screen.getByRole("button", { name: "刷新解读" })).toBeDisabled();
  } finally {
    lookup.release();
  }
});

it("clears old generation content and does not let an old lookup refresh its sealed history", async () => {
  saveOriginal("alice", slot, original);
  const lookup = pending();
  const nextRead = pending();
  let reads = 0;
  let lookups = 0;
  let completedLookups = 0;
  server.use(
    http.get("*/api/v1/ai/capabilities", () =>
      HttpResponse.json({
        serving: metaEnvelope().serving,
        data: { available: true, can_generate: false },
      }),
    ),
    http.post("*/api/v1/ai/interpretations/read", async () => {
      reads++;
      const initial = reads === 1;
      if (!initial) await nextRead.promise;
      return HttpResponse.json({ serving: metaEnvelope().serving, data: savedView(initial) });
    }),
    http.post("*/api/v1/ai/requests/lookup", async () => {
      lookups++;
      await lookup.promise;
      completedLookups++;
      return HttpResponse.json({
        serving: metaEnvelope().serving,
        data: {
          request_id: original.request_id,
          purpose: "interpretation",
          state: "completed",
          created_at: "2026-10-06T00:00:00Z",
          result: null,
        },
      });
    }),
  );
  const client = testQueryClient();
  client.setQueryData(META_QUERY_KEY, metaEnvelope({ viewer: "alice" }));
  const user = userEvent.setup();
  render(<AppProviders queryClient={client}>{interpretation()}</AppProviders>);
  try {
    expect(await screen.findAllByText("上一代私有解读。")).toHaveLength(4);
    await user.click(screen.getByRole("button", { name: "继续查看原请求" }));
    await waitFor(() => expect(lookups).toBe(1));
    act(() =>
      client.setQueryData(
        META_QUERY_KEY,
        metaEnvelope({ viewer: "alice", generationId: "f".repeat(64) }),
      ),
    );
    await waitFor(() => expect(screen.queryByText("上一代私有解读。")).not.toBeInTheDocument());
    await waitFor(() => expect(reads).toBe(2));
    lookup.release();
    await waitFor(() => expect(completedLookups).toBe(1));
    nextRead.release();
    expect(await screen.findByText("尚无保存的解读。")).toBeInTheDocument();
    expect(reads).toBe(2);
    expect(screen.queryByText("上一代私有解读。")).not.toBeInTheDocument();
  } finally {
    lookup.release();
    nextRead.release();
  }
});
