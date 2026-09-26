import { QueryClientProvider } from "@tanstack/react-query";
import { act, renderHook, waitFor } from "@testing-library/react";
import { HttpResponse, http } from "msw";
import type { ReactNode } from "react";
import { metaEnvelope } from "@/test/fixtures";
import { testQueryClient } from "@/test/queryClient";
import { server } from "@/test/server";
import { apiBaseUrl } from "./client";
import { useScreenCatalog } from "./screen";
import { fetchMeta, META_QUERY_KEY, useMeta } from "./useMeta";
import { useServingQuery } from "./useServingQuery";

describe("API client", () => {
  it("resolves the API next to the page, under whatever prefix serves it", () => {
    expect(apiBaseUrl("http://82.156.0.68:8081/app/")).toBe("http://82.156.0.68:8081/app");
    expect(apiBaseUrl("http://127.0.0.1:4173/app/index.html")).toBe("http://127.0.0.1:4173/app");
    expect(apiBaseUrl("http://127.0.0.1:4173/")).toBe("http://127.0.0.1:4173");
  });

  it("reads /api/v1/meta through the typed client", async () => {
    const envelope = await fetchMeta();
    expect(envelope.serving.state).toBe("ready");
    expect(envelope.data.generation?.generation_id.slice(0, 8)).toBe("a1b2c3d4");
  });
});

describe("useServingQuery", () => {
  it("splits an envelope into data and serving state", async () => {
    const client = testQueryClient();
    const wrapper = ({ children }: { children: ReactNode }) => (
      <QueryClientProvider client={client}>{children}</QueryClientProvider>
    );
    const envelope = metaEnvelope({ state: "stale", detail: "old" });
    const { result } = renderHook(
      () => useServingQuery(["probe"], async () => ({ data: 42, serving: envelope.serving })),
      { wrapper },
    );
    await waitFor(() => expect(result.current.data).toBe(42));
    expect(result.current.serving?.state).toBe("stale");
    expect(result.current.error).toBeNull();
  });

  it("Serving 换代不会自动重开独立的选股目录，手动刷新仍可更新", async () => {
    const client = testQueryClient();
    let generationId = "a".repeat(64);
    let catalogReads = 0;
    server.use(
      http.get("*/api/v1/meta", () => HttpResponse.json(metaEnvelope({ generationId }))),
      http.get("*/api/v1/screen/blocks", () => {
        catalogReads += 1;
        return HttpResponse.json({
          data: {
            blocks: [],
            dates: [],
            available: false,
            ranking_metrics: [],
            source: null,
            source_kind: "replica",
          },
          serving: metaEnvelope().serving,
        });
      }),
    );
    const wrapper = ({ children }: { children: ReactNode }) => (
      <QueryClientProvider client={client}>{children}</QueryClientProvider>
    );
    const { result } = renderHook(() => ({ meta: useMeta(), catalog: useScreenCatalog() }), {
      wrapper,
    });
    await waitFor(() =>
      expect(result.current.meta.data?.data.generation?.generation_id).toBe(generationId),
    );
    await waitFor(() => expect(catalogReads).toBe(1));
    generationId = "b".repeat(64);
    await act(async () => {
      await client.invalidateQueries({ queryKey: META_QUERY_KEY });
    });
    await waitFor(() =>
      expect(result.current.meta.data?.data.generation?.generation_id).toBe(generationId),
    );
    expect(catalogReads).toBe(1);
    result.current.catalog.refetch();
    await waitFor(() => expect(catalogReads).toBe(2));
  });

  it("默认 Serving 选股目录随代际更新，并让旧结果的来源可判过期", async () => {
    const client = testQueryClient();
    let generationId = "a".repeat(64);
    let catalogReads = 0;
    server.use(
      http.get("*/api/v1/meta", () => HttpResponse.json(metaEnvelope({ generationId }))),
      http.get("*/api/v1/screen/blocks", () => {
        catalogReads += 1;
        return HttpResponse.json({
          data: {
            blocks: [],
            dates: ["2026-09-24"],
            available: true,
            ranking_metrics: [],
            source: { identity: generationId, updated_at: "2026-09-24T07:31:00Z" },
            source_kind: "serving",
          },
          serving: metaEnvelope({ generationId }).serving,
        });
      }),
    );
    const wrapper = ({ children }: { children: ReactNode }) => (
      <QueryClientProvider client={client}>{children}</QueryClientProvider>
    );
    const { result } = renderHook(() => ({ meta: useMeta(), catalog: useScreenCatalog() }), {
      wrapper,
    });
    await waitFor(() => expect(catalogReads).toBe(1));
    const oldResultIdentity = result.current.catalog.data?.source?.identity;
    expect(oldResultIdentity).toBe(generationId);

    generationId = "b".repeat(64);
    await act(async () => {
      await client.invalidateQueries({ queryKey: META_QUERY_KEY });
    });
    await waitFor(() =>
      expect(result.current.meta.data?.data.generation?.generation_id).toBe(generationId),
    );
    await waitFor(() => expect(catalogReads).toBe(2));
    expect(result.current.catalog.data?.source?.identity).toBe(generationId);
    expect(result.current.catalog.data?.source?.identity).not.toBe(oldResultIdentity);
    result.current.catalog.refetch();
    await waitFor(() => expect(catalogReads).toBe(3));
  });

  it("初载不可用且来源为空的 Serving 选股目录仍随代际重取", async () => {
    const client = testQueryClient();
    let generationId = "a".repeat(64);
    let catalogReads = 0;
    server.use(
      http.get("*/api/v1/meta", () => HttpResponse.json(metaEnvelope({ generationId }))),
      http.get("*/api/v1/screen/blocks", () => {
        catalogReads += 1;
        return HttpResponse.json({
          data: {
            blocks: [],
            dates: [],
            available: false,
            ranking_metrics: [],
            source: null,
            source_kind: "serving",
          },
          serving: metaEnvelope({ generationId }).serving,
        });
      }),
    );
    const wrapper = ({ children }: { children: ReactNode }) => (
      <QueryClientProvider client={client}>{children}</QueryClientProvider>
    );
    const { result } = renderHook(() => ({ meta: useMeta(), catalog: useScreenCatalog() }), {
      wrapper,
    });
    await waitFor(() => expect(catalogReads).toBe(1));
    expect(result.current.catalog.data?.source).toBeNull();

    generationId = "b".repeat(64);
    await act(async () => {
      await client.invalidateQueries({ queryKey: META_QUERY_KEY });
    });
    await waitFor(() =>
      expect(result.current.meta.data?.data.generation?.generation_id).toBe(generationId),
    );
    await waitFor(() => expect(catalogReads).toBe(2));
  });
});
