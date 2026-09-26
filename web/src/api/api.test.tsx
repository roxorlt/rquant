import { QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { metaEnvelope } from "@/test/fixtures";
import { testQueryClient } from "@/test/queryClient";
import { apiBaseUrl } from "./client";
import { fetchMeta } from "./useMeta";
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
});
