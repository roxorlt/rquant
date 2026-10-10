import { act, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import type { Schemas } from "@/api/client";
import { META_QUERY_KEY } from "@/api/useMeta";
import { metaEnvelope } from "@/test/fixtures";
import { findJargon } from "@/test/jargon";
import { renderApp } from "@/test/render";
import { metaHandler, server } from "@/test/server";

type Summary = Schemas["FundamentalSummaryData"];

const identityA = "a".repeat(64);
const identityB = "b".repeat(64);

function summary(status: Summary["status"] = "ready", identity = identityA): Summary {
  const ready = status === "ready";
  return {
    status,
    decision_date: status === "ready" || status === "no_records" ? "2026-09-25" : null,
    waiting_for_today: ready,
    source: status === "not_configured" ? null : { identity, updated_at: "2026-09-28T01:15:00Z" },
    record_count: ready ? 1_234 : null,
    coverage_note: "全市场覆盖尚未核验",
    fields: (
      [
        ["pe_ttm", "市盈率", "倍"],
        ["pb", "市净率", "倍"],
        ["dv_ttm", "股息率", "%"],
        ["roe", "净资产收益率", "%"],
        ["or_yoy", "营收同比", "%"],
        ["netprofit_yoy", "归母净利同比", "%"],
      ] as const
    ).map(([key, label, unit]) => ({
      key,
      label,
      unit,
      known_count: ready ? 1_200 : null,
      unknown_count: ready ? 34 : null,
      reasons: ready ? [{ label: "披露尚未可见", count: 34 }] : [],
    })),
  };
}

describe("数据中心财务概况", () => {
  beforeEach(() => {
    server.use(
      http.get("*/api/v1/data/catalog", () =>
        HttpResponse.json({ data: { version: 1, datasets: [] }, serving: metaEnvelope().serving }),
      ),
    );
  });

  it("shows six verified field counts and dates without implying market coverage", async () => {
    const requested: string[] = [];
    server.use(
      http.get("*/api/v1/data/fundamentals/summary", ({ request }) => {
        requested.push(new URL(request.url).searchParams.get("expected_identity") ?? "");
        return HttpResponse.json(summary());
      }),
    );
    const user = userEvent.setup();
    renderApp("/datacenter");
    await user.click(await screen.findByRole("button", { name: "财务" }, { timeout: 3_000 }));
    const panel = screen.getByRole("region", { name: "财务概况" });
    expect(await within(panel).findByText("1,234")).toBeInTheDocument();
    expect(within(panel).getByText("2026-09-25")).toBeInTheDocument();
    expect(within(panel).getByText("副本同步时间")).toBeInTheDocument();
    expect(within(panel).getByText("等待今日 17:00")).toBeInTheDocument();
    expect(within(panel).getByText("全市场覆盖尚未核验")).toBeInTheDocument();
    expect(within(panel).getAllByText("1,200")).toHaveLength(6);
    expect(within(panel).getAllByText("34")).toHaveLength(6);
    for (const name of ["市盈率", "市净率", "股息率", "净资产收益率", "营收同比", "归母净利同比"]) {
      expect(within(panel).getByText(name)).toBeInTheDocument();
    }
    expect(requested).toEqual([""]);
    expect(findJargon(panel.textContent ?? "")).toEqual([]);
    expect(panel.textContent).not.toContain(identityA);

    await user.click(within(panel).getByRole("button", { name: "刷新财务数据" }));
    await waitFor(() => expect(requested).toEqual(["", identityA]));
  });

  it("hides old counts during refresh, clears A on 409, and discovers B once", async () => {
    const requested: string[] = [];
    let releaseChanged: (() => void) | undefined;
    const changed = new Promise<void>((resolve) => {
      releaseChanged = resolve;
    });
    server.use(
      http.get("*/api/v1/data/fundamentals/summary", async ({ request }) => {
        const expected = new URL(request.url).searchParams.get("expected_identity") ?? "";
        requested.push(expected);
        if (expected === identityA) {
          await changed;
          return HttpResponse.json({ detail: "changed" }, { status: 409 });
        }
        return HttpResponse.json(summary("ready", requested.length === 1 ? identityA : identityB));
      }),
    );
    const user = userEvent.setup();
    renderApp("/datacenter");
    await user.click(await screen.findByRole("button", { name: "财务" }));
    const panel = screen.getByRole("region", { name: "财务概况" });
    expect(await within(panel).findByText("1,234")).toBeInTheDocument();

    await user.click(within(panel).getByRole("button", { name: "刷新财务数据" }));
    expect(within(panel).queryByText("1,234")).not.toBeInTheDocument();
    expect(within(panel).getByRole("status", { name: "财务概况加载中" })).toBeInTheDocument();
    releaseChanged?.();
    expect(await within(panel).findByText("1,234")).toBeInTheDocument();
    expect(requested).toEqual(["", identityA, ""]);

    await user.click(within(panel).getByRole("button", { name: "刷新财务数据" }));
    await waitFor(() => expect(requested).toEqual(["", identityA, "", identityB]));
  });

  it("withdraws old counts on a failed request and offers a retry", async () => {
    let fail = false;
    server.use(
      http.get("*/api/v1/data/fundamentals/summary", () =>
        fail
          ? HttpResponse.json({ detail: "internal" }, { status: 503 })
          : HttpResponse.json(summary()),
      ),
    );
    const user = userEvent.setup();
    renderApp("/datacenter");
    await user.click(await screen.findByRole("button", { name: "财务" }));
    const panel = screen.getByRole("region", { name: "财务概况" });
    expect(await within(panel).findByText("1,234")).toBeInTheDocument();
    fail = true;
    await user.click(within(panel).getByRole("button", { name: "刷新财务数据" }));
    expect(within(panel).queryByText("1,234")).not.toBeInTheDocument();
    expect(await within(panel).findByText("暂时读不到财务数据")).toBeInTheDocument();
    expect(within(panel).getByRole("button", { name: "刷新财务数据" })).toBeEnabled();
  });

  it("ignores a replaced request before it can clear the pinned source", async () => {
    const requested: string[] = [];
    let releaseOld: (() => void) | undefined;
    const oldResponse = new Promise<void>((resolve) => {
      releaseOld = resolve;
    });
    server.use(
      http.get("*/api/v1/data/fundamentals/summary", async ({ request }) => {
        const expected = new URL(request.url).searchParams.get("expected_identity") ?? "";
        requested.push(expected);
        if (requested.length === 2) {
          await oldResponse;
          return HttpResponse.json({ detail: "changed" }, { status: 409 });
        }
        return HttpResponse.json(summary());
      }),
    );
    const user = userEvent.setup();
    renderApp("/datacenter");
    await user.click(await screen.findByRole("button", { name: "财务" }));
    const panel = screen.getByRole("region", { name: "财务概况" });
    expect(await within(panel).findByText("1,234")).toBeInTheDocument();
    await user.click(within(panel).getByRole("button", { name: "刷新财务数据" }));
    await waitFor(() => expect(requested).toHaveLength(2));
    await user.click(within(panel).getByRole("button", { name: "刷新财务数据" }));
    expect(await within(panel).findByText("1,234")).toBeInTheDocument();
    releaseOld?.();
    await user.click(within(panel).getByRole("button", { name: "刷新财务数据" }));
    await waitFor(() => expect(requested).toEqual(["", identityA, identityA, identityA]));
  });

  it.each([
    ["not_configured", "财务数据尚未接入"],
    ["calendar_unavailable", "交易日历待核验"],
    ["no_records", "该日暂无可核验记录"],
  ] as const)("shows %s without invented zero counts", async (state, copy) => {
    server.use(
      http.get("*/api/v1/data/fundamentals/summary", () => HttpResponse.json(summary(state))),
    );
    const user = userEvent.setup();
    renderApp("/datacenter");
    await user.click(await screen.findByRole("button", { name: "财务" }));
    const panel = screen.getByRole("region", { name: "财务概况" });
    expect(await within(panel).findByText(copy)).toBeInTheDocument();
    expect(within(panel).queryByText("0")).not.toBeInTheDocument();
    expect(within(panel).queryByText("1,234")).not.toBeInTheDocument();
    if (state !== "not_configured") {
      expect(within(panel).getByText("副本同步时间")).toBeInTheDocument();
    }
  });

  it("ignores Serving generation changes for its independent source", async () => {
    let requests = 0;
    server.use(
      http.get("*/api/v1/data/fundamentals/summary", () => {
        requests += 1;
        return HttpResponse.json(summary());
      }),
    );
    const user = userEvent.setup();
    const { queryClient } = renderApp("/datacenter");
    await user.click(await screen.findByRole("button", { name: "财务" }));
    expect(await screen.findByText("1,234")).toBeInTheDocument();
    const generation = metaEnvelope().data.generation;
    if (!generation) throw new Error("The synthetic meta fixture needs a generation");
    const changed = {
      ...metaEnvelope(),
      data: {
        ...metaEnvelope().data,
        generation: { ...generation, generation_id: "generation-b" },
      },
    };
    server.use(metaHandler(changed));
    act(() => queryClient.setQueryData(META_QUERY_KEY, changed));
    await waitFor(() => expect(screen.getByText("1,234")).toBeInTheDocument());
    expect(requests).toBe(1);
  });
});
