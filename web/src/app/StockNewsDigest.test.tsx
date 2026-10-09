import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import type { Schemas } from "@/api/client";
import { AppProviders } from "@/app/App";
import { readOriginal, saveOriginal } from "@/app/aiAssistanceSession";
import { metaEnvelope } from "@/test/fixtures";
import { testQueryClient } from "@/test/queryClient";
import { renderApp } from "@/test/render";
import { server } from "@/test/server";
import { StockNewsDigest } from "./StockNewsDigest";

const content: Schemas["AINewsContent"] = {
  purpose: "news_digest",
  digest: {
    owner_uid: "alice",
    stock_code: "000001.SZ",
    context_sha256: "a".repeat(64),
    status: "limited",
    coverage_complete: false,
    statements: [
      { nature: "actual", content: { text: "原文披露盈利增长。", citations: ["doc.one.actual"] } },
      {
        nature: "forecast",
        content: { text: "原文预测未来盈利。", citations: ["doc.one.forecast"] },
      },
    ],
  },
  sources: [
    {
      document_id: "original-one",
      title: "公司原始报告",
      url: "https://stock.eastmoney.com/a/20260924001.html",
      provider: "eastmoney",
      source_kind: "news",
      published_date: "2026-09-24",
      published_at: "2026-09-24T05:00:00Z",
      body_date: "2026-09-23",
      first_collected_at: "2026-09-25T06:00:00Z",
    },
  ],
  coverage: [],
  citations: ["actual", "forecast"].map((nature, index) => ({
    fact_id: `doc.one.${nature}`,
    document_id: "original-one",
    nature: nature as "actual" | "forecast",
    quote: index ? "公司预测未来盈利。" : "公司披露盈利增长。",
    body_start: index * 12,
    body_end: (index + 1) * 12,
    body_sha256: "b".repeat(64),
    source_path: `documents.original-one.body[${index * 12}:${(index + 1) * 12}]`,
    period_end: null,
  })),
};
it("separates disclosed facts and forecasts, opens exact evidence, and clears a changed account", async () => {
  let generated = 0;
  server.use(
    http.get("*/api/v1/ai/news/:code", ({ params }) =>
      HttpResponse.json({
        serving: metaEnvelope().serving,
        data: {
          stock_code: params.code,
          state: "ready",
          content,
          coverage: [],
          nightly_enabled: false,
        },
      }),
    ),
    http.get("*/api/v1/ai/capabilities", () =>
      HttpResponse.json({
        serving: metaEnvelope().serving,
        data: { available: true, can_generate: true },
      }),
    ),
    http.post("*/api/v1/ai/requests", () => {
      generated++;
      return HttpResponse.error();
    }),
  );
  const client = testQueryClient();
  const user = userEvent.setup();
  const { rerender } = render(
    <AppProviders queryClient={client}>
      <StockNewsDigest viewer="alice" stockCode="000001.SZ" />
    </AppProviders>,
  );
  const actual = await screen.findByRole("region", { name: "已披露事实" });
  expect(actual).toHaveTextContent("原文披露盈利增长。");
  expect(screen.getByRole("region", { name: "预测与展望" })).toHaveTextContent(
    "原文预测未来盈利。",
  );
  await user.click(within(actual).getByRole("button", { name: "查看摘要依据" }));
  expect(await screen.findByText("公司披露盈利增长。")).toBeInTheDocument();
  expect(screen.getByRole("link", { name: "查看原文" })).toHaveAttribute(
    "href",
    content.sources[0]?.url,
  );
  await user.click(screen.getByText("来源与日期"));
  expect(screen.getByText("2026-09-24")).toBeInTheDocument();
  expect(screen.getByText("2026-09-23")).toBeInTheDocument();
  expect(screen.getByText("首次采集")).toBeInTheDocument();
  expect(generated).toBe(0);
  rerender(
    <AppProviders queryClient={client}>
      <StockNewsDigest viewer="bob" stockCode="000001.SZ" />
    </AppProviders>,
  );
  await waitFor(() => expect(screen.queryByText("原文披露盈利增长。")).not.toBeInTheDocument());
});
it("only reads a candidate summary after the user chooses the actual stock on overview", async () => {
  const selected: string[] = [];
  server.use(
    http.get("*/api/v1/ai/news/:code", ({ params }) => {
      selected.push(String(params.code));
      return HttpResponse.json({
        serving: metaEnvelope().serving,
        data: {
          stock_code: params.code,
          state: "missing",
          coverage: [],
          nightly_enabled: false,
          message: "尚未采集这只股票。",
        },
      });
    }),
    http.get("*/api/v1/ai/capabilities", () =>
      HttpResponse.json({
        serving: metaEnvelope().serving,
        data: { available: false, can_generate: false },
      }),
    ),
  );
  const user = userEvent.setup();
  renderApp("/overview");
  const select = await screen.findByRole("combobox", { name: "选择候选股票" });
  expect(selected).toEqual([]);
  await user.selectOptions(select, "001268.SZ");
  expect(await screen.findByText("尚未采集这只股票。")).toBeInTheDocument();
  expect(selected).toEqual(["001268.SZ"]);
});
it.each(["not_dispatched", "completed"] as const)(
  "FCR-001 news preserves a missing original until confirmed %s",
  async (terminal) => {
    const original: Schemas["AINewsRequest"] = {
      purpose: "news_digest",
      request_id: "685cf99f-e774-4bb9-a21b-dfe01b859ac3",
      stock_code: "000001.SZ",
      context_sha256: "a".repeat(64),
    };
    saveOriginal("alice", "news:000001.SZ", original);
    const generated: unknown[] = [];
    const looked: unknown[] = [];
    server.use(
      http.get("*/api/v1/ai/news/:code", ({ params }) =>
        HttpResponse.json({
          serving: metaEnvelope().serving,
          data: {
            stock_code: params.code,
            state: "missing",
            content: null,
            context_sha256: "b".repeat(64),
            coverage: [],
            nightly_enabled: false,
            message: "尚无保存的摘要。",
          },
        }),
      ),
      http.get("*/api/v1/ai/capabilities", () =>
        HttpResponse.json({
          serving: metaEnvelope().serving,
          data: { available: true, can_generate: false, remaining_calls: 0 },
        }),
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
            purpose: "news_digest",
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
            purpose: "news_digest",
            state: "unknown",
            created_at: "2026-10-06T00:00:00Z",
            result: null,
          },
        });
      }),
    );
    const user = userEvent.setup();
    render(
      <AppProviders queryClient={testQueryClient()}>
        <StockNewsDigest viewer="alice" stockCode="000001.SZ" />
      </AppProviders>,
    );
    await user.click(await screen.findByRole("button", { name: "继续查看原请求" }));
    await screen.findByText("暂未查到原请求，请继续原请求。");
    expect(screen.queryByRole("button", { name: "新建摘要" })).not.toBeInTheDocument();
    expect(screen.queryByText("调用未发出。")).not.toBeInTheDocument();
    expect(readOriginal("alice", "news:000001.SZ")).toEqual(original);
    expect(generated).toEqual([]);
    await user.click(screen.getByRole("button", { name: "继续生成原请求" }));
    await screen.findByRole("button", { name: "继续查看原请求" });
    expect(generated).toEqual([original]);
    expect(readOriginal("alice", "news:000001.SZ")).toEqual(original);
    expect(screen.queryByRole("button", { name: "新建摘要" })).not.toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "继续查看原请求" }));
    await user.click(await screen.findByRole("button", { name: "新建摘要" }));
    expect(looked).toEqual([original, original]);
    expect(readOriginal("alice", "news:000001.SZ")).toBeNull();
    expect(screen.getByRole("button", { name: "生成摘要" })).toBeInTheDocument();
    expect(generated).toEqual([original]);
  },
);

it("does not turn missing source or unknown work into an empty result", async () => {
  server.use(
    http.get("*/api/v1/ai/news/:code", ({ params }) =>
      HttpResponse.json({
        serving: metaEnvelope().serving,
        data: {
          stock_code: params.code,
          state: "missing",
          content: null,
          coverage: [],
          nightly_enabled: true,
          progress: {
            request_id: "e28878d8-2d82-4c92-93ab-6c1e963f8530",
            scope_sha256: "a".repeat(64),
            start_date: "2026-09-01",
            end_date: "2026-09-24",
            total: 601,
            completed: 2,
            pending: 599,
            unknown: 1,
            complete: false,
          },
          message: "原文尚未采集。",
        },
      }),
    ),
    http.get("*/api/v1/ai/capabilities", () =>
      HttpResponse.json({
        serving: metaEnvelope().serving,
        data: {
          available: true,
          can_generate: false,
          daily_limit: 0,
          remaining_calls: 0,
          can_prepare_backtest: false,
          message: "调用尚未启用。",
        },
      }),
    ),
  );
  render(
    <AppProviders queryClient={testQueryClient()}>
      <StockNewsDigest viewer="alice" stockCode="000001.SZ" />
    </AppProviders>,
  );
  expect(await screen.findByText("原文尚未采集。")).toBeInTheDocument();
  expect(screen.getByText(/已完成 2.*待处理 599/)).toBeInTheDocument();
  expect(screen.queryByText("没有相关新闻")).not.toBeInTheDocument();
});
