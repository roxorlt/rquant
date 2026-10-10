import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import type { ScreenRunData } from "@/api/screen";
import { AppProviders } from "@/app/App";
import { metaEnvelope } from "@/test/fixtures";
import { testQueryClient } from "@/test/queryClient";
import { server } from "@/test/server";
import { ScreenWatchlistBatch } from "./ScreenWatchlistBatch";

const serving = metaEnvelope().serving;
const source: ScreenRunData["source"] = {
  mode: "daily",
  identity: "c".repeat(64),
  updated_at: "2026-09-24T07:30:00Z",
};
const rows = [
  { ts_code: "600001.SH", name: "样本一", close: 11, pct_chg: 1 },
  { ts_code: "600002.SH", name: "样本二", close: 12, pct_chg: 2 },
] as const;
const data: ScreenRunData = {
  status: "ready",
  trade_date: "2026-09-24",
  source,
  base_count: 80,
  total: 43,
  unknown_count: 0,
  rows: [...rows],
  steps: [],
  next_cursor: "next-page",
};

function installLocks(): void {
  const tails = new Map<string, Promise<void>>();
  Object.defineProperty(navigator, "locks", {
    configurable: true,
    value: {
      request: async (name: string, _options: unknown, task: () => Promise<void>) => {
        const prior = tails.get(name) ?? Promise.resolve();
        let release!: () => void;
        tails.set(
          name,
          new Promise<void>((resolve) => {
            release = resolve;
          }),
        );
        await prior;
        try {
          await task();
        } finally {
          release();
        }
      },
    },
  });
}

function watchlistHandlers() {
  server.use(
    http.get("*/api/v1/watchlist", () =>
      HttpResponse.json({
        data: {
          availability: "ready",
          available_at: "2026-09-24T07:31:00Z",
          message: "",
          items: [],
        },
        serving,
      }),
    ),
  );
}

function view(props: { pageIndex?: number; revision?: string; stale?: boolean } = {}) {
  const queryClient = testQueryClient();
  const renderPage = (pageIndex: number, revision: string, stale: boolean) => (
    <AppProviders queryClient={queryClient}>
      <ScreenWatchlistBatch
        data={data}
        pageIndex={pageIndex}
        revision={revision}
        stale={stale}
        running={false}
      />
    </AppProviders>
  );
  const result = render(
    renderPage(props.pageIndex ?? 0, props.revision ?? "first", props.stale ?? false),
  );
  return {
    ...result,
    show: (pageIndex: number, revision: string, stale = false) =>
      result.rerender(renderPage(pageIndex, revision, stale)),
  };
}

beforeEach(() => {
  window.localStorage.clear();
  installLocks();
});

it("确认范围随翻页立即失效，不会沿用旧股票", async () => {
  watchlistHandlers();
  const posted = vi.fn();
  server.use(http.post("*/api/v1/watchlist/commands", posted));
  const user = userEvent.setup();
  const page = view();
  const add = await screen.findByRole("button", { name: "加入本页 2 只" });
  await waitFor(() => expect(add).toBeEnabled());
  await user.click(add);
  const dialog = screen.getByRole("dialog", { name: "加入本页 2 只" });
  expect(dialog).toHaveTextContent("2026-09-24");
  expect(dialog).toHaveTextContent("第 1 页");
  expect(dialog).not.toHaveTextContent("43 只");
  page.show(1, "first");
  fireEvent.click(within(dialog).getByRole("button", { name: "确认加入本页 2 只" }));
  expect(posted).not.toHaveBeenCalled();
  expect(window.localStorage.length).toBe(0);
});

it("摘要区分已在名单与待同步，未见新代精确状态时不宣称已加入", async () => {
  watchlistHandlers();
  const sent: unknown[] = [];
  server.use(
    http.get("*/api/v1/watchlist/:code", ({ params }) =>
      HttpResponse.json({
        data: {
          availability: "ready",
          available_at: "2026-09-24T07:31:00Z",
          message: "",
          ts_code: params.code,
          status: params.code === rows[0].ts_code ? "active" : "absent",
          version: params.code === rows[0].ts_code ? 3 : null,
          expires_at: null,
          updated_at: null,
          price_levels: [],
          source: null,
        },
        serving,
      }),
    ),
    http.post("*/api/v1/watchlist/commands", async ({ request }) => {
      const body = (await request.json()) as {
        command_id: string;
        ts_code: string;
        action: string;
      };
      sent.push(body);
      return HttpResponse.json({
        command_id: body.command_id,
        ts_code: body.ts_code,
        action: body.action,
        status: "saved_syncing",
        version: 1,
        message: "已保存，正在同步",
      });
    }),
  );
  const user = userEvent.setup();
  view();
  const add = await screen.findByRole("button", { name: "加入本页 2 只" });
  await waitFor(() => expect(add).toBeEnabled());
  await user.click(add);
  await user.click(
    within(screen.getByRole("dialog")).getByRole("button", { name: "确认加入本页 2 只" }),
  );
  const summary = await screen.findByRole("status", { name: "" });
  await waitFor(() => expect(summary).toHaveTextContent("已在名单 1"));
  await waitFor(() => expect(summary).toHaveTextContent("已保存，正在同步 1"));
  expect(summary).not.toHaveTextContent("已加入 1");
  expect(sent).toHaveLength(1);
  expect(sent[0]).toMatchObject({
    ts_code: rows[1].ts_code,
    source: "screen_result",
    expected_version: null,
  });
});
