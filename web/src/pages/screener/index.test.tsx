import { act, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import { vi } from "vitest";
import type { Schemas } from "@/api/client";
import type {
  ExecuteScreenQuery,
  ScreenExecutionView,
  ScreenRunData,
  ScreenRunRequest,
} from "@/api/screen";
import { readOriginal, saveOriginal } from "@/app/aiAssistanceSession";
import { metaEnvelope } from "@/test/fixtures";
import { findJargon } from "@/test/jargon";
import { renderApp } from "@/test/render";
import { server } from "@/test/server";

const serving = metaEnvelope().serving;
const source: ScreenRunData["source"] = {
  mode: "daily",
  identity: "a".repeat(64),
  updated_at: "2026-09-24T07:30:00Z",
};
const PRIVATE_SCOPE = "1".repeat(64);
const RECENT_DESCRIPTIONS_KEY = `rquant.screen.recent-descriptions.v1:${PRIVATE_SCOPE}`;
type RunResolver = Parameters<typeof http.post>[1];
const aiReplies = new Map<string, Schemas["AIRequestView"]>();
function screenAiDraftHandler(resolver: RunResolver) {
  return http.post("*/api/v1/ai/requests", async (context) => {
    const body = (await context.request.clone().json()) as Schemas["AIScreenRequest"];
    const response = await resolver(context);
    if (!(response instanceof Response))
      throw new Error("Synthetic draft resolver must return an actual HTTP response");
    const raw = await response.json();
    const view: Schemas["AIRequestView"] = {
      request_id: body.request_id,
      purpose: "screen",
      state: "completed",
      created_at: "2026-10-06T06:00:00Z",
      message: response.ok
        ? null
        : String(raw.detail).includes("日期")
          ? raw.detail
          : "没能确定条件，请说清筛选范围和数值。",
      result: response.ok
        ? {
            purpose: "screen",
            definition: {
              schema_version: 1,
              mode: "daily",
              description: body.instruction,
              source_kind: raw.source_kind,
              source_identity: raw.source_identity,
              trade_date: raw.trade_date,
              conditions: raw.conditions.map((row: Schemas["ScreenCondition"]) => ({
                name: row.key,
                args: row.args,
              })),
              ranking: null,
            },
          }
        : null,
    };
    aiReplies.set(body.request_id, view);
    return HttpResponse.json({ data: view, serving });
  });
}
async function generateSuggestion(user: ReturnType<typeof userEvent.setup>) {
  const reset = screen.queryByRole("button", { name: "新建描述" });
  if (reset) await user.click(reset);
  await user.click(screen.getByRole("button", { name: "生成建议" }));
}

let recorded = new Map<
  string,
  {
    entry: ScreenExecutionView;
    data: ScreenRunData;
    page: (cursor: string) => Promise<Response | undefined>;
  }
>();
const drawerLifecycle = vi.hoisted(() => new Map<string, ((open: boolean) => void) | undefined>());
vi.mock("@/ui", async (original) => {
  const actual = await original<typeof import("@/ui")>();
  return {
    ...actual,
    SideDrawer: (props: Parameters<typeof actual.SideDrawer>[0]) => {
      if (typeof props.title === "string") drawerLifecycle.set(props.title, props.afterOpenChange);
      return <actual.SideDrawer {...props} />;
    },
  };
});

it.each([
  ["历史", "选股历史"],
  ["常用条件", "常用条件"],
])("%s 关闭后回到当前同页面入口", async (label, title) => {
  catalog();
  const user = userEvent.setup();
  renderApp("/screener");
  const trigger = await screen.findByRole("button", { name: label });
  await user.click(trigger);
  const dialog = await screen.findByRole("dialog", { name: title });
  const close = within(dialog).getByRole("button", { name: /close|关闭/i });
  close.focus();
  expect(screen.getByRole("button", { name: label })).not.toHaveFocus();
  await user.click(close);
  await waitFor(() => expect(screen.queryByRole("dialog", { name: title })).toBeNull());
  // Interrupted opening removes the real dialog without a false motion callback.
  await waitFor(() => expect(screen.getByRole("button", { name: label })).toHaveFocus());
  await act(async () => drawerLifecycle.get(title)?.(false));
  expect(screen.getByRole("button", { name: label })).toHaveFocus();
});

it("模式首次加载时保留同一盘中入口和页标题", async () => {
  catalog();
  let release: () => void = () => {};
  const pending = new Promise<void>((resolve) => {
    release = resolve;
  });
  let requested = false;
  server.use(
    http.get("*/api/v1/screen/blocks", async ({ request }) => {
      const intraday = new URL(request.url).searchParams.get("mode") === "intraday";
      if (intraday) {
        requested = true;
        await pending;
      }
      return HttpResponse.json({
        data: {
          blocks,
          dates: intraday ? [] : ["2026-09-24"],
          available: !intraday,
          ranking_metrics: [],
          source: intraday ? null : source,
          source_kind: intraday ? "intraday" : "replica",
          nl_generate_available: false,
        },
        serving,
      });
    }),
  );
  const user = userEvent.setup();
  renderApp("/screener");
  await screen.findByRole("button", { name: "运行筛选" });
  const title = screen.getByRole("heading", { name: "选股器" });
  const trigger = screen.getByRole("button", { name: "盘中" });
  try {
    await user.click(trigger);
    await waitFor(() => expect(requested).toBe(true));
    expect(trigger.isConnected).toBe(true);
    expect(screen.getByRole("button", { name: "盘中" })).toBe(trigger);
    expect(trigger).toHaveAttribute("aria-pressed", "true");
    expect(screen.getByRole("heading", { name: "选股器" })).toBe(title);
    expect(screen.queryByRole("button", { name: "运行筛选" })).toBeNull();
  } finally {
    await act(async () => release());
  }
  await screen.findByText("选股数据暂不可用");
  expect(screen.getByRole("button", { name: "盘中" })).toBe(trigger);
  expect(screen.getByRole("button", { name: "运行筛选" })).toBeDisabled();
});

it("重开待确认原请求显示查询和恢复入口，并保留同一 UUID 和完整正文", async () => {
  catalog();
  const command: ExecuteScreenQuery = {
    kind: "execute_screen_query",
    command_id: "reload-original-command",
    requested_at: "2026-10-05T07:00:00Z",
    page_size: 37,
    definition: {
      schema_version: 1,
      description: "原请求",
      mode: "daily",
      trade_date: "2026-09-24",
      source_kind: "replica",
      source_identity: "a".repeat(64),
      conditions: [{ name: "not_st", args: {} }],
      ranking: null,
    },
  };
  const original = { action: "execute" as const, command };
  sessionStorage.setItem(
    `rquant.screen-command.v1:${PRIVATE_SCOPE}`,
    JSON.stringify({ schema: 1, scope: PRIVATE_SCOPE, original }),
  );
  const seen: unknown[] = [];
  const execute = vi.fn();
  function recover(action: "lookup" | "resume") {
    return http.post(`*/api/v1/screen/query/${action}`, async ({ request }) => {
      seen.push(await request.json());
      return HttpResponse.json({
        available: true,
        owner_scope_tag: PRIVATE_SCOPE,
        presets: [],
        receipt: {
          command_id: command.command_id,
          status: "pending",
          enqueued_at: command.requested_at,
          completed_at: null,
          result: null,
          error: null,
        },
      });
    });
  }
  server.use(
    recover("lookup"),
    recover("resume"),
    http.post("*/api/v1/screen/query/execute", execute),
  );
  const user = userEvent.setup();
  renderApp("/screener");
  expect(await screen.findByRole("button", { name: "恢复原请求" })).toBeVisible();
  expect(screen.getByText("结果待确认，请核对原请求。")).toBeVisible();
  await user.click(screen.getByRole("button", { name: "查询原请求" }));
  await waitFor(() => expect(screen.getByRole("button", { name: "恢复原请求" })).toBeEnabled());
  await user.click(screen.getByRole("button", { name: "恢复原请求" }));
  await waitFor(() =>
    expect(seen).toEqual([
      { action: "lookup", original },
      { action: "resume", original },
    ]),
  );
  expect(execute).not.toHaveBeenCalled();
  expect(
    JSON.parse(sessionStorage.getItem(`rquant.screen-command.v1:${PRIVATE_SCOPE}`) ?? "null")
      .original,
  ).toEqual(original);
});

function screenRunHandler(resolver: RunResolver) {
  return http.post("*/api/v1/screen/query/execute", async (info) => {
    const command = (await info.request.json()) as ExecuteScreenQuery;
    const body: ScreenRunRequest = {
      mode: command.definition.mode,
      trade_date: command.definition.trade_date,
      conditions: command.definition.conditions.map((call) => ({
        key: call.name,
        args: call.args,
      })),
      page_size: command.page_size ?? 20,
      cursor: null,
      source_identity: command.definition.source_identity,
      ranking: command.definition.ranking ?? null,
    };
    const evaluate = async (cursor: string | null) => {
      const answer = await resolver({
        ...info,
        request: new Request(info.request.url, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ ...body, cursor }),
        }),
      });
      return answer instanceof Response ? answer : undefined;
    };
    const response = await evaluate(null);
    if (!response || !response.ok) return response;
    const envelope = (await response.json()) as Schemas["Envelope_ScreenRunData_"];
    const data = envelope.data;
    const entry: ScreenExecutionView = {
      execution_id: command.command_id,
      sequence: recorded.size + 1,
      command_hash: "c".repeat(64),
      plan_hash: "d".repeat(64),
      definition: { ...command.definition, trade_date: data.trade_date },
      original_command: command,
      source: data.source,
      started_at: command.requested_at,
      completed_at: command.requested_at,
      status: data.status === "ready" ? "succeeded" : "failed",
      base_count: data.base_count,
      total: data.total,
      unknown_count: data.unknown_count ?? 0,
      ranked_count: data.ranked_count ?? null,
      steps: data.steps,
      artifact_sha256: "e".repeat(64),
      member_rank_sha256: "f".repeat(64),
      failure_code: null,
    };
    recorded.set(command.command_id, { entry, data, page: (cursor) => evaluate(cursor) });
    return HttpResponse.json({
      available: true,
      owner_scope_tag: PRIVATE_SCOPE,
      receipt: {
        command_id: command.command_id,
        enqueued_at: command.requested_at,
        completed_at: command.requested_at,
        status: "succeeded",
        error: null,
        result: {},
      },
      presets: [],
    });
  });
}

beforeEach(() => {
  drawerLifecycle.clear();
  recorded = new Map();
  aiReplies.clear();
  server.use(
    http.get("*/api/v1/ai/capabilities", () =>
      HttpResponse.json({
        serving,
        data: {
          available: true,
          can_generate: true,
          can_prepare_backtest: false,
          daily_limit: 20,
          remaining_calls: 20,
        },
      }),
    ),
    http.post("*/api/v1/ai/requests/lookup", async ({ request }) => {
      const body = (await request.json()) as Schemas["AIScreenRequest"];
      const view = aiReplies.get(body.request_id);
      return view
        ? HttpResponse.json({ serving, data: view })
        : HttpResponse.json({ detail: "找不到原请求。" }, { status: 404 });
    }),
    http.get("*/api/v1/ai/news/:code", ({ params }) =>
      HttpResponse.json({
        serving,
        data: {
          stock_code: params.code,
          state: "missing",
          content: null,
          coverage: [],
          message: "原文尚未采集。",
          nightly_enabled: false,
          progress: null,
        },
      }),
    ),
    http.get("*/api/v1/screen/query/history", () =>
      HttpResponse.json({
        available: true,
        owner_scope_tag: PRIVATE_SCOPE,
        history: {
          owner_scope_tag: PRIVATE_SCOPE,
          items: [...recorded.values()].map((item) => item.entry).reverse(),
          next_cursor: null,
        },
        presets: [],
      }),
    ),
    http.get("*/api/v1/screen/query/presets", () =>
      HttpResponse.json({ available: true, owner_scope_tag: PRIVATE_SCOPE, presets: [] }),
    ),
    http.get("*/api/v1/screen/query/executions/:executionId", ({ params }) =>
      HttpResponse.json({
        available: true,
        owner_scope_tag: PRIVATE_SCOPE,
        execution: recorded.get(String(params.executionId))?.entry ?? null,
        presets: [],
      }),
    ),
    http.get(
      "*/api/v1/screen/query/executions/:executionId/results",
      async ({ params, request }) => {
        const current = recorded.get(String(params.executionId));
        if (!current) return HttpResponse.json({ detail: "未找到原请求。" }, { status: 404 });
        const cursor = new URL(request.url).searchParams.get("cursor");
        let data = current.data;
        if (cursor) {
          const response = await current.page(cursor);
          if (!response || !response.ok) return response;
          data = ((await response.json()) as Schemas["Envelope_ScreenRunData_"]).data;
        }
        return HttpResponse.json({
          available: true,
          owner_scope_tag: PRIVATE_SCOPE,
          results: {
            execution_id: current.entry.execution_id,
            artifact_sha256: current.entry.artifact_sha256,
            rows: data.rows,
            next_cursor: data.next_cursor,
          },
          presets: [],
        });
      },
    ),
  );
});
const blocks: Schemas["ScreenBlock"][] = [
  {
    key: "not_st",
    label: "排除 ST",
    hint: "剔除名称带 ST 的股票",
    category: "filter",
    category_label: "股票范围",
    parameters: [],
  },
  {
    key: "circ_mv_lt",
    label: "流通市值低于",
    hint: "筛出流通市值小于指定金额的股票",
    category: "filter",
    category_label: "股票范围",
    parameters: [
      {
        key: "threshold_yi",
        label: "市值上限（亿元）",
        input: "number",
        initial: 100,
        required: true,
        minimum: 0,
        maximum: 10000,
        scale: 1,
        custom_ma: false,
      },
    ],
  },
];

function catalog(available = true, nlAvailable = false) {
  server.use(
    http.get("*/api/v1/screen/tdx/preview/source", () =>
      HttpResponse.json({
        available,
        dates: available ? ["2026-09-24"] : [],
        source: available ? source : null,
      }),
    ),
    http.get("*/api/v1/screen/blocks", () =>
      HttpResponse.json({
        data: {
          blocks,
          dates: available ? ["2026-09-24"] : [],
          available,
          ranking_metrics: available
            ? [
                { value: "CIRC_MV[0]", label: "流通市值" },
                { value: "PCT_CHG[0]", label: "今日涨跌幅" },
              ]
            : [],
          source: available ? source : null,
          source_kind: "replica",
          nl_generate_available: nlAvailable,
        },
        serving,
      }),
    ),
  );
}

it("继续生成只用于已证明未发出的原选股请求，未知结果只查原 UUID", async () => {
  catalog(true, true);
  const original: Schemas["AIScreenRequest"] = {
    purpose: "screen",
    request_id: "685cf99f-e774-4bb9-a21b-dfe01b859ac3",
    instruction: "排除 ST",
    source_kind: "replica",
    source_identity: source.identity,
    trade_date: "2026-09-24",
    include_ranking: true,
  };
  saveOriginal(PRIVATE_SCOPE, "screen", original);
  const generated: unknown[] = [];
  const looked: unknown[] = [];
  server.use(
    http.get("*/api/v1/ai/capabilities", () =>
      HttpResponse.json({
        serving,
        data: { available: true, can_generate: false, remaining_calls: 0 },
      }),
    ),
    http.post("*/api/v1/ai/requests/lookup", async ({ request }) => {
      looked.push(await request.json());
      return HttpResponse.json({
        serving,
        data: {
          request_id: original.request_id,
          purpose: "screen",
          state: generated.length ? "unknown" : "reserved",
          created_at: "2026-10-06T00:00:00Z",
          result: null,
        },
      });
    }),
    http.post("*/api/v1/ai/requests", async ({ request }) => {
      generated.push(await request.json());
      return HttpResponse.json({
        serving,
        data: {
          request_id: original.request_id,
          purpose: "screen",
          state: "unknown",
          created_at: "2026-10-06T00:00:00Z",
          result: null,
        },
      });
    }),
  );
  const user = userEvent.setup();
  renderApp("/screener");
  await user.click(await screen.findByRole("button", { name: "继续查看原请求" }));
  expect(generated).toEqual([]);
  await user.click(await screen.findByRole("button", { name: "继续生成原请求" }));
  await waitFor(() => expect(generated).toEqual([original]));
  await user.click(await screen.findByRole("button", { name: "继续查看原请求" }));
  await waitFor(() => expect(looked).toEqual([original, original]));
  expect(generated).toEqual([original]);
});

it.each(["not_dispatched", "completed"] as const)(
  "FCR-001 screen preserves a missing original until confirmed %s",
  async (terminal) => {
    catalog(true, true);
    const original: Schemas["AIScreenRequest"] = {
      purpose: "screen",
      request_id: "685cf99f-e774-4bb9-a21b-dfe01b859ac3",
      instruction: "排除 ST",
      source_kind: "replica",
      source_identity: source.identity,
      trade_date: "2026-09-24",
      include_ranking: true,
    };
    saveOriginal(PRIVATE_SCOPE, "screen", original);
    const generated: unknown[] = [];
    const looked: unknown[] = [];
    server.use(
      http.get("*/api/v1/ai/capabilities", () =>
        HttpResponse.json({
          serving,
          data: { available: true, can_generate: false, remaining_calls: 0 },
        }),
      ),
      http.post("*/api/v1/ai/requests/lookup", async ({ request }) => {
        looked.push(await request.json());
        if (looked.length === 1) {
          return HttpResponse.json({ detail: "找不到原请求。" }, { status: 404 });
        }
        return HttpResponse.json({
          serving,
          data: {
            request_id: original.request_id,
            purpose: "screen",
            state: terminal,
            created_at: "2026-10-06T00:00:00Z",
            result: null,
          },
        });
      }),
      http.post("*/api/v1/ai/requests", async ({ request }) => {
        generated.push(await request.json());
        return HttpResponse.json({
          serving,
          data: {
            request_id: original.request_id,
            purpose: "screen",
            state: "unknown",
            created_at: "2026-10-06T00:00:00Z",
            result: null,
          },
        });
      }),
    );
    const user = userEvent.setup();
    renderApp("/screener");
    await user.click(await screen.findByRole("button", { name: "继续查看原请求" }));
    await screen.findByText("暂未查到原请求，请继续原请求。");
    const newDescription = screen.getByRole("button", { name: "新建描述" });
    expect(newDescription).toBeDisabled();
    await user.click(newDescription);
    expect(readOriginal(PRIVATE_SCOPE, "screen")).toEqual(original);
    expect(generated).toEqual([]);
    expect(screen.queryByText("调用未发出。")).not.toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "继续生成原请求" }));
    await screen.findByRole("button", { name: "继续查看原请求" });
    expect(generated).toEqual([original]);
    expect(readOriginal(PRIVATE_SCOPE, "screen")).toEqual(original);
    expect(screen.getByRole("button", { name: "新建描述" })).toBeDisabled();
    await user.click(screen.getByRole("button", { name: "继续查看原请求" }));
    await waitFor(() => expect(screen.getByRole("button", { name: "新建描述" })).toBeEnabled());
    expect(looked).toEqual([original, original]);
    await user.click(screen.getByRole("button", { name: "新建描述" }));
    expect(readOriginal(PRIVATE_SCOPE, "screen")).toBeNull();
    expect(screen.getByRole("button", { name: "生成建议" })).toBeInTheDocument();
    expect(generated).toEqual([original]);
  },
);

function stockDrawer() {
  server.use(
    http.get("*/api/v1/stocks/600001.SH/summary", () =>
      HttpResponse.json({
        data: { ts_code: "600001.SH", name: "样本01", price: 11, as_of: null, pools: [] },
        serving,
      }),
    ),
    http.get("*/api/v1/panorama/stocks/600001.SH/daily", () =>
      HttpResponse.json({ data: { ts_code: "600001.SH", name: "样本01", bars: [] }, serving }),
    ),
  );
}

describe("选股器", () => {
  it("运行保留完整原命令，私有回包失联后只恢复原请求", async () => {
    catalog();
    const seen: ExecuteScreenQuery[] = [];
    server.use(
      http.get("*/api/v1/screen/query/history", () =>
        HttpResponse.json({
          available: true,
          owner_scope_tag: "1".repeat(64),
          history: { owner_scope_tag: "1".repeat(64), items: [], next_cursor: null },
          presets: [],
        }),
      ),
      http.post("*/api/v1/screen/query/execute", async ({ request }) => {
        seen.push((await request.json()) as ExecuteScreenQuery);
        return HttpResponse.error();
      }),
      http.post("*/api/v1/screen/query/lookup", async ({ request }) => {
        const body = (await request.json()) as { original: { command: ExecuteScreenQuery } };
        seen.push(body.original.command);
        return HttpResponse.json({ detail: "未找到原请求。" }, { status: 404 });
      }),
    );
    renderApp("/screener");
    const user = userEvent.setup();
    await user.click(await screen.findByRole("button", { name: "运行筛选" }));
    await screen.findByText("结果待确认，请核对原请求。");
    await user.click(screen.getByRole("button", { name: "查询原请求" }));
    await waitFor(() => expect(seen).toHaveLength(2));
    expect(seen[1]).toEqual(seen[0]);
    expect(seen[0]).toMatchObject({
      kind: "execute_screen_query",
      definition: {
        source_identity: source.identity,
        trade_date: "2026-09-24",
        conditions: [{ name: "not_st", args: {} }],
      },
    });
    expect(screen.getByRole("button", { name: "历史" })).toBeVisible();
    expect(screen.queryByText("命中 0 只")).toBeNull();
  });
  it("批量操作只称本页两只，不把总命中数当成本页范围", async () => {
    Object.defineProperty(navigator, "locks", {
      configurable: true,
      value: {
        request: async (_name: string, _options: unknown, task: () => Promise<void>) => task(),
      },
    });
    catalog();
    server.use(
      screenRunHandler(() =>
        HttpResponse.json({
          data: {
            trade_date: "2026-09-24",
            status: "ready",
            base_count: 80,
            total: 43,
            steps: [{ label: "排除 ST", count: 43 }],
            rows: [
              { ts_code: "600001.SH", name: "样本01", close: 11, pct_chg: 1 },
              { ts_code: "600002.SH", name: "样本02", close: 12, pct_chg: 2 },
            ],
            next_cursor: "next-page",
            source,
          },
          serving,
        }),
      ),
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
    const user = userEvent.setup();
    renderApp("/screener");
    await user.click(await screen.findByRole("button", { name: "运行筛选" }));
    const add = await screen.findByRole("button", { name: "加入本页 2 只" });
    await waitFor(() => expect(add).toBeEnabled());
    await user.click(add);
    const dialog = screen.getByRole("dialog", { name: "加入本页 2 只" });
    expect(dialog).toHaveTextContent("2026-09-24");
    expect(dialog).toHaveTextContent("第 1 页");
    expect(dialog).not.toHaveTextContent("43 只");
  });

  it("最近描述只记校验成功的预览，去重置顶并保留最近五条", async () => {
    catalog(true, true);
    let fail = false;
    server.use(
      screenAiDraftHandler(() =>
        fail
          ? HttpResponse.json({ detail: "说法不够明确" }, { status: 422 })
          : HttpResponse.json({
              source_kind: "replica",
              source_identity: source.identity,
              trade_date: "2026-09-24",
              conditions: [{ key: "not_st", args: {} }],
            }),
      ),
    );
    const user = userEvent.setup();
    renderApp("/screener");
    const input = await screen.findByRole("textbox", { name: "选股描述" });
    expect(screen.getByText("最近描述")).toBeInTheDocument();
    for (const description of ["描述一", "描述二", "描述三", "描述四", "描述五", "描述六"]) {
      await user.clear(input);
      await user.type(input, description);
      await generateSuggestion(user);
      expect(await screen.findByRole("region", { name: "建议条件" })).toBeInTheDocument();
    }
    expect(JSON.parse(sessionStorage.getItem(RECENT_DESCRIPTIONS_KEY) ?? "null")).toEqual([
      "描述六",
      "描述五",
      "描述四",
      "描述三",
      "描述二",
    ]);
    fail = true;
    await user.clear(input);
    await user.type(input, "含糊描述");
    await generateSuggestion(user);
    expect(await screen.findByRole("alert")).toHaveTextContent("没能确定条件");
    expect(screen.queryByRole("button", { name: "含糊描述" })).toBeNull();
    expect(JSON.parse(sessionStorage.getItem(RECENT_DESCRIPTIONS_KEY) ?? "null")).toEqual([
      "描述六",
      "描述五",
      "描述四",
      "描述三",
      "描述二",
    ]);
    fail = false;
    await user.clear(input);
    await user.type(input, "描述三");
    await generateSuggestion(user);
    expect(await screen.findByRole("region", { name: "建议条件" })).toBeInTheDocument();
    expect(JSON.parse(sessionStorage.getItem(RECENT_DESCRIPTIONS_KEY) ?? "null")).toEqual([
      "描述三",
      "描述六",
      "描述五",
      "描述四",
      "描述二",
    ]);
    expect(screen.getAllByRole("button", { name: /^描述/ })).toHaveLength(5);
  });

  it("点击最近描述只回填和聚焦，清除旧建议但保留手工条件与真实结果，刷新后可恢复", async () => {
    catalog(true, true);
    let previews = 0;
    let runs = 0;
    server.use(
      screenAiDraftHandler(() => {
        previews += 1;
        return HttpResponse.json({
          source_kind: "replica",
          source_identity: source.identity,
          trade_date: "2026-09-24",
          conditions: [{ key: "not_st", args: {} }],
        });
      }),
      screenRunHandler(() => {
        runs += 1;
        return HttpResponse.json({
          data: {
            trade_date: "2026-09-24",
            status: "ready",
            base_count: 30,
            total: 27,
            steps: [{ label: "排除 ST", count: 27 }],
            rows: [],
            next_cursor: null,
            source,
          },
          serving,
        });
      }),
    );
    const user = userEvent.setup();
    const app = renderApp("/screener");
    await user.click(await screen.findByRole("button", { name: "运行筛选" }));
    expect(await screen.findByText("命中 27 只")).toBeInTheDocument();
    const input = screen.getByRole("textbox", { name: "选股描述" });
    await user.type(input, "排除 ST");
    await generateSuggestion(user);
    expect(await screen.findByRole("region", { name: "建议条件" })).toBeInTheDocument();
    await user.clear(input);
    await user.type(input, "排除风险股");
    await generateSuggestion(user);
    expect(await screen.findByRole("region", { name: "建议条件" })).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "排除 ST" }));
    expect(input).toHaveValue("排除 ST");
    expect(input).toHaveFocus();
    expect(screen.queryByRole("region", { name: "建议条件" })).toBeNull();
    expect(screen.getAllByRole("button", { name: /删除第/ })).toHaveLength(1);
    expect(screen.getByText("命中 27 只")).toBeInTheDocument();
    expect(previews).toBe(2);
    expect(runs).toBe(1);
    app.unmount();
    renderApp("/screener");
    expect(await screen.findByRole("button", { name: "排除风险股" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "排除 ST" })).toBeInTheDocument();
  });

  it("不可写的会话记录会阻止付费调用，并保留手动编辑", async () => {
    sessionStorage.setItem(RECENT_DESCRIPTIONS_KEY, "{损坏");
    const setItem = vi.spyOn(Storage.prototype, "setItem").mockImplementation(() => {
      throw new DOMException("blocked", "QuotaExceededError");
    });
    try {
      catalog(true, true);
      server.use(
        screenAiDraftHandler(() =>
          HttpResponse.json({
            source_kind: "replica",
            source_identity: source.identity,
            trade_date: "2026-09-24",
            conditions: [{ key: "not_st", args: {} }],
          }),
        ),
      );
      const user = userEvent.setup();
      renderApp("/screener");
      await user.type(await screen.findByRole("textbox", { name: "选股描述" }), "排除 ST");
      await generateSuggestion(user);
      expect(await screen.findByRole("alert")).toHaveTextContent("无法保存原请求");
      expect(screen.queryByRole("region", { name: "建议条件" })).toBeNull();
      expect(screen.getByRole("button", { name: "添加条件" })).toBeEnabled();
      expect(sessionStorage.getItem(RECENT_DESCRIPTIONS_KEY)).toBe("{损坏");
    } finally {
      setItem.mockRestore();
    }
  });

  it("一句话建议先预览，应用后旧结果过期，手改并运行才出现新命中", async () => {
    catalog(true, true);
    const previews: unknown[] = [];
    const runs: Schemas["ScreenRunRequest"][] = [];
    server.use(
      screenAiDraftHandler(async ({ request }) => {
        expect(request.headers.get("X-Rquant-Csrf")).toBe("1");
        previews.push(await request.json());
        return HttpResponse.json({
          source_kind: "replica",
          source_identity: source.identity,
          trade_date: "2026-09-24",
          conditions: [
            { key: "not_st", args: {} },
            { key: "circ_mv_lt", args: { threshold_yi: 80 } },
          ],
        });
      }),
      screenRunHandler(async ({ request }) => {
        const body = (await request.json()) as Schemas["ScreenRunRequest"];
        runs.push(body);
        return HttpResponse.json({
          data: {
            trade_date: body.trade_date,
            status: "ready",
            base_count: 30,
            total: runs.length === 1 ? 27 : 18,
            steps: [{ label: "排除 ST", count: runs.length === 1 ? 27 : 18 }],
            rows: [{ ts_code: "600001.SH", name: "样本01", close: 11, pct_chg: 1 }],
            next_cursor: null,
            source,
          },
          serving,
        });
      }),
    );
    const user = userEvent.setup();
    renderApp("/screener");
    await user.click(await screen.findByRole("button", { name: "运行筛选" }));
    expect(await screen.findByText("命中 27 只")).toBeInTheDocument();
    await user.type(screen.getByRole("textbox", { name: "选股描述" }), "排除 ST，市值低于 80 亿");
    await generateSuggestion(user);
    const preview = await screen.findByRole("region", { name: "建议条件" });
    expect(preview).toHaveTextContent("排除 ST");
    expect(preview).toHaveTextContent("流通市值低于");
    expect(preview).toHaveTextContent("市值上限（亿元） 80");
    expect(screen.queryByRole("spinbutton", { name: "市值上限（亿元）" })).toBeNull();
    expect(runs).toHaveLength(1);
    expect(previews).toMatchObject([
      {
        source_kind: "replica",
        source_identity: source.identity,
        trade_date: "2026-09-24",
        instruction: "排除 ST，市值低于 80 亿",
      },
    ]);
    await user.click(within(preview).getByRole("button", { name: "应用到条件" }));
    expect(screen.getByRole("spinbutton", { name: "市值上限（亿元）" })).toHaveValue(80);
    expect(screen.getByText(/条件已改，请重新运行/)).toBeInTheDocument();
    expect(runs).toHaveLength(1);
    const amount = screen.getByRole("spinbutton", { name: "市值上限（亿元）" });
    await user.clear(amount);
    await user.type(amount, "90");
    await user.click(screen.getByRole("button", { name: "运行筛选" }));
    await waitFor(() => expect(runs).toHaveLength(2));
    expect(runs[1]).toMatchObject({
      source_identity: source.identity,
      trade_date: "2026-09-24",
      conditions: [
        { key: "not_st", args: {} },
        { key: "circ_mv_lt", args: { threshold_yi: 90 } },
      ],
    });
    expect(await screen.findByText("命中 18 只")).toBeInTheDocument();
    expect(findJargon(document.body.textContent ?? "")).toEqual([]);
  });

  it("未启用生成时保留手工条件编辑", async () => {
    catalog();
    renderApp("/screener");
    expect(await screen.findByText("暂不能生成，仍可手动添加条件")).toBeInTheDocument();
    expect(screen.queryByRole("textbox", { name: "选股描述" })).toBeNull();
    expect(screen.getByRole("button", { name: "添加条件" })).toBeEnabled();
  });

  it("应用建议后可以撤销，恢复原条件和仍有效的旧结果", async () => {
    catalog(true, true);
    server.use(
      screenAiDraftHandler(() =>
        HttpResponse.json({
          source_kind: "replica",
          source_identity: source.identity,
          trade_date: "2026-09-24",
          conditions: [{ key: "circ_mv_lt", args: { threshold_yi: 80 } }],
        }),
      ),
      screenRunHandler(() =>
        HttpResponse.json({
          data: {
            trade_date: "2026-09-24",
            status: "ready",
            base_count: 30,
            total: 27,
            steps: [{ label: "排除 ST", count: 27 }],
            rows: [{ ts_code: "600001.SH", name: "样本01", close: 11, pct_chg: 1 }],
            next_cursor: null,
            source,
          },
          serving,
        }),
      ),
    );
    const user = userEvent.setup();
    renderApp("/screener");
    await user.click(await screen.findByRole("button", { name: "运行筛选" }));
    expect(await screen.findByText("命中 27 只")).toBeInTheDocument();
    await user.type(screen.getByRole("textbox", { name: "选股描述" }), "市值低于 80 亿");
    await generateSuggestion(user);
    await user.click(await screen.findByRole("button", { name: "应用到条件" }));
    expect(screen.getByRole("spinbutton", { name: "市值上限（亿元）" })).toHaveValue(80);
    expect(screen.getByText(/条件已改，请重新运行/)).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "撤销应用" }));
    expect(screen.queryByRole("spinbutton", { name: "市值上限（亿元）" })).toBeNull();
    expect(screen.queryByText(/条件已改，请重新运行/)).toBeNull();
    expect(screen.getByText("命中 27 只")).toBeInTheDocument();
  });

  it("运行中的旧请求晚于建议应用返回时仍标记结果过期", async () => {
    catalog(true, true);
    let release: (() => void) | undefined;
    const pending = new Promise<void>((resolve) => {
      release = resolve;
    });
    server.use(
      screenAiDraftHandler(() =>
        HttpResponse.json({
          source_kind: "replica",
          source_identity: source.identity,
          trade_date: "2026-09-24",
          conditions: [{ key: "not_st", args: {} }],
        }),
      ),
      screenRunHandler(async () => {
        await pending;
        return HttpResponse.json({
          data: {
            trade_date: "2026-09-24",
            status: "ready",
            base_count: 30,
            total: 27,
            steps: [{ label: "排除 ST", count: 27 }],
            rows: [],
            next_cursor: null,
            source,
          },
          serving,
        });
      }),
    );
    const user = userEvent.setup();
    renderApp("/screener");
    await user.click(await screen.findByRole("button", { name: "运行筛选" }));
    await user.type(screen.getByRole("textbox", { name: "选股描述" }), "排除 ST");
    await generateSuggestion(user);
    await user.click(await screen.findByRole("button", { name: "应用到条件" }));
    release?.();
    expect(await screen.findByText("命中 27 只")).toBeInTheDocument();
    expect(screen.getByText(/条件已改，请重新运行/)).toBeInTheDocument();
  });

  it("首次应用不误报数据更新，运行成功后可继续生成并撤销到本次条件", async () => {
    catalog(true, true);
    let previews = 0;
    let runs = 0;
    server.use(
      screenAiDraftHandler(() => {
        previews += 1;
        return HttpResponse.json({
          source_kind: "replica",
          source_identity: source.identity,
          trade_date: "2026-09-24",
          conditions: [{ key: "circ_mv_lt", args: { threshold_yi: previews === 1 ? 80 : 60 } }],
        });
      }),
      screenRunHandler(() => {
        runs += 1;
        return HttpResponse.json({
          data: {
            trade_date: "2026-09-24",
            status: "ready",
            base_count: 30,
            total: 12,
            steps: [{ label: "流通市值低于", count: 12 }],
            rows: [],
            next_cursor: null,
            source,
          },
          serving,
        });
      }),
    );
    const user = userEvent.setup();
    renderApp("/screener");
    const description = await screen.findByRole("textbox", { name: "选股描述" });
    await user.type(description, "市值低于 80 亿");
    await generateSuggestion(user);
    await user.click(await screen.findByRole("button", { name: "应用到条件" }));
    expect(screen.getByText("已加入条件，请核对后运行筛选。")).toBeInTheDocument();
    expect(screen.queryByText(/选股数据已更新，请重新筛选/)).toBeNull();
    await user.click(screen.getByRole("button", { name: "运行筛选" }));
    expect(await screen.findByText("命中 12 只")).toBeInTheDocument();
    expect(screen.getByRole("spinbutton", { name: "市值上限（亿元）" })).toHaveValue(80);
    expect(screen.queryByRole("button", { name: "撤销应用" })).toBeNull();
    await user.clear(description);
    await user.type(description, "市值低于 60 亿");
    await generateSuggestion(user);
    expect(await screen.findByRole("region", { name: "建议条件" })).toHaveTextContent(
      "市值上限（亿元） 60",
    );
    await user.click(screen.getByRole("button", { name: "应用到条件" }));
    expect(screen.getByRole("spinbutton", { name: "市值上限（亿元）" })).toHaveValue(60);
    expect(runs).toBe(1);
    await user.click(screen.getByRole("button", { name: "撤销应用" }));
    expect(screen.getByRole("spinbutton", { name: "市值上限（亿元）" })).toHaveValue(80);
    expect(screen.getByText("命中 12 只")).toBeInTheDocument();
  });

  it("手改条件会废弃迟到的生成结果，含糊描述不会改掉当前草稿", async () => {
    catalog(true, true);
    let release: (() => void) | undefined;
    const pending = new Promise<void>((resolve) => {
      release = resolve;
    });
    let calls = 0;
    server.use(
      screenAiDraftHandler(async () => {
        calls += 1;
        if (calls === 1) {
          await pending;
          return HttpResponse.json({
            source_kind: "replica",
            source_identity: source.identity,
            trade_date: "2026-09-24",
            conditions: [{ key: "circ_mv_lt", args: { threshold_yi: 80 } }],
          });
        }
        return HttpResponse.json({ detail: "内部解析细节不应展示" }, { status: 422 });
      }),
    );
    const user = userEvent.setup();
    renderApp("/screener");
    await user.type(await screen.findByRole("textbox", { name: "选股描述" }), "找一些股票");
    await generateSuggestion(user);
    expect(await screen.findByText("正在生成建议…")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "添加条件" }));
    release?.();
    await waitFor(() => expect(screen.queryByText("正在生成建议…")).toBeNull());
    expect(screen.queryByRole("region", { name: "建议条件" })).toBeNull();
    expect(sessionStorage.getItem(RECENT_DESCRIPTIONS_KEY)).toBeNull();
    expect(screen.getAllByText("排除 ST").length).toBeGreaterThanOrEqual(1);
    await generateSuggestion(user);
    expect(await screen.findByRole("alert")).toHaveTextContent(
      "没能确定条件，请说清筛选范围和数值。",
    );
    expect(screen.getAllByRole("button", { name: /删除第/ })).toHaveLength(2);
    expect(document.body).not.toHaveTextContent("内部解析细节不应展示");
    expect(sessionStorage.getItem(RECENT_DESCRIPTIONS_KEY)).toBeNull();
  });

  it("生成建议要求换日期时给出明确下一步且保留手工条件", async () => {
    catalog(true, true);
    server.use(
      screenAiDraftHandler(() =>
        HttpResponse.json({ detail: "请先选择想筛选的日期。" }, { status: 422 }),
      ),
    );
    const user = userEvent.setup();
    renderApp("/screener");
    await user.type(await screen.findByRole("textbox", { name: "选股描述" }), "筛上周的股票");
    await generateSuggestion(user);
    expect(await screen.findByRole("alert")).toHaveTextContent("请先选择想筛选的日期。");
    expect(screen.queryByRole("region", { name: "建议条件" })).toBeNull();
    expect(screen.getAllByRole("button", { name: /删除第/ })).toHaveLength(1);
  });

  it("筛选传目录身份，执行事实日期不符便保持待确认，不显示伪结果", async () => {
    let catalogReads = 0;
    server.use(
      http.get("*/api/v1/screen/blocks", () => {
        catalogReads += 1;
        return HttpResponse.json({
          data: {
            blocks,
            dates: ["2026-09-24"],
            available: true,
            ranking_metrics: [],
            source,
            source_kind: "serving",
            nl_generate_available: false,
          },
          serving,
        });
      }),
      screenRunHandler(async ({ request }) => {
        const body = (await request.json()) as Schemas["ScreenRunRequest"];
        expect(body.source_identity).toBe(source.identity);
        return HttpResponse.json({
          data: {
            trade_date: "2026-09-23",
            status: "ready",
            base_count: 30,
            total: 27,
            steps: [{ label: "排除 ST", count: 27 }],
            rows: [],
            next_cursor: null,
            source,
          },
          serving,
        });
      }),
    );
    const user = userEvent.setup();
    renderApp("/screener");
    await user.click(await screen.findByRole("button", { name: "运行筛选" }));
    expect(await screen.findByRole("status")).toHaveTextContent("结果待确认，请核对原请求。");
    expect(screen.queryByText("命中 27 只")).toBeNull();
    expect(catalogReads).toBeGreaterThanOrEqual(1);
  });

  it("建议在日期或来源变化后失效", async () => {
    let identity = source.identity;
    server.use(
      http.get("*/api/v1/screen/blocks", () =>
        HttpResponse.json({
          data: {
            blocks,
            dates: ["2026-09-24", "2026-09-23"],
            available: true,
            ranking_metrics: [],
            source: { ...source, identity },
            source_kind: "replica",
            nl_generate_available: true,
          },
          serving,
        }),
      ),
      screenAiDraftHandler(async ({ request }) => {
        const body = (await request.json()) as Schemas["ScreenNlPreviewRequest"];
        return HttpResponse.json({
          source_kind: body.source_kind,
          source_identity: body.source_identity,
          trade_date: body.trade_date,
          conditions: [{ key: "circ_mv_lt", args: { threshold_yi: 80 } }],
        });
      }),
    );
    const user = userEvent.setup();
    renderApp("/screener");
    await user.type(await screen.findByRole("textbox", { name: "选股描述" }), "市值低于 80 亿");
    await generateSuggestion(user);
    expect(await screen.findByRole("region", { name: "建议条件" })).toBeInTheDocument();
    await user.selectOptions(screen.getByRole("combobox", { name: "数据日期" }), "2026-09-23");
    expect(screen.queryByRole("region", { name: "建议条件" })).toBeNull();
    await generateSuggestion(user);
    expect(await screen.findByRole("region", { name: "建议条件" })).toBeInTheDocument();
    identity = "b".repeat(64);
    await user.click(screen.getByRole("button", { name: "刷新选股数据" }));
    await waitFor(() => expect(screen.queryByRole("region", { name: "建议条件" })).toBeNull());
    expect(screen.getByRole("button", { name: "运行筛选" })).toBeEnabled();
  });

  it("同一来源的条件目录能力改变后不能应用旧建议", async () => {
    let customRsi = true;
    const rsiBlock: Schemas["ScreenBlock"] = {
      key: "rsi_oversold",
      label: "RSI 超卖",
      hint: "筛选 RSI 较低的股票",
      category: "indicator",
      category_label: "技术指标",
      parameters: [
        {
          key: "period",
          label: "RSI 周期（日）",
          input: "integer",
          initial: 14,
          required: true,
          minimum: 2,
          maximum: 60,
          scale: 1,
          custom_ma: false,
        },
      ],
    };
    server.use(
      http.get("*/api/v1/screen/blocks", () =>
        HttpResponse.json({
          data: {
            blocks: [
              ...blocks,
              {
                ...rsiBlock,
                parameters: customRsi
                  ? rsiBlock.parameters
                  : [
                      {
                        ...rsiBlock.parameters[0],
                        input: "choice",
                        options: [{ value: "14", label: "14 日" }],
                      },
                    ],
              },
            ],
            dates: ["2026-09-24"],
            available: true,
            ranking_metrics: [],
            source,
            source_kind: "replica",
            nl_generate_available: true,
          },
          serving,
        }),
      ),
      screenAiDraftHandler(() =>
        HttpResponse.json({
          source_kind: "replica",
          source_identity: source.identity,
          trade_date: "2026-09-24",
          conditions: [{ key: "rsi_oversold", args: { period: 7 } }],
        }),
      ),
    );
    const user = userEvent.setup();
    renderApp("/screener");
    await user.type(await screen.findByRole("textbox", { name: "选股描述" }), "RSI 7 日低位");
    await generateSuggestion(user);
    expect(await screen.findByRole("region", { name: "建议条件" })).toHaveTextContent(
      "RSI 周期（日） 7",
    );
    customRsi = false;
    await user.click(screen.getByRole("button", { name: "刷新选股数据" }));
    await waitFor(() => expect(screen.queryByRole("region", { name: "建议条件" })).toBeNull());
    expect(screen.queryByRole("button", { name: "应用到条件" })).toBeNull();
    expect(screen.getAllByRole("button", { name: /删除第/ })).toHaveLength(1);
  });
  it("历史数据未发布时禁用预览，手动刷新后读取独立日期", async () => {
    catalog();
    let ready = false;
    server.use(
      http.get("*/api/v1/screen/tdx/preview/source", () =>
        HttpResponse.json({
          available: ready,
          dates: ready ? ["2026-09-23"] : [],
          source: ready ? { ...source, identity: "b".repeat(64) } : null,
        }),
      ),
    );
    const user = userEvent.setup();
    renderApp("/screener");
    await user.click(await screen.findByRole("button", { name: "导入公式" }));
    const dialog = await screen.findByRole("dialog", { name: "公式预览" });
    expect(await within(dialog).findByRole("status")).toHaveTextContent("预览数据暂不可用");
    expect(within(dialog).getByRole("button", { name: "预览这只股票" })).toBeDisabled();
    ready = true;
    await user.click(within(dialog).getByRole("button", { name: "刷新公式预览数据" }));
    await waitFor(() =>
      expect(within(dialog).getByRole("combobox", { name: "数据日期" })).toHaveValue("2026-09-23"),
    );
  });

  it.each([
    ["no_match", "不符合", null],
    ["unknown", "暂无法判断", "历史日线不完整，暂无法判断。"],
  ] as const)("公式单股预览如实展示 %s 结论", async (status, label, reason) => {
    catalog();
    server.use(
      http.post("*/api/v1/screen/tdx/parse", () =>
        HttpResponse.json({
          syntax_version: "tdx-v1",
          status: "parsed",
          capability: "parse_only",
          ast: null,
          translation: null,
          issues: [],
          unsupported: [],
        }),
      ),
      http.post("*/api/v1/screen/tdx/preview", () =>
        HttpResponse.json({
          stock_code: "600001.SH",
          trade_date: "2026-09-24",
          status,
          reason,
          source_updated_at: source.updated_at,
        }),
      ),
    );
    const user = userEvent.setup();
    renderApp("/screener");
    await user.click(await screen.findByRole("button", { name: "导入公式" }));
    const dialog = await screen.findByRole("dialog", { name: "公式预览" });
    await user.type(within(dialog).getByRole("textbox", { name: "通达信公式" }), "CLOSE>0");
    await user.type(within(dialog).getByRole("textbox", { name: "股票代码" }), "600001.SH");
    await user.click(within(dialog).getByRole("button", { name: "检查公式" }));
    await within(dialog).findByText("公式检查通过，可预览或批量运行。");
    await user.click(within(dialog).getByRole("button", { name: "预览这只股票" }));
    expect(await within(dialog).findByRole("status")).toHaveTextContent(label);
    if (reason) expect(within(dialog).getByRole("status")).toHaveTextContent(reason);
  });

  it("先检查公式再预览单股，输入和来源变化使旧结论失效", async () => {
    let identity = source.identity;
    const previews: Schemas["TdxPreviewRequest"][] = [];
    server.use(
      http.get("*/api/v1/screen/tdx/preview/source", () =>
        HttpResponse.json({
          available: true,
          dates: ["2026-09-24"],
          source: { ...source, identity },
        }),
      ),
      http.get("*/api/v1/screen/blocks", () =>
        HttpResponse.json({
          data: {
            blocks,
            dates: ["2026-09-24"],
            available: true,
            ranking_metrics: [],
            source: { ...source, identity },
            source_kind: "replica",
          },
          serving,
        }),
      ),
      http.post("*/api/v1/screen/tdx/parse", async ({ request }) => {
        const body = (await request.json()) as Schemas["TdxParseRequest"];
        if (body.source.includes("DYNAINFO")) {
          return HttpResponse.json({
            syntax_version: "tdx-v1",
            status: "rejected",
            capability: "parse_only",
            ast: null,
            translation: null,
            issues: [],
            unsupported: [{ message: "暂不支持函数「DYNAINFO」，请修改公式。" }],
          });
        }
        return HttpResponse.json({
          syntax_version: "tdx-v1",
          status: "parsed",
          capability: "parse_only",
          ast: null,
          translation: null,
          issues: [],
          unsupported: [],
        });
      }),
      http.post("*/api/v1/screen/tdx/preview", async ({ request }) => {
        previews.push((await request.json()) as Schemas["TdxPreviewRequest"]);
        return HttpResponse.json({
          stock_code: "600001.SH",
          trade_date: "2026-09-24",
          status: "match",
          reason: null,
          source_updated_at: "2026-09-24T07:30:00Z",
        });
      }),
    );
    const user = userEvent.setup();
    renderApp("/screener");
    await user.click(await screen.findByRole("button", { name: "导入公式" }));
    const dialog = await screen.findByRole("dialog", { name: "公式预览" });
    await user.type(
      within(dialog).getByRole("textbox", { name: "通达信公式" }),
      "CLOSE>MA(CLOSE,2)",
    );
    await user.type(within(dialog).getByRole("textbox", { name: "股票代码" }), "600001.SH");
    expect(within(dialog).getByRole("button", { name: "预览这只股票" })).toBeDisabled();
    await user.click(within(dialog).getByRole("button", { name: "检查公式" }));
    expect(await within(dialog).findByText("公式检查通过，可预览或批量运行。")).toBeInTheDocument();
    await user.click(within(dialog).getByRole("button", { name: "预览这只股票" }));
    expect(await within(dialog).findByText("符合")).toBeInTheDocument();
    expect(previews).toMatchObject([
      { source_identity: source.identity, trade_date: "2026-09-24" },
    ]);
    expect(dialog.textContent).not.toContain(source.identity);
    expect(findJargon(dialog.textContent ?? "")).toEqual([]);

    await user.type(within(dialog).getByRole("textbox", { name: "通达信公式" }), " AND OPEN>0");
    expect(within(dialog).getByRole("status")).toHaveTextContent("输入已改，请重新检查并预览");
    await user.clear(within(dialog).getByRole("textbox", { name: "通达信公式" }));
    await user.type(within(dialog).getByRole("textbox", { name: "通达信公式" }), "DYNAINFO(7)>0");
    await user.click(within(dialog).getByRole("button", { name: "检查公式" }));
    expect(await within(dialog).findByText(/暂不支持函数「DYNAINFO」/)).toBeInTheDocument();
    expect(within(dialog).getByRole("button", { name: "预览这只股票" })).toBeDisabled();

    await user.clear(within(dialog).getByRole("textbox", { name: "通达信公式" }));
    await user.type(within(dialog).getByRole("textbox", { name: "通达信公式" }), "CLOSE>0");
    await user.click(within(dialog).getByRole("button", { name: "检查公式" }));
    await user.click(within(dialog).getByRole("button", { name: "预览这只股票" }));
    expect(await within(dialog).findByText("符合")).toBeInTheDocument();
    identity = "b".repeat(64);
    await user.click(within(dialog).getByRole("button", { name: "刷新公式预览数据" }));
    expect(await within(dialog).findByRole("status")).toHaveTextContent(
      "公式预览数据已更新，请重新预览",
    );
    await user.keyboard("{Escape}");
    await waitFor(() => expect(screen.queryByRole("dialog", { name: "公式预览" })).toBeNull());
  });
  it("局部条件未知时在结果和逐条计数中明示未判定数量", async () => {
    catalog();
    server.use(
      screenRunHandler(() =>
        HttpResponse.json({
          data: {
            trade_date: "2026-09-24",
            status: "ready",
            base_count: 3,
            total: 0,
            unknown_count: 1,
            steps: [{ label: "排除 ST", count: 0, unknown_count: 1 }],
            rows: [],
            next_cursor: null,
            source,
          },
          serving,
        }),
      ),
    );
    const user = userEvent.setup();
    renderApp("/screener");
    await user.click(await screen.findByRole("button", { name: "运行筛选" }));
    expect(await screen.findByText(/未判定 1 只/)).toBeInTheDocument();
    expect(screen.getByRole("region", { name: "逐条命中" })).toHaveTextContent("未知 1 只");
    expect(screen.queryByText("没有命中股票")).not.toBeInTheDocument();
  });

  it("添加并编辑中文条件，运行后展示逐条命中、分页和个股详情", async () => {
    catalog();
    stockDrawer();
    const requests: Schemas["ScreenRunRequest"][] = [];
    server.use(
      screenRunHandler(async ({ request }) => {
        const body = (await request.json()) as Schemas["ScreenRunRequest"];
        requests.push(body);
        return HttpResponse.json({
          data: {
            trade_date: body.trade_date,
            status: "ready",
            base_count: 30,
            total: 18,
            steps: [
              { label: "排除 ST", count: 27 },
              { label: "流通市值低于", count: 18 },
            ],
            rows: [
              {
                ts_code: body.cursor ? "600021.SH" : "600001.SH",
                name: body.cursor ? "样本21" : "样本01",
                close: 11,
                pct_chg: 1.2,
              },
            ],
            next_cursor: body.cursor ? null : "next-page-token",
            source,
          },
          serving,
        });
      }),
    );
    const user = userEvent.setup();
    renderApp("/screener");

    expect(await screen.findByRole("heading", { level: 1, name: "选股器" })).toBeInTheDocument();
    await user.selectOptions(
      await screen.findByRole("combobox", { name: "条件目录" }),
      "circ_mv_lt",
    );
    await user.click(screen.getByRole("button", { name: "添加条件" }));
    const amount = screen.getByRole("spinbutton", { name: "市值上限（亿元）" });
    await user.clear(amount);
    await user.type(amount, "80");
    await user.click(screen.getByRole("button", { name: "运行筛选" }));

    expect(await screen.findByText("命中 18 只")).toBeInTheDocument();
    expect(screen.getByRole("region", { name: "逐条命中" })).toHaveTextContent("27");
    expect(requests[0]).toMatchObject({
      trade_date: "2026-09-24",
      conditions: [
        { key: "not_st", args: {} },
        { key: "circ_mv_lt", args: { threshold_yi: 80 } },
      ],
    });
    expect(screen.getByRole("table", { name: "选股结果" })).toHaveTextContent("样本01");
    expect(findJargon(document.body.textContent ?? "")).toEqual([]);

    await user.click(within(screen.getByRole("table", { name: "选股结果" })).getByText("样本01"));
    expect(await screen.findByRole("dialog", { name: /样本01/ })).toBeInTheDocument();
    await user.keyboard("{Escape}");
    await user.click(screen.getByRole("button", { name: "下一页" }));
    await waitFor(() => expect(requests).toHaveLength(2));
    expect(requests[1]?.cursor).toBe("next-page-token");
    expect(await screen.findByText("样本21")).toBeInTheDocument();
  });

  it("副本均线可用数字键盘填写周期，范围可查并按数值提交", async () => {
    const dynamicBlocks: Schemas["ScreenBlock"][] = [
      blocks[0] as Schemas["ScreenBlock"],
      {
        key: "above_ma",
        label: "收盘价高于均线",
        hint: "收盘价高于指定周期的均线",
        category: "indicator",
        category_label: "指标",
        parameters: [
          {
            key: "period",
            label: "均线周期（日）",
            input: "integer",
            initial: 20,
            required: true,
            minimum: 2,
            maximum: 250,
            scale: 1,
            hint: "可填 2–250 个交易日",
            custom_ma: false,
          },
          {
            key: "offset",
            label: "相对日期",
            input: "integer",
            initial: 0,
            required: false,
            minimum: 0,
            maximum: 30,
            scale: 1,
            hint: "0 为所选交易日，最多往前 30 个交易日",
            custom_ma: false,
          },
        ],
      },
      {
        key: "cross_above",
        label: "均线上穿",
        hint: "短期均线由下向上穿过长期均线",
        category: "indicator",
        category_label: "指标",
        parameters: [
          ...(["fast", "slow"] as const).map((key) => ({
            key,
            label: key === "fast" ? "快线（日）" : "慢线（日）",
            input: "integer" as const,
            initial: key === "fast" ? 5 : 20,
            required: true,
            minimum: 2,
            maximum: 250,
            scale: 1,
            hint: "可填 2–250 个交易日",
            custom_ma: false,
          })),
          {
            key: "offset",
            label: "相对日期",
            input: "integer",
            initial: 0,
            required: false,
            minimum: 0,
            maximum: 30,
            scale: 1,
            custom_ma: false,
          },
        ],
      },
    ];
    const requests: Schemas["ScreenRunRequest"][] = [];
    server.use(
      http.get("*/api/v1/screen/blocks", () =>
        HttpResponse.json({
          data: {
            blocks: dynamicBlocks,
            dates: ["2026-09-24"],
            available: true,
            ranking_metrics: [],
            source,
            source_kind: "replica",
          },
          serving,
        }),
      ),
      screenRunHandler(async ({ request }) => {
        const body = (await request.json()) as Schemas["ScreenRunRequest"];
        requests.push(body);
        return HttpResponse.json({
          data: {
            trade_date: body.trade_date,
            status: "ready",
            base_count: 3,
            total: 0,
            steps: [],
            rows: [],
            next_cursor: null,
            source,
          },
          serving,
        });
      }),
    );
    const user = userEvent.setup();
    renderApp("/screener");
    await user.selectOptions(await screen.findByRole("combobox", { name: "条件目录" }), "above_ma");
    await user.click(screen.getByRole("button", { name: "添加条件" }));
    const period = screen.getByRole("spinbutton", { name: "均线周期（日）" });
    expect(period).toHaveAttribute("min", "2");
    expect(period).toHaveAttribute("max", "250");
    expect(period).toHaveAttribute("inputmode", "numeric");
    await user.hover(
      screen.getByRole("img", { name: "均线周期（日）说明" }).parentElement as HTMLElement,
    );
    expect(await screen.findByRole("tooltip")).toHaveTextContent("2–250");
    await user.clear(period);
    await user.type(period, "7");
    await user.selectOptions(screen.getByRole("combobox", { name: "条件目录" }), "cross_above");
    await user.click(screen.getByRole("button", { name: "添加条件" }));
    const fast = screen.getByRole("spinbutton", { name: "快线（日）" });
    const slow = screen.getByRole("spinbutton", { name: "慢线（日）" });
    expect(fast).toHaveAttribute("inputmode", "numeric");
    await user.clear(fast);
    await user.type(fast, "2");
    await user.clear(slow);
    await user.type(slow, "3");
    const offsets = screen.getAllByRole("spinbutton", { name: "相对日期" });
    await user.clear(offsets[1] as HTMLElement);
    await user.type(offsets[1] as HTMLElement, "30");
    await user.click(screen.getByRole("button", { name: "运行筛选" }));
    await waitFor(() => expect(requests).toHaveLength(1));
    expect(requests[0]?.conditions).toMatchObject([
      { key: "not_st", args: {} },
      { key: "above_ma", args: { period: 7, offset: 0 } },
      { key: "cross_above", args: { fast: 2, slow: 3, offset: 30 } },
    ]);
    expect(findJargon(document.body.textContent ?? "")).toEqual([]);
  });

  it("副本比较和区间条件可选择自定义均线，且固定数字仍可填写", async () => {
    const fields = [
      { value: "CLOSE[0]", label: "收盘价" },
      { value: "MA5[0]", label: "5 日均线" },
    ];
    const customBlocks: Schemas["ScreenBlock"][] = [
      blocks[0] as Schemas["ScreenBlock"],
      {
        key: "gt",
        label: "大于",
        hint: "比较两项数据",
        category: "compare",
        category_label: "数值比较",
        parameters: (["left", "right"] as const).map((key) => ({
          key,
          label: key === "left" ? "左侧" : "右侧",
          input: "operand" as const,
          initial: key === "left" ? "CLOSE[0]" : "MA5[0]",
          required: true,
          scale: 1,
          options: fields,
          custom_ma: true,
          hint: "均线周期 2–250 日，相对日期 0–30 日",
        })),
      },
      {
        key: "between",
        label: "落在区间",
        hint: "指定数据位于上下限之间",
        category: "compare",
        category_label: "数值比较",
        parameters: [
          {
            key: "field",
            label: "比较项",
            input: "field",
            initial: "CLOSE[0]",
            required: true,
            scale: 1,
            options: fields,
            custom_ma: true,
            hint: "均线周期 2–250 日，相对日期 0–30 日",
          },
          ...(["low", "high"] as const).map((key) => ({
            key,
            label: key === "low" ? "下限" : "上限",
            input: "number" as const,
            initial: key === "low" ? 0 : 20,
            required: true,
            scale: 1,
            custom_ma: false,
          })),
        ],
      },
    ];
    const requests: Schemas["ScreenRunRequest"][] = [];
    server.use(
      http.get("*/api/v1/screen/blocks", () =>
        HttpResponse.json({
          data: {
            blocks: customBlocks,
            dates: ["2026-09-24"],
            available: true,
            ranking_metrics: [],
            source,
            source_kind: "replica",
          },
          serving,
        }),
      ),
      screenRunHandler(async ({ request }) => {
        const body = (await request.json()) as Schemas["ScreenRunRequest"];
        requests.push(body);
        return HttpResponse.json({
          data: {
            trade_date: body.trade_date,
            status: "ready",
            base_count: 3,
            total: 0,
            steps: [],
            rows: [],
            next_cursor: null,
            source,
          },
          serving,
        });
      }),
    );
    const user = userEvent.setup();
    renderApp("/screener");
    await user.selectOptions(await screen.findByRole("combobox", { name: "条件目录" }), "gt");
    await user.click(screen.getByRole("button", { name: "添加条件" }));
    await user.selectOptions(screen.getByRole("combobox", { name: "左侧" }), "__custom_ma__");
    const leftPeriod = screen.getByRole("spinbutton", { name: "左侧均线周期（日）" });
    const leftOffset = screen.getByRole("spinbutton", { name: "左侧相对日期" });
    expect(leftPeriod).toHaveAttribute("min", "2");
    expect(leftPeriod).toHaveAttribute("max", "250");
    expect(leftPeriod).toHaveAttribute("inputmode", "numeric");
    expect(leftOffset).toHaveAttribute("min", "0");
    expect(leftOffset).toHaveAttribute("max", "30");
    await user.tab();
    expect(leftPeriod).toHaveFocus();
    await user.clear(leftPeriod);
    await user.type(leftPeriod, "7");
    await user.clear(leftOffset);
    await user.type(leftOffset, "2");
    await user.selectOptions(screen.getByRole("combobox", { name: "右侧" }), "__number__");
    await user.clear(screen.getByRole("spinbutton", { name: "右侧数值" }));
    await user.type(screen.getByRole("spinbutton", { name: "右侧数值" }), "10.5");
    await user.click(screen.getByRole("button", { name: "运行筛选" }));
    await waitFor(() => expect(requests).toHaveLength(1));
    expect(requests[0]?.conditions[1]).toEqual({
      key: "gt",
      args: { left: "MA7[2]", right: 10.5 },
    });

    await user.selectOptions(screen.getByRole("combobox", { name: "右侧" }), "__custom_ma__");
    await user.clear(screen.getByRole("spinbutton", { name: "右侧均线周期（日）" }));
    await user.type(screen.getByRole("spinbutton", { name: "右侧均线周期（日）" }), "3");
    await user.clear(screen.getByRole("spinbutton", { name: "右侧相对日期" }));
    await user.type(screen.getByRole("spinbutton", { name: "右侧相对日期" }), "1");
    await user.selectOptions(screen.getByRole("combobox", { name: "条件目录" }), "between");
    await user.click(screen.getByRole("button", { name: "添加条件" }));
    await user.selectOptions(screen.getByRole("combobox", { name: "比较项" }), "__custom_ma__");
    await user.clear(screen.getByRole("spinbutton", { name: "比较项均线周期（日）" }));
    await user.type(screen.getByRole("spinbutton", { name: "比较项均线周期（日）" }), "2");
    await user.clear(screen.getByRole("spinbutton", { name: "比较项相对日期" }));
    await user.type(screen.getByRole("spinbutton", { name: "比较项相对日期" }), "30");
    await user.click(screen.getByRole("button", { name: "运行筛选" }));
    await waitFor(() => expect(requests).toHaveLength(2));
    expect(requests[1]?.conditions.slice(1)).toEqual([
      { key: "gt", args: { left: "MA7[2]", right: "MA3[1]" } },
      { key: "between", args: { field: "MA2[30]", low: 0, high: 20 } },
    ]);
    expect(document.body).not.toHaveTextContent(/MA(?:7\[2\]|3\[1\]|2\[30\])/);
    expect(findJargon(document.body.textContent ?? "")).toEqual([]);
  });

  it("就绪副本可填写 RSI 周期和偏移，并在比较项使用自定义 RSI", async () => {
    const dynamicBlocks: Schemas["ScreenBlock"][] = [
      blocks[0] as Schemas["ScreenBlock"],
      {
        key: "rsi_oversold",
        label: "RSI 超卖",
        hint: "RSI 低于指定值",
        category: "indicator",
        category_label: "指标",
        parameters: [
          {
            key: "period",
            label: "RSI 周期（日）",
            input: "integer",
            initial: 14,
            required: false,
            minimum: 2,
            maximum: 60,
            scale: 1,
            hint: "可填 2–60 个交易日",
            custom_ma: false,
          },
          {
            key: "threshold",
            label: "RSI 门槛",
            input: "number",
            initial: 30,
            required: true,
            minimum: 0,
            maximum: 100,
            scale: 1,
            custom_ma: false,
          },
          {
            key: "offset",
            label: "相对日期",
            input: "integer",
            initial: 0,
            required: false,
            minimum: 0,
            maximum: 30,
            scale: 1,
            custom_ma: false,
          },
        ],
      },
      {
        key: "gt",
        label: "大于",
        hint: "比较两项数据",
        category: "compare",
        category_label: "数值比较",
        parameters: (["left", "right"] as const).map((key) => ({
          key,
          label: key === "left" ? "左侧" : "右侧",
          input: "operand" as const,
          initial: key === "left" ? "CLOSE[0]" : "MA5[0]",
          required: true,
          scale: 1,
          options: [{ value: "CLOSE[0]", label: "收盘价" }],
          custom_ma: true,
        })),
      },
    ];
    const requests: Schemas["ScreenRunRequest"][] = [];
    server.use(
      http.get("*/api/v1/screen/blocks", () =>
        HttpResponse.json({
          data: {
            blocks: dynamicBlocks,
            dates: ["2026-09-24"],
            available: true,
            ranking_metrics: [],
            source,
            source_kind: "replica",
          },
          serving,
        }),
      ),
      screenRunHandler(async ({ request }) => {
        const body = (await request.json()) as Schemas["ScreenRunRequest"];
        requests.push(body);
        return HttpResponse.json({
          data: {
            trade_date: body.trade_date,
            status: "ready",
            base_count: 3,
            total: 1,
            steps: [],
            rows: [],
            next_cursor: null,
            source,
          },
          serving,
        });
      }),
    );
    const user = userEvent.setup();
    renderApp("/screener");
    await user.selectOptions(
      await screen.findByRole("combobox", { name: "条件目录" }),
      "rsi_oversold",
    );
    await user.click(screen.getByRole("button", { name: "添加条件" }));
    const period = screen.getByRole("spinbutton", { name: "RSI 周期（日）" });
    expect(period).toHaveAttribute("min", "2");
    expect(period).toHaveAttribute("max", "60");
    await user.clear(period);
    await user.type(period, "7");
    const offset = screen.getByRole("spinbutton", { name: "相对日期" });
    await user.clear(offset);
    await user.type(offset, "30");
    await user.selectOptions(screen.getByRole("combobox", { name: "条件目录" }), "gt");
    await user.click(screen.getByRole("button", { name: "添加条件" }));
    await user.selectOptions(screen.getByRole("combobox", { name: "左侧" }), "__custom_rsi__");
    const leftPeriod = screen.getByRole("spinbutton", { name: "左侧RSI 周期（日）" });
    await user.clear(leftPeriod);
    await user.type(leftPeriod, "7");
    await user.clear(screen.getByRole("spinbutton", { name: "左侧相对日期" }));
    await user.type(screen.getByRole("spinbutton", { name: "左侧相对日期" }), "30");
    await user.click(screen.getByRole("button", { name: "运行筛选" }));
    await waitFor(() => expect(requests).toHaveLength(1));
    expect(requests[0]?.conditions.slice(1)).toEqual([
      { key: "rsi_oversold", args: { period: 7, threshold: 30, offset: 30 } },
      { key: "gt", args: { left: "RSI7[30]", right: "MA5[0]" } },
    ]);
  });

  it("自定义 RSI 不可用后保留旧周期并阻止静默换条件", async () => {
    let ready = true;
    const base = {
      key: "period",
      label: "指标周期",
      initial: "14",
      required: false,
      scale: 1,
      custom_ma: false,
    };
    server.use(
      http.get("*/api/v1/screen/blocks", () =>
        HttpResponse.json({
          data: {
            blocks: [
              blocks[0],
              {
                key: "rsi_oversold",
                label: "RSI 超卖",
                hint: "RSI 低于指定值",
                category: "indicator",
                category_label: "指标",
                parameters: [
                  ready
                    ? {
                        ...base,
                        label: "RSI 周期（日）",
                        input: "integer",
                        initial: 14,
                        minimum: 2,
                        maximum: 60,
                      }
                    : {
                        ...base,
                        input: "choice",
                        options: [
                          { value: "6", label: "6 日 RSI" },
                          { value: "14", label: "14 日 RSI" },
                        ],
                      },
                  {
                    key: "threshold",
                    label: "RSI 门槛",
                    input: "number",
                    initial: 30,
                    required: true,
                    scale: 1,
                    custom_ma: false,
                  },
                  {
                    key: "offset",
                    label: "相对日期",
                    input: "integer",
                    initial: 0,
                    required: false,
                    minimum: 0,
                    maximum: 30,
                    scale: 1,
                    custom_ma: false,
                  },
                ],
              },
            ],
            dates: ["2026-09-24"],
            available: true,
            ranking_metrics: [],
            source,
            source_kind: "replica",
          },
          serving,
        }),
      ),
    );
    const user = userEvent.setup();
    renderApp("/screener");
    await user.selectOptions(
      await screen.findByRole("combobox", { name: "条件目录" }),
      "rsi_oversold",
    );
    await user.click(screen.getByRole("button", { name: "添加条件" }));
    const period = screen.getByRole("spinbutton", { name: "RSI 周期（日）" });
    await user.clear(period);
    await user.type(period, "7");
    ready = false;
    await user.click(screen.getByRole("button", { name: "刷新选股数据" }));
    expect(await screen.findByText("自定义 RSI 暂不可用")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "运行筛选" })).toBeDisabled();
    expect(screen.getByRole("button", { name: "运行筛选" })).toHaveAttribute(
      "aria-description",
      "原条件暂不可用，请核对后再运行。",
    );
  });

  it("条件修改后标明旧结果；无数据或不支持的条件不显示伪结果", async () => {
    catalog();
    let unsupported = false;
    server.use(
      screenRunHandler(() =>
        unsupported
          ? HttpResponse.json(
              { detail: "当前数据还不支持这个条件，请换一条或稍后重试。" },
              { status: 422 },
            )
          : HttpResponse.json({
              data: {
                trade_date: "2026-09-24",
                status: "ready",
                base_count: 30,
                total: 27,
                steps: [{ label: "排除 ST", count: 27 }],
                rows: [{ ts_code: "600001.SH", name: "样本01", close: 11, pct_chg: 1.2 }],
                next_cursor: null,
                source,
              },
              serving,
            }),
      ),
    );
    const user = userEvent.setup();
    renderApp("/screener");
    await screen.findByRole("combobox", { name: "条件目录" });
    await user.click(screen.getByRole("button", { name: "运行筛选" }));
    expect(await screen.findByText("命中 27 只")).toBeInTheDocument();

    await user.selectOptions(screen.getByRole("combobox", { name: "条件目录" }), "circ_mv_lt");
    await user.click(screen.getByRole("button", { name: "添加条件" }));
    expect(screen.getByRole("status")).toHaveTextContent("条件已改，请重新运行");
    unsupported = true;
    await user.click(screen.getByRole("button", { name: "运行筛选" }));
    expect(await screen.findByText("结果待确认，请核对原请求。")).toBeVisible();
    expect(screen.getByText("条件已改，请重新运行。旧结果仅供参考。")).toBeVisible();
  });

  it("没有已发布选股数据时解释原因并禁用运行", async () => {
    catalog(false);
    renderApp("/screener");
    expect(await screen.findByText("选股数据暂不可用")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "刷新选股数据" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "运行筛选" })).toBeDisabled();
  });

  it("独立选股数据更新后保留条件并要求重跑，不显示来源身份", async () => {
    let identity = "a".repeat(64);
    server.use(
      http.get("*/api/v1/screen/blocks", () =>
        HttpResponse.json({
          data: {
            blocks,
            dates: ["2026-09-24"],
            available: true,
            ranking_metrics: [],
            source: { ...source, identity },
            source_kind: "replica",
          },
          serving,
        }),
      ),
      screenRunHandler(() =>
        HttpResponse.json({
          data: {
            trade_date: "2026-09-24",
            status: "ready",
            base_count: 30,
            total: 27,
            steps: [{ label: "排除 ST", count: 27 }],
            rows: [{ ts_code: "600001.SH", name: "样本01", close: 11, pct_chg: 1.2 }],
            next_cursor: "next-page",
            source: { ...source, identity },
          },
          serving,
        }),
      ),
    );
    const user = userEvent.setup();
    renderApp("/screener");
    await user.click(await screen.findByRole("button", { name: "运行筛选" }));
    expect(await screen.findByText("命中 27 只")).toBeInTheDocument();
    expect(document.querySelector(".screen-source")).toHaveTextContent(/选股数据.*更新/);
    expect(document.body.textContent).not.toContain(identity);
    identity = "b".repeat(64);
    await user.click(screen.getByRole("button", { name: "刷新选股数据" }));
    expect(await screen.findByRole("status")).toHaveTextContent("选股数据已更新，请重新筛选");
    expect(screen.queryByText("命中 27 只")).not.toBeInTheDocument();
    expect(screen.queryByRole("table", { name: "选股结果" })).not.toBeInTheDocument();
    expect(screen.getByRole("combobox", { name: "条件目录" })).toHaveValue("not_st");
    expect(document.body.textContent).not.toContain(identity);
    expect(findJargon(document.body.textContent ?? "")).toEqual([]);
  });

  it("基本面字段按倍数或百分比填写，并绑定目录的数据来源", async () => {
    const financialOptions = [
      { value: "PE_TTM[0]", label: "市盈率（倍）" },
      { value: "ROE[0]", label: "净资产收益率（%）" },
    ];
    const financialBlocks: Schemas["ScreenBlock"][] = [
      blocks[0] as Schemas["ScreenBlock"],
      {
        key: "gt",
        label: "大于",
        hint: "比较两项数据",
        category: "compare",
        category_label: "数值比较",
        parameters: (["left", "right"] as const).map((key) => ({
          key,
          label: key === "left" ? "左侧" : "右侧",
          input: "operand" as const,
          initial: key === "left" ? "PE_TTM[0]" : 9,
          required: true,
          scale: 1,
          options: financialOptions,
          custom_ma: true,
        })),
      },
      {
        key: "between",
        label: "落在区间",
        hint: "指定数据位于上下限之间",
        category: "compare",
        category_label: "数值比较",
        parameters: [
          {
            key: "field",
            label: "比较项",
            input: "field",
            initial: "PE_TTM[0]",
            required: true,
            scale: 1,
            options: financialOptions,
            custom_ma: true,
          },
          ...(["low", "high"] as const).map((key) => ({
            key,
            label: key === "low" ? "下限" : "上限",
            input: "number" as const,
            initial: key === "low" ? 0 : 20,
            required: true,
            scale: 1,
            options: [],
            custom_ma: false,
          })),
        ],
      },
    ];
    const requests: Schemas["ScreenRunRequest"][] = [];
    server.use(
      http.get("*/api/v1/screen/blocks", () =>
        HttpResponse.json({
          data: {
            blocks: financialBlocks,
            dates: ["2026-09-24"],
            available: true,
            ranking_metrics: [],
            source,
            source_kind: "replica",
          },
          serving,
        }),
      ),
      screenRunHandler(async ({ request }) => {
        const body = (await request.json()) as Schemas["ScreenRunRequest"];
        requests.push(body);
        return HttpResponse.json({
          data: {
            trade_date: body.trade_date,
            status: "ready",
            base_count: 2,
            total: 1,
            unknown_count: 1,
            steps: [{ label: "大于", count: 1, unknown_count: 1 }],
            rows: [{ ts_code: "600001.SH", name: "样本01", close: 11, pct_chg: 1 }],
            next_cursor: null,
            source,
          },
          serving,
        });
      }),
    );
    const user = userEvent.setup();
    renderApp("/screener");
    await user.selectOptions(await screen.findByRole("combobox", { name: "条件目录" }), "gt");
    await user.click(screen.getByRole("button", { name: "添加条件" }));
    expect(screen.getAllByRole("option", { name: "市盈率（倍）" })).toHaveLength(2);
    expect(screen.getByRole("spinbutton", { name: "右侧数值（倍）" })).toHaveValue(9);
    await user.selectOptions(screen.getByRole("combobox", { name: "条件目录" }), "between");
    await user.click(screen.getByRole("button", { name: "添加条件" }));
    await user.selectOptions(screen.getByRole("combobox", { name: "比较项" }), "ROE[0]");
    expect(screen.getByRole("spinbutton", { name: "下限（%）" })).toHaveAttribute(
      "inputmode",
      "decimal",
    );
    expect(screen.getByRole("spinbutton", { name: "上限（%）" })).toHaveValue(20);
    await user.click(screen.getByRole("button", { name: "运行筛选" }));
    await waitFor(() => expect(requests).toHaveLength(1));
    expect(requests[0]).toMatchObject({
      source_identity: source.identity,
      conditions: [
        { key: "not_st", args: {} },
        { key: "gt", args: { left: "PE_TTM[0]", right: 9 } },
        { key: "between", args: { field: "ROE[0]", low: 0, high: 20 } },
      ],
    });
    expect(await screen.findByText(/未判定 1 只/)).toBeInTheDocument();
    expect(document.body.textContent).not.toContain(source.identity);
    expect(findJargon(document.body.textContent ?? "")).toEqual([]);
  });

  it("编辑多项排名及前 N，展示比例折算、分数、翻页和旧结果提示", async () => {
    catalog();
    const requests: Schemas["ScreenRunRequest"][] = [];
    server.use(
      screenRunHandler(async ({ request }) => {
        const body = (await request.json()) as Schemas["ScreenRunRequest"];
        requests.push(body);
        return HttpResponse.json({
          data: {
            trade_date: body.trade_date,
            status: "ready",
            base_count: 30,
            total: 27,
            ranked_count: 25,
            steps: [{ label: "排除 ST", count: 27 }],
            rows: [
              {
                ts_code: body.cursor ? "600021.SH" : "600029.SH",
                name: body.cursor ? "样本21" : "样本29",
                close: 21,
                pct_chg: 6,
                ranking_score: body.cursor ? 65 : 95,
                rank_position: body.cursor ? 21 : 1,
              },
            ],
            next_cursor: body.cursor ? null : "rank-page-token",
            source,
          },
          serving,
        });
      }),
    );
    const user = userEvent.setup();
    renderApp("/screener");
    await screen.findByRole("combobox", { name: "条件目录" });
    expect(screen.queryByRole("option", { name: "20 日涨幅" })).not.toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "添加排名" }));
    await user.selectOptions(screen.getByRole("combobox", { name: "第 1 项指标" }), "PCT_CHG[0]");
    await user.click(screen.getByRole("button", { name: "添加排名" }));
    await user.clear(screen.getByRole("spinbutton", { name: "第 1 项权重" }));
    await user.type(screen.getByRole("spinbutton", { name: "第 1 项权重" }), "60");
    await user.clear(screen.getByRole("spinbutton", { name: "第 2 项权重" }));
    await user.type(screen.getByRole("spinbutton", { name: "第 2 项权重" }), "30");
    await user.selectOptions(screen.getByRole("combobox", { name: "第 2 项方向" }), "asc");
    await user.clear(screen.getByRole("spinbutton", { name: "取前 N 只" }));
    await user.type(screen.getByRole("spinbutton", { name: "取前 N 只" }), "25");
    expect(screen.getByText(/权重合计 90%/)).toHaveTextContent("运行时按比例折算为 100%");

    await user.click(screen.getByRole("button", { name: "运行筛选" }));
    expect(await screen.findByText("命中 27 只")).toBeInTheDocument();
    expect(screen.getByText("按排名分展示前 25 只")).toBeInTheDocument();
    expect(screen.getByRole("table", { name: "选股结果" })).toHaveTextContent("95.0");
    expect(screen.getByRole("columnheader", { name: "排名分" })).toBeInTheDocument();
    expect(requests[0]?.ranking).toEqual({
      conditions: [
        { metric: "PCT_CHG[0]", ascending: false, weight: 60 },
        { metric: "CIRC_MV[0]", ascending: true, weight: 30 },
      ],
      top_n: 25,
    });
    await user.click(screen.getByRole("button", { name: "下一页" }));
    await waitFor(() => expect(requests).toHaveLength(2));
    expect(requests[1]?.cursor).toBe("rank-page-token");
    expect(await screen.findByText("样本21")).toBeInTheDocument();

    await user.clear(screen.getByRole("spinbutton", { name: "第 2 项权重" }));
    await user.type(screen.getByRole("spinbutton", { name: "第 2 项权重" }), "40");
    expect(screen.getByRole("status")).toHaveTextContent("条件已改，请重新运行");
    expect(screen.getByRole("button", { name: "下一页" })).toBeDisabled();
    await user.click(screen.getByRole("button", { name: "删除第 2 项排名" }));
    expect(screen.getByText(/权重合计 60%/)).toBeInTheDocument();
  });

  it("只从最新未过期的成功筛选保存定义，并在条件改变后要求重跑", async () => {
    catalog();
    server.use(
      http.get("*/api/v1/pools", () =>
        HttpResponse.json({
          data: {
            state: "ready",
            latest_trade_date: null,
            definitions_available: true,
            rules_available: true,
            canvases: [],
            canvases_truncated: false,
            pools: [],
            pools_truncated: false,
          },
          serving,
        }),
      ),
    );
    const commands: Record<string, unknown>[] = [];
    server.use(
      screenRunHandler(async ({ request }) => {
        const body = (await request.json()) as Schemas["ScreenRunRequest"];
        return HttpResponse.json({
          data: {
            trade_date: body.trade_date,
            status: "ready",
            base_count: 30,
            total: 27,
            steps: [{ label: "排除 ST", count: 27 }],
            rows: [],
            next_cursor: null,
            source,
          },
          serving,
        });
      }),
      http.post("*/api/v1/pools/editor/commands", async ({ request }) => {
        expect(request.headers.get("X-Rquant-Csrf")).toBe("1");
        const body = (await request.json()) as Record<string, unknown>;
        commands.push(body);
        return HttpResponse.json({
          command_id: body.command_id,
          status: "succeeded",
          message: "池子已保存",
          pool_version: "b".repeat(64),
        });
      }),
    );
    const user = userEvent.setup();
    renderApp("/screener");
    await user.click(await screen.findByRole("button", { name: "运行筛选" }));
    expect(await screen.findByText("命中 27 只")).toBeInTheDocument();
    const open = screen.getByRole("button", { name: "保存为池子" });
    expect(open).toBeEnabled();
    await user.click(open);
    const dialog = screen.getByRole("dialog", { name: "保存为池子" });
    await user.type(within(dialog).getByRole("textbox", { name: "池子名称" }), "首次观察");
    await user.click(within(dialog).getByRole("button", { name: "保存池子" }));
    await waitFor(() => expect(commands).toHaveLength(1));
    expect(commands[0]).toMatchObject({
      kind: "save_user_pool_v3",
      base_name: "首次观察",
      ranking: null,
      rule_calls: [{ name: "not_st", args: {} }],
      depends_on: null,
      delay_days: 0,
    });
    expect(within(dialog).getByText("保存请求已完成")).toBeInTheDocument();
    await user.click(within(dialog).getByRole("button", { name: "返回选股" }));
    await user.selectOptions(screen.getByRole("combobox", { name: "条件目录" }), "circ_mv_lt");
    await user.click(screen.getByRole("button", { name: "添加条件" }));
    expect(screen.getByRole("button", { name: "保存为池子" })).toBeDisabled();
    expect(screen.getByText("条件已改，请重新运行。旧结果仅供参考。")).toBeInTheDocument();
    expect(findJargon(document.body.textContent ?? "")).toEqual([]);
  });

  it("有排名时保存完整排名定义与前 N，只能提交最新运行快照", async () => {
    catalog();
    server.use(
      http.get("*/api/v1/pools", () =>
        HttpResponse.json({
          data: {
            state: "ready",
            latest_trade_date: null,
            definitions_available: true,
            rules_available: true,
            canvases: [],
            canvases_truncated: false,
            pools: [],
            pools_truncated: false,
          },
          serving,
        }),
      ),
    );
    const commands: Record<string, unknown>[] = [];
    server.use(
      screenRunHandler(async ({ request }) => {
        const body = (await request.json()) as Schemas["ScreenRunRequest"];
        return HttpResponse.json({
          data: {
            trade_date: body.trade_date,
            status: "ready",
            base_count: 30,
            total: 27,
            ranked_count: 20,
            steps: [{ label: "排除 ST", count: 27 }],
            rows: [],
            next_cursor: null,
            source,
          },
          serving,
        });
      }),
      http.post("*/api/v1/pools/editor/commands", async ({ request }) => {
        const body = (await request.json()) as Record<string, unknown>;
        commands.push(body);
        return HttpResponse.json({
          command_id: body.command_id,
          status: "succeeded",
          message: "池子已保存",
          pool_version: "b".repeat(64),
        });
      }),
    );
    const user = userEvent.setup();
    renderApp("/screener");
    await screen.findByRole("button", { name: "添加排名" });
    await user.click(screen.getByRole("button", { name: "添加排名" }));
    await user.click(screen.getByRole("button", { name: "运行筛选" }));
    expect(await screen.findByText("按排名分展示前 20 只")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "保存为池子" }));
    const dialog = screen.getByRole("dialog", { name: "保存为池子" });
    await user.type(within(dialog).getByRole("textbox", { name: "池子名称" }), "排序观察");
    await user.click(within(dialog).getByRole("button", { name: "保存池子" }));
    await waitFor(() => expect(commands).toHaveLength(1));
    expect(commands[0]).toMatchObject({
      kind: "save_user_pool_v3",
      ranking: {
        conditions: [{ metric: "CIRC_MV[0]", ascending: true, weight: 100 }],
        top_n: 20,
      },
    });
  });

  it("保存失联后刷新页面，使用同一请求继续核对", async () => {
    catalog();
    const commands: Record<string, unknown>[] = [];
    server.use(
      http.get("*/api/v1/pools", () =>
        HttpResponse.json({
          data: {
            state: "ready",
            latest_trade_date: null,
            definitions_available: true,
            rules_available: true,
            canvases: [],
            canvases_truncated: false,
            pools: [],
            pools_truncated: false,
          },
          serving,
        }),
      ),
      screenRunHandler(() =>
        HttpResponse.json({
          data: {
            trade_date: "2026-09-24",
            status: "ready",
            base_count: 30,
            total: 27,
            steps: [],
            rows: [],
            next_cursor: null,
            source,
          },
          serving,
        }),
      ),
      http.post("*/api/v1/pools/editor/commands", async ({ request }) => {
        const body = (await request.json()) as Record<string, unknown>;
        commands.push(body);
        return commands.length === 1
          ? HttpResponse.json({ detail: "连接断开" }, { status: 503 })
          : HttpResponse.json({
              command_id: body.command_id,
              status: "succeeded",
              message: "池子已保存",
              pool_version: "b".repeat(64),
            });
      }),
    );
    const user = userEvent.setup();
    const app = renderApp("/screener");
    await user.click(await screen.findByRole("button", { name: "运行筛选" }));
    await screen.findByText("命中 27 只");
    await user.click(screen.getByRole("button", { name: "保存为池子" }));
    const dialog = screen.getByRole("dialog", { name: "保存为池子" });
    await user.type(within(dialog).getByRole("textbox", { name: "池子名称" }), "续查观察");
    await user.click(within(dialog).getByRole("button", { name: "保存池子" }));
    expect(await within(dialog).findByText("保存状态待确认")).toBeInTheDocument();
    app.unmount();
    renderApp("/screener");
    await user.click(await screen.findByRole("button", { name: "查看保存进度" }));
    await user.click(
      within(screen.getByRole("dialog", { name: "保存为池子" })).getByRole("button", {
        name: "继续核对",
      }),
    );
    await waitFor(() => expect(commands).toHaveLength(2));
    expect(commands[1]).toEqual(commands[0]);
    expect(screen.getByText("保存请求已完成", { exact: true })).toBeInTheDocument();
  });

  it("同代读回后分别提示规则发布与下次选股结果", async () => {
    catalog();
    const version = "b".repeat(64);
    let editorGeneration: string | null = "c".repeat(64);
    let resultState = "not_run";
    server.use(
      screenRunHandler(() =>
        HttpResponse.json({
          data: {
            trade_date: "2026-09-24",
            status: "ready",
            base_count: 30,
            total: 27,
            steps: [],
            rows: [],
            next_cursor: null,
            source,
          },
          serving,
        }),
      ),
      http.post("*/api/v1/pools/editor/commands", async ({ request }) => {
        const body = (await request.json()) as { command_id: string };
        return HttpResponse.json({
          command_id: body.command_id,
          status: "succeeded",
          message: "池子已保存",
          pool_version: version,
        });
      }),
      http.get("*/api/v1/pools/editor", () =>
        HttpResponse.json({
          data: {
            state: "ready",
            canvas_create_available: true,
            nl_preview_available: false,
            canvases: [],
            copy_sources: [],
            pools: [
              {
                key: "user/发布观察",
                display_name: "发布观察",
                description: "",
                version,
                save_kind: "save_user_pool_v3",
                depends_on: null,
                delay_days: 0,
                rule_calls: [{ name: "not_st", args: {} }],
                include_columns: [],
                ranking: null,
              },
            ],
          },
          serving: { ...serving, generation_id: editorGeneration },
        }),
      ),
      http.get("*/api/v1/pools", () =>
        HttpResponse.json({
          data: {
            state: "ready",
            latest_trade_date: "2026-09-24",
            definitions_available: true,
            rules_available: true,
            canvases: [],
            canvases_truncated: false,
            pools_truncated: false,
            pools: [
              {
                key: "user/发布观察",
                name: "发布观察",
                state: "unpublished",
                trade_date: null,
                member_count: null,
                gain_verified_count: 0,
                gain_sample_avg_pct: null,
                steps: [],
                steps_truncated: false,
                members: [],
                members_truncated: false,
                definition: {
                  name: "发布观察",
                  state: "available",
                  status_label: "已发布",
                  reason_label: null,
                  source_label: "自建规则",
                  description: "",
                  depends_on: null,
                  delay_label: null,
                  rules: [{ label: "排除 ST", parameters: [] }],
                  ranking: null,
                },
                result: {
                  state: resultState,
                  status_label: "等待选股",
                  trade_date: resultState === "current_rules" ? "2026-09-24" : null,
                  hit_count: resultState === "current_rules" ? 27 : null,
                },
              },
            ],
          },
          serving,
        }),
      ),
    );
    server.use(
      http.get("*/api/v1/screen/query/history", () =>
        HttpResponse.json({
          available: true,
          owner_scope_tag: "1".repeat(64),
          history: { owner_scope_tag: "1".repeat(64), items: [], next_cursor: null },
          presets: [],
          daily_writer_capability: null,
          daily_run_evidence:
            resultState === "current_rules"
              ? [
                  {
                    trade_date: "2026-09-24",
                    preset_name: "user/发布观察",
                    definition_version: version,
                    result_version: "b".repeat(64),
                    source_kind: "daily_writer",
                    source_identity: "c".repeat(64),
                    content_digest: "d".repeat(64),
                    decision_at: "2026-09-24T09:00:00Z",
                    universe_count: 30,
                    hit_count: 27,
                    unknown_count: 0,
                    ranking_plan_digest: null,
                    member_rank_digest: "e".repeat(64),
                    persisted_extra_digest: "f".repeat(64),
                    writer_contract_fingerprint: "a".repeat(64),
                    evidence_version: "a".repeat(64),
                    completed_at: "2026-09-24T10:00:00Z",
                  },
                ]
              : [],
        }),
      ),
    );
    const user = userEvent.setup();
    renderApp("/screener");
    await user.click(await screen.findByRole("button", { name: "运行筛选" }));
    await screen.findByText("命中 27 只");
    await user.click(screen.getByRole("button", { name: "保存为池子" }));
    const dialog = screen.getByRole("dialog", { name: "保存为池子" });
    await user.type(within(dialog).getByRole("textbox", { name: "池子名称" }), "发布观察");
    await user.click(within(dialog).getByRole("button", { name: "保存池子" }));
    expect(await within(dialog).findByText("等待规则发布")).toBeInTheDocument();
    editorGeneration = serving.generation_id;
    await user.click(within(dialog).getByRole("button", { name: "检查更新" }));
    expect(await within(dialog).findByText("规则已发布")).toBeInTheDocument();
    expect(within(dialog).getByText("等待日终结果确认")).toBeInTheDocument();
    resultState = "current_rules";
    await user.click(within(dialog).getByRole("button", { name: "检查更新" }));
    expect(await within(dialog).findByText("结果已按新规则更新")).toBeInTheDocument();
  });

  it.each(["未安装", "同代已安装", "旧代回证"])(
    "自定义指标保存核验日终能力：%s",
    async (writerState) => {
      catalog();
      server.use(
        http.get("*/api/v1/screen/blocks", () =>
          HttpResponse.json({
            data: {
              blocks: [
                ...blocks,
                {
                  key: "rsi_oversold",
                  label: "RSI 超卖",
                  hint: "RSI 低于指定值",
                  category: "indicator",
                  category_label: "指标",
                  parameters: [
                    {
                      key: "period",
                      label: "周期",
                      input: "integer",
                      initial: 7,
                      required: true,
                      minimum: 2,
                      maximum: 60,
                      scale: 1,
                      custom_ma: false,
                    },
                  ],
                },
              ],
              dates: ["2026-09-24"],
              available: true,
              ranking_metrics: [],
              source,
              source_kind: "replica",
              nl_generate_available: false,
            },
            serving,
          }),
        ),
        screenRunHandler(() =>
          HttpResponse.json({
            data: {
              trade_date: "2026-09-24",
              status: "ready",
              base_count: 30,
              total: 4,
              steps: [],
              rows: [],
              next_cursor: null,
              source,
            },
            serving,
          }),
        ),
      );
      server.use(
        http.get("*/api/v1/screen/query/history", () =>
          HttpResponse.json({
            available: true,
            owner_scope_tag: "1".repeat(64),
            history: { owner_scope_tag: "1".repeat(64), items: [], next_cursor: null },
            presets: [],
            daily_writer_capability:
              writerState === "未安装"
                ? null
                : {
                    contract: "daily-screen-writer/v1",
                    serving_generation_id:
                      writerState === "同代已安装" ? serving.generation_id : "f".repeat(64),
                    writer_contract_fingerprint: "a".repeat(64),
                    verified_result_version: "b".repeat(64),
                    verified_evidence_version: "c".repeat(64),
                    completed_at: "2026-09-24T10:00:00Z",
                    canonical_receipt_id: "1".repeat(64),
                    canonical_generation_id: "2".repeat(64),
                    source_generation_id: "3".repeat(64),
                  },
            daily_run_evidence: [],
          }),
        ),
      );
      const user = userEvent.setup();
      renderApp("/screener");
      await user.selectOptions(
        await screen.findByRole("combobox", { name: "条件目录" }),
        "rsi_oversold",
      );
      await user.click(screen.getByRole("button", { name: "添加条件" }));
      await user.click(screen.getByRole("button", { name: "运行筛选" }));
      await screen.findByText("命中 4 只");
      if (writerState === "同代已安装")
        expect(screen.getByRole("button", { name: "保存为池子" })).toBeEnabled();
      else {
        expect(screen.getByRole("button", { name: "保存为池子" })).toBeDisabled();
        expect(screen.getByText("自定义 RSI 暂不能保存为每日池子。")).toBeInTheDocument();
      }
    },
  );
});
