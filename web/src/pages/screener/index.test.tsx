import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import { vi } from "vitest";
import type { Schemas } from "@/api/client";
import { metaEnvelope } from "@/test/fixtures";
import { findJargon } from "@/test/jargon";
import { renderApp } from "@/test/render";
import { server } from "@/test/server";

const serving = metaEnvelope().serving;
const source = { identity: "a".repeat(64), updated_at: "2026-09-24T07:30:00Z" };
const RECENT_DESCRIPTIONS_KEY = "rquant.screen.recent-descriptions.v1";
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
  it("最近描述只记校验成功的预览，去重置顶并保留最近五条", async () => {
    catalog(true, true);
    let fail = false;
    server.use(
      http.post("*/api/v1/screen/nl-preview", () =>
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
      await user.click(screen.getByRole("button", { name: "生成条件" }));
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
    await user.click(screen.getByRole("button", { name: "生成条件" }));
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
    await user.click(screen.getByRole("button", { name: "生成条件" }));
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
      http.post("*/api/v1/screen/nl-preview", () => {
        previews += 1;
        return HttpResponse.json({
          source_kind: "replica",
          source_identity: source.identity,
          trade_date: "2026-09-24",
          conditions: [{ key: "not_st", args: {} }],
        });
      }),
      http.post("*/api/v1/screen/run", () => {
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
    await user.click(screen.getByRole("button", { name: "生成条件" }));
    expect(await screen.findByRole("region", { name: "建议条件" })).toBeInTheDocument();
    await user.clear(input);
    await user.type(input, "排除风险股");
    await user.click(screen.getByRole("button", { name: "生成条件" }));
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

  it("损坏或不可写的会话记录不影响生成条件", async () => {
    sessionStorage.setItem(RECENT_DESCRIPTIONS_KEY, "{损坏");
    const setItem = vi.spyOn(Storage.prototype, "setItem").mockImplementation(() => {
      throw new DOMException("blocked", "QuotaExceededError");
    });
    try {
      catalog(true, true);
      server.use(
        http.post("*/api/v1/screen/nl-preview", () =>
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
      await user.click(screen.getByRole("button", { name: "生成条件" }));
      expect(await screen.findByRole("region", { name: "建议条件" })).toBeInTheDocument();
      expect(screen.getByRole("button", { name: "排除 ST" })).toBeInTheDocument();
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
      http.post("*/api/v1/screen/nl-preview", async ({ request }) => {
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
      http.post("*/api/v1/screen/run", async ({ request }) => {
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
    await user.click(screen.getByRole("button", { name: "生成条件" }));
    const preview = await screen.findByRole("region", { name: "建议条件" });
    expect(preview).toHaveTextContent("排除 ST");
    expect(preview).toHaveTextContent("流通市值低于");
    expect(preview).toHaveTextContent("市值上限（亿元） 80");
    expect(screen.queryByRole("spinbutton", { name: "市值上限（亿元）" })).toBeNull();
    expect(runs).toHaveLength(1);
    expect(previews).toEqual([
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
      http.post("*/api/v1/screen/nl-preview", () =>
        HttpResponse.json({
          source_kind: "replica",
          source_identity: source.identity,
          trade_date: "2026-09-24",
          conditions: [{ key: "circ_mv_lt", args: { threshold_yi: 80 } }],
        }),
      ),
      http.post("*/api/v1/screen/run", () =>
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
    await user.click(screen.getByRole("button", { name: "生成条件" }));
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
      http.post("*/api/v1/screen/nl-preview", () =>
        HttpResponse.json({
          source_kind: "replica",
          source_identity: source.identity,
          trade_date: "2026-09-24",
          conditions: [{ key: "not_st", args: {} }],
        }),
      ),
      http.post("*/api/v1/screen/run", async () => {
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
    await user.click(screen.getByRole("button", { name: "生成条件" }));
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
      http.post("*/api/v1/screen/nl-preview", () => {
        previews += 1;
        return HttpResponse.json({
          source_kind: "replica",
          source_identity: source.identity,
          trade_date: "2026-09-24",
          conditions: [{ key: "circ_mv_lt", args: { threshold_yi: previews === 1 ? 80 : 60 } }],
        });
      }),
      http.post("*/api/v1/screen/run", () => {
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
    await user.click(screen.getByRole("button", { name: "生成条件" }));
    await user.click(await screen.findByRole("button", { name: "应用到条件" }));
    expect(screen.getByText("已加入条件，请核对后运行筛选。")).toBeInTheDocument();
    expect(screen.queryByText(/选股数据已更新，请重新筛选/)).toBeNull();
    await user.click(screen.getByRole("button", { name: "运行筛选" }));
    expect(await screen.findByText("命中 12 只")).toBeInTheDocument();
    expect(screen.getByRole("spinbutton", { name: "市值上限（亿元）" })).toHaveValue(80);
    expect(screen.queryByRole("button", { name: "撤销应用" })).toBeNull();
    await user.clear(description);
    await user.type(description, "市值低于 60 亿");
    await user.click(screen.getByRole("button", { name: "生成条件" }));
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
      http.post("*/api/v1/screen/nl-preview", async () => {
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
    await user.click(screen.getByRole("button", { name: "生成条件" }));
    expect(await screen.findByText("正在生成条件…")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "添加条件" }));
    release?.();
    await waitFor(() => expect(screen.queryByText("正在生成条件…")).toBeNull());
    expect(screen.queryByRole("region", { name: "建议条件" })).toBeNull();
    expect(sessionStorage.getItem(RECENT_DESCRIPTIONS_KEY)).toBeNull();
    expect(screen.getAllByText("排除 ST").length).toBeGreaterThanOrEqual(1);
    await user.click(screen.getByRole("button", { name: "生成条件" }));
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
      http.post("*/api/v1/screen/nl-preview", () =>
        HttpResponse.json({ detail: "请先选择想筛选的日期。" }, { status: 422 }),
      ),
    );
    const user = userEvent.setup();
    renderApp("/screener");
    await user.type(await screen.findByRole("textbox", { name: "选股描述" }), "筛上周的股票");
    await user.click(screen.getByRole("button", { name: "生成条件" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("请先选择想筛选的日期。");
    expect(screen.queryByRole("region", { name: "建议条件" })).toBeNull();
    expect(screen.getAllByRole("button", { name: /删除第/ })).toHaveLength(1);
  });

  it("Serving 筛选传目录身份，响应身份或日期不符便清旧结果并刷新目录", async () => {
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
      http.post("*/api/v1/screen/run", async ({ request }) => {
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
    expect(await screen.findByRole("status")).toHaveTextContent("选股数据已更新，请重新筛选。");
    expect(screen.queryByText("命中 27 只")).toBeNull();
    await waitFor(() => expect(catalogReads).toBeGreaterThanOrEqual(2));
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
      http.post("*/api/v1/screen/nl-preview", async ({ request }) => {
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
    await user.click(screen.getByRole("button", { name: "生成条件" }));
    expect(await screen.findByRole("region", { name: "建议条件" })).toBeInTheDocument();
    await user.selectOptions(screen.getByRole("combobox", { name: "数据日期" }), "2026-09-23");
    expect(screen.queryByRole("region", { name: "建议条件" })).toBeNull();
    await user.click(screen.getByRole("button", { name: "生成条件" }));
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
      http.post("*/api/v1/screen/nl-preview", () =>
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
    await user.click(screen.getByRole("button", { name: "生成条件" }));
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
      http.post("*/api/v1/screen/run", () =>
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
      http.post("*/api/v1/screen/run", async ({ request }) => {
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
    await user.selectOptions(screen.getByRole("combobox", { name: "条件目录" }), "circ_mv_lt");
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
      http.post("*/api/v1/screen/run", async ({ request }) => {
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
      http.post("*/api/v1/screen/run", async ({ request }) => {
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
      http.post("*/api/v1/screen/run", async ({ request }) => {
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

  it("自定义 RSI 不可用后将旧周期恢复为目录选项", async () => {
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
    expect(screen.getByRole("combobox", { name: "指标周期" })).toHaveValue("14");
  });

  it("条件修改后标明旧结果；无数据或不支持的条件不显示伪结果", async () => {
    catalog();
    let unsupported = false;
    server.use(
      http.post("*/api/v1/screen/run", () =>
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
    expect(await screen.findByRole("alert")).toHaveTextContent("当前数据还不支持这个条件");
    expect(screen.getByRole("status")).toHaveTextContent("条件已改，请重新运行");
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
      http.post("*/api/v1/screen/run", () =>
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
      http.post("*/api/v1/screen/run", async ({ request }) => {
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
      http.post("*/api/v1/screen/run", async ({ request }) => {
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
      http.post("*/api/v1/screen/run", async ({ request }) => {
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
      http.post("*/api/v1/screen/run", async ({ request }) => {
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
      http.post("*/api/v1/screen/run", () =>
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
      http.post("*/api/v1/screen/run", () =>
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
                  trade_date: null,
                  hit_count: null,
                },
              },
            ],
          },
          serving,
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
    expect(within(dialog).getByText("等待下次选股结果")).toBeInTheDocument();
    resultState = "current_rules";
    await user.click(within(dialog).getByRole("button", { name: "检查更新" }));
    expect(await within(dialog).findByText("结果已按新规则更新")).toBeInTheDocument();
  });

  it("自定义指标虽能预览，也不允许保存成无法每日重算的池子", async () => {
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
      http.post("*/api/v1/screen/run", () =>
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
    const user = userEvent.setup();
    renderApp("/screener");
    await user.selectOptions(
      await screen.findByRole("combobox", { name: "条件目录" }),
      "rsi_oversold",
    );
    await user.click(screen.getByRole("button", { name: "添加条件" }));
    await user.click(screen.getByRole("button", { name: "运行筛选" }));
    await screen.findByText("命中 4 只");
    expect(screen.getByRole("button", { name: "保存为池子" })).toBeDisabled();
    expect(screen.getByText("自定义 RSI 暂不能保存为每日池子。")).toBeInTheDocument();
  });
});
