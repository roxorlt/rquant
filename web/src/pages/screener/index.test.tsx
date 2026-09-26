import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import type { Schemas } from "@/api/client";
import { metaEnvelope } from "@/test/fixtures";
import { findJargon } from "@/test/jargon";
import { renderApp } from "@/test/render";
import { server } from "@/test/server";

const serving = metaEnvelope().serving;
const source = { identity: "a".repeat(64), updated_at: "2026-09-24T07:30:00Z" };
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
      },
    ],
  },
];

function catalog(available = true) {
  server.use(
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
    await within(dialog).findByText("公式可以预览这只股票。");
    await user.click(within(dialog).getByRole("button", { name: "预览这只股票" }));
    expect(await within(dialog).findByRole("status")).toHaveTextContent(label);
    if (reason) expect(within(dialog).getByRole("status")).toHaveTextContent(reason);
  });

  it("先检查公式再预览单股，输入和来源变化使旧结论失效", async () => {
    let identity = source.identity;
    const previews: Schemas["TdxPreviewRequest"][] = [];
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
    expect(await within(dialog).findByText("公式可以预览这只股票。")).toBeInTheDocument();
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
    await user.click(within(dialog).getByRole("button", { name: "刷新选股数据" }));
    expect(await within(dialog).findByRole("status")).toHaveTextContent(
      "选股数据已更新，请重新预览",
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
    expect(screen.getByRole("button", { name: "下一页" })).toBeDisabled();
    expect(screen.getByRole("combobox", { name: "条件目录" })).toHaveValue("not_st");
    expect(document.body.textContent).not.toContain(identity);
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
});
