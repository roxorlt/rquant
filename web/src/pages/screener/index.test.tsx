import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import type { Schemas } from "@/api/client";
import { metaEnvelope } from "@/test/fixtures";
import { findJargon } from "@/test/jargon";
import { renderApp } from "@/test/render";
import { server } from "@/test/server";

const serving = metaEnvelope().serving;
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
    expect(await screen.findByText("选股数据还没有发布")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "运行筛选" })).toBeDisabled();
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
