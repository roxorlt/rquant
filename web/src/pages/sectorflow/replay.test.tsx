import { fireEvent, screen, waitFor, within } from "@testing-library/react";
import { renderApp } from "@/test/render";

const serving = { state: "ready", generation_id: "g", generated_at: "2026-10-21T07:00:00Z" };

function sector(name: string, net: number, pct: number, series: number[], pctSeries: number[]) {
  return {
    name,
    net,
    pct,
    amount: 1,
    limit_up: 0,
    n: 1,
    series,
    ratio: series.map(() => 0),
    pct_series: pctSeries,
  };
}

const flow = {
  date: "2026-10-21",
  time: "15:00",
  t: ["11:30", "15:00"],
  days: ["2026-10-21"],
  warming: false,
  unchanged: false,
  summary: { amount: 1e8, up: 1, down: 1, limit_up: 0, limit_down: 0 },
  sectors: [
    sector("半导体", 3, 1.2, [1, 3], [0.2, 1.2]),
    sector("银行", -2, 9.99, [-1, -2], [-4.25, 9.99]),
  ],
  leaders: [
    {
      name: "半导体",
      net: 3,
      pct: 1.2,
      limit_up: 0,
      stocks: [
        {
          code: "600519.SH",
          name: "最新茅台",
          net: 1e8,
          net_ratio: 0.01,
          pct: 1.2,
          amount: 2e8,
          pre_close: 10,
          sealed: false,
          spark: [10, 11],
        },
      ],
    },
  ],
};

const replayLeaders = [
  {
    name: "半导体",
    net: 1,
    pct: 0.2,
    limit_up: 0,
    stocks: [
      {
        code: "000001.SZ",
        name: "回放平安",
        net: 1e8,
        net_ratio: 0.02,
        pct: 0.2,
        amount: 2e8,
        pre_close: 10,
        sealed: false,
        spark: [10, 10.2],
      },
    ],
  },
];

type FetchImpl = (input: RequestInfo | URL, init?: RequestInit) => Promise<Response>;

function json(data: unknown, status = 200): Response {
  return new Response(JSON.stringify({ data, serving }), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

function install(leaders: FetchImpl): void {
  const fetchMock: FetchImpl = async (input, init) => {
    const url = new URL(input instanceof Request ? input.url : String(input), "http://test");
    const path = url.pathname;
    if (path.endsWith("/sector-flow/leaders")) return leaders(input, init);
    if (path.endsWith("/sector-flow")) return json(flow);
    if (path.endsWith("/meta"))
      return json({ app: "rQuant", version: "test", role: "admin", generation: serving });
    if (path.endsWith("/health")) return json({ services: [], freshness: [], layers: [] });
    if (path.endsWith("/alerts")) return json({ items: [] });
    return new Response("missing", { status: 404 });
  };
  vi.stubGlobal("fetch", fetchMock);
}

async function scrubToReplay(): Promise<HTMLElement> {
  const slider = await screen.findByRole("slider", { name: "回放时间" });
  fireEvent.change(slider, { target: { value: "0" } });
  return slider;
}

beforeEach(() => {
  install(async () => json(replayLeaders));
});

function outflowText(): string {
  return document.querySelector(".sf-out")?.textContent ?? "";
}

it("shows the latest outflow percent, then the replay minute's percent", async () => {
  renderApp("/sectorflow");
  await screen.findByText("最新茅台");
  expect(outflowText()).toContain("银行");
  expect(outflowText()).toContain("+9.99%");
  expect(screen.queryByText(/回放时，主图和下面各卡片/)).not.toBeInTheDocument();
  expect(document.body).not.toHaveTextContent("回放时按该分钟重新计算");

  await scrubToReplay();
  await screen.findByText("回放平安");
  expect(outflowText()).toContain("银行");
  expect(outflowText()).toContain("−4.25%");
  expect(outflowText()).not.toContain("+9.99%");
  expect(screen.getByText(/回放时，主图和下面各卡片按回放到的那一分钟算/)).toBeInTheDocument();
});

it("hides the latest leaders while the replay minute is still loading", async () => {
  install(() => new Promise(() => undefined));
  renderApp("/sectorflow");
  await screen.findByText("最新茅台");
  await scrubToReplay();
  await waitFor(() => expect(screen.queryByText("最新茅台")).not.toBeInTheDocument());
  expect(screen.queryByText("+9.99%")).not.toBeInTheDocument();
  expect(document.querySelector(".sf-chart-skel")).not.toBeNull();
  expect(document.querySelectorAll(".sf-skel-card").length).toBeGreaterThan(0);
  expect(screen.getByRole("slider", { name: "回放时间" })).toBeInTheDocument();
});

it("keeps a single retry when the replay minute fails", async () => {
  install(async () => new Response("no", { status: 503 }));
  renderApp("/sectorflow");
  await screen.findByText("最新茅台");
  await scrubToReplay();
  const alert = await screen.findByRole("alert");
  expect(alert).toHaveTextContent("11:30 的回放数据没加载出来");
  expect(alert).toHaveTextContent(
    "网络或服务暂时没响应。顶部的成交额、涨跌家数、涨停跌停数不受影响。",
  );
  expect(screen.getAllByRole("button", { name: "重试" })).toHaveLength(1);
  const misses = screen.getAllByText("回放数据没加载出来");
  expect(misses.length).toBeGreaterThanOrEqual(1);
  for (const miss of misses) {
    expect(within(miss).queryByRole("button")).not.toBeInTheDocument();
  }
  expect(screen.queryByText(/回放时，主图和下面各卡片/)).not.toBeInTheDocument();
  expect(screen.queryByText("最新茅台")).not.toBeInTheDocument();
});
