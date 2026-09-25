import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { metaEnvelope } from "@/test/fixtures";
import { findJargon } from "@/test/jargon";
import { renderApp } from "@/test/render";
import { metaHandler, server } from "@/test/server";
import { NAV_GROUPS, PAGES } from "./pages";

describe("app shell", () => {
  it("renders the top bar, the grouped rail and the first page", async () => {
    renderApp("/overview");

    expect(screen.getByRole("link", { name: "rQuant 投研" })).toHaveAttribute("href", "/overview");
    const rail = screen.getByRole("navigation", { name: "主导航" });
    const groups = within(rail).getAllByRole("group");
    expect(groups.map((group) => group.getAttribute("aria-label"))).toEqual([...NAV_GROUPS]);
    const links = within(rail).getAllByRole("link");
    expect(links.map((link) => link.getAttribute("href"))).toEqual(PAGES.map((page) => page.path));
    expect(await screen.findByRole("heading", { level: 1, name: "总览" })).toBeInTheDocument();
    expect(within(rail).getByRole("link", { name: "总览" })).toHaveAttribute(
      "aria-current",
      "page",
    );
    await waitFor(() => expect(document.title).toBe("总览 · rQuant 投研"));
  });

  it.each(PAGES.filter((page) => !page.ready).map((page) => [page.path, page.title] as const))(
    "route %s shows a clean 即将上线 card for %s",
    async (path, title) => {
      renderApp(path);
      expect(await screen.findByRole("heading", { level: 1, name: title })).toBeInTheDocument();
      const card = screen.getByRole("region", { name: "即将上线" });
      expect(card).toHaveTextContent(PAGES.find((page) => page.path === path)?.summary ?? "");
      expect(findJargon(document.body.textContent ?? "")).toEqual([]);
    },
  );

  it("redirects / to the overview and shows a not-found page for unknown paths", async () => {
    const { router } = renderApp("/");
    expect(await screen.findByRole("heading", { level: 1, name: "总览" })).toBeInTheDocument();
    expect(router.state.location.pathname).toBe("/overview");

    renderApp("/no-such-page");
    expect(
      await screen.findByRole("heading", { level: 1, name: "没有这个页面" }),
    ).toBeInTheDocument();
  });

  it("shows the data chip in plain words and keeps the version in its tooltip", async () => {
    const user = userEvent.setup();
    renderApp("/overview");
    const chip = await screen.findByText("数据 3 分钟前更新");
    expect(chip.closest(".gen-tag")).toHaveAttribute("data-state", "ready");
    expect(screen.queryByText(/a1b2c3d4/)).not.toBeInTheDocument();
    expect(screen.getByText("连续竞价").closest(".phase")).toHaveTextContent("市场阶段：连续竞价");
    expect(screen.getByText("2026-09-24")).toBeInTheDocument();
    expect(document.querySelector(".banner")).toBeNull();
    await user.hover(chip);
    const tip = await screen.findByRole("tooltip");
    expect(tip).toHaveTextContent("数据版本a1b2c3d4e5f6");
    expect(tip).toHaveTextContent("2026-09-24 15:31:00");
  });

  it("calls a holiday 休市 from the calendar and names the next trading day", async () => {
    const holiday = metaEnvelope({
      phase: "non_trading_day",
      phaseLabel: "休市",
      isTradingDay: false,
    });
    holiday.data.market = {
      ...holiday.data.market,
      trade_date: "2026-09-25",
      previous_trading_day: "2026-09-24",
      next_trading_day: "2026-09-28",
    };
    server.use(metaHandler(holiday));
    renderApp("/overview");
    expect(await screen.findByText("休市")).toBeInTheDocument();
    expect(screen.getByText("2026-09-25")).toBeInTheDocument();
    expect(screen.getByText("下一交易日 09-28 周一")).toBeInTheDocument();
    const topbar = document.querySelector(".topbar") as HTMLElement;
    expect(topbar).not.toHaveTextContent("· 交易日");
    expect(topbar).toHaveTextContent("2026-09-25 周五");
  });

  it("shows the banner's one sentence and a way to 系统健康 when the data is stale", async () => {
    const stale = metaEnvelope({ state: "stale", detail: "serving generation stale: 900s" });
    stale.serving.message = "数据已 15 分钟没有更新，页面上的数字可能不是最新的。";
    server.use(metaHandler(stale));
    renderApp("/overview");
    const text = await screen.findByText("数据已 15 分钟没有更新，页面上的数字可能不是最新的。");
    const banner = text.closest(".banner") as HTMLElement;
    expect(banner).toHaveAttribute("role", "status");
    expect(banner).not.toHaveTextContent("serving");
    expect(within(banner).getByRole("link", { name: "看系统健康" })).toHaveAttribute(
      "href",
      "/health",
    );
  });

  it("shows no banner for degraded datasets inside a fresh generation", async () => {
    server.use(metaHandler(metaEnvelope({ state: "ready" })));
    renderApp("/health");
    expect(await screen.findByRole("heading", { level: 1, name: "系统健康" })).toBeInTheDocument();
    expect(document.querySelector(".banner")).toBeNull();
  });

  it("says the API is unreachable instead of showing a quiet page", async () => {
    const { http, HttpResponse } = await import("msw");
    server.use(http.get("*/api/v1/meta", () => HttpResponse.json({}, { status: 502 })));
    renderApp("/datacenter");
    expect(await screen.findByRole("alert")).toHaveTextContent("连不上网页接口，请稍后刷新页面。");
    expect(screen.getByText("数据 未连接")).toBeInTheDocument();
  });
});

describe("theme", () => {
  it("cycles system → light → dark → system and remembers the choice", async () => {
    const user = userEvent.setup();
    renderApp("/overview");
    const root = document.documentElement;
    const button = screen.getByRole("button", { name: "主题：跟随系统，点击切换" });
    expect(root).not.toHaveAttribute("data-theme");

    await user.click(button);
    expect(root).toHaveAttribute("data-theme", "light");
    expect(window.localStorage.getItem("rq.theme")).toBe("light");
    await user.click(screen.getByRole("button", { name: "主题：浅色，点击切换" }));
    expect(root).toHaveAttribute("data-theme", "dark");
    expect(window.localStorage.getItem("rq.theme")).toBe("dark");
    await user.click(screen.getByRole("button", { name: "主题：深色，点击切换" }));
    expect(root).not.toHaveAttribute("data-theme");
    expect(window.localStorage.getItem("rq.theme")).toBeNull();
  });

  it("restores a stored theme on load", () => {
    window.localStorage.setItem("rq.theme", "dark");
    renderApp("/overview");
    expect(document.documentElement).toHaveAttribute("data-theme", "dark");
    expect(screen.getByRole("button", { name: "主题：深色，点击切换" })).toBeInTheDocument();
  });

  it("can be picked from the 我的 menu, which also leads to the reports", async () => {
    const user = userEvent.setup();
    const { router } = renderApp("/overview");
    await user.click(screen.getByRole("button", { name: "我的" }));
    await user.click(await screen.findByText("深色"));
    expect(document.documentElement).toHaveAttribute("data-theme", "dark");

    await user.click(screen.getByRole("button", { name: "我的" }));
    expect(await screen.findByText("当前用户：tester")).toBeInTheDocument();
    await user.click(screen.getByText("报告"));
    await waitFor(() => expect(router.state.location.pathname).toBe("/reports"));
    expect(await screen.findByRole("heading", { level: 1, name: "报告" })).toBeInTheDocument();
  });
});

describe("navigation", () => {
  it("collapses the rail and keeps it collapsed after a reload", async () => {
    const user = userEvent.setup();
    const first = renderApp("/overview");
    const collapse = screen.getByRole("button", { name: "收起导航" });
    await user.click(collapse);
    expect(document.querySelector(".app")).toHaveClass("rail-min");
    expect(screen.getByRole("button", { name: "展开导航" })).toHaveAttribute(
      "aria-expanded",
      "false",
    );
    expect(window.localStorage.getItem("rq.rail")).toBe("min");
    first.unmount();

    renderApp("/overview");
    expect(document.querySelector(".app")).toHaveClass("rail-min");
    await user.click(screen.getByRole("button", { name: "展开导航" }));
    expect(document.querySelector(".app")).not.toHaveClass("rail-min");
    expect(window.localStorage.getItem("rq.rail")).toBe("full");
  });

  it("puts the most used pages in the phone tab bar", async () => {
    renderApp("/overview");
    const bar = screen.getByRole("navigation", { name: "常用页面" });
    expect(
      within(bar)
        .getAllByRole("link")
        .map((link) => link.textContent),
    ).toEqual(["总览", "盯盘", "全景", "健康"]);
    expect(within(bar).getByRole("link", { name: "总览" })).toHaveAttribute("aria-current", "page");
  });

  it("opens the phone navigation sheet and closes it after choosing a page", async () => {
    const user = userEvent.setup();
    const { router } = renderApp("/overview");
    const opener = screen.getByRole("button", { name: "更多页面" });
    await user.click(opener);
    const sheet = await screen.findByRole("navigation", { name: "页面导航" });
    expect(opener).toHaveAttribute("aria-expanded", "true");
    await user.click(within(sheet).getByRole("link", { name: "市场全景" }));
    await waitFor(() => expect(router.state.location.pathname).toBe("/panorama"));
    expect(await screen.findByRole("heading", { level: 1, name: "市场全景" })).toBeInTheDocument();
    await waitFor(() => expect(opener).toHaveAttribute("aria-expanded", "false"));
  });
});

describe("reports and licences", () => {
  it("lists the two reports and filters the gap table", async () => {
    const user = userEvent.setup();
    renderApp("/reports");
    expect(await screen.findByRole("link", { name: "差距总览" })).toBeInTheDocument();
    expect(
      screen.getByRole("link", { name: "量化投研平台调研，以及 rQuant 还差什么" }),
    ).toBeInTheDocument();

    await user.click(screen.getByRole("link", { name: "差距总览" }));
    const table = await screen.findByRole("table", { name: "差距总览" });
    expect(within(table).getAllByRole("row")).toHaveLength(17);
    await user.click(screen.getByRole("button", { name: "缺" }));
    expect(within(table).getAllByRole("row")).toHaveLength(3);
    expect(within(table).getByText("研究环境")).toBeInTheDocument();
    expect(within(table).getByText("组合与风控")).toBeInTheDocument();
  });

  it("shows the research snapshot in a sandboxed same-origin frame", async () => {
    renderApp("/reports/2026-09-24-quant-platform-research");
    const frame = await screen.findByTitle("量化投研平台调研，以及 rQuant 还差什么");
    expect(frame).toHaveAttribute("src", "reports/2026-09-24-quant-platform-research.html");
    expect(frame.getAttribute("sandbox")).not.toContain("allow-scripts");
  });

  it("lists bundled licences with their versions", async () => {
    renderApp("/licenses");
    const table = await screen.findByRole("table", { name: "开源许可" });
    expect(within(table).getByText("lightweight-charts")).toBeInTheDocument();
    expect(within(table).getByText("5.2.1")).toBeInTheDocument();
    expect(within(table).getByText("OFL-1.1")).toBeInTheDocument();
  });
});
