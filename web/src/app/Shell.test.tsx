import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { metaEnvelope } from "@/test/fixtures";
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
    expect(screen.getByText("M1 开发中")).toBeInTheDocument();
    await waitFor(() => expect(document.title).toBe("总览 · rQuant 投研"));
  });

  it.each(PAGES.map((page) => [page.path, page.title, page.schedule] as const))(
    "route %s renders the %s placeholder",
    async (path, title, schedule) => {
      renderApp(path);
      expect(await screen.findByRole("heading", { level: 1, name: title })).toBeInTheDocument();
      expect(screen.getByText(schedule)).toBeInTheDocument();
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

  it("shows the generation marker, market phase and viewer from /api/v1/meta", async () => {
    renderApp("/overview");
    expect(await screen.findByText("a1b2c3d4")).toBeInTheDocument();
    expect(screen.getByText("· 3 分钟前")).toBeInTheDocument();
    expect(screen.getByText("· 正常")).toBeInTheDocument();
    expect(screen.getByText("连续竞价").closest(".phase")).toHaveTextContent("市场阶段：连续竞价");
    expect(screen.getByText("2026-09-24")).toBeInTheDocument();
    expect(screen.queryByRole("status")).not.toBeInTheDocument();
  });

  it("shows the serving banner when the generation is degraded", async () => {
    server.use(
      metaHandler(metaEnvelope({ state: "degraded", detail: "serving generation degraded: x" })),
    );
    renderApp("/health");
    expect(await screen.findByRole("status")).toHaveTextContent(
      "运行时数据处于降级状态：serving generation degraded: x",
    );
  });

  it("says the API is unreachable instead of showing a quiet page", async () => {
    const { http, HttpResponse } = await import("msw");
    server.use(http.get("*/api/v1/meta", () => HttpResponse.json({}, { status: 502 })));
    renderApp("/overview");
    expect(await screen.findByRole("alert")).toHaveTextContent(
      "网页接口不可用：网页 API 返回 HTTP 502",
    );
    expect(screen.getByText("数据代 未连接")).toBeInTheDocument();
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

  it("opens the phone navigation sheet and closes it after choosing a page", async () => {
    const user = userEvent.setup();
    const { router } = renderApp("/overview");
    const opener = screen.getByRole("button", { name: "打开导航" });
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
