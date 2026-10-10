import { act, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import tokensCss from "@/styles/tokens.css?raw";
import { ThemeProvider } from "@/theme/ThemeProvider";
import { Button } from "./Button";
import { ChangeText } from "./ChangeText";
import { ConfirmDialog } from "./ConfirmDialog";
import { KpiStrip } from "./KpiStrip";
import { ServingBanner, servingBannerMessage } from "./ServingBanner";
import { StatusBadge } from "./StatusBadge";
import { antdThemeFor } from "./theme";
import { UiProvider } from "./UiProvider";

describe("ServingBanner", () => {
  it("shows the envelope's one-sentence message, with a fallback per state", () => {
    expect(servingBannerMessage("ready", "ignored")).toBeNull();
    expect(servingBannerMessage("stale", "数据已 12 分钟没有更新。")).toEqual({
      tone: "warn",
      text: "数据已 12 分钟没有更新。",
    });
    expect(servingBannerMessage("degraded", null)?.text).toBe(
      "最新一批数据没有通过校验，暂时显示上一批。",
    );
    expect(servingBannerMessage("unavailable", "  ")).toEqual({
      tone: "crit",
      text: "暂时读不到数据，请稍后刷新。",
    });
  });

  it("renders nothing when ready, a status when stale and an alert when unavailable", () => {
    const { rerender, container } = render(<ServingBanner state="ready" message={null} />);
    expect(container).toBeEmptyDOMElement();
    rerender(<ServingBanner state="stale" message="数据已 12 分钟没有更新。" detail="x" />);
    expect(screen.getByRole("status")).toHaveAttribute("data-state", "stale");
    expect(screen.getByRole("status")).toHaveTextContent("数据已 12 分钟没有更新。详情");
    rerender(<ServingBanner state="unavailable" message="暂时读不到数据。" action="看系统健康" />);
    expect(screen.getByRole("alert")).toHaveTextContent("暂时读不到数据。看系统健康");
  });

  it("keeps the technical detail out of the text until hovered", async () => {
    const user = userEvent.setup();
    render(
      <ThemeProvider>
        <UiProvider>
          <ServingBanner state="stale" message="数据没有按时更新。" detail="built_at 900s old" />
        </UiProvider>
      </ThemeProvider>,
    );
    expect(screen.getByRole("status")).not.toHaveTextContent("built_at");
    await user.hover(screen.getByText("详情"));
    expect(await screen.findByRole("tooltip")).toHaveTextContent("built_at 900s old");
  });
});

describe("small building blocks", () => {
  it("colours changes red up and green down with an explicit sign", () => {
    render(
      <>
        <ChangeText value={2.5} />
        <ChangeText value={-1} />
        <ChangeText value={0} />
      </>,
    );
    expect(screen.getByText("+2.50%")).toHaveClass("num", "up");
    expect(screen.getByText("−1.00%")).toHaveClass("num", "down");
    expect(screen.getByText("0.00%")).not.toHaveClass("up");
  });

  it("draws the KPI strip as one labelled section", () => {
    render(
      <KpiStrip
        label="今日关键数字"
        items={[{ key: "a", label: "今日候选", value: 18, unit: "只" }]}
      />,
    );
    expect(screen.getByRole("region", { name: "今日关键数字" })).toHaveTextContent("今日候选18只");
  });

  it("disables a button and explains why in a tooltip", async () => {
    const user = userEvent.setup();
    render(
      <ThemeProvider>
        <UiProvider>
          <Button disabledReason="即将上线">操作记录</Button>
        </UiProvider>
      </ThemeProvider>,
    );
    const button = screen.getByRole("button", { name: "操作记录" });
    expect(button).toBeDisabled();
    expect(button).toHaveAccessibleDescription("即将上线");
    expect(button).not.toHaveAttribute("title");
    await user.hover(button.parentElement as HTMLElement);
    expect(await screen.findByRole("tooltip")).toHaveTextContent("即将上线");
  });

  it("draws a status as icon, colour and a short word, with the reason on hover", async () => {
    const user = userEvent.setup();
    render(
      <ThemeProvider>
        <UiProvider>
          <StatusBadge state="waiting" label="等待开盘" reason="盘中服务，休市日不运行" />
        </UiProvider>
      </ThemeProvider>,
    );
    const badge = screen.getByText("等待开盘").closest(".status");
    expect(badge).toHaveAttribute("data-state", "waiting");
    expect(badge?.querySelector("svg")).toHaveAttribute("aria-hidden", "true");
    expect(document.body).not.toHaveTextContent("休市日不运行");
    await user.hover(screen.getByText("等待开盘"));
    expect(await screen.findByRole("tooltip")).toHaveTextContent("盘中服务，休市日不运行");
  });
});

describe("ConfirmDialog", () => {
  function renderDialog(props: Partial<Parameters<typeof ConfirmDialog>[0]> = {}) {
    const onConfirm = vi.fn();
    render(
      <ThemeProvider>
        <UiProvider>
          <ConfirmDialog
            open
            level="high"
            title="执行回补"
            description="将回补 2024-09 至 2025-04 的日线。"
            confirmName="rquant-backfill"
            expiresAt={new Date(Date.now() + 120_000)}
            onConfirm={onConfirm}
            onCancel={() => undefined}
            {...props}
          />
        </UiProvider>
      </ThemeProvider>,
    );
    return { onConfirm };
  }

  it("requires the exact object name before a high-risk action", async () => {
    const user = userEvent.setup();
    const { onConfirm } = renderDialog();
    const ok = await screen.findByRole("button", { name: "确认执行" });
    expect(ok).toBeDisabled();
    await user.type(screen.getByRole("textbox"), "rquant-backfil");
    expect(ok).toBeDisabled();
    await user.type(screen.getByRole("textbox"), "l");
    expect(ok).toBeEnabled();
    await user.click(ok);
    expect(onConfirm).toHaveBeenCalledOnce();
  });

  it("refuses once the confirmation token has expired", async () => {
    renderDialog({ expiresAt: new Date(Date.now() - 1000) });
    expect(await screen.findByRole("alert")).toHaveTextContent("确认已过期");
    expect(screen.getByRole("button", { name: "确认执行" })).toBeDisabled();
  });

  it("asks for one click on a heavy action", async () => {
    const user = userEvent.setup();
    const { onConfirm } = renderDialog({
      level: "heavy",
      confirmName: undefined,
      expiresAt: undefined,
    });
    expect(screen.queryByRole("textbox")).not.toBeInTheDocument();
    await act(async () => {
      await user.click(await screen.findByRole("button", { name: "确认执行" }));
    });
    expect(onConfirm).toHaveBeenCalledOnce();
  });
});

describe("antd theme", () => {
  it("takes its seed colours from the CSS variables and follows dark mode", () => {
    const style = document.createElement("style");
    style.textContent = tokensCss;
    document.head.append(style);
    expect(antdThemeFor("light").token).toMatchObject({
      colorPrimary: "#2e58e6",
      colorError: "#d6333b",
    });
    document.documentElement.setAttribute("data-theme", "dark");
    expect(antdThemeFor("dark").token).toMatchObject({
      colorPrimary: "#7090ff",
      colorBgContainer: "#161a21",
    });
    style.remove();
  });

  it("leaves undefined variables to antd's defaults", () => {
    const token = antdThemeFor("light", () => "").token ?? {};
    expect(token).not.toHaveProperty("colorPrimary");
    expect(token).toMatchObject({ borderRadius: 6, controlHeight: 32 });
  });
});
