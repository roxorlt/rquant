import { act, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import tokensCss from "@/styles/tokens.css?raw";
import { ThemeProvider } from "@/theme/ThemeProvider";
import { Button } from "./Button";
import { ChangeText } from "./ChangeText";
import { ConfirmDialog } from "./ConfirmDialog";
import { KpiStrip } from "./KpiStrip";
import { ServingBanner, servingBannerMessage } from "./ServingBanner";
import { antdThemeFor } from "./theme";
import { UiProvider } from "./UiProvider";

describe("ServingBanner", () => {
  it("matches the Streamlit wording for the four states", () => {
    expect(servingBannerMessage("ready", "ok", "数据")).toBeNull();
    expect(servingBannerMessage("stale", "built_at 旧", "数据")).toEqual({
      tone: "warn",
      text: "数据已过期：built_at 旧",
    });
    expect(servingBannerMessage("degraded", "x", "数据")?.text).toBe("数据处于降级状态：x");
    expect(servingBannerMessage("unavailable", "  ", "数据")).toEqual({
      tone: "crit",
      text: "数据不可用：未提供状态详情",
    });
  });

  it("renders nothing when ready, a status when degraded and an alert when unavailable", () => {
    const { rerender, container } = render(<ServingBanner state="ready" detail="" label="数据" />);
    expect(container).toBeEmptyDOMElement();
    rerender(<ServingBanner state="degraded" detail="x" label="数据" />);
    expect(screen.getByRole("status")).toHaveAttribute("data-state", "degraded");
    rerender(<ServingBanner state="unavailable" detail="y" label="数据" />);
    expect(screen.getByRole("alert")).toHaveTextContent("数据不可用：y");
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

  it("disables a button with a visible reason", () => {
    render(<Button disabledReason="S1 批次后开放">操作记录</Button>);
    const button = screen.getByRole("button", { name: "操作记录" });
    expect(button).toBeDisabled();
    expect(button).toHaveAttribute("title", "S1 批次后开放");
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
