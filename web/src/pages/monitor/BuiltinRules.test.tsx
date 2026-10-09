import { act, render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { vi } from "vitest";
import type { Schemas } from "@/api/client";
import { findJargon } from "@/test/jargon";
import { BuiltinRules } from "./BuiltinRules";

const at = "2026-09-24T02:00:00Z";
const builtins: Schemas["MonitorBuiltinStatus"][] = [
  ["pool2_levels", "档位提醒"],
  ["pool_attack", "上攻"],
  ["surge", "爆量"],
  ["pulse", "异动"],
].map(([id, label]) => ({
  builtin_id: id as Schemas["MonitorBuiltinStatus"]["builtin_id"],
  label: label ?? "",
  enabled: true,
  state: "ready",
  state_label: "正常",
  source_note: "同批原报价，阈值已核对。",
  observed_at: at,
  evaluated_at: at,
  source_valid_until: "2026-09-24T02:01:00Z",
  last_triggered_at: null,
  matched_count: 0,
  channels: ["pushdeer"],
}));

const runtime: Schemas["MonitorRuntimeData"] = {
  state: "ready",
  source_label: "已核对",
  source_note: "当前完整范围",
  observed_at: at,
  mode: "shadow",
  mode_label: "仅记录",
  applied_revision: 2,
  builtins,
  channels: [],
};
const choices: Schemas["MonitorBuiltinControlView"][] = builtins.map((item) => ({
  builtin_id: item.builtin_id,
  label: item.label,
  enabled: true,
  revision: 2,
  can_request: true,
}));

describe("原内置规则", () => {
  it("shows four original rule facts and uses only the matching control revision", async () => {
    const toggle = vi.fn();
    const user = userEvent.setup();
    render(<BuiltinRules data={runtime} choices={choices} onToggle={toggle} />);
    const section = screen.getByRole("region", { name: "内置规则" });
    expect(within(section).getAllByRole("article")).toHaveLength(4);
    expect(within(section).getAllByText("正常")).toHaveLength(4);
    await user.click(within(section).getByRole("button", { name: "暂停档位提醒" }));
    expect(toggle).toHaveBeenCalledExactlyOnceWith(choices[0]);
    expect(findJargon(document.body.textContent ?? "")).toEqual([]);
  });

  it("keeps unknown counts empty and exposes source detail through keyboard and tap", async () => {
    const user = userEvent.setup();
    const unknown = {
      ...builtins[0],
      state: "unknown" as const,
      matched_count: null,
      source_note: "上游断开，等待恢复后重新核对。",
    } as Schemas["MonitorBuiltinStatus"];
    render(<BuiltinRules data={{ ...runtime, builtins: [unknown] }} choices={[]} />);
    const card = screen.getByRole("article", { name: "档位提醒" });
    expect(card).toHaveTextContent("注意");
    expect(card).toHaveTextContent("本次触发—");
    expect(card).not.toHaveTextContent("本次触发0");
    const detail = within(card).getByRole("button", { name: "档位提醒来源说明" });
    act(() => detail.focus());
    expect(await screen.findByRole("tooltip")).toHaveTextContent("上游断开");
    await user.click(detail);
    expect(await screen.findByRole("tooltip")).toHaveTextContent("上游断开");
    expect(within(card).getByRole("button", { name: "暂停档位提醒" })).toBeDisabled();
  });

  it("keeps disabled and opening-wait states neutral", () => {
    render(
      <BuiltinRules
        data={{
          ...runtime,
          builtins: [
            {
              ...builtins[0],
              enabled: false,
              state: "disabled",
            } as Schemas["MonitorBuiltinStatus"],
            { ...builtins[1], state: "waiting" } as Schemas["MonitorBuiltinStatus"],
          ],
        }}
        choices={[]}
      />,
    );
    expect(screen.getByRole("article", { name: "档位提醒" })).toHaveTextContent("未运行");
    expect(screen.getByRole("article", { name: "上攻" })).toHaveTextContent("等待开盘");
    expect(document.querySelector('[data-state="crit"]')).toBeNull();
  });

  it("does not invent configured rules when the runtime is missing", () => {
    render(<BuiltinRules data={undefined} choices={choices} />);
    expect(screen.getByText("内置规则尚未就绪")).toBeInTheDocument();
    expect(screen.queryAllByRole("article")).toHaveLength(0);
    expect(screen.queryAllByRole("button", { name: /^暂停/ })).toHaveLength(0);
  });

  it("opens the original source detail by tap on a touch device", async () => {
    vi.spyOn(window, "matchMedia").mockImplementation((query) => ({
      matches: query === "(hover: none)",
      media: query,
      onchange: null,
      addEventListener: () => undefined,
      removeEventListener: () => undefined,
      addListener: () => undefined,
      removeListener: () => undefined,
      dispatchEvent: () => false,
    }));
    const user = userEvent.setup();
    render(<BuiltinRules data={runtime} choices={[]} />);
    await user.click(screen.getByRole("button", { name: "档位提醒来源说明" }));
    expect(await screen.findByRole("tooltip")).toHaveTextContent("同批原报价，阈值已核对");
  });
});
