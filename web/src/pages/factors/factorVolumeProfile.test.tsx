import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useState } from "react";
import { findJargon } from "@/test/jargon";
import { FactorDailyFeatures } from "./FactorDailyFeatures";
import { FactorEditor, type FactorEditorDraft } from "./FactorEditor";
import {
  mixedVolumeProfileResearch,
  nullVolumeProfileResearch,
  volumeProfileCapability,
  volumeProfileResearch,
} from "./factorVolumeProfile.fixture";

async function tip(label: string, content: string) {
  const anchor = screen.getByText(label, { exact: true }).closest<HTMLElement>(".tip-anchor");
  if (!anchor) throw new Error("Tip anchor required");
  act(() => anchor.focus());
  return waitFor(() => {
    const target = screen
      .getAllByRole("tooltip")
      .find((item) => item.textContent?.includes(content));
    expect(target).toBeDefined();
    return target;
  });
}

it("成交分布原值保留元、股和百分比，覆盖明确按实际日数", async () => {
  const { container } = render(<FactorDailyFeatures research={volumeProfileResearch} />);
  expect(screen.getByRole("heading", { name: "90日成交分布" })).toBeInTheDocument();
  expect(container.textContent).not.toMatch(/vp90_|09:25|SSE|原始首次观察/);
  expect(
    findJargon(
      [...container.querySelectorAll("h3,summary,label,th,td")]
        .map((node) => node.textContent)
        .join(" "),
    ),
  ).toEqual([]);
  expect(await tip("成交分布口径", "90个实际日线日期")).toHaveTextContent("历史回顾");
  await userEvent.click(screen.getByText("成交分布示例（10只）"));
  const table = screen.getByRole("table", { name: "成交分布原值示例" });
  const field = screen.getByRole("combobox", { name: "成交分布字段" });
  await userEvent.selectOptions(field, "vp90_total_amount");
  expect(table).toHaveTextContent("10,000.00");
  await userEvent.selectOptions(field, "vp90_total_vol");
  expect(table).toHaveTextContent("800");
  expect(table).toHaveTextContent("5 / 90 日");
  await userEvent.selectOptions(field, "vp90_concentration_top5_pct");
  expect(table).toHaveTextContent("100.00%");
  expect(await tip("示例说明", "完整范围")).toHaveTextContent("12只");
  await userEvent.click(screen.getByText("查看字段覆盖"));
  expect(screen.getByRole("table", { name: "字段覆盖" })).toHaveTextContent("12 / 12");
});

it("不可用成交分布保留缺因，切换旧来源不留示例", async () => {
  const app = render(<FactorDailyFeatures research={nullVolumeProfileResearch} />);
  await userEvent.click(screen.getByText("成交分布示例（10只）"));
  const table = screen.getByRole("table", { name: "成交分布原值示例" });
  const missing = within(table).getAllByText("—")[0];
  if (!missing) throw new Error("Missing value required");
  fireEvent.focus(missing.closest(".tip-anchor") ?? missing);
  expect(await screen.findByRole("tooltip")).toHaveTextContent("成交量或成交额总和非正数");
  app.rerender(<FactorDailyFeatures research={null} />);
  expect(screen.queryByText("成交分布口径")).toBeNull();
});

it("混合来源分别显示成交分布与已有来源", () => {
  render(<FactorDailyFeatures research={mixedVolumeProfileResearch} />);
  expect(screen.getByRole("heading", { name: "字段来源" })).toBeInTheDocument();
  for (const label of [
    "日线字段口径",
    "分钟字段口径",
    "市场温度口径",
    "竞价字段口径",
    "成交分布口径",
  ])
    expect(screen.getByText(label)).toBeInTheDocument();
});

function Editor() {
  const [draft, setDraft] = useState<FactorEditorDraft>({
    generation_id: "a".repeat(64),
    mode: "create",
    factor_id: "vp",
    expected_head: null,
    name_zh: "",
    category: "technical",
    category_label: "技术",
    direction: "higher_is_better",
    expression: "",
  });
  return (
    <FactorEditor
      draft={draft}
      open
      capabilities={volumeProfileCapability}
      catalog={undefined}
      currentDefinition={null}
      currentGeneration={draft.generation_id}
      canSave
      storageReady
      stale={false}
      canRebase={false}
      busy={false}
      onChange={setDraft}
      onClose={() => {}}
      onRebase={() => {}}
      onSubmit={() => {}}
    />
  );
}

it("中文搜索和键盘插入成交分布字段，提示说明单位与窗口", async () => {
  render(<Editor />);
  await userEvent.type(screen.getByRole("searchbox", { name: "搜索字段" }), "90日");
  const group = screen.getByRole("group", { name: "成交分布" });
  const info = within(group).getByRole("button", { name: "90日可比成交量说明" });
  fireEvent.focus(info);
  expect(await screen.findByRole("tooltip")).toHaveTextContent("单位：股");
  const insert = within(group).getByRole("button", { name: "插入90日成交均价" });
  insert.focus();
  await userEvent.keyboard("{Enter}");
  expect(screen.getByRole("textbox", { name: "表达式" })).toHaveValue("vp90_vwap");
});
