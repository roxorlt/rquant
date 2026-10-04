import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useState } from "react";
import { findJargon } from "@/test/jargon";
import { FactorDailyFeatures } from "./FactorDailyFeatures";
import { FactorEditor, type FactorEditorDraft } from "./FactorEditor";
import {
  auctionCapability,
  auctionResearch,
  mixedAuctionResearch,
  nullAuctionResearch,
} from "./factorAuction.fixture";

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

it("竞价原值示例标清样本数，覆盖保留完整域，口径放提示", async () => {
  const { container } = render(<FactorDailyFeatures research={auctionResearch} />);
  expect(screen.getByRole("heading", { name: "竞价字段" })).toBeInTheDocument();
  expect(container.textContent).not.toMatch(/09:25|09:30|SSE|board_auction|库存原值/);
  expect(
    findJargon(
      [...container.querySelectorAll("h3,summary,label,th,td")]
        .map((node) => node.textContent)
        .join(" "),
    ),
  ).toEqual([]);
  expect(await tip("竞价字段口径", "历史回顾")).toHaveTextContent("原题材完整成员");
  await userEvent.click(screen.getByText("原值示例（10只）"));
  const table = screen.getByRole("table", { name: "竞价原值示例" });
  expect(table).toHaveTextContent("题材竞价金额比");
  expect(table).toHaveTextContent("题材竞价高开占比");
  expect(table).toHaveTextContent("题材成员数");
  expect(table).toHaveTextContent("0.6667");
  expect(table).not.toHaveTextContent("66.67%");
  expect(await tip("示例说明", "完整范围")).toHaveTextContent("12只");
  await userEvent.click(screen.getByText("查看字段覆盖"));
  expect(screen.getByRole("table", { name: "字段覆盖" })).toHaveTextContent("3 / 12");
});

it("缺值显示原因，合法零占比保留0，切换旧来源不留竞价", async () => {
  const day = nullAuctionResearch.daily_feature_coverage_days?.[0];
  const stock = day?.auction_values?.[0];
  if (!day || !stock) throw new Error("Captured auction preview required");
  const research = {
    ...nullAuctionResearch,
    daily_feature_coverage_days: [
      {
        ...day,
        auction_values: [
          {
            ...stock,
            values: stock.values.map((value) =>
              value.column === "board_gap_up_ratio"
                ? { ...value, value: 0, status: "valid" as const }
                : value,
            ),
          },
        ],
      },
    ],
  };
  const app = render(<FactorDailyFeatures research={research} />);
  await userEvent.click(screen.getByText("原值示例（1只）"));
  const table = screen.getByRole("table", { name: "竞价原值示例" });
  expect(table).toHaveTextContent("0.0000");
  const missing = within(table).getByText("—");
  fireEvent.focus(missing.closest(".tip-anchor") ?? missing);
  expect(await screen.findByRole("tooltip")).toHaveTextContent("竞价值为空");
  app.rerender(<FactorDailyFeatures research={null} />);
  expect(screen.queryByText("竞价字段口径")).toBeNull();
});

it("混合来源分别展示日线、分钟、市场温度和竞价", () => {
  render(<FactorDailyFeatures research={mixedAuctionResearch} />);
  expect(screen.getByRole("heading", { name: "字段来源" })).toBeInTheDocument();
  for (const label of ["日线字段口径", "分钟字段口径", "市场温度口径", "竞价字段口径"])
    expect(screen.getByText(label)).toBeInTheDocument();
});

function Editor() {
  const [draft, setDraft] = useState<FactorEditorDraft>({
    generation_id: "a".repeat(64),
    mode: "create",
    factor_id: "auction",
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
      capabilities={auctionCapability}
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

it("搜索插入沿用原字段，金额比、占比和成员单位区分", async () => {
  render(<Editor />);
  const search = screen.getByRole("searchbox", { name: "搜索字段" });
  await userEvent.type(search, "竞价");
  const group = screen.getByRole("group", { name: "竞价" });
  await userEvent.click(within(group).getByRole("button", { name: "插入题材竞价金额比" }));
  expect(screen.getByRole("textbox", { name: "表达式" })).toHaveValue("board_auction_amount_ratio");
  const info = within(group).getByRole("button", { name: "题材竞价高开占比说明" });
  fireEvent.focus(info);
  expect(await screen.findByRole("tooltip")).toHaveTextContent("单位：比例（0–1）");
});
