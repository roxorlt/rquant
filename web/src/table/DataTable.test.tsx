import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useState } from "react";
import { compareCells, type DataColumn, DataTable } from "./DataTable";

interface Row {
  code: string;
  name: string;
  change: number | null;
}

const ROWS: Row[] = [
  { code: "600001.SH", name: "样本01", change: 1.5 },
  { code: "600002.SH", name: "样本02", change: null },
  { code: "600003.SH", name: "样本03", change: -2.25 },
];

const COLUMNS: DataColumn<Row>[] = [
  { id: "code", header: "代码", value: (row) => row.code, sortable: true },
  { id: "name", header: "名称", value: (row) => row.name },
  { id: "change", header: "涨幅", value: (row) => row.change, numeric: true, sortable: true },
];

function codes(): string[] {
  const table = screen.getByRole("table", { name: "样本" });
  return within(table)
    .getAllByRole("row")
    .slice(1)
    .map((row) => within(row).getAllByRole("cell")[0]?.textContent ?? "");
}

function Selectable() {
  const [selected, setSelected] = useState<string | null>(null);
  return (
    <>
      <DataTable
        rows={ROWS}
        columns={COLUMNS}
        rowKey={(row) => row.code}
        label="样本"
        onSelect={(row) => setSelected(row.code)}
        selectedKey={selected}
      />
      <output>{selected ?? "none"}</output>
    </>
  );
}

describe("DataTable", () => {
  it("sorts on header click, nulls last, and marks aria-sort", async () => {
    const user = userEvent.setup();
    render(<DataTable rows={ROWS} columns={COLUMNS} rowKey={(row) => row.code} label="样本" />);
    expect(codes()).toEqual(["600001.SH", "600002.SH", "600003.SH"]);

    await user.click(screen.getByRole("button", { name: /涨幅/ }));
    expect(codes()).toEqual(["600003.SH", "600001.SH", "600002.SH"]);
    expect(screen.getByRole("columnheader", { name: /涨幅/ })).toHaveAttribute(
      "aria-sort",
      "ascending",
    );
    await user.click(screen.getByRole("button", { name: /涨幅/ }));
    expect(screen.getByRole("columnheader", { name: /涨幅/ })).toHaveAttribute(
      "aria-sort",
      "descending",
    );
    expect(codes()[0]).toBe("600001.SH");
    expect(screen.getByRole("columnheader", { name: "名称" })).not.toHaveAttribute("aria-sort");
  });

  it("renders a dash for empty cells and right-aligns numbers", () => {
    render(<DataTable rows={ROWS} columns={COLUMNS} rowKey={(row) => row.code} label="样本" />);
    const cells = within(screen.getAllByRole("row")[2] as HTMLElement).getAllByRole("cell");
    expect(cells[2]).toHaveTextContent("—");
    expect(cells[2]).toHaveClass("num");
  });

  it("selects one row by click or keyboard and moves with the arrows", async () => {
    const user = userEvent.setup();
    render(<Selectable />);
    const rows = screen.getAllByRole("row").slice(1);
    await user.click(rows[1] as HTMLElement);
    expect(screen.getByRole("status")).toHaveTextContent("600002.SH");
    expect(rows[1]).toHaveAttribute("aria-selected", "true");
    expect(rows[0]).toHaveAttribute("aria-selected", "false");

    (rows[0] as HTMLElement).focus();
    await user.keyboard("{ArrowDown}{ArrowDown}");
    expect(document.activeElement).toBe(rows[2]);
    await user.keyboard("{ArrowDown}");
    expect(document.activeElement).toBe(rows[2]);
    await user.keyboard("{Enter}");
    expect(screen.getByRole("status")).toHaveTextContent("600003.SH");
  });

  it("says so when there are no rows", () => {
    render(
      <DataTable
        rows={[]}
        columns={COLUMNS}
        rowKey={(row) => row.code}
        label="样本"
        emptyText="空"
      />,
    );
    expect(screen.getByRole("cell", { name: "空" })).toBeInTheDocument();
  });

  it("renders only a window of rows for long lists", () => {
    const many = Array.from({ length: 1000 }, (_, index) => ({
      code: `C${String(index).padStart(4, "0")}`,
      name: `N${index}`,
      change: index,
    }));
    render(
      <DataTable
        rows={many}
        columns={COLUMNS}
        rowKey={(row) => row.code}
        label="样本"
        height={400}
      />,
    );
    const table = screen.getByRole("table", { name: "样本" });
    expect(table).toHaveAttribute("aria-rowcount", "1001");
    expect(within(table).getAllByRole("row").length).toBeLessThan(100);
  });

  it("orders mixed values with nulls last", () => {
    expect([3, null, 1].sort(compareCells)).toEqual([1, 3, null]);
    expect(["乙", "甲"].sort(compareCells)).toEqual(["甲", "乙"]);
  });
});
