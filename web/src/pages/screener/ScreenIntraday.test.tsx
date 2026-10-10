import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { vi } from "vitest";
import { ScreenIntraday } from "./ScreenIntraday";

test("keyboard switches mode and shows the actual daily anchor", async () => {
  const onChange = vi.fn();
  const user = userEvent.setup();
  const view = render(<ScreenIntraday mode="daily" source={null} onChange={onChange} />);
  const live = screen.getByRole("button", { name: "盘中" });
  live.focus();
  await user.keyboard("{Enter}");
  expect(onChange).toHaveBeenCalledWith("intraday");
  view.rerender(
    <ScreenIntraday
      mode="intraday"
      source={{
        identity: "a".repeat(64),
        updated_at: "2026-10-05T01:40:02Z",
        mode: "intraday",
        cutoff: "2026-10-05T01:40:02Z",
        daily_anchor_date: "2026-09-30",
      }}
      onChange={onChange}
    />,
  );
  expect(live).toHaveAttribute("aria-pressed", "true");
  expect(screen.getByText(/日线基准 2026-09-30/)).toBeInTheDocument();
});
