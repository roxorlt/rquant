import { describe, expect, it } from "vitest";
import { homePath } from "./home";
import { unreadAlertCount, worstHealth } from "./status";

describe("home path", () => {
  it("sends a phone to pools and a wider screen to overview", () => {
    expect(homePath(true)).toBe("/pools");
    expect(homePath(false)).toBe("/overview");
  });
});

describe("top bar status", () => {
  it("counts unconfirmed alerts on the latest trade date only", () => {
    expect(
      unreadAlertCount([
        { trade_date: "2026-10-17", acked_at: null },
        { trade_date: "2026-10-20", acked_at: null },
        { trade_date: "2026-10-20", acked_at: "2026-10-20T08:00:00Z" },
        { trade_date: "2026-10-20", acked_at: null },
      ]),
    ).toBe(2);
  });

  it("shows no badge number when everything on the latest day is confirmed", () => {
    expect(
      unreadAlertCount([
        { trade_date: "2026-10-17", acked_at: null },
        { trade_date: "2026-10-20", acked_at: "2026-10-20T08:00:00Z" },
      ]),
    ).toBe(0);
  });

  it("picks the worst health layer", () => {
    expect(worstHealth([])).toBe("unknown");
    expect(worstHealth(["ok", "ok"])).toBe("ok");
    expect(worstHealth(["ok", "warn"])).toBe("warn");
    expect(worstHealth(["warn", "crit", "ok"])).toBe("crit");
  });
});
