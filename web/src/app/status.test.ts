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
  it("counts today's alerts newer than seenAt only", () => {
    expect(
      unreadAlertCount(
        [
          { trade_date: "2026-10-17", at: "2026-10-17T06:00:00Z" },
          { trade_date: "2026-10-20", at: "2026-10-20T05:00:00Z" },
          { trade_date: "2026-10-20", at: "2026-10-20T06:30:00Z" },
          { trade_date: "2026-10-20", at: "2026-10-20T07:00:00Z" },
        ],
        "2026-10-20",
        "2026-10-20T06:00:00Z",
      ),
    ).toBe(2);
  });

  it("shows no badge number when alerts belong to an older trade date or have all been seen", () => {
    expect(
      unreadAlertCount(
        [{ trade_date: "2026-10-17", at: "2026-10-17T06:00:00Z" }],
        "2026-10-20",
        null,
      ),
    ).toBe(0);
    expect(
      unreadAlertCount(
        [{ trade_date: "2026-10-20", at: "2026-10-20T06:00:00Z" }],
        "2026-10-20",
        "2026-10-20T06:00:00Z",
      ),
    ).toBe(0);
  });

  it("picks the worst health layer", () => {
    expect(worstHealth([])).toBe("unknown");
    expect(worstHealth(["ok", "ok"])).toBe("ok");
    expect(worstHealth(["ok", "warn"])).toBe("warn");
    expect(worstHealth(["warn", "crit", "ok"])).toBe("crit");
  });
});
