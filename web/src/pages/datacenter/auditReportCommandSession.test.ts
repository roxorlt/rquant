import { ApiError, type Schemas } from "@/api/client";
import {
  AUDIT_REPORT_JOURNAL_KEY,
  AuditReportCommandSession,
  latestClosedAuditDate,
  validateAuditReportRange,
} from "./auditReportCommandSession";

type Command = Schemas["AuditReportCommandRequest"];
type Receipt = Schemas["AuditReportCommandReceipt"];
type Market = Schemas["MarketInfo"];
const TASK = "a".repeat(32);
const MARKET: Market = {
  trade_date: "2026-09-28",
  phase: "after_close",
  phase_label: "收盘",
  is_trading_day: true,
  previous_trading_day: "2026-09-24",
  next_trading_day: "2026-09-29",
};
const EVIDENCE = { market: MARKET };

beforeEach(() => window.sessionStorage.clear());

function makeSession(post: (body: Command) => Promise<Receipt>, storage = window.sessionStorage) {
  let number = 0;
  return new AuditReportCommandSession(
    storage,
    post,
    () => `audit-web-${++number}`,
    () => "2026-09-28T07:00:00.000Z",
  );
}

it("saves the exact request before POST and marks queued only from a matching task receipt", async () => {
  const post = vi.fn(async (body: Command): Promise<Receipt> => {
    expect(
      JSON.parse(window.sessionStorage.getItem(AUDIT_REPORT_JOURNAL_KEY) ?? "{}").body,
    ).toEqual(body);
    return { command_id: body.command_id, status: "queued", task_id: TASK, message: "已排队" };
  });
  const session = makeSession(post);

  await session.start("2024-09-01", "2026-09-28", EVIDENCE);

  expect(Object.keys(post.mock.calls[0]?.[0] ?? {}).sort()).toEqual([
    "audit_start",
    "command_id",
    "observed_through",
    "requested_at",
  ]);
  expect(session.snapshot().journal).toMatchObject({ status: "queued", taskId: TASK });
  expect(window.sessionStorage.getItem("rquant.backfill-plan-command.v1")).toBeNull();
  await session.advance();
  expect(post).toHaveBeenCalledTimes(1);
});

it("uses the original request after a timeout, reload, and attempted date change", async () => {
  const sent: Command[] = [];
  const post = vi.fn(async (body: Command): Promise<Receipt> => {
    sent.push(body);
    if (sent.length === 1) throw new ApiError(503, "状态待确认");
    return { command_id: body.command_id, status: "queued", task_id: TASK, message: "已排队" };
  });
  const session = makeSession(post);
  await session.start("2024-09-01", "2026-09-28", EVIDENCE);
  expect(session.snapshot().journal?.status).toBe("unknown");

  const restored = makeSession(post);
  await restored.start("2024-10-01", "2026-09-28", EVIDENCE);
  expect(sent).toHaveLength(1);
  expect(restored.snapshot().message).toMatch(/上一次请求/);
  await restored.advance();
  expect(sent).toHaveLength(2);
  expect(sent[1]).toEqual(sent[0]);
  expect(restored.snapshot().journal).toMatchObject({ status: "queued", taskId: TASK });
});

it.each(["pending", "processing", "ambiguous"] as const)(
  "keeps a %s receipt retriable with the same request",
  async (status) => {
    const sent: Command[] = [];
    const post = vi.fn(async (body: Command): Promise<Receipt> => {
      sent.push(body);
      return sent.length === 1
        ? { command_id: body.command_id, status, message: "待确认" }
        : { command_id: body.command_id, status: "queued", task_id: TASK, message: "已排队" };
    });
    const session = makeSession(post);
    await session.start("2024-09-01", "2026-09-28", EVIDENCE);
    expect(session.snapshot().journal?.status).toBe(status);
    await session.advance();
    expect(sent[1]).toEqual(sent[0]);
    expect(session.snapshot().journal).toMatchObject({ status: "queued", taskId: TASK });
  },
);

it.each([
  { command_id: "other", status: "queued", task_id: TASK, message: "已排队" },
  { command_id: "audit-web-1", status: "queued", task_id: "short", message: "已排队" },
  { command_id: "audit-web-1", status: "queued", message: "已排队" },
  { command_id: "audit-web-1", status: "processing", task_id: TASK, message: "处理中" },
] as Receipt[])("never accepts an unverified receipt: %j", async (receipt) => {
  const post = vi.fn(async (): Promise<Receipt> => receipt);
  const session = makeSession(post);

  await session.start("2024-09-01", "2026-09-28", EVIDENCE);

  expect(session.snapshot().journal).toMatchObject({ status: "unknown", taskId: null });
  expect(session.snapshot().message).toMatch(/待确认/);
  await session.start("2024-10-01", "2026-09-28", EVIDENCE);
  expect(post).toHaveBeenCalledTimes(1);
});

it("does not POST when browser storage cannot prove the request was saved", async () => {
  const post = vi.fn(
    async (body: Command): Promise<Receipt> => ({
      command_id: body.command_id,
      status: "queued",
      task_id: TASK,
      message: "已排队",
    }),
  );
  const storage = {
    getItem: () => null,
    setItem: () => {
      throw new Error("storage unavailable");
    },
    removeItem: () => undefined,
  } as unknown as Storage;
  const session = makeSession(post, storage);

  await session.start("2024-09-01", "2026-09-28", EVIDENCE);

  expect(post).not.toHaveBeenCalled();
  expect(session.snapshot().storageAvailable).toBe(false);
});

it("blocks new requests when an existing browser record cannot be checked", async () => {
  window.sessionStorage.setItem(AUDIT_REPORT_JOURNAL_KEY, "{broken");
  const post = vi.fn(
    async (body: Command): Promise<Receipt> => ({
      command_id: body.command_id,
      status: "queued",
      task_id: TASK,
      message: "已排队",
    }),
  );
  const session = makeSession(post);

  await session.start("2024-09-01", "2026-09-28", EVIDENCE);

  expect(post).not.toHaveBeenCalled();
  expect(session.snapshot().storageAvailable).toBe(false);
  expect(window.sessionStorage.getItem(AUDIT_REPORT_JOURNAL_KEY)).toBe("{broken");
});

it.each([401, 403, 413, 422])(
  "allows a new request after a definite HTTP %i rejection",
  async (status) => {
    const post = vi.fn(async (body: Command): Promise<Receipt> => {
      if (post.mock.calls.length === 1) throw new ApiError(status, "未通过检查");
      return { command_id: body.command_id, status: "queued", task_id: TASK, message: "已排队" };
    });
    const session = makeSession(post);
    await session.start("2024-09-01", "2026-09-28", EVIDENCE);
    expect(session.snapshot().journal?.status).toBe("failed");
    await session.start("2024-10-01", "2026-09-28", EVIDENCE);
    expect(post.mock.calls.map(([body]) => body.command_id)).toEqual([
      "audit-web-1",
      "audit-web-2",
    ]);
  },
);

it("permits a new request after an explicit failed receipt", async () => {
  const post = vi.fn(
    async (body: Command): Promise<Receipt> => ({
      command_id: body.command_id,
      status: "failed",
      message: "失败",
    }),
  );
  const session = makeSession(post);
  await session.start("2024-09-01", "2026-09-28", EVIDENCE);
  expect(session.snapshot().journal?.status).toBe("failed");
  await session.start("2024-10-01", "2026-09-28", EVIDENCE);
  expect(post.mock.calls.map(([body]) => body.command_id)).toEqual(["audit-web-1", "audit-web-2"]);
});

it("derives the last closed SSE day from market phase and the trade calendar", () => {
  const beforeClose: Market = { ...MARKET, phase: "closing_auction", phase_label: "尾盘" };
  const weekend: Market = {
    ...MARKET,
    trade_date: "2026-09-27",
    phase: "non_trading_day",
    phase_label: "休市",
    is_trading_day: false,
    next_trading_day: "2026-09-28",
  };
  const holiday: Market = { ...weekend, trade_date: "2026-09-25" };

  expect(latestClosedAuditDate(beforeClose)).toBe("2026-09-24");
  expect(latestClosedAuditDate({ ...MARKET, phase: "continuous" })).toBe("2026-09-24");
  expect(latestClosedAuditDate(MARKET)).toBe("2026-09-28");
  expect(latestClosedAuditDate(weekend)).toBe("2026-09-24");
  expect(latestClosedAuditDate(holiday)).toBe("2026-09-24");
  expect(latestClosedAuditDate(null)).toBeNull();
  expect(latestClosedAuditDate({ ...MARKET, is_trading_day: null, phase: "unknown" })).toBeNull();
  expect(latestClosedAuditDate({ ...MARKET, phase: "non_trading_day" })).toBeNull();
  expect(latestClosedAuditDate({ ...beforeClose, previous_trading_day: null })).toBeNull();
});

it("rejects dates beyond the close or without proof they were open SSE days", async () => {
  const weekend: Market = {
    ...MARKET,
    trade_date: "2026-09-27",
    phase: "non_trading_day",
    phase_label: "休市",
    is_trading_day: false,
  };
  expect(validateAuditReportRange("2024-09-01", "2026-09-28", { market: weekend })).not.toBeNull();
  expect(validateAuditReportRange("2024-09-01", "2026-09-27", { market: weekend })).not.toBeNull();
  expect(validateAuditReportRange("2024-09-01", "2026-09-25", EVIDENCE)).not.toBeNull();
  expect(validateAuditReportRange("2024-09-01", "2026-09-24", { market: weekend })).toBeNull();
  expect(validateAuditReportRange("2024-09-01", "2026-09-24", EVIDENCE)).toBeNull();
  expect(validateAuditReportRange("2024-09-01", "2026-09-28", EVIDENCE)).toBeNull();

  const post = vi.fn(
    async (body: Command): Promise<Receipt> => ({
      command_id: body.command_id,
      status: "queued",
      task_id: TASK,
      message: "已排队",
    }),
  );
  const session = makeSession(post);
  await session.start("2024-09-01", "2026-09-25", EVIDENCE);
  expect(post).not.toHaveBeenCalled();
  expect(session.snapshot().journal).toBeNull();
  await session.start("2024-09-01", "2026-09-28", { market: null });
  expect(post).not.toHaveBeenCalled();
  expect(session.snapshot().journal).toBeNull();
  await session.start("2024-09-01", "2026-09-28", undefined);
  expect(post).not.toHaveBeenCalled();
  expect(session.snapshot().journal).toBeNull();
});

it("validates real dates and the 1–3660 day limit without a browser clock", () => {
  expect(validateAuditReportRange("2025-02-30", "2026-09-28", EVIDENCE)).not.toBeNull();
  expect(validateAuditReportRange("2026-09-29", "2026-09-28", EVIDENCE)).not.toBeNull();
  expect(validateAuditReportRange("2010-01-01", "2026-09-28", EVIDENCE)).not.toBeNull();
  expect(validateAuditReportRange("2024-09-01", "2026-09-28", EVIDENCE)).toBeNull();
  expect(validateAuditReportRange("2024-09-01", "2026-09-28", { market: null })).not.toBeNull();
  expect(validateAuditReportRange("2024-09-01", "2026-09-28", undefined)).not.toBeNull();
});

it("accepts a historical open end only from an injected trusted calendar and still enforces close", () => {
  expect(validateAuditReportRange("2024-09-01", "2025-04-30", EVIDENCE)).not.toBeNull();
  expect(
    validateAuditReportRange("2024-09-01", "2025-04-30", {
      market: MARKET,
      verifiedOpenDates: ["2025-04-30"],
    }),
  ).toBeNull();
  expect(
    validateAuditReportRange("2024-09-01", "2026-09-29", {
      market: MARKET,
      verifiedOpenDates: ["2026-09-29"],
    }),
  ).not.toBeNull();
});
