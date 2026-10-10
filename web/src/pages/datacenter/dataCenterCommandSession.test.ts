import type {
  DataCenterCommand,
  DataCenterCommandReceipt,
  DataCenterConfirmation,
} from "@/api/endpoints";
import { DataCenterCommandSession } from "./dataCenterCommandSession";

const body: DataCenterCommand = {
  kind: "prepare_backfill_execution",
  command_id: "original-request",
  requested_at: "2026-10-05T10:00:00Z",
  plan_task_id: "a".repeat(32),
  plan_hash: "b".repeat(64),
};
const confirmation: DataCenterConfirmation = {
  kind: "backfill",
  execution_id: "c".repeat(64),
  intent_id: "d".repeat(64),
  prepare_command_id: body.command_id,
  plan_hash: body.plan_hash,
  plan_task_id: body.plan_task_id,
  exact_dates_sha256: "e".repeat(64),
  start_date: "2026-09-24",
  end_date: "2026-09-25",
  missing_date_count: 2,
  report_periods: [],
  expires_at: "2026-10-05T10:05:00Z",
};

const prepared: DataCenterCommandReceipt = {
  command_id: body.command_id,
  status: "prepared",
  message: "请确认范围",
  confirmation,
};
beforeEach(() => window.sessionStorage.clear());

it("persists before dispatch and retries the exact original request after a lost response", async () => {
  const post = vi.fn(async (sent: DataCenterCommand): Promise<DataCenterCommandReceipt> => {
    expect(window.sessionStorage.getItem("rquant-data-center-command-v1")).toContain(
      sent.command_id,
    );
    if (post.mock.calls.length === 1) throw new Error("response lost");
    return prepared;
  });
  const first = new DataCenterCommandSession(window.sessionStorage, post);
  first.sync("owner", "generation");
  await first.start(body);
  expect(first.snapshot().uncertain).toBe(true);
  await first.start({ ...body, command_id: "different" });
  expect(post).toHaveBeenCalledTimes(1);
  const restored = new DataCenterCommandSession(window.sessionStorage, post);
  restored.sync("owner", "generation");
  await restored.retry();
  expect(post.mock.calls.map((call) => call[0])).toEqual([body, body]);
  expect(restored.snapshot().uncertain).toBe(false);
});

it("blocks another owner and malformed or unavailable storage", async () => {
  const post = vi.fn(async (): Promise<DataCenterCommandReceipt> => {
    throw new Error("no response");
  });
  const session = new DataCenterCommandSession(window.sessionStorage, post);
  session.sync("owner", "generation");
  await session.start(body);
  session.sync("another", "generation");
  await session.retry();
  expect(post).toHaveBeenCalledTimes(1);
  expect(session.snapshot().body).toBeNull();
  const unavailable = new DataCenterCommandSession(null, post);
  unavailable.sync("owner", "generation");
  await unavailable.start(body);
  expect(unavailable.snapshot().storageAvailable).toBe(false);
  window.sessionStorage.setItem("rquant-data-center-command-v1", "{}");
  expect(
    new DataCenterCommandSession(window.sessionStorage, post).snapshot().storageAvailable,
  ).toBe(false);
});

it("clears a prepared confirmation when the served data changes", async () => {
  const session = new DataCenterCommandSession(window.sessionStorage, async () => prepared);
  session.sync("owner", "old");
  await session.start(body);
  session.sync("owner", "new");
  expect(session.snapshot().body).toBeNull();
  expect(session.snapshot().receipt).toBeNull();
});

it("withholds a private receipt from a changed context before the sync effect runs", async () => {
  const session = new DataCenterCommandSession(window.sessionStorage, async () => prepared);
  session.sync("owner", "old");
  await session.start(body);
  expect(session.matchesContext("owner", "old")).toBe(true);
  expect(session.matchesContext("another", "old")).toBe(false);
  expect(session.matchesContext(null, "old")).toBe(false);
  expect(session.matchesContext("owner", "new")).toBe(false);
  expect(session.snapshot().receipt).toEqual(prepared);
});

it.each([
  { ...prepared, status: "queued" as const },
  { ...prepared, confirmation: null },
  { ...prepared, confirmation: { ...confirmation, prepare_command_id: "different" } },
  { ...prepared, confirmation: { ...confirmation, plan_hash: "f".repeat(64) } },
])("keeps an unrelated response unresolved", async (receipt) => {
  const session = new DataCenterCommandSession(window.sessionStorage, async () => receipt);
  session.sync("owner", "generation");
  await session.start(body);
  expect(session.snapshot().uncertain).toBe(true);
  expect(session.snapshot().receipt).toBeNull();
});

it("withdraws a preparation that returned after the served data changed", async () => {
  let release: ((value: DataCenterCommandReceipt) => void) | undefined;
  const session = new DataCenterCommandSession(
    window.sessionStorage,
    () =>
      new Promise((resolve) => {
        release = resolve;
      }),
  );
  session.sync("owner", "old");
  const request = session.start(body);
  session.sync("owner", "new");
  release?.(prepared);
  await request;
  expect(session.snapshot().receipt).toBeNull();
  expect(session.snapshot().uncertain).toBe(false);
  expect(window.sessionStorage.getItem("rquant-data-center-command-v1")).toBeNull();
});
