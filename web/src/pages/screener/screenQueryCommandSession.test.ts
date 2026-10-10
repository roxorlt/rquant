import type { ExecuteScreenQuery, ScreenOriginalAction, ScreenQueryReadData } from "@/api/screen";
import { ScreenQueryCommandSession } from "./screenQueryCommandSession";

const scope = "a".repeat(64);
const command: ExecuteScreenQuery = {
  kind: "execute_screen_query",
  command_id: "original-screen-1",
  requested_at: "2026-10-05T07:00:00Z",
  page_size: 20,
  definition: {
    schema_version: 1,
    description: "原描述",
    mode: "daily",
    trade_date: "2026-09-30",
    source_kind: "replica",
    source_identity: "b".repeat(64),
    cutoff: null,
    conditions: [{ name: "not_st", args: {} }],
    ranking: null,
  },
};
const original: ScreenOriginalAction = { action: "execute", command };
function confirmed(): ScreenQueryReadData {
  return {
    available: true,
    owner_scope_tag: scope,
    presets: [],
    history: null,
    daily_run_evidence: [],
    receipt: {
      command_id: command.command_id,
      status: "succeeded",
      enqueued_at: command.requested_at,
      completed_at: command.requested_at,
      result: {},
      error: null,
    },
    execution: {
      execution_id: command.command_id,
      sequence: 1,
      command_hash: "c".repeat(64),
      plan_hash: "d".repeat(64),
      definition: command.definition,
      original_command: command,
      started_at: command.requested_at,
      completed_at: command.requested_at,
      status: "succeeded",
      base_count: 10,
      total: 0,
      unknown_count: 2,
      ranked_count: null,
      steps: [],
      artifact_sha256: "e".repeat(64),
      member_rank_sha256: "f".repeat(64),
      failure_code: null,
    },
    results: {
      execution_id: command.command_id,
      artifact_sha256: "e".repeat(64),
      rows: [],
      next_cursor: null,
    },
  };
}

it("回包丢失后重开只核对原命令，缺服务端结果不能称成功", async () => {
  const seen: { original: ScreenOriginalAction; operation: string }[] = [];
  const transport = async (body: ScreenOriginalAction, operation: string) => {
    seen.push({ original: structuredClone(body), operation });
    if (seen.length === 1) throw new Error("reply lost");
    if (seen.length === 2) return { ...confirmed(), execution: null, results: null };
    return confirmed();
  };
  const session = new ScreenQueryCommandSession(scope, window.sessionStorage, transport);
  await session.submit(original);
  expect(session.snapshot().status).toBe("unknown");
  expect(session.clear()).toBe(false);
  const reopened = new ScreenQueryCommandSession(scope, window.sessionStorage, transport);
  expect(reopened.snapshot()).toMatchObject({
    original,
    status: "unknown",
    message: "结果待确认，请核对原请求。",
  });
  expect(seen).toHaveLength(1);
  await reopened.recover("lookup");
  expect(reopened.snapshot().status).toBe("unknown");
  await reopened.recover("resume");
  expect(reopened.snapshot().status).toBe("succeeded");
  expect(seen).toEqual([
    { original, operation: "submit" },
    { original, operation: "lookup" },
    { original, operation: "resume" },
  ]);
});

it("退出清除私人待确认原文并忽略晚到回包", async () => {
  let reply: ((value: ScreenQueryReadData) => void) | undefined;
  const session = new ScreenQueryCommandSession(
    scope,
    window.sessionStorage,
    () =>
      new Promise((resolve) => {
        reply = resolve;
      }),
  );
  const pending = session.submit(original);
  session.dispose(true);
  reply?.(confirmed());
  await pending;
  expect(window.sessionStorage.length).toBe(0);
  expect(session.snapshot().data).toBeNull();
  const other = new ScreenQueryCommandSession("1".repeat(64), window.sessionStorage, async () =>
    confirmed(),
  );
  expect(other.snapshot().original).toBeNull();
});

it("存储不可用时拒绝提交，跨用户回包也不确认", async () => {
  const transport = vi.fn(async () => ({ ...confirmed(), owner_scope_tag: "1".repeat(64) }));
  const broken = {
    getItem: () => null,
    setItem: () => {
      throw new Error("full");
    },
    removeItem: () => undefined,
  } as unknown as Storage;
  const refused = new ScreenQueryCommandSession(scope, broken, transport);
  await refused.submit(original);
  expect(transport).not.toHaveBeenCalled();
  const active = new ScreenQueryCommandSession(scope, window.sessionStorage, transport);
  await active.submit(original);
  expect(active.snapshot().status).toBe("unknown");
});
