import type { Schemas } from "@/api/client";
import {
  matchesEditingSession,
  PRICE_RULE_JOURNAL,
  PriceAlertRuleCommandSession,
} from "./priceAlertRuleCommandSession";

type Command = Schemas["PriceAlertRuleCommandRequest"];
type Receipt = Schemas["PriceAlertRuleCommandReceipt"];
const body = (id = "command-a", ruleId = "rule-a"): Command => ({
  command_id: id,
  requested_at: "2026-10-05T01:00:00.000Z",
  generation_id: "a".repeat(64),
  rule_id: ruleId,
  action: "save",
  expected_version: null,
  ts_code: "600001.SH",
  membership_version: 1,
  rule: {
    name: "到价提醒",
    priority: "P2",
    enabled: true,
    comparison: "gte",
    threshold: "10.123456",
    valid_from: "09:30:01.123456",
    valid_until: "14:57:02",
  },
});
const receipt = (value: Command, status: Receipt["status"] = "saved_syncing"): Receipt => ({
  command_id: value.command_id,
  rule_id: value.rule_id,
  action: value.action,
  status,
  version: 1,
  message: "设置已写入，等待同步。",
});
const lock = async <T>(_name: string, action: () => Promise<T>): Promise<T> => action();
beforeEach(() => localStorage.clear());

function session(
  post: (value: Command, resume: boolean) => Promise<Receipt>,
  owner = "alice",
  storage: Storage | null = localStorage,
) {
  return new PriceAlertRuleCommandSession(storage, owner, post, lock);
}

it("stores the full exact request before sending and restores with resume only", async () => {
  const seen: [Command, boolean][] = [];
  const post = vi.fn(async (value: Command, resume: boolean) => {
    seen.push([value, resume]);
    expect(
      JSON.parse(localStorage.getItem(`${PRICE_RULE_JOURNAL}:alice:${value.command_id}`) ?? "{}")
        .body,
    ).toEqual(value);
    if (!resume) throw new Error("lost reply");
    return receipt(value);
  });
  const first = session(post);
  expect(await first.start(body())).toBe(true);
  expect(first.snapshot().entries[0]?.status).toBe("unknown");
  const restored = session(post);
  await restored.advance("command-a");
  expect(seen).toEqual([
    [body(), false],
    [body(), true],
  ]);
  expect(restored.snapshot().entries[0]?.body.rule?.threshold).toBe("10.123456");
});

it("never sends when durable storage fails or the owner journal is corrupt", async () => {
  const post = vi.fn(async (value: Command) => receipt(value));
  const badStorage = {
    ...localStorage,
    length: 0,
    key: () => null,
    getItem: () => null,
    setItem: () => {
      throw new Error("quota");
    },
  } as Storage;
  expect(await session(post, "alice", badStorage).start(body())).toBe(false);
  localStorage.setItem(`${PRICE_RULE_JOURNAL}:alice:bad`, "broken");
  expect(await session(post).start(body())).toBe(false);
  expect(post).not.toHaveBeenCalled();
});

it("blocks another unresolved request for the same rule, including another tab", async () => {
  const post = vi.fn(async (value: Command) => receipt(value));
  await session(post).start(body());
  expect(await session(post).start(body("command-b"))).toBe(false);
  expect(post).toHaveBeenCalledTimes(1);
  expect(await session(post).start(body("command-c", "rule-b"))).toBe(true);
});

it("does not restore or expose another user's records", async () => {
  const post = vi.fn(async (value: Command) => receipt(value));
  await session(post).start(body());
  const bob = session(post, "bob");
  expect(bob.snapshot().entries).toEqual([]);
  await bob.advance("command-a");
  expect(post).toHaveBeenCalledTimes(1);
});

it.each(["command", "rule", "action", "version"])(
  "keeps a mismatched %s receipt uncertain",
  async (field) => {
    const post = vi.fn(
      async (value: Command) =>
        ({
          ...receipt(value),
          ...(field === "command"
            ? { command_id: "other" }
            : field === "rule"
              ? { rule_id: "other" }
              : field === "action"
                ? { action: "delete" }
                : { version: 7 }),
        }) as Receipt,
    );
    const current = session(post);
    await current.start(body());
    expect(current.snapshot().entries[0]?.status).toBe("unknown");
  },
);

it("unknown resume preserves the request and cannot be treated as a new operation", async () => {
  const current = session(async (value) => receipt(value, "not_found"));
  await current.start(body());
  await current.advance("command-a");
  expect(await current.start(body("new-command"))).toBe(false);
  expect(current.snapshot().entries[0]?.status).toBe("not_found");
});

it("a delayed receipt belongs to its original edit session only", () => {
  const a = { owner: "alice", sessionId: "edit-a", ruleId: "rule-a", readVersion: 1 };
  expect(matchesEditingSession(a, a)).toBe(true);
  for (const replacement of [
    { sessionId: "edit-b" },
    { ruleId: "rule-b" },
    { owner: "bob" },
    { readVersion: 2 },
  ]) {
    expect(matchesEditingSession(a, { ...a, ...replacement })).toBe(false);
  }
  expect(matchesEditingSession(a, null)).toBe(false);
});

it("a verified superseded reply frees the rule for the new authoritative version", async () => {
  const current = session(async (value) => receipt(value, "superseded"));
  await current.start(body());
  expect(await current.start({ ...body("new"), expected_version: 3 })).toBe(true);
});

it("freezes caller fields before waiting for the cross-tab lock", async () => {
  let release: () => void = () => undefined;
  const waiting = new Promise<void>((resolve) => {
    release = resolve;
  });
  const seen: Command[] = [];
  const delayedLock = async <T>(_name: string, action: () => Promise<T>): Promise<T> => {
    await waiting;
    return action();
  };
  const current = new PriceAlertRuleCommandSession(
    localStorage,
    "alice",
    async (value) => {
      seen.push(value);
      return receipt(value);
    },
    delayedLock,
  );
  const original = body();
  const start = current.start(original);
  original.rule!.threshold = "99.99";
  release();
  await start;
  expect(seen[0]?.rule?.threshold).toBe("10.123456");
});

it("retains 100 unresolved originals and refuses capacity overflow", async () => {
  for (let index = 0; index < 100; index++)
    localStorage.setItem(
      `${PRICE_RULE_JOURNAL}:alice:command-${index}`,
      JSON.stringify({
        body: body(`command-${index}`, `rule-${index}`),
        status: "unknown",
        version: null,
      }),
    );
  const post = vi.fn(async (value: Command) => receipt(value));
  const current = session(post);
  expect(await current.start(body("overflow", "overflow-rule"))).toBe(false);
  expect(localStorage.length).toBe(100);
  expect(post).not.toHaveBeenCalled();
});

it("an old pending reply cannot downgrade another tab's verified publication", async () => {
  let release: () => void = () => undefined;
  const wait = new Promise<void>((resolve) => {
    release = resolve;
  });
  let postEntered = false;
  const first = session(async (value) => {
    postEntered = true;
    await wait;
    return receipt(value, "pending");
  });
  const starting = first.start(body());
  await vi.waitFor(() => expect(postEntered).toBe(true));
  await session(async (value) => receipt(value, "published")).advance("command-a");
  release();
  await starting;
  expect(first.snapshot().entries[0]?.status).toBe("published");
});

it("the frozen identity also guards admission after waiting for another tab", async () => {
  const original = { body: body(), status: "saved_syncing", version: 1 };
  localStorage.setItem(`${PRICE_RULE_JOURNAL}:alice:command-a`, JSON.stringify(original));
  let release: () => void = () => undefined;
  const waiting = new Promise<void>((resolve) => {
    release = resolve;
  });
  const post = vi.fn(async (value: Command) => receipt(value));
  const current = new PriceAlertRuleCommandSession(
    localStorage,
    "alice",
    post,
    async (_name, action) => {
      await waiting;
      return action();
    },
  );
  const input = body();
  const started = current.start(input);
  input.command_id = "different-command";
  input.rule_id = "different-rule";
  release();
  expect(await started).toBe(false);
  expect(JSON.parse(localStorage.getItem(`${PRICE_RULE_JOURNAL}:alice:command-a`) ?? "{}")).toEqual(
    original,
  );
  expect(post).not.toHaveBeenCalled();
});

it("deactivation stops another user's original recovery and malformed final records block sending", async () => {
  const post = vi.fn(async (value: Command) => receipt(value));
  localStorage.setItem(
    `${PRICE_RULE_JOURNAL}:alice:command-a`,
    JSON.stringify({ body: body(), status: "unknown", version: null }),
  );
  const current = session(post);
  current.setActive(false);
  await current.resumePending();
  expect(post).not.toHaveBeenCalled();
  localStorage.setItem(
    `${PRICE_RULE_JOURNAL}:alice:command-a`,
    JSON.stringify({ body: body(), status: "published", version: null }),
  );
  expect(await session(post).start(body("new"))).toBe(false);
  expect(post).not.toHaveBeenCalled();
});
