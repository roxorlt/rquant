import { ApiError, type Schemas } from "./client";
import {
  PRICE_RULE_JOURNAL_KEY,
  type PriceRuleCommandDraft,
  PriceRuleCommandSession,
  projectedRuleMatches,
} from "./priceAlertRuleCommand";

type Body = Schemas["SavePriceAlertRuleRequest"];
type Receipt = Schemas["PriceAlertRuleCommandReceipt"];
const RULE_ID = "web-rule-1";
const GENERATION = "a".repeat(64);
const NEXT_GENERATION = "b".repeat(64);
const AT = "2026-09-29T07:00:00.000Z";
const SAVE: PriceRuleCommandDraft = {
  kind: "save_price_alert_rule",
  generation_id: GENERATION,
  ts_code: "600001.SH",
  membership_version: 2,
  expected_version: null,
  rule: {
    rule_id: RULE_ID,
    name: "上破提醒",
    priority: "P2",
    enabled: true,
    comparison: "gte",
    threshold: "12.50",
    valid_from: "09:30:00",
    valid_until: "14:57:00",
  },
};

let sequence = 0;
beforeEach(() => window.localStorage.clear());

function session(post: (body: Body) => Promise<Receipt>, viewer = "tester") {
  return new PriceRuleCommandSession({
    storage: window.localStorage,
    viewer,
    post: post as (
      body:
        | Schemas["SavePriceAlertRuleRequest"]
        | Schemas["SetPriceAlertRuleEnabledRequest"]
        | Schemas["DeletePriceAlertRuleRequest"],
    ) => Promise<Receipt>,
    verifyNew: async () => "ready" as const,
    verifyOwner: async () => true,
    nextId: () => `web-command-${++sequence}`,
    now: () => AT,
    withLock: async (_key, task) => task(),
    isCurrent: () => true,
  });
}

it("persists an exact new price-rule request before POST and resumes it after lost response", async () => {
  const sent: Body[] = [];
  const post = vi.fn(async (body: Body): Promise<Receipt> => {
    sent.push(body);
    const saved = window.localStorage.getItem(`${PRICE_RULE_JOURNAL_KEY}:tester:${RULE_ID}`);
    expect(JSON.parse(saved ?? "{}").body).toEqual(body);
    if (sent.length === 1) throw new ApiError(503, "连接断开");
    return {
      command_id: body.command_id,
      kind: body.kind,
      rule_id: RULE_ID,
      status: "saved_syncing",
      version: 1,
      message: "已保存，正在同步",
    };
  });
  await session(post).start(SAVE);
  const reopened = session(post);
  expect(reopened.snapshot().entries[RULE_ID]?.status).toBe("unknown");
  await reopened.start(SAVE);
  expect(sent).toHaveLength(1);
  await reopened.advance(RULE_ID);
  expect(sent[1]).toEqual(sent[0]);
  expect(reopened.snapshot().entries[RULE_ID]).toMatchObject({
    status: "saved_syncing",
    version: 1,
  });
});

it("keeps an ambiguous conflict on its original command after a tab reopens", async () => {
  const sent: Body[] = [];
  const post = vi.fn(async (body: Body): Promise<Receipt> => {
    sent.push(body);
    return {
      command_id: body.command_id,
      kind: body.kind,
      rule_id: RULE_ID,
      status: "conflict",
      version: null,
      reason: "command_conflict",
      message: "规则已变化",
    };
  });
  await session(post).start(SAVE);
  const reopened = session(post);
  await reopened.start({ ...SAVE, generation_id: NEXT_GENERATION });
  expect(sent).toHaveLength(1);
  await reopened.advance(RULE_ID);
  expect(sent[1]).toEqual(sent[0]);
});

it("lets a new version replace only a proved no-effect CAS rejection", async () => {
  const sent: Body[] = [];
  const post = vi.fn(async (body: Body): Promise<Receipt> => {
    sent.push(body);
    return {
      command_id: body.command_id,
      kind: body.kind,
      rule_id: RULE_ID,
      status: sent.length === 1 ? "conflict" : "saved_syncing",
      version: sent.length === 1 ? null : 1,
      reason: sent.length === 1 ? "version_conflict" : null,
      message: "规则已变化",
    };
  });
  const current = session(post);
  await current.start(SAVE);
  expect(current.snapshot().entries[RULE_ID]?.status).toBe("conflict");
  await current.start(SAVE);
  expect(sent).toHaveLength(1);
  await current.start({ ...SAVE, generation_id: NEXT_GENERATION });
  expect(sent).toHaveLength(2);
  expect(sent[1]?.generation_id).toBe(NEXT_GENERATION);
  expect(sent[1]?.command_id).not.toBe(sent[0]?.command_id);
});

it("resolves a lost first response through the original command before allowing the next generation", async () => {
  const sent: Body[] = [];
  const post = async (body: Body): Promise<Receipt> => {
    sent.push(body);
    if (sent.length === 1) throw new ApiError(503, "连接断开");
    return {
      command_id: body.command_id,
      kind: body.kind,
      rule_id: RULE_ID,
      status: sent.length === 2 ? "conflict" : "saved_syncing",
      version: sent.length === 2 ? null : 1,
      reason: sent.length === 2 ? "generation_changed" : null,
      message: "规则状态已更新",
    };
  };
  await session(post).start(SAVE);
  const reopened = session(post);
  await reopened.start({ ...SAVE, generation_id: NEXT_GENERATION });
  expect(sent).toHaveLength(1);
  await reopened.advance(RULE_ID);
  expect(sent[1]).toEqual(sent[0]);
  await reopened.start({ ...SAVE, generation_id: NEXT_GENERATION });
  expect(sent[2]?.generation_id).toBe(NEXT_GENERATION);
  expect(sent[2]?.command_id).not.toBe(sent[0]?.command_id);
});

it("allows a fresh command in the same generation only after a durable failed receipt", async () => {
  const sent: Body[] = [];
  const post = async (body: Body): Promise<Receipt> => {
    sent.push(body);
    return {
      command_id: body.command_id,
      kind: body.kind,
      rule_id: RULE_ID,
      status: sent.length === 1 ? "failed" : "saved_syncing",
      version: sent.length === 1 ? null : 1,
      reason: null,
      message: sent.length === 1 ? "未保存" : "已保存，正在同步",
    };
  };
  await session(post).start(SAVE);
  const reopened = session(post);
  expect(reopened.snapshot().entries[RULE_ID]?.status).toBe("failed");
  await reopened.start(SAVE);
  expect(sent).toHaveLength(2);
  expect(sent[1]?.generation_id).toBe(GENERATION);
  expect(sent[1]?.command_id).not.toBe(sent[0]?.command_id);
});

it("reads a different tab's durable original before starting, and never reads another viewer's journal", async () => {
  const sent: Body[] = [];
  const post = async (body: Body): Promise<Receipt> => {
    sent.push(body);
    throw new ApiError(503, "连接断开");
  };
  const first = session(post);
  const second = session(post);
  await first.start(SAVE);
  await second.start(SAVE);
  expect(sent).toHaveLength(1);
  expect(second.snapshot().entries[RULE_ID]?.body).toEqual(sent[0]);
  const otherViewer = session(post, "other-user");
  expect(otherViewer.snapshot().entries[RULE_ID]).toBeUndefined();
  await otherViewer.advance(RULE_ID);
  expect(sent).toHaveLength(1);
});

it("only treats a newer matching rule fact as a published save", () => {
  const body: Body = { ...SAVE, command_id: "web-command-1", requested_at: AT };
  const item: Schemas["PriceAlertRuleItemData"] = {
    rule_id: RULE_ID,
    version: 1,
    deleted: false,
    ts_code: "600001.SH",
    membership_version: 2,
    name: "上破提醒",
    priority: "P2",
    enabled: true,
    comparison: "gte",
    threshold: "12.50",
    valid_from: "09:30:00",
    valid_until: "14:57:00",
    scope_status: "valid",
    updated_at: "2026-09-29T07:01:00Z",
  };
  const entry = { body, status: "saved_syncing" as const, version: 1, reason: null };
  expect(projectedRuleMatches(entry, item, GENERATION, "2026-09-29T07:01:00Z")).toBe(false);
  expect(projectedRuleMatches(entry, item, NEXT_GENERATION, "2026-09-29T07:01:00Z")).toBe(true);
  expect(
    projectedRuleMatches(
      entry,
      { ...item, threshold: "13.00" },
      NEXT_GENERATION,
      "2026-09-29T07:01:00Z",
    ),
  ).toBe(false);
  expect(projectedRuleMatches(entry, item, NEXT_GENERATION, "2026-09-29T06:59:00Z")).toBe(false);
});
