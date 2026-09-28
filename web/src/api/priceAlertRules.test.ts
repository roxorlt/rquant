import { HttpResponse, http } from "msw";
import { metaEnvelope } from "@/test/fixtures";
import { metaHandler, server } from "@/test/server";
import type { Schemas } from "./client";
import { verifyPriceRuleBasis, verifyPriceRuleOwner } from "./priceAlertRules";

const GENERATION = metaEnvelope().serving.generation_id as string;
const SAVE: Omit<Schemas["SavePriceAlertRuleRequest"], "command_id" | "requested_at"> = {
  kind: "save_price_alert_rule",
  generation_id: GENERATION,
  ts_code: "600001.SH",
  membership_version: 2,
  expected_version: null,
  rule: {
    rule_id: "web-rule-1",
    name: "上破提醒",
    priority: "P2",
    enabled: true,
    comparison: "gte",
    threshold: "12.50",
    valid_from: "09:30:00",
    valid_until: "14:57:00",
  },
};

function rules(generationId = GENERATION) {
  return {
    serving: { ...metaEnvelope({ generationId }).serving },
    data: {
      availability: "ready",
      available_at: "2026-09-24T07:31:00Z",
      evaluation_running: false,
      items: [],
      message: "暂无价格规则。",
    },
  };
}

function watchlist(version = 2): Schemas["Envelope_ManualWatchlistExactData_"] {
  return {
    serving: metaEnvelope().serving,
    data: {
      availability: "ready",
      available_at: "2026-09-24T07:31:00Z",
      message: "",
      ts_code: "600001.SH",
      status: "active",
      version,
      expires_at: "2026-09-24T08:00:00Z",
      updated_at: "2026-09-24T07:30:00Z",
      source: "detail",
      price_levels: [],
    },
  };
}

it("checks owner, same data version, rule CAS and active manual membership before a new save", async () => {
  server.use(
    http.get("*/api/v1/monitor/rules", () => HttpResponse.json(rules())),
    http.get("*/api/v1/watchlist/600001.SH", () => HttpResponse.json(watchlist())),
  );
  await expect(verifyPriceRuleBasis("tester", SAVE)).resolves.toBe("ready");
  const noExpiry = watchlist();
  noExpiry.data.expires_at = null;
  server.use(http.get("*/api/v1/watchlist/600001.SH", () => HttpResponse.json(noExpiry)));
  await expect(verifyPriceRuleBasis("tester", SAVE)).resolves.toBe("ready");
  server.use(http.get("*/api/v1/watchlist/600001.SH", () => HttpResponse.json(watchlist(3))));
  await expect(verifyPriceRuleBasis("tester", SAVE)).resolves.toBe("stale");
  server.use(metaHandler(metaEnvelope({ viewer: "other-user" })));
  await expect(verifyPriceRuleBasis("tester", SAVE)).resolves.toBe("stale");
});

it("rechecks owner for original-command recovery without requiring its old data version", async () => {
  server.use(metaHandler(metaEnvelope({ generationId: "b".repeat(64) })));
  await expect(verifyPriceRuleOwner("tester")).resolves.toBe(true);
  server.use(metaHandler(metaEnvelope({ viewer: "other-user" })));
  await expect(verifyPriceRuleOwner("tester")).resolves.toBe(false);
});
