import type { Schemas } from "@/api/client";
import { templateCatalog, templateDetail, templateSources } from "../strategies/template.fixture";
import { experimentFixture } from "./formal.fixture";

// Synthetic UI data uses the existing typed C5 and M8 fixtures. It is not a
// market reference or a claim that these displayed tasks ran on real prices.
const serving = experimentFixture.capabilities.serving;
const latest: Schemas["StrategyTemplateDetailData"] = {
  ...templateDetail,
  head: {
    ...templateDetail.head,
    version: 2,
    record_hash: "e".repeat(64),
    registration_fingerprint: "d".repeat(64),
    spec_fingerprint: "c".repeat(64),
  },
  rules: { ...templateDetail.rules, exit: { ...templateDetail.rules.exit, max_holding_days: 6 } },
};
latest.current_head = latest.head;
const originalConfig = experimentFixture.family.data.items[0]?.configuration;
if (!originalConfig) throw new Error("original experiment fixture config is missing");
export const experimentTemplateFixture = {
  catalog: { data: templateCatalog(latest), serving },
  latest: { data: latest, serving },
  detail: { data: { ...templateDetail, current_head: latest.head }, serving },
  sources: { data: templateSources, serving },
  versions: {
    data: {
      strategy_id: templateDetail.strategy_id,
      current_head: latest.head,
      versions: [
        { ...latest, is_head: true },
        { ...templateDetail, is_head: false },
      ],
      next_before_version: null,
    } satisfies Schemas["StrategyTemplateVersionsData"],
    serving,
  },
  capabilities: {
    ...experimentFixture.capabilities,
    data: { ...experimentFixture.capabilities.data, can_search_templates: true },
  },
  mine: {
    ...experimentFixture.mine,
    data: {
      ...experimentFixture.mine.data,
      items: [],
      retained_count: 0,
      oldest_registered_at: null,
      preparing_families: [
        {
          family_id: experimentFixture.family.data.family_id,
          name: "退出规则实验",
          registered_at:
            experimentFixture.mine.data.items[0]?.registered_at ?? "2026-10-05T08:00:00Z",
          state: "preparing",
          planned_count: 4,
          definition_saved_count: 2,
          input_prepared_count: 1,
          failed_count: 1,
          cancelled_count: 0,
        },
      ],
      preparing_window_truncated: false,
    } satisfies Schemas["ExperimentMineData"],
  },
  family: {
    ...experimentFixture.family,
    data: {
      ...experimentFixture.family.data,
      name: "退出规则实验",
      preparation_state: "preparing",
      preparations: [0, 1, 2, 3].map((index) => {
        const configuration =
          experimentFixture.family.data.items[index]?.configuration ?? originalConfig;
        return {
          index,
          configuration,
          definition_state: index === 2 ? "failed" : index < 2 ? "saved" : "pending",
          input_prepared: index === 0,
          failure: index === 2 ? "capacity" : null,
          strategy_name: templateDetail.name,
          strategy_version: templateDetail.head.version,
          rules: {
            ...templateDetail.rules,
            weight_rule: configuration.weight_rule,
            rebalance_rule: configuration.rebalance_rule,
          },
          metrics:
            experimentFixture.family.data.items[0]?.metrics?.map((metric) => ({
              ...metric,
              value: null,
            })) ?? [],
        };
      }),
      items: [],
      failed_count: 1,
      cancelled_count: 0,
    } satisfies Schemas["ExperimentFamilyData"],
  },
};
