import type { Schemas } from "@/api/client";
import { metaEnvelope } from "@/test/fixtures";

export const templateGeneration = metaEnvelope().serving.generation_id ?? "a".repeat(64);
export const templateId = `template_${"1".repeat(32)}`;
export const templateHead: Schemas["StrategyTemplateHead"] = {
  version: 1,
  registration_fingerprint: "2".repeat(64),
  record_hash: "3".repeat(64),
  spec_fingerprint: "4".repeat(64),
};
export const templateRules: Schemas["StrategyTemplate-Output"] = {
  template_contract: "strategy-template/v1",
  entry: { kind: "conditions", conditions: [{ key: "not_st", args: {} }] },
  exit: {
    stop_loss: "0.1",
    take_profit: null,
    trailing_profit: null,
    max_holding_days: 5,
    exit_time: null,
  },
  weight_rule: {
    method: "equal",
    max_positions: 5,
    max_stock_weight: "0.2",
    max_industry_weight: null,
    cash_reserve: "0",
    min_target_amount: "0",
  },
  rebalance_rule: { kind: "daily", every_n_days: null },
  index_filter: null,
};
export const templateDetail: Schemas["StrategyTemplateDetailData"] = {
  strategy_id: templateId,
  name: "低位观察",
  head: templateHead,
  current_head: templateHead,
  rules: templateRules,
  saved_at: "2026-09-24T07:00:00Z",
  change_note: "首次保存",
  archived: false,
  latest_run: null,
  can_save: true,
  can_archive: true,
  can_run: true,
};
export const templateSources: Schemas["StrategyTemplateSourcesData"] = {
  availability: "populated",
  can_create: true,
  pools: [
    {
      pool_key: "my-pool",
      version: 2,
      body_hash: "5".repeat(64),
      owner_id: "tester",
      name: "重点观察",
    },
  ],
  signals: [
    {
      strategy_id: "auction_gap",
      version: 1,
      source_hash: "6".repeat(64),
      owner_id: null,
      name: "竞价跳空",
      actions: ["b_confirm"],
    },
  ],
  comparison_fields: ["CLOSE", "MA5"],
  conditions: [
    {
      key: "not_st",
      label: "排除 ST",
      parameter_schema: {},
      block: {
        key: "not_st",
        label: "排除 ST",
        hint: "剔除名称带 ST 的股票",
        category: "filter",
        category_label: "股票范围",
        parameters: [],
      },
    },
    {
      key: "circ_mv_lt",
      label: "流通市值低于",
      parameter_schema: {},
      block: {
        key: "circ_mv_lt",
        label: "流通市值低于",
        hint: "筛出流通市值小于指定金额的股票",
        category: "filter",
        category_label: "股票范围",
        parameters: [
          {
            key: "threshold_yi",
            label: "流通市值上限（亿元）",
            input: "number",
            initial: 100,
            required: true,
            minimum: 0,
            maximum: 1000000,
            scale: 1,
            options: [],
            hint: null,
            custom_ma: false,
          },
        ],
      },
    },
  ],
};

export function templateEnvelope<T>(data: T, generationId = templateGeneration) {
  return { data, serving: metaEnvelope({ generationId }).serving };
}

export function templateCatalog(detail = templateDetail): Schemas["StrategyTemplateCatalogData"] {
  return {
    availability: "populated",
    available_at: detail.saved_at,
    can_create: true,
    templates: [
      {
        strategy_id: detail.strategy_id,
        name: detail.name,
        head: detail.current_head,
        saved_at: detail.saved_at,
        entry_kind: detail.rules.entry.kind,
        archived: detail.archived,
        phase: "未评估",
        latest_run: detail.latest_run,
      },
    ],
  };
}
