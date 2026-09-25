import type { MetaEnvelope } from "@/api/client";
import type { HealthEnvelope, OverviewEnvelope } from "@/api/endpoints";

/** A synthetic /api/v1/meta envelope (shape from the generated schema). */
export function metaEnvelope(
  overrides: {
    state?: MetaEnvelope["serving"]["state"];
    detail?: string;
    generationId?: string;
    viewer?: string | null;
    phase?: MetaEnvelope["data"]["market"]["phase"];
    phaseLabel?: string;
    isTradingDay?: boolean | null;
  } = {},
): MetaEnvelope {
  const generationId =
    overrides.generationId ?? "a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90";
  return {
    data: {
      server_time: "2026-09-24T07:31:30Z",
      viewer: overrides.viewer === undefined ? "tester" : overrides.viewer,
      generation: {
        generation_id: generationId,
        built_at: "2026-09-24T07:31:00Z",
        published_at: "2026-09-24T07:31:00Z",
        previous_generation_id: null,
        producer_commit: "0e5b0e5b0e5b0e5b0e5b0e5b0e5b0e5b0e5b0e5b",
        schema_version: 3,
        age_seconds: 190,
      },
      datasets: [],
      projections: [],
      market: {
        trade_date: "2026-09-24",
        phase: overrides.phase ?? "continuous",
        phase_label: overrides.phaseLabel ?? "连续竞价",
        is_trading_day: overrides.isTradingDay === undefined ? true : overrides.isTradingDay,
        previous_trading_day: "2026-09-23",
        next_trading_day: "2026-09-25",
      },
    },
    serving: {
      generation_id: generationId,
      built_at: "2026-09-24T07:31:00Z",
      age_seconds: 190,
      state: overrides.state ?? "ready",
      message: null,
      detail: overrides.detail ?? "serving generation verified",
    },
  };
}

const SERVING = {
  generation_id: "a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90",
  built_at: "2026-09-24T05:23:21Z",
  age_seconds: 40,
  state: "ready",
  message: null,
  detail: "serving generation verified",
} as const;

/** A synthetic /api/v1/overview envelope shaped like the 2026-09-24 replay. */
export function overviewEnvelope(
  overrides: Partial<OverviewEnvelope["data"]> = {},
): OverviewEnvelope {
  return {
    serving: { ...SERVING },
    data: {
      session: {
        today: "2026-09-25",
        trade_date: "2026-09-24",
        is_today: false,
        phase: "non_trading_day",
        next_trading_day: "2026-09-28",
      },
      pipeline: [
        {
          key: "reference",
          name: "参考数据",
          window: "09:20",
          state: "done",
          state_label: "已完成",
          value: "5,556 只",
          hint: "股票列表、停牌、涨跌停价等当天的参考数据",
        },
        {
          key: "signals",
          name: "盘中信号",
          window: "09:30–15:00",
          state: "done",
          state_label: "已完成",
          value: "2 条",
          hint: "各策略在分钟线上确认后发出的信号",
        },
      ],
      candidates: {
        total: 3,
        groups: [
          {
            key: "screen:n-shape-pool1",
            name: "N 字一池",
            count: 2,
            as_of: "2026-09-23",
            source: "screen",
          },
          {
            key: "signals:auction_gap",
            name: "竞价跳空",
            count: 1,
            as_of: "2026-09-24",
            source: "signals",
          },
        ],
        items: [
          {
            code: "001266.SZ",
            name: "宏英智能",
            group: "screen:n-shape-pool1",
            group_name: "N 字一池",
            close: 37.62,
            pct_chg: -0.34,
            first_seen_at: null,
          },
          {
            code: "001268.SZ",
            name: "联合精密",
            group: "screen:n-shape-pool1",
            group_name: "N 字一池",
            close: 27.17,
            pct_chg: 2.26,
            first_seen_at: null,
          },
          {
            code: "002238.SZ",
            name: null,
            group: "signals:auction_gap",
            group_name: "竞价跳空",
            close: null,
            pct_chg: null,
            first_seen_at: "2026-09-24T05:04:14Z",
          },
        ],
      },
      signals: {
        total: 2,
        by_action: [
          { action: "b_intent", label: "买入意向", count: 1 },
          { action: "watch", label: "观察", count: 1 },
        ],
        items: [
          {
            signal_id: "270460a7462598976cf31c31bc1b43ea71912be5f8606158a08bdc8ef34f7c45",
            sequence: 6,
            at: "2026-09-24T05:05:14Z",
            code: "002238.SZ",
            name: "天威视讯",
            strategy_id: "auction_gap",
            strategy_name: "竞价跳空",
            action: "b_intent",
            action_label: "买入意向",
            delivery: "recorded",
            delivery_label: "仅记录",
            reasons: ["竞价跳空确认", "均价线支撑"],
          },
          {
            signal_id: "c7557ec7aa9bd161d3f697ff7be5da33a70abe3e8d5b3f9330a0501e081b4132",
            sequence: 5,
            at: "2026-09-24T05:04:14Z",
            code: "002238.SZ",
            name: "天威视讯",
            strategy_id: "auction_gap",
            strategy_name: "竞价跳空",
            action: "watch",
            action_label: "观察",
            delivery: "failed",
            delivery_label: "失败",
            reasons: [],
          },
        ],
      },
      deliveries: {
        total: 2,
        delivered: 1,
        sending: 0,
        failed: 1,
        expired: 0,
        mode: "shadow",
        mode_label: "仅记录",
        mode_note: "正式推送开通前只记录不发送",
      },
      paper: {
        account_id: "shadow-main",
        as_of: "2026-09-24T05:06:18Z",
        nav: 99990,
        cash: 97889.95,
        unrealized_pnl: -10,
        realized_pnl: 0,
        holdings: [
          {
            code: "603937.SH",
            name: "丽岛新材",
            quantity: 100,
            available_quantity: 0,
            average_cost: 12.7564,
            market_price: 12.7064,
            market_value: 1270.64,
            unrealized_pnl: -5,
            unrealized_pct: -0.39,
          },
        ],
        note: "持仓按最近成交价估值，不是实时价",
      },
      services: { total: 24, ok: 11, warn: 1, crit: 1, idle: 6, waiting: 5 },
      freshness: {
        on_time: 6,
        checked: 10,
        no_source: 2,
        late: ["日线", "分钟线"],
        caveats: [],
      },
      attention: [
        {
          level: "crit",
          title: "1 条推送失败",
          reason: "手机可能没有收到这些信号",
          to: "/health",
          action: "看健康",
        },
        {
          level: "warn",
          title: "推送还没有正式开通",
          reason: "正式推送开通前只记录不发送",
          to: "/health",
          action: "看健康",
        },
      ],
      ...overrides,
    },
  };
}

/** A synthetic /api/v1/health envelope shaped like the 2026-09-24 replay. */
export function healthEnvelope(overrides: Partial<HealthEnvelope["data"]> = {}): HealthEnvelope {
  const service = (
    service_id: string,
    name: string,
    plane: string,
    plane_label: string,
    state: "ok" | "warn" | "crit" | "idle" | "waiting",
    label: string,
    reason: string,
    extra: Partial<HealthEnvelope["data"]["services"][number]> = {},
  ): HealthEnvelope["data"]["services"][number] => ({
    service_id,
    name,
    plane,
    plane_label,
    status: { state, label, reason },
    heartbeat_at: state === "idle" || state === "waiting" ? null : "2026-09-24T05:23:07Z",
    observed_at: "2026-09-24T05:23:20Z",
    raw_status: state === "idle" || state === "waiting" ? "missing" : "running",
    stale: state === "idle" || state === "waiting",
    input_sequence: 6,
    output_sequence: 6,
    backlog_count: 0,
    consecutive_failures: 0,
    last_error: null,
    ...extra,
  });
  return {
    serving: { ...SERVING },
    data: {
      counts: { total: 4, ok: 1, warn: 1, crit: 1, idle: 0, waiting: 1 },
      services: [
        service(
          "reference-slow.publisher.v1",
          "参考数据发布",
          "live",
          "盘中",
          "crit",
          "异常",
          "连续失败 239 次",
          {
            raw_status: "degraded",
            consecutive_failures: 239,
            last_error: "ReferenceSlowRuntimeError: reference slow publisher started after 09:25",
          },
        ),
        service(
          "notifier.admin.shadow.v1",
          "通知推送",
          "live",
          "盘中",
          "warn",
          "注意",
          "影子模式：只记录，不真正推送",
        ),
        service(
          "auction-match.source.v1",
          "竞价撮合数据",
          "live",
          "盘中",
          "waiting",
          "等待开盘",
          "盘中服务，休市日不运行",
        ),
        service(
          "signal-router.all-strategies.v1",
          "信号路由",
          "live",
          "盘中",
          "ok",
          "正常",
          "运行中",
        ),
      ],
      freshness: [
        {
          key: "minute_coverage",
          name: "分钟线",
          kind: "market",
          latest_at: "2026-09-23T07:00:00Z",
          latest_date: "2026-09-23",
          status: { state: "warn", label: "延迟", reason: "落后 1 个交易日" },
        },
        {
          key: "signals",
          name: "盘中信号",
          kind: "dataset",
          latest_at: "2026-09-24T05:05:19Z",
          latest_date: null,
          status: { state: "ok", label: "按时", reason: "按时更新" },
        },
        {
          key: "lab_jobs",
          name: "研究任务",
          kind: "dataset",
          latest_at: null,
          latest_date: null,
          status: { state: "idle", label: "未发布", reason: "研究任务服务没有运行，暂时没有数据" },
        },
      ],
      page_data: {
        status: { state: "ok", label: "正常", reason: "约每分钟更新一次" },
        built_at: "2026-09-24T05:23:21Z",
        published_at: "2026-09-24T05:23:21Z",
        age_seconds: 40,
        generation_id: SERVING.generation_id,
        tables_total: 29,
        unpublished: [
          { key: "pulse_alert", name: "脉搏异动提醒" },
          { key: "strategy_trade", name: "回测交易" },
        ],
      },
      errors: [
        {
          service_id: "reference-slow.publisher.v1",
          name: "参考数据发布",
          at: "2026-09-24T05:23:07Z",
          summary: "连续失败 239 次",
          message: "ReferenceSlowRuntimeError: reference slow publisher started after 09:25",
        },
      ],
      ...overrides,
    },
  };
}
