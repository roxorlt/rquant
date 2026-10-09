import { act, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import type { HealthData } from "@/api/endpoints";
import { META_QUERY_KEY } from "@/api/useMeta";
import { healthEnvelope, metaEnvelope } from "@/test/fixtures";
import { findJargon } from "@/test/jargon";
import { renderApp } from "@/test/render";
import { healthHandler, server } from "@/test/server";

async function renderHealth() {
  const utils = renderApp("/health");
  await screen.findByRole("table", { name: "运行服务" });
  return utils;
}

describe("系统健康", () => {
  it("counts services by plain state", async () => {
    await renderHealth();
    const kpis = screen.getByRole("region", { name: "服务与数据" });
    expect(kpis).toHaveTextContent("服务4个");
    expect(kpis).toHaveTextContent("异常1");
    expect(kpis).toHaveTextContent("未运行1其中 1 个是盘中服务，不在交易时段");
  });

  it("names services in plain words; the technical id is in the tooltip", async () => {
    const user = userEvent.setup();
    await renderHealth();
    const table = screen.getByRole("table", { name: "运行服务" });
    const rows = within(table).getAllByRole("row").slice(1);
    expect(rows.map((row) => within(row).getAllByRole("cell")[0]?.textContent)).toEqual([
      "参考数据发布",
      "通知推送",
      "竞价撮合数据",
      "信号路由",
    ]);
    expect(table).not.toHaveTextContent("notifier.admin.shadow.v1");
    await user.hover(within(table).getByText("通知推送"));
    expect(await screen.findByRole("tooltip")).toHaveTextContent("notifier.admin.shadow.v1");
  });

  it("shows expected off-session idleness as 等待开盘, not red", async () => {
    await renderHealth();
    const badge = screen.getByText("等待开盘").closest(".status");
    expect(badge).toHaveAttribute("data-state", "waiting");
  });

  it("filters to what needs attention", async () => {
    const user = userEvent.setup();
    await renderHealth();
    await user.click(screen.getByRole("button", { name: "只看异常" }));
    const table = screen.getByRole("table", { name: "运行服务" });
    expect(within(table).getAllByRole("row")).toHaveLength(3);
    expect(table).not.toHaveTextContent("信号路由");
  });

  it("opens a detail drawer with the technical fields", async () => {
    const user = userEvent.setup();
    await renderHealth();
    await user.click(screen.getByText("参考数据发布", { selector: "td .nm" }));
    const drawer = await screen.findByRole("dialog");
    expect(drawer).toHaveTextContent("reference-slow.publisher.v1");
    expect(drawer).toHaveTextContent("连续失败239");
    expect(drawer).toHaveTextContent("ReferenceSlowRuntimeError");
  });

  it("shows data freshness, page data and recent errors", async () => {
    await renderHealth();
    const freshness = screen.getByRole("table", { name: "数据新鲜度" });
    expect(freshness).toHaveTextContent("分钟线09-23 周三延迟");
    expect(freshness).toHaveTextContent("研究任务—未发布");
    expect(screen.getByText("2 个")).toBeInTheDocument();
    expect(screen.getByText("连续失败 239 次", { selector: ".what" })).toBeInTheDocument();
  });

  it("keeps ids and hashes out of the page text", async () => {
    await renderHealth();
    await waitFor(() => expect(document.querySelector(".page-skel")).toBeNull());
    expect(findJargon(document.body.textContent ?? "")).toEqual([]);
  });
});

function observedHealth() {
  const at = "2026-09-24T05:23:20Z";
  const layers: NonNullable<HealthData["layers"]> = [
    ["host", "主机与服务", "/tasks", "任务与调度"],
    ["market", "行情数据", "/datacenter", "数据中心"],
    ["strategy", "策略与信号", "/strategies", "策略"],
    ["orders", "模拟订单", "/paper", "模拟盘"],
    ["risk", "组合风险", "/paper", "组合风控"],
    ["comparison", "收益对照", "/backtest", "回测结果"],
  ].map(([key, name, href, label]) => ({
    key: key as NonNullable<HealthData["layers"]>[number]["key"],
    name: name ?? "",
    status: { state: "idle", label: "未运行", reason: "尚无可核验观测" },
    observed_at: at,
    metrics: [],
    exposure: [],
    links: [{ href: href ?? "", label: label ?? "" }],
  }));
  const host = layers[0];
  const strategy = layers[2];
  const risk = layers[4];
  if (!host || !strategy || !risk) throw new Error("fixture layers missing");
  host.metrics = [
    {
      key: "host-observation",
      name: "主机 CPU",
      value: null,
      unit: "ratio",
      available: false,
      status: host.status,
      observed_at: at,
      valid_until: null,
      temporal_basis: "unknown",
      scope_label: "当前主机",
      scope_detail: '{"host_name":"fixture-host"}',
      source_name: "任务与调度",
      source_generation_id: "a".repeat(64),
      event_time_start: at,
      event_time_end: at,
      available_at: at,
      link: { href: "/tasks", label: "任务与调度" },
    },
  ];
  const hostMetric = host.metrics[0];
  if (!hostMetric) throw new Error("fixture host metric missing");
  strategy.metrics = [
    {
      ...hostMetric,
      key: "batch-observation",
      name: "已处理候选",
      unit: "count",
      value: 7,
      available: true,
      status: { state: "warn", label: "注意", reason: "已核验数值，尚无判断规则" },
      temporal_basis: "as_of",
      scope_label: "原业务范围",
      scope_detail: '{"batch_id":"original-batch"}',
      source_name: "策略与信号",
      link: { href: "/strategies", label: "策略" },
    },
  ];
  risk.exposure = ["first-account", "second-account"].flatMap((scope_key, index) =>
    [
      {
        kind: "industry" as const,
        name: "银行",
        portfolio_weight: index ? "0.20" : "0.60",
        benchmark_weight: "0.50",
        deviation: index ? "-0.30" : "0.10",
      },
      {
        kind: "unknown" as const,
        name: "未分类",
        portfolio_weight: "0.10",
        benchmark_weight: "0",
        deviation: "0.10",
      },
      {
        kind: "cash" as const,
        name: "现金",
        portfolio_weight: index ? "0.70" : "0.30",
        benchmark_weight: "0.50",
        deviation: index ? "0.20" : "-0.20",
      },
    ].map((row) => ({
      ...row,
      scope_key,
      scope_label: `组合 ${index + 1}`,
      scope_detail: JSON.stringify({ account_id: scope_key }),
      source_name: "原组合风控",
      source_generation_id: "b".repeat(64),
      source_identity: "c".repeat(64),
      observed_at: at,
      valid_until: at,
      link: { href: "/paper", label: "原组合风控" },
    })),
  );
  const envelope = healthEnvelope({ layers, viewer_id: "tester" });
  const first = envelope.data.services[0];
  if (!first) throw new Error("fixture service missing");
  first.detail = {
    available: true,
    reason: "观测已核验",
    source_name: "原服务心跳",
    observed_at: at,
    started_at: at,
    observations: [{ key: "processed_candidates", label: "已处理候选", value: 7 }],
    degraded_reason: "部分功能暂不可用，请查看原运行日志",
  };
  return envelope;
}

describe("六层观测", () => {
  it("shows six source links, unknown values and completed facts without fake zero", async () => {
    server.use(healthHandler(observedHealth()));
    const user = userEvent.setup();
    await renderHealth();
    for (const name of [
      "主机与服务",
      "行情数据",
      "策略与信号",
      "模拟订单",
      "组合风险",
      "收益对照",
    ]) {
      expect(screen.getByRole("region", { name })).toBeInTheDocument();
    }
    const host = screen.getByRole("region", { name: "主机与服务" });
    expect(host).toHaveTextContent("主机 CPU—");
    expect(host).not.toHaveTextContent("0.00%");
    expect(within(host).getByRole("link", { name: "任务与调度" })).toHaveAttribute(
      "href",
      "/tasks",
    );
    expect(screen.getByRole("region", { name: "策略与信号" })).toHaveTextContent("已处理候选7");
    await user.click(within(host).getByRole("button", { name: "主机 CPU详情" }));
    expect(await screen.findByRole("tooltip")).toHaveTextContent("fixture-host");
  });

  it("keeps each account's original industry, unknown and cash rows together", async () => {
    server.use(healthHandler(observedHealth()));
    await renderHealth();
    const first = screen.getByRole("table", { name: "组合 1暴露" });
    const second = screen.getByRole("table", { name: "组合 2暴露" });
    expect(first).toHaveTextContent("银行60.00%");
    expect(second).toHaveTextContent("银行20.00%");
    expect(first).toHaveTextContent("未分类10.00%");
    expect(second).toHaveTextContent("现金70.00%");
    expect(findJargon(document.body.textContent ?? "")).toEqual([]);
  });

  it("labels each account's risk, cash, order and comparison values in the page body", async () => {
    const envelope = observedHealth();
    const host = envelope.data.layers?.[0]?.metrics[0];
    if (!host) throw new Error("fixture host metric missing");
    for (const layer of envelope.data.layers ?? []) {
      const names =
        layer.key === "risk"
          ? ["组合风控", "现金权重"]
          : layer.key === "orders"
            ? ["拒单率"]
            : layer.key === "comparison"
              ? ["回测区间"]
              : [];
      layer.metrics = names.flatMap((name) =>
        ["first-account", "second-account"].map((account, index) => ({
          ...host,
          key: `${account}:${name}`,
          name,
          scope_label: `组合 ${index + 1}`,
          scope_detail: JSON.stringify({ account_id: account }),
          available: true,
          value:
            name === "现金权重"
              ? index
                ? "0.70"
                : "0.30"
              : name === "拒单率"
                ? index
                  ? "0.40"
                  : "0.20"
                : name === "组合风控"
                  ? index
                    ? "breached"
                    : "clear"
                  : index
                    ? "outside"
                    : "inside",
          unit: ["现金权重", "拒单率"].includes(name)
            ? "ratio"
            : name === "组合风控"
              ? "risk_state"
              : "band_position",
          status: index
            ? { state: "crit" as const, label: "异常", reason: "原业务判断" }
            : { state: "ok" as const, label: "正常", reason: "原业务判断" },
          link: { href: "/paper", label: "模拟盘" },
        })),
      );
    }
    server.use(healthHandler(envelope));
    const user = userEvent.setup();
    await renderHealth();
    for (const layer of envelope.data.layers ?? []) {
      if (!["risk", "orders", "comparison"].includes(layer.key)) continue;
      const card = screen.getByRole("region", { name: layer.name });
      const rows = card.querySelectorAll(".health-metrics li");
      expect(rows).toHaveLength(layer.metrics.length);
      for (const [index, item] of layer.metrics.entries()) {
        const row = rows[index];
        if (!row) throw new Error("missing account metric row");
        expect(row.querySelector(".health-metric-scope")).toHaveTextContent(item.scope_label);
        expect(row).not.toHaveTextContent("account_id");
        expect(within(row as HTMLElement).getByRole("button")).toHaveAccessibleName(
          `${item.scope_label}${item.name}详情`,
        );
        expect(row.querySelector("strong")).toHaveTextContent(
          item.unit === "ratio"
            ? `${(Number(item.value) * 100).toFixed(2)}%`
            : item.unit === "risk_state"
              ? item.value === "clear"
                ? "未触线"
                : "已触线"
              : item.value === "inside"
                ? "区间内"
                : "区间外",
        );
      }
    }
    const risk = screen.getByRole("region", { name: "组合风险" });
    const details = within(risk).getByRole("button", { name: "组合 2现金权重详情" });
    act(() => details.focus());
    expect(await screen.findByRole("tooltip")).toHaveTextContent("second-account");
    await user.click(details);
    expect(screen.getByRole("tooltip")).toHaveTextContent("second-account");
  });

  it("clears an open heartbeat detail and private rows as soon as the viewer or generation changes", async () => {
    server.use(healthHandler(observedHealth()));
    const user = userEvent.setup();
    const { queryClient } = await renderHealth();
    await user.click(screen.getByText("参考数据发布", { selector: "td .nm" }));
    const drawer = await screen.findByRole("dialog");
    expect(drawer).toHaveTextContent("已处理候选7");
    act(() => {
      queryClient.setQueryData(META_QUERY_KEY, metaEnvelope({ viewer: "other-owner" }));
    });
    await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
    expect(screen.queryByRole("table", { name: "组合 1暴露" })).not.toBeInTheDocument();
    act(() => {
      queryClient.setQueryData(META_QUERY_KEY, metaEnvelope({ generationId: "f".repeat(64) }));
    });
    expect(screen.queryByRole("table", { name: "组合 1暴露" })).not.toBeInTheDocument();
  });
});
