import type { CSSProperties } from "react";
import { Link } from "react-router";
import { useAlerts, useHealth, useOverview } from "@/api/endpoints";
import {
  layerTitle,
  listedStockCode,
  presetLabel,
  publicLine,
  publicLines,
  signalActionLabel,
  signalActionTone,
  triggerLabel,
} from "@/copy/publicText";
import { formatListClock, formatShanghaiDateTime } from "@/format/time";
import { DataTable } from "@/table/DataTable";
import { SortMenu } from "@/table/SortMenu";
import { usePreference } from "@/theme/storage";
import { Button, KpiStrip, PageHeader, Panel, Pill } from "@/ui";
import { StockLink } from "@/ui/StockLink";
import { PHONE_QUERY, useMedia } from "../panorama/useMedia";
import { QueryView } from "../shared";

const STATE_PILL = {
  ok: ["ok", "正常"],
  warn: ["warn", "注意"],
  crit: ["crit", "需处理"],
  unknown: ["idle", "未知"],
} as const;

function Pipeline() {
  const health = useHealth();
  const layers = health.data?.layers ?? [];
  if (!layers.length) return null;
  return (
    <section aria-labelledby="pipe-h">
      <div className="block-h">
        <h2 id="pipe-h">今日链路</h2>
      </div>
      <ol
        className="pipe"
        aria-label="今日链路"
        style={{ "--pipe-cols": layers.length } as CSSProperties}
      >
        {layers.map((layer) => {
          const [kind, text] = STATE_PILL[layer.state];
          return (
            <li key={layer.key}>
              <span className="pn">{layerTitle(layer.title)}</span>
              <Pill kind={kind}>{text}</Pill>
              <span className="pd">{publicLines(layer.detail).slice(0, 2).join("；") || "—"}</span>
            </li>
          );
        })}
      </ol>
    </section>
  );
}

function Attention() {
  const health = useHealth();
  const alerts = useAlerts();
  const items: {
    key: string;
    kind: "crit" | "warn" | "acc";
    text: string;
    sub: string;
    to: string;
    btn: string;
  }[] = [];
  for (const layer of health.data?.layers ?? []) {
    if (layer.state === "crit" || layer.state === "warn")
      items.push({
        key: layer.key,
        kind: layer.state,
        text: `${layerTitle(layer.title)}：${layer.state === "crit" ? "异常" : "需留意"}`,
        sub: publicLine(layer.detail[0] ?? "") ?? "",
        to: "/health",
        btn: "看健康",
      });
  }
  const unacked = (alerts.data?.items ?? []).filter((a) => !a.acked_at).length;
  if (unacked)
    items.push({
      key: "alerts",
      kind: "acc",
      text: `${unacked} 条告警未确认`,
      sub: "在盯盘页逐条确认",
      to: "/monitor",
      btn: "去盯盘",
    });
  return (
    <Panel title="需要关注" sub={`${items.length} 项`} flush>
      {items.length ? (
        <ul className="attn" aria-label="需要关注">
          {items.map((a) => (
            <li key={a.key}>
              <Pill kind={a.kind}>
                {a.kind === "crit" ? "需处理" : a.kind === "warn" ? "注意" : "等你操作"}
              </Pill>
              <span className="txt">
                {a.text}
                <span className="s">{a.sub}</span>
              </span>
              <Link className="btn sm" to={a.to}>
                {a.btn}
              </Link>
            </li>
          ))}
        </ul>
      ) : (
        <p className="hint" style={{ padding: "12px 16px" }}>
          {(health.data?.layers ?? []).some((layer) => layer.state === "unknown")
            ? "还不知道服务是否正常"
            : "一切正常"}
        </p>
      )}
    </Panel>
  );
}

function sortedSignals<T extends { at: string | null }>(rows: readonly T[], sort: string): T[] {
  const desc = sort !== "at:asc";
  return [...rows].sort((a, b) => {
    const av = a.at ?? "";
    const bv = b.at ?? "";
    return desc ? bv.localeCompare(av) : av.localeCompare(bv);
  });
}

function SignalList({
  rows,
}: {
  rows: readonly {
    sequence: number;
    at: string | null;
    code: string;
    name: string | null;
    strategy_id: string;
    action: string;
  }[];
}) {
  if (!rows.length) {
    return (
      <p className="hint" style={{ padding: "12px 16px" }}>
        今天还没有信号：策略信号在盘后生成，生成后会出现在这里。
      </p>
    );
  }
  return (
    <ul className="li2-list" aria-label="最新信号">
      {rows.map((row) => {
        const code = listedStockCode(row.code);
        const tone = signalActionTone(row.action);
        return (
          <li key={row.sequence} className="li2">
            <span className="t">
              <span className="name">{row.name ?? "—"}</span>
              <span className="code">{code ? <StockLink code={code} /> : "—"}</span>
            </span>
            <span className="meta">
              {row.at ? formatListClock(row.at) : "—"} · {presetLabel(row.strategy_id)}
            </span>
            <span className="v">
              <span className={tone || undefined}>{signalActionLabel(row.action)}</span>
            </span>
          </li>
        );
      })}
    </ul>
  );
}

function LatestAlerts() {
  const alerts = useAlerts();
  const health = useHealth();
  const rows = (alerts.data?.items ?? []).slice(0, 6);
  const monitor = (health.data?.layers ?? []).find((layer) => /盯盘|通知/.test(layer.title));
  const empty =
    !monitor || monitor.state === "unknown"
      ? "今天还没有告警。还没有识别到盯盘服务。"
      : "今天还没有告警。盯盘服务在运行，还没有触发规则。";
  return (
    <Panel
      title="最新告警"
      flush
      actions={
        <Link className="btn ghost sm" to="/monitor">
          全部告警
        </Link>
      }
    >
      <DataTable
        label="最新告警"
        rows={rows}
        rowKey={(row) => row.alert_id}
        emptyText={empty}
        columns={[
          {
            id: "at",
            header: "时间",
            value: (r) => r.at ?? null,
            cell: (r) => (r.at ? formatShanghaiDateTime(r.at).slice(-8, -3) : "—"),
          },
          {
            id: "code",
            header: "股票",
            value: (r) => r.code,
            cell: (r) => `${r.name ?? ""} ${r.code}`,
          },
          {
            id: "type",
            header: "规则",
            value: (r) => triggerLabel(r.trigger_type ?? r.level),
          },
          {
            id: "state",
            header: "状态",
            value: (r) => (r.acked_at ? 1 : 0),
            cell: (r) =>
              r.acked_at ? <Pill kind="ok">已确认</Pill> : <Pill kind="warn">未确认</Pill>,
          },
        ]}
      />
    </Panel>
  );
}

export default function OverviewPage() {
  const query = useOverview();
  const health = useHealth();
  const phone = useMedia(PHONE_QUERY);
  const [signalSort, setSignalSort] = usePreference(
    "overview.signalSort",
    "at:desc",
    (v: unknown): v is string => typeof v === "string",
  );
  return (
    <>
      <PageHeader
        eyebrow="概览"
        title="总览"
        note={query.data?.trade_date ?? undefined}
        actions={
          <>
            <Button onClick={() => query.refetch()}>刷新</Button>
            <Link className="btn primary" to="/monitor">
              去盯盘
            </Link>
          </>
        }
      />
      <Pipeline />
      <QueryView query={query}>
        {(data) => (
          <>
            <KpiStrip
              label="关键数字"
              items={data.kpis.map((kpi) => {
                const layers = health.data?.layers ?? [];
                const unknown = layers.some((layer) => layer.state === "unknown");
                if (unknown && (kpi.key === "services" || kpi.label === "服务异常")) {
                  return { ...kpi, value: "—", tone: "muted" as const };
                }
                return { ...kpi, tone: kpi.tone ?? undefined };
              })}
            />
            <div className="g2">
              <LatestAlerts />
              <Attention />
            </div>
            <Panel
              title={`最新信号 · ${data.signals.length}`}
              sub="盘后策略产出，最新在上"
              flush
              actions={
                phone ? (
                  <SortMenu
                    value={signalSort}
                    onChange={setSignalSort}
                    options={[
                      { value: "at:desc", label: "时间降序" },
                      { value: "at:asc", label: "时间升序" },
                    ]}
                  />
                ) : undefined
              }
            >
              {phone ? (
                <SignalList rows={sortedSignals(data.signals, signalSort)} />
              ) : (
                <DataTable
                  label="最新信号"
                  rows={data.signals}
                  rowKey={(row) => String(row.sequence)}
                  initialSort={{ id: "at", desc: true }}
                  emptyText="今天还没有信号：策略信号在盘后生成，生成后会出现在这里。"
                  columns={[
                    {
                      id: "at",
                      header: "时间",
                      sortable: true,
                      value: (row) => row.at ?? null,
                      cell: (row) => (row.at ? formatShanghaiDateTime(row.at) : "—"),
                    },
                    {
                      id: "code",
                      header: "代码",
                      value: (row) => listedStockCode(row.code),
                      cell: (row) => {
                        const code = listedStockCode(row.code);
                        return code ? <StockLink code={code} /> : "—";
                      },
                    },
                    { id: "name", header: "名称", value: (row) => row.name ?? null },
                    {
                      id: "strategy",
                      header: "策略",
                      phoneLine: 2,
                      value: (row) => presetLabel(row.strategy_id),
                    },
                    {
                      id: "action",
                      header: "动作",
                      phoneLine: 2,
                      value: (row) => row.action,
                      cell: (row) => (
                        <span className={signalActionTone(row.action) || undefined}>
                          {signalActionLabel(row.action)}
                        </span>
                      ),
                    },
                  ]}
                />
              )}
            </Panel>
          </>
        )}
      </QueryView>
    </>
  );
}
