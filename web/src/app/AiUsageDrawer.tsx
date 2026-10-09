import { useState } from "react";
import { useAiUsage } from "@/api/aiAssistance";
import { shanghaiDate } from "@/format/time";
import { DataTable } from "@/table/DataTable";
import { Button, EmptyState, Panel, SideDrawer, SkeletonRows, Tip } from "@/ui";

const integer = (value: number | null | undefined) =>
  value == null ? "—" : value.toLocaleString("zh-CN");
export function AiUsageDrawer({
  viewer,
  open,
  onClose,
}: {
  viewer: string | null;
  open: boolean;
  onClose: () => void;
}) {
  return (
    <SideDrawer open={open} onClose={onClose} title="AI 用量" wide>
      <Usage key={viewer} viewer={viewer} enabled={open} />
    </SideDrawer>
  );
}
function Usage({ viewer, enabled }: { viewer: string | null; enabled: boolean }) {
  const today = shanghaiDate(new Date()).date;
  const [start, setStart] = useState(today);
  const [end, setEnd] = useState(today);
  const usage = useAiUsage(viewer, start, end, enabled);
  const data = usage.data;
  const summary = data?.summary;
  if (!viewer) return <EmptyState title="请先登录" hint="登录后可查看本人的用量。" />;
  return (
    <div className="screen-nl">
      <div className="row">
        <label className="field">
          <span className="lbl">开始日期</span>
          <input
            className="inp num"
            type="date"
            value={start}
            max={end}
            onChange={(event) => setStart(event.target.value)}
          />
        </label>
        <label className="field">
          <span className="lbl">结束日期</span>
          <input
            className="inp num"
            type="date"
            value={end}
            max={today}
            min={start}
            onChange={(event) => setEnd(event.target.value)}
          />
        </label>
        <Button size="sm" onClick={usage.refetch}>
          刷新用量
        </Button>
      </div>
      {usage.isLoading ? (
        <SkeletonRows rows={3} />
      ) : usage.error || !data?.available || !summary ? (
        <EmptyState title={usage.error?.message ?? data?.message ?? "用量暂不可用。"} />
      ) : (
        <>
          <Panel title="本人调用">
            <dl className="kv">
              <dt>调用次数</dt>
              <dd className="num">{integer(summary.calls)}</dd>
              <dt>输入用量</dt>
              <dd className="num">{integer(summary.input_tokens)}</dd>
              <dt>输出用量</dt>
              <dd className="num">{integer(summary.output_tokens)}</dd>
              <dt>当日剩余次数</dt>
              <dd className="num">
                {integer(data.remaining_calls)}{" "}
                <Tip content="当前账号共用每日调用上限。这里只显示本人的用量；结果未知的调用仍占一次。">
                  <span className="screen-help" role="img" aria-label="调用次数说明">
                    ⓘ
                  </span>
                </Tip>
              </dd>
            </dl>
            {summary.unknown_usage_calls > 0 ? (
              <p className="hint">
                部分用量未知{" "}
                <Tip
                  content={`已知输入 ${integer(summary.known_input_tokens)}；已知输出 ${integer(summary.known_output_tokens)}；${integer(summary.unknown_usage_calls)} 次未返回完整用量。未知不会按零计费。`}
                >
                  <span className="screen-help" role="img" aria-label="未知用量说明">
                    ⓘ
                  </span>
                </Tip>
              </p>
            ) : null}
          </Panel>
          <DataTable
            label="每日 AI 用量"
            rows={summary.days}
            rowKey={(row) => row.day}
            emptyText="所选区间没有调用。"
            initialSort={{ id: "day", desc: true }}
            columns={[
              { id: "day", header: "日期", value: (row) => row.day },
              { id: "calls", header: "次数", numeric: true, value: (row) => row.calls },
              {
                id: "input",
                header: "输入",
                numeric: true,
                value: (row) => row.input_tokens ?? null,
                cell: (row) => integer(row.input_tokens),
              },
              {
                id: "output",
                header: "输出",
                numeric: true,
                value: (row) => row.output_tokens ?? null,
                cell: (row) => integer(row.output_tokens),
              },
              {
                id: "unknown",
                header: "未知",
                numeric: true,
                secondary: true,
                value: (row) => row.unknown_usage_calls,
              },
            ]}
          />
        </>
      )}
    </div>
  );
}
