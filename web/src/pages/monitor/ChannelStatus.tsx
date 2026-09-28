import type { MonitorChannelsData } from "@/api/endpoints";
import { formatCount, formatPercent } from "@/format/number";
import { Button, EmptyState, Panel, RelativeTime, SkeletonKpis, Tip } from "@/ui";

export function ChannelStatus({
  data,
  loading,
  retry,
}: {
  data: MonitorChannelsData | undefined;
  loading: boolean;
  retry: () => void;
}) {
  return (
    <section className="monitor-channels" aria-label="推送通道状态">
      <div className="monitor-channels-heading">
        <h2>通道提交记录</h2>
        <Tip content="仅统计已发布的旧通知提交记录；新信号通知另列。">
          <span>统计范围</span>
        </Tip>
      </div>
      {loading ? (
        <div aria-busy="true" role="status" aria-label="推送记录加载中">
          <SkeletonKpis count={2} />
        </div>
      ) : data?.state !== "ready" ? (
        <Panel>
          <EmptyState
            title="推送记录暂无法核对"
            hint={
              <Button size="sm" onClick={retry}>
                重试
              </Button>
            }
          />
        </Panel>
      ) : (
        <div className="monitor-channel-grid">
          {data.channels.map((channel) => (
            <article
              key={channel.channel}
              className="monitor-channel-card"
              aria-label={channel.channel_label}
            >
              <header className="monitor-channel-head">
                <strong>{channel.channel_label}</strong>
              </header>
              <div className="monitor-channel-main">
                <span className="monitor-channel-rate num">
                  {formatPercent(channel.seven_day_success_pct, 1)}
                </span>
                <Tip content="近 7 日成功提交数占提交尝试数；新信号通知另列。成功提交不代表手机送达。">
                  <span className="monitor-channel-rate-label">近 7 日提交成功率</span>
                </Tip>
                {channel.seven_day_attempts === 0 ? (
                  <span className="monitor-channel-empty">近 7 日无提交记录</span>
                ) : null}
              </div>
              <dl className="monitor-channel-facts">
                <div>
                  <dt>今日成功提交</dt>
                  <dd className="num">{formatCount(channel.today_submitted)}</dd>
                </div>
                <div>
                  <dt>最近成功</dt>
                  <dd>
                    <RelativeTime at={channel.last_success_at} />
                  </dd>
                </div>
              </dl>
            </article>
          ))}
        </div>
      )}
    </section>
  );
}
