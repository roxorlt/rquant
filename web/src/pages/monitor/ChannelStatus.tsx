import type { MonitorChannelsData, MonitorRuntimeData } from "@/api/endpoints";
import { formatCount, formatPercent } from "@/format/number";
import { formatShanghaiDateTime } from "@/format/time";
import { Button, EmptyState, Panel, RelativeTime, SkeletonKpis, Tip } from "@/ui";

export function ChannelStatus({
  data,
  loading,
  retry,
  runtime,
  runtimeLoading = false,
  retryRuntime = retry,
}: {
  data: MonitorChannelsData | undefined;
  loading: boolean;
  retry: () => void;
  runtime?: MonitorRuntimeData;
  runtimeLoading?: boolean;
  retryRuntime?: () => void;
}) {
  return (
    <>
      <section className="monitor-channels" aria-label="当前通道尝试">
        <div className="monitor-channels-heading">
          <h2>当前通道尝试</h2>
          <Tip content={runtime?.source_note ?? "当前来源尚未核对。"}>
            <span>统计范围</span>
          </Tip>
        </div>
        {runtimeLoading ? (
          <SkeletonKpis count={2} />
        ) : runtime?.state !== "ready" ? (
          <Panel>
            <EmptyState
              title="当前尝试暂无法核对"
              hint={
                <Button size="sm" onClick={retryRuntime}>
                  重试
                </Button>
              }
            />
          </Panel>
        ) : !runtime.channels?.length ? (
          <Panel>
            <EmptyState title="当前窗口暂无通道尝试" hint="出现提醒后会显示实际记录。" />
          </Panel>
        ) : (
          <div className="monitor-channel-grid">
            {runtime.channels.map((channel) => (
              <article
                key={channel.channel}
                className="monitor-channel-card"
                aria-label={`${channel.channel_label}当前通知`}
              >
                <header className="monitor-channel-head">
                  <strong>{channel.channel_label}</strong>
                  <span className="hint">{channel.mode === "shadow" ? "仅记录" : "正式推送"}</span>
                </header>
                <div className="monitor-channel-main">
                  <span className="monitor-channel-rate num">
                    {formatPercent(channel.accepted_pct, 1)}
                  </span>
                  <Tip
                    content={`已核对窗口内的实际请求提交率；无请求或存在未知结果时不显示百分比。通道接受不代表手机送达。${formatShanghaiDateTime(channel.covered_from)} 至 ${formatShanghaiDateTime(channel.covered_through)}`}
                  >
                    <span className="monitor-channel-rate-label">窗口提交成功率</span>
                  </Tip>
                </div>
                <dl className="monitor-channel-facts">
                  <div>
                    <dt>
                      <Tip content="此窗口内原通知成员数，合并后仍逐条核对准入。">逻辑通知</Tip>
                    </dt>
                    <dd className="num">{formatCount(channel.logical_count)}</dd>
                  </div>
                  <div>
                    <dt>
                      <Tip content="通知成员的实际发送准入尝试；合并的一次请求可包含多个成员。">
                        成员尝试
                      </Tip>
                    </dt>
                    <dd className="num">{formatCount(channel.member_attempts)}</dd>
                  </div>
                  <div>
                    <dt>
                      <Tip content="原通道发送入口记录的真实 POST 调用次数；可能已请求另列。">
                        实际请求
                      </Tip>
                    </dt>
                    <dd className="num">{formatCount(channel.physical_requests)}</dd>
                  </div>
                  <div>
                    <dt>成员重试</dt>
                    <dd className="num">{formatCount(channel.member_retries)}</dd>
                  </div>
                  <div>
                    <dt>通道接受</dt>
                    <dd className="num">{formatCount(channel.accepted_count)}</dd>
                  </div>
                  <div>
                    <dt>通道拒绝</dt>
                    <dd className="num">{formatCount(channel.rejected_count)}</dd>
                  </div>
                  <div>
                    <dt>结果未明</dt>
                    <dd className="num">{formatCount(channel.physical_unknown_count)}</dd>
                  </div>
                  <div>
                    <dt>
                      <Tip content="请求入口已有意图，但崩溃或无回执使实际提交次数无法确认；不会自动重发。">
                        可能已请求
                      </Tip>
                    </dt>
                    <dd className="num">{formatCount(channel.possible_requests)}</dd>
                  </div>
                </dl>
              </article>
            ))}
          </div>
        )}
      </section>
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
    </>
  );
}
