import { useEffect, useRef } from "react";
import { formatNumber } from "@/format/number";
import {
  Button,
  ChangeText,
  ConfirmDialog,
  EmptyState,
  Panel,
  RelativeTime,
  SkeletonLine,
  StatusBadge,
  Tip,
} from "@/ui";
import type { useFactorTracking } from "./useFactorTracking";

type Tracking = ReturnType<typeof useFactorTracking>;
const schedule = "每工作日 18:40 更新；仅使用已成熟且可核验的数据。";

export function FactorTrackingAction({ tracking }: { tracking: Tracking }) {
  const paused = tracking.panel?.status === "paused";
  if (tracking.operation !== null || tracking.occupied)
    return (
      <Button size="sm" disabledReason={tracking.blockedReason ?? "请先完成本次跟踪操作。"}>
        加入跟踪
      </Button>
    );
  if (tracking.panel?.tracked && !paused)
    return (
      <Button
        size="sm"
        disabledReason={
          !tracking.canCancel ? (tracking.blockedReason ?? "暂时不能取消跟踪。") : undefined
        }
        onClick={() => void tracking.submit(true)}
      >
        取消跟踪
      </Button>
    );
  return (
    <>
      <Button
        size="sm"
        disabledReason={
          !tracking.canJoin
            ? (tracking.blockedReason ?? tracking.joinBlockedReason ?? "暂时不能加入跟踪。")
            : undefined
        }
        onClick={tracking.open}
      >
        {paused ? "重新加入" : "加入跟踪"}
      </Button>
      {paused && tracking.canCancel ? (
        <Button size="sm" onClick={() => void tracking.submit(true)}>
          取消跟踪
        </Button>
      ) : null}
    </>
  );
}

export function FactorTrackingStatus({ tracking }: { tracking: Tracking }) {
  const record = tracking.operation;
  if (record === null) return null;
  return (
    <Panel title="本次跟踪" label="本次跟踪">
      <div className="factor-run-summary">
        <span>
          {record.factorName} · 第 {record.request.expected_head.version} 版
        </span>
        <span>{record.request.tracked ? "加入跟踪" : "取消跟踪"}</span>
      </div>
      <div
        className="factor-command-state"
        role={record.result?.status === "rejected" ? "alert" : "status"}
      >
        <p>{tracking.status}</p>
        {record.result?.status === "rejected" ? (
          <Button
            size="sm"
            disabled={tracking.busy || !tracking.sameActor}
            onClick={() => void tracking.finish()}
          >
            重新确认跟踪
          </Button>
        ) : tracking.completed ? (
          <Button size="sm" disabled={tracking.busy} onClick={() => void tracking.finish()}>
            继续查看跟踪
          </Button>
        ) : (
          <div className="factor-command-actions">
            <Button
              size="sm"
              disabled={tracking.busy || !tracking.canContinue}
              onClick={() => void tracking.refresh()}
            >
              刷新跟踪状态
            </Button>
            {record.result?.status !== "applied" ? (
              <Button
                size="sm"
                disabled={tracking.busy || !tracking.canContinue}
                onClick={() => void tracking.continueTracking("retry")}
              >
                用原请求重试跟踪
              </Button>
            ) : null}
          </div>
        )}
      </div>
      {tracking.blockedReason ? <p className="hint">{tracking.blockedReason}</p> : null}
    </Panel>
  );
}

export function FactorTrackingPanel({
  tracking,
  focusRequested = false,
}: {
  tracking: Tracking;
  focusRequested?: boolean;
}) {
  const anchor = useRef<HTMLElement>(null);
  const focusedFactor = useRef<string | null>(null);
  const factorId = tracking.selected?.factor_id;
  useEffect(() => {
    if (
      !focusRequested ||
      !tracking.panelVerified ||
      !factorId ||
      focusedFactor.current === factorId
    )
      return;
    focusedFactor.current = factorId;
    anchor.current?.focus();
    anchor.current?.scrollIntoView?.({ block: "start", behavior: "smooth" });
  }, [focusRequested, tracking.panelVerified, factorId]);
  const panel = tracking.panel;
  const summary = panel?.summary;
  const paused = panel?.status === "paused";
  return (
    <section ref={anchor} className="factor-tracking-anchor" tabIndex={-1} aria-label="因子跟踪">
      <Panel
        title="因子跟踪"
        sub={tracking.selected?.name_zh}
        actions={
          <Tip
            content={
              panel ? (
                <>
                  {panel.policy_label}
                  <br />
                  {schedule}
                  <br />
                  表达式自身的行业或市值处理仍按保存的定义生效。
                </>
              ) : (
                schedule
              )
            }
          >
            跟踪策略
          </Tip>
        }
      >
        {tracking.panelQuery.isLoading ? (
          <div className="factor-tracking-body" role="status" aria-label="正在加载跟踪记录">
            <SkeletonLine width="55%" />
            <SkeletonLine />
            <SkeletonLine width="75%" />
          </div>
        ) : panel === null || panel.availability === "unavailable" ? (
          <EmptyState
            title={tracking.panelNotice ? "等待跟踪同步" : "跟踪暂时无法查看"}
            hint={
              panel?.availability === "unavailable"
                ? (panel.reason ?? "跟踪数据尚未发布。")
                : (tracking.panelNotice ?? tracking.blockedReason ?? "请稍后刷新。")
            }
          />
        ) : panel.status === "not_tracked" ? (
          <EmptyState title="尚未加入跟踪" hint="加入后会保留当前版本的每日研究记录。" />
        ) : (
          <div className="factor-tracking-body">
            <div className="factor-research-meta">
              <span>
                第 {panel.definition_head?.version} 版{paused ? " · 已暂停" : " · 已跟踪"}
              </span>
              <StatusBadge
                state={
                  paused || summary?.invalidated
                    ? "warn"
                    : panel.status === "active"
                      ? "ok"
                      : "idle"
                }
                label={
                  paused || summary?.invalidated
                    ? "注意"
                    : panel.status === "active"
                      ? "正常"
                      : "未运行"
                }
                reason={panel.reason ?? summary?.reason}
              />
              {summary?.invalidated ? <span className="factor-partial">跟踪已失效</span> : null}
              {summary?.latest_trade_date ? (
                <span>
                  最近成熟日{" "}
                  <time dateTime={summary.latest_trade_date}>{summary.latest_trade_date}</time>
                </span>
              ) : null}
              <span>
                更新 <RelativeTime at={panel.updated_at} />
              </span>
              <Tip content={panel.basis_label}>统计口径</Tip>
            </div>
            {panel.reason ? (
              <p className="hint" role="status">
                {panel.reason}
              </p>
            ) : null}
            {summary ? (
              <>
                <dl className="factor-stats">
                  <div>
                    <dt>
                      <Tip content="最近成熟交易日的方向对齐 RankIC，不另作方向翻转。">昨日 IC</Tip>
                    </dt>
                    <dd className="num">{formatNumber(summary.yesterday_ic, 4)}</dd>
                  </div>
                  <div>
                    <dt>近20日平均 IC</dt>
                    <dd className="num">{formatNumber(summary.ic_20.mean, 4)}</dd>
                  </div>
                  <div>
                    <dt>
                      <Tip content="近20个成熟交易日的平均 IC 与样本标准差之比；缺少有效记录时不显示数值。">
                        近20日 IR
                      </Tip>
                    </dt>
                    <dd className="num">{formatNumber(summary.ic_20.ir, 4)}</dd>
                  </div>
                  <div>
                    <dt>昨日多空收益</dt>
                    <dd>
                      <ChangeText
                        value={
                          summary.yesterday_long_short == null
                            ? null
                            : summary.yesterday_long_short * 100
                        }
                      />
                    </dd>
                  </div>
                  <div>
                    <dt>
                      <Tip content="最近5个成熟交易日，两端组合各自累计后相减；覆盖不完整时不填补收益。">
                        最近5日多空收益
                      </Tip>
                    </dt>
                    <dd>
                      <ChangeText
                        value={
                          summary.week_long_short == null ? null : summary.week_long_short * 100
                        }
                      />
                    </dd>
                  </div>
                  <div>
                    <dt>
                      <Tip content="从本段实际起日计算；定义更新后重新加入会开始新段，历史版本不拼接累计。">
                        累计多空收益
                      </Tip>
                    </dt>
                    <dd>
                      <ChangeText
                        value={
                          summary.cumulative_long_short == null
                            ? null
                            : summary.cumulative_long_short * 100
                        }
                      />
                    </dd>
                  </div>
                </dl>
                <div className="factor-research-meta">
                  <span>
                    有效 IC{" "}
                    <b className="num">
                      {summary.ic_20.valid_day_count} / {summary.ic_20.source_day_count}
                    </b>{" "}
                    日
                  </span>
                  <span>
                    近20日完整覆盖 <b className="num">{summary.complete_day_count} / 20</b>
                  </span>
                  <span>
                    最近5日完整覆盖{" "}
                    <b className="num">
                      {summary.week_complete_day_count} / {summary.week_day_count}
                    </b>
                  </span>
                </div>
                {summary.reason ? <Tip content={summary.reason}>覆盖说明</Tip> : null}
              </>
            ) : (
              <EmptyState title="等待跟踪记录" hint={panel.reason ?? "成熟数据可核验后会显示。"} />
            )}
            <div className="factor-research-meta">
              <span>
                累计实际起日{" "}
                <time dateTime={panel.actual_start_date ?? undefined}>
                  {panel.actual_start_date ?? "—"}
                </time>
              </span>
            </div>
          </div>
        )}
      </Panel>
    </section>
  );
}

export function FactorTrackingConfirmation({ tracking }: { tracking: Tracking }) {
  const record = tracking.confirmation;
  return (
    <ConfirmDialog
      open={record !== null}
      level="heavy"
      title="加入因子跟踪"
      confirmLabel="确认加入"
      busy={tracking.busy}
      disabled={!tracking.confirmationCurrent}
      description={
        record ? (
          <>
            <p>
              {record.factorName} · 第 {record.request.expected_head.version} 版
            </p>
            <p>全市场 · 每日 · 5组 · RankIC · 无运行后中性化</p>
            <p className="hint">每工作日 18:40 更新，累计从本次实际起日计算。</p>
            {!tracking.confirmationCurrent ? (
              <p role="alert">因子或跟踪条件已变化，请关闭后重新确认。</p>
            ) : null}
          </>
        ) : null
      }
      onConfirm={() => void tracking.submit()}
      onCancel={tracking.cancelConfirmation}
    />
  );
}
