/**
 * The data-state banner. It appears only when the page data itself is in question (the
 * generation is stale, a newer one failed verification, or nothing can be read) and says
 * so in one short sentence with what to do; the technical reason is in a tooltip.
 * Per-dataset freshness is not a banner: it is shown as data where it matters
 * (web/CLAUDE.md 「数据状态横幅」).
 */

import type { ReactNode } from "react";
import type { components } from "@/api/schema";
import { Tip } from "./Tip";

export type ServingState = components["schemas"]["ServingState"];

export interface ServingBannerMessage {
  tone: "warn" | "crit";
  text: string;
}

const FALLBACK: Record<Exclude<ServingState, "ready">, string> = {
  stale: "数据已经一段时间没有更新，页面上的数字可能不是最新的。",
  degraded: "最新一批数据没有通过校验，暂时显示上一批。",
  unavailable: "暂时读不到数据，请稍后刷新。",
};

export function servingBannerMessage(
  state: ServingState,
  message: string | null | undefined,
): ServingBannerMessage | null {
  if (state === "ready") {
    return null;
  }
  return {
    tone: state === "unavailable" ? "crit" : "warn",
    text: message?.trim() || FALLBACK[state],
  };
}

export interface ServingBannerProps {
  state: ServingState;
  message?: string | null;
  /** Technical reason, for the tooltip. */
  detail?: string | null;
  /** What to do, e.g. a link to 系统健康. */
  action?: ReactNode;
}

export function ServingBanner({ state, message, detail, action }: ServingBannerProps) {
  const banner = servingBannerMessage(state, message);
  if (banner === null) {
    return null;
  }
  return (
    <div
      className={`banner ${banner.tone}`}
      role={banner.tone === "crit" ? "alert" : "status"}
      data-state={state}
    >
      <span className="d" aria-hidden="true" />
      <span className="banner-text">{banner.text}</span>
      {detail ? (
        <Tip content={<span className="tip-detail">{detail}</span>} placement="bottom">
          <span className="banner-more">详情</span>
        </Tip>
      ) : null}
      {action ? <span className="banner-action">{action}</span> : null}
    </div>
  );
}
