/**
 * Data-state banner for every page that reads Serving data. The wording and
 * the four states match the Streamlit pages' render_serving_state_banner
 * (src/rquant/dashboard/serving_page_ui.py): nothing is shown when the data
 * is ready, a warning when it is stale or degraded, an error when it is
 * unavailable.
 */

import type { components } from "@/api/schema";

export type ServingState = components["schemas"]["ServingState"];

export interface ServingBannerMessage {
  tone: "warn" | "crit";
  text: string;
}

export function servingBannerMessage(
  state: ServingState,
  detail: string,
  label: string,
): ServingBannerMessage | null {
  const reason = detail.trim() || "未提供状态详情";
  switch (state) {
    case "ready":
      return null;
    case "stale":
      return { tone: "warn", text: `${label}已过期：${reason}` };
    case "degraded":
      return { tone: "warn", text: `${label}处于降级状态：${reason}` };
    case "unavailable":
      return { tone: "crit", text: `${label}不可用：${reason}` };
  }
}

export interface ServingBannerProps {
  state: ServingState;
  detail: string;
  /** What the data is, e.g. "运行控制台数据". */
  label: string;
}

export function ServingBanner({ state, detail, label }: ServingBannerProps) {
  const message = servingBannerMessage(state, detail, label);
  if (message === null) {
    return null;
  }
  return (
    <div
      className={`banner ${message.tone}`}
      role={message.tone === "crit" ? "alert" : "status"}
      data-state={state}
    >
      <span className="d" aria-hidden="true" />
      <span>{message.text}</span>
    </div>
  );
}
