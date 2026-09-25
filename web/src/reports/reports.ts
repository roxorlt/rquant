import gapStatus from "./gap-status.json";
import reportIndex from "./index.json";

/** Report registry (index.json) and the structured gap table (gap-status.json). */

export type ReportEntry =
  | { id: string; kind: "gap-status"; title: string; date: string; summary: string }
  | { id: string; kind: "html"; title: string; date: string; summary: string; file: string };

export type GapStatus = "已有" | "部分" | "缺" | "不做";

export interface GapModule {
  id: string;
  name: string;
  tier: string;
  status: GapStatus;
  statusNote: string | null;
  has: string;
  lacks: string;
  capabilities: string[];
  workstreams: string[];
  page: string | null;
  doneVersion: string | null;
  updated: string;
}

export interface GapStatusDocument {
  schemaVersion: 1;
  title: string;
  updated: string;
  latestVersion: string | null;
  source: string;
  statusLegend: Record<GapStatus, string>;
  conclusions: string[];
  modules: GapModule[];
}

export const REPORTS = reportIndex as ReportEntry[];
export const GAP_STATUS = gapStatus as GapStatusDocument;

export function reportById(id: string | undefined): ReportEntry | undefined {
  return REPORTS.find((report) => report.id === id);
}
