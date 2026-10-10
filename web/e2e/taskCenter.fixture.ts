import type { Schemas } from "../src/api/client.ts";
import raw from "./taskCenter.fixture.json" with { type: "json" };

// Baseline bytes came from the actual synthetic Web API. Variants exercise React
// state and transport; the original journal and native ACK proofs are separate.
export const taskMeta = raw.meta as Schemas["Envelope_MetaData_"];
export const taskOverview = raw.overview as Schemas["Envelope_TaskOverviewData_"];
export const taskUnitCapabilities = raw.unit_capabilities as Schemas["TaskControlCapabilitiesData"];
export const taskSchedulingCapabilities =
  raw.scheduling_capabilities as Schemas["TaskControlCapabilitiesData"];
export const taskClock = raw.fixture_clock;
function requiredGeneration(): string {
  const generation = taskOverview.serving.generation_id;
  if (typeof generation !== "string") throw new Error("actual browser baseline has no generation");
  return generation;
}
export const taskGeneration = requiredGeneration();

export function taskScenarioOverview(): Schemas["Envelope_TaskOverviewData_"] {
  const overview = structuredClone(taskOverview);
  const row = (unit: string, name: string): Schemas["ScheduledTaskItem"] => {
    const original = overview.data.scheduled.items.find((item) => item.service_unit === unit);
    if (!original) throw new Error(`actual task baseline is missing ${unit}`);
    return {
      ...original,
      name,
      status: { state: "ok", label: "正常", reason: "等待下次触发" },
      last_trigger_at: "2026-10-06T00:00:00Z",
      next_at: "2026-10-07T01:30:00Z",
    };
  };
  overview.data.scheduled.items = [
    row("rquant-backup.service", "备份数据"),
    row("rquant-daily.service", "日线更新"),
  ];
  overview.data.resources.cpu_usage_percent = 0;
  overview.data.resources.cpu_note = "两个真实计数之间的使用率。";
  overview.data.resources.groups = overview.data.resources.groups.map((group) => ({
    ...group,
    cpu_usage_percent: group.slice_unit === "rquant-live.slice" ? 0 : null,
    cpu_note:
      group.slice_unit === "rquant-live.slice"
        ? "两个真实计数之间的使用率。"
        : "空组尚无完整计数。",
  }));
  return overview;
}
