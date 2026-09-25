import type { Schemas } from "@/api/client";
import { EMPTY } from "@/format/number";
import { Pill, type PillKind, Tip } from "@/ui";

type Stage = Schemas["PipelineStage"];

const KIND: Record<Stage["state"], PillKind> = {
  done: "ok",
  running: "acc",
  waiting: "idle",
  paused: "warn",
  late: "warn",
};

/** Today's chain from reference data to the phone, one step per cell. */
export function Pipeline({ stages }: { stages: readonly Stage[] }) {
  return (
    <ol className="pipe" aria-label="今日链路">
      {stages.map((stage) => (
        <li key={stage.key} data-s={stage.state}>
          <span className="pt">{stage.window}</span>
          <Tip content={stage.hint}>
            <span className="pn">{stage.name}</span>
          </Tip>
          <Pill kind={KIND[stage.state]}>{stage.state_label}</Pill>
          <span className="pc">{stage.value ?? EMPTY}</span>
        </li>
      ))}
    </ol>
  );
}
