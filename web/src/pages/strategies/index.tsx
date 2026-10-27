import { PageHeader } from "@/ui";
import { StrategyList } from "../backtest/Portfolio";
import { EventStudies } from "./EventStudies";

export default function StrategiesPage() {
  return (
    <>
      <PageHeader
        eyebrow="策略与验证"
        title="策略"
        note="每条策略保留版本，回测时按名称和版本复现"
      />
      <StrategyList />
      <EventStudies />
    </>
  );
}
