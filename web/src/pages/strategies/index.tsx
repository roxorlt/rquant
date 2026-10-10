import { PageHeader } from "@/ui";
import { StrategyList } from "../backtest/Portfolio";

export default function StrategiesPage() {
  return (
    <>
      <PageHeader
        eyebrow="策略与验证"
        title="策略"
        note="版本化策略配置；回测用 --strategy 名称@版本 复现"
      />
      <StrategyList />
    </>
  );
}
