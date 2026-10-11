import { FlowSteps, PageHeader } from "@/ui";
import { PortfolioRuns } from "../backtest/Portfolio";

export default function ExperimentsPage() {
  return (
    <>
      <PageHeader
        eyebrow="策略与验证"
        title="实验记录"
        note="每次组合回测都是一条实验；可两两对比、看过拟合指标"
        actions={<FlowSteps />}
      />
      <PortfolioRuns />
    </>
  );
}
