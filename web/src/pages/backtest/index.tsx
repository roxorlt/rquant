import { useSearchParams } from "react-router";
import { Segmented } from "@/ui";
import MinuteReplay from "./MinuteReplay";
import PortfolioPage from "./PortfolioPage";
import "./portfolio.css";

export default function BacktestPage() {
  const [params, setParams] = useSearchParams();
  const view = params.get("view") === "minute" ? "minute" : "portfolio";
  return (
    <>
      <div className="pb-mode">
        <Segmented
          label="回测类型"
          value={view}
          onChange={(value) => setParams(value === "minute" ? { view: "minute" } : {})}
          options={[
            { value: "portfolio", label: "日线组合" },
            { value: "minute", label: "分钟回放" },
          ]}
        />
      </div>
      {view === "minute" ? <MinuteReplay /> : <PortfolioPage />}
    </>
  );
}
