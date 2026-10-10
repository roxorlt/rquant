import type { ScreenQueryDefinition, ScreenRunData } from "@/api/screen";
import { Button, RelativeTime, Tip } from "@/ui";

export function ScreenIntraday({mode,source,onChange}: {
  mode: ScreenQueryDefinition["mode"];
  source: ScreenRunData["source"];
  onChange: (mode: ScreenQueryDefinition["mode"]) => void;
}) {
  return <div className="screen-mode" aria-label="选股模式">
    <div className="screen-query-actions">
      <Button size="sm" variant={mode==="daily" ? "primary" : "ghost"} aria-pressed={mode==="daily"} onClick={() => onChange("daily")}>日线</Button>
      <Tip content="使用同一时点的盘中行情。日线条件取上个已收盘交易日；缺数据的股票列为未知。">
        <Button size="sm" variant={mode==="intraday" ? "primary" : "ghost"} aria-pressed={mode==="intraday"} onClick={() => onChange("intraday")}>盘中</Button>
      </Tip>
    </div>
    {mode==="intraday" && source?.cutoff ? <span className="hint">行情 <RelativeTime at={source.cutoff} />{source.daily_anchor_date ? <Tip content="收盘价、均线、RSI 和日线形态均以这一天为基准。"><span> · 日线基准 {source.daily_anchor_date}</span></Tip> : null}</span> : null}
  </div>;
}
