import { Link } from "react-router";
import type { MetaEnvelope } from "@/api/client";
import { phaseTone } from "@/format/session";
import { formatTradeDate, shanghaiDate, weekdayOf } from "@/format/time";
import { THEME_LABELS, useTheme } from "@/theme/ThemeProvider";
import { Button, Tip } from "@/ui";
import { GenerationBadge } from "./GenerationBadge";
import { BrandMark, SearchIcon, SparkIcon, ThemeIcon } from "./icons";
import { APP_TITLE, HOME_PATH } from "./pages";
import { UserMenu } from "./UserMenu";

export interface TopbarProps {
  meta: MetaEnvelope | undefined;
  metaReceivedAt: number;
  metaFailed: boolean;
}

function TradeDay({ meta }: { meta: MetaEnvelope | undefined }) {
  const market = meta?.data.market;
  const today = shanghaiDate(new Date());
  const date = market?.trade_date ?? today.date;
  const weekday = market ? weekdayOf(market.trade_date) : today.weekday;
  return (
    <div className="tb-day">
      <span className="mono">{date}</span> {weekday}
    </div>
  );
}

/** Phase from the Serving trade calendar; on a closed day, when the market opens next. */
function PhasePill({ meta }: { meta: MetaEnvelope | undefined }) {
  const market = meta?.data.market;
  const label = market?.phase_label ?? "阶段未知";
  const next =
    market?.is_trading_day === false && market.next_trading_day
      ? `下一交易日 ${formatTradeDate(market.next_trading_day)}`
      : null;
  const unknown = market?.phase === "unknown" ? "交易日历暂时读不到，无法判断今天是否开市" : null;
  return (
    <>
      <Tip content={next ?? unknown} placement="bottom">
        <span className="phase" data-phase={market ? phaseTone(market.phase) : "close"}>
          <span className="pdot" aria-hidden="true" />
          <span className="sr-only">市场阶段：</span>
          <span>{label}</span>
        </span>
      </Tip>
      {next ? <span className="tb-next long">{next}</span> : null}
    </>
  );
}

export function Topbar({ meta, metaReceivedAt, metaFailed }: TopbarProps) {
  const { mode, cycle } = useTheme();
  return (
    <header className="topbar">
      <Link className="brand" to={HOME_PATH}>
        <span className="brand-mark" aria-hidden="true">
          <BrandMark />
        </span>
        <span className="brand-name">{APP_TITLE}</span>
      </Link>
      <div className="tb-status">
        <TradeDay meta={meta} />
        <PhasePill meta={meta} />
        <GenerationBadge meta={meta} failed={metaFailed} receivedAt={metaReceivedAt} />
      </div>
      {/* biome-ignore lint/a11y/useSemanticElements: <search> is newer than the Safari 15 build target. */}
      <form className="search is-soon" role="search" onSubmit={(event) => event.preventDefault()}>
        <Tip content="股票搜索即将上线" placement="bottom" className="search-tip">
          <SearchIcon />
          <input
            type="search"
            disabled
            placeholder="代码 / 名称"
            aria-label="搜索股票（即将上线）"
          />
        </Tip>
      </form>
      <Button className="ai-btn" disabledReason="AI 助手即将上线">
        <SparkIcon />
        <span className="lbl">AI 助手</span>
      </Button>
      <button
        className="icon-btn theme-btn"
        type="button"
        aria-label={`主题：${THEME_LABELS[mode]}，点击切换`}
        title={`主题：${THEME_LABELS[mode]}`}
        onClick={cycle}
      >
        <ThemeIcon mode={mode} />
      </button>
      <UserMenu viewer={meta?.data.viewer} />
    </header>
  );
}
