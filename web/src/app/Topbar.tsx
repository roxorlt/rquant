import { Link } from "react-router";
import type { MetaEnvelope } from "@/api/client";
import { phaseTone } from "@/format/session";
import { shanghaiDate, weekdayOf } from "@/format/time";
import { THEME_LABELS, useTheme } from "@/theme/ThemeProvider";
import { Button } from "@/ui";
import { GenerationBadge } from "./GenerationBadge";
import { BrandMark, MenuIcon, SearchIcon, SparkIcon, ThemeIcon } from "./icons";
import { PHONE_NAV_ID } from "./PhoneNav";
import { APP_TITLE, HOME_PATH } from "./pages";
import { UserMenu } from "./UserMenu";

export interface TopbarProps {
  meta: MetaEnvelope | undefined;
  metaFailed: boolean;
  navOpen: boolean;
  onOpenNav: () => void;
}

function TradeDay({ meta }: { meta: MetaEnvelope | undefined }) {
  const market = meta?.data.market;
  const date = market?.trade_date ?? shanghaiDate(new Date()).date;
  const weekday = market ? weekdayOf(market.trade_date) : shanghaiDate(new Date()).weekday;
  const kind =
    market?.is_trading_day === true ? "交易日" : market?.is_trading_day === false ? "休市" : null;
  return (
    <div className="tb-day">
      <span className="mono">{date}</span> {weekday}
      {kind ? <span className="long"> · {kind}</span> : null}
    </div>
  );
}

function PhasePill({ meta }: { meta: MetaEnvelope | undefined }) {
  const market = meta?.data.market;
  const label = market?.phase_label ?? "阶段未知";
  return (
    <span className="phase" data-phase={market ? phaseTone(market.phase) : "close"}>
      <span className="pdot" aria-hidden="true" />
      <span className="sr-only">市场阶段：</span>
      <span>{label}</span>
    </span>
  );
}

export function Topbar({ meta, metaFailed, navOpen, onOpenNav }: TopbarProps) {
  const { mode, cycle } = useTheme();
  return (
    <header className="topbar">
      <button
        className="icon-btn only-phone"
        type="button"
        aria-label="打开导航"
        aria-controls={PHONE_NAV_ID}
        aria-expanded={navOpen}
        onClick={onOpenNav}
      >
        <MenuIcon />
      </button>
      <Link className="brand" to={HOME_PATH}>
        <span className="brand-mark" aria-hidden="true">
          <BrandMark />
        </span>
        <span className="brand-name">{APP_TITLE}</span>
      </Link>
      <TradeDay meta={meta} />
      <PhasePill meta={meta} />
      <GenerationBadge meta={meta} failed={metaFailed} />
      {/* biome-ignore lint/a11y/useSemanticElements: <search> is newer than the Safari 15 build target. */}
      <form className="search" role="search" onSubmit={(event) => event.preventDefault()}>
        <SearchIcon />
        <input
          type="search"
          disabled
          placeholder="代码 / 名称（M1 开放）"
          aria-label="搜索股票代码或名称（M1 开放）"
        />
      </form>
      <Button className="ai-btn" disabledReason="AI 助手在后续版本开放（v2 排期第 13–16 周）">
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
