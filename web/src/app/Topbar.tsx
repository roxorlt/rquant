import { type FormEvent, useEffect, useRef, useState } from "react";
import { Link, useMatches, useNavigate } from "react-router";
import type { MetaEnvelope } from "@/api/client";
import { useAlerts, useHealth } from "@/api/endpoints";
import { fullCode } from "@/format/code";
import { formatShanghaiDateTime, shanghaiDate } from "@/format/time";
import { THEME_LABELS, useTheme } from "@/theme/ThemeProvider";
import { currentHomePath } from "./home";
import { BellIcon, BrandMark, SearchIcon, ThemeIcon } from "./icons";
import { APP_TITLE } from "./pages";
import { HEALTH_LABEL, unreadAlertCount, worstHealth } from "./status";

function isTitled(value: unknown): value is { title: string } {
  return (
    typeof value === "object" &&
    value !== null &&
    "title" in value &&
    typeof value.title === "string"
  );
}

function usePageTitle(): string {
  const matches = useMatches();
  const handle = [...matches].reverse().find((match) => isTitled(match.handle))?.handle;
  return isTitled(handle) ? handle.title : APP_TITLE;
}

export function Topbar({ meta, failed }: { meta: MetaEnvelope | undefined; failed: boolean }) {
  const { mode, cycle } = useTheme();
  const navigate = useNavigate();
  const pageTitle = usePageTitle();
  const health = useHealth();
  const alerts = useAlerts();
  const [q, setQ] = useState("");
  const [searchOpen, setSearchOpen] = useState(false);
  const searchRef = useRef<HTMLInputElement>(null);
  const today = shanghaiDate(new Date());
  const go = (event: FormEvent) => {
    event.preventDefault();
    const code = q.trim();
    const full = fullCode(code);
    if (full) navigate(`/stock/${full}`);
    else if (code) navigate(`/screener?q=${encodeURIComponent(code)}`);
    setSearchOpen(false);
  };
  const generation = meta?.data.generation;
  const dataTime = failed
    ? "未连接"
    : generation?.generated_at
      ? formatShanghaiDateTime(generation.generated_at)
      : "读取中";
  const text = failed ? "数据 未连接" : `数据 ${dataTime}`;
  const tone = worstHealth((health.data?.layers ?? []).map((layer) => layer.state));
  const healthLabel = HEALTH_LABEL[tone];
  const unread = unreadAlertCount(alerts.data?.items ?? []);

  useEffect(() => {
    if (searchOpen) searchRef.current?.focus();
  }, [searchOpen]);

  return (
    <header className="topbar">
      <Link
        className="brand"
        to={currentHomePath()}
        onClick={(event) => {
          event.preventDefault();
          navigate(currentHomePath());
        }}
      >
        <span className="brand-mark" aria-hidden="true">
          <BrandMark />
        </span>
        <span className="brand-name">{APP_TITLE}</span>
      </Link>
      <h1 className="tb-title">{pageTitle}</h1>
      <span className="tb-date">{`${today.date} ${today.weekday}`}</span>
      <div className="tb-status">
        <span
          className="gen-tag"
          data-state={failed ? "unavailable" : (meta?.serving.state ?? "loading")}
        >
          <span className="gdot" aria-hidden="true" />
          {text}
        </span>
        {meta?.data.notice ? (
          <span className="gen-tag" data-state="stale" role="status">
            {meta.data.notice}
          </span>
        ) : null}
      </div>
      <search className="search">
        <form onSubmit={go}>
          <input
            type="search"
            aria-label="搜索股票"
            placeholder="代码 / 名称，如 600519 或 茅台"
            title="按 / 或 Ctrl+K 快速跳转"
            value={q}
            onChange={(e) => setQ(e.target.value)}
          />
          <kbd className="search-kbd">/</kbd>
        </form>
      </search>
      <Link
        className={`hstat ${tone}`}
        to="/health"
        aria-label={`系统状态：${healthLabel}，数据 ${dataTime}`}
        title={`数据 ${dataTime}`}
      >
        <i />
        <span className="hstat-text">{healthLabel}</span>
      </Link>
      <Link
        className="icon-btn bell"
        to="/monitor"
        aria-label={unread > 0 ? `告警，${unread} 条未读` : "告警，没有未读"}
      >
        <BellIcon />
        {unread > 0 ? <span className="badge">{unread > 99 ? "99+" : unread}</span> : null}
      </Link>
      <button
        className="icon-btn search-open only-phone"
        type="button"
        aria-label="搜索"
        onClick={() => setSearchOpen(true)}
      >
        <SearchIcon />
      </button>
      <button
        className="icon-btn theme-btn"
        type="button"
        aria-label={`主题：${THEME_LABELS[mode]}，点击切换`}
        title={`主题：${THEME_LABELS[mode]}`}
        onClick={cycle}
      >
        <ThemeIcon mode={mode} />
      </button>
      {searchOpen ? (
        <div
          className="search-screen"
          role="dialog"
          aria-modal="true"
          aria-label="搜索"
          onKeyDown={(event) => {
            if (event.key === "Escape") setSearchOpen(false);
          }}
        >
          <form onSubmit={go}>
            <input
              ref={searchRef}
              className="search-screen-input"
              type="search"
              enterKeyHint="search"
              aria-label="搜索股票"
              placeholder="代码 / 名称"
              value={q}
              onChange={(e) => setQ(e.target.value)}
            />
            <button className="btn" type="button" onClick={() => setSearchOpen(false)}>
              关闭
            </button>
          </form>
        </div>
      ) : null}
    </header>
  );
}
