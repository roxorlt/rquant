import { Link } from "react-router";
import type { MetaEnvelope } from "@/api/client";
import { formatShanghaiDateTime } from "@/format/time";
import { THEME_LABELS, useTheme } from "@/theme/ThemeProvider";
import { BrandMark, ThemeIcon } from "./icons";
import { APP_TITLE, HOME_PATH } from "./pages";

export function Topbar({ meta, failed }: { meta: MetaEnvelope | undefined; failed: boolean }) {
  const { mode, cycle } = useTheme();
  const generation = meta?.data.generation;
  const text = failed
    ? "数据 未连接"
    : generation?.generated_at
      ? `数据 ${formatShanghaiDateTime(generation.generated_at)}`
      : "数据 读取中";
  return (
    <header className="topbar">
      <Link className="brand" to={HOME_PATH}>
        <span className="brand-mark" aria-hidden="true">
          <BrandMark />
        </span>
        <span className="brand-name">{APP_TITLE}</span>
      </Link>
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
      <button
        className="icon-btn theme-btn"
        type="button"
        aria-label={`主题：${THEME_LABELS[mode]}，点击切换`}
        title={`主题：${THEME_LABELS[mode]}`}
        onClick={cycle}
      >
        <ThemeIcon mode={mode} />
      </button>
    </header>
  );
}
