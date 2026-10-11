import { type KeyboardEvent, useEffect, useMemo, useRef, useState } from "react";
import { useNavigate } from "react-router";
import { PAGES } from "./pages";

interface Command {
  key: string;
  label: string;
  hint: string;
  run: () => void;
}

function isTyping(target: EventTarget | null): boolean {
  if (!(target instanceof HTMLElement)) return false;
  const tag = target.tagName;
  return tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT" || target.isContentEditable;
}

/**
 * Ctrl/⌘+K or "/" opens a palette: jump to any page or search a stock code / name.
 * Arrows move, Enter runs, Esc closes. No dependency beyond the router.
 */
export function CommandPalette() {
  const navigate = useNavigate();
  const [open, setOpen] = useState(false);
  const [q, setQ] = useState("");
  const [active, setActive] = useState(0);
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    const onKey = (event: globalThis.KeyboardEvent) => {
      const combo = (event.ctrlKey || event.metaKey) && event.key.toLowerCase() === "k";
      if (combo || (event.key === "/" && !isTyping(event.target))) {
        event.preventDefault();
        setQ("");
        setActive(0);
        setOpen(true);
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);

  useEffect(() => {
    if (open) inputRef.current?.focus();
  }, [open]);

  const commands = useMemo<Command[]>(() => {
    const term = q.trim().toLowerCase();
    const close = () => setOpen(false);
    const pages = PAGES.filter(
      (p) => !term || p.title.toLowerCase().includes(term) || p.id.includes(term),
    ).map<Command>((p) => ({
      key: p.id,
      label: p.title,
      hint: p.group,
      run: () => {
        close();
        navigate(p.path);
      },
    }));
    if (!term) return pages;
    const stock: Command = {
      key: "stock",
      label: `搜索股票「${q.trim()}」`,
      hint: "选股器",
      run: () => {
        close();
        navigate(`/screener?q=${encodeURIComponent(q.trim())}`);
      },
    };
    // A code-like query (digits / .SH) puts the stock search first.
    return /^[0-9.a-z]+$/.test(term) && /\d/.test(term) ? [stock, ...pages] : [...pages, stock];
  }, [q, navigate]);

  if (!open) return null;

  const onKeyDown = (event: KeyboardEvent<HTMLInputElement>) => {
    if (event.key === "ArrowDown") {
      event.preventDefault();
      setActive((i) => Math.min(i + 1, commands.length - 1));
    } else if (event.key === "ArrowUp") {
      event.preventDefault();
      setActive((i) => Math.max(i - 1, 0));
    } else if (event.key === "Enter") {
      event.preventDefault();
      commands[active]?.run();
    } else if (event.key === "Escape") {
      event.preventDefault();
      setOpen(false);
    }
  };

  return (
    // biome-ignore lint/a11y/noStaticElementInteractions: click-outside closes; Esc is the keyboard path.
    <div className="cmdk-backdrop" onMouseDown={() => setOpen(false)} role="presentation">
      <div
        className="cmdk"
        role="dialog"
        aria-modal="true"
        aria-label="快速跳转"
        onMouseDown={(e) => e.stopPropagation()}
      >
        <input
          ref={inputRef}
          className="cmdk-input"
          aria-label="跳转到页面或搜索股票"
          placeholder="跳转页面，或输入代码 / 名称搜索股票…"
          value={q}
          onChange={(e) => {
            setQ(e.target.value);
            setActive(0);
          }}
          onKeyDown={onKeyDown}
        />
        <ul className="cmdk-list">
          {commands.map((c, i) => (
            <li key={c.key}>
              <button
                type="button"
                className={i === active ? "cmdk-item active" : "cmdk-item"}
                onMouseEnter={() => setActive(i)}
                onClick={c.run}
              >
                <span>{c.label}</span>
                <span className="cmdk-hint">{c.hint}</span>
              </button>
            </li>
          ))}
        </ul>
        <div className="cmdk-foot">↑↓ 选择 · Enter 打开 · Esc 关闭 · Ctrl+K 或 / 随时唤起</div>
      </div>
    </div>
  );
}
