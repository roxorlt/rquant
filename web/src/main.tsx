import "./styles/fonts.css";
import "./styles/tokens.css";
import "./styles/base.css";
import "./styles/shell.css";
import "./styles/components.css";
import "./styles/tables.css";
import "./styles/screener.css";
import "./styles/pages.css";
import { installStaticPreview } from "virtual:static-preview";
import { createRoot } from "react-dom/client";
import { App } from "./app/App";
import { hydratePreferences } from "./theme/storage";
import { applyThemeMode, readStoredThemeMode } from "./theme/ThemeProvider";

installStaticPreview();

// Before the first render, so the first paint already uses the stored theme.
applyThemeMode(readStoredThemeMode());

const container = document.getElementById("root");
if (container === null) {
  throw new Error("index.html has no #root element");
}
const root = container;
// Page settings saved on another device arrive before the first render; at most 1.5 s,
// and a missing or failing server just means this browser's copy is used.
void hydratePreferences().finally(() => createRoot(root).render(<App />));
