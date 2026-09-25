import "./styles/fonts.css";
import "./styles/tokens.css";
import "./styles/base.css";
import "./styles/shell.css";
import "./styles/components.css";
import { createRoot } from "react-dom/client";
import { App } from "./app/App";
import { applyThemeMode, readStoredThemeMode } from "./theme/ThemeProvider";

// Before the first render, so the first paint already uses the stored theme.
applyThemeMode(readStoredThemeMode());

const container = document.getElementById("root");
if (container === null) {
  throw new Error("index.html has no #root element");
}
createRoot(container).render(<App />);
