import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

/** Shared settings of the e2e run (playwright.config.ts and the specs). */
export const REPO_ROOT = fileURLToPath(new URL("../..", import.meta.url));
export const API_PORT = Number(process.env.RQ_E2E_API_PORT ?? 18768);
export const WEB_PORT = Number(process.env.RQ_E2E_WEB_PORT ?? 14173);
export const SERVING_ROOT =
  process.env.RQ_E2E_SERVING_ROOT ?? join(tmpdir(), "rquant-web-e2e", "serving");
export const APP_URL = `http://127.0.0.1:${WEB_PORT}/app/`;
/** Runs the repository's Python without re-syncing the environment. */
export const UV_RUN = process.env.RQ_E2E_UV_RUN ?? "uv run --no-sync";
