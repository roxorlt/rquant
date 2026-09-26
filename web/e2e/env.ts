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

/**
 * A copy of a replayed production Serving root (never committed: the repo is public).
 * When set, the API serves it instead of the synthetic fixture and the specs expect the
 * replay's rows (6 signals, 6 deliveries, 2 paper holdings on 2026-09-24).
 */
export const REPLAY_ROOT = process.env.RQ_E2E_REPLAY_ROOT ?? null;
/**
 * The API clock starts here and runs (scripts/serve_web_fixture.py): 5 minutes after the
 * synthetic generation (2026-09-24 15:36 in Shanghai, after the close), or 40 seconds
 * after the replay generation (13:24, mid-session).
 */
export const API_NOW =
  process.env.RQ_E2E_NOW ?? (REPLAY_ROOT ? "2026-09-24T05:24:01Z" : "2026-09-24T07:36:00Z");
