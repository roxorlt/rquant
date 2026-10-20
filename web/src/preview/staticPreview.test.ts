import { execFileSync } from "node:child_process";
import { mkdtempSync, readdirSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { installStaticPreview, STATIC_PREVIEW_WRITE_MOCK } from "./staticPreview";

const webRoot = process.cwd();
const originalFetch = globalThis.fetch;

afterEach(() => {
  globalThis.fetch = originalFetch;
});

function bundleText(outDir: string): string {
  const assets = join(outDir, "assets");
  return readdirSync(assets)
    .filter((name) => name.endsWith(".js"))
    .map((name) => readFileSync(join(assets, name), "utf8"))
    .join("\n");
}

function build(mode: string, outDir: string): void {
  execFileSync(
    "pnpm",
    ["exec", "vite", "build", "--mode", mode, "--outDir", outDir, "--emptyOutDir"],
    { cwd: webRoot, stdio: "pipe", timeout: 180_000 },
  );
}

describe("static preview write mock", () => {
  it("answers non-GET with success, and 503 when the page asks to fail", async () => {
    installStaticPreview();
    window.history.pushState({}, "", "/preview.html");
    const ok = await fetch("/api/v1/watchlist", { method: "POST", body: "{}" });
    expect(ok.status).toBe(200);
    expect(await ok.json()).toMatchObject({ status: "succeeded" });

    const read = await fetch("/api/v1/screen");
    expect(read.status).toBe(200);
    const screen = (await read.json()) as { data: { rows: unknown[] } };
    expect(screen.data.rows.length).toBeGreaterThan(0);

    window.history.pushState({}, "", "/preview.html?fail=1");
    const failed = await fetch("/api/v1/pools", { method: "POST", body: "{}" });
    expect(failed.status).toBe(503);
    window.history.pushState({}, "", "/preview.html");
  });

  it("is absent from the production bundle and present in the static preview bundle", () => {
    const prod = mkdtempSync(join(tmpdir(), "rquant-prod-"));
    const preview = mkdtempSync(join(tmpdir(), "rquant-preview-"));
    try {
      build("production", prod);
      build("static-preview", preview);
      expect(bundleText(prod)).not.toContain(STATIC_PREVIEW_WRITE_MOCK);
      expect(bundleText(preview)).toContain(STATIC_PREVIEW_WRITE_MOCK);
    } finally {
      rmSync(prod, { recursive: true, force: true });
      rmSync(preview, { recursive: true, force: true });
    }
  }, 240_000);
});
