import { execFileSync } from "node:child_process";
import { mkdtempSync, readdirSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it } from "vitest";
import { renderApp } from "@/test/render";
import {
  installStaticPreview,
  resetStaticPreviewWrites,
  STATIC_PREVIEW_WRITE_MOCK,
} from "./staticPreview";

const webRoot = process.cwd();
const originalFetch = globalThis.fetch;
const alertId = "a".repeat(64);

/** Names and codes that must not ship in the built pages. */
const REAL_LISTINGS = [
  "贵州茅台",
  "600519",
  "平安银行",
  "000001.SZ",
  "宁德时代",
  "300750",
  "中国平安",
  "601318",
];

const serving = {
  state: "ready",
  generation_id: "preview",
  generated_at: "2026-10-20T07:00:00Z",
  message: null,
};

afterEach(() => {
  resetStaticPreviewWrites();
  globalThis.fetch = originalFetch;
  document.head.querySelector("base")?.remove();
  window.history.pushState({}, "", "/");
});

function setPage(dir: string): void {
  document.head.querySelector("base")?.remove();
  const base = document.createElement("base");
  base.href = `http://localhost:3000/states/${dir}/`;
  document.head.append(base);
}

function stateOf(url: string): string {
  const match = /\/states\/([^/]+)\//.exec(url);
  return match?.[1] ?? "normal";
}

function frozenBody(url: string): { status: number; body: unknown } {
  const state = stateOf(url);
  if (state === "error") return { status: 503, body: { detail: "数据没有准备好" } };
  const path = new URL(url, "http://localhost:3000").pathname;
  const role = state === "viewer" ? "viewer" : "admin";
  const stock =
    state === "empty"
      ? null
      : state === "viewer"
        ? { code: "XQ0001.BJ", name: "星河股份" }
        : state === "stale"
          ? { code: "XQ0002.BJ", name: "北窗科技" }
          : { code: "XQ0003.BJ", name: "青田材料" };
  if (path.endsWith("/api/v1/meta")) {
    return {
      status: 200,
      body: {
        data: {
          app: "rQuant",
          version: "preview",
          role,
          user: state,
          notice: state === "stale" ? "这是一份过期快照" : null,
          generation: {
            ...serving,
            state: state === "stale" ? "stale" : "ready",
            message: state === "stale" ? "数据已过期" : null,
          },
        },
        serving:
          state === "stale" ? { ...serving, state: "stale", message: "数据已过期" } : serving,
      },
    };
  }
  if (path.endsWith("/api/v1/alerts")) {
    return {
      status: 200,
      body: {
        data: {
          items: stock
            ? [
                {
                  alert_id: alertId,
                  at: "2026-10-20T06:38:00Z",
                  code: stock.code,
                  name: stock.name,
                  level: "L1",
                  trade_date: "2026-10-20",
                  trigger_type: "跳空高开",
                  trigger_price: 10,
                  level_price: 9.8,
                  pool: null,
                  acked_at: null,
                  acked_by: null,
                },
              ]
            : [],
        },
        serving,
      },
    };
  }
  if (path.endsWith("/api/v1/surge")) {
    return { status: 200, body: { data: { date: "2026-10-20", items: [], rules: null }, serving } };
  }
  if (path.endsWith("/api/v1/alert-rules")) {
    return { status: 200, body: { data: { rules: [] }, serving } };
  }
  if (path.endsWith("/api/v1/screener-schemes")) {
    return { status: 200, body: { data: { schemes: [] }, serving } };
  }
  if (path.endsWith("/api/v1/pools")) {
    return { status: 200, body: { data: { trade_date: "2026-10-20", pools: [] }, serving } };
  }
  return { status: 200, body: { data: {}, serving } };
}

function installFrozenFiles(): { urls: string[] } {
  const urls: string[] = [];
  globalThis.fetch = async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
    const method = (
      init?.method ?? (input instanceof Request ? input.method : "GET")
    ).toUpperCase();
    urls.push(`${method} ${url}`);
    const frozen = frozenBody(url);
    return new Response(JSON.stringify(frozen.body), {
      status: frozen.status,
      headers: { "content-type": "application/json" },
    });
  };
  installStaticPreview();
  return { urls };
}

function bundleText(outDir: string): string {
  const assets = join(outDir, "assets");
  const files = [join(outDir, "index.html")];
  for (const name of readdirSync(assets)) {
    if (name.endsWith(".js") || name.endsWith(".css") || name.endsWith(".html")) {
      files.push(join(assets, name));
    }
  }
  return files.map((file) => readFileSync(file, "utf8")).join("\n");
}

function build(mode: string, outDir: string): void {
  execFileSync(
    "pnpm",
    ["exec", "vite", "build", "--mode", mode, "--outDir", outDir, "--emptyOutDir"],
    { cwd: webRoot, stdio: "pipe", timeout: 180_000 },
  );
}

describe("static preview reads frozen files", () => {
  it("issues GET against the frozen file for each preview state", async () => {
    const { urls } = installFrozenFiles();
    const viewer = await fetch("http://localhost:3000/states/viewer/api/v1/meta");
    const empty = await fetch("http://localhost:3000/states/empty/api/v1/alerts");
    const errored = await fetch("http://localhost:3000/states/error/api/v1/overview");
    const stale = await fetch("http://localhost:3000/states/stale/api/v1/meta");

    expect(urls.filter((line) => line.startsWith("GET"))).toEqual([
      "GET http://localhost:3000/states/viewer/api/v1/meta",
      "GET http://localhost:3000/states/empty/api/v1/alerts",
      "GET http://localhost:3000/states/error/api/v1/overview",
      "GET http://localhost:3000/states/stale/api/v1/meta",
    ]);
    expect(viewer.status).toBe(200);
    expect(await viewer.json()).toMatchObject({ data: { role: "viewer", user: "viewer" } });
    expect(empty.status).toBe(200);
    expect(((await empty.json()) as { data: { items: unknown[] } }).data.items).toEqual([]);
    expect(errored.status).toBe(503);
    expect(stale.status).toBe(200);
    expect(await stale.json()).toMatchObject({ data: { notice: "这是一份过期快照" } });
  });

  it("keeps a confirmed alert confirmed after the frozen file is read again", async () => {
    installFrozenFiles();
    const file = "http://localhost:3000/states/normal/api/v1/alerts";
    const before = (await (await fetch(file)).json()) as {
      data: { items: { acked_at: string | null }[] };
    };
    expect(before.data.items[0]?.acked_at).toBeNull();

    const ack = await fetch("http://localhost:3000/states/normal/api/v1/alerts/ack", {
      method: "POST",
      body: JSON.stringify({ alert_id: alertId }),
    });
    expect(ack.status).toBe(200);

    const refreshed = (await (await fetch(file)).json()) as {
      data: { items: { acked_at: string | null; name: string }[] };
    };
    expect(refreshed.data.items[0]?.acked_at).toBeTruthy();
    expect(refreshed.data.items[0]?.name).toBe("青田材料");

    const again = (await (await fetch(file)).json()) as {
      data: { items: { acked_at: string | null }[] };
    };
    expect(again.data.items[0]?.acked_at).toBe(refreshed.data.items[0]?.acked_at);
  });

  it("answers writes with 503 when the page asks to fail, and does not keep that write", async () => {
    installFrozenFiles();
    window.history.pushState({}, "", "/preview.html?fail=1");
    const failed = await fetch("http://localhost:3000/states/normal/api/v1/alerts/ack", {
      method: "POST",
      body: JSON.stringify({ alert_id: alertId }),
    });
    expect(failed.status).toBe(503);
    const read = (await (
      await fetch("http://localhost:3000/states/normal/api/v1/alerts")
    ).json()) as { data: { items: { acked_at: string | null }[] } };
    expect(read.data.items[0]?.acked_at).toBeNull();
  });

  it("shows the read-only banner from the viewer file, not a hard-coded admin role", async () => {
    setPage("viewer");
    installFrozenFiles();
    renderApp("/monitor");
    expect(await screen.findByText("当前是只读账号，可以查看，不能修改")).toBeInTheDocument();
    expect(await screen.findByText("星河股份")).toBeInTheDocument();
    expect(screen.queryByText("青田材料")).not.toBeInTheDocument();
    expect(screen.queryByText("贵州茅台")).not.toBeInTheDocument();
  });

  it("keeps the empty preview empty", async () => {
    setPage("empty");
    installFrozenFiles();
    renderApp("/monitor");
    expect(await screen.findByText("今天还没有告警。规则在下方调整。")).toBeInTheDocument();
    expect(screen.queryByText("星河股份")).not.toBeInTheDocument();
    expect(screen.queryByText("当前是只读账号，可以查看，不能修改")).not.toBeInTheDocument();
  });

  it("shows the error preview from the failed frozen read", async () => {
    setPage("error");
    installFrozenFiles();
    renderApp("/overview");
    expect(await screen.findByText("暂时读不到数据")).toBeInTheDocument();
    expect(screen.queryByText("星河股份")).not.toBeInTheDocument();
  });

  it("returns success only for the allowlisted writes", async () => {
    installFrozenFiles();
    const allowed = [
      ["POST", "/api/v1/alerts/ack"],
      ["POST", "/api/v1/alerts/unack"],
      ["POST", "/api/v1/watchlist"],
      ["PUT", "/api/v1/screener-schemes"],
      ["DELETE", "/api/v1/screener-schemes"],
      ["POST", "/api/v1/pools"],
      ["POST", "/api/v1/alert-rules"],
    ] as const;
    for (const [method, path] of allowed) {
      const response = await fetch(`http://localhost:3000/states/normal${path}`, {
        method,
        body: JSON.stringify({ name: "演示", alert_id: alertId, code: "XQ0003.BJ", rule_id: "r1" }),
      });
      expect(response.status, `${method} ${path}`).toBe(200);
    }
    const blocked = [
      ["POST", "/api/v1/query"],
      ["POST", "/api/v1/formula/translate"],
      ["POST", "/api/v1/formula/from-text"],
      ["POST", "/api/v1/portfolio-backtests/run-1/explain"],
    ] as const;
    for (const [method, path] of blocked) {
      const response = await fetch(`http://localhost:3000/states/normal${path}`, {
        method,
        body: JSON.stringify({ sql: "SELECT 1", tdx: "C>0", text: "站上均线" }),
      });
      expect(response.status, `${method} ${path}`).toBe(503);
    }
  });

  it("shows the query error notice when a query write is refused", async () => {
    setPage("normal");
    installFrozenFiles();
    renderApp("/query");
    expect(await screen.findByRole("button", { name: "运行" })).toBeInTheDocument();
    screen.getByRole("button", { name: "运行" }).click();
    expect(await screen.findByText("查询没有运行")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "重试" })).toBeInTheDocument();
    expect(screen.queryByText("有 0 处不认识")).not.toBeInTheDocument();
  });

  it("shows the formula error notice when translation is refused", async () => {
    setPage("normal");
    installFrozenFiles();
    renderApp("/screener");
    const translate = await screen.findByRole("button", { name: "翻译" });
    translate.click();
    expect(await screen.findByText(/公式没翻译上/)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "重试" })).toBeInTheDocument();
    expect(screen.queryByText("有 0 处不认识")).not.toBeInTheDocument();
  });

  it("still shows 已确认 after the alerts query is refreshed", async () => {
    setPage("normal");
    installFrozenFiles();
    const view = renderApp("/monitor");
    expect(await screen.findByText("青田材料")).toBeInTheDocument();
    expect(screen.getByText("未确认")).toBeInTheDocument();
    screen.getByRole("button", { name: "确认" }).click();
    await waitFor(() => expect(screen.getByText("已确认")).toBeInTheDocument());
    await view.queryClient.invalidateQueries({ queryKey: ["alerts"] });
    await waitFor(() => expect(screen.getByText("已确认")).toBeInTheDocument());
    expect(screen.queryByText("未确认")).not.toBeInTheDocument();
  });

  it("is absent from the production bundle, and the preview bundle has no real listings", () => {
    const prod = mkdtempSync(join(tmpdir(), "rquant-prod-"));
    const preview = mkdtempSync(join(tmpdir(), "rquant-preview-"));
    try {
      build("production", prod);
      build("static-preview", preview);
      const prodText = bundleText(prod);
      const previewText = bundleText(preview);
      expect(prodText).not.toContain(STATIC_PREVIEW_WRITE_MOCK);
      expect(previewText).toContain(STATIC_PREVIEW_WRITE_MOCK);
      for (const name of REAL_LISTINGS) {
        expect(previewText, name).not.toContain(name);
        expect(prodText, name).not.toContain(name);
      }
    } finally {
      rmSync(prod, { recursive: true, force: true });
      rmSync(preview, { recursive: true, force: true });
    }
  }, 240_000);
});
