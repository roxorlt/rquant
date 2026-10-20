import { fileURLToPath } from "node:url";
import react from "@vitejs/plugin-react";
import { defineConfig, type Plugin } from "vite";

const STATIC_PREVIEW_ID = "virtual:static-preview";
const STATIC_PREVIEW_STUB = "\0virtual:static-preview-stub";

/** The write mock is a real module only for `vite build --mode static-preview`. */
function staticPreviewPlugin(): Plugin {
  let mode = "production";
  const real = fileURLToPath(new URL("./src/preview/staticPreview.ts", import.meta.url));
  return {
    name: "rquant-static-preview",
    enforce: "pre",
    configResolved(config) {
      mode = config.mode;
    },
    resolveId(id) {
      if (id !== STATIC_PREVIEW_ID) return null;
      return mode === "static-preview" ? real : STATIC_PREVIEW_STUB;
    },
    load(id) {
      if (id !== STATIC_PREVIEW_STUB) return null;
      return "export function installStaticPreview() {}\n";
    },
  };
}

// Relative base: the built files work under any path prefix. Production serves
// them at /app/ (nginx), the e2e static server mirrors that, and hash routing
// (/app/#/panorama) means no server needs a history fallback.
const API_TARGET = process.env.RQ_WEB_API_TARGET ?? "http://127.0.0.1:8768";

export default defineConfig({
  base: "./",
  plugins: [react(), staticPreviewPlugin()],
  resolve: {
    alias: { "@": fileURLToPath(new URL("./src", import.meta.url)) },
  },
  build: {
    outDir: "dist",
    assetsDir: "assets",
    target: ["chrome100", "safari15"],
    sourcemap: false,
    // Size is guarded by scripts/check-size.mjs; the built-in report only slows builds.
    reportCompressedSize: false,
    chunkSizeWarningLimit: 1500,
  },
  server: {
    port: 5173,
    strictPort: true,
    proxy: { "/api": API_TARGET },
  },
  preview: {
    port: 4173,
    strictPort: true,
    proxy: { "/api": API_TARGET },
  },
});
