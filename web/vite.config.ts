import { fileURLToPath } from "node:url";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

// Relative base: the built files work under any path prefix. Production serves
// them at /app/ (nginx), the e2e static server mirrors that, and hash routing
// (/app/#/panorama) means no server needs a history fallback.
const API_TARGET = process.env.RQ_WEB_API_TARGET ?? "http://127.0.0.1:8768";

export default defineConfig({
  base: "./",
  plugins: [react()],
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
