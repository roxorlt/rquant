#!/usr/bin/env node
// A small stand-in for the production nginx block (plan v2 §2.2), used by the
// Playwright tests: web/dist served under /app/ with the same headers
// (Content-Security-Policy, X-Frame-Options, cache rules), /app → /app/, and
// /app/api/ proxied to the web API with the /app prefix stripped.
//
//   node e2e/static-server.mjs --port 4173 --api http://127.0.0.1:8768
import { createReadStream, existsSync, statSync } from "node:fs";
import { createServer, request } from "node:http";
import { extname, join, normalize, sep } from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";

const { values } = parseArgs({
  options: {
    port: { type: "string", default: "4173" },
    api: { type: "string", default: "http://127.0.0.1:8768" },
    user: { type: "string", default: "e2e" },
  },
});
const root = fileURLToPath(new URL("../dist/", import.meta.url));
const api = new URL(values.api);

const CSP =
  "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; " +
  "img-src 'self' data: blob:; font-src 'self'; connect-src 'self'; frame-src 'self'; " +
  "frame-ancestors 'self'; base-uri 'self'; form-action 'self'";
const TYPES = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".woff2": "font/woff2",
  ".txt": "text/plain; charset=utf-8",
  ".json": "application/json",
  ".svg": "image/svg+xml",
};

function send(response, status, headers, body = "") {
  response.writeHead(status, headers);
  response.end(body);
}

function proxy(clientRequest, clientResponse, path) {
  const upstream = request(
    {
      hostname: api.hostname,
      port: api.port,
      path,
      method: clientRequest.method,
      headers: {
        ...clientRequest.headers,
        host: clientRequest.headers.host ?? api.host,
        "x-rquant-user": values.user,
      },
    },
    (upstreamResponse) => {
      clientResponse.writeHead(upstreamResponse.statusCode ?? 502, upstreamResponse.headers);
      upstreamResponse.pipe(clientResponse);
    },
  );
  upstream.on("error", () =>
    send(clientResponse, 502, { "content-type": "text/plain" }, "bad gateway"),
  );
  clientRequest.pipe(upstream);
}

const server = createServer((req, res) => {
  const url = new URL(req.url ?? "/", "http://localhost");
  if (url.pathname === "/app") {
    return send(res, 301, { location: "/app/" });
  }
  if (url.pathname.startsWith("/app/api/")) {
    return proxy(req, res, url.pathname.slice("/app".length) + url.search);
  }
  if (!url.pathname.startsWith("/app/")) {
    return send(res, 404, { "content-type": "text/plain" }, "not found");
  }
  let relative = decodeURIComponent(url.pathname.slice("/app/".length));
  if (relative === "" || relative.endsWith("/")) {
    relative += "index.html";
  }
  const file = normalize(join(root, relative));
  if (
    !file.startsWith(root.endsWith(sep) ? root : root + sep) ||
    !existsSync(file) ||
    !statSync(file).isFile()
  ) {
    return send(res, 404, { "content-type": "text/plain" }, "not found");
  }
  const immutable = relative.startsWith("assets/");
  const headers = {
    "content-type": TYPES[extname(file)] ?? "application/octet-stream",
    "x-content-type-options": "nosniff",
    "cache-control": immutable ? "private, max-age=31536000, immutable" : "no-cache",
  };
  if (!immutable) {
    Object.assign(headers, {
      "x-frame-options": "SAMEORIGIN",
      "referrer-policy": "same-origin",
      "content-security-policy": CSP,
    });
  }
  res.writeHead(200, headers);
  createReadStream(file).pipe(res);
});

server.listen(Number(values.port), "127.0.0.1", () => {
  console.log(`static /app/ on http://127.0.0.1:${values.port}/app/ → API ${api.href}`);
});
