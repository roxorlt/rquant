import { expect, test } from "@playwright/test";
import { API_PORT, APP_URL } from "./env.ts";

test("browser proxy proves its identity and replaces forged browser identity headers", async ({
  request,
}) => {
  const path = "api/v1/monitor/channels";
  const proxiedUrl = new URL(path, APP_URL).toString();
  const normal = await request.get(proxiedUrl);
  expect(normal.status()).toBe(200);

  const forged = await request.get(proxiedUrl, {
    headers: { "x-rquant-user": "attacker", "x-rquant-proxy-proof": "a".repeat(64) },
  });
  expect(forged.status()).toBe(200);
  const normalBody = await normal.json();
  const forgedBody = await forged.json();
  expect(forgedBody.data).toEqual(normalBody.data);
  expect(forgedBody.serving.generation_id).toBe(normalBody.serving.generation_id);

  const direct = await request.get(`http://127.0.0.1:${API_PORT}/api/v1/monitor/channels`, {
    headers: { "x-rquant-user": "e2e", "x-rquant-proxy-proof": "a".repeat(64) },
  });
  expect(direct.status()).toBe(401);
});
