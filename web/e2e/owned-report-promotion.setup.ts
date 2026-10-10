import { expect, test as setup } from "@playwright/test";
import type { Schemas } from "../src/api/client.ts";

setup("read the original owner's completed fixture clock", async ({ request }) => {
  const response = await request.get("/app/api/v1/meta");
  expect(response.status()).toBe(200);
  const envelope: Schemas["Envelope_MetaData_"] = await response.json();
  expect(envelope.serving.state).toBe("ready");
  expect(envelope.data.viewer).toBe("alice");
  expect(envelope.data.generation?.generation_id).toBeTruthy();
  expect(envelope.data.collaboration?.available).toBe(true);
  expect(envelope.data.collaboration?.role).toBe("admin");
  expect(Number.isFinite(Date.parse(envelope.data.server_time))).toBe(true);
  process.env.RQ_E2E_NOW = envelope.data.server_time;
});
