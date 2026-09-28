import { HttpResponse, http } from "msw";
import { server } from "@/test/server";
import { submitBackfillPlanCommand } from "./backfillPlanCommand";
import type { Schemas } from "./client";

it("sends only the typed browser fields with the CSRF header", async () => {
  const body: Schemas["BackfillPlanCommandRequest"] = {
    command_id: "web-test",
    requested_at: "2026-09-27T07:00:00.000Z",
    audit_start: "2024-09-01",
    completed_through: "2025-04-30",
  };
  let sent: unknown = null;
  let csrf: string | null = null;
  server.use(
    http.post("*/api/v1/data/backfill-plans/commands", async ({ request }) => {
      sent = await request.json();
      csrf = request.headers.get("X-Rquant-Csrf");
      return HttpResponse.json({
        command_id: body.command_id,
        status: "queued",
        task_id: "a".repeat(32),
        message: "已排队",
      });
    }),
  );
  const receipt = await submitBackfillPlanCommand(body);
  expect(sent).toEqual(body);
  expect(csrf).toBe("1");
  expect(receipt.status).toBe("queued");
});
