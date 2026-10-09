import { render, screen } from "@testing-library/react";
import { HttpResponse, http } from "msw";
import { AppProviders } from "@/app/App";
import { metaEnvelope } from "@/test/fixtures";
import { testQueryClient } from "@/test/queryClient";
import { server } from "@/test/server";
import { AiUsageDrawer } from "./AiUsageDrawer";

it("shows unknown usage as unknown and clears the prior private account", async () => {
  server.use(
    http.get("*/api/v1/ai/usage", () =>
      HttpResponse.json({
        serving: metaEnvelope().serving,
        data: {
          available: true,
          remaining_calls: 2,
          daily_limit: 3,
          summary: {
            start_date: "2026-09-24",
            end_date: "2026-09-24",
            calls: 1,
            input_tokens: null,
            output_tokens: null,
            known_input_tokens: 12,
            known_output_tokens: 0,
            unknown_usage_calls: 1,
            days: [],
          },
        },
      }),
    ),
  );
  const client = testQueryClient();
  const { rerender } = render(
    <AppProviders queryClient={client}>
      <AiUsageDrawer open viewer="alice" onClose={() => {}} />
    </AppProviders>,
  );
  expect(await screen.findByText("部分用量未知")).toBeInTheDocument();
  rerender(
    <AppProviders queryClient={client}>
      <AiUsageDrawer open viewer={null} onClose={() => {}} />
    </AppProviders>,
  );
  expect(screen.queryByText("部分用量未知")).not.toBeInTheDocument();
  expect(screen.getByText("请先登录")).toBeInTheDocument();
});
