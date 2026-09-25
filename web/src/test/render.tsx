import { render } from "@testing-library/react";
import { createMemoryRouter, RouterProvider } from "react-router";
import { AppProviders } from "@/app/App";
import { appRoutes } from "@/app/routes";
import { testQueryClient } from "./queryClient";

/** The whole app (providers + real routes) at `path`, on a memory router. */
export function renderApp(path = "/overview") {
  const router = createMemoryRouter(appRoutes, { initialEntries: [path] });
  const utils = render(
    <AppProviders queryClient={testQueryClient()}>
      <RouterProvider router={router} />
    </AppProviders>,
  );
  return { ...utils, router };
}
