import { type ReactNode, StrictMode } from "react";
import { createHashRouter, RouterProvider } from "react-router";
import { ApiProvider, type QueryClient } from "@/api/ApiProvider";
import { ThemeProvider } from "@/theme/ThemeProvider";
import { UiProvider } from "@/ui";
import { appRoutes } from "./routes";

export function AppProviders({
  children,
  queryClient,
}: {
  children: ReactNode;
  queryClient?: QueryClient;
}) {
  return (
    <ThemeProvider>
      <UiProvider>
        <ApiProvider client={queryClient}>{children}</ApiProvider>
      </UiProvider>
    </ThemeProvider>
  );
}

const router = createHashRouter(appRoutes);

export function App() {
  return (
    <StrictMode>
      <AppProviders>
        <RouterProvider router={router} />
      </AppProviders>
    </StrictMode>
  );
}
