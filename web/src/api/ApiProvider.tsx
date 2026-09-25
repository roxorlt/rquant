import { QueryClient, QueryClientProvider } from "@tanstack/react-query";

export type { QueryClient };

import { type ReactNode, useState } from "react";

export function createQueryClient(): QueryClient {
  return new QueryClient({
    defaultOptions: {
      queries: {
        // Serving publishes about once a minute and useMeta invalidates on each
        // new generation, so nothing else needs to poll or refetch on focus.
        staleTime: 60_000,
        retry: 1,
        refetchOnWindowFocus: false,
      },
    },
  });
}

export function ApiProvider({ children, client }: { children: ReactNode; client?: QueryClient }) {
  const [queryClient] = useState(() => client ?? createQueryClient());
  return <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>;
}
