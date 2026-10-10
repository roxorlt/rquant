import { QueryClient } from "@tanstack/react-query";

/** A query client for tests: no retries, nothing kept between tests. */
export function testQueryClient(): QueryClient {
  return new QueryClient({ defaultOptions: { queries: { retry: false, gcTime: 0 } } });
}
