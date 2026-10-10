import type { Schemas } from "./client";

test("CSV uses the same values, escapes quotes, and prevents formula prefixes", async () => {
  const modules = import.meta.glob("./researchQuery.ts");
  const loader = modules["./researchQuery.ts"];
  expect(loader, "query result serializer is not implemented").toBeDefined();
  const query = (await loader?.()) as {
    queryResultCsv: (result: Schemas["QueryResult"]) => string;
  };
  const result: Schemas["QueryResult"] = {
    status: "ready",
    elapsed_ms: 1,
    message: "",
    columns: [
      { name: "same", data_type: "VARCHAR" },
      { name: "same", data_type: "VARCHAR" },
    ],
    rows: [
      ["=1+1", 'a,"b\n'],
      [" \t@sum(1)", { kind: "decimal", text: "1.20" }],
    ],
  };
  expect(query.queryResultCsv(result)).toContain("'=1+1");
  expect(query.queryResultCsv(result)).toContain("' \t@sum(1)");
  expect(query.queryResultCsv(result)).toContain('"a,""b\n"');
  expect(query.queryResultCsv(result)).toContain("1.20");
});
