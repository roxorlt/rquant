import fontLicenseUrl from "@/assets/fonts/OFL.txt?url";
import { type DataColumn, DataTable } from "@/table/DataTable";
import { PageHeader, Panel } from "@/ui";
import packageJson from "../../package.json";

interface LicenseRow {
  name: string;
  license: string;
  url: string;
  note?: string;
}

/** Everything bundled into the page, with its licence (runtime dependencies + fonts). */
const LICENSES: readonly LicenseRow[] = [
  { name: "react", license: "MIT", url: "https://github.com/facebook/react" },
  { name: "react-dom", license: "MIT", url: "https://github.com/facebook/react" },
  { name: "react-router", license: "MIT", url: "https://github.com/remix-run/react-router" },
  { name: "antd", license: "MIT", url: "https://github.com/ant-design/ant-design" },
  { name: "dayjs", license: "MIT", url: "https://github.com/iamkun/dayjs" },
  { name: "@tanstack/react-query", license: "MIT", url: "https://github.com/TanStack/query" },
  { name: "@tanstack/react-table", license: "MIT", url: "https://github.com/TanStack/table" },
  { name: "@tanstack/react-virtual", license: "MIT", url: "https://github.com/TanStack/virtual" },
  {
    name: "openapi-fetch",
    license: "MIT",
    url: "https://github.com/openapi-ts/openapi-typescript",
  },
  { name: "echarts", license: "Apache-2.0", url: "https://github.com/apache/echarts" },
  {
    name: "lightweight-charts",
    license: "Apache-2.0",
    url: "https://github.com/tradingview/lightweight-charts",
    note: "TradingView Lightweight Charts™，Copyright © TradingView, Inc.；图表内保留 TradingView 署名",
  },
  { name: "@xyflow/react", license: "MIT", url: "https://github.com/xyflow/xyflow" },
  { name: "@dagrejs/dagre", license: "MIT", url: "https://github.com/dagrejs/dagre" },
  {
    name: "IBM Plex Mono / IBM Plex Sans Condensed（拉丁子集）",
    license: "OFL-1.1",
    url: fontLicenseUrl,
    note: "Copyright IBM Corp.，经 @fontsource 5.3.0 取得",
  },
];

const versions: Record<string, string> = packageJson.dependencies;

const COLUMNS: readonly DataColumn<LicenseRow>[] = [
  {
    id: "name",
    header: "组件",
    value: (row) => row.name,
    cell: (row) => (
      <a href={row.url} target="_blank" rel="noopener">
        {row.name}
      </a>
    ),
  },
  { id: "version", header: "版本", value: (row) => versions[row.name] ?? "—" },
  { id: "license", header: "许可", value: (row) => row.license },
  { id: "note", header: "说明", value: (row) => row.note ?? "", wrap: true },
];

export default function LicensesPage() {
  return (
    <>
      <PageHeader
        eyebrow="我的"
        title="开源许可"
        note="页面里打包的第三方组件与字体，以及各自的许可。"
      />
      <Panel flush label="开源组件">
        <DataTable rows={LICENSES} columns={COLUMNS} rowKey={(row) => row.name} label="开源许可" />
      </Panel>
    </>
  );
}
