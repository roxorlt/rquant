import { useSearchParams } from "react-router";
import { usePulse, useRefreshPanorama } from "@/api/endpoints";
import { formatTradeDate } from "@/format/time";
import { Button, PageHeader, Tabs } from "@/ui";
import { MarketTab } from "./MarketTab";
import { SurgeTab } from "./SurgeTab";

export default function PanoramaPage() {
  const [params, setParams] = useSearchParams();
  const tab = params.get("tab") === "surge" ? "surge" : "market";
  const refresh = useRefreshPanorama();
  const pulse = usePulse();
  const day = pulse.data?.trade_date ?? null;
  return (
    <>
      <PageHeader
        eyebrow="跟踪与告警"
        title="市场全景"
        note={day ? formatTradeDate(day) : undefined}
        actions={
          <Button size="sm" variant="ghost" onClick={refresh}>
            刷新
          </Button>
        }
      />
      <Tabs
        activeKey={tab}
        onChange={(key) => setParams(key === "surge" ? { tab: "surge" } : {}, { replace: true })}
        items={[
          { key: "market", label: "市场全景", children: <MarketTab /> },
          { key: "surge", label: "爆量记录", children: <SurgeTab today={day} /> },
        ]}
      />
    </>
  );
}
