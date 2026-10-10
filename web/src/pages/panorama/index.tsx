import { useState } from "react";
import { useSearchParams } from "react-router";
import { usePulse, useRefreshPanorama, useStockSearch } from "@/api/endpoints";
import { useCurrentMeta } from "@/api/useMeta";
import { StockNewsDigest } from "@/app/StockNewsDigest";
import { formatTradeDate } from "@/format/time";
import { Button, EmptyState, PageHeader, Panel, SkeletonRows, Tabs } from "@/ui";
import { MarketTab } from "./MarketTab";
import { SurgeTab } from "./SurgeTab";

function NewsResearch({ viewer }: { viewer: string | null }) {
  const [input, setInput] = useState("");
  const [query, setQuery] = useState("");
  const [selected, setSelected] = useState<string | null>(null);
  const search = useStockSearch(query);
  return (
    <Panel title="核对个股原文">
      <form
        className="row"
        onSubmit={(event) => {
          event.preventDefault();
          setSelected(null);
          setQuery(input.trim());
        }}
      >
        <label className="field">
          <span className="lbl">股票名称或代码</span>
          <input
            className="inp"
            value={input}
            maxLength={64}
            onChange={(event) => setInput(event.target.value)}
          />
        </label>
        <Button type="submit" disabled={!input.trim() || search.isFetching}>
          查找股票
        </Button>
      </form>
      {search.isLoading ? (
        <SkeletonRows rows={2} />
      ) : search.error ? (
        <EmptyState title="股票资料暂不可用" hint={search.error.message} />
      ) : query && search.data?.available ? (
        <section className="row" aria-label="选择研究股票">
          {search.data.rows.map((stock) => (
            <Button
              key={stock.ts_code}
              aria-pressed={selected === stock.ts_code}
              onClick={() => setSelected(stock.ts_code)}
            >
              {stock.name} · {stock.ts_code}
            </Button>
          ))}
          {!search.data.rows.length ? (
            <EmptyState title="没有找到这只股票" hint="换一个名称或完整代码再查找。" />
          ) : null}
          {search.data.truncated ? (
            <p className="hint">结果较多，请输入更完整的名称或代码。</p>
          ) : null}
        </section>
      ) : null}
      <StockNewsDigest viewer={viewer} stockCode={selected} />
    </Panel>
  );
}

export default function PanoramaPage() {
  const [params, setParams] = useSearchParams();
  const tab = params.get("tab") === "surge" ? "surge" : "market";
  const refresh = useRefreshPanorama();
  const pulse = usePulse();
  const day = pulse.data?.trade_date ?? null;
  const meta = useCurrentMeta().data?.data;
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
      <NewsResearch
        key={`${meta?.viewer}:${meta?.generation?.generation_id}`}
        viewer={meta?.viewer ?? null}
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
