import { useState } from "react";
import { ApiError } from "@/api/client";
import {
  fetchTdxParse,
  fetchTdxPreview,
  type TdxParseData,
  type TdxPreviewData,
  useTdxPreviewSource,
} from "@/api/screen";
import { Button, RelativeTime, SideDrawer, Tip } from "@/ui";
import { FormulaMarketPanel } from "./FormulaMarketPanel";

type Check = { source: string; data: TdxParseData };
type Preview = { key: string; sourceIdentity: string; data: TdxPreviewData };
type Notice = { key: string; message: string };

interface FormulaPreviewDialogProps {
  onClose: () => void;
}

export function FormulaPreviewDialog({ onClose }: FormulaPreviewDialogProps) {
  const [formula, setFormula] = useState("");
  const [stockCode, setStockCode] = useState("");
  const [selectedDate, setSelectedDate] = useState("");
  const [check, setCheck] = useState<Check | null>(null);
  const [preview, setPreview] = useState<Preview | null>(null);
  const [notice, setNotice] = useState<Notice | null>(null);
  const [checking, setChecking] = useState(false);
  const [previewing, setPreviewing] = useState(false);
  const catalog = useTdxPreviewSource();
  const dates = catalog.data?.dates ?? [];
  const tradeDate = dates.includes(selectedDate) ? selectedDate : (dates[0] ?? null);
  const source = catalog.data?.source ?? null;
  const previewKey = JSON.stringify([formula, stockCode, tradeDate, source?.identity]);
  const checked = check?.source === formula ? check.data : null;
  const currentPreview = preview?.key === previewKey ? preview.data : null;
  const sourceChanged = preview !== null && preview.sourceIdentity !== source?.identity;
  const sourceReady = catalog.data?.available === true && source !== null && tradeDate !== null;
  const stockValid = /^\d{6}\.(?:SH|SZ|BJ)$/.test(stockCode);
  const canPreview = sourceReady && checked?.status === "parsed" && stockValid && !previewing;

  let status: string | null = null;
  let tone = "";
  if (sourceChanged) {
    status = "公式预览数据已更新，请重新预览。";
    tone = "warn";
  } else if (check !== null && check.source !== formula) {
    status = "输入已改，请重新检查并预览。";
    tone = "warn";
  } else if (notice?.key === previewKey || notice?.key === formula) {
    status = notice.message;
    tone = "warn";
  } else if (currentPreview !== null) {
    status =
      currentPreview.status === "match"
        ? "符合"
        : currentPreview.status === "no_match"
          ? "不符合"
          : "暂无法判断";
    tone =
      currentPreview.status === "match" ? "ok" : currentPreview.status === "unknown" ? "warn" : "";
  } else if (preview !== null && check?.source === formula) {
    status = "股票或日期已改，请重新预览。";
    tone = "warn";
  } else if (checked?.status === "rejected") {
    status =
      checked.issues[0]?.message ??
      checked.unsupported[0]?.message ??
      "公式暂无法预览，请修改后重试。";
    tone = "warn";
  } else if (!sourceReady) {
    status = catalog.isLoading ? "正在读取单股数据…" : "单股预览数据暂不可用，请稍后刷新。";
    tone = "warn";
  } else if (checked?.status === "parsed") {
    status = "公式检查通过，可预览或批量运行。";
    tone = "ok";
  }

  async function checkFormula() {
    const submitted = formula;
    setChecking(true);
    setNotice(null);
    try {
      const data = await fetchTdxParse({ source: submitted });
      setCheck({ source: submitted, data });
      setPreview(null);
    } catch (caught) {
      setNotice({
        key: submitted,
        message: caught instanceof Error ? caught.message : "公式暂时无法检查。",
      });
    } finally {
      setChecking(false);
    }
  }

  async function previewStock() {
    if (!canPreview || source === null || tradeDate === null) return;
    const submittedKey = previewKey;
    const sourceIdentity = source.identity;
    setPreviewing(true);
    setNotice(null);
    try {
      const data = await fetchTdxPreview({
        source: formula,
        source_identity: sourceIdentity,
        stock_code: stockCode,
        trade_date: tradeDate,
      });
      setPreview({ key: submittedKey, sourceIdentity, data });
    } catch (caught) {
      if (caught instanceof ApiError && caught.status === 409) {
        setNotice({ key: submittedKey, message: "公式预览数据已更新，请刷新后重新预览。" });
        void catalog.refetch();
      } else {
        setNotice({
          key: submittedKey,
          message: caught instanceof Error ? caught.message : "这只股票暂时无法预览。",
        });
      }
    } finally {
      setPreviewing(false);
    }
  }

  return (
    <SideDrawer open onClose={onClose} title="公式预览" wide>
      <div className="tdx-preview">
        <div className="tdx-preview-source-info">
          <span>
            公式预览数据{" "}
            {source ? <RelativeTime at={source.updated_at} suffix="更新" /> : "暂不可用"}
          </span>
          <Button
            size="sm"
            variant="ghost"
            aria-label="刷新公式预览数据"
            onClick={() => void catalog.refetch()}
          >
            刷新
          </Button>
        </div>
        <label className="field">
          <span className="lbl">通达信公式</span>
          <textarea
            className="inp tdx-preview-source"
            value={formula}
            maxLength={4096}
            spellCheck={false}
            onChange={(event) => setFormula(event.target.value)}
            placeholder="例如 CLOSE > MA(CLOSE, 20)"
          />
        </label>
        <div className="tdx-preview-row">
          <label className="field">
            <span className="lbl">股票代码</span>
            <input
              className="inp num"
              value={stockCode}
              maxLength={9}
              onChange={(event) => setStockCode(event.target.value.toUpperCase())}
              placeholder="600001.SH"
              autoComplete="off"
            />
          </label>
          <label className="field">
            <span className="lbl">数据日期</span>
            <select
              className="inp tdx-preview-date num"
              value={tradeDate ?? ""}
              onChange={(event) => setSelectedDate(event.target.value)}
              disabled={dates.length === 0}
            >
              {dates.length === 0 ? <option value="">暂无日期</option> : null}
              {dates.map((day) => (
                <option key={day} value={day}>
                  {day}
                </option>
              ))}
            </select>
          </label>
        </div>
        <div className="tdx-preview-actions">
          <Tip content="先检查公式是否支持，再用所选日期预览一只股票。">
            <Button
              onClick={() => void checkFormula()}
              disabled={formula.trim() === "" || checking}
            >
              {checking ? "正在检查…" : "检查公式"}
            </Button>
          </Tip>
          <Button
            variant="primary"
            onClick={() => void previewStock()}
            disabledReason={
              !sourceReady
                ? "公式预览数据暂不可用"
                : checked?.status !== "parsed"
                  ? "先检查公式"
                  : !stockValid
                    ? "输入完整股票代码，如 600001.SH"
                    : undefined
            }
            disabled={previewing}
          >
            {previewing ? "正在预览…" : "预览这只股票"}
          </Button>
        </div>
        {status ? (
          <div className={`tdx-preview-result ${tone}`} role="status" aria-live="polite">
            <strong>{status}</strong>
            {currentPreview?.reason ? <span>{currentPreview.reason}</span> : null}
            {currentPreview ? (
              <small>
                公式预览数据 <RelativeTime at={currentPreview.source_updated_at} suffix="更新" />
              </small>
            ) : null}
          </div>
        ) : null}
        <FormulaMarketPanel formula={formula} checked={checked?.status === "parsed"} />
      </div>
    </SideDrawer>
  );
}
