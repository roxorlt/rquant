import { useParams } from "react-router";
import { NotFound } from "@/app/NotFound";
import { GapStatusReport } from "./GapStatusReport";
import { HtmlReport } from "./HtmlReport";
import { reportById } from "./reports";

export default function ReportPage() {
  const { reportId } = useParams();
  const report = reportById(reportId);
  if (report === undefined) {
    return <NotFound />;
  }
  if (report.kind === "gap-status") {
    return <GapStatusReport />;
  }
  return <HtmlReport title={report.title} date={report.date} file={report.file} />;
}
