import { Link } from "react-router";
import { PageHeader, Panel } from "@/ui";
import { REPORTS } from "./reports";

export default function ReportsPage() {
  return (
    <>
      <PageHeader
        eyebrow="我的"
        title="报告"
        note="评估材料，不在左侧导航里。差距总览随每个发布列车更新；调研报告是固定快照，以后有新调研会按日期另加一份。"
      />
      <Panel flush label="报告列表">
        <ul className="report-list">
          {REPORTS.map((report) => (
            <li key={report.id}>
              <span className="cell2">
                <Link className="t" to={`/reports/${report.id}`}>
                  {report.title}
                </Link>
                <span className="hint">{report.summary}</span>
              </span>
              <span className="mono small muted">{report.date}</span>
            </li>
          ))}
        </ul>
      </Panel>
    </>
  );
}
