import { Link } from "react-router";
import { PageHeader } from "@/ui";
import { HOME_PATH } from "./pages";

export function NotFound() {
  return (
    <>
      <PageHeader
        eyebrow="页面不存在"
        title="没有这个页面"
        note="地址可能写错了，或者这个页面已经改名。"
      />
      <p>
        <Link to={HOME_PATH}>回到总览</Link>
      </p>
    </>
  );
}
