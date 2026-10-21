import { ApiError } from "@/api/client";

export interface FailureCopy {
  /** 「{对象}没{动作}上：{原因}。{下一步}」 */
  text: string;
  retry: boolean;
  health: boolean;
}

/** Map a failed write to end-user copy. Never includes HTTP, API, or a status code. */
export function failureCopy(
  object: string,
  verb: string,
  error: unknown,
  attempt = 1,
): FailureCopy {
  const status = error instanceof ApiError ? error.status : 0;
  let reason = "服务没有响应";
  let next = "可以直接重试";
  let retry = true;
  let health = false;
  if (status === 403) {
    reason = "当前账号只能查看";
    next = "";
    retry = false;
  } else if (status === 404 || status === 405 || status === 503) {
    reason = "这项操作暂时用不了";
    next = "稍后再试，或去看系统健康";
    health = true;
  } else if (status === 0) {
    reason = "网络断开";
    next = "恢复后重试";
  }
  if (attempt >= 3) {
    next = "多次没成功，稍后再试或去看系统健康";
    health = true;
  }
  const times = attempt === 2 ? "（第 2 次）" : "";
  const tail = next ? `。${next}` : "";
  const done = verb === "加进池子" ? `${object}没${verb}` : `${object}没${verb}上`;
  return { text: `${done}：${reason}${tail}${times}`, retry, health };
}
