/** Dates and times in the Asia/Shanghai market clock, whatever the viewer's zone. */

export interface ShanghaiDate {
  /** "YYYY-MM-DD" */
  date: string;
  /** "周一" … "周日" */
  weekday: string;
}

const DATE_FORMAT = new Intl.DateTimeFormat("en-CA", {
  timeZone: "Asia/Shanghai",
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
});
const WEEKDAY_FORMAT = new Intl.DateTimeFormat("zh-CN", {
  timeZone: "Asia/Shanghai",
  weekday: "short",
});
const TIME_FORMAT = new Intl.DateTimeFormat("en-GB", {
  timeZone: "Asia/Shanghai",
  hour: "2-digit",
  minute: "2-digit",
  hourCycle: "h23",
});
const DATE_TIME_FORMAT = new Intl.DateTimeFormat("sv-SE", {
  timeZone: "Asia/Shanghai",
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
  hourCycle: "h23",
});

export function shanghaiDate(at: Date): ShanghaiDate {
  return { date: DATE_FORMAT.format(at), weekday: WEEKDAY_FORMAT.format(at) };
}

/** Weekday of a "YYYY-MM-DD" trade date (a calendar date, not an instant). */
export function weekdayOf(isoDate: string): string {
  return WEEKDAY_FORMAT.format(new Date(`${isoDate}T12:00:00+08:00`));
}

/** "HH:MM" in Shanghai. */
export function formatShanghaiTime(at: Date | string): string {
  return TIME_FORMAT.format(typeof at === "string" ? new Date(at) : at);
}

/** "YYYY-MM-DD HH:MM:SS" in Shanghai. */
export function formatShanghaiDateTime(at: Date | string): string {
  return DATE_TIME_FORMAT.format(typeof at === "string" ? new Date(at) : at);
}

/** How long ago, in coarse Chinese units: 刚刚 / 3 分钟前 / 2 小时前 / 5 天前. */
export function formatAge(seconds: number): string {
  if (!Number.isFinite(seconds) || seconds < 60) {
    return "刚刚";
  }
  if (seconds < 3600) {
    return `${Math.floor(seconds / 60)} 分钟前`;
  }
  if (seconds < 86400) {
    return `${Math.floor(seconds / 3600)} 小时前`;
  }
  return `${Math.floor(seconds / 86400)} 天前`;
}
