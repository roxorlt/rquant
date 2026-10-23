/** Width at which the phone shell (bottom tabs) takes over. Matches tokens v1.1. */
export const PHONE_QUERY = "(max-width: 599px)";

export const DESKTOP_HOME = "/overview";
export const PHONE_HOME = "/pools";

/** Root `/` only. A direct link to a page is never rewritten. */
export function homePath(phone: boolean): typeof DESKTOP_HOME | typeof PHONE_HOME {
  return phone ? PHONE_HOME : DESKTOP_HOME;
}

export function currentHomePath(): typeof DESKTOP_HOME | typeof PHONE_HOME {
  if (typeof window.matchMedia !== "function") return DESKTOP_HOME;
  return homePath(window.matchMedia(PHONE_QUERY).matches);
}
