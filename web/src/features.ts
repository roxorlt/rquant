/** Build-time switches. A missing value stays off; set the Vite env to "1" or "true". */

function enabled(value: unknown): boolean {
  return value === "1" || value === "true";
}

/** Saved screener schemes. Production has no scheme store, so this stays off. */
export function screenerSchemesEnabled(): boolean {
  return enabled(import.meta.env.VITE_FEATURE_SCREENER_SCHEMES);
}

/** Undo an alert acknowledgement. Production page-control rejects unack, so this stays off. */
export function alertUnackEnabled(): boolean {
  return enabled(import.meta.env.VITE_FEATURE_ALERT_UNACK);
}
