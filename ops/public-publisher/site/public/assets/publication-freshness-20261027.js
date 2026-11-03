// Shared by the public page and its external publication-health check.
// These limits describe snapshot delivery, not stream availability or SLOs.
export const PUBLIC_SOURCES = Object.freeze({
  prom: {file: "stream-v3-prometheus.json", schema: "stream-v3-prometheus-public.v1", label: "Monitoring", live: true, lists: ["items"]},
  loki: {file: "stream-v3-loki.json", schema: "stream-v3-loki-public.v1", label: "Event history", lists: ["sections"]},
  rel: {file: "reliability-indicators.json", schema: "stream-v3-reliability-public.v6", label: "Reliability", lists: ["items", "tiers"]},
  relay: {file: "relay-observation.json", schema: "stream.relay_public.v1", label: "Relay observation", live: true},
  twitch: {file: "twitch-observation.json", schema: "stream-v3-twitch-public.v1", label: "Twitch observation", live: true},
  tsli: {file: "twitch-sli.json", schema: "stream.twitch_sli_public.v1", label: "Twitch history", lists: ["windows", "timeline"]},
  clock: {file: "clock-observation.json", schema: "stream.clock_observation_public.v1", label: "Clock observations", lists: ["sources"]},
  egress: {file: "oci-egress.json", schema: "stream.oci_egress_public.v1", label: "Data transfer", lists: ["days"]},
});
export const MAX_SNAPSHOT_AGE_SECONDS = 180;
export const FUTURE_TOLERANCE_SECONDS = 30;

export function publicationState(freshness, pageAvailable = true) {
  const issues = Object.keys(PUBLIC_SOURCES).filter(key => freshness?.[key]?.state !== "fresh");
  return {
    liveDataReady: pageAvailable && !issues.some(key => PUBLIC_SOURCES[key].live),
    observationsComplete: pageAvailable && issues.length === 0,
    issues,
  };
}

export function snapshotFreshness(key, value, nowSeconds) {
  const contract = PUBLIC_SOURCES[key];
  if (!contract) throw new Error("Unknown public source");
  let state = "missing", generatedAt = null, ageSeconds = null;
  if (value !== null && value !== undefined) {
    if (typeof value !== "object" || Array.isArray(value) || value.schema !== contract.schema
      || (contract.lists || []).some(name => !Array.isArray(value[name]))) {
      state = "invalid_schema";
    } else {
      const stamp = value.generated_at;
      const parsed = typeof stamp === "number" ? stamp
        : typeof stamp === "string" && /^\d{4}-\d{2}-\d{2}T/.test(stamp) ? Date.parse(stamp) / 1000 : NaN;
      if (!Number.isFinite(parsed) || parsed <= 0 || !Number.isFinite(nowSeconds)) {
        state = "invalid_timestamp";
      } else {
        generatedAt = parsed;
        ageSeconds = nowSeconds - parsed;
        state = ageSeconds < -FUTURE_TOLERANCE_SECONDS ? "future"
          : ageSeconds > MAX_SNAPSHOT_AGE_SECONDS ? "stale" : "fresh";
      }
    }
  }
  return {state, generatedAt, ageSeconds};
}

export function snapshotView(raw, nowSeconds) {
  const view = {freshness: {}};
  for (const key of Object.keys(PUBLIC_SOURCES)) {
    const value = raw?.[key];
    const result = snapshotFreshness(key, value, nowSeconds);
    view.freshness[key] = result;
    view[key] = result.state === "fresh" ? value : null;
  }
  return view;
}
