/* Presentation helpers. These observations do not authorize route changes. */
(() => {
  "use strict";
  const finite = value => typeof value === "number" && Number.isFinite(value);
  const ageValid = (timestamp, now) => finite(timestamp) && now - timestamp >= 0 && now - timestamp <= 180;
  const flowStates = new Set(["FLOWING", "CONNECTED", "STALLED", "DISCONNECTED", "UNKNOWN"]);
  const flow = value => ({
    status: flowStates.has(value?.status) ? value.status : "UNKNOWN",
    mbps: finite(value?.mbps) && value.mbps >= 0 ? value.mbps : null,
  });

  function delivery(relay, prom, now = Date.now() / 1000) {
    const observed = Date.parse(relay?.observed_at) / 1000;
    const fresh = relay?.schema === "stream.relay_public.v1" && relay.status === "OBSERVED"
      && relay.source_fresh === true && ageValid(relay.generated_at, now) && ageValid(observed, now);
    const links = {
      input: flow(fresh ? relay.input : null),
      youtube: flow(fresh ? relay.destinations?.youtube : null),
      twitch: flow(fresh ? relay.destinations?.twitch : null),
    };
    const promFresh = prom?.schema === "stream-v3-prometheus-public.v1" && ageValid(prom.generated_at, now);
    const clear = id => {
      const item = prom?.items?.find(row => row.id === id);
      return item?.status === "ok" && item.value === 0;
    };
    const youtubeObserved = promFresh && ["ingest_issue", "youtube_issue", "same_url_issue"].every(clear);
    const relayUnavailable = fresh && (relay.relay?.status === "STOPPED"
      || ["STALLED", "DISCONNECTED"].includes(links.input.status));
    const consistent = promFresh && fresh && Math.abs(prom.generated_at - observed) <= 90;
    const inferred = relayUnavailable && links.youtube.status !== "FLOWING" && youtubeObserved && consistent;
    const primary = fresh && links.input.status === "FLOWING";
    const direct = inferred ? "INFERRED" : primary ? "STANDBY" : "UNKNOWN";
    const badge = inferred ? "DIRECT PATH INFERRED" : primary ? "RELAY INPUT OBSERVED" : "ROUTE NOT CONFIRMED";
    const note = inferred
      ? "Relay delivery is not progressing while YouTube ingest and public delivery are observed. The direct path is inferred; route selection and direct bitrate are not published."
      : primary
        ? "Arrows show each link's own counter progress. The direct route is a standby path; its bitrate is not measured here."
        : "Current route cannot be confirmed. Missing relay observations do not establish that the direct path is active.";
    return {fresh, links, direct, badge, note, youtubeObserved};
  }

  function series(points, options = {}) {
    const empty = {path: "", markers: [], samples: 0, gaps: 0};
    if (!Array.isArray(points) || !points.length || points.some(point => !finite(point?.ts))) return empty;
    const ordered = [...points].sort((a, b) => a.ts - b.ts);
    const unique = [];
    for (const point of ordered) {
      const value = finite(point.value) ? point.value : null;
      const previous = unique[unique.length - 1];
      if (previous?.ts === point.ts) {
        if (previous.value !== value) previous.value = null;
      } else unique.push({ts: point.ts, value});
    }
    const end = finite(options.end) ? options.end : unique[unique.length - 1].ts;
    const start = finite(options.window) && options.window > 0 ? end - options.window : unique[0].ts;
    const visible = unique.filter(point => point.ts >= start && point.ts <= end);
    const values = visible.filter(point => finite(point.value)).map(point => point.value);
    if (!values.length) return empty;
    const min = Math.min(...values), max = Math.max(...values);
    const step = finite(options.step) && options.step > 0 ? options.step : null;
    let pen = false, previous = null, gaps = 0;
    const commands = [], markers = [];
    for (const point of visible) {
      if (!finite(point.value)) {pen = false; previous = null; gaps++; continue;}
      if (previous !== null && step !== null && point.ts - previous > step * 1.5) {pen = false; gaps++;}
      const x = (point.ts - start) / Math.max(1, end - start) * 120;
      const y = max === min ? 19 : 34 - (point.value - min) / (max - min) * 30;
      commands.push(`${pen ? "L" : "M"}${x.toFixed(1)},${y.toFixed(1)}`);
      markers.push({x, y}); pen = true; previous = point.ts;
    }
    return {path: commands.join(" "), markers, samples: markers.length, gaps};
  }

  window.TokyoGraphs = Object.freeze({finite, delivery, series});
})();
