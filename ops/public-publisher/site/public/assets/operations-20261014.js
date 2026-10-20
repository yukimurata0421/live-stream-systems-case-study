"use strict";
(() => {
  const byId = id => document.getElementById(id);
  const states = {FLOWING:"Sending", CONNECTED:"Measuring", STALLED:"No progress", DISCONNECTED:"Disconnected", UNKNOWN:"Unknown"};
  const issueText = {RELAY_STOPPED:"Relay process stopped",INPUT_NOT_PROGRESSING:"Relay input is not progressing",YOUTUBE_NOT_PROGRESSING:"YouTube output is not progressing",TWITCH_NOT_PROGRESSING:"Twitch output is not progressing",VM_MEMORY_HIGH:"VM memory usage is high",VM_DISK_LOW:"VM disk free space is low",DAILY_RENEWAL_HELD:"Twitch renewal needs attention",TWITCH_OFFLINE:"Twitch is offline",RELAY_OBSERVATION_UNAVAILABLE:"Relay observation unavailable",TWITCH_API_UNAVAILABLE:"Twitch API observation unavailable"};
  const date = v => {
    const d = new Date(v);
    return v && Number.isFinite(d.getTime()) ? d.toLocaleString("en-GB", {timeZone:"Asia/Tokyo",day:"2-digit",month:"short",hour:"2-digit",minute:"2-digit",hour12:false}) + " JST" : "Not observed";
  };
  const rate = v => TokyoGraphs.finite(v) && v >= 0 ? v.toFixed(2) + " Mbps" : "Not measured";
  const set = (id,text) => {if(byId(id)) byId(id).textContent = text;};
  function render(v, prom) {
    const model = TokyoGraphs.delivery(v, prom);
    const fresh = model.fresh;
    const vm = fresh ? v.vm : {};
    const observed = fresh && vm?.status === "OBSERVED";
    const pill = byId("relayState");
    pill.textContent = observed ? "Observed" : "Unknown";pill.className = "status-pill " + (observed ? "ok" : "unknown");
    for (const [key,id] of [["cpu_percent","relayCpu"],["memory_percent","relayMemory"],["disk_free_percent","relayDisk"]]) {
      const valid = observed && TokyoGraphs.finite(vm[key]) && vm[key] >= 0 && vm[key] <= 100;
      set(id,valid ? vm[key].toFixed(1)+"%" : "—");
      byId(id+"Meter").style.width = valid ? Math.min(100,Math.max(0,vm[key]))+"%" : "0%";
    }
    const uptime = observed && TokyoGraphs.finite(vm.uptime_seconds) && vm.uptime_seconds >= 0 ? vm.uptime_seconds : null;
    set("relayUptime",uptime !== null ? `${Math.floor(uptime/86400)}d ${Math.floor(uptime%86400/3600)}h` : "Unknown");
    set("relayObserved",fresh ? "Checked "+date(v.observed_at) : "Observation unavailable or stale");
    const flows = model.links;
    for (const [key,flow] of Object.entries(flows)) {
      const status = flow?.status || "UNKNOWN";
      document.querySelectorAll(`[data-flow-status="${key}"]`).forEach(el => {
        el.classList.remove("flowing","stalled","disconnected","unknown");el.classList.add(status.toLowerCase());
      });
      document.querySelectorAll(`[data-rate="${key}"]`).forEach(el => {el.textContent = fresh && status !== "UNKNOWN" ? rate(flow?.mbps) : "Not observed";});
      set("relay"+key[0].toUpperCase()+key.slice(1)+"Meta",states[status] || "Unknown");
    }
    for (const [key, state] of Object.entries({...Object.fromEntries(Object.entries(model.links).map(([key, link]) => [key, link.status])), direct: model.direct})) {
      document.querySelectorAll(`[data-route-link="${key}"]`).forEach(node => {
        node.dataset.linkStatus = state;
        if (node.tagName.toLowerCase() === "path") node.setAttribute("marker-end", `url(#${state === "INFERRED" ? "arrowRouteInferred" : ["STALLED", "DISCONNECTED"].includes(state) ? "arrowRouteFault" : state === "FLOWING" ? "arrowNormal" : "arrowFallback"})`);
      });
    }
    const directNote = model.direct === "INFERRED" ? "Direct route inferred · bitrate not measured" : model.direct === "STANDBY" ? "Standby · bitrate not measured" : "Direct route not confirmed";
    set("routeBadge", model.badge); set("routeEvidence", model.note);
    set("directRouteLabel", "Dell → YouTube · " + directNote);
    set("directRouteMeta", directNote);
    document.querySelectorAll("[data-direct-note]").forEach(node => {node.textContent = directNote;});
    document.querySelectorAll('[data-provider-node="youtube"]').forEach(node => {
      node.classList.toggle("inferred", model.direct === "INFERRED");
    });
    set("youtubeRouteNote", model.direct === "INFERRED" ? "Ingest seen · direct inferred" : model.fresh && model.links.youtube.status === "FLOWING" ? "Relay output progressing" : "Delivery not confirmed");
    const due = fresh ? v.daily?.next_renewal_at : null;
    set("relayDaily",due ? date(due) : "Schedule not observed");
    set("relayDailyMeta",fresh && v.daily?.planned_break
      ? "Scheduled Twitch session update in progress. YouTube is monitored independently."
      : model.direct === "INFERRED"
        ? "Twitch renews at 00:00 JST. YouTube delivery is observed; the direct path is inferred."
        : model.fresh && model.links.youtube.status === "FLOWING"
          ? "Twitch renews at 00:00 JST. YouTube relay output is progressing."
          : "Twitch renewal is scheduled separately. The YouTube delivery path is not confirmed.");
    const status = fresh ? v.relay?.status : "UNKNOWN";
    set("relayProcess",status === "RUNNING" ? "Running" : status === "STOPPED" ? "Stopped" : "Unknown");
    set("vmRelayProcess",status === "RUNNING" ? "Running" : status === "STOPPED" ? "Stopped" : "Unknown");
    set("vmOutputs",fresh ? Object.values(v.destinations || {}).filter(x => x.status === "FLOWING").length + " / 2" : "— / 2");
    set("vmRelayRestarts",fresh && Number.isInteger(v.relay?.restart_count) ? String(v.relay.restart_count) : "—");
    const list = byId("relayIssues");list.replaceChildren();
    const issues = fresh ? v.issues : ["RELAY_OBSERVATION_UNAVAILABLE"];
    for (const issue of issues || []) {
      if (!issueText[issue]) continue;
      const el = document.createElement("span");el.className = "signal";el.textContent = issueText[issue];list.appendChild(el);
    }
  }
  let latestRequest = 0;
  async function refresh() {
    const request = ++latestRequest;
    try {
      const read = async name => {
        const r = await fetch("./"+name+"?t="+Date.now(), {cache:"no-store", signal:AbortSignal.timeout(10000)});
        if (!r.ok) throw new Error("unavailable");
        return r.json();
      };
      const result = await Promise.allSettled([read("relay-observation.json"), read("stream-v3-prometheus.json")]);
      if (request === latestRequest) render(result[0].status === "fulfilled" ? result[0].value : null, result[1].status === "fulfilled" ? result[1].value : null);
    } catch (_) {if (request === latestRequest) render(null, null);}
  }
  byId("refresh").addEventListener("click",refresh);
  refresh();
  setInterval(() => byId("refresh").click(),60000);
})();
