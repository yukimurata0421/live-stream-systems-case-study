"use strict";
(() => {
  const byId = id => document.getElementById(id);
  const states = {FLOWING:"Sending", CONNECTED:"Measuring", STALLED:"No progress", DISCONNECTED:"Disconnected", UNKNOWN:"Unknown"};
  const issueText = {RELAY_STOPPED:"Relay process stopped",INPUT_NOT_PROGRESSING:"Relay input is not progressing",YOUTUBE_NOT_PROGRESSING:"YouTube output is not progressing",TWITCH_NOT_PROGRESSING:"Twitch output is not progressing",VM_MEMORY_HIGH:"VM memory usage is high",VM_DISK_LOW:"VM disk free space is low",DAILY_RENEWAL_HELD:"Twitch renewal needs attention",TWITCH_OFFLINE:"Twitch is offline",RELAY_OBSERVATION_UNAVAILABLE:"Relay observation unavailable",TWITCH_API_UNAVAILABLE:"Twitch API observation unavailable"};
  const date = v => {
    const d = new Date(v);
    return v && Number.isFinite(d.getTime()) ? d.toLocaleString("en-GB", {timeZone:"Asia/Tokyo",day:"2-digit",month:"short",hour:"2-digit",minute:"2-digit",hour12:false}) + " JST" : "Not observed";
  };
  const rate = v => typeof v === "number" && Number.isFinite(v) ? v.toFixed(2) + " Mbps" : "Measuring";
  const set = (id,text) => {if(byId(id)) byId(id).textContent = text;};
  function render(v) {
    const now = Date.now()/1000;
    const age = now-v?.generated_at, sourceAge = now-new Date(v?.observed_at).getTime()/1000;
    const fresh = v?.schema === "stream.relay_public.v1" && age >= 0 && age <= 180 && sourceAge >= 0 && sourceAge <= 180 && v.source_fresh === true;
    const vm = fresh ? v.vm : {};
    const observed = fresh && vm?.status === "OBSERVED";
    const pill = byId("relayState");
    pill.textContent = observed ? "Observed" : "Unknown";pill.className = "status-pill " + (observed ? "ok" : "unknown");
    for (const [key,id] of [["cpu_percent","relayCpu"],["memory_percent","relayMemory"],["disk_free_percent","relayDisk"]]) {
      const valid = observed && typeof vm[key] === "number";
      set(id,valid ? vm[key].toFixed(1)+"%" : "—");
      byId(id+"Meter").style.width = valid ? Math.min(100,Math.max(0,vm[key]))+"%" : "0%";
    }
    const uptime = observed && typeof vm.uptime_seconds === "number" ? vm.uptime_seconds : null;
    set("relayUptime",uptime !== null ? `${Math.floor(uptime/86400)}d ${Math.floor(uptime%86400/3600)}h` : "Unknown");
    set("relayObserved",fresh ? "Checked "+date(v.observed_at) : "Observation unavailable or stale");
    const flows = {input:fresh ? v.input : null,youtube:fresh ? v.destinations?.youtube : null,twitch:fresh ? v.destinations?.twitch : null};
    for (const [key,flow] of Object.entries(flows)) {
      const status = flow?.status || "UNKNOWN";
      document.querySelectorAll(`[data-flow-status="${key}"]`).forEach(el => {
        el.classList.remove("flowing","stalled","disconnected","unknown");el.classList.add(status.toLowerCase());
      });
      document.querySelectorAll(`[data-rate="${key}"]`).forEach(el => {el.textContent = fresh && status !== "UNKNOWN" ? rate(flow?.mbps) : "Not observed";});
      set("relay"+key[0].toUpperCase()+key.slice(1)+"Meta",states[status] || "Unknown");
    }
    const due = fresh ? v.daily?.next_renewal_at : null;
    set("relayDaily",due ? date(due) : "Schedule not observed");
    set("relayDailyMeta",fresh && v.daily?.planned_break ? "Scheduled Twitch session update in progress. YouTube is monitored independently." : "Twitch reconnects at 00:00 JST. YouTube continues through the relay.");
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
  async function refresh() {
    try {
      const r = await fetch("./relay-observation.json?t="+Date.now(),{cache:"no-store"});
      if (!r.ok) throw new Error("unavailable");
      render(await r.json());
    } catch (_) {render(null);}
  }
  byId("refresh").addEventListener("click",refresh);
  refresh();
  setInterval(() => byId("refresh").click(),60000);
})();
