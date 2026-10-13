"use strict";
(() => {
  const byId = (id) => document.getElementById(id);
  const date = (value) => {
    const parsed = new Date(value);
    return value && Number.isFinite(parsed.getTime())
      ? parsed.toLocaleString("ja-JP", {timeZone: "Asia/Tokyo", hour12: false}) + " JST" : "not available";
  };
  function render(value) {
    const now = Date.now() / 1000;
    const snapshotFresh = typeof value?.generated_at === "number" && now >= value.generated_at && now - value.generated_at <= 180;
    const observation = value?.observation;
    const observed = new Date(observation?.observed_at).getTime() / 1000;
    const measured = snapshotFresh && observation?.source_fresh === true && observation.status === "OBSERVED"
      && Number.isFinite(observed) && now >= observed && now - observed <= 180;
    const state = measured && ["LIVE", "OFFLINE"].includes(observation.provider_state) ? observation.provider_state : "UNKNOWN";
    byId("twitchState").textContent = state === "UNKNOWN" ? "Unknown" : state;
    byId("twitchState").className = state === "LIVE" ? "ok" : "unknown";
    const status = observation?.status === "NOT_CONFIGURED" ? "Live observation is not configured" : !snapshotFresh ? "Published observation is unavailable or stale" : !observation?.source_fresh ? "Source observation is unavailable or stale" : "Provider observation";
    byId("twitchStatus").textContent = status + " · " + date(observation?.observed_at);
    byId("twitchSession").textContent = state === "LIVE" ? "Live session" : "Not observed";
    byId("twitchSessionMeta").textContent = state === "LIVE" ? "Started " + date(value.session?.started_at) + " · ID " + value.session?.id : "Start time unavailable";
    const reception = value?.reception;
    const stamp = reception?.verified_at && new Date(reception.verified_at).getTime();
    const verified = Number.isFinite(stamp) && stamp <= Date.now() && reception?.non_silent === true;
    byId("twitchPlayback").textContent = verified ? "Previously verified" : "Not verified";
    byId("twitchPlaybackMeta").textContent = verified ? date(reception.verified_at) + " · " + reception.video + " · " + reception.audio + ". This is the last sample, not continuous reception." : "Playback evidence unavailable";
  }
  async function refresh() {
    try {
      const response = await fetch("./twitch-observation.json?t=" + Date.now(), {cache:"no-store"});
      if (!response.ok) throw new Error("unavailable");
      const value = await response.json();
      render(value.schema === "stream-v3-twitch-public.v1" ? value : null);
    } catch (_) { render(null); }
  }
  byId("refresh")?.addEventListener("click", refresh);
  refresh();
})();
