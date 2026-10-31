import * as maplibregl from "/map/vendor/maplibre-gl.mjs";

// No live-data requests, aircraft, weather, clock or receiver-health claims.
const maintenance = new URLSearchParams(location.search).get("mode") === "maintenance";
window.offlineSlate = {ready: false, errors: [], content: maintenance ? "MAINTENANCE_SLATE" : "OFFLINE_SLATE"};
if (maintenance) {
  document.title = "Scheduled maintenance";
  document.querySelector("#map").setAttribute("aria-label", "Background map during scheduled maintenance");
  document.querySelector(".status").textContent = "SCHEDULED MAINTENANCE";
  document.querySelector(".eyebrow").lastChild.textContent = "SYSTEM MAINTENANCE";
  document.querySelector("h1").textContent = "Scheduled maintenance";
  const lines = document.querySelectorAll(".notice p");
  lines[0].textContent = "Live video and data are temporarily paused.";
  lines[1].textContent = "Live data will resume after maintenance.";
}
maplibregl.setWorkerCount(1);
const map = new maplibregl.Map({
  container: "map",
  style: "/map/style.json",
  center: [140.75, 36.35],
  zoom: 6.6,
  bearing: 0,
  pitch: 0,
  interactive: false,
  attributionControl: false,
  preserveDrawingBuffer: true,
  fadeDuration: 0,
  renderWorldCopies: false,
  pixelRatio: 1,
});
map.on("error", (event) => {
  if (window.offlineSlate.errors.length < 8) window.offlineSlate.errors.push(String(event.error?.message || "map_render_error").slice(0, 300));
});
map.once("idle", async () => {
  await document.fonts.ready;
  const water = map.queryRenderedFeatures(undefined, {layers: ["water"]}).length;
  const land = map.queryRenderedFeatures(undefined, {layers: ["prefecture-border"]}).length;
  Object.assign(window.offlineSlate, {water, land, tilesLoaded: map.areTilesLoaded()});
  window.offlineSlate.ready = map.areTilesLoaded() && water > 0 && land > 0
    && window.offlineSlate.errors.length === 0;
});
