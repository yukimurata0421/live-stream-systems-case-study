/** Build the bounded semantic precipitation report sent with render heartbeat. */
export function precipitationRenderSnapshot(diagnostics = {}) {
  const evaluated = diagnostics.precipitationEvaluated === true;
  const available = evaluated && diagnostics.precipitationAvailable === true;
  let fresh = evaluated && diagnostics.precipitationFresh === true;
  const hasPrecipitation = evaluated ? diagnostics.precipitationHasRain === true : null;
  let layerLoaded = evaluated && diagnostics.precipitationLayerLoaded === true;
  const validtime = /^\d{14}$/.test(String(diagnostics.precipitationValidtime || ""))
    ? String(diagnostics.precipitationValidtime)
    : "";
  let layerValidtime = /^\d{14}$/.test(String(diagnostics.precipitationLayerValidtime || ""))
    ? String(diagnostics.precipitationLayerValidtime)
    : "";
  let state = "warming_up";

  if (!evaluated) {
    fresh = false;
    layerLoaded = false;
    layerValidtime = "";
  } else if (!available && !fresh) {
    layerLoaded = false;
    layerValidtime = "";
    state = "unavailable";
  } else if (!fresh) {
    layerLoaded = false;
    layerValidtime = "";
    state = "stale";
  } else if (available && hasPrecipitation === false) {
    layerLoaded = false;
    layerValidtime = "";
    state = "no_rain";
  } else if (fresh && hasPrecipitation === true && layerLoaded) {
    if (layerValidtime === validtime) {
      state = available ? "layer_loaded" : "layer_loaded_lkg";
    } else if (available) {
      state = "layer_mismatch";
    } else {
      fresh = false;
      layerLoaded = false;
      layerValidtime = "";
      state = "unavailable";
    }
  } else if (available && fresh && hasPrecipitation === true) {
    layerLoaded = false;
    layerValidtime = "";
    state = "layer_missing";
  } else {
    fresh = false;
    layerLoaded = false;
    layerValidtime = "";
    state = "unavailable";
  }

  return {
    evaluated,
    available,
    fresh,
    has_precipitation: hasPrecipitation,
    layer_loaded: layerLoaded,
    validtime,
    layer_validtime: layerValidtime,
    state,
  };
}
