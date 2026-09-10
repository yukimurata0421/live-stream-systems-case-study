/** Wait for a MapLibre source without discarding slow, still-valid work. */
export function waitForMapSourceReady(map, sourceId, options = {}) {
  const timeoutMs = Number.isFinite(options.timeoutMs) && options.timeoutMs > 0
    ? options.timeoutMs
    : 120_000;
  const pollMs = Number.isFinite(options.pollMs) && options.pollMs > 0
    ? options.pollMs
    : 250;
  const startedAt = Date.now();

  return new Promise((resolve) => {
    let settled = false;
    let pollTimer = null;
    let timeoutTimer = null;

    function cleanup() {
      if (pollTimer !== null) clearTimeout(pollTimer);
      if (timeoutTimer !== null) clearTimeout(timeoutTimer);
      map.off("sourcedata", onSourceData);
    }

    function finish(ready, reason) {
      if (settled) return;
      settled = true;
      cleanup();
      resolve({
        ready,
        reason,
        elapsed_ms: Math.max(0, Date.now() - startedAt),
      });
    }

    function scheduleCheck() {
      if (settled) return;
      if (pollTimer !== null) clearTimeout(pollTimer);
      pollTimer = setTimeout(check, pollMs);
    }

    function check() {
      if (settled) return;
      pollTimer = null;
      try {
        if (!map.getSource(sourceId)) {
          finish(false, "source_removed");
          return;
        }
        if (map.isSourceLoaded(sourceId)) {
          finish(true, "loaded");
          return;
        }
      } catch (_error) {
        finish(false, "source_check_failed");
        return;
      }
      scheduleCheck();
    }

    function onSourceData(event) {
      if (event?.sourceId !== sourceId) return;
      check();
    }

    map.on("sourcedata", onSourceData);
    timeoutTimer = setTimeout(() => finish(false, "timeout"), timeoutMs);
    check();
  });
}
