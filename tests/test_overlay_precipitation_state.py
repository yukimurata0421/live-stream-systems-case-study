from __future__ import annotations

import html as html_lib
import http.server
import json
import re
import shutil
import subprocess
import threading
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
OVERLAY_HTML = ROOT / "ui" / "overlay" / "index.html"


class _HarnessHandler(http.server.BaseHTTPRequestHandler):
    body = b""

    def do_GET(self) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(type(self).body)))
        self.end_headers()
        self.wfile.write(type(self).body)

    def log_message(self, _format: str, *_args: object) -> None:
        return


class OverlayPrecipitationStateTests(unittest.TestCase):
    def test_browser_executes_all_precipitation_card_state_transitions(self) -> None:
        chromium = next(
            (
                path
                for name in ("chromium", "chromium-browser", "google-chrome")
                if (path := shutil.which(name))
            ),
            None,
        )
        if chromium is None:
            self.skipTest("Chromium is not installed")

        source = OVERLAY_HTML.read_text(encoding="utf-8")
        start = source.index("function responseContentTypeIncludes")
        end = source.index("function setPrecipitationState", start)
        state_functions = source[start:end]
        harness = f"""<!doctype html>
<html><body><pre id="result">pending</pre><script>
const BASE = Date.parse("2026-08-02T11:00:00Z");
const PRECIPITATION_WARMUP_MS = 20_000;
const PRECIPITATION_UNAVAILABLE_MS = 30 * 60_000;
const overlayStartedMs = BASE;
let lastGoodPrecipitationStatus = null;
let lastGoodPrecipitationHealth = null;
let overlayResponseValidationFailures = 0;
function pad2(value) {{ return String(value).padStart(2, "0"); }}
function clock(date, utc) {{
  const h = utc ? date.getUTCHours() : date.getHours();
  const m = utc ? date.getUTCMinutes() : date.getMinutes();
  const s = utc ? date.getUTCSeconds() : date.getSeconds();
  return `${{pad2(h)}}:${{pad2(m)}}:${{pad2(s)}}`;
}}
function toEpochMs(isoUtc) {{
  if (!isoUtc) return 0;
  const value = Date.parse(isoUtc);
  return Number.isFinite(value) ? value : 0;
}}
{state_functions}
const status = (observedAt, hasRain) => ({{
  schema: "stream_v3.precipitation.v1",
  available: true,
  analysis_only: true,
  forecast_minutes: 0,
  observed_at_utc: observedAt,
  stale_after_sec: 900,
  has_precipitation: hasRain,
}});
const currentHealth = {{schema: "stream_v3.precipitation_fetcher_health.v1", state: "current", success: true}};
const retryHealth = {{
  schema: "stream_v3.precipitation_fetcher_health.v1",
  state: "retrying",
  success: false,
  next_retry_at_utc: "2026-08-02T11:02:00Z",
}};
const view = (value) => ({{state: value.state, notice: value.notice}});
const initialValidated = validatedPrecipitationPayloads(
  status("2026-08-02T10:55:00Z", true),
  currentHealth,
);
const fallbackValidated = validatedPrecipitationPayloads(
  {{now: 123, aircraft: []}},
  {{now: 123, aircraft: []}},
);
const results = {{
  validNowPlayingSnapshot: validNowPlayingSnapshot({{now_playing: {{title: "Track"}}}}),
  invalidNowPlayingSnapshot: validNowPlayingSnapshot({{now: 123, aircraft: []}}),
  validNowPlayingText: validNowPlayingText("Now Playing: Track"),
  invalidNowPlayingText: validNowPlayingText('{{"now":123,"aircraft":[]}}'),
  validStatus: validPrecipitationStatus(status("2026-08-02T10:55:00Z", true)),
  invalidStatus: validPrecipitationStatus({{now: 123, aircraft: []}}),
  validHealth: validPrecipitationHealth(currentHealth),
  invalidHealth: validPrecipitationHealth({{now: 123, aircraft: []}}),
  fallbackUsed: fallbackValidated.usedFallback,
  fallbackStatusPreserved: fallbackValidated.status === initialValidated.status,
  fallbackHealthState: fallbackValidated.health.state,
  validationFailures: overlayResponseValidationFailures,
  initialWarmup: view(precipitationPresentation(null, null, BASE + 10_000)),
  initialUnavailable: view(precipitationPresentation(null, null, BASE + 21_000)),
  current: view(precipitationPresentation(status("2026-08-02T10:55:00Z", true), currentHealth, BASE)),
  noRain: view(precipitationPresentation(status("2026-08-02T10:55:00Z", false), currentHealth, BASE)),
  lastData: view(precipitationPresentation(status("2026-08-02T10:55:00Z", true), retryHealth, BASE)),
  stale: view(precipitationPresentation(status("2026-08-02T10:44:00Z", true), retryHealth, BASE)),
  unavailable: view(precipitationPresentation(status("2026-08-02T10:29:00Z", true), retryHealth, BASE)),
  fetcherWarmup: view(precipitationPresentation(null, {{
    state: "warming_up",
    checked_at_utc: "2026-08-02T11:00:55Z",
  }}, BASE + 60_000)),
}};
document.getElementById("result").textContent = JSON.stringify(results);
</script></body></html>"""

        _HarnessHandler.body = harness.encode("utf-8")
        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _HarnessHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        host, port = server.server_address
        try:
            completed = subprocess.run(
                [
                    chromium,
                    "--headless",
                    "--no-sandbox",
                    "--disable-gpu",
                    "--disable-dev-shm-usage",
                    "--virtual-time-budget=1000",
                    "--dump-dom",
                    f"http://{host}:{port}/",
                ],
                text=True,
                capture_output=True,
                timeout=20,
                check=False,
            )
        finally:
            server.shutdown()
            server.server_close()

        self.assertEqual(completed.returncode, 0, completed.stderr[-1000:])
        match = re.search(r'<pre id="result">(.*?)</pre>', completed.stdout, re.S)
        self.assertIsNotNone(match, completed.stdout[-2000:])
        assert match is not None
        results = json.loads(html_lib.unescape(match.group(1)))
        self.assertEqual(
            results,
            {
                "validNowPlayingSnapshot": True,
                "invalidNowPlayingSnapshot": False,
                "validNowPlayingText": True,
                "invalidNowPlayingText": False,
                "validStatus": True,
                "invalidStatus": False,
                "validHealth": True,
                "invalidHealth": False,
                "fallbackUsed": True,
                "fallbackStatusPreserved": True,
                "fallbackHealthState": "retrying",
                "validationFailures": 1,
                "initialWarmup": {"state": "WARMING UP", "notice": "INITIAL REQUEST"},
                "initialUnavailable": {"state": "DATA UNAVAILABLE", "notice": ""},
                "current": {"state": "CURRENT", "notice": ""},
                "noRain": {"state": "NO RAIN", "notice": ""},
                "lastData": {"state": "LAST DATA", "notice": "RETRY 20:02"},
                "stale": {"state": "STALE", "notice": "RETRY 20:02"},
                "unavailable": {"state": "DATA UNAVAILABLE", "notice": "RETRY 20:02"},
                "fetcherWarmup": {"state": "WARMING UP", "notice": "INITIAL REQUEST"},
            },
        )


if __name__ == "__main__":
    unittest.main()
