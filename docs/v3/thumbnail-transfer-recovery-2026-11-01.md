# Recovering Thumbnail Observation After Slow CDN Transfers

Date: 2026-11-01 JST.

The public dashboard was publishing fresh JSON while its Twitch image observer
could no longer finish downloading a thumbnail. The tile correctly became
UNKNOWN, but the age graph's value label selected the last non-missing historic
value. It therefore kept showing about six minutes beside an observation gap.

## Evidence and correction

The last verified image before the incident was read at 15:33 JST. All 151 saved
attempts from 15:34 through 18:04 failed. A normal transfer on the monitoring host
took 19.18 seconds; the worker allowed about eight seconds. An address crossover
check reproduced the slow behavior from either host, while another CDN address
returned the same-size image in under a second. This identified a route-dependent
transfer problem, without establishing which CDN or network component caused it.

A 25-second acquisition deadline recovered one image but still failed on later
body stalls. The failed phase was retained. An isolated resolver comparison also
returned a slow route and was not deployed. The final correction keeps the
existing one-minute request cadence, gives acquisition a 45-second total
deadline, and gives OCR its own 10-second budget after acquisition. A body read
waits for the remaining total deadline instead of inheriting the short connection
timeout. The existing limits on response size, dimensions, OCR calls and provider
Retry-After remain in force. No CDN IP is pinned and no host DNS setting changes.

Per-attempt private diagnostics record the peer, transfer stage, header latency,
received bytes, elapsed time and exception type. The public JSON allowlist does
not expose these raw diagnostics.

The renderer keeps the historical line and its missing buckets. If current
observation is unavailable it says **Not observed** and shows approximately how
long it has been since the last valid clock read. An unknown capture no longer
claims a new image. A missing latest graph bucket does not inherit an older
bucket's number. A marker makes the first isolated recovered point visible, and
the label includes its bucket time. The last-read age is approximate because the public metric has
collection and publication cadence; it is not presented as an exact timestamp.

## Verification

At 18:51 JST, all 19 natural cycles under the final version had verified clock
readings, including four distinct image clocks. The longest cycle was 38.599
seconds. At 18:52 JST the public page showed the latest 18:50 bucket, a visible
endpoint and a fresh thumbnail observation.

- The packaged worker candidate passed the focused thumbnail and evidence tests,
  including a blocked body read and OCR after a simulated slow download. The
  public checkout passed its corresponding tests and API compatibility checks.
- A Chromium fixture exercised missing current data, missing latest buckets,
  historical-line retention and recovery without a page reload.
- Only the thumbnail observer was restarted. The source change uses its existing
  environment and preserves its state/history. The monitoring exporter, API
  collector and media path were retained.
- Natural observations, public JSON and an already open public page were checked
  in order. The final checkpoint and its limits are in
  [current status](../current-status.md#thumbnail-transfer-recovery-2026-11-01-jst).

An initial local regression found that a completed response can close its socket
before the next timeout update; the body loop now recognizes completion. A
candidate test setup initially lacked the separately deployed metrics helper,
and the browser fixture initially lacked a transfer-section element. Those setup
failures were retained and corrected before their successful checks were used.
They were not production stream failures.

The standalone renderer regression is
[thumbnail-freshness-20261101.html](../../ops/public-publisher/site/tests/thumbnail-freshness-20261101.html).
Serve that directory over local HTTP and open the fixture in a browser. It uses
synthetic public JSON and sends no notification or provider request. The renderer
source is included for that fixture; the case-study checkout's historical demo
entrypoint is not the production website deployment.

Long-term CDN behavior and public-player audio/video continuity were not
qualified by this repair. A thumbnail only observes a provider-generated image.
