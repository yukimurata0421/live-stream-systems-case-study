# Twitch Thumbnail Clock Observation

The thumbnail monitor runs on the monitoring host beside the existing relay,
native-forward and Twitch API observers. It checks the clock inside a provider
image. It does not establish that the public video player or audio is healthy.

This page is the stable contract. [Observability](../observability.md#video-observation-layers)
defines how this layer relates to the others, and the
[clock observation case study](clock-observation-case-study.md) explains why it
was added. Dated measurements and source provenance are listed under
[Dated Records](#dated-records); [current status](../current-status.md) owns
unobserved behavior and later changes.

## Scope

The official [Get Streams API](https://dev.twitch.tv/docs/api/reference/#get-streams)
provides `thumbnail_url`, described as an image from a frame in the last five
minutes. This does not specify an internal ingest/transform checkpoint or a
strict refresh SLA. The fixed scope is `twitch_provider_thumbnail`, not an
ingest-success or public-player E2E label. Image clock age is not live playback
latency. A thumbnail clock supports only that Twitch produced an image whose
paired JST/UTC clock was readable and changed between images; it says nothing
about the video CDN, player, audio or aircraft-data correctness.

## Acquisition

The [source adapter](../../ops/scripts/stream_v3_twitch_thumbnail_source.py)
observes the existing collector's Get Streams response. It publishes the original
API evidence first, unchanged, then mirrors the URL into a separate tmpfs file.
There is no additional Helix request, HLS extraction, embedded player or browser.
An add-on write failure does not change a successful API observation into failure.

The [worker](../../src/stream_v3/twitch_thumbnail_worker.py) joins that RAM source
to the API evidence using host/boot, collector instance/revision, sequence,
channel/stream identity and timestamps. Only the API-supplied HTTPS template on
the thumbnail host is accepted. Redirects, userinfo, queries and fragments are
rejected. No fixed-channel or video-URL fallback is used.

The worker fetches at most once per minute, uses conditional GET/ETag, honors
Retry-After, and bounds response size to 2 MiB and JPEG dimensions to 1920x1080.
It requests 1080p: at 720p the UTC clock region was too small to pass the paired
confidence check. Every different image gets OCR; identical images reuse the
verified reading. A different JPEG with the same clock is not a new sample. No
real-time cadence test is applied to provider images that refresh in minutes.

## Stream Changes

The thumbnail URL is channel-scoped and survives a stream-ID change, such as the
daily Twitch session renewal. Clear the old ETag as soon as fresh API evidence
identifies a new stream, even before the RAM sidecar is available. Fetch the body
first; a known pre-stream image can then remain in the explicit waiting state
across 304 responses. Clear the ETag also when decode/analysis fails without a
reusable clock cache.

Only the first valid clock of a new stream receives a startup allowance of at
most 720 seconds from the API's `started_at`. If no start time is available, the
first LIVE observation becomes the persisted origin. Restart, API UNKNOWN and
capture failures do not extend it. A first accepted clock ends the allowance;
later observation gaps use the ordinary 300-second rule. Worker publication
failures receive no stream-start allowance.

## Clock Reading

JST and UTC readings require confidence at least 70 and a nine-hour offset.
One alternate-contrast read is allowed. If both original readings already have
the correct nine-hour relationship but confidence is too low, one additional
binary-threshold-128 pass is allowed. Both values must exactly match the original
readings and both confidences must meet the same threshold of 70. Different
readings never win by voting; transformed copies are not independent evidence.
The cycle deadline and a maximum of six OCR calls still apply. UTC maps to the
nearest day; future or regressing readings remain uncertain. A persisted numeric
anchor survives capture/OCR errors and worker restart. It is not a saved image.

| Result | Meaning |
| --- | --- |
| `CLOCK_OBSERVED` | First accepted paired reading for this stream. |
| `ADVANCING_CONFIRMED` | A later distinct clock value was accepted. |
| `UNCHANGED` | Same provider sample; normal between refreshes. |
| `WAITING_NEW_STREAM_THUMBNAIL` | Only a pre-stream image is available before this stream's first accepted clock. No fault images are retained for this wait. |
| `WAITING_POLL` / `WAITING_RETRY` | Normal poll spacing or provider Retry-After; no capture was attempted. |
| `OCR_UNCERTAIN` | Confidence, date or clock binding did not establish a reading. |
| `SOURCE_OFFLINE` | Fresh API evidence explicitly says OFFLINE. |
| `UNKNOWN` | Source, HTTP, resource or publication failure. |

## Freshness And Warnings

Image age up to 360 seconds is `FRESH`; up to 840 is `AGING`. Older images are
`STALE_SUSPECT`. Above 900 seconds with fresh native-forward progress, the
diagnostic becomes `STALE_CORRELATED`. That is a disagreement, not proof of a
stopped public video stream. These are initial thresholds, not a measured SLO.

The [metrics](../../src/stream_v3/twitch_thumbnail_metrics.py) use the separate
`stream_v3_twitch_thumbnail_` namespace and fixed target/scope labels. API, worker
and native facts keep their own freshness limits. A missing RAM source cannot
hide a thumbnail gap when the original API evidence still reports LIVE.

The [warning rules](../../ops/monitoring/prometheus/rules/twitch_thumbnail.yml)
cover:

- five minutes without a valid observation while the API reports LIVE, after any
  startup allowance;
- five minutes without worker publication;
- ten minutes with the publication metric absent;
- a verified image clock older than fourteen minutes.

The 840-second threshold allows one missed six-minute image refresh plus poll
timing; it is provisional.

Unreadable, missing, malformed, non-object or oversized state leaves the last
publication timestamp intact in the exporter. It exports `state_readable=0`
and the increasing age; an exporter that has never read state starts its timer
at startup. An `absent_over_time` rule covers total metric disappearance. The
fixed job/instance selector in that rule must match the configured scrape target.

The warnings reuse the [existing notification loop](../../ops/scripts/stream_v3_clock_alert_notify.py)
and require two distinct fresh observations for a recovery message. They add no
Critical classification, SLO denominator, recovery action or notification service.

## OCR Accounting

History flags `ocr_attempted` and `ocr_binary_retried` describe work performed
in that cycle. Cached readings and repeat scrapes do not count as OCR. The
`ocr_observations_total` and `ocr_binary_retries_total` metrics in this namespace
persist across capture failures, stream changes and worker restarts; losing
numeric state resets them. They include failed observations/retries, and count
observations rather than individual OCR commands. A same-window increase ratio
measures retry frequency; a window with zero OCR has no evaluated ratio. They
do not reconstruct observations from before deployment.

## Resources And Storage

Normal full images are processed in memory and discarded. Only small clock
regions remain in RAM. Numeric state and a 1 MiB x 2 history are retained on disk.
An abnormal episode may retain small ROI evidence, capped at 8 MiB and seven days.
The [service template](../../ops/systemd/stream-v3-twitch-thumbnail.service) uses
a separate non-root user, 0.2-core CPU quota, 256 MiB memory limit, no swap, a
32 MiB temporary filesystem, no capabilities and process-group cleanup. It is
independent of the delivery host and has no recovery privileges.

## Configuration And Credential-Free Checks

Start from the [configuration example](../../ops/config/twitch_thumbnail.example.json).
Replace the example account, calibrate the ROIs and prepare the immutable
runtime release. The repository service file is the canonical unit: its command,
Python path, libraries and the example OCR configuration use the root-owned
`/opt/stream-v3-twitch-thumbnail/current` symlink. That symlink selects a release
containing source, a venv, OCR runtime/data and a manifest. Install the unit
unchanged and switch the symlink atomically after validation. Copying this
checkout does not install the unit, select a release or enable a service. The
example contains no production account, credentials or automatic installer.

The source adapter uses `TWITCH_API_BASELINE` for the root-owned collector and
`TWITCH_THUMBNAIL_SOURCE_FILE` for the tmpfs sidecar. Preserve the collector's
existing arguments, service credentials and `monitoring_v4.twitch_api_evidence.v1`
contract. Reconcile the adapter if that collector's CLI changes.

The exporter overlay accepts `TWITCH_THUMBNAIL_OBSERVATION_FILE`; it keeps the
legacy expensive cache while refreshing the small observation each scrape.
Scrape the new namespace in the existing clock job. Rule examples use that job's
name and are not installed by copying this checkout.

```bash
PYTHONDONTWRITEBYTECODE=1 OMP_THREAD_LIMIT=1 python3 -m pytest -q -p no:cacheprovider \
  tests/test_twitch_thumbnail.py tests/test_clock_observation_alerts.py
promtool test rules ops/monitoring/prometheus/tests/twitch_thumbnail.yml
```

These tests use generated images, temporary files and mocked HTTP/OCR. They make
no provider requests, send no notifications and change no delivery services.

## Dated Records

Each record describes its own window and source selection; none shows deployment
of this public checkout.

- [Initial source port](public-source-port-2026-10-22-thumbnail.md): first
  operated window, resolution calibration, resources and source boundary.
- [Review follow-up](public-source-port-2026-10-22-thumbnail-review.md): stream
  rollover, missing-metric handling and the OCR retry with its reanalysis.
- [Operated OCR counter rollout](thumbnail-ocr-rollout-2026-10-22.md): activation
  and the first live counter progression.
