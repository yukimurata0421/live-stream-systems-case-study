# Twitch Thumbnail Clock Observation

The thumbnail collector runs in K3s on the monitoring host beside the existing
host-based relay, native-forward and Twitch API observers. It checks the clock inside a provider
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
API evidence first, unchanged, then publishes a coherent API/URL bundle over
a private HTTP endpoint. A tmpfs mirror remains for legacy consumers and rollback.
There is no additional Helix request, HLS extraction, embedded player or browser.
An add-on write failure does not change a successful API observation into failure.

The [worker](../../src/stream_v3/twitch_thumbnail_worker.py) reads the bundle via
a Kubernetes Service and joins its source to the API evidence using host/boot,
collector instance/revision, sequence,
channel/stream identity and timestamps. Only the API-supplied HTTPS template on
the thumbnail host is accepted. Redirects, userinfo, queries and fragments are
rejected. No fixed-channel or video-URL fallback is used.

The worker fetches at most once per minute, uses conditional GET/ETag, honors
Retry-After, and bounds response size to 2 MiB and JPEG dimensions to 1920x1080.
It requests 1080p: at 720p the UTC clock region was too small to pass the paired
confidence check. Every different image gets OCR; identical images reuse the
verified reading. A different JPEG with the same clock is not a new sample. No
real-time cadence test is applied to provider images that refresh in minutes.

The default UTC ROI is `(.0795,.968,.0415,.022)`; JST remains
`(.019,.968,.04,.022)`. Explicit configuration takes precedence. The UTC left
edge was widened by three pixels at 1080p after a paired comparison; the right
edge, height, overlay and return-feed ROIs were unchanged. ROI changes invalidate
the verified cache even for an identical JPEG.

## Stream Changes

The thumbnail URL is channel-scoped and survives a stream-ID change, such as the
daily Twitch session renewal. Clear the old ETag as soon as fresh API evidence
identifies a new stream, even before the matching source is available. Fetch the body
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
Acquisition has a 45-second total deadline. Once the image is available, OCR
receives a separate 10-second budget, with at most two seconds per call and
eight calls. The relay read retains its two-second timeout and polling remains
once per minute. Body reads use the remaining acquisition deadline instead of
inheriting the shorter connection timeout. Reads are tried in this order:

1. The original clock regions.
2. One alternate-contrast read.
3. If both original readings already have the correct nine-hour relationship
   but confidence is too low, one binary-threshold-128 pass. Both values must
   exactly match the original readings and both confidences must reach 70.
4. One paired retry using bilinear 2x interpolation followed by the existing
   OCR enlargement. At least one original reading must already have confidence
   70, and every parseable original value, including a low-confidence one, must
   remain unchanged. Both retry confidences must reach 70.

Different readings never win by voting; transformed copies are not independent
evidence, and an originally inconsistent nine-hour pair is never repaired by
choosing transformed results. UTC maps to the nearest day; future or regressing
readings remain uncertain. A persisted numeric anchor survives capture/OCR
errors and worker restart. It is not a saved image.

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

History records `ocr_method`, `ocr_source_method`, `ocr_failure_reason` and
fixed per-stage results. Final methods are original, contrast, binary or
interpolation; cached readings retain the original method separately. Failures
use fixed reason codes; exception text and URLs never become metric labels.

`ocr_observations_total` counts actual OCR observations. For contrast, binary
and interpolation, `ocr_<method>_retries_total` counts started retries and
`ocr_<method>_accepted_total` counts final verified observations. A readable pair
rejected by timestamp, stream binding or regression is not accepted. Cached
readings and repeat scrapes do not increment counters. Counters survive errors,
stream changes and restart through numeric state; new counters start at zero
without reconstructing history. A same-window ratio needs a nonzero denominator.
Fixed-label gauges expose method, failure reason and evidence-save status.

## Provider And Cache Timing

The HTTP client records a bounded `Age` header on both 200 and 304 responses.
The worker compares the image body hash with persisted state, so restarting or
receiving the same image in a full 200 response does not count as an image
change. It keeps the first-fetch timestamp and that response's `Age` even when
OCR fails; a later successful reading uses those original values.

These gauges use the `stream_v3_twitch_thumbnail_` prefix:

| Suffix | Meaning |
| --- | --- |
| `cdn_age_seconds` | Current response's HTTP `Age`. |
| `discovery_lag_seconds` | First-fetch time minus the verified image clock. |
| `cdn_age_at_discovery_seconds` | HTTP `Age` from that image's first fetch. |
| `origin_side_age_estimate_seconds` | Discovery lag minus the first-fetch `Age`; absent when `Age` is unavailable. |
| `seconds_since_image_change` | Time since a different image body was first fetched. |

HTTP `Age` measures cache response age since generation or revalidation, not
time since Twitch rendered the image. Revalidation resets it. The origin-side
value is therefore an upper-side estimate of rendering delay, not a measurement
of that delay or proof of which upstream component caused it. The timing gauges
require a fresh verified observation; the image-change age requires fresh LIVE
evidence. Poll spacing, OCR confidence and freshness thresholds are unchanged.

The fixed localhost native-forward reader reuses one HTTP-only opener. It does
not allocate a TLS context on every poll, follow redirects, or use proxy settings.

## Resources And Storage

Normal full images are processed in memory and discarded. Only small clock
regions remain in RAM. Numeric state and a 1 MiB x 2 history are retained on disk.

Each different abnormal image/stream/ROI combination may retain a small paired
ROI archive. The [thumbnail evidence store](../../src/stream_v3/twitch_thumbnail_evidence.py)
deduplicates from archive metadata across restart and caps allocated space at
8 MiB, age at seven days and archive count at 64. It protects one necessary
current archive while pruning old evidence; if a new archive cannot fit it skips
saving and exposes `capacity_limited`. UNKNOWN does not release that protection.
Normal observations, failed captures and regions larger than 320x96 pixels are
rejected. New archives identify `jst.pgm` and `utc.pgm`; legacy evidence stays
unchanged. No normal image or full-frame archive is added.

The [K3s definitions](../../ops/kubernetes/twitch-thumbnail/README.md) use one
Recreate Deployment, UID/GID 1000, a 200m CPU / 256Mi memory limit, a read-only root,
a 32Mi tmpfs, no capabilities and no service-account token. Python, Pillow,
Tesseract, English OCR data and tini are bundled in a manifest-digest-pinned image.
No API or recovery credential is mounted. No hostPath or hostNetwork is used.

The numeric state, polling cooldowns, verified anchors, counters and history move
into a PVC with hashes verified at cutover. Retain its PV on rollback. Local-path
storage requests do not enforce a filesystem quota or provide multi-host
availability. The Pod remains pinned to the producer's node to retain boot binding.
Readiness/liveness track monotonic loop progress, including UNKNOWN publications;
a provider error alone does not restart a healthy collector loop.

The existing Helix collector, relay observer and metrics exporter remain host
services. A GET-only facts adapter contains their legacy paths and loopback
boundary. This is a migration of thumbnail acquisition/OCR, not the entire
monitoring stack. Dell's media path and local CRA processing are unchanged.

## Configuration And Credential-Free Checks

The [deployment template](../../ops/kubernetes/twitch-thumbnail/manifest.json)
is rendered with an explicit account, private pod gateway and imported image
manifest digest. Its default is zero replicas for state transfer. The current
configuration uses `source_url` and `forward_url` Service endpoints. Retired
systemd/file-input installation examples have been removed from the public tree.
The runtime retains file-input compatibility for the deployed rollback path;
the actual rollback source/unit/config remain on the host. Dated provenance keeps
the original paths and hashes as historical records, not installation guidance.

The source adapter retains `TWITCH_API_BASELINE`, the collector's existing
arguments/credentials and the original evidence contract. Its optional
`TWITCH_THUMBNAIL_FACTS_BIND` listener binds only the pod gateway. `/bundle`
returns a coherent pair, `/forward` relays only the fixed native observation,
and neither renews timestamps or exposes a general proxy. HTTP failure stays
UNKNOWN; there is no fallback to stale files. The old tmpfs mirror remains
compatible but is not an input to the Pod.

The public builder runs `collect_clock_public.py`; its SSH alias is configured
with `STREAM_V3_CLOCK_SSH_TARGET` (falling back to the monitoring alias). It emits
only the fixed numeric public schema and preserves an existing artifact's
original timestamps on fetch failure. The page loads the current renderer, and
the reference uploader includes that renderer and `clock-observation.json` in
its explicit allowlist. No generated snapshots or credentials are shipped.

The exporter overlay reads `TWITCH_THUMBNAIL_OBSERVATION_URL` from the private
publication Service. It preserves the existing metric/job labels and historical
series; `TWITCH_THUMBNAIL_OBSERVATION_FILE` remains a legacy option. HTTP errors
or an old publication cannot manufacture a new cycle timestamp. NetworkPolicy
limits the Pod's connections; node administrators remain trusted. Example
resources include no live host addresses, credentials or runtime state.

During rollback, confirm Pod termination and transfer its latest PVC state back
before starting the old worker. Do not rewind last-request/cooldown timestamps
or erase unknown history. The old source/unit/config remain available. Deployment
measurements and the public-screen checkpoint are in the
[dated migration record](thumbnail-k3s-migration-2026-11-01.md).

```bash
PYTHONDONTWRITEBYTECODE=1 OMP_THREAD_LIMIT=1 python3 -m pytest -q -p no:cacheprovider \
  tests/test_twitch_thumbnail.py tests/test_twitch_thumbnail_evidence.py \
  tests/test_clock_observation_alerts.py tests/test_thumbnail_kubernetes.py
promtool test rules ops/monitoring/prometheus/tests/twitch_thumbnail.yml
```

These tests use generated images, temporary files, local HTTP fixtures and mocked OCR. They make
no provider requests, send no notifications and change no delivery services.

## Dated Records

- [K3s collector migration](thumbnail-k3s-migration-2026-11-01.md): Service boundaries, preserved state, natural cycles and public-screen verification.

Each record describes its own window and source selection; none shows deployment
of this public checkout.

- [Slow-transfer recovery and missing-age display](thumbnail-transfer-recovery-2026-11-01.md):
  separate download/OCR budgets, transfer diagnostics and public-renderer verification.

- [Initial source port](public-source-port-2026-10-22-thumbnail.md): first
  operated window, resolution calibration, resources and source boundary.
- [Review follow-up](public-source-port-2026-10-22-thumbnail-review.md): stream
  rollover, missing-metric handling and the OCR retry with its reanalysis.
- [Operated OCR counter rollout](thumbnail-ocr-rollout-2026-10-22.md): activation
  and the first live counter progression.
- [OCR and evidence follow-up](public-source-port-2026-10-22-thumbnail-evidence.md):
  conservative interpolation, bounded evidence, ROI comparison and diagnostics.
