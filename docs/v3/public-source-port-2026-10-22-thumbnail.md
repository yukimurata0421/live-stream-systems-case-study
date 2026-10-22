# Public Source Port: Provider Thumbnails And Clock Observation Gaps

This dated port follows the [return-feed monitor port](public-source-port-2026-10-22.md).
It adds Twitch thumbnail observations beside the existing fast relay and native
facts, plus the preceding correction for prolonged return-reader failures.
The [stable thumbnail contract](twitch-thumbnail-monitor.md) defines scope and
thresholds; [current status](../current-status.md) owns later changes.

## Source And Publication Boundary

The selected monitoring source ends at commit `740bcc00`, following `9eb59246`.
The base public checkout is `5a787ee`, including the independent Twitch history
repair. That repair, its collector cadence/backoff, public publisher and UI are
preserved. A separate, concurrent thumbnail-review patch is outside this snapshot.
The public source is not a mirror of an uncommitted development directory.

New comments, notifications and documentation are English. Existing public
`delivery-host` / `relay-host` aliases, bindings and exporter mappings are kept.
Account, release and Tesseract paths are examples. The publication record tracks
source and public digests plus exclusions. No runtime state, raw logs, images,
credentials, private settings, dependency binaries or deployment receipts are ported.

## Dated Operated-System Evidence

These observations were obtained on the operated monitoring host on
2026-10-22 JST, before public adaptation. They do not show deployment of this
public checkout.

| Observation window or check | Result | Limit |
| --- | --- | --- |
| Thumbnail history, 10:29-10:59 JST | Thirty observations, five distinct clock values and four progress confirmations; no normal image files retained. | Short normal window, not an accuracy rate or player E2E test. |
| Real image calibration | 720p UTC confidence was 45.4; 1080p JST/UTC confidence was 96.0/95.2. | One image at each size; no general OCR quality guarantee. |
| 1080p fetch and OCR | About 263 KB, 1.907 s elapsed and 0.297 s CPU. | Includes the stated single request only. |
| Worker resources | CPU quota 0.2 core, memory limit 256 MiB, observed peak about 56.1 MiB, no swap. | Peak and cumulative CPU are not a steady-state average. |
| ADS-B, 90-second window | Same readsb PID; 25,972 additional messages; position JSON age at most 0.862 s; observed memory PSI avg10 zero. | No stopped-monitor baseline or controlled RF conditions; not causal proof of zero impact. |
| API compatibility | Original collector source digest unchanged; normal cycles retained one API request. | Startup authentication/validation requests still occur. |
| Return-reader correction, 09:22-09:55 JST | FFmpeg 5.1.9 with 5 s / 5 MB probing; isolated production-like 5 fps / GOP 10 acquisition passed 6/6. | Isolated primary/monitor exercise, not a platform reception test. |

The return-reader correction also adds reconnect jitter, probe-failure reason
codes, cumulative timing checks and stable clock identity across reader sessions.
Runtime attestation is cached for at most 60 seconds once ready, ten seconds
before readiness, and invalidated by relevant boot/Pod/peer changes. Older native
faults cannot override a newer verified frame; disagreement remains diagnostic.
Five-minute observation-gap warnings reuse the existing notification baseline.

## Validation And Remaining Limits

The selected private commit tree passed 83 related source tests. Public checks
are recorded separately in
[`publication/thumbnail-clock-port-20261022.json`](../../publication/thumbnail-clock-port-20261022.json).
They cover URL/source binding, original API payload/request compatibility,
cached-image deduplication, date boundaries, uncertain reads, bounded native and
API freshness, warning recovery, documentation links, English prose and selected
source digests. They use temporary local state and mocked transport.

No public example was deployed, no stream was switched and no test notification
was sent. Twenty-four-hour quality, thumbnail refresh distribution, daily stream
rollover and actual warning delivery remain unverified in this snapshot. The
operated short normal window does not establish any of those results.
