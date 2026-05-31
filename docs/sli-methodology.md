# SLI Methodology And Measured Baseline

This page is a curated public version of the SLI method that shaped `stream_v3`.
The measured numbers below are a `stream` / `stream_v2` production baseline from
May 2026. They are not presented as current `stream_v3` uptime. Their value is
that they show how the project measured the system before the v3 split-plane
design inherited the same operating constraints.

## Why This Baseline Matters

The v3 documents describe the current architecture, but the SLI discipline came
from the v2 production period. That history matters because v3 did not change the
objective function; it moved delivery into k3s and made the observation plane
clearer. The durable rule is:

```text
Do not collapse every signal into one availability percentage.
```

For this stream, the public YouTube URL is part of the product identity. A short
local restart can be acceptable if the same watch URL survives. Creating a
replacement broadcast may keep "a stream" online, but it can lose viewers,
bookmarks, embeds, and external links. For that reason, same-watch-URL continuity
is treated as a production invariant, not as a peer availability SLI.

## Metric Classification

| Classification | Examples | How to read it |
| --- | --- | --- |
| Production Invariant | `same_watch_url_continuity`, `replacement_broadcast_count` | Identity and destructive-action safety. Keep the URL stable and replacement count at zero; do not average this into availability. |
| Primary SLI | YouTube live ingest/public state, ADS-B JSON availability, ADS-B messages moving, overlay/upstream ADS-B map-source availability | Viewer-facing product value and source freshness. Report as ratios with windows and denominators. |
| Guardrail | Upload budget, YouTube API daily units, memory pressure, recovery action safety | Operating bounds that prevent the system from damaging itself or the shared environment. A pass does not prove viewer quality. |
| Secondary SLI | Discord delivery observation, now-playing freshness | Supporting behavior. Failures matter, but they are not automatically stream outages. |
| Event / Incident Metric | FFmpeg exits, TCP stalls, fast-recovery restarts, resolver fast mode | Count, cluster, root-cause, and measure MTTR. Turning these directly into a percentage usually hides the real story. |

This classification is the practical difference between "the dashboard has many
panels" and "the operator knows which objective each panel protects."

## Measured Baseline

Label: **v2 baseline, 2026-05, 14-day observation snapshot**.

Requested window: `2026-05-06 10:35:44 JST` to
`2026-05-20 10:35:44 JST`. Some SLIs started later than the requested window;
their actual observation windows are shown separately. The source rows came from
the production `stream` and `stream_v2` runtime JSONL logs, including current and
rotated files.

| Class | Signal | Observation window | Measured result | Interpretation |
| --- | --- | --- | --- | --- |
| Primary SLI | YouTube strict OK | `2026-05-06 10:36:17` to `2026-05-20 10:34:31 JST` | `3608 / 3656` samples, `98.687%` | Strict status only. Warnings, degraded public state, restart, and startup grace were counted as non-OK. |
| Guardrail | Upload within 5 Mbps | `2026-05-10 12:33:05` to `2026-05-20 10:35:35 JST` | `850586 / 852128` seconds, `99.819%` within budget | Upload stayed inside the warning ceiling for almost all measured seconds; this is not the same as viewer continuity. |
| Production Invariant | Same watch URL continuity | `2026-05-06 10:36:17` to `2026-05-20 10:34:31 JST` | `3561 / 3563` definitive samples, `99.944%`; replacement broadcasts observed locally: `0` | Two transient resolver mismatches recovered without replacement. The invariant stayed intact. |
| Primary SLI | ADS-B JSON OK | `2026-05-10 05:00:34` to `2026-05-20 10:21:11 JST` | overlay `941 / 941`, upstream `941 / 941`, `100.000%` | Source JSON was reachable in the measured report-only window. |
| Primary SLI | ADS-B messages moving | `2026-05-10 05:00:34` to `2026-05-20 10:21:11 JST` | overlay `940 / 941`, upstream `940 / 941`, `99.894%` | A 5-second sample proxy for source freshness, not a direct viewer-frame age measurement. |
| Primary / Report-only | Overlay and upstream ADS-B map-source availability | `2026-05-10 05:00:34` to `2026-05-20 10:21:11 JST` | overlay `940 / 941`, upstream `940 / 941`, `99.894%` | Useful evidence, but not a restart trigger by itself. |
| Secondary SLI | Discord delivery observation | `2026-05-10 05:12:29` to `2026-05-20 08:04:04 JST` | `351 / 361` deliveries, `97.230%` | Notification delivery quality, not proof of stream availability. |
| Guardrail | YouTube API quota-exceeded event rate | `2026-05-06 10:36:14` to `2026-05-20 10:34:31 JST` | `10 / 17836` calls, `0.056%` | Quota pressure was tracked separately from delivery health. PT-day accounting remained the authoritative view. |

Upload distribution during the measured upload window:

| Percentile | Mbps |
| --- | ---: |
| p50 | `4.693` |
| p95 | `4.818` |
| p99 | `4.869` |
| max | `10.614` |

The upload ceiling was a warning boundary, not the tuning target. The point was
to keep YouTube input quality and public continuity intact while staying inside a
shared-network budget.

## MTTR And Event Reading

The same snapshot recorded event and recovery timing, but it deliberately kept
these out of the availability percentage.

| Event layer | Count / group | p50 | p95 | max | Meaning |
| --- | ---: | ---: | ---: | ---: | --- |
| FFmpeg child self-recovery, exit `224` | `n=8` | `5.0s` | `5.7s` | `6.0s` | Broken-pipe-style child recovery inside the stream engine. |
| FFmpeg child self-recovery, exit `251` | `n=64` | `5.0s` | `5.0s` | `6.0s` | Engine-managed restart recovery. |
| Fast-recovery clusters, `tcp_stall` primary trigger | `9 clusters` | `90.0s` | `1190.8s` | `1474.0s` | Local transport recovered, but this is not direct viewer MTTR. |
| Fast-recovery clusters, `network_down` primary trigger | `4 clusters` | `145.0s` | `147.7s` | `148.0s` | Includes clusters near `ffmpeg_missing` attempts. |
| Fast-recovery clusters, `low_upload_pressure` primary trigger | `3 clusters` | `280.0s` | `1861.3s` | `2037.0s` | Shows why restart attempts must be clustered before interpretation. |
| Resolver fast mode | `10 pairs` | `51.0s` | `150.5s` | `195.0s` | URL identity search/confirmation mode; not automatically viewer-visible outage. |

This is the reason the public docs distinguish raw attempts, retry episodes,
incident clusters, and MTTR. `ffmpeg_restart_scheduled=102` over the snapshot is
not the same thing as 102 independent viewer incidents.

## Limitations And Unknowns

The baseline is useful because it is measured, and also because it names what it
does not prove:

- Viewer-visible interruption seconds were unknown. The snapshot did not measure
  YouTube player buffering, viewer reconnects, or frame generation changes as a
  direct viewer-time SLI.
- Fast-recovery MTTR was local transport MTTR, not viewer MTTR.
- Direct ADS-B age was unknown. The logs did not yet carry
  `aircraft_json_age`, `visual_frame_age`, or direct overlay frame staleness.
- Root overlap deduplication was unknown for the `stream` to `stream_v2`
  migration overlap. The snapshot did not assert a perfect cross-root duplicate
  key.
- Visual availability before `2026-05-10 05:00 JST`, upload budget before
  `2026-05-10 12:33 JST`, and Discord delivery before
  `2026-05-10 05:12 JST` were unknown because those measurement series had not
  started.
- YouTube replacement inventory was not fully audited through YouTube Studio or a
  full Data API inventory pass. The `replacement_broadcast_count=0` claim is a
  local watchdog and subsystem-log observation.
- PT-day API quota accounting had partial days at the edges of the JST 14-day
  window, so closed-day and open-day views were kept separate.

The important claim is therefore modest and verifiable: by May 2026 the project
had moved from "the stream appears to work" to "the stream is measured by
classified objectives, with denominators, windows, and explicit unknowns."

## How v3 Uses This

`stream_v3` should not copy the v2 numbers as current production status. It
inherits the method:

- keep URL continuity and replacement prevention as production invariants;
- keep YouTube live delivery, ADS-B freshness, visual correctness, audio health,
  and local ingest as distinct SLIs;
- keep upload, API quota, memory, and recovery safety as guardrails;
- report recovery behavior as attempts, episodes, clusters, root cause, and MTTR;
- label windows as rolling, cumulative, or regime-bounded before comparing them;
- publish unknowns instead of silently converting them into false precision.
