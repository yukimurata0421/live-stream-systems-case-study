# Executive Summary

`stream_v3` is a public reliability engineering case study for a self-built
24/7 YouTube Live pipeline. The workload is ADS-B visualization with NCS music,
but the engineering focus is the delivery system around it: browser rendering,
PulseAudio, AutoDJ, FFmpeg/NVENC, YouTube evidence, API quota, observability,
guarded recovery, k3s runtime boundaries, and public-release safety.

## What Was Built

The current public architecture separates the system into two main planes:

- Dell delivery node: browser rendering, audio, AutoDJ, FFmpeg, NVENC, RTMPS,
  fast local recovery, and k3s runtime ownership.
- HP ProDesk observability host: YouTube resolver/watchdog, stream watchdog,
  SLI summaries, notifications, Prometheus/Loki/Grafana, and staged recovery
  requests.

The ADS-B source chain is also explicit: Airspy on HP ProDesk, `airspy_adsb`,
ProDesk readsb, Dell readsb, Dell modified tar1090, and then the `stream_v3`
delivery workload. Raspberry Pi is only the public status/dashboard gateway,
not the monitoring backend.

The current public status site, <https://yukimurata0421.dev/>, is a sanitized
static snapshot served through GCS + Cloudflare. It is meant to expose freshness,
decision checks, guardrails, trends, and recovery-boundary summaries without
publishing Grafana, Prometheus, Loki, raw logs, credentials, or home-network
ingress.

## What Makes It Operationally Interesting

The hard part is not only publishing pixels to YouTube. The system has to avoid
bad recovery actions while real symptoms are ambiguous:

- a TCP stall must not automatically become a YouTube broadcast replacement;
- a YouTube API or OAuth failure must not automatically become delivery
  failure;
- lower upload is not accepted if it makes YouTube classify the input as
  unhealthy;
- a live RTMPS socket is not proof that viewers see correct video and audio;
- dashboard red state is not trusted until raw metric source, freshness, and
  owner are checked.

The central operating invariant is same-watch-URL preservation. Short local
recovery is acceptable when it protects the public URL. Destructive YouTube
lifecycle mutation is intentionally harder because it can break viewers,
bookmarks, embeds, and external links.

## Highest-Signal Evidence

| Claim | Public evidence |
| --- | --- |
| Recovery is guarded before destructive action. | `src/stream_v2/recovery_orchestrator/gate.py`, `ops/scripts/v3_shadow_acceptance.py`, `tests/test_v3_shadow_acceptance.py` |
| Same-URL continuity is a production invariant. | `docs/28-day-same-url-sli-case-study.md`, `docs/v3/youtube-lifecycle-safety.md` |
| TCP stall diagnosis was split by evidence layer. | `docs/v3/tcp-stall-case-study.md`, `ops/scripts/wan_address_observer.py`, `ops/scripts/persistent_tcp_anchor_observer.py` |
| Encoder/upload tuning uses measured YouTube health, not nominal bitrate alone. | `docs/v3/encoder-upload-case-study.md`, `docs/v3/encoder-fps-tuning-2026-05-31.md` |
| Public operational evidence is reduced before publication. | `docs/v3/public-status-snapshot.md`, <https://yukimurata0421.dev/> |
| Public validation is non-mutating. | `.github/workflows/public-snapshot-check.yml`, `docs/test-strategy-and-safety-boundary.md` |
| Claims can be mapped to code and tests. | `docs/implementation-review-map.md` |

## What This Repository Does Not Claim

This repository does not claim to be a reusable streaming product. It does not
ship secrets, production state, media files, raw private logs, or a one-command
restore procedure. Public CI does not perform live YouTube mutation or apply to
a production cluster.

Some reliability claims are measured, some are tested, and some are only
documented as future drills. `docs/operational-scorecard.md` is the compact
ledger for that distinction.

## Suggested Review In 20 Minutes

1. `README.md`
2. `docs/operational-scorecard.md`
3. `docs/implementation-review-map.md`
4. `docs/v3/youtube-lifecycle-safety.md`
5. `docs/v3/tcp-stall-case-study.md`
6. `docs/v3/encoder-upload-case-study.md`
7. `docs/v3/public-status-snapshot.md`
8. `docs/test-strategy-and-safety-boundary.md`

The review signal is operational judgment: naming failure domains, refusing
unsafe actions from weak evidence, and keeping public claims proportional to the
evidence that exists.
