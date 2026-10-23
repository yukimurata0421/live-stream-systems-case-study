# SLI And Dashboard Contract

The dashboard separates present state from historical degradation.

The [ADS-B source coverage contract](adsb-source-coverage.md) defines how
irregular monitor events cover time, and why complete evidence coverage is
different from lossless reception of individual ADS-B messages.

## Measured Results To Read First

| Question | Public answer | Evidence boundary |
| --- | --- | --- |
| What RTO has actually been drilled? | k3s service restart recovered to stream_v3 observability metrics OK in 10.7 seconds. The same FFmpeg PID and TCP socket survived the drill, and `bytes_sent` advanced by 37,503,068 bytes. | This is k3s control-plane / observability RTO plus RTMPS process-continuity evidence. It is not node reboot, disk restore, RTMPS reconnect RTO, or readsb/tar1090 source recovery. |
| Was there monitored viewer-facing burn in that drill? | No monitored burn: YouTube ingest, public watch, same-URL, and watchdog metrics stayed OK in the sampled window. | Prometheus/YouTube sampling does not prove every frame seen by every viewer. |
| How should error budget be read? | Same-watch-URL continuity is an invariant. A drill can burn control-plane budget without burning viewer-facing budget if the public stream remains live and the RTMPS socket continues sending. | Long-window same-URL and availability claims still require 14-day or 28-day review windows. |
| What MTTR has been retained? | Historical `tcp_stall` clusters had 90.0s median local transport MTTR, 1190.8s p95, and 1474.0s max. | Local transport recovery is not automatically viewer MTTR. |
| What recovery-classifier replay is retained? | On 2026-06-06, the current classifier replay covered retained fast-recovery stream restarts at 5/5 in 7d and 6/6 in 30d. | This is classifier replay over historical events. It does not backfill old shadow logs or prove executor production execution. |

## Metric Classes

The dashboard must keep objective classes separate:

| Class | Dashboard signals | Reading rule |
| --- | --- | --- |
| Production Invariant | same URL preservation, same watch URL continuity, replacement broadcast count, single-publisher safety | Preserve identity and block destructive actions. Do not average these into availability. |
| Primary SLI | YouTube availability, YouTube public/live/ingest state, ADS-B source freshness, local ingest connected, visual correctness, audio route and RMS health | Product health. Report ratios only with a clear window, denominator, and evidence source. |
| Guardrail | FFmpeg TCP send budget, YouTube API daily units, runtime memory, recovery action safety | Operating boundary. A pass means the system stayed inside a constraint, not that viewers saw perfect output. |
| Secondary SLI | now-playing freshness, notification delivery, public snapshot freshness, private dashboard reachability | Supporting behavior. Important to operate, but not automatically a delivery outage. |
| Event / Incident Metric | FFmpeg exits and restarts, TCP stalls, TLS failures, resolver fast mode, recovery requests | Count, cluster, root-cause, and measure MTTR. Do not turn raw attempts into a fake availability percentage. |

## Current Input Quality And Historical Gates

The raw OAuth watchdog state is the authority for current YouTube input
quality. Prometheus is a sampled projection of that same source. The report
keeps two comparisons with different scopes:

| Field | Scope | Reading rule |
| --- | --- | --- |
| `source_disagreement` | Raw and Prometheus measurements over the report window. | Preserve historical differences and the formal gate's measurement status. A current recovery does not rewrite that window. |
| `source_disagreement_current` | Latest usable raw state and Prometheus state at the report end. | Compare their `eligible` and `good` values only when both sources are available within their freshness limits. |

Read the current comparison together with `raw_current.available`,
`prometheus_current.available` and `current_crosscheck_reason`:

| `current_crosscheck_reason` | Current comparison |
| --- | --- |
| `current_states_agree` | Both sources are available and their eligibility and quality flags agree. |
| `current_state_disagreement` | Both are available and at least one compared flag differs; `source_disagreement_current=true`. |
| `raw_current_state_unavailable` | Raw evidence is unavailable or stale; comparison is unavailable. This reason also takes precedence when both sources are unavailable. |
| `prometheus_current_state_unavailable` | Raw evidence is available but Prometheus evidence is unavailable or stale; comparison is unavailable. |

The disagreement boolean is false in both agreement and unavailable cases.
Only the reason and availability fields distinguish those outcomes. Agreement
establishes consistency between projections; current quality still depends on
the raw state's eligibility, classification and `good` flag.

Actionable raw OAuth warning/error evidence and multi-window burn have separate
input-quality notification paths. Raw/Prometheus comparison alone does not open
a current input-quality incident. Measurement coverage/freshness, historical
formal compliance and current quality remain separate. The current comparison
does not change SLI targets, windows or gates. See the
[notification contract](notification-and-auto-recovery.md#delivery-and-current-state-rules)
for delivery and recovery rules.

## Error Budget Rule

Same-watch-URL continuity is a production invariant, not merely another primary
SLI. A short local restart can be less damaging than replacing the public YouTube
watch URL, because the URL carries viewers, bookmarks, embeds, and external
links.

Availability and URL identity are higher priority than visual quality warnings.
Encoder changes should not sacrifice delivery continuity unless the operator
explicitly accepts that tradeoff.

The upload ceiling is a warning boundary, not the tuning target. The
5fps/3400k contract (4000k since 2026-10-22) was accepted because the measured windows stayed below
5.0 Mbps while avoiding the larger per-frame quality loss of 10fps.
`encoder-upload-case-study.md` documents the additional encoder lesson: the
move to NVENC CBR increased measured upload versus the older v2 CPU path, and
the lower-upload VBR/CQ profile was rejected because YouTube input health got
worse.

Visual correctness, audio correctness, ADS-B source freshness, and memory
guardrails are also separate from transport availability. See
`visual-audio-health-model.md`, `memory-guard-case-study.md`, and
`failure-taxonomy.md` for the operational split.

The measured v2 baseline that established this classification is summarized in
[`../sli-methodology.md`](../sli-methodology.md). That page is historical
evidence for the method, not a current v3 uptime statement.

The
[`historical 28-day review and continuity checkpoints`](../same-url-continuity-evidence.md)
keeps its original regime-bounded ratios separate from the later exact-identity
ledger. Neither artifact is retroactively reinterpreted through a newer schema.

`rolling-sli-error-budget-feedback.md` documents the current feedback reading
rule: rolling 24h, 7d, and available 30d windows are used for operator
feedback, while public reliability claims remain tied to explicitly documented
historical windows or a named exact-identity ledger endpoint. It also records why same-URL
metric-zero samples are not budget burn unless URL identity evidence or
replacement evidence also fails.

## Dashboard Caution

Long-window fields can be stale. Operators should compare dashboard signals
against fresh runtime evidence before deciding that the stream is currently
failing.

Exporter timeout and `No data` dashboard states are observability-plane
incidents until raw delivery evidence also fails. `observability-plane-self-check.md`
documents the snapshot fallback, last-good exporter payload, and monitoring
watchdog contract used to keep that distinction visible.

Shadow recovery comparison uses executable recovery intent, not every selected
report-only action. `current_classifier_replay` is shown separately from
`production_without_shadow` so historical gaps remain visible while current
classifier coverage can be reviewed. See
[`fast-recovery-classifier-replay.md`](fast-recovery-classifier-replay.md).

## Metric Contract Hygiene

The exporter should not turn missing evidence into `0` or OK. ADS-B and audio
dashboard panels read the subsystem snapshot first:

```text
stream_v3_adsb_evidence_age_seconds
stream_v3_adsb_rendering_ok
stream_v3_adsb_messages_moving
stream_v3_audio_ok
stream_v3_audio_fault_count
```

Legacy pulse, SLO, cgroup, and stream-watchdog detail files are emitted only
when the underlying state field exists. A missing file means "not observed,"
not "healthy."

Window labels must describe the actual aggregation window. A 24-hour restart or
TLS count must not be copied into `window_hours="1"` or `window_hours="8"`.
Open-day YouTube API cost is a single PT-day gauge
`stream_v3_youtube_api_open_day_units`; historical closed days remain labelled
by `pt_day`.

Runtime memory uses `stream_v3_runtime_memory_*` for delivery Pod alerting.
Monitoring-host memory is exported with `stream_v3_monitor_host_*` names as
diagnostic capacity evidence, not as the primary v3 runtime memory guard.
