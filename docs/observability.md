# Observability

The observability layer exists to answer three questions:

1. Is the stream currently delivering video and audio?
2. Is YouTube receiving and serving the expected live URL?
3. What fresh facts can a separate recovery authority use without Monitoring
   deciding or executing the action itself?

The measured SLI baseline and classification rules are summarized in
[`sli-methodology.md`](sli-methodology.md). That page uses v2 production evidence
as a historical baseline for the method; current v3 dashboard panels must still
use v3 source labels and current runtime evidence.

## Signals

Key signal groups:

- local FFmpeg process and TCP ingest state
- k3s runtime status and in-container probes
- upload throughput, `notsent`, `unacked`, and `lastsnd` samples
- report-only WAN identity and TCP anchor probes for transport root-cause
  splitting
- YouTube Data API, OAuth, and public watch-page state
- resolver cache freshness
- now-playing metadata freshness
- visual correctness checks
- PulseAudio route and RMS checks
- runtime heartbeat age
- capture-helper memory guardrail and OOM indicators
- Prometheus scrape freshness

## Metrics

The v3 exporter focuses on runtime and decision evidence:

- `stream_v3_upload_latest_mbps`
- `stream_v3_upload_p95_mbps`
- `stream_v3_network_ffmpeg_socket_lastsnd_ms`
- `stream_v3_network_ffmpeg_socket_notsent_bytes`
- `stream_v3_adsb_evidence_age_seconds`
- `stream_v3_audio_ok`
- `stream_v3_audio_fault_count`
- `stream_v3_runtime_memory_current_mib`
- `stream_v3_runtime_memory_usage_ratio`
- `stream_v3_monitor_host_mem_available_ratio`
- `stream_v3_recovery_action_pending`
- `stream_v3_recovery_action_executable`

Metric names and dashboard queries must keep production and shadow sources
separate. v3 evidence uses `stream_v3_*` metrics and the v3 observability monitor job;
v2-compatible names should not be aggregated with broad `max()` queries unless
the job/source label is intentionally scoped. A red panel is a prompt to inspect
the raw series and source labels, not proof that the delivery plane is down.

Exporter mappings are treated as contracts. If a health summary JSON shape
changes, the exporter and dashboard tests must verify the exact path being read;
otherwise a panel can show a convincing but false `PASS` or `FAIL`.
Missing optional state is not exported as `0`. For example, absent ADS-B,
audio, SLO, cgroup, or stream-watchdog detail files should remove the metric
series or fall back to subsystem evidence; they must not create a false OK.

Twitch's required OCI path is assessed from relay state, input and output
counter progress, and fresh API observations. An API timeout with advancing OCI
counters is observation degradation; an explicit path interruption is a separate
critical condition. The derived telemetry feeds notification decisions, while
raw signals remain available for diagnosis. See the Monitoring v4
[relay/provider evidence](../monitoring-v4/docs/relay-provider-evidence.md#oci-aware-notification-decisions)
for cadence, metric definitions, channel routing, and recovery confirmation.

## Video Observation Layers

"Is the video still advancing for viewers?" has no single signal. Each layer
below observes a different point on the path, at a different cadence. This
table is the one place that defines how far each layer reaches; component
contracts link here instead of repeating it.

| Layer | Observed at | Cadence | Supports | Does not establish |
| --- | --- | --- | --- | --- |
| Return-feed clock | OCI common relay output, read back to the delivery host | 10 s samples, continuous reader | The rendered JST/UTC clock advanced at the relay output; display age. | Reception by YouTube or Twitch. |
| Fallback-sender clock | Secondary output of the direct YouTube backup encoder | 10 s while the fallback runs | The backup encoder output advanced. | YouTube accepting or serving it. |
| Native forward counters | Per-destination forward connections on the relay | 60 s, 90 s freshness | Bytes progressed on each destination's connection. | Provider decoding. |
| Provider API | YouTube stream status and health; Twitch Get Streams | Provider-specific | The provider reported LIVE, OFFLINE, or a health state. | Continuous decoded video. |
| Provider thumbnail clock | Twitch `thumbnail_url` image (Twitch only) | 60 s polls; new images arrived every six minutes in the observed window | Twitch produced an image whose clock advanced. | Video CDN, player, or audio health. |
| Public player | Not observed | — | — | — |

No layer here establishes viewer playback, so none is reported as end-to-end
success. Layers are combined, not substituted: a stale thumbnail with progressing
forward bytes is a disagreement to diagnose, not proof that video stopped, and
an unavailable return reader cannot stand in for the fallback sample. YouTube
has no provider-image layer. Public-player acquisition is not used.

The return-feed and fallback layers are defined by the
[return-feed clock monitor](v3/relay-clock-monitor.md); the thumbnail layer by
the [thumbnail contract](v3/twitch-thumbnail-monitor.md). Both report observation
gaps as warnings and hold no recovery authority. The
[clock observation case study](v3/clock-observation-case-study.md) explains how
this layering was reached, including a period when the return reader was blind
for hours without any warning.

`stream_v3_oci_monitor_clock_advancing` and the corresponding fallback metric
require paired OCR progression and fresh runtime/source binding.
`stream_v3_monitor_probe_*` reports scope, native flow, delay validity, freeze
coverage, failure class, source freshness and configuration diagnostics. Display
age is measured at complete decoded-frame arrival and includes decoder and queue
delay. Missing observations do not count as observed freeze time. Importer cycle
deduplication protects sample denominators; collector failures have a separate
history.

## Dashboards

Dashboard state should separate current failures from historical degradation.
For example, a stale long-window field must not override a fresh local ingest
sample. The monitoring layer records both so an operator can tell whether the
fault is current, historical, or a dashboard false positive.

Transport cause probes are intentionally separated from recovery authority.
`ops/scripts/wan_address_observer.py` records route, address, public IPv4, and
fresh TCP-anchor state. `ops/scripts/persistent_tcp_anchor_observer.py` keeps
non-YouTube TCP/TLS anchors open and records whether reconnect succeeds after an
existing-flow failure. The TCP stall case study in
[`v3/tcp-stall-case-study.md`](v3/tcp-stall-case-study.md) shows how those
signals were used to exclude YouTube-ingest and Google-only explanations before
classifying the recurring event as a WAN/session refresh.

Viewer-facing media health is tracked separately from transport state.
[`v3/visual-audio-health-model.md`](v3/visual-audio-health-model.md) explains
why RTMPS connected is not proof of correct video or audio.
[`v3/memory-guard-case-study.md`](v3/memory-guard-case-study.md) explains why
Xvfb shared-memory pressure is handled as capture-stack evidence instead of a
generic reason to mutate YouTube lifecycle state.

YouTube lifecycle state is also deliberately isolated from delivery symptoms.
[`v3/youtube-lifecycle-safety.md`](v3/youtube-lifecycle-safety.md) documents the
same-URL, quota, and stale-cache gates that must pass before destructive
YouTube actions are allowed.

`ops/monitoring/` contains the Prometheus, Loki, Grafana, and Alloy
configuration used to present this evidence. It is a display and retention
stack running on `arena-server` (HP ProDesk) and has no recovery authority.
The Raspberry Pi pulls allowlisted evidence from it and publishes a static
snapshot; the path is described once in
[architecture](architecture.md#public-status-path).

The public status site at <https://yukimurata0421.dev/> is a separate,
sanitized evidence surface. It shows a static GCS + Cloudflare snapshot with
freshness, decision checks, guardrails, trends, and recovery-boundary summaries;
it does not expose Grafana, Prometheus, Loki, raw logs, credentials, or
home-network ingress, and public readers do not consume the home uplink. The
boundary is documented in
[`v3/public-status-snapshot.md`](v3/public-status-snapshot.md).

## API Cost Guard

YouTube API usage is tracked separately from stream health. The watchdog can use
cached or public evidence when quota burn rate is too high. Recovery logic should
avoid creating additional API pressure during an incident unless that action is
explicitly justified.
