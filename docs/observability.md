# Observability

The observability layer exists to answer three questions:

1. Is the stream currently delivering video and audio?
2. Is YouTube receiving and serving the expected live URL?
3. Is a recovery action safe, necessary, and scoped to the right subsystem?

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
- `stream_v3_audio_dj_missing_count`
- `stream_v3_runtime_memory_current_mib`
- `stream_v3_runtime_memory_usage_ratio`
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
configuration used to present this evidence. It is an observability display and
retention stack; recovery ownership still flows through the monitor guard and
staged request path.

In the current production topology this monitoring backend remains on HP
ProDesk. Raspberry Pi has an nginx `/grafana/` proxy to HP ProDesk Grafana. The
public snapshot collector runs on Raspberry Pi, queries allowlisted
Prometheus/Loki evidence through the Pi-local
`http://127.0.0.1:8088/grafana` datasource proxy path, pushes static
JSON/assets outbound to GCS, and Cloudflare serves `yukimurata0421.dev`.
Existing `adsb-open.addevlab.com` Grafana shortcuts are separate Cloudflare
Tunnel routes back to Raspberry Pi nginx; Pi nginx shortcut paths such as
`/stream-v3-grafana` redirect into that path. They are not the static
`yukimurata0421.dev` status path.

The public status site at <https://yukimurata0421.dev/> is a separate,
sanitized evidence surface. It shows a static GCS + Cloudflare snapshot with
freshness, decision checks, guardrails, trends, and recovery-boundary summaries;
it does not expose Grafana, Prometheus, Loki, raw logs, credentials, or
home-network ingress. The boundary is documented in
[`v3/public-status-snapshot.md`](v3/public-status-snapshot.md).

## API Cost Guard

YouTube API usage is tracked separately from stream health. The watchdog can use
cached or public evidence when quota burn rate is too high. Recovery logic should
avoid creating additional API pressure during an incident unless that action is
explicitly justified.
