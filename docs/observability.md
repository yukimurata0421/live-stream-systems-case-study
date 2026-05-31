# Observability

The observability layer exists to answer three questions:

1. Is the stream currently delivering video and audio?
2. Is YouTube receiving and serving the expected live URL?
3. Is a recovery action safe, necessary, and scoped to the right subsystem?

## Signals

Key signal groups:

- local FFmpeg process and TCP ingest state
- upload throughput, `notsent`, `unacked`, and `lastsnd` samples
- YouTube Data API and public watch-page state
- resolver cache freshness
- now-playing metadata freshness
- PulseAudio route and RMS checks
- runtime heartbeat age
- memory guardrail and OOM indicators
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

## Dashboards

Dashboard state should separate current failures from historical degradation.
For example, a stale long-window field must not override a fresh local ingest
sample. The monitoring layer records both so an operator can tell whether the
fault is current, historical, or a dashboard false positive.

`ops/monitoring/` contains the Prometheus, Loki, Grafana, and Alloy
configuration used to present this evidence. It is an observability display and
retention stack; recovery ownership still flows through the monitor guard and
staged request path.

## API Cost Guard

YouTube API usage is tracked separately from stream health. The watchdog can use
cached or public evidence when quota burn rate is too high. Recovery logic should
avoid creating additional API pressure during an incident unless that action is
explicitly justified.
