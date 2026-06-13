# Failure Taxonomy

`stream_v3` avoids treating every warning as a stream outage. Failures are
classified by owner, detection source, action boundary, and evidence needed
before recovery.

## Principles

- A dashboard `FAIL` is not enough to restart delivery.
- A YouTube API error is not automatically a delivery failure.
- Audio or visual faults must not create a new YouTube broadcast by themselves.
- Monitoring unknowns must not authorize destructive delivery actions.
- Same-URL preservation remains a production invariant.

## Taxonomy

| Failure | Primary detection | Owner | Allowed response | Evidence |
| --- | --- | --- | --- | --- |
| `runtime_pod_not_ready` | Pod not `3/3 Running` or deployment unavailable | delivery plane | inspect Pod events/logs; rollout restart if justified | `kubectl`, container logs, runtime state |
| `stream_engine_missing_or_crashed` | stream-engine restart count, missing Xvfb/Chromium/FFmpeg | delivery plane | recover browser/audio/FFmpeg stack | stream-engine events, Pod status |
| `xvfb_shmem_runaway_oom` | Xvfb RSS/RssShmem guard, kernel OOM, cgroup events | delivery plane | ordered capture-stack restart | process status, cgroup events, memory guard |
| `ffmpeg_ingest_disconnected` | missing RTMPS socket or stale send samples | delivery plane | fast recovery or FFmpeg child restart | fast-recovery events, TCP samples |
| `tcp_stall` | queued bytes, growing `lastsnd_ms`, low send Mbps | delivery + observability | local recovery; cause observers stay report-only | TCP samples, WAN anchors, same URL state |
| `low_upload_pressure` | low upload plus queue pressure | delivery plane | temporary recovery profile only after strong evidence | upload latest/p95, queue metrics |
| `youtube_low_bitrate_warning` | YouTube warning while local delivery continues | observability plane | compare encoder, upload, public/live state | watchdog stats, Studio/API evidence |
| `dashboard_false_fail` | dashboard red but raw evidence healthy | observability plane | fix query/exporter/source labels | raw Prometheus, exporter output |
| `same_url_changed` | resolver/watchdog reports URL identity mismatch | observability plane | verify public URL, resolver cache, broadcast identity before action | resolver state, public watch page, API/OAuth |
| `visual_data_fetch_error` | capture shows map error band or blank map | delivery or ADS-B source boundary | repair overlay/source path; do not mutate YouTube | capture, upstream report, browser source |
| `now_playing_unknown` | overlay metadata shows unknown title | delivery plane | repair metadata path or DJ state | now-playing JSON/text, play history |
| `pulse_unavailable` | PulseAudio socket/sink/source missing | delivery plane | repair Pulse/DJ/audio route | `pactl`, audio logs, sink/source state |
| `audio_energy_low` | monitor energy low while video may be healthy | delivery plane | staged DJ/audio/stream recovery; never broadcast replacement | RMS/volumedetect, transition grace state |
| `upstream_adsb_stale` | ADS-B JSON or aircraft movement stale | observability/source boundary | classify source freshness; avoid FFmpeg restart unless delivery also fails | overlay/upstream reports |
| `report_missing` | report-only artifact stale or absent | observability plane | fix timer/output path/stale threshold | report JSONL, systemd timer logs |
| `prometheus_stale_metric` | metric timestamp or source label stale | observability plane | fix exporter/query/source labels | raw metrics, scrape timestamp |
| `exporter_timeout_no_data` | exporter health fails or required `stream_v3_*` series missing while raw delivery evidence is healthy | observability plane | refresh snapshot, inspect exporter/watchdog, fix metric contract | exporter `/metrics`, snapshot age, monitoring watchdog state |
| `same_url_metric_zero_only` | same-URL metric drops to zero while resolver/watchdog identity and replacement evidence remain healthy | observability plane | treat as stale/unknown observability until raw URL identity fails | resolver state, public/API/OAuth evidence, replacement counters |
| `memory_guard_warn` | memory warning without current runtime impact | delivery + observability | observe and correlate; no destructive action by memory alone | cgroup/process/PSI/runtime evidence |
| `secrets_missing` | redacted env or Kubernetes Secret missing | delivery/control plane | restore secret from local private source; do not log value | redacted env checks |
| `gpu_nvenc_unavailable` | FFmpeg cannot open NVENC | delivery plane | fix GPU runtime/driver/device plugin | FFmpeg log, `nvidia-smi`, resource request |

## Escalation Rules

Immediate delivery-plane recovery is appropriate only when fresh evidence shows
the delivery path is actually broken: Pod unavailable, FFmpeg missing, RTMPS
send stopped, Pulse unavailable, GPU unavailable, or capture stack failure.

Observability-plane problems are handled in the observability plane first:
stale metrics, report-missing events, false dashboard failures, resolver cache
mismatch, and notification noise.

YouTube lifecycle mutation is the highest-risk class. It requires explicit
identity, ownership, freshness, and action-gate evidence.

## Required Evidence

When a failure is recorded publicly or internally, the minimum evidence shape is:

- timestamp and timezone;
- failure name from the taxonomy;
- delivery-plane and observability-plane observations;
- capture or reason capture was unavailable;
- audio probe result;
- raw metric names and values;
- YouTube public/live/ingest/same-URL state;
- recovery action, actor, and result if any;
- residual risk or follow-up.
