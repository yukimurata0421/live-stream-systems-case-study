# Program Map

## Delivery Plane

| Program | Owner | Purpose |
| --- | --- | --- |
| `stream_v3.control_loop` | k3s runtime | loop runner for selected task sets |
| `stream_core.stream_engine` | delivery plane | browser, audio, FFmpeg, ingest |
| `dj.auto_dj` | delivery plane | music playback and metadata |
| `watchers.fast_recovery` | delivery plane | local recovery evaluation |

## Observability Plane

| Program | Owner | Purpose |
| --- | --- | --- |
| `watchers.youtube_video_id_resolver` | observability plane | resolve current live video identity |
| `watchers.youtube_watchdog` | observability plane | YouTube health and lifecycle evidence |
| `watchers.stream_watchdog` | observability plane | local delivery and stream evidence |
| `stream_v2 recovery_orchestrator` | observability plane | action planning and guard checks |
| `stream_v3_prometheus_exporter.py` | observability plane | Prometheus metrics |

## Deployment Assets

- `deploy/k3s/Containerfile`
- `deploy/k3s/shadow`
- `deploy/k3s/streaming`
- `deploy/k3s/v3-observer`
- `ops/systemd/stream-v3-arena-monitor.service`
- `ops/monitoring/prometheus/prometheus.yml`
- `ops/monitoring/prometheus/rules/stream_v3.yml`
