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
| `stream_v3_health_snapshot.py` | observability plane | last-known-good health and objective SLI snapshots |
| `stream_v3_monitoring_watchdog.py` | observability plane | exporter, metric-contract, and snapshot freshness self-check |

## Host Diagnostic Observers

These programs are report-only instrumentation for recurring RTMPS transport
faults. They do not restart the stream or mutate YouTube state.

| Program | Owner | Purpose |
| --- | --- | --- |
| `wan_address_observer.py` | delivery host | route, public IPv4, IPv6 prefix, and fresh TCP-anchor samples |
| `persistent_tcp_anchor_observer.py` | delivery host | long-lived Cloudflare/Google TCP/TLS anchors and reconnect-after-failure evidence |
| `stream-v3-wan-address-observer-burst.timer` | delivery host | 10-second samples during the recurring morning validation window |
| failure-triggered WAN snapshot | delivery host | 5-second follow-up samples after all-anchor failure or failed reconnect-after-failure |

## Deployment Assets

- `deploy/k3s/Containerfile`
- `deploy/k3s/shadow`
- `deploy/k3s/streaming`
- `deploy/k3s/v3-observer`
- `ops/systemd/stream-v3-observability-monitor.service`
- `ops/systemd/stream-v3-health-snapshot.timer`
- `ops/systemd/stream-v3-monitoring-watchdog.timer`
- `ops/systemd/stream-v3-wan-address-observer.timer`
- `ops/systemd/stream-v3-wan-address-observer-burst.timer`
- `ops/systemd/stream-v3-persistent-anchor-observer.service`
- `ops/monitoring/prometheus/prometheus.yml`
- `ops/monitoring/prometheus/rules/stream_v3.yml`
