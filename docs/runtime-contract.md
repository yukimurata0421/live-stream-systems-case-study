# Runtime Contract

`stream_v3` treats streaming as a delivery-plane workload and monitoring as a
separate observability-plane workload.

The physical deployment has three hosts and five logical roles: HP ProDesk
`192.168.0.60` owns the Airspy/`airspy_adsb`/readsb source role and the
observability role; the Dell workstation `192.168.0.35` owns Dell-side readsb, a
modified tar1090 map endpoint, and the k3s delivery role; Raspberry Pi
`192.168.0.50` owns the public nginx status/dashboard gateway role.

The production ADS-B data path is:

```text
Airspy USB on HP ProDesk
  -> airspy_adsb
  -> readsb on HP ProDesk
  -> Beast feed to Dell 192.168.0.35:30104
  -> readsb on Dell workstation
  -> Dell modified tar1090 HTTP endpoint
  -> stream_v3 browser rendering and overlay
```

## Delivery Plane

The delivery plane runs the live output path:

- `stream-v3-runtime` k3s deployment
- browser rendering and overlay capture
- PulseAudio sink and monitor source
- AutoDJ playback and now-playing metadata
- FFmpeg RTMPS ingest
- NVIDIA NVENC H.264 encoding
- local fast recovery loop

The delivery runtime consumes ADS-B map/source data through the
browser map upstream environment contract. It does not manage the Airspy device
directly. The public map component is a modified tar1090 endpoint.

The production-oriented encoder target is:

```text
VIDEO_ENCODER=h264_nvenc
VIDEO_NVENC_RC=cbr
VIDEO_NVENC_PRESET=p4
FRAME_RATE=5
VIDEO_BITRATE=3400k
VIDEO_MAXRATE=3400k
VIDEO_BUFSIZE=6800k
AUDIO_BITRATE=192k
AUDIO_SAMPLE_RATE=48000
```

This is the current v3 runtime target. It is intentionally different from the
earlier v2 implementation, but it preserves the low-bandwidth lineage that moved
from 5fps/3500k/audio192k to 4fps/3400k/audio192k, then adopted
5fps/3400k/audio192k in v3 after a 4fps, 5fps, and 10fps upload/health trial.

CPU encoding is retained only as a fallback and local debug path.

## Observability Plane

The observability plane owns health classification and recovery requests:

- YouTube video resolver
- YouTube watchdog
- read-only YouTube Data API, OAuth, and public watch-page probes
- stream watchdog
- k3s runtime, state-file, and log evidence collection
- subsystem status summary
- recovery orchestrator
- notification status loop
- Prometheus exporter
- `ops/monitoring` Prometheus, Loki, Grafana, and Alloy configuration for
  evidence presentation
- staged remote recovery request tooling

The observability plane may request recovery, but it does not directly own the
FFmpeg process.

## Public Gateway

Raspberry Pi exposes the public nginx `:8088` status UI and proxies `/grafana/`
to HP ProDesk Grafana. Prometheus and Loki remain on HP ProDesk in the current
production shape; the Raspberry Pi is an entrypoint, not the metrics/logs
backend.

## Safety Gates

Production mutation is guarded by explicit flags and supervisor mode:

```text
STREAM_V3_MODE=streaming
STREAM_V3_CUTOVER_ENABLE=1
STREAM_K8S_DRY_RUN=0
TEST_MODE=0
```

Shadow mode keeps `TEST_MODE=1`, `STREAM_K8S_DRY_RUN=1`, and
`STREAM_V3_CUTOVER_ENABLE=0`.

`src/stream_v2/recovery_orchestrator/gate.py` intentionally reports
`shadow_budget_not_enforced` and `shadow_cooldown_not_enforced` in shadow
plans. Shadow mode must explain what would happen without mutating restart
history, consuming budget, or extending cooldown. Production enforcement lives
in the mutating recovery paths:

- `src/watchers/fast_recovery.py` enforces restart-induced downtime budgets,
  block events, and sustained-emergency overrides for local delivery recovery.
- `src/watchers/youtube_health.py` and `src/watchers/decision/action_gate.py`
  enforce restart budgets, cooldown, budget-release reconfirmation, and
  emergency override evidence before YouTube-oriented recovery actions.
- Production mutation also requires the explicit mode and dry-run flags above.

## State Boundary

Runtime state lives under `.state/` or `/state` in deployment contexts. It is
not part of the public repository. State files include now-playing metadata,
runtime heartbeats, watchdog summaries, SLI outputs, and recovery action plans.
