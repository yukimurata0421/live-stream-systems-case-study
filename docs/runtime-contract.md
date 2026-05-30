# Runtime Contract

`stream_v3` treats streaming as a delivery-plane workload and monitoring as a
separate observability-plane workload.

The physical deployment has three tiers: a Dell workstation for k3s delivery, an
HP ProDesk for observability / arena services, and a Raspberry Pi for ADS-B
edge/source data.

## Delivery Plane

The delivery plane runs the live output path:

- `stream-v3-runtime` k3s deployment
- browser rendering and overlay capture
- PulseAudio sink and monitor source
- AutoDJ playback and now-playing metadata
- FFmpeg RTMPS ingest
- NVIDIA NVENC H.264 encoding
- local fast recovery loop

The production-oriented encoder target is:

```text
VIDEO_ENCODER=h264_nvenc
VIDEO_NVENC_RC=cbr
VIDEO_NVENC_PRESET=p5
FRAME_RATE=30
VIDEO_BITRATE=3300k
VIDEO_MAXRATE=3300k
VIDEO_BUFSIZE=6600k
AUDIO_BITRATE=192k
AUDIO_SAMPLE_RATE=48000
```

CPU encoding is retained only as a fallback and local debug path.

## Observability Plane

The observability plane owns health classification and recovery requests:

- YouTube video resolver
- YouTube watchdog and public probe
- stream watchdog
- subsystem status summary
- recovery orchestrator
- notification status loop
- Prometheus exporter
- Loki and Grafana configuration
- staged remote recovery request tooling

The observability plane may request recovery, but it does not directly own the
FFmpeg process.

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

## State Boundary

Runtime state lives under `.state/` or `/state` in deployment contexts. It is
not part of the public repository. State files include now-playing metadata,
runtime heartbeats, watchdog summaries, SLI outputs, and recovery action plans.
