# Physical Topology

`stream_v3` is deployed as a three-tier physical system with k3s in the delivery
tier and a separate observability tier.

## Tiers

| Tier | Hardware | Responsibility |
| --- | --- | --- |
| Delivery | Dell workstation | k3s node for `stream-v3-runtime`, browser rendering, PulseAudio, AutoDJ, FFmpeg, NVIDIA NVENC, and local fast recovery |
| Observability | HP ProDesk | arena/prodesk monitoring node for YouTube monitoring, watchdogs, recovery orchestration, SLI, Prometheus, Loki, Grafana, and notifications |
| Edge Source | Raspberry Pi | ADS-B source node feeding readsb/tar1090-style aircraft/map data into the rendering path |

## Why It Matters

The physical split makes the delivery/observability split real:

- the Dell workstation spends its resources on video, audio, GPU encode, and
  YouTube ingest;
- the HP ProDesk keeps monitoring state, dashboards, long-window SLI, and staged
  recovery logic away from the delivery workload;
- the Raspberry Pi keeps ADS-B collection at the edge so map/source failures can
  be reasoned about separately from stream delivery failures.

## k3s Boundary

k3s is used for the delivery workload on the Dell tier. The observability tier
does not directly own the FFmpeg process. It observes evidence and requests
staged recovery when the action gate allows it.

## Failure-Domain Boundary

The topology separates three failure domains:

- edge/source data failure;
- delivery/media runtime failure;
- observability/classification failure.

That separation is what lets the system avoid treating every monitoring warning
as a stream restart condition.
