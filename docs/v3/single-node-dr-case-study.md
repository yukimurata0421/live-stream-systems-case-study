# Single-Node DR Case Study

`stream_v3` runs the production delivery workload on a single k3s node. That is
an explicit trade-off: the deployment is small and understandable, but node,
disk, or host failure can affect the entire delivery plane. This document
summarizes the public DR model and the first measured drill.

## Risk Model

The delivery node owns:

- the `stream-v3-runtime` deployment;
- browser rendering, PulseAudio, AutoDJ, FFmpeg, and NVENC;
- node-local k3s state and local-path storage;
- the RTMPS connection to YouTube.

The observability host owns monitoring, Prometheus/Loki/Grafana, YouTube
watchdog state, notification state, and staged recovery decisions. That split
helps recovery reasoning, but it does not remove the single-node delivery risk.

## DR Objective

The highest-priority recovery objective is not "make Kubernetes green." The
objective is:

```text
restore or preserve YouTube live delivery
preserve the same public watch URL when recoverable
avoid destructive YouTube lifecycle actions from ambiguous evidence
retain enough evidence to classify the failure after recovery
```

## Measured Drill

The first public DR drill was a safe single-node control-plane drill:

```text
fault injection: systemctl restart k3s
scope: k3s service / API / kubelet / control-plane availability
not included: OS reboot, power loss, disk loss, PVC restore, spare-host rebuild
```

The measured result was:

| Checkpoint | Result |
| --- | --- |
| API ready | recovered inside the drill window |
| node Ready | recovered inside the drill window |
| deployment available | recovered inside the drill window |
| Pod 3/3 | recovered inside the drill window |
| arena metrics OK | measured RTO upper bound: 10.7 seconds from fault injection |
| same URL | preserved |
| YouTube ingest/public/watchdog samples | stayed OK in the measured window |

This was not a full viewer-facing RTMPS recovery drill. The stream-engine
container and FFmpeg process survived the k3s restart window. The correct
interpretation is:

```text
k3s control-plane / observability recovery was measured at 10.7s.
The drill did not prove node reboot, disk restore, spare-host rebuild, or
FFmpeg reconnect RTO.
```

That distinction is important. Inflating this drill into a full disaster
recovery claim would be misleading.

## Branch Model

The DR runbook separates recovery by failure branch.

| Branch | Condition | Primary action | Public claim status |
| --- | --- | --- | --- |
| Node alive | k3s node reachable; runtime unhealthy | inspect Pod, logs, metrics; rollout restart if justified | documented |
| Image/config broken | deployment can run after rebuild or config fix | rebuild local image, validate manifests, reapply overlay | documented |
| Node dead, disk survives | old disk can be mounted read-only | recover repo, music, state PVC data, and secrets into a new host | planned |
| Node and disk lost | local-path state lost | restore repo/music/secrets; use observability host for historical evidence | planned |
| v2 rollback | v3 recovery exceeds RTO and same-URL risk is accepted | explicitly record reason and authority before rollback | conceptual fallback |

The public repository does not ship private secrets, music files, runtime state,
or local-path PV contents, so the public DR claim is about method and evidence
discipline, not a portable one-command restore.

## RTO / RPO Boundaries

| Scenario | Current status |
| --- | --- |
| k3s service restart | measured: 10.7s to arena metrics OK |
| Pod/runtime restart | covered by runtime and fast-recovery tests; live RTO depends on trigger |
| OS reboot | not publicly measured |
| disk survives | procedure documented, not publicly measured |
| spare host rebuild | target documented, not publicly measured |
| disk loss | RPO depends on repo, music backup, secrets, and observability logs |

The RPO boundary is intentionally explicit:

- Git contains code, manifests, tests, and public docs.
- Runtime state and private logs are outside Git.
- Secrets are reconstructed from local secret stores, not committed.
- Observability state can support incident reconstruction when delivery-local
  state is lost.

## Host Watchdog Layer

The system also added a host-freeze guardrail. In-Pod recovery cannot fix a
frozen OS, PID1 stall, or kernel lockup, so the delivery host enables systemd
watchdog behavior and kernel lockup panic settings where available.

The public decision is scoped:

- software watchdog fallback is useful for PID1/userspace stalls;
- hardware watchdog support depends on BIOS and platform settings;
- panic or destructive watchdog drills should be done only in a maintenance
  window;
- watchdog recovery is separate from k3s service restart recovery.

## Review Signal

The value of this case study is not that a single-node system is ideal. It is
that the failure branches, measured and unmeasured claims, RTO/RPO boundaries,
and same-URL safety constraints are named directly.

For review, the important questions are:

- Does the system distinguish control-plane recovery from viewer-facing
  recovery?
- Does it preserve same-URL identity before rebuilding infrastructure?
- Does it avoid deleting local-path storage while node recovery is still
  possible?
- Does it say which DR claims are measured and which remain future drills?

That is the reliability behavior this public snapshot is meant to show.
