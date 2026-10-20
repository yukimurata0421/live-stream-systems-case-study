# Route Network Observation And Baselines

> This page records the 2026-10-13 diagnostic design and its SSH-tunnel socket
> assumptions. For the route introduced on 2026-10-20, read the
> [current OCI delivery record](self-healing-oci-fanout.md). The earlier local
> socket assumptions and throughput baseline are not a calibrated baseline for
> the replacement route.

## Why the OCI route needs a new baseline

The primary route changed the meaning of the sender socket: Dell ACK progress
now ends at a local SSH tunnel, while OCI ingress and provider forwarding are
independent observations. Historical direct-RTMPS Mbps and stall timings cannot
be transferred to that socket as a calibrated restart threshold.

The observer therefore records four planes: `local_ack`, `oci_input`,
`youtube`, and `twitch`. It produces supporting diagnostic facts only:
`authority=FACTS_ONLY_SHADOW`, `action_enabled=false`, and
`restart_threshold_mbps=null`. The earlier TCP derivative analyzer remains a
separate analysis surface. There is no newly calibrated hypothesis test or
production restart decision in this port.

## Multiple probes and diagnosis

The collector combines ICMP to Cloudflare, Google, and Quad9, the default
gateway when available, persistent IPv4/IPv6 TCP anchors, and existing local
DNS/TCP facts. OCI input/output counters provide another independent plane.

| Correlated evidence | Supporting assessment |
| --- | --- |
| All three public pings fail, both IPv4 anchors fail, local TCP/network facts corroborate loss, and OCI input does not progress for three samples | `LOCAL_WAN_LOSS_SUSPECTED`; surviving TCP reachability gives `IPV4_PATH_LOSS_SUSPECTED`. |
| Ping fails while TCP or OCI input progresses | `ICMP_UNREACHABLE_WITH_TCP_PROGRESS`; ICMP failure alone is insufficient. |
| DNS fails while an independent address remains reachable | `DNS_FAILURE_SUSPECTED`. |
| OCI input stalls for three samples | `OCI_INPUT_NOT_PROGRESSING`; this is not a carrier/CPE ownership verdict. |
| OCI input progresses but an output stalls | `PROVIDER_OUTPUT_NOT_PROGRESSING`. |
| A required observation is missing or stale | Observation unavailable or incomplete; missing data does not become healthy data. |

The normal cadence is 15 seconds. Three consecutive observations are required
for the sustained loss/stall assessments above. This is supporting local
classification; Monitoring and CRA retain incident and recovery authority.

## Baseline contract

The measurement regime binds route and route generation, encoder-profile
digest, relay-source digest, WAN identity digest, and collector version.
Missing observations cannot create or switch a bound regime. Counter resets,
connection changes, and boot changes break rate continuity.

Each plane has causal one-day and seven-day windows. The current observation
is scored against earlier samples before being appended. Healthy training
requires fresh bound runtime and relay evidence, progress on the relevant
planes, no pending effect, reachable network, working DNS, fresh inactive
maintenance, no send-queue pressure, and no provider error. Confirmed planned
Twitch breaks are censored rather than learned as ordinary zero throughput.

Readiness requires a healthy span of at least the window minus 45 seconds,
at least 90% eligible normal samples, and a maximum observed gap of 45 seconds.
Median, MAD, and lower percentiles remain descriptive statistics; neither a
low percentile nor `ready=true` grants action authority. Observation retention
is 30 days with a 2 GiB output stop bound.

The new deployment observed on 2026-10-13 had a building baseline. This public
port contains no production database or later readiness claim.

## Run a credential-free replay

From the repository root:

```bash
python3 ops/scripts/route_network_observer.py \
  --replay examples/route-observation/replay.json
```

The six frames are labeled `SYNTHETIC_NOT_LIVE`. They progress from warmup to
transport flow and then corroborated connectivity loss. All frames report
`action_enabled=false`; their short duration cannot satisfy baseline readiness.
Replay uses fixture timestamps and temporary local state. It calls no network
probe, SSH command, process signal, or provider API.

`--live` requires both `--config` and `--state`. The
[example](../../examples/route-observation/config.example.json) names generic
read-only source paths and an explicitly configured relay SSH alias. It is not
a production-ready config. The reader also requires a machine-bound host
contract, fixed local MediaMTX GET endpoints, and a bound supervisor source.
Those bindings must be reviewed for the destination installation. No service
installer or private SSH/credential configuration is included.

See [observer code](../../ops/scripts/route_network_observer.py),
[relay reader](../../ops/scripts/oci_relay_readonly_snapshot.py), and
[tests](../../tests/test_route_network_observer.py).
