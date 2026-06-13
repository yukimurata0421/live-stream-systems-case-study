# Memory Guard Case Study

This case study explains why `stream_v3` treats memory as diagnostic evidence
and a guardrail, not as a standalone reason to perform destructive recovery.

## Problem

During early v3 production work, the `stream-engine` container was restarted by
the kernel OOM path. The largest signal was not FFmpeg. It was Xvfb shared
memory growth inside the browser capture stack.

The incident mattered because the normal container memory panel did not explain
the failure quickly enough. Prometheus current-memory samples showed ordinary
values before and after the event, while the kernel recorded a short-lived
container peak near the limit.

## Why Container Current Memory Was Not Enough

Container-level current memory is useful, but it can miss short peaks between
scrapes.

In this failure mode:

- Xvfb shared memory grew quickly;
- the container cgroup hit the memory limit;
- `memory.oom.group` killed the stream-engine container group;
- FFmpeg and the browser stack were recreated after the container restart;
- the next Prometheus sample no longer showed the peak that caused the OOM.

The conclusion was not "Prometheus is bad." The conclusion was that the
measurement layer was too coarse for this specific failure mode.

## Added Guard

The mitigation added a process-level capture-helper guard:

```text
watch Xvfb VmRSS and RssShmem
record capture_helper_memory_guard_triggered
stop FFmpeg normally
recreate Chromium/Xvfb before the next FFmpeg start
avoid full container OOM when possible
```

This keeps the recovery local to the delivery plane. It avoids turning a browser
capture memory problem into a broad YouTube lifecycle incident.

## Memory Policy

The memory policy is intentionally conservative:

```text
memory alone never authorizes YouTube broadcast replacement
memory alone never authorizes same-URL reset
memory alone should not trigger stream-wide restart without correlated runtime impact
memory can support subsystem recovery when it correlates with rendering, audio,
delivery, OOM, or process-level evidence
```

This avoids a common operational mistake: restarting a live system because a
dashboard looks visually large, without checking limit ratio, cgroup events,
process breakdown, swap, PSI, and current delivery health.

## Evidence Layers

| Layer | Evidence | Use |
| --- | --- | --- |
| Process | Xvfb RSS and shared memory | catch capture-helper runaway before container OOM |
| Cgroup | `memory.current`, `memory.peak`, `memory.events` | prove current usage, peaks, and OOM events |
| Host | MemAvailable, swap, PSI | distinguish host pressure from delivery Pod pressure |
| Runtime | FFmpeg alive, RTMPS connected, capture/audio freshness | decide whether memory is correlated with product impact |
| Prometheus | `stream_v3_runtime_memory_*`, `stream_v3_monitor_host_*`, optional `stream_v3_cgroup_*` | runtime alert evidence plus host/cgroup diagnostics |

The observability model separates host memory from delivery Pod memory. The HP
ProDesk observability host can be healthy while the Dell delivery Pod is under
pressure, and the reverse can also be true.
Host memory is not the primary v3 runtime alert. The exporter names host
capacity as `stream_v3_monitor_host_*`, while `stream_v3_runtime_memory_*`
tracks the delivery Pod.

## Public Implementation Hooks

- `src/stream_core/stream_engine.py` records capture-helper memory guard events
  and coordinates ordered capture-stack restart.
- `src/stream_core/engine/config.py` exposes guard thresholds.
- `src/stream_core/cli_support/memory_status.py` splits file cache from
  non-reclaimable memory and historical one-shot peaks.
- `src/stream_core/cli_support/resource_memory.py` correlates host, cgroup,
  process, and runtime state.
- `ops/scripts/stream_v3_prometheus_exporter.py` exports v3 runtime memory
  metrics.
- `tests/test_stream_engine_wait_modes.py`, `tests/test_memory_status.py`,
  `tests/test_resource_memory.py`, and `tests/test_stream_v3_prometheus_exporter.py`
  cover the guard and evidence shape.

## Review Signal

The useful operational signal is the layered diagnosis:

- the system did not trust one memory number;
- it identified the concrete process class responsible for the peak;
- it changed the recovery behavior to restart the capture stack in order;
- it kept memory as supporting evidence unless runtime impact or OOM evidence
  was present;
- it made the public metrics distinguish host, Pod, cgroup, and process-level
  memory stories.

That is a stronger operational claim than "we added a memory alert."
