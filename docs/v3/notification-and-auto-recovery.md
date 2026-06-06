# Notification And Auto-Recovery Events

`stream_v3` separates active incidents from informational auto-recovery events.
That distinction keeps notifications useful without hiding recovery activity.

## Problem

Some faults recover automatically:

- FFmpeg exits and the stream engine starts a new child process;
- fast recovery restarts delivery after `tcp_stall` or `network_down`;
- a k8s container restart count increases and then returns healthy;
- runtime `run_id` changes after a controlled or automatic restart.

These events are worth recording, but they are not always active incidents.
Paging on every recovered event creates noise; ignoring them entirely makes
post-incident review harder.

## Policy

```text
current failure -> active incident notification
auto-recovered delivery event -> one informational notification
historical degradation -> routine-check evidence, not active page
stale report/dashboard state -> observability follow-up, not delivery restart
```

Single FFmpeg child recovery without current YouTube/public/same-URL impact is
reported as an auto-recovered event, not as a warning incident.

## Event Classes

| Event | Notification class | Evidence |
| --- | --- | --- |
| current delivery fail | active incident | current fail signal, YouTube/public/ingest/capture/audio context |
| fast recovery restart | auto-recovered info when recovered | trigger, timestamp, restart result, same URL state |
| FFmpeg child restart | auto-recovered info | scheduled restart and later `ffmpeg_started` in same run/restart count |
| k8s container restart count change | auto-recovered info | Pod UID, container, restart count delta, last state |
| runtime lifecycle change | auto-recovered info after baseline | run_id change and runtime evidence |
| report missing | observability warning/follow-up | timer/output path and stale threshold |

## Noise Controls

The notification layer uses:

- state keys to avoid duplicate notifications;
- freshness windows for stream-engine, stream-watchdog, and runtime lifecycle
  events;
- outbox bounds and retry behavior;
- replay contracts to prevent recovered history from being reported as current
  failure;
- separate active-incident and auto-recovered event lists.

## Public Implementation Hooks

- `src/stream_core/notifications/status_loop.py` collects notification
  candidates.
- `src/stream_core/notifications/incidents.py` classifies active incidents and
  recovery observations.
- `src/stream_core/notifications/outbox.py` deduplicates and bounds delivery.
- `src/watchers/stream_watchdog.py` records k8s container restart deltas and
  syncs runtime event evidence.
- `tests/test_operational_replay_contracts.py` verifies replay behavior.
- `tests/test_critical_helper_contracts.py` covers notification outbox and
  auto-recovered state behavior.

## Review Signal

The system treats notification delivery as a secondary SLI. It matters for
operations, but notification failure is not proof of stream failure, and
auto-recovery information is not automatically an active outage.
