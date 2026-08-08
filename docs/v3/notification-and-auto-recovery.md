# Notification And Auto-Recovery Events

`stream_v3` separates active incidents, observed restart actions, unconfirmed
recovery, and confirmed auto-recovery. That distinction keeps notification
names aligned with the evidence they actually carry.

## Problem

Some faults recover automatically:

- FFmpeg exits and the stream engine starts a new child process;
- fast recovery restarts delivery after `tcp_stall` or `network_down`;
- a k3s Pod/container restart count increases and then returns healthy;
- runtime `run_id` changes after a controlled or automatic restart.

These events are worth recording, but they are not always active incidents.
Paging on every recovered event creates noise; ignoring them entirely makes
post-incident review harder.

## Policy

```text
current failure -> active incident notification
restart observed, confirmation window open -> restart_observed
restart observed, confirmation window expired -> recovery_unconfirmed
restart observed, recovered TCP send sample present -> auto_recovered
historical degradation -> routine-check evidence, not active page
stale report/dashboard state -> observability follow-up, not delivery restart
```

For fast-recovery events, a completed restart is not enough to claim delivery
recovery. The default confirmation requires a post-restart TCP send sample at
or above 4.5 Mbps. Before the 180-second confirmation window expires, the event
is `restart_observed`; after that it becomes `recovery_unconfirmed`. A later
qualifying sample can still promote the same event to `auto_recovered`.

This is the `SV3-EVIDENCE-STRENGTH` contract. The implementation returns
explicit evidence fields rather than inferring recovery from diagnostic text:

```text
restart_observed=true
recovery_confirmed=false
recovery_lag_sec=unknown
```

Single FFmpeg child recovery with its own lifecycle evidence and no current
YouTube/public/same-URL impact remains an auto-recovered event rather than a
warning incident.

Fast-recovery stream restarts are also replayed by the current local-delivery
classifier. That replay is SLI evidence, not notification delivery evidence:
it explains current classifier coverage for retained restart events without
rewriting historical shadow logs.

## Event Classes

| Event | Notification class | Evidence |
| --- | --- | --- |
| current delivery fail | active incident | current fail signal, YouTube/public/ingest/capture/audio context |
| fast recovery restart, confirmation pending | `restart_observed` info | trigger, timestamp, restart result, available TCP context |
| fast recovery restart, timeout without qualifying sample | `recovery_unconfirmed` warning | restart result, first low-speed sample or missing-sample context |
| fast recovery restart with qualifying sample | `auto_recovered` info | pre/post TCP samples and measured recovery lag |
| FFmpeg child restart | auto-recovered info | scheduled restart and later `ffmpeg_started` in same run/restart count |
| k3s Pod/container restart count change | auto-recovered info | Pod UID, container, restart count delta, last state |
| runtime lifecycle change | auto-recovered info after baseline | run_id change and runtime evidence |
| report missing | observability warning/follow-up | timer/output path and stale threshold |

## Noise Controls

The notification layer uses:

- state keys to avoid duplicate notifications;
- phase-specific acknowledgements so later evidence can promote an event
  without repeating an unchanged phase;
- freshness windows for stream-engine, stream-watchdog, and runtime lifecycle
  events;
- outbox bounds and retry behavior;
- replay contracts to prevent recovered history from being reported as current
  failure;
- separate active-incident, restart-observed, recovery-unconfirmed, and
  auto-recovered event lists.

## Public Implementation Hooks

- `src/stream_core/notifications/status_loop.py` collects notification
  candidates.
- `src/stream_core/notifications/incidents.py` classifies active incidents and
  recovery observations.
- `src/stream_core/notifications/outbox.py` deduplicates and bounds delivery.
- `src/watchers/stream_watchdog.py` records k3s Pod/container restart deltas and
  syncs runtime event evidence.
- `src/stream_v2/sli.py` exposes `current_classifier_replay` for retained
  fast-recovery restart events.
- `tests/test_operational_replay_contracts.py` verifies replay behavior.
- `tests/test_critical_helper_contracts.py` covers notification outbox and
  recovery-evidence state behavior, including low-speed post-restart samples.

## Review Signal

The system treats notification delivery as a secondary SLI. It matters for
operations, but notification failure is not proof of stream failure, and
restart information is not automatically proof of delivery recovery.

The diagnostic boundary for those messages is documented in
[`notification-diagnostic-boundary.md`](notification-diagnostic-boundary.md).
That article explains which fault layers the notification can separate directly
and which root-cause claims still require raw evidence.
