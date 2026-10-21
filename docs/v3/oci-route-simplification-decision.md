# Replacing An Unfinished OCI Recovery Design

Status: accepted
Decision date: 2026-10-20 JST
Scope: the Dell to OCI to YouTube/Twitch media route and its automatic recovery.
Superseded design: the signed-lease relay and the route-control design in
[OCI relay and Twitch](oci-relay-and-twitch.md).

## Context

The goal was to add Twitch while preserving the existing YouTube broadcast and
using one normal upload from home. The [current route record](self-healing-oci-fanout.md)
explains the uplink constraint and the choice of an OCI VM after the Tokyo OKE
capacity attempt.

On 2026-10-13, one Dell feed reached the relay and was forwarded to YouTube and
Twitch during the retained operational windows. The relay only forwarded while
the Dell host renewed an HMAC-signed lease (10-second expiry, renewed every 2 seconds), a
guard owned MediaMTX, and a separate daily owner renewed Twitch. Route changes
were meant to be decided by a central authority, admitted by typed commands,
and fenced by expiry.

From 2026-10-14 the work expanded into a substantially different migration and
recovery system: backup-first migration, signed checkpoints, typed transport
between three hosts, and native qualification before production steps. The
complete migration and automatic recovery path remained unfinished. This was
more than a completed system waiting for its final production-enable switch;
the October 13 relay's operation did not establish the new system's completion.

By 2026-10-19 20:34 JST there had been 104 qualification runs, with 31,200 seconds
of recorded budget: 58 failed and 46 passed. Those pass counts did not establish
the end-to-end goal. Many recorded failures involved fixtures, isolation mounts,
dependencies, and preflight checks. Repeated patches and additional gates
increased the implementation and verification work without completing delivery
and recovery. This consumed operator time and implementation effort while the
original goal remained unmet.

At 06:31 JST on 2026-10-20, a short network interruption ended the one-shot
YouTube backup sender that had been started through this process. It did not
reconnect. The main sender reconnected after five seconds, so the broadcast
continued, but the component meant to provide safety had less recovery
behavior than the component it protected.

## Decision

The operator's goal is unattended, stable delivery that recovers by itself.
The route was changed in two steps on the same morning:

1. About 08:11 JST: restore the earlier relay route (plan B), leaving the
   unfinished migration system behind.
2. 08:43 JST: replace it with the [self-healing fan-out](self-healing-oci-fanout.md).
   Relay, keeper, and fallback controller each use ordinary restart loops
   (`Restart=always`, Kubernetes restarts, reconnecting senders) plus one
   controller with hysteresis: start after 30 seconds of failure, stop after
   300 seconds of continuous health.

The signed lease, guard, SSH tunnel, typed route transport, and the previous
daily owner were stopped. Their files and history were kept.

## What was given up

| Property of the earlier design | Status now | Mitigation or accepted risk |
| --- | --- | --- |
| Forwarding stops when a signed lease expires | Removed | The relay admits publishers only from the encoder's tunnel address. A stale sender is not fenced by expiry. |
| Route actions authorized centrally with a budget ledger | Removed for this route | The only automatic action is scaling one fallback Deployment between 0 and 1, with fixed thresholds. |
| At most one upload at any time | Relaxed | The fallback window includes 300 seconds of continuous relay health before stopping the direct upload. Both ingests belong to the same YouTube broadcast. |
| Native qualification before each change | Not used for this route | Unit tests cover the decision logic. A drill exercised automatic fallback start and stop around a manual relay stop and restart. |

These trade-offs apply to the media route only. Recovery authority for other
scopes, and the retained evidence for the earlier design, are unchanged.

## Consequences

- One upload in normal operation, as the uplink requires.
- The design is judged by whether delivery, fallback, and return work together.
  When repeated fixes add states or gates without completing that path, review
  the structure before adding another patch.
- Recovery no longer depends on the monitoring host or on a human approval.
- The control code is small enough to review in one sitting: the
  [keeper](../../ops/selfheal/forward_keeper.py) and the
  [fallback controller](../../ops/selfheal/fallback_controller.py).
- Monitoring had to follow the new route. The relay observer, the upload
  measurement, and the YouTube watchdog's port list all needed changes. At the
  time of the drill, the watchdog did not count the fallback connection, so its
  automatic replacement-broadcast creation was disabled. Later changes are
  tracked in [current status](../current-status.md).

## What this does not show

Drills on 2026-10-20 and 2026-10-21 exercised a stopped relay service, tunnel
loss, and a relay-host reset. A prolonged relay-host stop and provider-side
failures were not exercised. The first keeper-driven Twitch
renewal had not yet run when this decision was recorded. The decision replaced
an unfinished system whose complexity and repeated verification work had
prevented it from meeting the operational goal. The replacement still needs
observation beyond the one relay-stop drill.
