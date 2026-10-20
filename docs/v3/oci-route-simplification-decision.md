# Retiring The Signed-Lease OCI Route

Status: accepted
Decision date: 2026-10-20 JST
Scope: the Dell to OCI to YouTube/Twitch media route and its automatic recovery.
Superseded design: the signed-lease relay and the route-control design in
[OCI relay and Twitch](oci-relay-and-twitch.md).

## Context

The OCI relay exists because the home uplink cannot comfortably carry two
separate uploads. From 2026-10-13, one Dell feed reached the relay and was
forwarded to YouTube and Twitch. The relay only forwarded while the Dell host
renewed an HMAC-signed lease (10-second expiry, renewed every 2 seconds), a
guard owned MediaMTX, and a separate daily owner renewed Twitch. Route changes
were meant to be decided by a central authority, admitted by typed commands,
and fenced by expiry.

From 2026-10-14 the work shifted to automating the route introduction itself:
a backup-first migration, signed checkpoints, a typed transport between three
hosts, and native qualification runs that had to pass before any production
step. By 2026-10-19 20:34 JST there had been 104 qualification runs
(31,200 seconds of budget), 58 failed and 46 passed, and automatic production
enablement had not completed. Many of the recorded failures were in test
fixtures, isolation mounts, dependencies, and preflight checks rather than in
forwarding media, and each fix added another gate to qualify.

At 06:31 JST on 2026-10-20, a short network interruption ended the one-shot
YouTube backup sender that had been started through this process. It did not
reconnect. The main sender reconnected after five seconds, so the broadcast
continued, but the component meant to provide safety had less recovery
behavior than the component it protected.

## Decision

The operator's goal is unattended, stable delivery that recovers by itself.
The route was changed in two steps on the same morning:

1. About 08:11 JST: restore the 2026-10-13 relay route that had already run in
   production (plan B), without the new qualification gates.
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
| At most one upload at any time | Relaxed | Two uploads are allowed only during a fallback window. The backup ingest belongs to the same YouTube broadcast, so YouTube treats it as redundancy rather than a competing publisher. |
| Native qualification before each change | Not used for this route | Unit tests cover the decision logic. A manual drill confirmed automatic fallback and return. |

These trade-offs apply to the media route only. Recovery authority for other
scopes, and the retained evidence for the earlier design, are unchanged.

## Consequences

- One upload in normal operation, as the uplink requires.
- Recovery no longer depends on the monitoring host or on a human approval.
- The control code is small enough to review in one sitting: the
  [keeper](../../ops/selfheal/forward_keeper.py) and the
  [fallback controller](../../ops/selfheal/fallback_controller.py).
- Monitoring had to follow the new route. The relay observer, the upload
  measurement, and the YouTube watchdog's port list all needed changes, and the
  watchdog's automatic replacement broadcast had to be disabled because it does
  not yet count the fallback connection.

## What this does not show

One drill exercised a stopped relay service. Tunnel loss, a relay host outage,
and provider-side failures were not exercised. The first keeper-driven Twitch
renewal had not yet run when this decision was recorded. The earlier route's
safety properties were not disproved; the decision is that, for this system,
their cost in delivery risk and operator time exceeded their benefit.
