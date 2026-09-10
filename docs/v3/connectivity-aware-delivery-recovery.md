# Connectivity-Aware Delivery Recovery

Status: public source contract

Recorded: 2026-08-16 JST

This document explains how the delivery runtime avoids amplifying a physical
connectivity failure. It describes repository behavior and safety boundaries;
it is not evidence that a particular release is currently deployed.

## Problem

Restarting FFmpeg every few seconds cannot restore RTMPS while DNS or the
upstream TCP path is unavailable. Applying a long exponential backoff to every
failure is also undesirable because it delays recovery after an ordinary
FFmpeg-only exit.

The runtime therefore distinguishes process failure from connectivity
failure. A network outage is recorded as an episode and is not treated as a
reason to recreate the Pod or repeatedly launch new FFmpeg children.

## Responsibility boundary

| Plane | Responsibility |
| --- | --- |
| Dell delivery runtime | Mechanical liveness of the FFmpeg, Chromium, Xvfb, and overlay processes that it owns. |
| Dell local protection loop | Bounded DNS/TCP/process observations and suppression of destructive action while connectivity is unavailable. |
| arena-server observation plane | Incident correlation, notifications, SLO interpretation, and YouTube/external-evidence assessment. |
| Raspberry Pi public publisher | Sanitized static publication only; no delivery control or incident authority. |

The delivery host does not use public-viewer evidence, SLO burn, or YouTube
API state as local restart authority. The observation plane does not turn a
dashboard failure into delivery mutation.

## FFmpeg behavior

| Condition | Behavior |
| --- | --- |
| FFmpeg exits while DNS and RTMPS TCP are reachable | Retain the normal short child-retry path. |
| FFmpeg exits while DNS or RTMPS TCP is unreachable | Enter `waiting_connectivity` and do not launch another child. |
| Connectivity becomes reachable | Leave the wait state on the next bounded probe and launch FFmpeg without an accumulated backoff delay. |
| Local protection observes `network_down` | Update episode evidence; do not restart the Deployment, Pod, or FFmpeg. |
| A separately confirmed TCP stall is actionable | Request termination of the FFmpeg child only; do not escalate to Pod or host restart. |

The probe derives only the host and port from the RTMPS URL. It does not write
the stream key to events or state. DNS resolution and bounded TCP connection
checks are recorded separately.

## Browser and map behavior

Map-render heartbeat is evaluated independently from the streaming socket.
After a startup grace period, repeated heartbeat failures may restart Chromium
alone. The recovery is deferred while connectivity is unavailable and is
subject to a cooldown.

A browser-only recovery does not restart FFmpeg, NVENC, RTMPS, AutoDJ, the
precipitation fetcher, or the Pod. Xvfb failure remains a separate
capture-stack condition because it changes the display identity.

## Resolver durability

The example K3s host configuration points kubelet and `dnsPolicy=Default`
workloads at `/run/systemd/resolve/resolv.conf` rather than the local
systemd-resolved stub. Installing that example is an explicit host operation;
public CI does not apply it or restart K3s.

## Incident correlation

A fresh local connectivity episode becomes the root delivery-connectivity
incident. Component symptoms that can share that physical cause may be held as
deferred active state so recovery is not announced before component-specific
evidence becomes healthy again.

Formal multi-window SLO burn and independent external black-box evidence stay
separate. They are not suppressed merely because a local connectivity episode
exists.

## Invariants

- Do not restart from the presence or absence of one TCP state alone.
- Do not recreate a Pod during a physical network outage.
- Do not restart FFmpeg because only a browser heartbeat failed.
- Keep Discord and Slack credentials out of the delivery runtime.
- Keep one notification writer on the arena-server observation plane.
- Do not represent deferred child incidents as recovered until fresh
  component evidence supports that transition.

## Review path

- `src/stream_core/engine/connectivity.py`
- `src/stream_core/engine/runtime_recovery.py`
- `src/stream_core/stream_engine.py`
- `src/watchers/fast_recovery.py`
- `src/watchers/fast_recovery_core/connectivity_policy.py`
- `src/stream_core/notifications/connectivity_correlation.py`
- `src/stream_core/notifications/status_loop.py`
- `deploy/host/k3s/50-stream-v3-resolver.yaml`
- `tests/test_connectivity_recovery.py`

## Rollback boundary

The FFmpeg connectivity gate and browser-only recovery have independent
configuration switches. Runtime image rollback, observation-plane rollback,
and any K3s resolver change are separate operations and require their own live
identity, impact, and rollback checks.
