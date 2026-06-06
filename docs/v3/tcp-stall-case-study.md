# TCP Stall Root-Cause Case Study

This case study explains how `stream_v3` handled a recurring RTMPS TCP stall
without misclassifying it as a YouTube incident or authorizing unsafe recovery.
It is intentionally written as a public reliability review artifact: raw
runtime state, exact public IP addresses, and private host logs are excluded,
but the diagnostic model and retained implementation hooks are public.

## Problem

The live stream showed a recurring short transport failure around the same
morning window in JST. The visible symptom was a stalled RTMPS send path:
`lastsnd_ms` increased, queued bytes appeared in `notsent`, upload throughput
fell, and fast recovery restarted the delivery workload as `network_down` or
`tcp_stall`.

The operational risk was not only the stall itself. A wrong diagnosis could
trigger the wrong remediation:

- replacing or rebinding a YouTube broadcast when the live URL was still
  recoverable;
- tuning encoder bitrate for a network identity refresh rather than an input
  quality problem;
- blaming YouTube ingest when the host could not create new non-YouTube TCP
  flows either;
- paging on dashboard symptoms that were stale or too coarse for root cause.

The goal was to preserve same-URL continuity, recover quickly, and classify the
fault layer before changing recovery policy.

## Evidence Model

The diagnosis used four evidence groups.

| Evidence group | Signal | Why it mattered |
| --- | --- | --- |
| Delivery TCP state | `bytes_sent_delta`, `send_mbps`, `notsent`, `unacked`, `lastsnd_ms`, FFmpeg PID, fast-recovery trigger | Proves whether the RTMPS sender was actually moving bytes or only appeared connected. |
| WAN identity | default routes, global IPv4/IPv6 address state, public IPv4 identity, IPv6 delegated prefix | Distinguishes an application stall from a host or carrier session refresh. |
| New TCP anchors | fresh TCP connects to Cloudflare AS13335 and Google AS15169 IP literals | Tests whether new non-YouTube flows can be created during the window. |
| Persistent TCP/TLS anchors | long-lived Cloudflare and Google flows, plus immediate reconnect after failure | Distinguishes existing-flow blackholes from endpoint keepalive noise or simple NAT flow expiry. |

DNS was retained as supporting context, not as the primary signal. The local
resolver path went through the local system resolver and CPE, so DNS success did
not prove the WAN session was healthy, and DNS failure did not by itself prove a
YouTube ingest problem.

## Cause Split

| Hypothesis | Supporting evidence would look like | Observed evidence | Decision |
| --- | --- | --- | --- |
| YouTube ingest edge problem | RTMPS stalls while Cloudflare and Google anchors stay healthy. | Cloudflare and Google anchors failed during the same window. | Excluded. |
| Google or AS15169 path issue | Google and YouTube fail while Cloudflare AS13335 remains healthy. | Cloudflare and Google failed together. | Excluded. |
| DNS-only failure | Name resolution fails, but IP-literal TCP anchors stay healthy. | IP-literal anchors failed, including fresh connects. | Excluded as primary cause. |
| Server-side keepalive close | A persistent anchor closes, then immediate reconnect succeeds. | Existing flows failed and immediate reconnect failed. | Excluded for the recurring event. |
| NAT mapping or existing-flow flush | Existing flows fail, but new TCP anchors succeed immediately. | Existing flows and fresh TCP anchors both failed. | Not the primary pattern. |
| WAN or carrier session refresh | Existing flows fail, fresh TCP anchors fail, routes or delegated prefixes churn, and public identity changes. | This pattern repeated in the same morning window across three consecutive days. | Accepted as the most likely layer. |

The accepted cause statement is deliberately scoped: the recurring fault was a
short WAN identity/session refresh around the observed JST window. The retained
evidence could not prove whether the initiator was the CPE or the carrier, so
the next action was CPE setting and log inspection rather than a YouTube or
encoder change.

## Three-Day Confirmation

The same branch repeated across three consecutive JST days.

| Day | Failure signature | Identity evidence | Recovery evidence |
| --- | --- | --- | --- |
| Day 1 | Cloudflare and Google persistent anchors failed; reconnect after failure failed; new TCP anchors failed. | Public IPv4 identity changed; IPv6 delegated prefix changed. | Fast recovery restarted the stream as `network_down`; the next periodic RTMPS sample showed normal send throughput. |
| Day 2 | Same all-anchor outage and reconnect failure pattern. | Public IPv4 identity changed; IPv6 delegated prefix changed. | Fast recovery restarted the stream; RTMPS send recovered by the next periodic sample. |
| Day 3 | Same all-anchor outage and reconnect failure pattern. | Public IPv4 identity changed; IPv6 delegated prefix changed. | Fast recovery restarted the stream; RTMPS send recovered by the next periodic sample. |

The periodic RTMPS sample cadence was coarser than the fast-recovery decision
cadence, so the public conclusion uses an upper bound: the next periodic sample
proved recovery, while actual recovery likely happened earlier.

## Operational Decision

No YouTube broadcast replacement was justified by this evidence. The same-URL
goal remained the primary user-facing SLO, and the fault layer was outside the
YouTube lifecycle control plane.

The accepted response was:

- keep delivery-plane fast recovery active for `network_down` and confirmed
  `tcp_stall` events;
- keep WAN cause observers report-only until repeated correlation exists;
- allow temporary low-upload recovery profiles only after strong transport
  recovery evidence, not from dashboard warnings alone;
- inspect CPE scheduled reconnect, reboot, keepalive, and WAN-session settings;
- treat a carrier-side daily refresh as an infrastructure constraint if no CPE
  trigger is found.

This avoided two common failure modes: overfitting encoder settings to a WAN
identity event, and letting a monitoring symptom mutate YouTube state.

## Public Implementation Hooks

The public repository keeps the implementation surface that made this diagnosis
reviewable:

- `src/watchers/fast_recovery_core/decision.py` classifies delivery-side
  `network_down`, `tcp_stall`, and low-upload pressure from RTMPS TCP samples.
- `src/watchers/fast_recovery.py` records restart events and RTMPS sample
  context.
- `src/watchers/network_observer.py` keeps route, DNS, TCP-connect, and current
  FFmpeg socket evidence separate from recovery authority.
- `ops/scripts/wan_address_observer.py` records host WAN identity, default
  routes, global addresses, public IPv4 identity, and fresh TCP anchors.
- `ops/scripts/persistent_tcp_anchor_observer.py` keeps non-YouTube TCP/TLS
  anchors open and records whether immediate reconnect succeeds after an
  existing-flow failure.
- `ops/systemd/stream-v3-wan-address-observer.timer` and
  `ops/systemd/stream-v3-persistent-anchor-observer.service` show the retained
  host-side scheduling model for those report-only observers.

The observer scripts are diagnostic tools. They improve root-cause confidence,
but they do not directly authorize destructive recovery.

## Review Signal

The important part is not that the network failed. The important part is the
control discipline:

- the system separated product impact, delivery transport, YouTube lifecycle,
  dashboard evidence, and WAN cause evidence;
- each hypothesis had a falsifiable evidence pattern;
- weak or noisy probes stayed report-only until correlation repeated;
- the remediation matched the fault layer;
- same-URL preservation was protected from ambiguous control-plane actions.

That is the reliability behavior this public snapshot is meant to demonstrate.
