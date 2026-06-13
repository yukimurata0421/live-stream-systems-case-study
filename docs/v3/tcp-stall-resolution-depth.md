# TCP Stall Resolution Depth

This page complements `tcp-stall-case-study.md`. The case study explains the
root-cause split that was already public. This document records the later
resolution model that raised diagnostic granularity for recurring RTMPS stalls.

The goal is to make the evidence ladder reviewable without publishing raw
private logs, exact public IP values, CPE configuration, packet captures, or
host-specific state paths.

## Why Another Layer Was Needed

The early model had two useful observers:

- persistent non-YouTube TCP/TLS anchors sampled every 15 seconds;
- WAN address and fresh TCP-anchor samples on a coarser scheduled cadence.

That was enough to prove that persistent Cloudflare AS13335 and Google AS15169
anchors failed together, and that immediate reconnect could also fail. It was
not always enough to catch the shortest route/address transition. One recurrence
was visible in persistent anchors while the scheduled WAN sample landed just
before or after the transition.

The fix was not "sample everything faster forever." The fix was targeted
granularity:

- a short morning burst around the recurring validation window;
- failure-triggered WAN snapshots after all-anchor failure or failed
  reconnect-after-failure;
- higher-resolution RTMPS socket and route-event evidence for the next
  recurrence.

## Resolution Ladder

| Layer | Evidence | Public-retained state |
| --- | --- | --- |
| Delivery RTMPS sample | `bytes_sent_delta`, send Mbps, `lastsnd_ms`, `notsent`, `unacked`, FFmpeg PID, trigger reason | Retained through fast-recovery metrics and case-study summaries. |
| WAN identity and fresh anchors | default route, public identity check, IPv6 prefix/route state, fresh TCP connects to Cloudflare AS13335 and Google AS15169 | Retained through `ops/scripts/wan_address_observer.py` and systemd scheduling examples. |
| Persistent non-YouTube anchors | long-lived Cloudflare/Google TCP/TLS flows, existing-flow failure, immediate reconnect result | Retained through `ops/scripts/persistent_tcp_anchor_observer.py`. |
| Failure-triggered WAN snapshot | 5-second follow-up samples after all-anchor failure or failed reconnect-after-failure | Retained through public observer behavior and tests. |
| High-cadence RTMPS socket burst | `ss -tinp` style state, peer, RTO, RTT, `lastsnd`, `notsent`, `unacked`, retransmission counters | Retained through `ops/scripts/rtmps_tcp_burst_observer.py` and an opt-in systemd timer. |
| Netlink route/address event stream | route delete/add, default gateway changes, address deprecation/restoration | Retained through `ops/scripts/netlink_wan_event_observer.py`. |
| CPE event ingest | CPE syslog/API events such as scheduled reconnect, modem/session detach, PDN reattach, DHCPv6-PD changes | Retained through `ops/scripts/cpe_event_ingest.py`; real CPE export configuration remains private. |
| Bounded packet metadata | snaplen-limited packet metadata around RTMPS recurrence windows | Retained through `ops/scripts/rtmps_tcpdump_ring.py`; dry-run is the public-safe default and raw packet captures are not public artifacts. |

The bounded packet metadata layer is intentionally metadata-only; it is not a
payload publication path.

The public-retained code is therefore enough to review the main hypothesis
split and the higher-resolution attribution model. The raw outputs remain
private because they can contain public IPs, CPE details, socket peers, or
packet metadata.

## Reading Rules

### DNS Is Supporting Evidence

DNS success is supporting evidence, not proof that the WAN path is healthy.
DNS failure is also not enough to blame YouTube ingest. Local resolver and CPE
behavior can survive, mask, or lag a short WAN/session event.

The primary split uses TCP anchors, route/address state, and RTMPS socket
behavior.

### A New Connection Alone Is Weak

`new_connection=true` by itself is not a failure. It can be normal lifecycle
behavior, endpoint keepalive churn, or reconnect after idle close.

The stronger evidence pattern is:

```text
persistent anchor failure
immediate reconnect failure
fresh TCP anchor failure
RTMPS socket backlog or reset
route/address transition in the same window
```

### Cloudflare Plus Google Matters

When Cloudflare AS13335 and Google AS15169 fail together across IPv4 and IPv6,
the event is unlikely to be a YouTube ingest edge-only problem or a
Google/AS15169-only carrier path issue.

That does not prove whether the initiating owner is CPE or carrier. It does
move the fault layer away from YouTube lifecycle mutation and toward
WAN/session/route behavior.

### Upload Pressure Is Separate

Upload p95, raw over-budget seconds, and low-upload pressure are guardrail or
delivery symptoms. They do not prove the cause of a TCP stall by themselves.

The encoding contract should change only when upload evidence, YouTube input
quality, and transport evidence agree. Otherwise the system risks tuning the
encoder for a WAN/session event.

## Owner Split After Higher Resolution

The higher-resolution model narrowed the classification to a short
WAN/session/route outage. It did not make the final owner claim public.

Remaining owner split:

```text
CPE scheduled reconnect, reboot, session refresh, or NAT/session flush
carrier-side mobile session refresh, PDN reattach, or route refresh
```

Host-side observers can prove that existing flows failed, fresh connects
failed, route state changed, and RTMPS stalled or reset. They cannot prove CPE
intent without CPE logs or configuration. The public repository keeps the CPE
classification helper, but real CPE logs and raw packet captures stay outside
Git.

## Operational Decision

The action boundary remains unchanged:

- preserve the same YouTube watch URL;
- recover the local delivery path when fresh delivery evidence fails;
- keep WAN observers report-only;
- do not create a replacement broadcast for WAN/session evidence;
- do not restart solely from upload budget pressure;
- use higher-resolution evidence to improve classification, not to broaden
  recovery authority.

## Public Boundary

The public repository intentionally keeps:

- the hypothesis split;
- the existing public observer hooks;
- the high-resolution observer scripts;
- the tests for public observer behavior;
- the sanitized case-study summaries.

It intentionally excludes:

- raw JSONL event payloads;
- exact public IP values;
- private host paths;
- CPE admin details;
- packet captures;
- generated packet metadata;
- credentials or internal endpoints.

This is the right public shape for a hiring repository: reviewers can inspect
the reasoning and the retained code boundary without receiving private
operational data.
