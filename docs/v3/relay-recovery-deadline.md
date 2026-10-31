# Relay Stop Across FFmpeg Generations

Dated record, October 31, 2026. This page is step 5 of the
[revision history](fallback-policy-revisions.md). Its rule let upstream ACKs
renew the uncertainty deadline, which hid YouTube copy failures; the
[next step](youtube-egress-failover.md) corrected that. The current rule is the
[fallback decision contract](fallback-decision-contract.md). The policy table
and the seven test results below describe this revision only.

On October 31, 2026, a clock-fault fix was found to suppress genuine relay
recovery. Holding every unavailable observation meant short FIFO retries and
FFmpeg generation changes could prevent the 30-second failure window forever.
The [initial study](relay-stop-main-loop.md) preserves the failed 75-second
test. Extending media retries just to help the detector was not adopted.

The correction separates confirmed failure from a bounded availability decision
under uncertainty. It changes the controller and read-only facts producer;
operated FFmpeg retry settings remain 0.2 seconds and 20 attempts.

| Evidence | Start policy |
| --- | --- |
| Fresh, nonce/host/boot/clock-bound response says the relay unit is stopped | Confirmed MEDIA_FAILURE for 30 s |
| Existing fresh stalled-media or FIFO failure evidence | Existing 30 s window |
| No verified relay health and no observed ACK progress for 90 s | Start under uncertainty; keep OBSERVATION_LOST/CLOCK_FAULT assessment |
| Clock or facts loss while local ACKs continue advancing | Hold replicas and retain diagnostic status/logs |
| Fresh verified relay input/output/video/audio progress | Hold backup off, or stop it after 300 continuous healthy seconds |
| Planned maintenance inhibition | No start; reset the uncertainty deadline |

The producer reports a stopped relay only after two reads find its fixed unit
cgroup absent or empty. Permission errors, multiple processes, mismatched
identity and a racing start remain unavailable observations. Its negative
response still carries the ordinary nonce and clock envelope; consumer binding,
freshness and typed-field checks precede MEDIA_FAILURE. Copy-process facts are
not required when the relay itself is confirmed stopped.

The controller's `ProgressDeadline` uses local boottime. Fresh same-generation
ACK growth or verified relay health resets it; missing/frozen exports and FFmpeg
generation changes do not. Counters from different generations are never
subtracted. ACK growth remains usable even when a faulty wall clock repeats
the timestamp. Maintenance resets the deadline, and controller restart begins
a new one. Ninety seconds is a policy grace period of six normal polls, not a
measured clock tolerance. Actual startup also includes polling and sender delay.

Complete observation loss can cause a backup start while media is actually
flowing: that is the accepted availability tradeoff. Clock error alone with
advancing ACKs does not start backup. If valid relay health never returns, a
backup can remain active longer than 300 seconds; that number is a continuous
healthy stop condition, not a maximum duration.

## Automated native checks

The candidate controller's real main, HTTP nonce exchange, real FFmpeg 5.1.9,
kernel TCP counters and the runtime transport producer ran in isolated network
namespaces. Faults targeted owned processes only. Backup used an independent
local RTMP receiver. Synthetic 320x180/10fps H.264/AAC and an owned five-second
restart supervisor do not reproduce the entire production engine or WAN.

| Owned fault / condition | Result |
| --- | --- |
| Relay stop, operated FIFO settings | One backup start after 47.358 s despite four sender restarts; receiver bytes advanced |
| Relay stop, longer recovery control | One start after 47.804 s; receiver bytes advanced |
| Relay stop, transport export frozen | One start after 46.934 s; receiver bytes advanced |
| Relay and facts listener stopped, operated FIFO settings | One start after 94.036 s despite eight restarts; receiver bytes advanced |
| Clock envelope fault only, ACKs advancing | No start during 135 s |
| Facts listener only stopped, ACKs advancing | No start during 135 s |
| Clock fault plus frozen local wall timestamp, ACKs advancing | No start during 135 s |

A deterministic main-loop regression also first reproduced a false start when
the timestamp stayed fixed but ACK counters grew. The witness was corrected to
recognize that intrinsic progress; stale bodies without growth remain unknown.
The related public suite passed 155 tests and 126 subtests. Source tests,
native receiver byte progress and production fault outcomes are separate gates.

Earlier setups failed before fault injection because checkout input mapping
expected a helper missing from the operated observer. A compatibility mapper
now preserves the pinned older observer's normal fields and keeps the newer
helper when present. An incorrect runtime-kit mount in one launcher was also
preserved as failed setup evidence. All owned children exited; no production
fault or provider lifecycle mutation was used.

## Operated correction and limits

Review found a remaining downstream gap in this revision: local ACK progress
proves Dell-to-relay transport, and can keep renewing the deadline while the
YouTube copy unit is stopped or its heartbeat is unavailable. The initial
seven conditions did not test that fault. The [downstream correction](youtube-egress-failover.md) separately covers these
faults; these earlier results must not be read as proof of that coverage.

The controller consumer was updated before the optional stopped-relay producer.
Both passed cold reads under their actual service UIDs; the facts check used
the real CLI on an owned localhost listener. The existing observer hash pin,
clock envelope behavior and firewall rule were retained. Loaded process/source
identity and the actual consumer's fresh healthy observations were checked.
Only controller and facts services were restarted. Media Pod/container identity,
relay/copy/keeper process identity and restart counts were preserved; ACKs
advanced and the production backup remained at zero. Exact preimages and
postimage-checked rollback scripts were retained.

The facts producer retains its operated clock module. The timing functions it
uses match the common source contract; no host clock or NTP service was changed.
The separate causal guard, maintenance lock and fallback-only credentials remain.
Raw observations and failed attempts stay in bounded private HDD evidence.

This establishes the isolated relay-stop paths and operated read behavior.
Production failure injection, actual YouTube backup selection and uninterrupted
public-player AV remain unverified. The earlier [player study](route-failover-validation.md)
still records a roughly 48-49-second local reception gap; this detector change
does not remove cold-backup startup delay.

Sources: [controller](../../ops/selfheal/fallback_controller.py),
[witness/deadline](../../ops/selfheal/route_assessment.py),
[facts producer](../../ops/selfheal/route_facts_http.py),
[full-loop regressions](../../tests/test_fallback_relay_stop_loop.py) and
[negative-facts boundaries](../../tests/test_relay_availability.py).
