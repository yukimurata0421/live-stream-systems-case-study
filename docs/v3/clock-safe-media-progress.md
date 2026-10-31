# Clock Labels, Missing Samples, And Bound Media Progress

Dated record, October 31, 2026. This is step 8 of the
[fallback revision history](fallback-policy-revisions.md). The single current
rule is the [fallback decision contract](fallback-decision-contract.md).

## What The Audit Found

The previous clock-hold grace still trusted a classification beyond its proof.
A returning CLOCK_FAULT label could restart grace even when no downstream media
facts existed, so alternating clock rejection and missing observations could
starve fallback forever. Conversely, a missing local ACK export could cause
backup to start despite fresh downstream progress during a wall-clock fault.
Both were reproduced in real-controller-main deterministic traces before the
change.

A second counter problem extended beyond clock faults. Missing samples erased
the comparison baseline. When normal fresh media facts alternated with HTTP
503, every good sample became a first sample, so no full-route confirmation
arrived and backup started at logical 90 s even though the same-route counters
kept increasing. The previous controller/source hash and that trace were kept.

The audit also checked that a fresh output error is evaluated before a bad UTC
label can hide it, and that a local boottime gap breaks continuity even when the
remote monotonic interval is short.

## The Correction

The existing producer already sends source boottime and bound copy-heartbeat
freshness. A new nonce, machine/boot binding, whole-RTT conservative source age,
producer freshness and typed same-identity counters can establish media
progress without trusting UTC labels. This includes input bytes, actual copy
output bytes and both video/audio packet counts. It is availability evidence,
not HEALTHY, viewer reception or permission for secondary recovery.

The deadline renews from new bound media progress, never from a clock label or
upstream ACK alone. Losing genuine clock-safe progress starts one grace;
unknown classifications cannot restart it. A short missing sample retains a
comparison baseline only within 45 s of local and source monotonic time.
Missing samples still reset healthy time, and resumed normal observations need
another consecutive sample before earning HEALTHY. Backup stop therefore keeps
its continuous 300 s requirement.

Wrong nonce, mismatched boot/destination, expired or replayed source age,
unconfirmed copy records, counter regressions and generation gaps do not prove
progress. The UTC-failed path does not renew a UTC cache, lease or authority.
Positive evidence can prevent a false start with missing local ACK observation;
complete proof loss still leads to a conservative availability start. With the
same observations, healthy delivery and a hidden failure cannot always be
distinguished. The policy does not promise zero false starts under total
observation loss.

The existing arena projection accepts the typed progress witness for clock-hold
notification even without local ACK export. Existing state/outbox/routing and
recovery pairing remain. No new provider or credential is introduced.

## Verification

Related checks passed 203 private tests / 107 subtests and 205 public tests /
111 subtests. The transition audit exhaustively checked 6,561 unknown-label
suffixes, 1,024 failure/unknown patterns, bounded comparison gaps and healthy
stop windows. Real HTTP tests checked nonce, identity, boottime age and producer
freshness, including invalid/replayed responses. The operated notifier baseline
passed 13 integration tests with fake senders; each projection checkout passed
five tests.

The same frozen controller/producer ran in twenty isolated native conditions.
It used real FFmpeg 5.1.9, the operated FIFO flags, real nonce HTTP, kernel
transport exports and independent local RTMP backup receivers. Faults targeted
owned processes only, under reduced UID/network/resource boundaries. Every
start case reached receiver byte progress. The final case used a separate
harness variant to select normal rather than clock-failed 503 alternation; its
controller/producer files were byte-identical to the other conditions.

| Owned condition | Start from initial injected fault |
| --- | --- |
| clock-alternating-missing | 95.041 s |
| clock-alternating-flow | No start |
| clock-no-ack | No start |
| clock-flow-then-missing | 203.996 s |
| clock-gap-long | 206.545 s |
| clock-gap-transient | No start |
| runtime | 47.088 s |
| held | 47.470 s |
| frozen | 47.140 s |
| blind | 93.946 s |
| copy-stop | 47.207 s |
| copy-hang | 79.294 s |
| copy-missing | 94.524 s |
| copy-stop-clock | 47.293 s |
| facts-only | 94.281 s |
| copy-hang-clock | 79.119 s |
| clock | No start |
| clock-frozen-stamp | No start |
| copy-clock-multi-reader | No start |
| facts-alternating-flow | No start |

The clock-then-loss cases first ran clock-safe media for 105 s before removing
proof or returning HTTP 503. Their initial-fault timings include that phase;
all native timings include polling and owned sender startup. Synthetic 320x180/
10 fps H.264/AAC and an owned five-second restart supervisor do not reproduce
the whole production engine or a WAN.

## Operated Check And Remaining Limits

The Dell controller and existing read-only arena projection/notification
adapter were updated after applicable gates. Real-UID cold reads and the actual
notification CLI loader passed. Running source/mount/environment and natural
consumer observations were checked. Media Pod/container, FFmpeg generation and
relay/copy/keeper identities were preserved; ACKs advanced, primary status
returned HEALTHY and production backup stayed at zero. Exact postimage-checked
rollback preimages were retained.

No production fault or external test notification was injected. Actual YouTube
backup selection, uninterrupted public-player AV, real alert delivery and the
long-term false-start rate remain unverified. The earlier player test still
contains a roughly 48-49 s local reception gap. Raw and failed evidence stays in
bounded private HDD storage.

Sources: [parser/controller](../../ops/selfheal/fallback_controller.py),
[deadline](../../ops/selfheal/route_assessment.py),
[transition audit](../../tests/test_route_transition_audit.py),
[real-main traces](../../tests/test_fallback_relay_stop_loop.py),
[read-only projection](../../monitoring-v4/src/stream_monitoring_v4/compatibility/route_causality.py).
