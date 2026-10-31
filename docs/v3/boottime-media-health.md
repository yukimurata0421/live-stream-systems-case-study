# Boottime Media Health And Independent UTC Diagnostics

Dated record, October 31, 2026. This is step 9 of the
[revision history](fallback-policy-revisions.md). The
[decision contract](fallback-decision-contract.md) is the current rule.

## Why Simplify

The preceding revision already authenticated fresh media evidence with a
request nonce, pinned host, matching boot and a conservative boottime age bound.
It nevertheless denied HEALTHY when UTC labels failed. A healthy backup could
therefore upload indefinitely during a clock fault. The extra transition grace
also duplicated a deadline already renewed by proven media progress.

Media freshness now has one acceptance path. UTC intervals and timestamp labels
remain diagnostic; they do not reset healthy time or prevent backup retirement.
The downstream progress deadline has no transition grace or upstream ACK renewal.
A fresh downstream counter proof takes precedence over contradictory local
STOPPED evidence, which remains visible as a diagnostic conflict.

Arena separately keeps its clock-related secondary-recovery veto. The existing
critical notifier reports a sustained UTC diagnostic failure even if media is
HEALTHY, and clears it only when fresh UTC diagnostics pass. The incident ID and
existing durable notification pipeline remain in use; no provider or recovery
authority was added.

## Validation

The frozen consumer passed 22 isolated native conditions. Related private tests:
210 passed / 93 subtests; public port: 212 passed / 97 subtests. The 19 notification
tests are a subset of that suite and use the actual operated notification
implementation with fake senders. Projection tests: eight in each checkout.
Documentation structure checks also passed.

Transition checks still enumerate 6,561 unknown-label suffixes and 1,024 mixed
failure/unknown patterns. Boot/nonce/destination mismatches, stale boottime proof,
unconfirmed copy records and excessive comparison gaps are rejected. Intermittent
missing samples never accumulate retirement credit.

| Isolated condition | Observed result |
| --- | --- |
| UTC/503 alternation without copy evidence | Backup started 94.7 s after the injected condition; independent RTMP receiver advanced. |
| UTC/503 alternation with bound media progress | No backup start in the bounded test window. |
| UTC fault and unavailable local ACK evidence | No backup start in the bounded test window. |
| Bound progress followed by missing copy facts | Backup started 189.1 s after the injected condition; independent RTMP receiver advanced. |
| UTC fault followed by sustained facts loss | Backup started 190.0 s after the injected condition; independent RTMP receiver advanced. |
| UTC fault with a short observation gap | No backup start in the bounded test window. |
| Relay stop, operated FIFO/repeated generations | Backup started 47.5 s after the injected condition; independent RTMP receiver advanced. |
| Relay stop, held-generation control | Backup started 47.2 s after the injected condition; independent RTMP receiver advanced. |
| Relay stop, frozen local export | Backup started 46.9 s after the injected condition; independent RTMP receiver advanced. |
| Relay and facts unavailable | Backup started 94.6 s after the injected condition; independent RTMP receiver advanced. |
| YouTube copy stopped | Backup started 48.0 s after the injected condition; independent RTMP receiver advanced. |
| YouTube copy suspended | Backup started 78.6 s after the injected condition; independent RTMP receiver advanced. |
| Copy record unavailable, upstream ACKs advancing | Backup started 95.0 s after the injected condition; independent RTMP receiver advanced. |
| Copy stopped plus UTC fault | Backup started 47.5 s after the injected condition; independent RTMP receiver advanced. |
| Facts listener unavailable; media still flows | Backup started 94.3 s after the injected condition; independent RTMP receiver advanced. |
| Copy suspended plus UTC fault | Backup started 79.3 s after the injected condition; independent RTMP receiver advanced. |
| UTC fault with verified media | No backup start in the bounded test window. |
| UTC fault and fixed local transport timestamp | No backup start in the bounded test window. |
| Copy UTC labels fixed with multiple readers | No backup start in the bounded test window. |
| Normal facts/503 alternation | No backup start in the bounded test window. |
| Bound downstream progress contradicts local STOPPED | No backup start in the bounded test window. |
| Running backup, healthy media and sustained UTC rejection | Stopped after 303.0 s of continuous media health while UTC remained rejected; no controller start. |

The backup-retirement condition retained 699 independent
primary-receiver observations, all ready, with advancing bytes. The longest
unchanged counter interval was 0.599 s.
This is reception evidence at the local RTMP receiver, not decoded public-player
frame or audio continuity. The stop occurred with 303.0 s of
healthy credit and 286.8 s of UTC rejection; health had
already begun before the clock fault was injected.

All conditions used the same controller/producer bytes, real FFmpeg and kernel
transport observations, bounded unprivileged private-network processes, and at
most three simultaneous media cases. Runtime and blind-stop cases retained the
operated FIFO settings; held/frozen controls used the previously defined longer
retry wait. No production retry setting was adjusted to make detection pass. All owned children exited. Two earlier
setup launches were aborted: a capability-free guard could not write a
user-owned directory, and failed user-side archival was followed by an unintended
second launch. Those two setup processes were stopped before fault injection; the evidence
was retained. A later readiness timeout arrived just before the HEALTHY
response became visible; two sibling cases were stopped with it. Their partial
evidence is retained separately. Only the helper readiness wait changed from
50 to 80 s; the fault windows and controller/producer bytes did not change.
The queue kept its original end time. Completed successful conditions were
retained, and only incomplete conditions were repeated. Root reconciled ownership, and setup and launch became separate
checked operations. These launches are not successful fault conditions.

## Operated Checkpoint

Applied 2026-10-31T20:58:32.310129+09:00: controller `b38eb215dec46486`, read-only arena projection
`545156b88e99d6d4`, existing-notifier adapter `dbe6f9cd39b4e025`.
OCI media/facts source, main Pod/container identities, FFmpeg generation and
connection, relay/copy/keeper process identities and FIFO settings were retained.
The controller returned HEALTHY with backup zero and advancing ACKs. Actual-user
cold reads, the actual notification CLI/loader and a natural notification cycle
passed. The first post-restart read preceded status recreation; the application
was not repeated, and read-only reconciliation after the initial baseline
confirmed the new process and health. Existing disabled restart flags, admission
hooks, routing, state and outbox remain. Root-pinned rollback preimages are retained.

## Limits

Boottime freshness proves bounded observation age, and advancing counters prove
media progress at their capture point. Neither establishes YouTube's ingest
selection, public-player continuity or viewer audio/video. Production fault
injection, real notification delivery and long-term false-start rate remain
unverified. Facts loss can still start an unnecessary backup; continuous media
health is required to stop it.
