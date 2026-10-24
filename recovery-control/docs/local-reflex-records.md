# Local Reflex Records

Dated evidence, candidates and set-aside plans for the
[local reflex path](local-reflex-authority.md). The current contract is in that
page and the procedure in [operations](local-reflex-operations.md). A past pass,
approval, time, count or unresolved state here is not a statement about now.
The development records these summarize are in Japanese and keep host
identifiers, hashes and raw evidence privately; this page keeps the findings and
their limits.

| Date (JST) | Record | Standing |
| --- | --- | --- |
| 2026-10-23 | [Daily WAN renumbering stalls](#daily-wan-renumbering-stalls) | Basis of the engine grace window |
| 2026-10-22 | [Stall criterion replay](#stall-criterion-replay) | Basis of the acknowledgement criterion |
| 2026-10-22 | [Minimal path dry-run deployment](#minimal-path-dry-run-deployment) | Agent and central updated in `dry_run` |
| 2026-10-22 | [Minimal path isolated verification](#minimal-path-isolated-verification) | Not deployed for `enforce` |
| 2026-10-22 | [Retirement-evidence correction](#retirement-evidence-correction) | Applied to the existing controller |
| 2026-10-22 | [Runtime image qualification](#runtime-image-qualification) | Candidate only |
| 2026-10-22 | [Runtime binding candidate](#runtime-binding-candidate) | Frozen; signed acknowledgement kept |
| 2026-10-22 | [Runtime image rollout plan](#runtime-image-rollout-plan) | On hold; not a procedure |

## Daily WAN Renumbering Stalls

Of the 42 stalls confirmed by the acknowledgement criterion between 2026-09-12
and 10-22, 40 fell between 06:28 and 06:36 JST, on 37 days. The home WAN
address is renumbered around then and existing connections never come back.
The other two were on 10-21 around 09:40-09:50 JST.

On 10-23 the agent saw acknowledgements stop at 06:31:51 with about 1.3 MB
unsent, confirmed the stall at 06:32:00 while ingest was unreachable, and logged
`WOULD_RESTART` at 06:32:02 once ingest answered. FFmpeg exited on its own at
06:32:05 after a broken pipe, the stream engine started the next generation
five seconds later, and delivery was healthy again at 06:32:24, with no request
from the agent or the existing controller. The agent recorded
`GENERATION_CHANGED_WITHOUT_ACTION`.

From stall start to a new FFmpeg generation took a median of 45 s and a 95th
percentile of 55 s; three cases took longer (139 s, 196 s, and one with missing
observations). No confirmed stall resumed on the same generation. Acting at
confirmation, about 10 s in, would race the engine's own recovery every morning
and could kill a process that was about to exit (SIGKILL follows SIGTERM after
two seconds).

The relay output clock lost up to 41 s of picture between good readings, and at
least about 25 s given the new generation's start. That is an estimate up to
the relay output, not viewer playback; the readback shares the same home line.

Changes that followed: a policy `stall_grace_sec` (default 90 s) during which a
confirmed stall is only recorded as `WITHIN_ENGINE_GRACE`, and no intervention
while FFmpeg is not running. Replaying the same period with a 90 s grace gave no
intervention candidate; the 139 s and 196 s cases had acknowledgement progress
in between and were not single stalls. A relay output interruption recorder in
the stream repository keeps each daily gap. Shortening the interruption itself
(FFmpeg I/O timeouts, the engine's restart wait, output reconnection) is a
delivery runtime question.

## Stall Criterion Replay

Recorded TCP samples from 2026-09-12 to 10-22 (736,549 observations, 85 FFmpeg
generation changes) were replayed through the agent's own tracker with
`tools/replay_tcp_samples.py`. The collector samples every five seconds, so the
replay confirms after two intervals, about 10 s, matching five two-second
samples in production. Network and Executor conditions are not replayed.

| Criterion | Confirmed | Followed by replacement | Recovered without restart | Detected before replacement |
| --- | ---: | ---: | ---: | ---: |
| Acknowledgement progress (adopted) | 42 | 42 | 0 | 39 / 85 |
| `bytes_sent` unchanged (earlier) | 31 | 26 | 5 | 30 / 85 |

In the 10-22 06:31 stall, `bytes_sent` grew by about 30 bytes per sample while
acknowledgements, `notsent` and `unacked` were frozen, so only the new criterion
detected it. The existing controller uses the same `bytes_sent` basis. The 46
undetected changes had 2-3.6 MB of acknowledged progress per sample until the
change. Confirmation to replacement took a median of 40 s (minimum 14 s). Most
stalls were replaced by another path first, so whether they would have
recovered by themselves is unknown; the false-positive rate at two-second
sampling is left to production `dry_run`.

## Minimal Path Dry-Run Deployment

Agent and central were updated to a fixed release of the isolation-verified
source and restarted, cra-01 first. The signed v1 `dry_run` policy, the existing
controller, delivery, the runtime image and authority, the host contract and the
database schema were unchanged; the agent kept a deliberately invalid Executor
socket.

In a six-minute window both services were active with no automatic restart, the
agent ingested fresh older-format observations as `LEGACY_UNBOUND` and
`HEALTHY` with zero actions, and audit sequences advanced with matching
contiguous and signed-acknowledged values. Central verification found no
violations, gaps or conflicts. The delivery Pod, containers and FFmpeg
generation were unchanged and bytes kept being sent. No fault was injected;
this does not show a restart effect, `enforce`, or provider reception.

## Minimal Path Isolated Verification

Agent and central dropped v4 requests and the successor event chain as
requirements: requests are always v2, the generation fence, expiry, send-time
recheck and persistent limits remain, an older status reply is reconciled by
bracketing the same request with fresh observations, and execution result and
later delivery are kept apart. Signed acknowledgement, sync, signing, central
storage and the database schema did not change.

136 isolated tests against the older runtime source passed, including a real
older Executor socket, asynchronous replies, lost replies, stop file, expiry,
missing observations, target changes and budget persistence; six candidate
compatibility tests passed. In a network-less, unprivileged container from the
same older production image, a real FFmpeg sent through loopback TLS to a real
receiver:

| Real FFmpeg test | Result |
| --- | --- |
| Real TCP backpressure, stall decision, v2 request, reply discarded, status query, real SIGTERM, new generation progress | `EFFECT_OBSERVED`, `DELIVERY_RESUMED`, one physical attempt |
| FFmpeg exit confirmation delayed until the Executor reported unknown, then sending recovered | `OUTCOME_UNKNOWN`, `DELIVERY_RESUMED_EXECUTION_UNKNOWN`, one physical attempt |
| Agent rebuilt from the same database after both | One consumption kept, no resend, existing controller reads it |

Two early runs stopped on the isolation fixture with zero requests. A third
found that an empty generation between FFmpeg exit and start was settled too
early; the attempt was kept, and a wait and regression test were added. This is
not a resubmission of audit to production cra-01 or a provider-side check, and
the single-route assumption still applies.

## Retirement-Evidence Correction

Isolated runtime qualification found that the existing controller reported a
delivery restart still awaiting verification as unknown when retiring it. A
one-line correction now reports only `EFFECT_OBSERVED_AWAITING_VERIFICATION` as
`EFFECT_OBSERVED`; `OUTCOME_UNKNOWN` and `EXECUTION_STARTED` stay unknown.
Signals, resend, budget and fences are untouched.

The corrected file was added as a read-only bind through a single systemd
drop-in, picked up on the next natural ten-second cycle without restarting the
timer or service, and confirmed in the running processes. In a three-minute
window, 20 natural cycles succeeded, the delivery Pod, FFmpeg generation and
Executor ledger counts were unchanged, and sending and acknowledgement advanced.
The branch the correction serves has not occurred in production; its behavior
rests on the isolated test.

## Runtime Image Qualification

A candidate delivery image added one layer of five changed files on top of the
existing image. It was started in isolation through the real entrypoint, stream
engine and FFmpeg to a local TLS receiver, and checked against the existing
controller and v2, new v4 requests, retention on failure, and rollback to the
existing image. It revealed the retirement-evidence mismatch above, which made
the combination with the existing controller unfit for production until
corrected. The image was not pushed or deployed.

## Runtime Binding Candidate

The candidate bound a request to one verified socket (`runtime.delivery_transport.v1`
and `runtime.typed_effect_request.v4`), produced successor evidence linking the
request to the FFmpeg exit and start, and signed audit acknowledgements. Socket
binding and successor evidence are frozen and no longer required. Signed
acknowledgement was adopted and runs; it shares the policy key, which is a known
trust-domain limit. Applying the binding requires replacing the delivery Pod,
which may drop FFmpeg and its RTMPS connection, so it was not applied while
keeping the current broadcast.

## Runtime Image Rollout Plan

A plan to replace the delivery Pod once during a scheduled interruption, keeping
`dry_run`, the existing controller and the retirement correction. Neither
replacement nor rollback can be done without an interruption in the current
layout. The plan is frozen, is not part of the current procedure, and any use
would start from a fresh check of the then-current state.
