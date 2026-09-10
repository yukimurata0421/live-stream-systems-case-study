# Why CRA Exists

Recorded: 2026-09-10 JST

Status: public design rationale. This document summarizes the operational
evidence that shaped the repository contract; it is not a current deployment,
health, or action-authority record.

## The problem was competing recovery decisions

The delivery system originally had more than one path capable of deciding that
recovery was necessary. A local controller could see the FFmpeg child directly,
while a remote controller could see monitoring evidence and mutate a broader
runtime object. Each path had useful information, but they did not share one
durable command intent, one authority lease, or one effect fence.

That gap was observed, not merely anticipated:

- during one controlled cutover, an independent remote-recovery path changed
  the runtime while the cutover controller was evaluating its own candidate;
  the candidate disappeared and the cutover rolled back; and
- a later EffectLedger review found 14 typed recovery requests and 14 entries
  at the effect boundary. They formed seven duplicate restart scopes: in each
  pair, an earlier request for the same exact FFmpeg generation ended as
  `OUTCOME_UNKNOWN`, then a new request identifier reached `EFFECT_OBSERVED`
  about 32 to 33 seconds later.

The second observation proves duplicate restart attempts against the same
logical target scope. It does not prove that both attempts caused separate
FFmpeg child exits, nor does it by itself establish viewer impact or the exact
initiator. Those narrower causal claims were not available from the retained
evidence.

## Why request identifiers were insufficient

Request-level idempotency prevents one request from being applied twice. It
does not prevent two independent decisions from creating two request
identifiers for the same managed FFmpeg generation. A lost acknowledgement or
ambiguous result makes that distinction critical: treating
`OUTCOME_UNKNOWN` as "not executed" can turn uncertainty into a second physical
effect.

The safety identity therefore has to be wider than a request ID and narrower
than a host or Pod. The controlled object is the exact managed FFmpeg child,
with a logical generation retained across PID changes where required by the
recovery transaction.

## The CRA decision

CRA introduces one central intent truth while keeping observation, decision,
and execution in separate failure domains.

| Before | CRA contract |
| --- | --- |
| Independent recovery paths could decide separately. | cra-01 is the single central owner of policy, budget, cooldown, authorization, and final verdict. |
| A new request ID could bypass request-level deduplication. | `logical_generation_scope_id` and `effect_scope_id` fence the target at Dell and the runtime boundary. |
| An ambiguous response could be interpreted as permission to try again. | `OUTCOME_UNKNOWN` blocks automatic retry until append-only reconciliation resolves the scope. |
| Monitoring and recovery control could collide. | arena-server publishes signed facts only and cannot authorize or deliver an action. |
| A broad runtime mutation could substitute for a child-level effect. | The Effect Executor can target only the exact managed FFmpeg child. |
| Local fallback and central recovery could overlap after reconnection. | A bounded authority lease, shared scope, and exactly-once journal import gate handback to central authority. |

This is central authority, not central execution. cra-01 cannot signal a
process, arena-server cannot create a command, and the Dell Agent cannot
redefine policy. The physical boundary still rejects stale identity, mismatched
generation, expired authority, an occupied fence, and unresolved prior effect.

## Why the alternatives were rejected

- Keeping recovery authority in monitoring would let the evidence producer
  also decide the action and would preserve the collision domain.
- Keeping only local autonomous recovery would lose the shared incident,
  budget, cooldown, and cross-host evidence needed for a final verdict.
- Relying on request-ID deduplication would not close the observed new-ID,
  same-generation duplicate scope.
- Automatically retrying an unknown outcome would optimize for availability by
  accepting an unbounded duplicate-effect risk.
- Restarting a Pod, container, or host would widen the failure domain beyond
  the exact FFmpeg child the evidence identified.

## What the design claims

The repository implements the contracts needed to make duplicate decisions
fail closed: single central authorization, exact-target and logical-generation
fencing, durable local effect state, no automatic retry after ambiguity, and
append-only reconciliation. The harness exercises duplicate delivery, crash
windows, authority conflicts, stale targets, and delayed outcomes without a
production command path.

It does not claim that the public snapshot is deployed, that recovery is
enabled, that every historical restart has a proven cause, or that CRA alone
makes the service highly available. Those require separately bound live
release, runtime, external-health, and operational evidence.

Continue with [the three-host architecture](architecture.md),
[the safety model](safety-model.md), and
[the harness trust model](harness-trust.md).
