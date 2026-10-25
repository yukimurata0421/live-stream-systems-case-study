# Why CRA Exists

Independent recovery decisions once reached the same managed FFmpeg generation.
A retained ledger review found seven duplicate restart scopes: an earlier
request ended as `OUTCOME_UNKNOWN`, then a different request reached
`EFFECT_OBSERVED` about 32 to 33 seconds later. This proves duplicate attempts;
it does not prove two separate process exits or their viewer impact. The
[original rationale and its limits](history.md) remain in Git.

Request-ID idempotency alone cannot stop two decisions from creating different
IDs for the same generation. CRA therefore persists a reservation before sending,
keeps finite consumption across restarts, and relies on the Stream Executor's
exact-target generation fence. Unknown execution blocks retry until reconciled.

The original design approved every action centrally and added a local decider
for LAN loss, with leases, epochs and journal handback between them. The current
CRA has one local decision path under centrally signed policy in every network
state. Central keeps policy and audit verification; the existing Stream Executor
keeps the physical boundary. This removes the second decider and handback
machinery without removing the fence or the no-retry rule.

The agent first lets Stream recover itself. Daily output interruptions normally
need a connection reopened, which FIFO can do without replacing the encoder.
CRA's default 90-second grace and lifecycle guard reserve intervention for a
running generation that remains stalled beyond that window. The operated mode
is still `dry_run`; an effective live CRA intervention is not demonstrated.

Continue with [architecture](architecture.md), the
[current contract](local-reflex-authority.md), [safety model](safety-model.md),
and [test boundaries](harness-trust.md).
