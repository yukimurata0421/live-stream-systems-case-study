# Local Reflex Authority

The 2026-10-22 source port adds `src/cra/`, a smaller recovery path for one
action: restart the managed FFmpeg child after a confirmed local TCP stall, at
most once per FFmpeg generation. It is public code and test surface, not a
production authorization or qualification result.

This page is the current contract for that path. How to inspect, evaluate,
switch and stop it is in [local reflex operations](local-reflex-operations.md);
dated implementation, verification and rollout evidence is in
[local reflex records](local-reflex-records.md).

## Current stage

The path runs in production only as `dry_run`, beside the existing fast-recovery
controller, which keeps Executor authority. A minimal `enforce` path that needs
no change to the delivery Pod is implemented and verified in isolation. Handing
authority over and switching to `enforce` are separate, explicitly approved
steps that have not been taken.

| Item | Decision | Relation to what runs |
| --- | --- | --- |
| Asynchronous outcome query | Required. The same request is queried read-only and never resent. | Verified against the existing runtime in isolation; agent and central run in `dry_run`. |
| Compatibility with the existing controller | Required while migrating and rolling back. | Its retirement-evidence correction is applied and kept. |
| Signed audit acknowledgement | Kept as implemented; further extension frozen. | Running. It grants or removes no execution right. |
| Causal successor evidence | Not a precondition for the minimal path; further adoption frozen. | Removed from agent and central requirements. The prototype and its records are kept. |
| Socket-level binding and v4 requests | Adoption deferred. | Candidate image and source kept; not in the production runtime. |
| Production deployment of the candidate runtime image | Frozen. | The minimal path is proven on the existing runtime first. |

A `dry_run` deployment is not permission or evidence for `enforce`. Freezing an
item does not delete its implementation or failure history.

## Why the shape changed

The three-host design asked cra-01 to authorize every restart while Dell also
had to recover alone during LAN loss. That produced two deciders and a mode
switch (central active, local fallback, reconciling), which in turn required
leases, authority epochs, signed journal handback, three databases, and a large
soak apparatus to prove the switch safe.

The reflex path removes the mode switch. Dell always decides locally under the
last adopted policy. cra-01 owns that policy and verifies the outcome after the
fact. LAN loss changes only what reaches cra-01, not what Dell does.

## Ownership

| Component | Owns | Must not own |
| --- | --- | --- |
| cra-01 `cra-central` | Signed policy versions, audit intake, invariant verification. | Per-action approval or any path to the Effect Executor. |
| Dell `cra-agent` | Stall confirmation, local guards, durable per-generation claim, request, outcome check. | Policy definition; adopting an unsigned or older policy. |
| Effect Executor | The one physical effect, with its own effect-scope fence. | Deciding whether a stall is confirmed. |

The Effect Executor in `runtime_boundary` is reused unchanged. Requests use
`runtime.typed_effect_request.v2`; the request and idempotency key derive from
the FFmpeg generation, so a restarted agent cannot mint a second request.

## Invariants

- A generation is claimed in local SQLite before any request; the claim is
  unique, and an unknown outcome is never retried.
- One stalled interval means no newly acknowledged bytes while data is still
  queued (`notsent` or `unacked` at or above the policy threshold). A stall
  counts only across distinct, fresh observation sequences of the same target,
  with no `network_down` and a reachable ingest.
- Uptime, Executor in-flight count, Executor authority, cooldown, hourly and
  daily limits, policy mode, and a local stop file all block a request.
- Policy is adopted only with a valid cra-01 signature and a higher version.
  Without one, the built-in policy is `dry_run`.
- The audit log is append-only. cra-01 stores it in order without duplicates,
  keeps conflicting records separately, and acknowledges each batch with a
  signature the agent checks before advancing its watermark.

`cra-central verify` checks the stored audit against these rules and against the
published policy history. In `dry_run`, the same check summarizes how often a
confirmed stall recovered by itself, which is the evidence for any later
`enforce` decision.

## Conditions for a restart

The minimal path allows a restart only within these conditions. Stronger causal
proof or audit transport is not silently added as a further execution
condition.

| Condition | Required behavior |
| --- | --- |
| One execution authority | The Executor's current authority is checked, so the agent and the existing controller can never both execute during migration. |
| Durable claim and generation fence | The claim is written before the request, and the agent and Executor fences block another attempt on the same generation. If the claim cannot be written, nothing is sent. |
| Recheck before sending | Signed policy mode and version, the stop file, observation freshness, target generation, Executor, route and limits are checked again. A request expires three seconds after creation. |
| Stall and route validity | See the stall criterion below. Network loss or an unreachable ingest never leads to a restart. |
| Engine self-recovery first | See waiting for the engine below. |
| Finite limits | Cooldown and hourly and daily limits are persistent. Rejected, unknown and cancelled-after-claim attempts keep their consumption; restarts and deployments do not refund it. |
| Asynchronous outcome | `EXECUTION_STARTED` is not completion. The same request is queried; an unknown outcome is kept and never resent. |
| Fence cleanup | The agent closes only fences for its own requests that reached `DELIVERY_RESUMED`; unknown ones wait for an operator. |
| Outcome observation | Execution results and later delivery progress are recorded separately. External recovery is not credited to the agent. |

### Single-route assumption

Without a Pod change the path is limited to one delivery output and destination,
where the actual FFmpeg arguments and connection match the observed target.
Reachability uses the `ingest_endpoints` entry for the observed
`delivery_route`; an unknown route or invalid observation stops the request.
`enforce` needs the Executor instance, route and route generation; an older
observation without a route is limited to `dry_run`. Requests are always
`runtime.typed_effect_request.v2`, and a binding that appears later does not
switch them to v4.

The existing observation's TCP fields cannot prove socket uniqueness. The path
accepts the single-route operating assumption and must not be applied silently
to multiple outputs or dynamically changing routes. When an older status reply
lacks the Executor instance, the target is confirmed by fresh Pod, Executor and
target observations before and after querying the same request; any change or
regression defers reconciliation.

## Stall criterion

The first version counted an interval as stalled when `bytes_sent` did not move.
In the 2026-10-22 06:31 JST stall, retransmissions and probes kept `bytes_sent`
growing by about 30 bytes per sample while `bytes_acked`, `notsent` and
`unacked` were frozen, so that criterion missed it. Acknowledgement progress is
now the signal; `bytes_sent` with `lastsnd` remains only for observations that
lack `bytes_acked`.

`tools/replay_tcp_samples.py` replays recorded collector samples through the
agent's own tracker. Over 2026-09-12 to 10-22 (736,549 samples, 85 FFmpeg
replacements):

| Criterion | Confirmed stalls | Followed by replacement | Recovered without restart |
| --- | --- | --- | --- |
| Acknowledgement progress | 42 | 42 | 0 |
| `bytes_sent` unchanged | 31 | 26 | 5 |

The 46 replacements without a prior detection showed normal acknowledgement
progress until the replacement. Most confirmed stalls ended in a replacement by
another path, so whether they would have recovered on their own is unknown.
The collector samples every five seconds; the replay confirms after two
intervals to match the agent's five two-second samples.

## Waiting for the engine

In the same record, 40 of the 42 confirmed stalls fell between 06:28 and 06:36
JST: the home WAN address is renumbered around that time each day, and the old
connection never comes back. FFmpeg then exits on its own and the stream engine
starts the next generation 5 s later, 14-55 s after the stall began. Acting at
confirmation (about 10 s) would race that self-recovery every morning.

A confirmed stall therefore stays `WITHIN_ENGINE_GRACE` until it has lasted
`stall_grace_sec` (default 90 s; optional in v1 policies) on the same generation
and connection. Network probing waits for the same point, and a FFmpeg that is
already exiting or waiting to restart is left to the engine. Replaying the record
with a 90 s grace yields no intervention candidates, so the agent is now insurance
for a stall the engine does not resolve. Shortening the daily interruption itself
is a delivery runtime question, not a reflex one.

## Executor fence and authority

Requests stay on `runtime.typed_effect_request.v2`, so `enforce` needs no Pod
change. The production Executor keeps a delivery restart in
`EFFECT_OBSERVED_AWAITING_VERIFICATION` and admits no further request until it
is reconciled. After the agent verifies `DELIVERY_RESUMED` for its own request,
it closes that fence with the Executor's existing v1 reconciliation evidence. A
fence with an unknown outcome stays open for an operator.

`tools/executor_authority.py` hands authority over and back through the ledger's
own compare-and-set, which refuses an unexpected current producer or any
unresolved fence. It does not run the ledger constructor, so it causes no
migration or backfill writes. Handing authority to the agent while it is in
`dry_run` would leave nothing able to restart FFmpeg.

## Judging the outcome

The path distinguishes a confirmed execution, a later new generation that
resumed sending, and resumed sending whose execution stayed unknown. It does
not require exit and start events tied to the request, or proof that the agent
alone caused recovery. A resumed outcome needs a new generation started after
the request and two fresh observations of that generation and route with
`bytes_sent` increasing. A missing observation resets the comparison, the gap
between exit and start is waited out inside the window, and evaluation uses the
policy in force when the request was made.

| Outcome | Meaning |
| --- | --- |
| `DELIVERY_RESUMED` | The Executor confirmed execution and a new generation then made sending progress. Not proof that the restart was the only cause. |
| `DELIVERY_RESUMED_EXECUTION_UNKNOWN` | A new generation made progress within the window, but execution remained unknown at the end of it. It is not rewritten as success and not resent. |
| `GENERATION_CHANGED_UNATTRIBUTED` | A different Pod, Executor or route, or a generation that existed before the request. Not counted as an effect of the request. |

TCP sending is not provider reception. cra-01 verifies the execution result and
the two observations without a successor event chain. Older `RECOVERED` records
keep their own label and are not added to these outcomes.

## Audit and signed acknowledgement

The Dell audit is append-only with a sequence number. cra-01 acknowledges up to
the last contiguously stored sequence and records gaps and content conflicts
separately. A stopped sync grants or removes no execution right; undelivered
records stay on Dell.

Each acknowledgement is signed, so a forged one cannot advance the agent's
watermark and leave records undelivered. That records remain on Dell and that
automatic resending continues are separate guarantees. The acknowledgement
reuses the policy signing key; extending it or moving it to a separate key is
frozen and belongs to a separate audit threat-model decision.

## Change discipline

- `src/cra/**/*.py`, including blank lines and comments, is reviewed against a
  guide of 3,000 lines. Tests, tools and records are counted separately. The
  guide is not a hard limit or a quality claim.
- This page is the contract and [operations](local-reflex-operations.md) the one
  procedure. Hashes, identifiers, counts and failures belong in the
  [records](local-reflex-records.md), not in every reading.
- A proposal that changes the delivery Pod is first compared with a no-Pod-change
  alternative, including interruption, rollback, and added state or dependencies.
- An addition names the concrete failure it handles and first checks whether an
  existing mechanism or a removable dependency solves it. Safety mechanisms and
  failure history are not deleted to meet the size guide.

## Boundaries

Public tests run the agent against fakes and against the real Effect Executor
with a temporary ledger. Source presence does not establish a deployment. The
included policy example is `dry_run`; `enforce` additionally needs an Executor
authority handoff and a host contract that permits local restart decisions.
A Pod-side transport binding and successor evidence were prototyped and are
frozen; neither is required by the current path.
The three-host packages remain in this repository for review; the reflex path
does not consume arena projections.
