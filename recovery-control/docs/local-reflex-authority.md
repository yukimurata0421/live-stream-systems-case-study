# Local Reflex Authority

The 2026-10-22 source port adds `src/cra/`, a smaller recovery path for one
action: restart the managed FFmpeg child after a confirmed local TCP stall, at
most once per FFmpeg generation. It is public code and test surface, not a
production authorization or qualification result.

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

## Boundaries

Public tests run the agent against fakes and against the real Effect Executor
with a temporary ledger. Source presence does not establish a deployment. The
included policy example is `dry_run`; `enforce` additionally needs an Executor
authority handoff and a host contract that permits local restart decisions.
A Pod-side transport binding and successor evidence were prototyped and are
frozen; neither is required by the current path.
The three-host packages remain in this repository for review; the reflex path
does not consume arena projections.
