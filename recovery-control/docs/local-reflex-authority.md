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
- A stall counts only across distinct observation sequences, with no
  `network_down` and a reachable ingest.
- Uptime, Executor in-flight count, Executor authority, cooldown, hourly and
  daily limits, policy mode, and a local stop file all block a request.
- Policy is adopted only with a valid cra-01 signature and a higher version.
  Without one, the built-in policy is `dry_run`.
- The audit log is append-only. cra-01 stores it in order without duplicates and
  keeps conflicting records separately.

`cra-central verify` checks the stored audit against these rules and against the
published policy history. In `dry_run`, the same check summarizes how often a
confirmed stall recovered by itself, which is the evidence for any later
`enforce` decision.

## Boundaries

Public tests run the agent against fakes and against the real Effect Executor
with a temporary ledger. Source presence does not establish a deployment. The
included policy example is `dry_run`; `enforce` additionally needs an Executor
authority handoff and a host contract that permits local restart decisions.
The three-host packages remain in this repository for review; the reflex path
does not consume arena projections.
