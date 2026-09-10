# Three-Host Recovery Architecture

## Goal

The control plane reduces recovery time without allowing one monitoring
process, one stale cache, or one host to both decide and perform a restart.
Host, Pod, container, and FFmpeg child are distinct failure domains.
The observed duplicate-decision history behind this split is summarized in
[`why-cra.md`](why-cra.md).

## Ownership

| Component | Owns | Must not own |
| --- | --- | --- |
| arena-server | Observation, freshness, parity, incident evidence, signed facts-only projection. | Action, authorization, command, recovery budget, or final verdict. |
| cra-01 | Policy, budget, cooldown, authorization, command lifecycle, final `RECOVERED` / `FAILED` / `UNKNOWN`, reconciliation. | Direct process, container, Pod, or host mutation. |
| Dell Recovery Agent | Credential use, authority lease, exact target admission, durable effect fence, local execution truth. | Reimplementation of central policy or independent final verdict. |
| Effect Executor | The one physical effect against the exact managed FFmpeg child. | Deciding whether the command is authorized. |

## Evidence flow

Dell publishes signed target and runtime observations through a GET-only mTLS
server. arena-server pulls those observations and joins them with a
PostgreSQL transaction running `REPEATABLE READ READ ONLY`. The resulting
facts bundle is checked for freshness, parity, and forbidden control fields,
then signed and published as a Monitoring Evidence Projection.

cra-01 pulls that projection into an atomic inbox. The authorizer evaluates
the incident, required checks, cooldown, budgets, unresolved commands, and
operating mode. The verifier later joins execution evidence with fresh pre/post
monitoring evidence; it has no effect capability.

## Transaction model

Each host has a separate SQLite truth. There is no cross-host distributed
transaction.

The Central database commits command intent and an outbox message in one local
transaction. Dell admits a request only after producer, generation, target,
freshness, and fence checks. The runtime reserves `physical_attempt_count = 1`
before the effect function can run. Ambiguous outcomes remain
`OUTCOME_UNKNOWN` until append-only reconciliation proves an effect or no
effect.

## Local fallback

Local fallback is bounded by a prior authority lease and shares the same
logical-generation effect scope as central commands. Reconnection does not
allow Dell to push arbitrary truth into the central database. cra-01 pulls the
signed local journal, imports it exactly once, reconciles uncertainty, and only
then installs a new central authority epoch.

## Integration boundaries

- `stream_v3` owns delivery media and the exact FFmpeg child lifecycle.
- Monitoring v4 owns arena-server evidence storage and facts generation.
- This repository owns the protocol, central authority, Dell Agent boundary,
  and cross-host evidence/reconciliation contracts.

These source boundaries do not prove that any particular revision is deployed.
