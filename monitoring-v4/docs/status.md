# Implementation status

This file reports code availability separately from elapsed-time and
production-authority gates.

| Phase | Code status in this milestone | Evidence still required |
| --- | --- | --- |
| R0 | ownership inventory and import guards implemented | a deployment-specific, read-only live inventory |
| R1 | versioned contracts, isolated SQLite, schema-compatible PostgreSQL, backend-neutral repository ports, and schema-v6 durable artifact publication implemented | none for the isolated code gate |
| R2 | seven-domain read-only adapters, sanitized lifecycle evidence, all-or-nothing safe-input generations, and revision-pinned fresh-coverage ledger implemented | seven days at the required received/fresh source coverage |
| R3 | deterministic reduction, verified rollout evidence, and revision-pinned semantic parity ledger implemented | fourteen days of classified shadow parity |
| R4 | incident episodes, debounce, repeat, single recovery, and event-scoped recovery idempotency implemented | none for the isolated code gate |
| R5 | durable intents, SQLite/PostgreSQL leases, delivery epoch, fenced attempts, terminal-uncertain handling, and fake providers implemented | real-provider decision, credential handoff, single-writer cutover, and soak |
| R6 | typed SLI projection, read-only compatibility metrics, durable public-safe artifact reconciliation, evidence report, and credential-free k3s exporter implemented | fixed-end-time parity, full metric/schema parity, publisher boundary review, and production cutover decision |

The public tree also contains a read-only soak checkpoint evaluator. It binds
every checkpoint to one build revision, one migration-source revision, and one
epoch, returning `NOT_YET_ELIGIBLE`, `EVIDENCE_PENDING`, or a gate result without
writing the database or authorizing recovery.

R7-R8 are not implemented in this milestone. No production notification
credential, real delivery process, public publisher cutover, or runtime
mutation authority is enabled. R2 and R3 remain incomplete until their elapsed
live evidence gates are satisfied for one unchanged revision pair.

## Validation recorded for this public milestone

- Committed private implementation lineage through `8f2c1aa...` is represented
  by public history through `997863c`.
- Current host validation: `391 tests / 385 passed / 6 skipped`.
- Disposable PostgreSQL 17: the six normally skipped destructive integration
  tests all passed after schema versions 1 through 6 were applied.
- k3s rendering: 19 resources and 1,277 lines before placeholder substitution.
- Public snapshot validation: zero issues at the current validation point.

The machine-readable phase file also includes a sanitized historical private
deployment checkpoint assessed on 2026-08-14 16:05 JST. It records the start
of a new unchanged-revision soak, not completion of the seven- or fourteen-day
gate and not current health. The public implementation commit has not been
deployed by this publication operation.

The implementation details and residual failure boundaries are recorded in
[Responsibility refactor and failure hardening](hardening.md).

The evidence and non-guarantee for each known failure mode are recorded in
[Failure-mode mitigation record](failure-mode-mitigation-record.md).
