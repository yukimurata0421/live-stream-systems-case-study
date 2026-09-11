# Responsibility refactor and failure hardening

## Scope and provenance

Public implementation commit
`139ca84f84de6df73f1a13ba584abebc36dadabd` reconstructs a reviewed private
dirty-worktree snapshot identified by behavioral content identity
`73aae2f9eb4db9e9ddbab79823fa5ea2d52fb3f9`. The content identity is not a Git
commit. Portable paths, explicit external-source arguments, self-contained
fixtures, and public sanitation intentionally make the two trees byte-different.

This milestone changes the credential-free arena shadow only. It does not add a
real notification provider, notification credential, runtime mutation command,
Dell executor, or Raspberry Pi dependency. It also does not claim that this
public commit is deployed.

## Responsibility split

The refactor reduces the size of change and authority boundaries without
turning each domain into a Pod or database.

| Area | Split responsibilities | Boundary preserved |
| --- | --- | --- |
| reporting | read model, streaming accumulator, coverage, parity, policy, builder | read-only evidence aggregation |
| host sentinel | Kubernetes reads, storage facts, continuity, evidence decoding, checks, payload, atomic output | detection only; no restart or delivery |
| safe input | stable I/O, sanitation, lifecycle projection, rollout projection, generation, publication | only the projector reads raw external state |
| observation | worker process, scheduler/deadline, result model, summary, persistence | adapters remain read-only |
| storage | observation, current, incident, health, metric, publication, outbox, and maintenance stores | services receive role-specific protocols |
| notification outbox | intent, lease, and delivery state machines | incident judgment remains separate from provider delivery |
| shadow cycle | orchestration, projection/parity, transactional cycle record, artifact reconciliation | CLI handles arguments, locks, and output only |

`PostgresMonitoringRepository` is not a subclass of the SQLite root. Shared
domain stores are composed behind backend-neutral protocols; SQLite-only file
backup/restore/forensics are not exposed by PostgreSQL. The database grant
matrix and repository ports are separate but mutually checked boundaries.

## Durable artifact publication

Before schema v6, a shadow cycle could commit successfully and the process
could stop before the public-safe file was replaced. The database would show a
completed cycle while the artifact stayed permanently old.

Schema v6 adds `public_artifact_publications`:

1. the cycle row and a canonical payload intent are committed in one database
   transaction;
2. the pending payload is written with symlink-resistant atomic file I/O;
3. the file is read back and compared byte-for-byte;
4. the ledger is acknowledged as published;
5. a later cycle reconciles any pending or missing latest artifact before and
   after normal processing.

| Failure point | Recovery behavior |
| --- | --- |
| before database commit | neither cycle nor intent exists |
| after commit, before file write | pending intent recreates the file |
| after file write, before acknowledgement | exact existing bytes are acknowledged without rewriting |
| after acknowledgement, file later missing | latest published payload repairs the file |
| filesystem failure | intent remains pending with bounded failure evidence |
| newer cycle while an old intent is pending | old intent becomes superseded; newest canonical payload wins |

This is convergent at-least-once reconciliation, not a claim that PostgreSQL
and a host filesystem participate in one atomic transaction.

## Event-aware evidence

Polling current state cannot prove every short event. A process may stop and
self-recover between two healthy samples. The safe-input projector therefore
creates a bounded, sanitized runtime-lifecycle projection from completed event
pairs. It excludes URLs, commands, PIDs, stderr, credentials, and arbitrary log
text.

The lifecycle adapter records the edge as historical evidence and never defines
delivery current. Only a recent completed edge followed by a post-recovery good
delivery current can create a closed, recovery-only episode. Stable event-based
transition identity and an episode/phase lookup prevent repeat polling from
creating duplicate intents. An ordinary overlapping current-state recovery
takes precedence.

Planned-rollout evidence is also versioned. A proof requires the expected
workload, a bounded annotation window, a matching running-Pod start, and an
observation time after both the plan and Pod start. It can classify only
delivery/rendering parity differences inside that window. It does not turn a
bad current state into good, suppress unrelated domains, or rewrite the
immutable original cycle.

## Safe-input generation

The projector publishes a complete twelve-file generation:

1. stable-read and sanitize every required source into memory;
2. reject the whole candidate if any source is missing, changing, oversized,
   non-regular, malformed, or has unknown revision identity;
3. hold the exclusive projection lock and atomically replace all members;
4. publish a versioned manifest last with the exact name, size, SHA-256, source
   revision, and stable generation ID;
5. let the core hold the shared lock and revalidate the complete manifest before
   running adapters.

A source rejection leaves the previous complete generation unchanged. A crash
during member publication produces a digest mismatch, so the core fails closed
until the next complete generation converges.

## Persistence and redundancy controls

| Failure class | Control | Explicit limit |
| --- | --- | --- |
| duplicate core writer | one `Recreate` replica, database advisory lock, application lease | not active-active |
| PostgreSQL Pod replacement | one-replica StatefulSet and retained PVC | same node and k3s failure domain |
| PVC/device loss | primary dump plus SHA-matched copy on a different filesystem | both copies remain on one host |
| logically unusable dump | daily full restore into a disposable network-isolated PostgreSQL and versioned attestation | bounded time, memory, and temporary storage |
| destructive row retention | fresh matching dual copies, distinct-device check, and exact fresh restore attestation | bounded batches; referenced evidence is retained |
| destructive file retention | separate job, newest pairs and restore anchor protected, checksum unlinked before dump | 35/90-day policy and candidate-count limit |
| cluster or evidence failure | host systemd sentinel checks Pod count/identity/restarts, report, disk, backup, and restore | detection only; no automatic k3s restart |

Backup, restore, and retention jobs use `concurrencyPolicy: Forbid` and a bounded
missed-schedule window. Retention independently rechecks evidence immediately
before deletion; schedule order is not treated as a safety guarantee.

## Delivery and process safety

The isolated outbox includes a delivery epoch, fenced lease ownership, bounded
provider deadline, and terminal `uncertain` handling. If a provider may have
accepted a request but the acknowledgement is lost, automatic retry stops
instead of risking duplicate delivery. No real provider or credential is part
of this milestone.

Readiness and liveness are different. Invalid input or a temporary database
failure withdraws readiness; a process that still advances its heartbeat is not
restarted merely because data is unavailable. Stalled schedulers and deadlocks
stop advancing heartbeat and can be restarted by k3s. Health-file paths reject
same-path, symlink, and hard-link aliases so liveness cannot masquerade as data
readiness.

## Verification record

- Host: `242 collected / 236 passed / 6 skipped`.
- Fixed Python 3.13 audit image, network disabled and root filesystem read-only:
  the same `242 / 236 / 6` result.
- Disposable PostgreSQL 17: all six normally skipped destructive integration
  tests passed after schema versions 1 through 6 were applied.
- Integration scope includes v2-to-v6 in-place upgrade, SQLite import
  idempotency and conflict rollback, schema/reference enforcement, role/table
  privileges, exporter read-only behavior, single-writer lock, retention, and
  artifact reconciliation.
- Production Dockerfile build and `pip check`: passed.
- k3s manifests: 19 resources and 1,277 rendered lines before public placeholder
  substitution.
- Public snapshot validation: 228 files and zero findings at the implementation
  commit validation point.

The six default skips are not silently counted as passes; they require explicit
disposable database DSNs and are verified as a separate integration gate.

## Remaining boundaries

- Unit, fault-injection, and disposable-database tests cannot prove that every
  future kernel, disk, k3s, or PostgreSQL failure is impossible.
- R2 still needs seven elapsed days at the required received/fresh coverage for
  one unchanged revision pair.
- R3 still needs fourteen elapsed days of classified semantic parity for that
  pair.
- A separate PostgreSQL Pod is process/resource isolation, not node high
  availability.
- Same-host different-device backup does not cover host, power, or site loss.
- R5 real delivery, R6 public-source authority cutover, R7 typed Dell execution,
  and R8 retirement of legacy authority remain outside this milestone.

The per-failure classification, evidence mapping, and explicit unknowns are in
[Failure-mode mitigation record](failure-mode-mitigation-record.md).
