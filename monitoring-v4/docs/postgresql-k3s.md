# PostgreSQL and k3s subsystem

## Decision

The credential-free Monitoring v4 shadow is modeled as a k3s subsystem. Its
PostgreSQL database runs in a dedicated one-replica StatefulSet with a retained
PVC. Input projection, decision core, reporting, metrics export, and backup are
separate workloads.

A separate PostgreSQL Pod is a process, resource, and rollout boundary. It is
not a different node, control plane, or physical disk. If the single arena k3s
cluster or node stops, the application Pods and PostgreSQL stop together.

## Why the design moved beyond SQLite

SQLite was the right R1 backend for validating versioned contracts,
transactions, replay, backup/restore, and strict isolation from production. A
single-process shadow can own one file without a network database.

The constraint changed when the shadow was divided into k3s workloads. Keeping
SQLite would require a single writer Pod or a carefully shared filesystem,
while reporter, exporter, and backup behavior would inherit file ownership,
locking, mount, and rescheduling semantics. Database availability would remain
tightly coupled to the application writer that owned the file.

PostgreSQL moves transaction, lease, connection, and role enforcement behind a
stable Service. The core, reporter, exporters, backup, maintenance, and
migration jobs can be independent clients. Application Pod replacement no longer changes the
database endpoint or the retained volume. This resolves the SQLite-file and
single-writer-Pod ownership boundary that blocked clean orchestration.

It does **not** resolve node-level single points of failure. Replication,
another node, and off-host backup require a separate HA/disaster-recovery
decision and evidence.

The current hardening keeps one database writer but makes an application/file
crash window recoverable: schema v6 stores a public-artifact publication intent
in the same transaction as its shadow cycle, then reconciles exact file bytes
after commit. A process exit between those operations leaves a pending intent
instead of silently freezing an old artifact.

## Workload split

| Workload | Replicas | Responsibility | Write boundary |
| --- | ---: | --- | --- |
| PostgreSQL StatefulSet | 1 | v4 ledger, current state, outbox, projections | retained 20 GiB PVC |
| safe-input projector | 1, Recreate | allowlisted projection of external read-only state | fixed v4-owned input files |
| core | 1, Recreate | observation, reduction, incidents, notification intents | PostgreSQL and private shadow artifact |
| reporter | 1, Recreate | revision-pinned coverage/parity report | private report artifact |
| exporter | 2 | read-only metrics and health | none |
| backup CronJob | one at a time | custom-format dump, SHA-256 validation, and different-device copy | two host directories outside the PVC |
| restore verification CronJob | one at a time | credential-free full restore into a disposable database and versioned attestation | temporary database and bounded attestation directory |
| database retention CronJob | one at a time | bounded row retention only after fresh dual-copy and restore evidence | maintenance-scoped tables |
| backup-file retention CronJob | one at a time | 35/90-day file policy with protected newest pairs and exact restore anchor | primary and independent backup directories |
| migration/import Jobs | on demand | schema and idempotent SQLite merge | explicitly scoped database |
| host sentinel | outside k3s | cluster/workload/freshness detection | sentinel state only |

No notifier, controller, or Dell executor Pod is included. Database role names
for a future notifier do not grant a credential or deploy a delivery process.

## Input and network isolation

Only the projector mounts external raw state. It reconstructs a fixed set of
JSON documents from typed allowlists, stages one complete generation in memory,
and publishes an exact-file size/digest manifest last. The core revalidates
every member while holding the shared projection lock. A rejected source leaves
the previous complete generation unchanged; a crash-partial generation is
rejected rather than mixed with the old one. The projector has no database
environment, receives no service-account token, and has no permitted network
egress. The core mounts only that safe projection.

The namespace starts with default-deny ingress and egress. PostgreSQL port 5432
is allowed only from labeled core, reporter, exporter, backup, maintenance, and
migration workloads. Exporter port 9118 is allowed only from declared monitoring
namespaces. Application containers run non-root with a read-only root
filesystem, no capabilities, and no service-account token.

This boundary reduces reachability; it is not a general DLP system. An already
allowlisted ordinary string could still contain inappropriate producer data,
so source ownership and field semantics remain part of the contract.

## Database roles

Migration, core, reporter, exporter, backup, maintenance, and future notifier
use separate roles. Grants are table-scoped: the reporter reads only migration/shadow-cycle
evidence, the exporter reads its metric tables, and the core cannot write
delivery result tables. Secret values are created at deployment time and are
not present in this repository.

Application services depend on role-specific repository protocols rather than
one backend-specific root object. PostgreSQL does not inherit SQLite backup or
restore operations, and reporter/exporter ports expose no raw connection or
write methods. The role/table matrix generates grants and is checked against a
disposable PostgreSQL instance.

The current core role is not fully operation-scoped: it can update or delete
some core-owned tables even where append-only application behavior is intended.
Narrower operation-specific roles or stored operations remain follow-up work.

## Migration and rollback

The SQLite source is retained for forensics and rollback. A final import first
creates a checkpointed, read-only SQLite snapshot, then performs an idempotent
PostgreSQL merge. Immutable rows are compared inside the same transaction; a
mismatch rolls the merge back.

Cutover order is single-writer by construction: validate the candidate k3s
subsystem, stop the legacy SQLite timer, confirm no running oneshot, import the
frozen snapshot, verify the merge, and then let only the PostgreSQL core create
new cycles.

Rollback reverses authority without deleting evidence: scale the PostgreSQL
core to zero, confirm the writer stopped, and enable exactly one SQLite writer.
The PostgreSQL PVC and dumps remain intact.

## Operational limits

- A retained local PVC protects against ordinary Pod recreation, not disk loss.
- Primary and independent dumps must be on different filesystems, have the same
  SHA-256, and pass a bounded full restore before retention can delete older
  evidence. Both copies are still same-host protection, not an off-host
  disaster backup.
- Restore and retention fail closed when evidence is stale, future-dated,
  mismatched, over the candidate limit, or no longer on separate devices.
- Database liveness is not equated with application correctness. Readiness and
  last-success freshness use bounded startup windows to avoid restart loops
  during a planned database restart.
- The host sentinel is detection-only. It cannot restart k3s, send Discord or
  Slack messages, or touch the streaming runtime.
- No production notifier credential, public publisher cutover, or runtime
  mutation authority is enabled by this subsystem.
