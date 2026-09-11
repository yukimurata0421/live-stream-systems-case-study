# Failure-mode mitigation record

Recorded: 2026-08-14 JST

Public source baseline:
`d93d580e3ac587ca93e3cebffc41cc928c327bfe`

Implementation milestone:
`139ca84f84de6df73f1a13ba584abebc36dadabd`

Private behavioral identity used for reconstruction:
`73aae2f9eb4db9e9ddbab79823fa5ea2d52fb3f9`

## Purpose and claim boundary

This is a factual register of known Monitoring v4 failure modes and the design
used to address each one. “Addressed” does not always mean “made impossible.”
The register distinguishes five outcomes:

| Outcome | Meaning in this record |
| --- | --- |
| prevented | the checked boundary rejects the condition before the protected side effect, and a test exercises that rejection |
| fail-closed | processing or deletion stops and exposes an unavailable/bad result; availability may still be lost |
| reconciled | durable state allows a later attempt to converge after a tested crash window |
| mitigated | likelihood or impact is reduced, but the failure remains possible |
| detected only | the condition is surfaced; no automatic repair is authorized |

An outcome applies only to the code and deployment contracts in this
repository. It does not prove that every out-of-tree overlay, host setting,
kernel behavior, external provider, or future change obeys the same boundary.

The private high-resolution operational logs were intentionally not copied into
this public repository. Where a failure was first observed in private
operation, this document says so. Public code and tests can verify the resulting
design, but they cannot independently reproduce the omitted raw event.

## Evidence index

The tables use these short evidence references:

- `T-ARCH`: [architecture and inventory tests](../tests/test_architecture_inventory.py)
- `T-ADAPTER`: [adapter and reducer tests](../tests/test_adapters_reducers.py)
- `T-CONTRACT`: [contract and SQLite storage tests](../tests/test_contracts_storage.py)
- `T-SAFE`: [safe-input runtime tests](../tests/test_safe_input_runtime.py)
- `T-LIFECYCLE`: [runtime lifecycle tests](../tests/test_runtime_lifecycle.py)
- `T-INCIDENT`: [incident and replay tests](../tests/test_incidents_replay.py)
- `T-NOTIFY`: [notification and dispatcher tests](../tests/test_notifications.py)
- `T-PUBLICATION`: [artifact publication tests](../tests/test_publication.py)
- `T-PIPELINE`: [end-to-end pipeline tests](../tests/test_pipeline_e2e.py)
- `T-REPORT`: [exporter and evidence-report tests](../tests/test_exporter_shadow_report.py)
- `T-PG`: [PostgreSQL and k3s contract tests](../tests/test_postgres_k3s.py)
- `T-PG-LIVE`: [disposable PostgreSQL integration tests](../tests/test_postgres_live_integration.py)
- `T-MIGRATE`: [PostgreSQL migration-command tests](../tests/test_postgres_migrate_command.py)
- `T-BACKUP`: [backup-file retention tests](../tests/test_backup_retention.py)
- `T-RESTORE`: [restore-selector tests](../tests/test_restore_select.py)
- `T-RETENTION`: [database-row retention tests](../tests/test_retention.py)
- `T-SENTINEL`: [host sentinel tests](../tests/test_sentinel.py)
- `T-ENTRY`: [long-running entrypoint tests](../tests/test_runtime_entrypoints.py)
- `T-HTTP`: [exporter HTTP tests](../tests/test_exporter_http.py)
- `T-BOOTSTRAP`: [database Secret bootstrap tests](../tests/test_bootstrap_secrets.py)

The default suite collects 242 tests: 236 pass and six disposable-PostgreSQL
tests skip without explicit DSNs. The six skipped tests were also run separately
against PostgreSQL 17 and passed. This proves the tested paths, not the absence
of unmodeled failures.

## A. Authority and isolation failures

| ID | Failure mode and impact | Design used | Outcome and evidence | What is not proven |
| --- | --- | --- | --- | --- |
| A-01 | Monitoring code imports the legacy runtime executor or production CLI and silently gains mutation authority. | Source-tree layering rejects contract-to-implementation, domain-to-adapter, and monitoring-to-runtime/legacy imports. | prevented for the tracked Python tree; `T-ARCH` | Dynamic imports, an out-of-tree package, or a later deployment wrapper are outside the static scan. |
| A-02 | V4 state or output is placed inside the migration-source tree and mutates the source of truth. | SQLite database and artifact paths are resolved before cycle writes; source and output containment are rejected. k3s mounts raw input only in the projector and mounts it read-only. | prevented at checked path boundaries; `T-PIPELINE`, `T-PG` | Root-level host remounts, bind-mount aliases not represented by the checked path, or an out-of-tree manifest are not proven safe. |
| A-03 | A notifier, Dell executor, or Raspberry Pi internal dependency is accidentally included in the isolated subsystem. | Public kustomization contains no notifier/executor; manifests carry no Discord/Slack value; reports require delivery, runtime mutation, and Pi-dependency flags to be explicitly false. | prevented in the rendered repository manifests and fail-closed in report checks; `T-ARCH`, `T-PG`, `T-SENTINEL` | A separately applied manifest or host service outside this repository cannot be ruled out by source tests. |
| A-04 | A credential or private state artifact is copied into the public mirror. | Public snapshot validator rejects private paths, state/log/database/dump/key-like files, oversized files, and known secret material; full-tree and worktree secret scans are separate release gates. | prevented for declared patterns and no finding in the recorded scans; `T-ARCH` and [validator](../ops/scripts/validate_public_snapshot.py) | Pattern scanning cannot prove that an arbitrary ordinary-looking string has no sensitive meaning. Manual review remains required. |
| A-05 | An analytical SLI or public artifact becomes current-state or automatic-recovery authority. | Contracts require non-formal projections to avoid compliance claims and require `no_automatic_recovery`; reducer/incident boundaries do not consume projections as runtime commands. | prevented in contract and projection paths; `T-CONTRACT`, [reliability tests](../tests/test_reliability_projection.py) | Future R6/R7 wiring is not covered until its authority graph is reviewed. |

## B. Source input and observation failures

| ID | Failure mode and impact | Design used | Outcome and evidence | What is not proven |
| --- | --- | --- | --- | --- |
| B-01 | A source file is a symlink, non-regular file, changes while read, is replaced across devices, or exceeds its bound; inconsistent bytes could be accepted. | Stable readers use non-following file access, type/size/identity checks before and after reads, and include device identity. | fail-closed; `T-SAFE` | A kernel or storage device returning internally corrupted but stable bytes is not detected unless a higher-level contract/digest fails. |
| B-02 | One invalid source arrives after some valid files and partially overwrites the last-good safe input. | The projector builds all twelve sanitized members in memory first. Any rejection aborts the candidate before target publication. | prevented for source rejection; `T-SAFE:test_rejected_source_preserves_the_complete_previous_generation` | Memory exhaustion before the bounded candidate is complete still causes an unavailable cycle; it does not produce a new generation. |
| B-03 | The projector exits while publishing files, leaving members from two generations that the core treats as one. | A versioned manifest containing exact names, sizes, SHA-256 values, source revision, and generation ID is published last. The core rehashes all members while holding the shared lock. | fail-closed after a partial publication and reconciled by the next complete projector cycle; `T-SAFE` | This is not an active-active directory transaction. Availability is lost until a complete generation succeeds. |
| B-04 | Projector and core run concurrently and the core sees an in-progress generation. | Projector holds an exclusive `flock`; core holds the same lock shared through validation and adapter reads. | prevented for cooperating processes; `T-SAFE:test_projection_lock_prevents_mixed_generation_reads` | A process that ignores the lock can still modify host files; filesystem ACLs and deployment ownership remain necessary. |
| B-05 | A large or rotated lifecycle JSONL is loaded without bound and causes memory exhaustion. | The reader uses a bounded tail, per-line size limit, line-count limit, and fixed maximum event output. | mitigated and fail-closed over declared bounds; `T-SAFE` | The configured bounds may still be too high for a much smaller future node; resource-limit tuning remains operational work. |
| B-06 | NaN, infinity, oversized integers, scalar coercion, missing/extra fields, or future timestamps enter typed evidence. | Strict JSON/contract decoders reject non-finite values, type coercion, unknown fields, invalid UTC time, and future evidence where disallowed. | prevented at implemented decoders; `T-CONTRACT`, `T-SAFE`, `T-LIFECYCLE`, `T-REPORT` | Semantic lies that are well-typed and within bounds require independent evidence; schema validation alone cannot detect them. |
| B-07 | An adapter hangs; the observer times out but leaves a worker/process alive, accumulating resource leaks. | Adapters execute behind a killable worker-process boundary with start and execution deadlines, terminate/kill cleanup, and bounded result decoding. | mitigated; repeated timeout leaves no adapter process in the test; `T-ADAPTER` | Uninterruptible kernel I/O (`D` state) may not terminate promptly and is not reproduced by the test. |
| B-08 | An adapter returns valid observations and a rejection in the same batch; the cycle is incorrectly counted successful and healthy. | Observer outcome has an explicit `partial` state. Coverage counts it as a collector failure and component health does not become good merely because some observations exist. | prevented in report/health calculation; `T-ADAPTER`, `T-REPORT` | A producer can still omit a field without rejection if the adapter contract mistakenly defines that field as optional. |
| B-09 | Producer scheduling jitter crosses wall-clock buckets and is mistaken for missing source coverage. | Coverage gates use V4 cycle receipt plus source freshness. Producer event-time gaps remain a diagnostic instead of directly defining the gate. | mitigated for the known scheduling model; `T-REPORT` | The correct freshness bound for a changed upstream schedule must be recalibrated; the code cannot infer producer intent. |
| B-10 | A missing or mutable build/source revision silently extends an earlier soak. | Coverage and parity are partitioned by build and source revision; unknown/non-immutable revision fails closed instead of joining prior evidence. | fail-closed; [source revision tests](../tests/test_source_revision.py), `T-REPORT` | The content-identity allowlist must continue to cover every behaviorally relevant deployment input. |
| B-11 | A stop and self-recovery completes between polling samples, so both current snapshots are good and the edge disappears. | The projector derives a bounded sanitized completed-lifecycle edge; the adapter stores historical evidence without redefining current; a recent edge can create a recovery-only closed episode after post-recovery good. | mitigated for producer-recorded completed edges; `T-LIFECYCLE`, `T-PIPELINE` | If the source process/node dies before writing its event ledger, this mechanism cannot recover the direct cause or even prove that edge. |
| B-12 | Planned-rollout proof arrives after a parity difference, or stale/future proof broadly excuses unrelated mismatches. | Versioned proof requires fixed workload, bounded window, matching Pod start, and observation after plan/start. Later report reconciliation is limited to delivery/rendering inside that verified window. | fail-closed for malformed proof and narrowly reconciled for valid delayed proof; `T-ADAPTER`, `T-REPORT` | A legitimate manual rollout without the required annotation remains unclassified by design. Human classification may still be needed. |

## C. Current-state, incident, and evidence-aggregation failures

| ID | Failure mode and impact | Design used | Outcome and evidence | What is not proven |
| --- | --- | --- | --- | --- |
| C-01 | Stale, future, replayed, or out-of-order evidence replaces newer canonical current. | Reducers choose by semantic event time/stable identity, reject future evidence, preserve canonical newer current, and do not backdate transitions after clock rollback. | prevented in tested ordering/rollback cases; `T-ADAPTER`, `T-CONTRACT`, `T-INCIDENT` | Large real clock steps and producer clocks that are wrong but within accepted skew may still create unavailable evidence. |
| C-02 | Raw OAuth is good while its Prometheus/rolling projection is bad; same-producer sampling disagreement opens a false input-quality incident. | Raw OAuth current evidence is authoritative. Prometheus/rolling values are diagnostic-only; `noData`, unknown, and same-producer disagreement do not define current bad. Independent coverage/freshness remains actionable. | prevented for the modeled input-quality policy; `T-ADAPTER`, `T-INCIDENT` | If raw OAuth itself is incorrect, this policy will follow the wrong authority until independent evidence or human review finds it. |
| C-03 | The same completed lifecycle edge is reread with a new current snapshot and creates repeated recovery transitions/intents. | Episode and transition identities are event-scoped; an existing episode/phase lookup also suppresses older snapshot-scoped transition forms. | prevented in replay and upgrade fixtures; `T-LIFECYCLE` | Corruption or manual deletion of the dedupe rows can remove the guard. Database integrity/backup then becomes the recovery boundary. |
| C-04 | Normal current-state recovery and lifecycle-edge recovery overlap and create two recovery notifications for one interval. | Current incident processing commits first; lifecycle processing checks for an overlapping closed/recovered episode before creating a recovery-only record. | prevented in the tested overlap; `T-LIFECYCLE:test_sampled_current_recovery_suppresses_duplicate_edge_recovery` | Two genuinely independent failures with overlapping timestamps may require policy-specific interpretation not encoded here. |
| C-05 | A very old lifecycle edge is backfilled and emits a misleading new recovery notification. | Recovery-only edge delivery is bounded to a recent window; older evidence is stored historically but cannot enqueue a new intent. | prevented for the configured 30-minute boundary; `T-LIFECYCLE` | Whether 30 minutes is the best operational boundary is a policy choice, not a theorem. |
| C-06 | Scheduler delay moves every repeat deadline forward, causing cadence drift or notification storms after catch-up. | Incident repeat deadlines and service timers are anchored to wall-clock slots rather than prior completion time. | prevented for tested cadence calculations; `T-INCIDENT`, [systemd cadence tests](../tests/test_systemd_cadence.py) | Suspend/resume and very long outages still require operator interpretation of missed windows. |
| C-07 | Maintenance or planned rollout is interpreted as “good,” erasing an actual bad state. | Maintenance suppresses creation/escalation where policy allows but does not rewrite current to good; bad after a planned rollout escalates normally. | prevented in incident fixtures; `T-INCIDENT` | Correct maintenance-window creation remains an external operational responsibility. |
| C-08 | Duplicate report rows dilute a bad parity cycle or wide JSON rows accumulate in memory/temp storage. | Report read model filters revision/time in SQL without wide sort; streaming accumulator deduplicates cycle semantics and retains bounded aggregate state. | mitigated; `T-REPORT` | The public tests do not independently contain the private live PostgreSQL temp-file counters that originally motivated this change. |
| C-09 | A stale/incomplete projection replaces the last-good public-safe artifact or asserts formal compliance. | Projection integrity must be complete; stale inputs cannot refresh output; public schema is fixed and excludes internal IDs/compliance claims. | prevented in projection and end-to-end fixtures; `T-PIPELINE`, [reliability tests](../tests/test_reliability_projection.py) | A downstream publisher can still mislabel the artifact if it ignores the documented schema/interpretation boundary. |

## D. Database, schema, migration, and publication failures

| ID | Failure mode and impact | Design used | Outcome and evidence | What is not proven |
| --- | --- | --- | --- | --- |
| D-01 | A single SQLite file is shared across independently scheduled k3s clients, coupling lock ownership and filesystem placement to one writer Pod. | Live shadow clients use PostgreSQL behind a stable Service with scoped roles; SQLite remains an isolated test, migration, forensic, and rollback backend. | mitigated for Pod/client coordination; `T-PG`, `T-PG-LIVE` | PostgreSQL is still one replica on the same node/k3s failure domain. This does not provide node HA. |
| D-02 | Two core writers process the same live slot concurrently. | Core is one `Recreate` replica, requires a live PostgreSQL lease name, and holds a session-scoped advisory lock for the cycle. | prevented for cooperating deployed cores; `T-PG`, `T-PG-LIVE` | An uncooperative writer using different lease/lock names or direct SQL is outside the guard. Active-active is not supported. |
| D-03 | Advisory unlock is ambiguous, but the physical session is returned to the pool and may retain the lock. | Unlock result is checked; an ambiguous session is discarded rather than reused. | fail-closed; `T-PG:test_live_cycle_guard_is_session_scoped_and_deterministic`, `T-RETENTION` | Database/network failure may still make lock ownership temporarily unknowable until PostgreSQL ends the session. |
| D-04 | Per-session timeout/read-only configuration fails, but the connection is returned as healthy. | Failed session setup closes/discards the raw connection before pool return. | prevented from reuse in the tested pool contract; `T-PG:test_failed_session_configuration_is_discarded_from_the_pool` | Driver/kernel defects below the connection API are not modeled. |
| D-05 | SQLite and PostgreSQL report the same schema version while table, column, index, or reference constraints differ. | Machine-readable schema-surface comparison covers v1-v6, including `ALTER TABLE` additions; live PostgreSQL introspection is checked separately. | prevented by release tests for declared schema surface; `T-PG`, `T-PG-LIVE` | Triggers/functions/planner behavior outside the declared surface require explicit new parity rules. |
| D-06 | `domain_current`, candidates, or transitions reference a missing snapshot; retention or corruption leaves dangling live evidence. | SQLite triggers and PostgreSQL foreign keys enforce live references; migration refuses a preexisting invalid graph; retention preserves referenced snapshots/observations. | prevented in tested writes and retention; `T-CONTRACT`, `T-PG-LIVE` | Physical database corruption is handled by integrity/backup procedures, not relational constraints. |
| D-07 | Role creation/grant changes commit partially before schema migration fails. | Role mutation, initial grants, schema migration, and final grants have explicit transactional boundaries; failures roll back the active boundary. | mitigated and rollback-tested; `T-MIGRATE` | PostgreSQL role DDL semantics or extensions added later must be reevaluated; tests cover current statements. |
| D-08 | Secret bootstrap treats any read error as “missing,” overwrites another creator, or updates an existing Secret without concurrency control. | Only NotFound means missing; create races preserve the winner; extension uses exact name/namespace and `resourceVersion` replacement with bounded retry. | mitigated; `T-BOOTSTRAP` | Compromise of Kubernetes API credentials or Secret storage is outside this bootstrap logic. |
| D-09 | SQLite import reads every table into memory, observes a moving WAL source, or partially merges before a semantic conflict. | A checkpointed `journal_mode=delete` snapshot is prepared; rows stream in stable primary-key order; source identity is rechecked; PostgreSQL merge/conflict handling is transactional and idempotent. | prevented for tested source/merge races and bounded memory by batch; `T-PG`, `T-PG-LIVE` | Import duration and disk demand for a much larger future database have not been load-tested to an established maximum. |
| D-10 | SQLite or PostgreSQL connection ownership leaks across command exit/error and exhausts descriptors/pool sessions. | Context-managed ownership and `finally` closure are required in report, snapshot, import, and command paths; ResourceWarning regression tests cover known leaks. | mitigated; `T-PG`, `T-REPORT`, `T-HTTP` | New call sites can reintroduce leaks unless they use the same ownership conventions and tests. |
| D-11 | A shadow cycle commits, then the process exits before its artifact file is published; DB and file remain permanently inconsistent. | Schema v6 commits a pending publication intent with the cycle. Exact canonical bytes are later written, reread, and acknowledged. | reconciled across tested crash windows; `T-PUBLICATION`, `T-PG-LIVE` | PostgreSQL and the filesystem are not one atomic transaction; temporary staleness remains observable until reconciliation succeeds. |
| D-12 | File is published but DB acknowledgement is lost, causing needless rewrite or conflicting replay. | Reconciliation recognizes exact existing bytes and acknowledges without rewrite; same cycle with different payload is rejected. | reconciled/prevented for exact payload identity; `T-PUBLICATION` | Filesystem metadata preservation is not an exactly-once guarantee; the guarantee is convergence of canonical bytes. |
| D-13 | Clock rollback makes a future-dated old published row hide a newer pending intent. | Publication selection ranks pending state before published state rather than relying only on timestamps. | reconciled in rollback fixture; `T-PUBLICATION:test_pending_intent_wins_after_wall_clock_rollback` | A malicious/manual DB rewrite can violate ledger assumptions and is not auto-repaired. |
| D-14 | A pending artifact is repaired only after a new source cycle succeeds; continuous source failure keeps a recoverable file stale. | Pending/latest publication is reconciled before normal source processing as well as after a successful cycle. | reconciled when filesystem is writable even if the new source cycle fails; `T-PUBLICATION` | Permanent filesystem read-only/full conditions leave the intent pending and availability degraded. |
| D-15 | SQLite lock contention or transaction failure is treated as success and silently drops evidence. | Locked database and write failures propagate; transaction tests require full rollback rather than partial success. | fail-closed; `T-CONTRACT` | Prolonged contention still causes availability loss; SQLite is not the live multi-client backend. |

## E. Notification delivery failures

| ID | Failure mode and impact | Design used | Outcome and evidence | What is not proven |
| --- | --- | --- | --- | --- |
| E-01 | A shadow intent is delivered merely because a provider object or credentials become available. | Intents remain nondeliverable without an explicitly activated delivery epoch/cutover boundary. | prevented in isolated tests; `T-NOTIFY:test_shadow_intents_remain_quarantined_even_if_provider_is_configured` | No real provider is implemented here, so an eventual production cutover still needs an end-to-end authority review. |
| E-02 | Two dispatcher instances lease and send the same intent. | Lease owner identity is instance-specific; lease fencing and terminal state are checked transactionally. | prevented for cooperating repository clients; `T-NOTIFY` | External provider-side duplicates caused after acceptance are a separate failure mode. |
| E-03 | Provider accepts a request, but timeout/connection loss hides the acknowledgement; automatic retry sends a duplicate. | Timeout, invalid response, dangling attempt, or lost fence becomes terminal `uncertain` and is not automatically retried. | mitigated by stopping automatic duplicate risk; `T-NOTIFY` | The system cannot tell whether the provider accepted the message. Manual/provider receipt reconciliation is still required. |
| E-04 | Lease expires while an in-flight provider call continues, allowing another writer to send. | Attempt timeout must be shorter than lease; hard deadline and fence checks prevent stale completion from becoming a normal retry. | mitigated/prevented for tested timing boundaries; `T-NOTIFY` | Process suspension beyond expected clock behavior and provider calls that ignore cancellation remain provider/runtime concerns. |
| E-05 | HTTP 429, 5xx, or permanent 4xx are retried with one generic rule, causing storms or futile delivery. | 429 honors retry-after, 5xx uses bounded backoff, permanent 4xx is terminal, and success cannot be sent twice. | prevented for scripted-provider classifications; `T-NOTIFY` | Actual Discord/Slack response and idempotency semantics are not verified because real providers are absent. |

## F. Process, k3s, backup, restore, retention, and sentinel failures

| ID | Failure mode and impact | Design used | Outcome and evidence | What is not proven |
| --- | --- | --- | --- | --- |
| F-01 | Database/source unavailability makes readiness fail and k3s restarts a still-healthy process repeatedly, amplifying the outage. | Data readiness and process heartbeat are separate. Temporary cycle failure withdraws ready while a live scheduler continues heartbeat; liveness detects stalled process progress. | mitigated; `T-ENTRY`, `T-PG` | A process can theoretically update heartbeat while semantically wedged in an unmodeled path; component evidence remains necessary. |
| F-02 | Ready and heartbeat paths are identical or alias via symlink/hardlink, so liveness updates falsely assert readiness. | Entrypoints reject same resolved path, symlink alias, and existing inode alias before work. | prevented at entrypoint validation; `T-ENTRY` | Later host-level path replacement after startup is constrained by atomic/non-following writes but not claimed impossible under root attack. |
| F-03 | Relative “sleep after completion” scheduling accumulates drift. | Shadow cycles and reports compute the next exact wall-clock second/minute slot. | prevented for scheduling calculation; `T-PG`, [systemd cadence tests](../tests/test_systemd_cadence.py) | Long suspend/outage creates missed slots; it does not manufacture elapsed evidence. |
| F-04 | Aggressive SQL liveness probes churn connections or restart PostgreSQL during a planned restart. | PostgreSQL uses bounded `pg_isready` readiness without query liveness; exporter liveness is DB-independent while health/metrics expose DB/render failures separately. | mitigated; `T-PG`, `T-HTTP` | `pg_isready` cannot prove schema or semantic correctness; application integrity checks remain separate. |
| F-05 | Floating base image, PostgreSQL image, or Python dependency changes behavior without source revision change. | Container images use digests; runtime wheels use exact versions and SHA-256 hashes; source identity includes deployment/sentinel inputs. | mitigated; `T-PG` | Registry, signing, builder, or upstream artifact compromise is not fully solved by digest pinning alone. |
| F-06 | PostgreSQL Pod exits; state disappears with the process. | StatefulSet reattaches a retained PVC and k3s recreates the Pod. | mitigated for ordinary Pod recreation | PVC/device corruption, node loss, and control-plane loss remain separate failures. |
| F-07 | PVC/device is lost and the only backup is on the same filesystem. | Backup creates a primary custom-format dump and a SHA-matched copy on a different filesystem; sentinel and destructive retention independently verify device difference. | mitigated for one local filesystem/device loss; `T-PG`, `T-SENTINEL`, `T-BACKUP`, `T-RETENTION` | Both copies are on the same host. Host, power, theft, and site loss remain unresolved. |
| F-08 | Backup is partially written, same-second job collides, or readers see dump before its checksum. | Temp files are verified and synced; checksum hard-link is published before dump hard-link; existing final names are not replaced; directory sync completes publication. | prevented/recoverable for tested publication order and collisions; `T-PG` | Filesystem/kernel claims about `sync` durability depend on actual hardware and mount semantics. Sudden power-loss testing is not in this repository. |
| F-09 | `pg_restore --list` succeeds but the dump cannot be fully restored. | Scheduled restore verification selects matching stable copies, runs a credential-free full restore into disposable PostgreSQL, verifies schema v1-v6 and bounded row counts, then atomically publishes an attestation. | fail-closed for retention without attestation; `T-PG`, `T-RESTORE`, `T-SENTINEL`, `T-RETENTION` | Restore is bounded by time, memory, and temporary disk. A future database that exceeds those limits will stop the gate until capacity is reviewed. |
| F-10 | Backup/report/restore evidence exists but the capability-free host sentinel cannot read it because modes/groups differ. | Produced evidence uses owner/operator-group read-only modes and directories deny world access; tests assert report/backup/attestation permission contracts. | prevented for repository manifests/output modes; `T-REPORT`, `T-SENTINEL`, `T-PG` | Actual host ownership/group setup is deployment-specific and must be checked after install. |
| F-11 | Old backup files are deleted in the creation job before a valid full-restore anchor exists. | File deletion is removed from backup creation and isolated in a later retention command requiring fresh matching copies, separate devices, and exact fresh restore attestation. | prevented before deletion in tested gates; `T-PG`, `T-BACKUP` | Operator/manual deletion outside the command is not guarded. |
| F-12 | Database-row retention deletes evidence after seeing two paths that actually resolve to the same mounted filesystem. | The destructive command opens both roots without following symlinks, rechecks stable identity/device difference, and refuses unsafe age/attestation before SQL deletion. | prevented in unit and real distinct-filesystem fixtures; `T-RETENTION` | Complex storage layers may share a physical failure domain despite different device IDs; `st_dev` proves filesystem distinction, not independent hardware/site. |
| F-13 | File retention removes dump before checksum; a second unlink failure leaves only checksum and loses data. | Pair deletion unlinks checksum first and data-bearing dump last; injected second failure must leave dump bytes. | mitigated; `T-BACKUP:test_second_unlink_failure_preserves_data_bearing_dump` | Directory corruption or failure after both unlinks is outside pair-order protection. |
| F-14 | Symlinked backup root, changed file after inventory, or unbounded candidate count causes wrong deletion or memory exhaustion. | Root opens are no-follow; identity is rechecked before unlink; candidate/member limits abort before any deletion. | prevented/fail-closed; `T-BACKUP` | Bounds require operator cleanup if exceeded; the command deliberately does not guess which unexpected files are safe to remove. |
| F-15 | A future-mtime dump after clock rollback hides the newest usable backup. | Selection ignores future candidates for the current anchor and reports them diagnostically. | mitigated; `T-SENTINEL` | A badly wrong system clock can make all otherwise valid evidence unavailable until time is corrected. |
| F-16 | Restore input is selected with shell `ls`/copy, follows a symlink, changes during copy, crosses the wrong device, or leaks its source descriptor on temp-file failure. | Python selector uses stable no-follow reads, permission/digest/freshness/device checks, bounded copy, pre/post identity, and explicit descriptor cleanup. | prevented for tested races/failures; `T-RESTORE` | Hardware returning silent corruption after both digest checks is not independently detected until full restore/semantic checks. |
| F-17 | A Pod claims a required component label while running the wrong image, required replica counts are only partially present, or a replacement/restart is hidden. | Sentinel requires exact component counts, complete container identity, release/image match, Pod UID/restart continuity, and named failed checks. Auxiliary mutating Pods are separately classified. | detected only/fail-closed; `T-SENTINEL`, `T-PG` | Sentinel does not stop or replace the Pod. A compromised Kubernetes API can lie to the sentinel. |
| F-18 | Docker manifest digest is treated as identical to the runtime CRI image ID, causing false identity failures or false matches. | Release data carries an explicit runtime `app-image-id`; sentinel compares every running application container image ID to that recorded runtime identity. | fail-closed in structure/identity tests; `T-SENTINEL`, `T-PG` | Populating the correct CRI ID is a deployment step and must be verified on the actual node. |
| F-19 | k3s is stopped; an in-cluster monitor cannot report it or automatic restart creates an unsafe control loop. | A host systemd sentinel runs outside k3s, skips kubectl when k3s is inactive, records bad, and has no restart action or notification/runtime credential. | detected only; `T-SENTINEL`, `T-PG` | If the host/systemd itself is down, no local sentinel runs. External/off-host monitoring is required for that failure. |
| F-20 | Cluster downtime misses backup/restore/retention schedules, or catch-up runs them in an unsafe order. | CronJobs use `Forbid` and bounded missed-start deadlines. Every destructive retention job independently revalidates backup/device/restore evidence, so schedule order is not a safety precondition. | mitigated/fail-closed | Downtime beyond the catch-up window needs operator execution. This design does not guarantee RPO during prolonged cluster outage. |

## Privately observed failures that motivated design changes

The following statements come from the reviewed private operational record.
Raw private logs are not included here, so this public repository does not
independently prove the original occurrence. It does contain the resulting code
and regression tests.

| Observation | Resulting design | Publicly verifiable portion | Public evidence gap |
| --- | --- | --- | --- |
| A short runtime stop/self-recovery completed between polling samples; current was good before and after. | Sanitized completed lifecycle-edge projection and recovery-only incident path. | `T-LIFECYCLE`, `T-PIPELINE` | The original raw lifecycle log and exact host timeline are private. |
| Re-reading one lifecycle edge with changing current snapshots produced duplicate recovery transitions/intents in isolated state. No real provider delivery was enabled. | Event-scoped transition identity plus existing episode/phase dedupe. | `T-LIFECYCLE` | The original private database rows are not published. |
| Reporter aggregation of wide historical rows caused PostgreSQL temporary-file growth. | Revision/time filtering in SQL, narrow columns, no wide sort, streaming accumulator, explicit repository close. | `T-REPORT` | The original `pg_stat_database` counters and query plan are private. |
| First deployment of restore/report/backup evidence used permissions unreadable by the capability-free sentinel. | Operator-group read-only file modes and bounded directory modes, with permission tests. | `T-REPORT`, `T-SENTINEL`, `T-PG` | Actual host ownership is not reproduced by the public unit suite. |
| Docker/local image identity and CRI-reported runtime image identity were not interchangeable. | Explicit runtime image ID in release data and per-container sentinel comparison. | `T-SENTINEL`, `T-PG` | The original private CRI outputs are not published. |

## Known unresolved or not-yet-proven failure modes

These are intentionally not described as fixed.

| ID | Unknown or unresolved boundary | Current behavior | Evidence needed before a stronger claim |
| --- | --- | --- | --- |
| U-01 | Real Discord/Slack acceptance, idempotency, receipt lookup, and crash-window behavior. | No real provider or credential is implemented; delivery remains isolated. | Provider-specific integration, idempotency contract, receipt reconciliation, and production cutover soak. |
| U-02 | Active-active core or PostgreSQL high availability. | One core writer and one PostgreSQL replica are intentional. | Multi-node design, fencing valid for every write, replication/failover tests, split-brain handling, and RPO/RTO decision. |
| U-03 | Arena host, power, theft, or site loss. | Same-host different-filesystem dumps mitigate one local device failure only. | Authenticated off-host copy or another node/site, restore drill, retention policy, and measured RPO/RTO. |
| U-04 | Runtime/node failure before the source event ledger is written. | Polling/current and lifecycle projection may both miss the direct termination cause. | Independent Pod exit/FFmpeg stderr/event preservation outside the failed process and correlated replay. |
| U-05 | Uninterruptible kernel/storage I/O and hardware that acknowledges writes without durable media persistence. | Timeouts, liveness, sync, digests, and restore checks reduce impact but cannot repair the device. | Hardware/power-loss fault injection and storage-specific durability evidence. |
| U-06 | Future database growth beyond restore `emptyDir`, memory, time, or candidate limits. | Restore/retention fail closed when bounds are exceeded. | Trend data, worst-case restore benchmark, revised resource limits, and repeated restore evidence. |
| U-07 | Completion of the R2 seven-day and R3 fourteen-day unchanged-revision gates. | The recorded private checkpoint only started a new soak; elapsed completion is not claimed here. | Fixed-end-time revision-pinned reports after the minimum windows plus Pod/DB/backup/sentinel continuity evidence. |
| U-08 | Correctness of arbitrary out-of-tree Kubernetes overlays, host units, mounts, groups, or manual operator actions. | Public examples and validators cover this repository only. | Rendered deployment diff, server-side validation, live unit/config inspection, and post-deploy evidence. |
| U-09 | Compromise of registry, builder, Kubernetes API, host root, or an allowlisted producer that emits well-typed false data. | Digest/hash pins and least privilege reduce reach; they do not establish end-to-end trust. | Supply-chain signing/attestation, independent evidence sources, host security controls, and threat-model review. |
| U-10 | R6 public-source authority cutover, R7 typed Dell execution, and R8 retirement of legacy authority. | Not implemented or authorized by this milestone. | Versioned command contract, executor allowlist/fencing, rollback gates, production soak, and explicit authority transfer decision. |

## How to maintain this register

For every future fix:

1. add or update one failure-mode ID instead of claiming “all bugs are fixed”;
2. state whether the result is prevention, fail-closed behavior,
   reconciliation, mitigation, or detection only;
3. link the exact source/test/operational evidence;
4. state what the evidence does not prove;
5. never convert a current private deployment observation into a public-source
   or elapsed-soak claim;
6. add a new `U-*` item when the correct behavior or required evidence is not
   yet known.
