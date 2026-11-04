# Scoped Recovery Authority

The current contract separates observation, signed policy, local evaluation and
the physical FFmpeg effect. The [CRA contract](../../recovery-control/docs/local-reflex-authority.md)
owns the local reflex rule; this page describes its Stream integration boundary.
No public checkout, test or example enables production effects.

## Responsibility split

| Role | Owns | Must not own |
| --- | --- | --- |
| arena-server Monitoring | observation, current state, incident/parity evidence, and facts-only projection | recovery authorization, command delivery, final recovery verdict, or process signaling |
| cra-01 CRA central | signed policy versions, limits, audit intake and verification | per-action approval, raw-source ownership or direct process signaling |
| Dell CRA agent | local evaluation of adopted policy, durable claims and limits, outcome queries | defining policy or sending requests in `dry_run` |
| Dell Stream Executor | signed local facts and one exact, fenced FFmpeg-child effect | incident/SLO or stall-policy decisions; escalation to container, Pod, Deployment or host restart |
| Raspberry Pi publisher | allowlisted static status publication | internal databases, credentials, incident judgment, or control feedback |

The target identity is wider than a PID: host, boot, namespace, Pod UID,
container, container ID, FFmpeg generation, and PID must agree. Unknown or
drifted identity fails closed. `OUTCOME_UNKNOWN` is reconciled; it is not an
automatic retry signal.

## Published integration status

The integrated [`recovery-control/`](../../recovery-control/README.md)
subproject contains the local agent, signed-policy central service and their
tests. The Stream tree owns the observer, protocol and physical Executor;
they are not installed as part of CRA. The operated agent remains `dry_run`.
The earlier central per-action authorization design is retained in
[history](../../recovery-control/docs/history.md). The reason for applying local
transaction boundaries and effect fencing is documented in
[`why-cra.md`](../../recovery-control/docs/why-cra.md).

The stream delivery tree now publishes the matching
`src/stream_core/runtime_boundary_entrypoint.py`, typed effect adapter, local
maintenance audit, and lifecycle/evidence integration. The runtime boundary
admits only an exact FFmpeg-child target, writes the fence before the effect,
and reconciles an uncertain result instead of converting uncertainty into a
second restart. `tests/test_fast_recovery_runtime_boundary.py` exercises this
integration against test-owned child processes and files.

This source integration does not enable the path. Public configuration retains
the production-disabled policy, and no checkout, unit test, or successful
Harness run is evidence of a live CRA deployment or mutation authority.

## Legacy migration surfaces

`ops/scripts/stream_v3_remote_recovery.py`,
`ops/scripts/stream_v3_scoped_recovery.py`, and the recovery action-plan
renderer remain in the public history for audit and migration review. They are
not the current authority contract:

- arena-server must not execute them as an autonomous recovery authority;
- an absent FFmpeg child must not escalate to a container or Deployment
  restart;
- `shadow_mode` must not be discarded when consuming an action plan; and
- broad Kubernetes permissions do not become safe merely because a helper has
  an action allowlist.

Their committed defaults remain fail closed and public CI does not install or
run them against a cluster. Production retirement or replacement of these
surfaces requires a separate host-verified deployment and rollback task.

## What the public repository proves

The repository supports code review and local tests of message integrity,
freshness, exact-target fencing, idempotency, reconciliation, bounded test-owned
child effects, and correlated FFmpeg exit evidence. It does not prove a live
CRA deployment, a completed soak, production mutation authorization, or an
external recovery outcome.
