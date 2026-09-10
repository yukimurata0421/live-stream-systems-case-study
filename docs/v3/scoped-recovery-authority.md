# Scoped Recovery Authority

The current cross-host contract separates observation, authorization, and the
physical FFmpeg effect. No public checkout, test, or example enables this path.

## Responsibility split

| Role | Owns | Must not own |
| --- | --- | --- |
| arena-server Monitoring | observation, current state, incident/parity evidence, and facts-only projection | recovery authorization, command delivery, final recovery verdict, or process signaling |
| cra-01 Central Restart Authority | recovery policy, budget, cooldown, authorization, command lifecycle, and final verification | raw-source ownership or direct process signaling |
| Dell delivery runtime | signed local facts and one exact, fenced FFmpeg-child effect | incident/SLO policy, action selection, or escalation to container, Pod, Deployment, or host restart |
| Raspberry Pi publisher | allowlisted static status publication | internal databases, credentials, incident judgment, or control feedback |

The target identity is wider than a PID: host, boot, namespace, Pod UID,
container, container ID, FFmpeg generation, and PID must agree. Unknown or
drifted identity fails closed. `OUTCOME_UNKNOWN` is reconciled; it is not an
automatic retry signal.

## Published integration status

The sibling public `stream_recovery_control` repository contains the CRA,
facts-only arena projection, Dell Agent, protocol schemas, durable fences, and
repository-only tests. Its published policy keeps production behavior disabled.

This V3 repository contains the delivery runtime and legacy recovery surfaces,
but it does not yet publish a self-contained runtime-boundary entrypoint that
can be enabled against that CRA package. The private integration candidate also
depends on uncommitted V3 lifecycle/evidence changes. Copying only the adapter
would import successfully in some test layouts while misrepresenting runtime
lifecycle state, so it is intentionally not included here.

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

## What the public repositories prove

The repositories support code review and local tests of message integrity,
freshness, exact-target fencing, idempotency, reconciliation, and bounded
test-owned child effects. They do not prove a live CRA deployment, a completed
soak, production mutation authorization, or an external recovery outcome.
