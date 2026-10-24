# Local Reflex Operations

The [local reflex contract](local-reflex-authority.md) owns what the path may
do. This page is the single procedure for inspecting, evaluating, switching and
stopping it. It grants no approval for a production change. The current
operation keeps `dry_run`, keeps the existing fast-recovery controller in
charge, and holds back the candidate runtime image.

Paths below are those of the public examples in
[`ops/systemd/`](../ops/systemd/) and [`policy/`](../policy/). A real deployment
substitutes its own values.

## 1. Check the current state

1. Confirm the target hosts, source, configuration and service user from each
   host's loaded services, not from a checkout.
2. On Dell, read `/run/cra-agent/status.json` and the raw observation: policy,
   target generation, freshness and the reason for the latest decision.
3. Compare the actual delivery destination with the observed TCP metrics and the
   per-route `ingest_endpoints`. No extra port probe is needed for a no-action
   decision.
4. Check the existing controller's timer and authority, the delivery Pod and
   FFmpeg, and send and acknowledgement progress. An active unit or a PID alone
   is not proof of healthy delivery.
5. Compare the agent's local and acknowledged sequence with cra-01's contiguous
   high-water mark, gaps and conflicts. Unobserved or empty audit is not a pass.

The running release is the target of `/opt/cra/current`; do not infer it from a
checkout HEAD. Do not reinitialize existing keys, databases or configuration.

## 2. Evaluate `dry_run`

For one source, configuration, policy and observation window, compare actual
stalls with `WOULD_RESTART` decisions. Separate missing observations, network
loss, unreachable ingest, self-recovery and actions by the existing controller.
Do not mix statistics from before and after a fix, or sequences from different
generations or instances. Elapsed days alone are not a reason to enable
`enforce`. Audit sync problems are recorded apart from execution rights and
budget.

## 3. Verify a change to the minimal path

The minimal path is implemented and verified in isolation against the existing
runtime. Later changes keep these checks and keep isolated tests on an old image
apart from production deployment:

- Target the current runtime and v2 requests; do not require v4 binding or
  successor evidence, but do check route, route generation and Executor.
- Reconcile asynchronous outcomes through the existing status query. Where an
  older reply lacks a field, keep the target check consistent with live
  observation, request and fence; do not widen acceptance by dropping it.
- Keep agent and central evaluation consistent: execution result and later
  delivery progress are separate, and no successor event chain is required.
- Cover stop file and policy changes, slow probes, expiry, missing observations,
  generation changes, rejection, lost replies, unknown outcomes and limits that
  survive restart.
- Use a real older Executor, existing controller history, a query of the same
  request and no second attempt, in isolation. Never inject faults into the
  production runtime or production destinations, and never use production keys,
  databases or destinations in an isolated test.

To deploy a change in `dry_run`, build a fixed release that matches the verified
source and keep existing configuration, keys, databases and units. A release
built from uncommitted source records its content hash and base commit. Switch
cra-01 first, then Dell, restarting each service, and check the policy signature
and mode, observation freshness, new audit sequences and signed
acknowledgements, cra-01 gaps, conflicts and violations, delivery, and the
existing controller. On failure, return to the previous release and restart only
that service; databases, consumed budget and audit watermarks are not rolled
back.

## 4. Switching to `enforce`

The switch needs the step 3 verification and deployment, the contract's
conditions, the hosts' responsibility assignment and a rollback plan to agree,
and it is a separate explicit approval. The delivery host's operating contract
still assigns restart decisions to the three-host design; four of its
responsibility and prohibition entries must be revised in the same approval so
that the agent's local decision under a signed policy is allowed. cra-01's
ownership of policy, budget and verification does not change.

Before the switch, the [stall replay](local-reflex-records.md#stall-criterion-replay)
must support the current criterion. Then, without changing the delivery Pod:

1. Stop the existing controller's timer and wait for its running cycle to end.
2. `tools/executor_authority.py show`: confirm the authority and zero unresolved
   fences. It opens the ledger read-only.
3. `tools/executor_authority.py switch --expect independent-controller:2 --to cra-agent:3`.
   The ledger refuses an unexpected authority or any unresolved fence.
4. Point the agent's `executor_socket` at the real socket and restart the agent.
5. Publish a signed policy with a higher version in `enforce`, and confirm the
   status shows no `NOT_EXECUTOR_AUTHORITY`.

The ledger is `runtime/fast_recovery_effects.sqlite3` on the delivery Pod's
persistent volume. `--runtime-src` takes the same `runtime_boundary` source as
the running image (it needs `rfc8785`). The tool does not call the ledger
constructor, so it causes no migration or backfill writes.

Roll back in reverse order: policy to `off`, confirm the agent sends no
requests, `switch --expect cra-agent:3 --to independent-controller:2`, restart
the existing timer. While an unknown scope remains, neither direction switches;
reconcile it first. The existing and new controller must never execute at the
same time.

## 5. Stop and roll back

To stop new requests, create `/etc/cra-agent/STOP` on Dell or publish a policy
in `off`. A request already sent is not cancelled, and the stop file does not
stop the existing controller.

Roll back agent or central only after confirming the previous release exists and
is compatible. From a release with signed acknowledgements, roll back the agent
before cra-01. Rolling back a release does not roll back databases, consumed
budget, unknown outcomes or audit watermarks. To return authority to the
existing controller, stop new requests, reconcile unresolved outcomes, then hand
over.

The existing controller's retirement-evidence correction stays applied. If it
must be removed, remove only its single drop-in, reload systemd, and confirm the
original source in the next natural cycle; other drop-ins, the timer, ledger and
budget stay, and the delivery Pod is not restarted. Production deployment of the
candidate runtime image remains on hold, and its earlier plan is not the next
step of this procedure.

## Development checks

From `recovery-control/`, the reflex tests are under `tests/reflex/` and run in
the public suite described in the [README](../README.md#local-validation). The
CRA package is also checked directly:

```bash
.venv/bin/python -m pytest -q tests/reflex
.venv/bin/ruff check src/cra tests/reflex
.venv/bin/mypy src/cra
```

Tests specific to the candidate runtime may be skipped in an ordinary run.
Name the targets needed for the current runtime and v2, and do not read a run
with skips as proof that every path works.
