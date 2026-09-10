# Failure Injection Draft

Recorded: 2026-09-10 JST

Status: repository-only draft. These injections are test-only and do not
authorize a production signal, restart, network impairment, credential use, or
physical FFmpeg effect.

## Injection Contract

A failure injection introduces one named, bounded condition at a known boundary
and tests one immutable invariant. Every fault must complete this lifecycle:

```text
requested -> armed -> triggered -> completed
```

A skipped stage, wrong trigger, or unproven cleanup is `HARNESS_FAILURE`, not an
SUT pass. Scenario expectations are versioned in `harness/scenarios/` instead
of being derived from current SUT output.

## Current Families

| Family | Representative conditions | Expected containment |
| --- | --- | --- |
| Central crash windows | before commit, after commit before send, after send before receipt | No effect before durable authorization; uncertainty is not retried blindly. |
| Dell crash windows | after accept, after execution start, after fake effect | Durable exact-target fences prevent a second attempt. |
| Transport | heartbeat/response loss, timeout, reset, DNS, TLS, HTTP | Retryability is classified and bounded without bypassing authority. |
| Identity and ordering | stale target, sequence regression, duplicate delivery, source conflict | Stale or conflicting identity fails closed; duplicates remain idempotent. |
| Persistence | SQLite failure, short write, partial append or publication | False durability is rejected and ambiguity remains explicit. |
| Restart and restore | Agent or CRA restart with pending work | Durable state replays once without reviving an attempted effect. |
| Verification | stale evidence, TCP stalled, Pod-only or YouTube-only health | Weak evidence cannot create a false `RECOVERED` verdict. |
| Negative controls | duplicate effect, dual authority, unknown retry, early effect | The independent oracle detects each forbidden state. |
| Postmortem-derived I/O | malformed or partial input, response release, FIFO and child exit | A failure hypothesis becomes a reproducible test without private raw data. |

The main catalogs are `protocol_v1.json`, `compound_v2.json`,
`recovery_verification_v1.json`, and `postmortem_io_v1.json`.

## Safe Mechanisms And Evidence

Use the smallest mechanism that reaches the boundary: fake physical adapters,
injected transport exceptions, temporary SQLite/files, controlled loopback
mTLS, disposable child processes, malformed fixtures, and test-only mutation
adapters.

A promotable scenario records stable scenario/fault IDs, injection point,
immutable inputs and invariant, lifecycle evidence, pre/post identities, time
and attempt budgets, classification, cleanup, source revision, seed, artifact
digest, and unproven scope.

Postmortem coverage means the mechanism was represented. It does not prove the
historical root cause, recurrence rate, production deployment, or live repair.

## Boundary

Public CI must not read production credentials, target a production address,
signal a real process, use a production kubeconfig, mutate a Pod or Deployment,
restart a host, impair the LAN, or execute a real stream effect.

A physical or cross-host drill requires separate authorization naming the host,
component, failure domain, running release, observation window, rollback, and
stop condition. Classification, isolation, promotion, and stop rules are defined
once in the [harness engineering draft](harness-engineering-draft.md).
