# Failure Injection Draft

Recorded: 2026-09-10 JST

Status: current repository-only draft. The catalog and injectors described here
are test-only. This document does not authorize a production process signal,
network impairment, credential use, Pod or host restart, or physical FFmpeg
effect.

## Definition

Failure injection introduces one named, bounded condition at a known boundary
to test an explicit invariant. It differs from merely mocking a return value:
the run must prove that the fault was armed and triggered, observe the SUT
response, and classify incomplete instrumentation separately.

Every fault follows the same lifecycle:

```text
requested -> armed -> triggered -> completed
```

Skipping a step, triggering a different fault, or failing to prove cleanup is a
`HARNESS_FAILURE`. A non-triggered fault is not evidence that the SUT tolerated
it.

## Current Catalog Families

The public catalog spans the following boundaries:

| Family | Representative conditions | Invariant under test |
| --- | --- | --- |
| Central crash windows | before commit, after commit before send, after send before receipt | No effect before durable authorization; uncertain delivery is not retried blindly. |
| Dell crash windows | after accepted commit, after execution-started commit, after fake effect before status | Durable state and exact-target fences prevent a second physical attempt. |
| Transport | dropped heartbeat or response, timeout, reset, partial read, refusal, DNS/TLS/HTTP classes | Retryability is classified, bounded, and unable to bypass authority. |
| Identity and ordering | stale target, sequence gap/regression, duplicate delivery, source conflict, release overlap | Stale or conflicting identity fails closed; duplicates remain idempotent. |
| Persistence | SQLite write failure, short write, read-only or full store, interrupted append/state publication | No false durable success; ambiguity remains explicit and recoverable by reconciliation. |
| Restart and restore | Agent restart, CRA restart, restore with pending work | Durable state is replayed once without reviving an already attempted effect. |
| Recovery verification | stale evidence, TCP still stalled, Pod-ready-only, YouTube-only, unexpected target | A weak signal cannot produce a false `RECOVERED` verdict. |
| Semantic negative controls | duplicate effect, stale-target acceptance, effect before durable accept, dual authority, retry after unknown | The independent oracle detects each intentionally forbidden state. |
| Postmortem-derived I/O | malformed input, partial HTTP body, response release, append/state boundary, short write, FIFO or child behavior | A historical failure hypothesis becomes a reproducible regression without importing private evidence. |

Scenario expectations are versioned in JSON rather than reconstructed from a
prompt or the current implementation. Examples include
`harness/scenarios/protocol_v1.json`, `compound_v2.json`,
`recovery_verification_v1.json`, and `postmortem_io_v1.json`.

## Injection Mechanisms

Use the smallest mechanism that reaches the actual boundary:

- explicit fake physical adapters for authority and effect semantics;
- injected exceptions for transport-classification contracts;
- test-owned temporary SQLite databases and files for persistence boundaries;
- controlled loopback mTLS endpoints for real parsing and socket lifecycle;
- disposable child processes for owner, exit, and cleanup behavior;
- immutable malformed or partial payload fixtures; and
- broken mutation adapters for negative controls.

Each test population must declare whether it is fake-only, loopback, process,
or disposable-storage integration. A loopback/process test must not be reported
under the fake-only claim that all network and process counters are zero.

## Scenario Record

A promotable scenario records:

- stable scenario and fault IDs;
- source case or design risk, without private raw data;
- SUT boundary and exact injection point;
- immutable inputs and expected invariant;
- requested, armed, triggered, and completed evidence;
- pre-state and post-state identities;
- bounded time, attempt, and resource budgets;
- expected and actual result classifications;
- cleanup evidence;
- source revision, seed, and artifact digest; and
- explicit unproven scope.

Postmortem coverage shows that a failure mechanism was represented. It does not
prove the historical root cause, recurrence probability, or production fix by
itself.

## Expected Outcomes

An injected dependency failure may correctly end as retry-and-recover,
retry-budget exhausted, safe blocked, `OUTCOME_UNKNOWN`, reconciliation
required, or expected injected failure. The test passes when that outcome and
all safety invariants match the scenario—not merely when the command exits zero.

The following are never SUT passes:

- injector requested but not triggered;
- observer or artifact writer failure;
- missing pre/post or terminal evidence;
- an oracle exception or an expectation derived from SUT output;
- an uncleaned disposable process or store; or
- a production-isolation counter above its declared population boundary.

## Safety Boundary

Public CI and routine local runs must not read production credentials, target a
production address, signal a real process, use a production kubeconfig, mutate a
Pod or Deployment, restart a host, impair the LAN, or exercise a real stream
effect. Test output belongs outside Git unless a small sanitized fixture or
freeze is intentionally reviewed.

A future physical or cross-host drill needs separate authorization specifying
the target host, component, failure domain, running release, observation window,
rollback, and stop condition. Repository source and a green injection suite are
not that authorization.

## Promotion And Stop Rules

Promote an exploratory case only after minimization and deterministic replay.
Stop when fault identity, target identity, event ordering, oracle expectation,
artifact completeness, cleanup, or production isolation is uncertain. Record
the gap as harness, evidence, environment, safety, or unknown before deciding
whether application code should change.

See the [harness engineering draft](harness-engineering-draft.md) for trust and
freeze gates and the [chaos testing draft](chaos-testing-draft.md) for bounded
combinations of already understood faults.
