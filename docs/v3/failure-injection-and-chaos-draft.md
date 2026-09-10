# Failure Injection And Chaos Testing Draft

Recorded: 2026-09-10 JST

Status: repository-only draft. This document does not authorize production
fault injection, a live rollout, or a YouTube lifecycle mutation.

## Purpose

`stream_v3` already has deterministic tests for many failure boundaries, but it
does not yet expose one uniform, independently judged chaos harness. This draft
turns the current development record into a public plan without relabeling unit
tests, historical incidents, or isolated drills as production chaos evidence.

The intended progression is:

```text
immutable scenario
  -> test-owned fault
  -> runtime or policy under test
  -> event and state evidence
  -> independent expected invariant
  -> classified result
  -> fixed regression
  -> separately authorized live gate, if one is still needed
```

The existing [failure taxonomy](failure-taxonomy.md) describes operational
faults. This document describes how those faults may be tested safely.

## Present Public Coverage

| Boundary | Current public mechanism | What it establishes |
| --- | --- | --- |
| Runtime startup and child lifecycle | `TEST_MODE=1`, temporary state, fake child processes, and `tests/test_stream_engine_wait_modes.py` | Startup, wait, cleanup, and child-state decisions without a YouTube send. |
| Connectivity-aware recovery | `tests/test_connectivity_recovery.py`, fake sockets, and deterministic policy inputs | Physical-connectivity classifications can suppress repeated child launch or broad recovery. |
| FFmpeg fast recovery | `tests/test_fast_recovery.py` and `src/watchers/fast_recovery_core/` | Budget, cooldown, evidence, and scoped action decisions. It does not execute a production recovery here. |
| YouTube identity and lifecycle | resolver, cache-freshness, selection, quota, and evidence-decision tests | Stale or conflicting evidence does not silently authorize broadcast replacement. |
| Rendering and precipitation | map contract, runtime probe, alert-rule, and browser-fixture tests | Readiness, last-known-good, and alert semantics for controlled inputs. |
| Audio and Pulse | Pulse bootstrap, audio signal, and transition tests | Route and grace-period decisions under synthetic state. |
| Shadow and public boundaries | shadow acceptance, manifest validation, and publisher tests | Planned actions, redaction, and non-mutating publication behavior. |

These are implemented test surfaces. They are not one completed chaos campaign,
and a green result does not prove viewer recovery or current production state.

## Failure Injection Draft

Each injected condition should have a stable scenario ID, an immutable expected
invariant, a bounded trigger, and explicit pre/post evidence. The initial matrix
is intentionally scoped to test-owned resources.

| Family | Candidate injections | Required invariant |
| --- | --- | --- |
| Process lifecycle | child exits before readiness, FFmpeg exit, delayed cleanup, duplicate child candidate | No orphan or duplicate owner; recovery stays inside the permitted child boundary. |
| Transport and connectivity | timeout, reset, refused socket, stale send sample, connectivity loss state | Retry and suppression follow the classified failure; transport noise never creates a new broadcast. |
| Evidence freshness | stale resolver cache, timestamp regression, missing report, source disagreement | Missing, stale, or conflicting evidence yields degraded or unknown, not false health or action authority. |
| Identity and lifecycle | unexpected video ID, transient candidate URL, incomplete OAuth evidence | The expected identity remains the gate; destructive lifecycle actions stay blocked. |
| Rendering and data | delayed map source, stale precipitation, capture unavailable, renderer not ready | The affected display domain is isolated from unrelated audio, transport, and YouTube actions. |
| Audio | missing Pulse route, low energy inside and outside transition grace, stale now-playing state | Audio repair is staged and cannot expand into broadcast replacement. |
| Persistence | interrupted atomic write, malformed JSON, partial state, sequence gap | Last-good or fail-closed behavior is explicit; incomplete evidence cannot be counted as success. |

Allowed mechanisms are temporary files, fake process tables, disposable child
processes, controlled loopback endpoints, test-only state machines, sanitized
fixtures, and `TEST_MODE=1`. A requested injection must be proven to have
triggered; otherwise the result is a harness failure, not an SUT pass.

## Chaos Testing Draft

Chaos testing begins only after the deterministic row for a failure is trusted.
The proposed campaigns combine bounded dimensions rather than randomly touching
a live system:

- ordering: duplicate, delayed, missing, or out-of-order observations;
- time: TTL edges, cooldown boundaries, transition grace, and clock movement;
- concurrency: overlapping observer writes, child exits, and recovery proposals;
- compound domains: transport plus stale evidence, rendering plus report loss,
  or audio degradation plus a healthy video path;
- resource pressure: bounded test-owned storage or process limits, with no host
  load test; and
- recovery sequences: fault, partial recovery, recurrence, and clean closeout.

Every randomized run must preserve its seed, case index, scenario inputs,
expected invariant, result classification, and minimized regression. Unexplored
combinations remain unexplored; they are not counted as passing cells.

## Result Classification

Results must keep these states distinct:

- `PASS`: the injected condition occurred and all independent invariants held;
- `SUT_FAILURE`: complete evidence shows a deterministic contract violation;
- `HARNESS_FAILURE`: injection, observation, cleanup, or artifact handling failed;
- `MISSING_EVIDENCE`: the required event or state was absent;
- `UNKNOWN_AMBIGUOUS`: ordering or ownership cannot be decided safely;
- `EXPECTED_INJECTED_FAILURE`: the dependency failed and the SUT contained it as designed.

An exception in the test runner, an untriggered fault, or an incomplete oracle
must never be promoted to an application defect or a pass.

## Production Isolation And Stop Conditions

This draft grants no permission to kill production processes, restart a Pod or
host, disconnect a cable, impair a real network, load credentials, change a
stream key, or mutate a YouTube broadcast. A future live drill needs its own
target identity, observation window, rollback, stop condition, and explicit
authorization.

Stop the campaign when root cause is ambiguous, the oracle cannot decide, the
fault did not trigger, cleanup is unproven, an unrelated failure domain changes,
or the test requires broader authority than its approved scenario. Preserve the
evidence and classify the gap before changing the SUT.

## Current Limits

- The public repository has component and policy tests, not a unified V3 chaos
  runner with an independent oracle and append-only run artifact.
- Synthetic transport and process results do not prove real LAN, kernel, GPU,
  RTMPS, YouTube, or viewer behavior.
- Historical operational observations are inputs for scenario design, not
  repeatable injected results.
- Long-window SLI review and the documented 24-hour production smoke gate remain
  separate from harness success.
