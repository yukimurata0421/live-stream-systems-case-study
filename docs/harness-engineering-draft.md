# Harness Engineering Draft

Recorded: 2026-09-10 JST

Status: current repository-only draft. It documents implemented public test
surfaces and their intended trust model. It is not production authorization,
deployment evidence, or a formal soak result.

## Objective

The recovery path crosses monitoring, central authority, a Dell Agent, and an
exact FFmpeg-child effect boundary. A useful test must show more than an
expected final state: it must prove which fault occurred, which component acted,
which evidence was preserved, and whether the test system itself remained
trustworthy.

```text
immutable scenario and invariants
  -> bounded injector
  -> SUT
  -> facts-only observer
  -> append-only evidence
  -> independent oracle
  -> classification and trust gate
```

The public implementation is under `src/cra_harness/`, its stable scenario
catalogs are under `harness/scenarios/`, and its unit, integration, chaos,
release-boundary, and negative-control tests are under `tests/`.

## Trust Roles

| Role | Responsibility | Must not do |
| --- | --- | --- |
| SUT | Execute the authority, protocol, Dell Agent, persistence, reconciliation, and fake effect behavior being tested. | Decide whether its own behavior is correct. |
| Injector | Arm and trigger the exact scenario fault through an explicit test adapter. | Treat a requested but untriggered fault as coverage. |
| Observer | Record rows, messages, counters, identities, timestamps, and exits. | Calculate the final PASS verdict. |
| Oracle | Evaluate immutable invariants against evidence without importing the SUT transition or decision implementation as the answer key. | Weaken an invariant to accommodate observed output. |
| Classifier | Keep application, harness, evidence, environmental, and safety outcomes distinct. | Convert an infrastructure or oracle gap into an SUT defect. |
| Trust gate | Require evidence, isolation, negative controls, and artifact integrity to pass together. | Accept a partial green result as a trusted run. |

## Result Taxonomy

The current classifier supports:

- `PASS`: the scenario completed and all required invariants held;
- `SUT_FAILURE`: complete evidence and an independent oracle show a contract violation;
- `HARNESS_FAILURE`: injection, observation, generation, cleanup, or artifact handling failed;
- `MISSING_EVIDENCE`: a required event or field is absent;
- `UNKNOWN_AMBIGUOUS`: ordering, ownership, or outcome cannot be resolved safely;
- `EXPECTED_INJECTED_FAILURE`: a dependency failed and the SUT contained it as designed;
- `UNSUPPORTED_CONDITION`: the scenario is outside the declared contract;
- `ENVIRONMENT_FAILURE`: the disposable test environment is not valid;
- `SAFETY_GATE_FAILURE`: production isolation or another mandatory safety control failed.

A Python exception alone does not identify an SUT failure. The classifier must
first establish whether the SUT, injector, oracle, observer, environment, or
test assertion failed.

## Evidence Contract

Every evidence event binds at least a run ID, scenario ID, producer, monotonic
sequence, unique event ID, and observation time. Freezing a run rejects later
append, sequence gaps, duplicate event IDs, cross-run or cross-scenario records,
and out-of-window timestamps.

The minimum trusted bundle contains:

- immutable scenario inputs and expected invariants;
- fault lifecycle evidence;
- pre-state, transition, command, effect, post-state, and terminal evidence
  applicable to the scenario;
- exact target and release identities where the contract requires them;
- oracle and classifier output;
- production-isolation counters;
- negative-control detection results; and
- hashes or a manifest that bind the reviewed artifacts.

Missing evidence remains missing. A current-state snapshot must not be used to
invent an earlier transition, command receipt, or physical attempt.

## Verification Layers

| Layer | Purpose | Examples in this repository |
| --- | --- | --- |
| Deterministic protocol | Fix normal, duplicate, stale, crash-window, and reconciliation semantics. | `harness/scenarios/protocol_v1.json`, `compound_v2.json`, `e2e_v1.json` |
| Negative controls | Prove the oracle catches forbidden outcomes. | mutation controls and `tests/harness/negative_control/` |
| Operational state exploration | Exercise bounded combinations while preserving mandatory high-risk coverage. | v2/v3 runners, covering arrays, operational models |
| Real code with disposable resources | Test actual parsing, SQLite, loopback mTLS, child lifecycle, and short-write behavior without production access. | postmortem I/O, transport, concurrency, and runtime-boundary tests |
| Chaos campaigns | Explore ordering and compound faults after deterministic contracts are fixed. | inter-server, Dell, projection, parity, reconciliation, and owner campaigns |
| Release and soak gates | Bind a candidate to exact source, artifacts, and elapsed evidence. | release-boundary tests and no-action soak gates |

These layers do not substitute for each other. In particular, local harness
success does not prove a real network partition, production credential path,
physical FFmpeg effect, deployed revision, or viewer recovery.

## Negative Controls And Isolation

Negative controls intentionally construct forbidden states through a test-only
mutation adapter. The run is trusted only if the oracle detects every required
mutation while the production implementation remains unchanged.

The isolation gate requires the exact fake physical adapter and zero real
signal, process, Pod, Deployment, network, kubeconfig, credential-load, and
production-restart counts. An integration test that deliberately uses loopback
traffic or a disposable child belongs to a separately declared test population;
it cannot inherit a fake-only result silently.

## Regression, Repair, And Freeze

An exploratory failure may become a regression only after its seed, case index,
minimal inputs, injected fault, independent invariant, and source identity are
fixed. A repair requires a deterministic reproduction and a test that is red
before the patch and green after it.

Freeze a failure family only when the injector controls, actual-event evidence,
oracle, negative controls, artifact reproduction, targeted tests, and full
source-validation gate agree. New semantics or unrelated exploration belongs in
a new slice and must not rewrite the frozen result.

## Stop Conditions And Open Work

Stop when the fault did not trigger, the observer missed a required edge, the
oracle cannot decide, cleanup is unproven, the environment drifted, artifact
identity is uncertain, or the test would require broader authority. Preserve
the failure evidence before modifying the SUT.

Open work includes keeping scenario-to-oracle-to-evidence traceability complete,
binding every promoted postmortem case to a deterministic regression, and
retaining the strict separation between repository validation, no-action soak,
production deployment, and physical-effect verification.

See [failure injection](failure-injection-draft.md),
[chaos testing](chaos-testing-draft.md), and the concise
[harness trust boundary](harness-trust.md).
