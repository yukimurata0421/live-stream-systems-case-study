# Harness Engineering Draft

Recorded: 2026-09-10 JST

Status: repository-only draft. It describes implemented public test surfaces,
not a deployment, production action, or completed soak.

## Goal

The recovery path crosses Monitoring, central authority, a Dell Agent, and an
exact FFmpeg-child effect boundary. The harness must prove both SUT behavior and
the integrity of the test that observed it.

```text
scenario -> injector -> SUT -> facts-only observer -> append-only evidence
                                  |
invariants -----------------------+-> independent oracle -> classification
```

Implementation lives under `src/cra_harness/`, stable scenarios under
`harness/scenarios/`, and verification under `tests/harness/`.

## Trust Model

| Role | Responsibility | Boundary |
| --- | --- | --- |
| SUT | Execute authority, protocol, persistence, reconciliation, and fake-effect behavior. | Does not decide its own correctness. |
| Injector | Arm and trigger one declared test fault. | An untriggered fault is not coverage. |
| Observer | Record identities, events, counters, timestamps, and exits. | Does not decide PASS. |
| Oracle | Evaluate immutable invariants against evidence. | Does not import SUT decision logic as its answer key. |
| Trust gate | Join oracle, evidence, isolation, and negative-control results. | Rejects partial or ambiguous success. |

## Evidence And Classification

Every event binds its run, scenario, producer, sequence, event identity, and
observation time. A frozen run rejects later append, gaps, duplicates, incorrect
bindings, and out-of-window evidence.

The classifier keeps these outcomes distinct:

- `PASS` and `EXPECTED_INJECTED_FAILURE`;
- `SUT_FAILURE`;
- `HARNESS_FAILURE` and `ENVIRONMENT_FAILURE`;
- `MISSING_EVIDENCE` and `UNKNOWN_AMBIGUOUS`;
- `UNSUPPORTED_CONDITION`; and
- `SAFETY_GATE_FAILURE`.

An exception alone does not identify the owner. Missing evidence remains
missing and cannot be reconstructed from a later current-state snapshot.

## Verification Layers

| Layer | Public examples | What it establishes |
| --- | --- | --- |
| Deterministic protocol | `protocol_v1.json`, `compound_v2.json`, `e2e_v1.json` | Fixed normal, duplicate, crash-window, and reconciliation semantics. |
| Negative controls | mutation controls and `tests/harness/negative_control/` | The oracle detects intentionally forbidden outcomes. |
| State exploration | operational models, randomized runners, covering arrays | Bounded combinations with explicit unvisited coverage. |
| Disposable integration | SQLite, loopback mTLS, child lifecycle, partial I/O | Actual code behavior without production access. |
| Release and soak gates | release-boundary tests and no-action evaluators | Candidate identity and evidence eligibility, not production authority. |

No layer substitutes for another. Local harness success does not prove a real
network partition, credential path, physical FFmpeg effect, deployed revision,
or viewer recovery.

## Isolation, Promotion, And Stop Rules

The fake-only gate requires the exact fake adapter and zero production signal,
process, Pod, Deployment, network, kubeconfig, credential, and restart counts.
Loopback, child-process, and disposable-storage tests are declared as separate
test populations.

An exploratory case becomes a regression only after its seed, minimal input,
fault, invariant, source identity, and pre-patch failure are fixed. Freeze a
family only when injection, evidence, oracle, negative controls, cleanup,
targeted tests, and the full source gate agree.

Stop on an untriggered fault, oracle gap, missing terminal evidence, uncertain
artifact identity, failed cleanup, environment drift, or scope expansion.
Preserve and classify the evidence before changing the SUT.

See [failure injection](failure-injection-draft.md),
[chaos testing](chaos-testing-draft.md), and the concise
[harness trust boundary](harness-trust.md).
