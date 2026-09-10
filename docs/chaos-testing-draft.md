# Chaos Testing Draft

Recorded: 2026-09-10 JST

Status: current repository-only draft. The public source contains deterministic,
seeded chaos runners, but their results are isolated source validation. They are
not production chaos, deployment evidence, elapsed soak, or proof of viewer
recovery.

## Definition

Failure injection asks whether one named fault is contained. Chaos testing asks
whether the same invariants survive many bounded combinations, orders, and
recurrences after the individual failure contracts are understood.

Chaos in this repository is deterministic and finite:

```text
versioned dimensions + mandatory rows + fixed seed
  -> bounded generated cases
  -> invariant checks and classification
  -> coverage report
  -> minimized deterministic regression for every real defect
```

It is not permission to disrupt a running host or network.

## Current Campaign Surfaces

| Campaign | Dimensions explored | Safety boundary |
| --- | --- | --- |
| Inter-server resilience | Dell-to-arena and arena-to-CRA transport classes, recovery order, payload shape, resource state, credential overlap, clock movement, identity, and commit stage | Pure classification and model evaluation; no production network. |
| Recovery episode | retryable and non-retryable DNS, TCP, network, TLS, HTTP, security, contract, and storage faults with attempt and elapsed budgets | Observation plane retains zero control capability and physical effects. |
| Projection transport | transient exhaustion/recovery, resets, TLS EOF/timeout, DNS/HTTP classes, certificate, content type, and oversize payloads | Bounded synthetic transport behavior. |
| Dell failure domain | central-link loss, target-network loss, ambiguous outcomes, signed-journal gaps, loopback TCP, and local lifecycle candidates | Temporary files, controlled loopback sockets, and test-owned lifecycle objects. |
| Dell invariant lattice | authority, target, fencing, evidence, reconciliation, and failure-order combinations | Fake or isolated effect boundary; no live FFmpeg. |
| SQLite and concurrency | busy/locked stores, process contention, crash windows, and restart replay | Disposable databases under the pinned SQLite runtime. |
| Soak parity and blockers | convergence, pending/timeout/late/wrong-source proof, policy drift, cycle mutation, and blocker cardinality | Time-compressed model; not elapsed live time. |
| Owner observation | process exit, command-line identity, scheduler state, evidence freshness, and collection semantics | Test-owned processes and fixtures. |
| Reconciliation and retirement | delayed outcomes, return of central authority, target retirement, and exactly-once handback | No production command delivery. |

The runners and tests are visible under `src/cra_harness/`, `tools/run_*chaos.py`,
and `tests/harness/`. Their presence proves reviewable implementation, not that
every campaign was run for the current checkout or deployed release.

## Coverage Model

Exhaustive cross-products quickly become misleading or infeasible. Campaigns
therefore combine:

- mandatory rows for every named high-risk fault and selected cross-hop pairs;
- covering arrays for interactions among the remaining dimensions;
- weighted randomized cases for operationally important states;
- negative controls that must be detected; and
- explicit coverage reports for visited values, pairs, predicates, and cells.

Unvisited cells remain unvisited. A campaign must not infer full state-space
coverage from a large case count. If a mandatory predicate misses its minimum
hit count, the trust gate stays closed.

## Campaign Invariants

Every campaign preserves the system-level invariants relevant to its scope:

- no effect before durable central authorization;
- at most one physical attempt for one logical generation and exact target;
- `OUTCOME_UNKNOWN` blocks automatic retry;
- stale, conflicted, or incorrectly bound evidence cannot authorize or verify;
- the monitoring plane cannot gain command authority;
- central and local fallback authority cannot both be active;
- delayed handback reconciles append-only and exactly once;
- recovery verification needs independent delivery evidence, not Pod readiness
  or YouTube state alone;
- production-isolation counters remain inside the declared test population; and
- campaign cleanup leaves no test-owned target behind.

A chaos case may end in safe blocking, unknown, exhaustion, or reconciliation
and still pass if that is the immutable expected outcome.

## Reproducibility And Artifacts

Each run records its schema, source identity, seed, case count, dimensions,
mandatory coverage, failure classifications, bounded sample of failing cases,
and digest. Reusing or overwriting an immutable run identity is rejected.

For every apparent SUT defect:

1. preserve the original case and run metadata;
2. minimize the sequence without changing the independent invariant;
3. replay it outside the broad campaign;
4. determine whether the failure belongs to the SUT, harness, oracle,
   environment, evidence, or safety gate;
5. add a deterministic regression that fails before the repair; and
6. rerun the relevant campaign and full source gate without rewriting the
   original artifact.

Large generated artifacts, databases, logs, and duplicate runs stay outside the
public Git tree. Only small reviewed schemas, scenarios, fixtures, and freeze
summaries belong in the snapshot.

## Budgets And Stop Conditions

Every runner has fixed case, time, attempt, output, process, and storage bounds.
Do not expand those bounds mid-run to chase a desired green result.

Stop on an untriggered fault, oracle gap, missing terminal evidence, source or
release drift, unexplained cross-domain behavior, cleanup failure, artifact
integrity failure, or any sign that a production target could be touched. A
stopped campaign is evidence to classify, not a reason to weaken the invariant.

## Interpretation Limits

Deterministic chaos can expose ordering, persistence, identity, retry, and
classification defects quickly. It cannot reproduce every kernel scheduler,
storage device, switch, ISP, TLS implementation, external provider, or viewer
behavior. Time-compressed soak chaos does not count as elapsed soak time, and a
closed-network container does not prove a real multi-host partition.

Production deployment, real-fault execution, and final recovery verification
remain separately authorized evidence domains. See the
[failure injection draft](failure-injection-draft.md) and
[harness engineering draft](harness-engineering-draft.md) for the prerequisite
contracts.
