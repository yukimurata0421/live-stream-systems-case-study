# Chaos Testing Draft

Recorded: 2026-09-10 JST

Status: repository-only draft. The current runners are deterministic isolated
source validation, not production chaos, deployment evidence, elapsed soak, or
viewer-recovery proof.

## Model

Failure injection checks one named fault. Chaos testing checks whether the same
invariants survive bounded combinations, orders, and recurrences after the
individual contracts are understood.

```text
versioned dimensions + mandatory rows + fixed seed
  -> bounded cases -> invariant checks -> coverage report
  -> minimized deterministic regression for each confirmed defect
```

## Current Campaigns

| Campaign | Dimensions | Boundary |
| --- | --- | --- |
| Inter-server resilience | two transport hops, recovery order, payload, resource, credential, clock, identity, commit stage | Pure classification/model behavior; no production network. |
| Recovery and projection | DNS, TCP, TLS, HTTP, security, storage, attempt and elapsed budgets | No observation-plane action capability or physical effect. |
| Dell failure domain | central-link and target-network loss, unknown outcomes, journal gaps, local lifecycle | Temporary files, loopback sockets, and test-owned objects. |
| Invariant lattice | authority, target, fencing, evidence, reconciliation, failure order | Fake or isolated effect boundary. |
| SQLite and concurrency | locked stores, process contention, crash windows, restart replay | Disposable databases under the pinned SQLite runtime. |
| Soak parity and blockers | convergence, late/wrong-source proof, drift, blocker cardinality | Time-compressed model, not elapsed live time. |
| Owner and reconciliation | process identity, scheduler state, freshness, delayed outcomes, retirement | Test-owned processes and no production command delivery. |

Runners and tests are visible under `src/cra_harness/`, `tools/run_*chaos.py`,
and `tests/harness/`. Their presence does not prove every campaign was executed
for the current checkout.

## Coverage And Invariants

Campaigns combine mandatory high-risk rows, selected cross-hop pairs, covering
arrays, weighted randomized cases, and negative controls. Reports distinguish
visited values and predicates from the full state space; unvisited cells are not
passes.

Core invariants include:

- no effect before durable authorization;
- at most one attempt for one logical generation and exact target;
- no automatic retry after `OUTCOME_UNKNOWN`;
- no authorization or verification from stale or misbound evidence;
- no simultaneous central and local-fallback authority;
- append-only, exactly-once reconciliation; and
- no false recovery from Pod readiness or YouTube state alone.

Safe blocking, unknown, exhaustion, or reconciliation may be the expected
passing result for a scenario.

## Reproduction And Limits

Each run records source identity, seed, dimensions, mandatory coverage,
classifications, bounded failure samples, and a digest. A confirmed defect is
preserved, minimized, replayed outside the broad campaign, classified by owner,
and fixed with a deterministic pre-patch-red regression.

Large artifacts, databases, and logs stay outside Git. Stop on an untriggered
fault, oracle gap, missing terminal evidence, release drift, failed cleanup,
artifact-integrity failure, or possible production contact.

Deterministic chaos cannot prove real kernel, storage-device, LAN, ISP,
external-provider, credential, or viewer behavior. Time-compressed chaos does
not count as soak time. The common trust and stop contract is in the
[harness engineering draft](harness-engineering-draft.md).
