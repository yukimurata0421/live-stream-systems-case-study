# Monitoring v4 Harness Engineering Draft

Recorded: 2026-09-10 JST

Status: current repository-only draft. The public tree contains implemented
isolated harness components and preserved evidence, but this document does not
claim deployment, production fault injection, notification delivery, recovery
authority, or a completed real soak.

## Objective

Monitoring v4 turns observations into current state, incidents, notification
intents, parity, and public-safe evidence. Fast implementation makes a serial
`change -> real soak -> late failure -> patch -> restart soak` loop too slow for
known failure families. The harness therefore moves reproducible failures into
short, isolated campaigns while retaining the real unchanged-revision soak as a
separate final gate.

```text
immutable evidence or scenario
  -> deterministic replay
  -> independent invariant
  -> bounded state exploration or injected dependency fault
  -> attributed action evidence
  -> fixed regression and freeze
  -> unchanged-revision soak, when separately authorized
```

No layer replaces the layer below it. A unit test does not prove persistence; a
stateful campaign does not prove a kernel or network failure; and a local green
suite does not prove that a running service uses this revision.

## Current Implemented Layers

| Layer | Public implementation | Claim boundary |
| --- | --- | --- |
| Deterministic contracts | domain, reducer, incident, outbox, persistence, reporting, and adapter tests | Pure and isolated behavior for fixed inputs. |
| Historical replay | `tests/historical_replay_harness.py` and sanitized fixtures | Current code can be evaluated against retained, bounded evidence; missing normalization inputs remain unknown. |
| Stateful authority exploration | `tests/stateful_runtime.py` plus delivery, audio, YouTube lifecycle, and viewer-external domain harnesses | Source priority, TTL, canonical-current, incident, and action interactions under generated test-only sequences. |
| Persistence and concurrency | SQLite/PostgreSQL repository tests, durable publication tests, and gated disposable-database integration | Transaction, lease, reconciliation, migration, and artifact behavior in isolated environments. |
| Source-aware parity | `compatibility/parity_convergence.py` and its 22-row sanitized fixture | A later independent cycle may prove bounded skew convergence; the immutable original mismatch is not rewritten. |
| Frozen campaign evidence | `tests/artifacts/monitoring_v4/*stateful_freeze*.json` and `docs/stateful-harness-evidence.md` | Reproducible local campaign summaries, not bulk raw evidence or production soak proof. |

The preserved selected campaigns cover four different authority topologies and
record 18 runs, 3,600 examples, and 16,171 generated actions. Those are
synthetic campaign counts, not uptime, live-source coverage, or production
availability.

## Harness Trust Model

The harness separates these roles:

- the SUT performs Monitoring v4 reduction, persistence, incident, reporting,
  and intent behavior;
- the scenario generator creates bounded test actions but does not decide the
  expected canonical state;
- the domain oracle derives expectations from explicit source roles, priority,
  freshness, and incident invariants rather than importing the production
  reducer as its answer key;
- the runtime attributes actions and preserves evidence but does not invent a
  default policy for an unknown domain; and
- the classifier keeps a monitoring defect, harness defect, oracle gap, missing
  evidence, and expected perturbation separate.

Evidence flows from immutable raw or sanitized input to versioned derived
classification, fixture, regression, and freeze. Historical facts, synthetic
actions, derived results, and unknowns retain distinct identities.

## Stateful Action And Evidence Contract

A persistent `ACTION_STARTED` must end with exactly one `ACTION_COMPLETED` or
`ACTION_FAILED`. The run checks:

- started equals completed plus failed;
- there are no orphan or double terminal events;
- created transitions and intents are attributed from pre/post identity sets,
  not timestamps or lexical ID order;
- a created intent refers to the transition or episode created by that action;
- failed actions retain best-effort post-state; and
- cleanup is proven by final target absence, not by a cleanup function merely
  returning without an exception.

If terminal evidence might have been partially persisted, the runner stops
rather than appending a competing terminal event. If attribution or cleanup is
unprovable, the result is a harness failure or evidence gap, not a monitoring
defect.

## Failure Injection Draft

The next public failure-injection layer should keep each condition bounded and
test-owned.

| Family | Candidate injection | Expected containment |
| --- | --- | --- |
| Source timing | stale evidence, TTL edge, delayed arrival, same observation time, reordered receive time | Freshness and arrival ordering remain distinct; canonical current fails closed when authority cannot be decided. |
| Source roles | authoritative/supporting disagreement, priority failover, role drift | Only the versioned domain policy can define current authority. |
| Persistence | busy or unavailable store, commit/file-publication crash window, terminal append failure | Durable state and publication reconcile or remain explicitly pending/failed. |
| Process lifecycle | observation worker exit, deadline overrun, restart, partial result | The failure is recorded without converting missing evidence to healthy current. |
| Outbox and notification | lease expiry, provider timeout, ambiguous receipt, restart between send and acknowledgement | Intent and delivery state remain durable and idempotent; this public milestone still uses credential-free providers. |
| Safe-input generation | changing file, malformed member, digest mismatch, crash before manifest publication | The previous complete generation remains usable or the candidate fails closed as a whole. |
| Parity | bounded skew, late proof, wrong source pair, conflicting rollout identity | Only a later independent matching cycle can reclassify an eligible skew. |

An injection must record requested, armed, triggered, and completed states. An
untriggered injection is a harness failure. Disposable PostgreSQL, temporary
SQLite, synthetic adapters, controlled child processes, and loopback endpoints
are appropriate; production namespaces, credentials, providers, and runtime
effects are outside this draft.

## Chaos Testing Draft

Chaos campaigns should start only after deterministic scenarios and oracle
rules are stable. Proposed campaign dimensions include:

- long generated state sequences across source priority, TTL, incident repeat,
  recovery, and source return;
- failure ordering around database commit, file replacement, acknowledgement,
  worker restart, and lease expiry;
- cross-domain observation combinations that prove one domain cannot borrow
  another domain's authority rule;
- source-aware parity skew in both directions, including late, wrong-source,
  conflicting, and self-authorized proofs; and
- bounded resource and concurrency schedules for the disposable stores and
  artifact writer.

Every randomized run must retain the seed, action index, minimized trace,
expected invariant, actual evidence, classification, and exact source identity.
Coverage reports must distinguish visited cells from the full state space.

## Repair And Freeze Boundary

Only a deterministic SUT failure with complete evidence, an independent oracle,
an isolated reproduction, and a patch-before-red regression may enter a bounded
repair loop:

```text
discover -> minimize -> reproduce -> diagnose -> failing regression
  -> minimal local repair -> targeted verification -> resume exploration
```

Policy, schema, public API, authority, deployment, production configuration,
historical facts, and soak epochs remain human gates. A family is frozen only
after controls, actual-event evidence, artifact reproduction, targeted tests,
and the full source-validation gate pass. New exploration belongs in a separate
test-only slice rather than silently expanding a frozen campaign.

## Stop Conditions And Limits

Stop when the root cause is ambiguous, an oracle cannot decide, required
evidence is absent, injection or cleanup fails, cross-domain semantics would
change, or the requested test exceeds its resource or repair bound.

The current public harness does not prove real kernel, storage-device, LAN,
external-provider, credential, notification, Dell effect, or viewer behavior.
It does not satisfy the seven-day source-coverage or fourteen-day parity gates,
and it does not authorize R7-R8 or any production cutover.

See [stateful harness evidence](stateful-harness-evidence.md),
[source-aware parity hardening](source-aware-parity-hardening.md), and the
[failure-mode mitigation record](failure-mode-mitigation-record.md) for the
implemented evidence behind this draft.
