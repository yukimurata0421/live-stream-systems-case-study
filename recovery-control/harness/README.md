# CRA Harness Engineering

This directory contains the immutable scenario catalogs, evidence-lineage
schema, and sanitized fixtures for the test-only CRA harness.

```text
scenario registry
  -> environment factory
  -> fault injector: requested -> armed -> triggered -> completed
  -> SUT: CRA + Dell Agent + separate SQLite stores
  -> observer: facts only
  -> append-only evidence
  -> independent oracle
  -> classifier
  -> trust gate and report
```

`harness/scenarios/protocol_v1.json` is the common registry for deterministic
protocol scenarios, negative controls, and randomized exploration. A local run
uses an immutable run ID:

```bash
.venv/bin/cra-harness --run-id <immutable-run-id>
```

Reusing a run ID is rejected. Generated artifacts remain outside the public Git
snapshot. The base harness uses a `FakePhysicalAdapter`; it does not load a
production credential or perform a real signal, process, Pod, Deployment, or
network mutation.

When an exploratory case becomes a regression, preserve its seed, case index,
minimized input and fault, expected invariant, and source identity under a new
stable deterministic scenario ID. Unvisited state-space cells are not passes.

The postmortem-derived I/O catalog in `harness/scenarios/postmortem_io_v1.json`
uses temporary files, controlled loopback mTLS, and disposable test children in
a separate integration-test population. These mechanisms still do not prove a
production fault or soak result.

Read the public design records before extending a catalog:

- [`docs/harness-trust.md`](../docs/harness-trust.md)
- [`docs/harness-engineering-draft.md`](../docs/harness-engineering-draft.md)
- [`docs/failure-injection-draft.md`](../docs/failure-injection-draft.md)
- [`docs/chaos-testing-draft.md`](../docs/chaos-testing-draft.md)

Harness success must not be relabeled as a seven-day soak pass, production
fault injection, real physical-effect proof, or viewer recovery.
