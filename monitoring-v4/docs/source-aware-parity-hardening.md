# Source-aware parity convergence hardening

Recorded: 2026-08-17 JST

Status: implemented and locally verified; not deployed by this public mirror.

## Purpose

Monitoring v4 compares immutable current snapshots produced at different
cadences. A state difference can be a bounded sampling skew rather than a
contract difference, but accepting every time skew would hide real failures.
The implementation therefore records the original cycle as a violation and
only reclassifies it after a later, independently stored cycle proves that the
lagging view caught up.

The deterministic fixture
`tests/fixtures/monitoring_v4/2026-08-17_bidirectional_snapshot_convergence.json`
contains 22 sanitized comparisons and their later convergence proofs. It does
not contain database rows, observation identifiers, credentials, provider
payloads, host addresses, or private paths.

## Acceptance contract

A snapshot-skew candidate requires all of the following:

- an exact, versioned domain and source-pair policy;
- valid but different canonical states and valid timestamps;
- a non-zero skew within the pair-specific absolute bound;
- a later cycle for the same exact source pair;
- the lagging timestamp passing the original leading timestamp before the
  pair-specific deadline.

The immutable candidate cycle cannot authorize itself. Duplicate actual
domains, missing or substituted sources, invalid payloads, equal-timestamp
semantic differences, out-of-window proofs, late proofs, and conflicting
rollout evidence remain fail-closed.

## Implemented boundaries

- `compatibility/live_parity.py` validates one cycle and rejects duplicate
  actual-domain inputs.
- `compatibility/parity_convergence.py` owns the versioned source-pair bounds
  and convergence proof.
- `reporting/parity_contract.py` strictly validates immutable parity payloads.
- `reporting/parity.py` deduplicates report rows, validates rollout evidence,
  and derives retrospective acceptance from later proof.
- the report builder and sentinel preserve an explicit failure when rollout
  evidence identities conflict.

Focused regression coverage includes the 22-row fixture, both skew directions,
wrong-source proof, deadline and bound failures, duplicate domains,
self-authorized classifications, tampered rollout evidence identifiers, and
cross-row evidence conflicts.

## Unchanged authority

This hardening does not add a notification provider, runtime mutation,
recovery executor, or public-source authority. It does not modify source,
incident, notification, recovery, or database schema policy. A green local
test result is not evidence that any running service uses this revision.
