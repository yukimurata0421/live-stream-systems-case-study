# Monitoring v4 stateful harness evidence

Recorded: 2026-08-21 JST

Status: local, isolated verification evidence. This record is not proof of a
deployment, a production verification, or a soak.

## Verified authority topologies

The versioned harness covers four distinct current-selection contracts:

- `delivery`: an equal-priority authoritative pair;
- `audio`: unequal-priority authoritative failover and failback;
- `youtube_lifecycle`: authoritative, diagnostic, and supporting roles where
  only the allowed authoritative roles can define canonical current;
- `viewer_external`: a domain-specific supporting-current exception where the
  supporting role is explicitly allowed to participate in canonical current.

The domain-specific oracles remain separate. In particular, the supporting
rule for `youtube_lifecycle` is not reused for `viewer_external`.

## Common runtime contract

The shared stateful runtime records a persistent `ACTION_STARTED` event before
execution and exactly one terminal event: `ACTION_COMPLETED` XOR
`ACTION_FAILED`. It attributes pre/post identities, rejects orphan and double
terminal evidence, records best-effort failure post-state, enforces bounded
execution and resource guards, and proves cleanup from final target absence.

The selected full campaigns comprise 18 runs, 3,600 examples, and 16,171
generated actions. Their versioned freeze summaries report zero confirmed SUT
failures, harness failures, attribution gaps, oracle gaps, unknown outcomes,
and cleanup failures. These counts describe the preserved synthetic campaigns,
not a production soak.

## Canonical-current repairs

Stateful delivery exploration found two deterministic persistence defects in
the domain-generic current store:

1. a same-timestamp semantic replacement could be rejected by snapshot-ID
   ordering even when the reducer produced `unknown/source_disagreement`;
2. an expired canonical row could reject the fresh fallback candidate solely
   because the fallback observation timestamp was older.

The repository keeps a pre-repair reproduction, diagnosis, post-repair
reproduction, direct storage regressions, domain stateful regressions, and
cross-domain freeze lineage. Source policy, reducer policy, incident policy,
and schema revisions were not changed by these repairs.

## Evidence boundary

Small versioned freezes, representative reproductions, deterministic fixtures,
and the release-evidence manifest are tracked. Bulk run JSON, action JSONL,
duplicate reruns, and intermediate artifacts are intentionally excluded from
application Git.

The external private evidence index is identified without exposing its storage
location:

- manifest SHA-256:
  `3e053984f6679d7aba85c1bf93511732515e21fe8008399700422d4b13424762`;
- bundle SHA-256:
  `e52f8be6ac5ef12a231f73501cc69477c2664a1769c7c56471d15ab1ffd147da`;
- source files: 332;
- source bytes: 333,689,351.

The tracked manifest intentionally does not self-reference its containing Git
commit. An external post-commit attestation binds the final exact Git revision,
archive hash, PostgreSQL checks, Historical Replay attestation, and structured
suite result.

## Controller boundary

The campaign controller is a shadow-only plan generator. It validates topology,
source roles, oracle attestation, bounds, freeze identity, and human gates. Its
`execution_authorized` value remains `false`; no unknown-domain campaign or
production mutation capability is included.
