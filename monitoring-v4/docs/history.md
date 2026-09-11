# Reconstructed public history

This repository has an independent public Git history. It was reconstructed on
2026-08-13 from reviewed private milestones; the private `.git` directory and
high-resolution operational logs were not copied.

The source for the first baseline is private commit
`8047d911ec9c6c6deb2bd553cdc42ac8c8eb90c4`. The public tree adds portable
paths, self-contained tests, public-facing documentation, and an example
inventory. Those publication changes are not claims about the deployed tree.

The revision-pinned shadow milestone is reconstructed from private commits
`de10bde7693881bec4574158c26d00c13a073c39` through
`2b53a839eeb50565a30e2074441771f66189caa2`. It adds R2 coverage, R3 semantic
parity, R6 projections, and generic credential-free service examples. Private
live inventory and operational logs remain outside this mirror.

The PostgreSQL/k3s milestone is reconstructed from private commit
`832f4482268809dfa28c4b736d601d9c65b3e091`. It contains the source snapshot
created after the private credential-free subsystem had been stabilized. The
recorded private deployment content identity was
`ef2f0b0d5ed9e0f5fefac6d4ef1f72d508ae2afa`; that content identity is not a Git
commit and is not evidence that a public commit was deployed. Generic public
paths and service users replace private host-specific values.

The responsibility, publication, and redundancy milestone is public commit
`139ca84f84de6df73f1a13ba584abebc36dadabd`. Its reviewed private source was a
dirty-worktree snapshot identified by behavioral content identity
`73aae2f9eb4db9e9ddbab79823fa5ea2d52fb3f9`, not by a new private Git commit.
It adds schema-v6 durable artifact reconciliation, runtime lifecycle and
verified rollout evidence, role-specific repository ports, smaller reporting,
sentinel, safe-input, observer, storage and outbox modules, dual-device backup,
isolated full-restore evidence, gated retention, and fault-injection tests. The
public version replaces host-specific paths with portable examples.

Source availability, a passing unit test, a live deployment identity, and an
elapsed soak gate are different facts. Public commit dates record when this
mirror was assembled, not when a private implementation was first deployed.

Planned public milestones are:

1. isolated R0-R5 baseline — included;
2. revision-pinned R2/R3/R6 shadow evidence — included;
3. PostgreSQL and k3s credential-free subsystem — included;
4. public portability, provenance, and history validation — included in
   `1ab42730fe32b2f18716f5a279ff9a4ad9ac3b65`;
5. responsibility refactor and predictive-failure hardening — included in
   `139ca84f84de6df73f1a13ba584abebc36dadabd`;
6. updated public status and validation record — included in the documentation
   commit following the implementation milestone.

Exact public/private milestone mappings and the publication exclusions are in
[Public release boundary and provenance](public-release.md).

The provenance record maps exact implementation commit IDs while keeping
private operational evidence outside this repository.
