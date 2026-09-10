# Three-Host Source Consolidation

Review date: 2026-08-11 JST

This implementation record explains how the Dell delivery host, the
observability/arena host, and the Raspberry Pi publisher were compared before
their reusable source was consolidated into this public repository. It records
repository work only. No live service, Pod, timer, cloud object, or public site
was changed by this consolidation.

## Outcome

| Host role | Source comparison | Public repository result |
| --- | --- | --- |
| Observability / arena host | The deployed monitoring release contained no newer non-document source than the private canonical repository. Installed-unit drift was reviewed separately from release source. | Added the host-role drop-in that disables delivery-host WAN/RTMPS burst triggers, the durable reliability rollup, independent external evidence importer, single-writer notification contract, and an explicit-gate release example. |
| Dell delivery host | Host GPU/readiness helpers and units matched the canonical source. The running images were older, but contained no live-only source. | Imported no old image code. Kept current repository implementations and added the proposed map-legibility and explicit report-recording changes. |
| Raspberry Pi publisher | Eighteen active source, asset, Worker, and unit files were separated from generated snapshots, state, logs, backups, credentials, dependencies, and unrelated projects. | Added `ops/public-publisher/` as the reviewable canonical source, then converted host values to environment-driven public examples. |

An older running image is deployment evidence, not an authoritative source
branch. Copying it back would have reversed later tested changes. Conversely,
an installed unit can carry a legitimate host-role difference even when its
release source is otherwise identical; those differences were reviewed one by
one.

## Public Adaptations

The private source could not be copied mechanically. This public version keeps
the behavior while changing its environmental contract:

- host paths use `/opt/stream_v3` and `/var/lib/stream-v3/...` examples;
- logical host aliases replace private addresses;
- the external Monitoring project is required through
  `STREAM_V3_GCP_MONITORING_PROJECT`;
- remote recovery remains fail-closed with both apply flags set to `0`;
- the release installer refuses to run unless
  `STREAM_V3_RELEASE_APPLY=1` is set during an explicit release window;
- publisher destination, monitoring host, source path, and state path live in
  an external environment file; and
- generated JSON, local state, Worker caches, credentials, and raw evidence are
  ignored or excluded.

These are public reference defaults. They do not describe a completed rollout
on any of the three hosts.

## Code-First Migration Order

The consolidation intentionally followed this order:

1. compare checkout, installed configuration, running-image source, and
   generated state as separate evidence classes;
2. port reusable source and public safety adaptations;
3. run focused and full deterministic tests; and
4. add this curated documentation and review links.

The order prevents documentation from presenting a planned or inferred change
as implemented code.

## Verification Boundary

The public working tree passed 886 deterministic tests with 9 environment
skips, both shadow and streaming manifest validation, Python compilation, and
shell syntax checks after the code migration. These results prove repository
contracts. They do not prove that live units, k3s workloads, GCP checks, the
publisher timer, or the public site have changed.

## Review Paths

- Operational evidence: [`operational-reliability-and-external-evidence.md`](operational-reliability-and-external-evidence.md)
- Publisher boundary: [`public-publisher-boundary.md`](public-publisher-boundary.md)
- Map readability proposal: [`map-mobile-legibility-review.md`](map-mobile-legibility-review.md)
- Runtime ownership: [`current-runtime-contract.md`](current-runtime-contract.md)
- Public release exclusions: [`../public-release.md`](../public-release.md)
