# Public Source Port: OCI, Twitch, And Route Evidence

## Scope and cutoff

This port was prepared on 2026-10-14 JST from development sources and operational
records available on 2026-10-13 JST. It updates the public Stream, Monitoring,
CRA, and static-publisher source without changing a running service, deploying
an image, uploading a website, committing, or pushing a repository.

The source audit treated the development checkout, immutable running source,
existing public snapshot, and retained operational documents as distinct
inputs. A checkout HEAD alone was insufficient: the running delivery image
contained the single-sender integration hook that the development checkout
did not yet fully contain.

## Difference decisions

| Surface | Difference recognized | Public decision |
| --- | --- | --- |
| Dell delivery | Running source includes the producer/buffer/single-sender integration. | Verify manifest hashes, port the integration hook and modules, preserve existing public audit and portability helpers. |
| OCI relay | Lease guard, input freshness, per-provider forwarding, and daily Twitch ownership exist beyond the earlier direct-YouTube architecture. | Port core code and tests; replace host bindings with examples; omit private deployment/configuration. |
| Network diagnosis | Loopback ACK is insufficient after the tunnel migration. | Port four-plane rates, regime-specific causal baselines, multiple probes, and an offline synthetic replay. |
| Upload recovery | Low upload must not independently authorize restart. | Port current producer-source handling and classifier behavior. |
| Monitoring | Relay/API/reception projections and SLI separation are newer than the public snapshot. | Port read-only collectors/reducers, require configured account/revision binding, and keep public field allowlists. |
| CRA | Current evidence, authorization-revision binding, reconciliation, route fencing, and TCP analysis differ from the older snapshot. | Port schemas/migrations/core and portable tests; retain disabled production policy and public validation boundaries. |
| Static publisher | New relay and Twitch views depend on current collectors and layout assets. | Port source/assets only; omit generated live JSON, service installs, and upload execution. |
| Documentation | Public topology still described the historical direct sender. | Update entry points and current contracts; retain dated same-URL and recovery measurements as historical evidence. |

The integrated public repository and standalone Monitoring/CRA public
directories receive the same selected component changes. Their Git histories
and surrounding standalone documentation remain separate. No unrelated
development files or other archived repositories are overwritten.

## Provenance and exclusions

[`publication/source-map.json`](../../publication/source-map.json) records
source roles, source revisions, the running manifest digest, public file
digests, and adaptation decisions. Private worktrees had uncommitted changes;
their HEADs identify lineage, not an exact file snapshot. The resulting public
digests identify this port.

Excluded inputs include production credentials and destinations, raw logs,
private addresses, runtime databases, generated live snapshots, source-bound
private qualification artifacts, account metadata administration, and private
host deployment scripts. Synthetic fixtures are labeled and are not substituted
for failed live evidence.

The running source manifest explicitly carried
`UNVERIFIED_USER_CANCELLED_HARNESS`. This port retains that qualification
boundary. Model tests, source hash agreement, active services, and successful
HTTP observations cannot turn it into formal qualification.

## Validation and limits

Local checks use only the public source trees and an isolated dependency
environment. The route demo has no network or signal effects. Unit/protocol
tests, public-boundary scans, syntax checks, and documentation links are
reported in [`publication/validation.json`](../../publication/validation.json).
Native fault campaigns, production effects, cloud CI, live reception soak,
and scheduled-midnight renewal are separate gates and are not claimed here.

One imported reconciliation test expected an older protocol rejection reason.
The current ledger permits an observed-effect retirement only from
`EFFECT_OBSERVED_AWAITING_VERIFICATION`; an unknown scope claiming that outcome
remains rejected. The test was aligned with this state gate, and a positive
case verifies the permitted transition while preserving the original outcome.
No production recovery semantics were changed by that test correction.

Rollback is file-scoped: restore this port's modified files from the retained
pre-port public snapshot and remove its new files. Runtime rollback is outside
this publication operation. Deployment, qualification, commit, and push require
their own explicitly authorized work.

## README and publication correction: 2026-10-14 JST

The initial overview combined media, observation, publication, and a disabled
recovery source contract in one wide Mermaid graph. The revised README uses
separate media and publication graphs and a responsibility table for the
recovery contract. It restores the Grafana-proxy input alongside arena's public
artifacts and distinguishes original snapshot lineage from later uncommitted
source ports. Both diagrams are checked with a real Mermaid renderer.

The same review found a port omission in the uploader's fixed file allowlist:
new HTML referenced OCI/Twitch assets that the public staging step did not yet
copy. The current development allowlist is now ported, including those assets
and the three optional sanitized provider artifacts. A local frozen-upload test
checks referenced resources and unknown-file exclusion without invoking GCS.
No live publication or production component is changed by this correction.

## Hiring-reader revision: 2026-10-14 JST

The entrypoint now leads with the problem, dated outcome, and short failure
stories. The role-specific guide offers a short first pass; detailed identifiers,
measurements, and implementation maps remain available by link. Recent watchdog,
renewal, warning, cache, external-check, and harness-failure accounts are curated
without private logs or credentials.

A new read-only query extends the continuity checkpoint through October 14:
160 days of URL-identity observation and 140 checkpoint calendar dates. The
September checkpoint and original fixed availability ratios are retained.
The [aggregate checkpoint](../../publication/continuity-checkpoint.json) is
measurement evidence; it is not a claim of uninterrupted frames or a new SLO.
