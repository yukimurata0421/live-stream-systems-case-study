# Documentation Responsibilities

Readers should be able to follow a claim from its summary to the applicable
contract, then to dated evidence or source. Each page has one primary job.
This model covers the repository's documentation; it grants no deployment or
recovery authority.

## Reading Routes

The [repository README](../README.md) explains the project and its strongest
bounded evidence. The [guide](README.md) chooses the next question; the
[catalog](00_INDEX.md) is the exhaustive reference. The
[reviewer guide](hiring-reviewer-guide.md) adapts that route to an interview.
None of these pages is another operational contract.

| Page type | Owns | Link elsewhere for |
| --- | --- | --- |
| Overview or review map | Purpose, concise claims, and the route to evidence | Thresholds, current deployment details, full chronology |
| Current contract | Meaning, owner, inputs, decisions, failure behavior, limits | Dated test totals and rollout transcripts |
| Decision | Context, alternatives, choice, cost, and revisit condition | The complete current implementation |
| Dated evidence | Revision, observation window, method, result, failures and limits | A claim of present health or deployment |
| Procedure or source guide | Prerequisites, commands, expected result, stop and rollback boundaries | Implicit authorization or a universal installer |
| Current status | Latest retained checkpoint per topic, open gaps, disabled capabilities | The full historical log |
| Publication record | Selected source identity, adaptations, exclusions and validation | Loaded runtime identity or viewer reception |
| Proposal or superseded design | Its status, historical scope and successor | Current operational instructions |

## Which Page Owns A Rule

| Question | Primary document |
| --- | --- |
| Which hosts exist and who may act? | [Architecture](architecture.md) |
| What runs in the delivery runtime? | [Runtime contract](runtime-contract.md) |
| When does direct YouTube fallback start or stop? | [Fallback decision contract](v3/fallback-decision-contract.md) |
| What does an observation prove? | [Observation contract](v3/observation-standardization.md) |
| How is cross-host age established? | [Time contract](v3/cross-host-observation-time.md) |
| How do observation time and notifications interact? | [Notification time](v3/notification-observation-time.md), with [delivery rules](v3/notification-and-auto-recovery.md#delivery-and-current-state-rules) |
| What may leave the monitoring host? | [Publisher boundary](v3/public-publisher-boundary.md); [R2 transport](v3/r2-publication-transport.md) owns object delivery |
| How are source releases identified? | [Release provenance](v3/observation-release-provenance.md) |
| What has actually been observed? | [Current status](current-status.md), linking dated evidence |
| What are the CRA and Monitoring v4 contracts? | Their [CRA catalog](../recovery-control/docs/00_INDEX.md) and [Monitoring v4 guide](../monitoring-v4/README.md#read-next) |

Specialized contracts refine these boundaries; a summary does not override
them. The current source identifies implementation behavior. A claim about
the operated system additionally needs its own release and observation evidence.

## Writing And Updating

Lead with the answer to the page's question. Give enough context to understand
the decision, then its evidence and practical limit. Define a term where it is
first needed. Prefer concrete observations to labels such as "robust" or
"fully verified". Use a table for genuinely comparable cases, and a diagram
only when relationships are clearer than prose.

Summaries link to changing thresholds, test totals, revision counts and current
deployment details. A dated headline may retain a measured number when its
window and scope are visible, as with the same-URL checkpoint. Stable schema
names and command arguments belong in their source guides. Do not copy every
new port into the README or repeat the full limit list on every page.

When behavior changes, update the owning contract, current-status checkpoint,
source map and affected entry links together. Keep failed trials and older
measurements in dated records. If a former result is superseded, add a successor
link; do not rewrite it as though the later behavior had already been observed.

Keep the current-status overview short. Move accumulated chronology to a dated
archive, preserve inbound anchors, and state the evidence cutoff independently
of the edit date. An open gap closes only with evidence covering that same scope:
an embedded-player test does not qualify a watch page or a failover.

Before adding a page, choose its primary role and check whether an existing
owner already answers the question. Add it to the catalog and link it from the
relevant topic. Component READMEs own local setup; top-level guides link to them.
Keep filenames and useful anchors stable when restructuring.

## Validation Boundary

Documentation checks cover links, catalog coverage, evidence invariants and
publication digests. They cannot establish that prose matches a running service.
Review meaningful claims against the cited source or dated record. Preserve
the difference between local tests, Git publication, deployment, provider
acceptance and reception. The [test strategy](test-strategy-and-safety-boundary.md)
describes the executable checks and their limits.

The [November 4 review inventory](../publication/documentation-design-review-20261104.json)
records the structural review scope and distinguishes it from focused prose
reconciliation. It is not a new live-system audit.
