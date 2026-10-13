# Current Evidence And Route Protocol Source

The 2026-10-13 source port incorporates later evidence binding, authorization
revision checks, append-only effect reconciliation, TCP stall collection and
analysis, and the typed stream-route request. These are public code and test
surfaces, not a new production authorization or qualification result.

`runtime.typed_effect_request.v3` includes `SWITCH_STREAM_ROUTE`. Admission
binds the exact child target, expected route generation, executor, producer
identity, maintenance projection, and route transition. The existing durable
effect fence remains the one physical-attempt boundary. Source availability
does not enable automatic failover or broaden an FFmpeg-child request into a
Pod, container, or host restart.

Unknown effects retain their original outcome. A retired exact target can be
recorded separately after identity-bound replacement evidence; it is not
rewritten as a successful repair. An observed effect awaiting transport
verification has a distinct retirement gate. An unknown scope claiming that
observed outcome is rejected. No automatic retry closes an uncertain scope.

Monitoring remains facts-only. CRA owns policy, budget, cooldown, command
lifecycle, and final verification. `007_authorization_revision_binding.sql`
extends ordered migration state; operators must assess the full release and
rollback separately before applying any migration.

The TCP collector/analyzer records source-time and exact-target transport
samples. A causal baseline and offline analysis cannot alone prove remote
provider reception or authorize a calibrated production restart. The primary
OCI path also requires independent relay input and provider output evidence;
local loopback ACK has a narrower meaning.

Portable tests validate these contracts with temporary state and mocked
effects. Source-bound private qualification artifacts and raw live observations
are not included. Public policy remains `PROPOSED_LOCAL_VERIFICATION` with
`production_behavior_change_authorized=false`. Native campaigns, elapsed
soaks, scheduled renewals, cloud CI, and live recovery are separate gates.
