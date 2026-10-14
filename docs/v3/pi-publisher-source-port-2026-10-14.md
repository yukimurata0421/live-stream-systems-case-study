# Raspberry Pi Publisher Source Port

Recorded: 2026-10-14 JST. Scope: public source checkout only.

The publisher working tree contained changes absent from the public snapshot.
Its reliability projection now separates Viewer and ADS-B measurements,
includes Full-HD sample counts, and exposes a separate allowlisted active-incident
section. Its notification backlog query counts pending delivery without adding
active incidents. These behaviors have been imported with portable paths and
English documentation.

## Source Decisions

The audit read 34 source files from the site tree and two adjacent status
collector files. Twenty-three files already matched their public counterparts.
Each decision and selected hash is recorded in the
[publication manifest](../../publication/pi-publisher-source-port-20261014.json).

| Source | Public decision |
| --- | --- |
| Reliability collector | Import schema v5, separate evidence planes, Full-HD facts, and incident field filtering; retain portable source/state defaults |
| Status collector | Import the pending-delivery-only notification backlog label and query |
| Pi reliability tests | Adapt the seven existing synthetic tests to the public layout and retain three existing incident/ADS-B boundary checks |
| Build script | Retain the equivalent public behavior and repository-relative status default |
| Upload script | Retain the newer public frozen-tree allowlist, deadline, and durable-result protections; register five current HTML assets missing from that allowlist |
| Static assets and index | Preserve separately reviewed display candidates; this source commit leaves the existing committed display files in place |
| Service and Worker configuration | Retain public examples; do not copy machine-specific configuration |
| Older Japanese operations notes and README | Replace installation-specific instructions with an English source guide and the existing publisher boundary |

The public checkout is an independently curated snapshot. It is not a byte copy
of the live publisher. In particular, retained upload protections and display
assets must not be interpreted as evidence that those exact public sources are
installed on the Raspberry Pi.

The source audit also covered display candidates already present in the public
working tree. Their audit hashes are preserved in the manifest, but separate
uncommitted display changes are outside this commit. The committed source-map
entries identify the files included in this port.

## Validation and Limits

Portable tests use synthetic reports and mocked SSH/provider/cloud calls. They
check schema v5, unknown evidence, separated Viewer/ADS-B denominators, Full-HD
sample facts, incident field filtering, and notification backlog semantics.
Existing build, upload, relay, and Twitch adapter tests remain part of the
publisher verification. Source-map hashes, local documentation links, English
language checks, and the selected-file secret scan are verified separately.

One initial merged test used the older fixture's ADS-B counts. Its expected
values were corrected to the imported synthetic fixture, without changing the
collector behavior. The upload test also found five current HTML assets missing
from the public allowlist; their exact names were added without allowing arbitrary
files. The system Python lacked pytest, so the final suite used the existing
public development environment. Failed results remain in the private port evidence.

No live publisher file, service, timer, provider session, credential, GCS object,
Worker configuration, or stream process is changed by this source port. No
site upload or Git push is performed during source-port preparation. Local tests do not establish deployment, external
visibility, continuous reception, or historical SLO qualification.

See the [source guide](../../ops/public-publisher/site/README.md),
[publisher boundary](public-publisher-boundary.md), and
[secret-handling guidance](../security-and-secrets.md).
