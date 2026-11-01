# Thumbnail Acquisition Moves to K3s

On November 1, the provider-thumbnail acquisition and OCR worker moved into the
monitoring host's K3s. Dell's media path, local CRA processing and fallback policy
stayed in place. The existing Helix collector, relay observer and metrics
exporter remain host services; this is a bounded collector migration.

## Why the boundary changed

The previous worker depended on a host API-evidence path, a tmpfs URL mirror and
a relay endpoint on localhost. Wrapping those paths in hostPath/hostNetwork would
keep the same placement assumptions. Instead, the existing source adapter now
publishes a coherent API/URL bundle over a private Service and bridges only the
fixed native-forward GET endpoint. Original timestamps and boot/instance/sequence
binding are retained. No extra Helix requests or API credentials enter the Pod.
The legacy mirror remains compatible for rollback and other host consumers.

The worker publishes its actual observation through a second Service. The
existing exporter reads it over HTTP, keeping metric names, job labels and
history. A failed HTTP read cannot fall back to a frozen file or manufacture a
new timestamp. The running publisher and renderer did not need changes for the K3s cutover.

## Deployment and state

The digest-pinned image bundles Python, Pillow, Tesseract, English OCR data and
tini. The Pod is non-root, has a read-only root, resource limits, no service-account
token and no hostPath/hostNetwork. It is pinned to the producer's node for boot
binding. Readiness/liveness measure monotonic loop progress; UNKNOWN results
remain valid publications rather than restart triggers.

The old worker stopped after a completed cycle before the new one started. Ten
state files, totaling 1,793,549 bytes, retained identical SHA-256 hashes after
transfer into the PVC. Verified anchors, request/cooldown times, counters,
startup-grace origins, numeric history and bounded abnormal ROIs survived.
The bound PV uses Retain. Recreate and the existing state lock prevent concurrent
writers. The old source/unit/config remain available; rollback must transfer the
latest PVC state back instead of rewinding the initial snapshot.

## Evidence at this checkpoint

| Gate | Result |
| --- | --- |
| Candidate and public regression | 146 private tests and 139 public tests passed. Real local HTTP handoffs, stale/mismatched evidence rejection, publication loss, persisted polling limits, loop health and the standalone exporter entrypoint were exercised. |
| Image | Cold entrypoint and native OCR passed under non-root, read-only, 0.2 CPU / 256Mi conditions. A bounded synthetic clock ROI read 12:34:56 with confidence 95.29. |
| Kubernetes | Server dry-run, actual image/Pod identity, private Services, retained PVC and rendered configuration were checked. The Pod has no service-account token; facts were reachable while the tested public HTTP and node-API connections did not establish. This alone does not attribute the refusal solely to NetworkPolicy. |
| Natural cycles, 19:30–19:44 JST | Fourteen of fifteen cycles verified three image-clock values. The first cycle recorded a DNS-resolution failure as UNKNOWN; the following normal cycle recovered. Pod restarts: zero. Minimum request spacing across cutover: 60.0 seconds. |
| Public browser | The same open Chromium changed from UNKNOWN to AGING/FRESH. New-image count advanced from 201 to 202, and Thumbnail age advanced from the 19:30 to the 19:40 JST bucket. At 19:42:47, the tile was FRESH with picture age 3 min; the graph showed the ten-minute maximum, 7 min. DOM, JSON and a screenshot were retained privately. |
| Delivery and CRA | Main/observer Pod and container identities/restart counts matched. The existing FFmpeg generation/start identity remained; relay input and both forwards progressed, Twitch API was LIVE, and Dell's CRA/fallback processes and CRA release remained unchanged. The root-only CRA config was checked afterward: its change time predates the migration; no before/after hash comparison is claimed. |

Preparation failures were retained separately: an obsolete build was stopped
before deployment, an oversized OCR test fixture was corrected, SSH argument
quoting failed before importing state, and a transfer-Pod deletion deadline
raced its grace period. Deletion was confirmed before activating the collector.
These were not silently counted as successful runtime checks.

## Public checkout follow-up

A subsequent source-tree audit found that the current renderer had been copied
without wiring it into the public HTML entrypoint, builder and upload allowlist.
Those references and the clock-projection collector are now ported together.
The external uptime-check phrase remains in the page. The current page is tested
with synthetic public JSON, without contacting production or publishing objects.

Retired public systemd/file-input installation examples were removed. The
production host's rollback files and the runtime's compatibility inputs remain
unchanged. Historical port records keep their original paths/hashes, with the
removal recorded separately rather than rewriting old evidence. The earlier
renderer stays because a dated regression fixture and case study still use it.

## Limits

K3s does not remove the single-node or existing host-observer failure domains,
nor does it fix provider-CDN behavior or every transient DNS failure. Public
thumbnail evidence does not prove player video/audio continuity. A production
rollback exercise, node-loss exercise, future stream renewal and long-term OCR
success rate were not qualified. No production fault injection or test webhook
was used. The [stable contract](twitch-thumbnail-monitor.md),
[deployment definitions](../../ops/kubernetes/twitch-thumbnail/README.md) and
[current status](../current-status.md) separate these limits from implementation.
