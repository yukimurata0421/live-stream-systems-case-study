# Committed Observation Releases and Qualification Gates

The operated observation sources are now captured in private Git commits. A
release is built from committed blobs and an explicit profile, including the
profile itself. A dirty checkout cannot silently supply newer files to that build.
The [dated checkpoint](../../publication/observation-release-provenance-20261103.json)
records activation and verification separately.

## Why the source boundary matters

The running notification package was newer than its development checkout. The
monitor also loaded a pinned outer wrapper, an older monitor baseline and separate
producer versions. Recording only the newest directory would not reproduce that
execution. The private baseline therefore preserves the actual versions and their
references. Shared files are reused; differing operated versions remain explicit.
Those host-bound source snapshots and configuration references are not published.

The native CLI was rebuilt with the same regular-file contents and activated by
switching its existing pointer. The long-running monitor was not restarted. A
natural notification child used the new physical release directory and completed
successfully. Other source profiles were rebuilt and checked against their
operated files; that does not mean those services were redeployed.

## What the tools prove

`stream_v3_release.py` bounds file count and bytes, rejects path traversal and
committed symlinks, and records Git blob IDs, SHA256 values and executable modes.
Build, external binding, verification and activation are separate operations.
Activation pins both the expected artifact and previous pointer, uses a lock,
and changes no services or durable runtime state. Concurrent use of older switch
tools is outside that lock and is not an approved deployment path.

The old deployment helper now delegates to the committed builder. It no longer
implicitly overwrites units, enables timers or restarts monitoring services.
Historical release copies remain evidence, not deployment entrypoints.
The [example profile](../../configs/releases/source-audit-example.json) contains
only generic source-audit tooling; it is not a production installation recipe.

`stream_v3_release_audit.py` reports expected commit identity, source hashes,
source/reference matches and configured entrypoints through the existing adapter.
It distinguishes `committed-artifact` from `legacy-source-pinned`. A source match
on disk is not proof of bytes loaded in memory, effective credentials, service
health or viewer reception. The existing adapter now exposes continuous source
checks without adding a daemon or listener. Background verification cannot hold
HTTP scrapes open. Age starts at the audit start on the same host boottime; expiry
or failure removes positive cached results. A single unconfirmed episode remains
open across mismatch and missing evidence, and clears only after fresh source,
entrypoint, startup-build and scrape confirmation all succeed.

## Policy and retirement ownership

The [policy catalog](../../configs/observation/policy-catalog.json) lists meanings,
units, owners, source references and whether values are operational policy or
resource bounds. Its checker detects drift in the shared constants it references.
It never loads a single global policy into recovery, incidents or SLO evaluation.
Other settings remain owned by their source configuration or cloud alarm.

The [retirement plan](../../configs/observation/retirement-plan.json) lists actual
consumers, retained roles, acceptance conditions and review dates. Review dates
are not automatic deletion authority. Raw history, independent notification
health and separate incident owners serve different purposes and are retained.
An active consumer is moved and qualified before its old path is stopped.

## The player gate remains open

`stream_v3_player_av_probe.py` observes an anonymous public player in a separate,
bounded process. It binds evidence to the requested video, excludes advertisements,
and checks advancing video frames, decoded audio bytes and a nonzero audio signal.
A reset or error cannot join two shorter intervals into a successful observation.
Network diagnostics retain status/error codes, not signed media URLs or cookies.

The production player and an official YouTube sample both stopped in the same
headless environment after initially decoding video and audio. A headed attempt
first failed because its isolated virtual display loaded a GPU library that
crashed. Disabling that extension for the probe allowed it to start, but playback
still stopped. These results remain UNKNOWN; they do not establish a production
outage or continuous viewer reception.

A later separate-browser experiment observed a continuous interval of decoded
video and audio. Its committed integration did not reproduce that result and had
a cleanup failure. The experiment remains private and unqualified for continuous
operation; the public probe receives only the screenshot-byte saving correction.
A single successful experiment does not clear the operated-probe gate.

Old-path retirement and remaining adapter cutovers therefore remain gated.
Existing direct control facts, streaming processes, safety guards, SLO values and
incident/outbox/receipt history are preserved. No production failure was injected.

The probe uses the [YouTube player identity contract](https://developers.google.com/youtube/iframe_api_reference)
and the browser's [media audio source](https://developer.mozilla.org/en-US/docs/Web/API/AudioContext/createMediaElementSource)
for observation. Browser decoding is distinct from proving which ingest the
provider selected, a real failover, perceptual audio quality or a long soak.
