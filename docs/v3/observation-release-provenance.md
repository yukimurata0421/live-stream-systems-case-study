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

## Player qualification and remaining limits

`stream_v3_player_av_probe.py` separates anonymous watch-page diagnostics from
observation of the official embedded player. It binds video identity, excludes
advertisements, and requires advancing displayed frames, decoded audio and a
nonzero audio signal in the same continuous interval. Resets cannot join shorter
intervals. Network diagnostics retain codes, not signed URLs or credentials.

The earlier watch-page and low-resolution failures remain evidence. Matching the
browser version alone did not fix watch-page automation. The committed embedded
probe now passes the bounded AV and displayed-clock conditions together; the
[follow-up record](../../publication/player-reception-qualification-20261103.json)
contains the actual window and measurements. The earlier
[checkpoint](../../publication/observation-release-provenance-20261103.json)
records the preceding unqualified state.

Both the private and portable committed profiles were exercised natively. The
portable run had a shorter overlapping clock interval than its AV interval; the
record keeps these windows separate.

AV progress alone does not prove that current content is being received. Native
seekable ranges and a static player-duration value were rejected as freshness
proofs. The final probe reuses the existing paired JST/UTC clock reader, keeps its
confidence requirement, checks observer NTP and clock steps, and advances the
original reading age. Clock and AV evidence must overlap. Clock uncertainty is
not converted into media failure or a recovery action.

The [committed player profile](../../configs/releases/public-player-probe.json)
includes the real clock dependency closure. An isolated cold-load test rejects a
missing required module. Optional displayed-clock verification needs Playwright,
a compatible Chromium, Pillow, Tesseract and its language data. The ephemeral
loopback page serves only generated embed HTML and exits with the owned probe.
No scheduled player service or new persistent listener was enabled.

For a one-shot YouTube check on Dell, the
[runtime-matched launcher](../../ops/scripts/stream_v3_matched_player_probe.py)
reads the active runtime Pod and the SHA256 of its running Chrome executable.
It copies the browser from the matching local image digest without starting a
container or pulling an image. The probe checks that SHA256 before launch and
checks the browser version before opening the embed. A Pod, container, or
browser change during the check leaves the combined result unknown.

On Dell, set up a local profile once using the
[settings example](../../configs/matched-player.env.example). Store it as
`~/.config/stream-v3/matched-player.env` with mode `0600`. Use a verified
`public-player-probe` artifact, the local Playwright, Pillow and Tesseract
dependencies, and an existing trusted evidence directory. The launcher uses the
probe script alongside it and copies the clock readers from the verified artifact.

```bash
python3 -B ops/scripts/stream_v3_matched_player_probe.py
```

The launcher reads literal `KEY=value` settings and creates a private run
directory for each invocation. Use `--env-file` for a different profile or
`--video-id` to override the public video ID when the broadcast changes. The
individual CLI options still work and take precedence. For an explicit
`--output` directory, create its parent first and leave the final path unused.
The Chrome version and executable digest are discovered each run. When a new
runtime image is deployed, its exact digest must also be available in Dell's
local Docker image store.

The transient systemd unit uses an unprivileged user, a disposable profile,
private devices, and CPU, memory, task, and runtime limits. Keep
`binding.json` with `receiver/player/player.json` to distinguish the
runtime-to-browser match from the public playback result.

The observed surface is the official embedded player. Watch-page automation is
still unqualified. The time-of-day display does not independently prove a calendar
date. This is not a long soak, zero-gap guarantee, human audio validation, provider
ingest-selection proof or production failover qualification. Existing direct
control facts, streaming processes, safety guards, SLOs and durable history remain
unchanged. Current consumers must move and pass their checks before old paths stop.

The probe uses the [official player API](https://developers.google.com/youtube/iframe_api_reference)
for player identity and the browser's
[media audio source](https://developer.mozilla.org/en-US/docs/Web/API/AudioContext/createMediaElementSource)
for decoded audio observation. Browser/API time values remain diagnostics, not
freshness authority.
