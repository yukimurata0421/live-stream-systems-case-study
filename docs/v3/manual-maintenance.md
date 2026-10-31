# Manual Maintenance with a Cloud-Hosted Slate

The sender can be stopped for planned operating-system or GPU maintenance while
the relay repeats an English map-backed maintenance slate through its existing
YouTube primary and Twitch outputs. This is an explicit operator mode. It does
not infer that both delivery routes have failed, add a second backup writer or
replace the existing direct fallback.

See [current status](../current-status.md) for the operated installation and
[the source-port record](../../publication/manual-maintenance-port-20261030.json)
for file identities and validation boundaries.

## Start with one command

After installation and configuration, run this in the sender's terminal:

```bash
sudo stream-v3-maintenance on
```

The command checks identities, existing authority and unresolved fences, actual
backup Pod absence, input and both output paths. It records durable inhibition,
disables the sender recovery timer, arms the relay slate and stops only the main
Deployment. After observing the slate and audio/video/output progress, it asks:

```text
Are both players showing the maintenance slate? [yes/no]
```

Check the public YouTube and Twitch players, then answer `yes`. Only then does
the command enter `MAINTENANCE` and return `safe_to_update_dell=true`. This is an
operator attestation, not automated provider or player verification. The command
does not install updates or reboot the machine. The default window is 60 minutes;
`--minutes 90 --reason 'Security updates'` can set a different planned window.

No, EOF or interruption keeps any established slate and inhibition. Running
`on` again continues the same session without extending its deadline or
redispatching completed effects. Unknown outcomes require read-only reconciliation
first. A piped yes or preconfirmation flags cannot bypass the terminal check.

## Return with one command

After checking the sender's GPU and other maintained components, run:

```bash
sudo stream-v3-maintenance off
```

The command continues the saved return stages: start the original main workload,
restore only the saved relay fields, and require 300 seconds of continuous live
progress. It then asks in the same terminal:

```text
Are YouTube and Twitch playing live video/audio, with live data updating? [yes/no]
```

Inspect both public players and the live map data before answering `yes`. The
command checks progress again, restores the original recovery-timer state and
releases fallback inhibition. This is an operator attestation, not automated
provider or player verification. The command does not install updates or reboot.

No, EOF, Ctrl-C or failed verification keeps inhibition. Run `off` again to
continue the same session without replaying applied effects. Unknown outcomes
require read-only `reconcile` first. A terminal is required; piped input and
preconfirmation flags are rejected. When already `NORMAL`, `off` makes no changes
and does not prompt or assert current delivery health.

Missing or old observations, a gap over 45 seconds, a changed connection or a
frozen media/output counter reset the stability window. A confirmation taking
longer than that gap also holds completion; rerun `off` to establish fresh
stability and confirm again. Expiry does not release inhibition.

The lower-level commands remain available for diagnosis:

```bash
sudo stream-v3-maintenance resume
sudo stream-v3-maintenance verify
sudo stream-v3-maintenance complete --confirm-youtube --confirm-twitch --confirm-live-data
```

`status` reads saved state; `check` reads current content and both output paths.
Neither fabricates a public-player result. Expiry reports `overdue=true` and
does not clear inhibition. For a confirmed partial entry, `advance` or `on`
continues remaining stages. Cancel through `off` after reconciling any unknown
outcome. Never delete the state file to return to normal operation.

## Installed artifact and permission contract

The operated command uses one root-owned release containing
`stream-v3-maintenance`, `manual_maintenance.py`, `maintenance_host.py` and
`maintenance_state.py`; the executable resolves its symlink before importing.
The fallback controller needs the matching state module and configured store.
The store is initialized once without overwriting prior state: directory 0755,
root-owned state/lock 0644, and read-only access for the fallback service.

The [configuration example](../../ops/selfheal/manual-maintenance.config.example.json)
contains no working machine or destination bindings. The private configuration
is root-owned 0600. Machine hashes use the stripped machine-id. Source and
mounted effect-ledger bindings, existing credentials and Kubernetes restrictions
are retained. `ssh_operator` uses an existing operator's SSH configuration; it
neither copies keys nor grants access. The helper checks machine and boot
identity before each effect, and the controller must expose a fresh guard
capability for the configured store.

The installed slate consists of a readable, root-owned `offline.mp4` and
`manifest.json`, both hash-pinned and verified in the actual relay namespace.
Its content is `Scheduled maintenance`, using the existing map and attribution,
with paused aircraft/weather information and no frozen live clock. The media
contract is 1920x1080 at 5 fps, H.264/yuv420p, silent stereo AAC at 48 kHz,
approximately four seconds. The separate offline-slate mode remains distinct.
The [builder](../../ops/selfheal/offline_slate.py) and installed preflight verify
these artifacts; merely placing them does not arm the slate. These are artifact
and permission constraints, not a complete installation procedure or authority
to run maintenance on another deployment.

## Boundaries and failure handling

Only the main Deployment replica count, the fixed sender recovery timer/service
and three relay fields are owned: `alwaysAvailable`, `alwaysAvailableFile` and
`alwaysAvailableTracks`. Forward destinations, authentication, TLS, credentials,
the existing fence ledger and central recovery policy are not overwritten.
Backup desired replicas of zero are insufficient if its actual Pods remain.
Effects and fallback scaling share a lock, and the legacy health windows are
reset across inhibition. Monitoring continues to expose raw health separately.

This adapter requires the existing `independent-controller` authority and no
unresolved effect. It never switches authority or clears a fence. Pending intent
is fsynced before one dispatch. A timeout or lost acknowledgement remains
UNKNOWN; `reconcile` only confirms an observed target state and never resends.
Old sessions, failed evidence and uncertain outcomes remain retained.

Snapshot/preflight reads may wait up to five seconds when the fixed recovery
oneshot is naturally activating. This bounded readiness wait applies only to
read-only observations; mutating calls still receive one dispatch.

The mode covers sender maintenance. Updating or rebooting the relay requires an
independent delivery source. Relay API changes may rebuild the path and may be
lost after a relay restart; identity or configuration drift stops the operation.
Existing Twitch daily renewal still applies. Local output progress cannot prove
broadcast retention, platform selection, public playback or uninterrupted switching.

The [unit tests](../../tests/test_manual_maintenance.py) exercise persistence,
locks, read-only SQL, interrupted entry and the actual executable. The
[isolated lab](../../ops/selfheal/manual_maintenance_lab.py) uses real MediaMTX,
the copy bridge, FIFO output and recording decode over literal loopback. Its
OS/Kubernetes/ledger and player confirmations are fixtures; its stability window
is shortened to two seconds. These tests are distinct from a production
maintenance window or provider acceptance.
