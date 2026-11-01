# Cross-host observation time

The protocol supplies nonce-bound clock intervals for UTC diagnostics and
same-boot boottime bounds for observation freshness. It does not set system
clocks or extend authorization, signature, lease or command expiry.

The production media consumer uses boottime freshness for both availability
and media health. UTC diagnostics remain separate from backup start and stop;
arena retains its independent recovery veto. See the current
[fallback contract](fallback-decision-contract.md) and the
[boottime audit](boottime-media-health.md). The sections below describe the
original interval-clock rollout and the timing protocol used by other readers.

## Exchange and freshness

Each request carries a random 128-bit nonce. The client captures its send and
receive wall/boottime pair; the responder captures its receive and send pair.
Remote-minus-local clock offset lies in `[t3 - t4, t2 - t1]`, expanded by a
5 ms capture allowance. The midpoint is an estimate, and the half-width is
uncertainty. Asymmetric SSH startup and network delays can make that interval
wide; the midpoint alone is not a measured clock error.

Interval-based readers require the oldest possible source age to fit their
freshness limit. UTC models reject wall-clock discontinuities, nonce or host
identity mismatches, excessive exchange duration and expired models. The media
consumer uses the independent same-boot age proof described above.
Different hosts' monotonic readings are never subtracted. Local elapsed time
uses `CLOCK_BOOTTIME`, including suspend.

The controller's freshness, comparison gaps and decision timers are defined in
the [fallback decision contract](fallback-decision-contract.md). For interval
readers, cached evidence cannot add healthy time during communication loss,
and a new process starts without a cache. The UTC model lifetime is 180 seconds,
with a configured 1000 ppm pair
holdover budget. This is an operational budget, not a proven hardware bound.

The monitoring adapter normalizes source timestamps conservatively into its
own UTC domain. The publisher repeats that operation at its boundary and strips
the private transport envelope. The original source time and uncertainty remain
in private receipts. A failed fetch retains the previous artifact bytes and
timestamps. No failure rejuvenates an old snapshot.

## Code and packaging

`ops/selfheal/clock_contract.py` is the canonical dependency. The isolated clock
probe is `ops/selfheal/clock_observer.py`; its service template reads kernel/NTP
state with no clock-setting capability. Missing NTP reference time stays null.

The relay HTTP adapter accepts both legacy `/facts` and `/facts?nonce=...`.
The forced SSH observer accepts a fixed 32-character nonce on stdin, allowing
the existing sudo environment to remain unchanged. It does not accept arbitrary
commands or expand SSH or sudo permissions.

When building an immutable release, copy the canonical `clock_contract.py`
beside each deployed consumer. The published compatibility and publisher copies
are generated from that same source. Rebuild them together; do not maintain
independent algorithm variants. The public monitoring adapter preserves its
configured Twitch collector/user binding.

`ops/scripts/sync_observation_clock.py --check` validates the generated package, compatibility and publisher
copies; its explicit `--write` mode regenerates only those files. CI checks
this dependency boundary without installing or restarting services.

The clock mesh queries fixed, preconfigured SSH peers and exports observation
metrics through the existing relay metrics endpoint. Its current facts expire
after 90 seconds. It has no action, notification, or authorization code.
Configure real SSH aliases, host-key verification, and machine pins privately.
Public examples contain no working credentials or production identities.

## Compatibility and rollout order

Compatibility is one-way. The updated facts server still answers legacy
`/facts` requests and adds a clock envelope for `?nonce=...`. A new controller
requires that envelope. A legacy server may reject the nonce URL or return no
clock envelope; the new consumer then records UNKNOWN and does not fall back
to scalar timestamp validation. A clock error by itself is not a confirmed
media failure; how the controller acts on it is defined in the
[fallback decision contract](fallback-decision-contract.md). Old servers remain
incompatible and cannot add healthy time.

Response producers must be updated before their consumers: clock helpers,
HTTP facts and the forced relay observer, then controller/monitor consumers;
the monitor's public timing producer must precede the new Pi collector. Keep
each deployed script with its matching common module and the facts service's
observer hash pin. Mixed versions are not a supported steady configuration.

For already clock-capable deployments, the optional stopped-relay response and
progress deadline use a different order: update the consumer first, then the
facts producer. That consumer supports existing normal responses and the new
availability field. Roll back the producer before the consumer. The operated
observer pin is retained; a compatibility input mapper preserves its existing
normal-path fields when that observer lacks the newer input helper.

Rollback reverses that dependency order. Restore the publisher consumer first,
then controller and monitoring consumers, before restoring their old facts or
timing producers. Removing the envelope producer first can trigger the backup
policy even when media is flowing. Verify the actual release and observation
state at each boundary; no public template grants host-operation authority.

## Public artifact contracts

The [clock observer template](../../ops/selfheal/units/stream-clock-observer.service.in),
[monitor service template](../../monitoring-v4/deploy/systemd/stream-v4-relay-observer.service.in),
[clock-export drop-in](../../monitoring-v4/deploy/systemd/stream-v4-relay-observer.service.d/80-clock-contract.conf)
and [mesh template](../../monitoring-v4/deploy/systemd/stream-clock-mesh.service.in)
preserve the operated process/permission boundaries with generic placeholders.
They do not create users, keys or authorized commands, and are not a complete
installation procedure.

The matching [relay](../../monitoring-v4/config/relay-observer.example.json),
[mesh](../../monitoring-v4/config/clock-mesh.example.json),
[encoder pin](../../monitoring-v4/config/clock-peers.example.json) and
[publisher pin](../../configs/publisher-clock-peers.example.json) examples use
non-working zero pins and `relay.invalid`. The private bindings and SSH
configuration are excluded. Keep `maintenance_state.py` beside the controller
and `public_snapshot_time.py` beside each publisher collector. Internal clock
receipts remain private and are stripped from the published projection.

## Verification and limits

The reviewed public regression and documentation selection passed 197 tests
and 1,187 subtests. It covers
positive and negative ten-minute offsets, asymmetric delays, steps, reboot or
identity changes, cache expiry, real loopback HTTP, isolated interpreter imports,
public allowlists, and existing fallback/maintenance behavior. These tests do
not inject faults into a production stream or establish provider playback.

The operated change preserved delivery process identities and observed current
YouTube/Twitch output progress. A 10.8-second FIFO recovery event was also
observed during the operation window; its cause and player impact were not
established. A process remaining alive is not proof of uninterrupted playback.

The existing bounded HDD recording was continued with corrected age bounds and
clock uncertainty, preserving its deadline, capacity and earlier records. A
full recording window and long-term false-positive performance remain separate
evidence gates. See [the recording contract](fallback-clock-recording.md).

The [review resolutions and current validation](../../publication/clock-review-fixes-20261031.json) distinguish these source fixes from historical test checkpoints and operated deployment evidence.
