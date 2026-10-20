# Twitch Session Renewal

> Historical design and exercise: 2026-10-13 JST. This daily owner was replaced
> on 2026-10-20 by the keeper in the
> [current OCI route](self-healing-oci-fanout.md#daily-twitch-renewal).
> The manual exercise below does not verify the replacement keeper's renewal.

## Historical Contract

The daily owner renews Twitch at 00:00 JST while preserving YouTube forwarding.
It stops only the bound Twitch writer, waits at least 120 seconds with fresh
official OFFLINE evidence, then starts one writer and verifies a new provider
stream identity. The normal restart is therefore after 00:02 JST. The renewal
deadline is 600 seconds; wall-clock and boot-bound elapsed-clock disagreement,
identity drift, stale input, an invalid lease, or an uncertain effect leads to
a hold rather than an automatic retry.

The pure reducer in
[`twitch_session.py`](../../ops/relay/twitch_session.py) emits bounded commands.
The adapter in [`twitch_daily.py`](../../ops/relay/twitch_daily.py) persists the
state revision and outbox before dispatch, reserves each request at most once,
and retains uncertain requests across process restart. A new request ID or a
restarted process cannot be used to replay a dispatch.

The owner binds broadcaster, route generation, authority epoch, relay instance,
boot, and writer generation. It checks that the YouTube forwarding handler
identity and creation time stay fixed while its byte counter advances. It
does not restart the relay service, change the YouTube watch URL, or enable
automatic route failover.

## Historical exercise: 2026-10-13, 11:38–11:56 JST

The first manual STOP exercise unexpectedly recreated the YouTube forwarding
handler. The owner detected `DAILY_YOUTUBE_HANDLER_CHANGED`, entered HOLD,
and did not issue the Twitch START. This failed attempt is retained as a
failure, not counted as an uninterrupted renewal.

The cause was MediaMTX v1.21.2 configuration round-tripping: an empty
`moqTransport` returned by GET was decoded as a changed transport when sent
back in PATCH. The adapter now omits that empty field while preserving the
YouTube entry and all other forwarding configuration. The old command was
not resent; recovery and the next bounded exercise had distinct identities.

In the corrected 11:50–11:53 exercise, the retained observations showed the
same YouTube handler and advancing counters, fresh Twitch OFFLINE for the
required wait, and a new Twitch stream ID. A guest HLS sample at 11:53:24
contained 1080p H.264 and 48 kHz stereo AAC; a one-second PCM decode had
nonzero energy. API LIVE, forwarding bytes, video reception, and audio decode
are recorded as separate evidence layers.

This publication records that manual exercise. It does not claim a later
scheduled-midnight exercise, 48-hour continuous reception, complete crash and
reconciliation qualification, or frame-by-frame continuity. The native
qualification manifest remains `UNVERIFIED_USER_CANCELLED_HARNESS`.

## Monitoring and publication

The API collector requires configured broadcaster and collector-revision
bindings. Missing identity, token acquisition failure, stale API data, or
unexpected responses become UNKNOWN, not OFFLINE. Private app credentials,
provider destinations, and account-administration helpers are excluded.

Relay transport, official API state, and public media reception have separate
projections. A planned break is suppressed only with fresh bounded daily-owner
evidence. It never suppresses a YouTube output fault. Static public projections
allowlist fields and exclude raw provider errors, configuration, machine
identity, and credentials.

Relevant tests are
[`test_twitch_session.py`](../../tests/test_twitch_session.py),
[`test_twitch_daily.py`](../../tests/test_twitch_daily.py), and
[`test_relay_monitor.py`](../../monitoring-v4/tests/test_relay_monitor.py).
They exercise synthetic facts and mocked effects. They are not a new live
renewal or deployment gate.
