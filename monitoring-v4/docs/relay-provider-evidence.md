# Relay And Provider Evidence

The 2026-10-13 source update adds read-only OCI relay input/output, Twitch
API, public reception, and SLI projections. They have independent source times
and freshness gates. Byte progress, provider LIVE state, and decoded media are
separate evidence planes; none grants recovery authority.

`compatibility/relay_observe.py` collects fixed local sources and allowlisted
GET endpoints. `relay_monitor.py` reduces bound raw evidence to transport and
resource facts. `collectors/twitch_api.py` produces read-only official provider
evidence; `twitch_public.py`, `twitch_sli.py`, and the public projection modules
retain UNKNOWN on stale or unbound input. Broadcaster identity and collector
revision come from explicit operator bindings, not a baked-in private account.
Synthetic tests use fixture identities.

A planned Twitch break is recognized only with fresh bounded daily-owner
state. It cannot suppress a YouTube output issue. Counter, boot, or handler
changes invalidate rates. Output projections exclude forwarding destinations,
raw provider errors, credentials, boot identity, and machine bindings.

Account metadata administration and credential installers are excluded.
CLI collection needs reviewed source paths and credentials held outside this
repository. There is no enabled notification provider, runtime executor,
route policy, or public-site deployment in this source update.

Focused tests cover relay reduction, API identity/freshness, public projection,
SLI uncertainty, and recovery verification. They use synthetic/mocked inputs.
They do not complete a seven-day coverage gate, fourteen-day parity gate,
provider reception soak, or native failure qualification. Earlier milestone
counts in historical documents retain their original validation window.
