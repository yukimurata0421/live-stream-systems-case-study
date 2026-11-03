# R2 Publication Transport

The portable R2 publisher uploads a fixed public allowlist, verifies object
bodies and keeps uncertain writes in a private durable receipt. New HTTPS
connections prefer IPv6 and fall back to IPv4 before sending HTTP. A stale
supporting feed remains visible as missing evidence while fresh live-status
feeds can continue to advance.

This is a source and configuration guide. The
[port record](../../publication/r2-publication-transport-port-20261103.json)
records offline validation; the [dated migration record](r2-publication-and-freshness-case-study.md)
describes earlier operated observations. Neither is a deployment of these
portable examples or proof of viewer playback.

The port includes a shared freshness module for Worker contract checks. It
preserves the packaged page and does not import unrelated browser, external
monitoring importer or notification-policy changes from the private tree.
Generated JSON, private status files, raw evidence and installation credentials
remain excluded.

## Publication Contract

`ops/public-publisher/site/scripts/push_to_r2.py` builds the existing public
artifacts and freezes the files named in `r2-public-files.json`. That manifest
matches this checkout's assets rather than a private installation's filenames.
Files must be bounded regular files; links, traversal, duplicate JSON keys,
non-finite values and oversized trees are rejected. Observation timestamps
must be no more than 180 seconds old and must not be in the future.

Each fresh observation is eligible independently. Missing or invalid feeds
produce a `PARTIAL` cycle and retain their old remote bytes and original age;
they do not prevent other fresh feeds from being published. A complete cycle
reports `SUCCEEDED`. Failure counters and prior failure timestamps are retained.
Static assets are versioned together with the index, and `site-release.json`
is announced only after the preceding static objects are verified. This is an
ordered object update, not an atomic bucket-wide transaction.

The destination binding includes account, bucket and prefix. Private receipts
track the attempted body digest and one of three states:

| State | Meaning and next step |
| --- | --- |
| `VERIFIED` | The observed R2 body matches the intended digest; unchanged bodies need no new PUT. |
| `NOT_SENT` | DNS or a transport-proven connection failure happened before HTTP transmission; a later cycle may publish its current snapshot. |
| `OUTCOME_UNKNOWN` | HTTP may have reached the provider; reconcile the attempted digest with GET before another write. A mismatch remains unresolved. |

A PUT is never automatically repeated after an uncertain outcome. Eligible
transient GET failures receive at most three attempts in total, with 0.25 and
0.75 second backoff within the remaining cycle budget. PUT readback and
uncertain-outcome reconciliation are separate from provider acceptance. The
cycle has a 240-second budget and at most two concurrent object requests.
The invocation lock and destination binding prevent overlapping or silently
retargeted receipt use. Do not erase unresolved receipts to force publication.

## IPv6 Preference And DNS

`r2_network.py` keeps at most two HTTPS connections to the fixed
`api.cloudflare.com:443` endpoint. CA validation, hostname verification and
TLS SNI remain enabled. It uses no redirect, address pin, insecure TLS mode or
automatic HTTP replay.

Address lookup requests both families. For each new connection, IPv6 candidates
share a one-second TCP plus TLS budget; IPv4 is then attempted within the
remaining connection timeout. No AAAA result also leads to IPv4. Once HTTP
has begun, a broken response is reconciled as a possibly completed write;
it does not trigger a second PUT over another family. Existing healthy pooled
connections are reused, so family preference is evaluated on new connections.

DNS transport and the resolved R2 address family are independent. The optional
resolver example sends DNS over IPv4 while still requesting A and AAAA records.
`render_r2_resolver.py` retains the first existing IPv4 resolver and adds
`1.1.1.1` and `8.8.8.8`, deduplicated, with `timeout:1 attempts:2`. It refuses
unreviewed search/domain/options configuration and writes only a new output
file. The `95-r2-network.conf` bind mount applies that file only inside the
publisher service; it does not replace the host's resolver configuration.
`96-r2-ipv6-preferred.conf` resets the address-family restriction and allows
`AF_UNIX AF_INET AF_INET6`.

Keepalive reduces repeated lookups. On lookup failure, bounded diagnostic
queries report fixed result codes, timing, resolver ordinals and a resolver-file
fingerprint; resolver addresses, arbitrary error strings and credentials are
not emitted. Counters distinguish DNS failures, connection attempts, chosen
families, IPv4 fallback and connection reuse.

These measures reduce repeated DNS work and isolate resolver configuration;
they do not prove the cause of an earlier DNS incident. Correlated resolver
failure, upstream connectivity loss and blocking system resolver behavior remain
possible. Actual IPv6 routing and resolver reachability require an installation
check; local fixture success cannot establish either.

## Installation Example

The shipped service still selects the retained GCS uploader. Installing R2 is
an explicit operator action, outside public CI:

1. Install the source beneath `/opt/stream-v3-public-publisher/`. Configure the
   existing collectors using external environment values and read-only bridges
   described in the [source guide](../../ops/public-publisher/site/README.md).
   `STREAM_V3_EGRESS_SSH_TARGET` selects the egress projection's monitoring alias.
2. Merge `systemd/stream-v3-r2-publisher.env.example` into the service's existing
   `/etc/default/stream-v3-public-publisher` environment. Set the bucket, private
   status path and credential-file path. Keep the token in a separate mode-0600
   file owned by root or the service user, following `cloudflare-r2.env.example`.
   Its account must match the chosen bucket. Keep receipts and events outside
   the public tree and preserve them across service updates.
3. Set the same non-root prefix in `YUKIMURATA_R2_PREFIX` and the Worker's
   `PUBLIC_R2_PREFIX`; both default to `public/`. Only slash-terminated segments
   containing letters, numbers, `_` or `-` are accepted. No credentials or
   installation identities belong in the repository.
4. If using the isolated resolver, render a reviewed host resolver input to a
   new private file, then install it at
   `/etc/stream-v3-public-publisher/resolv.conf`. Install `95-r2-network.conf`
   only after that file exists. Review any host-specific search/domain needs.
5. Install `96-r2-ipv6-preferred.conf` and copy `r2.conf.example` to an active
   `r2.conf` in `/etc/systemd/system/yukimurata-public-push.service.d/`. Inspect the merged unit,
   including the selected executable, environment, resolver mount, permitted
   families and writable state directory before enabling a timer.
6. Configure the private bucket binding using
   `cloudflare-worker/wrangler.r2.toml.example` and `src/r2.js`. Configure the
   intended domain separately. The Worker serves GET/HEAD only and reads its
   bucket directly; an absent or failed binding yields 503 without GCS fallback.

Retain the prior unit, environment, resolver file and source revision before
an installation change. A rollback restores that exact installation binding;
it preserves publication receipts and failure history. Do not enable a second
uploader to the same prefix as a rollback shortcut.

## Worker And Health Endpoints

An optional `YUKIMURATA_PUBLICATION_HEALTH_URL` selects the installation's
HTTPS `/health/publication` endpoint. If unset, diagnostics report
`HEALTH_NOT_CONFIGURED` and make no request. The configured URL cannot contain
credentials, query parameters or a fragment; redirects are refused. This
diagnostic does not turn uploader success into an external-reception claim.

The Worker primary endpoint requires the page and three live-status feeds:
monitoring, relay and Twitch observation. `/health/observations` additionally
requires the five supporting feeds. Both return all feed states with
`Cache-Control: no-store`. Normal JSON has a 60-second browser lifetime and
static assets 300 seconds; release announcements are not cached. These are
publication checks, independent of media playback and formal SLO judgments.

## Offline Validation

From the repository root, with the existing development environment and Node.js 22:

```bash
.venv/bin/python -B -m pytest -q ops/public-publisher/site/tests tests/test_public_r2_port.py tests/test_public_publisher_build.py tests/test_public_observation_bundle.py
node --test ops/public-publisher/site/cloudflare-worker/tests/r2-worker.test.mjs
.venv/bin/python ops/scripts/validate_public_port.py
.venv/bin/python ops/scripts/validate_public_language.py
```

Python tests use local TLS/DNS peers and mocked providers. Worker tests use an
in-memory bucket, including the packaged index and configured-prefix checks.
No check installs a service, uploads an object or contacts the real provider.
`push_to_r2.py --plan` also makes no requests and uses no credentials, but it
requires the generated public inputs to exist and pass freshness validation;
a source-only checkout does not contain those live JSON files.
