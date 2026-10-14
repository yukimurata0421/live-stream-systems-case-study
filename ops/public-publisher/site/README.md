# Static Operations Publisher

This directory contains the public source for a Raspberry Pi publisher. It
collects reduced monitoring evidence, builds static artifacts, and uploads an
allowlisted tree to a configured GCS prefix. The publisher does not control the
stream, select recovery actions, or store a monitoring database.

The [publisher boundary](../../../docs/v3/public-publisher-boundary.md) defines
the data and upload limits. The [source-port record](../../../docs/v3/pi-publisher-source-port-2026-10-14.md)
explains the differences imported from the publisher working tree.

## Source Layout

| Path | Purpose |
| --- | --- |
| `../status/` | Reduced Prometheus/Loki views with redacted text and allowlisted labels |
| `scripts/collect_reliability.py` | Seven numeric reliability indicators and a separate active-incident projection |
| `scripts/collect_twitch_public.py` | Sanitized Twitch provider and previous reception evidence |
| `scripts/collect_relay_public.py` | Sanitized relay input and separate provider outputs |
| `scripts/collect_twitch_sli.py` | Allowlisted arena-produced Twitch history; no local SLI reducer |
| `scripts/build_public.py` | Collection order and static-artifact assembly |
| `scripts/push_to_gcs.py` | Frozen upload allowlist, deadlines, and private publication status |
| `public/` | Static HTML, CSS, and JavaScript; generated live JSON is excluded |
| `systemd/` | Inert service/timer and external-environment examples |
| `cloudflare-worker/` | Edge source and an example routing configuration |

## Validate Without Live Services

Run these commands from the repository root:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -e '.[dev]'
.venv/bin/python -m pytest -q tests/test_public_*.py ops/public-publisher/status/tests
.venv/bin/python ops/scripts/validate_public_port.py
```

The publisher tests use synthetic data and mocked transports. They do not
upload objects, contact providers, or install a service. The HTML fixtures in
`tests/` exercise missing measurements and display recovery locally.

## Configure an Installation

Use `systemd/stream-v3-public-publisher.env.example` as the starting point for
an external environment file. Set an explicit GCS bucket **and prefix**, a
read-only monitoring SSH alias, the deployed source directory, and the persistent
monitoring state directory. `STATUS_DIR` selects the adjacent status collector.
Configure the existing provider collectors and their fixed read-only bridges
for the intended installation. Keep credentials and actual host configuration
outside this repository.

`scripts/build_public.py` performs real collection when invoked without mocks.
`scripts/push_to_gcs.py` also performs a cloud upload. Review the unit examples,
destination, credentials, and read-only source binding before either command
is used. Public CI does not perform these operations.

The Worker source retains a published example origin. Replace its origin and
the routes in `cloudflare-worker/wrangler.toml.example` for your deployment;
the example is not evidence of a live Worker configuration.

## Interpret the Evidence

Reliability schema `stream-v3-reliability-public.v5` separates Viewer evidence
from ADS-B source availability. Full-HD counts describe recorded sample
dimensions; unavailable or older low-resolution evidence does not establish
current visual correctness. Quiet aircraft activity is compatible with a
healthy, fresh source heartbeat.

`active_incidents` contains only identifiers, severity, recovery type, and
observation time. A failed incident read produces an unavailable count, not a
healthy zero. Notification backlog counts pending delivery only; it does not
count unresolved incidents. A formal rolling SLO breach can remain active after
the current stream has recovered.

Failed collection retains the last artifact and its original age. Publication
success, freshness, provider state, historical SLO assessment, and viewer
reception are separate results. Generated JSON, logs, caches, databases,
backups, and private installation files are not included in this source tree.
