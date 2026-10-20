# Public Source Port: Self-Healing OCI Fan-Out

## Scope

This port was prepared on 2026-10-20 JST. It publishes the route that replaced
the signed-lease relay that morning, the monitoring changes that followed it,
and the redesigned public status page. It also updates the entry documents so
that they describe the current route, and records why the earlier design was
retired. Nothing was deployed, and no running service was changed, by this
publication.

The integrated repository and the standalone Monitoring repository receive the
same monitoring changes. Their histories stay separate.

## What was ported

| Surface | Public files | Adaptation |
| --- | --- | --- |
| Self-healing route | `ops/selfheal/`, `deploy/k3s/youtube-backup/`, `tests/test_selfheal.py` | English comments; relay API address and Twitch account are required configuration; units are examples with placeholders; neutral image tag and host paths. |
| Relay observation | `monitoring-v4/.../relay_observe.py`, `relay_public.py`, `tests/test_relay_observe.py` | Reads MediaMTX and keeper state; reports a renewal's own midnight while it runs; adds the fallback state and rate. Generic host identifier. |
| Alert delivery | `monitoring-v4/.../twitch_alert_notify.py`, its test and example unit | Notification text in English. The webhook stays in an environment file outside the repository. |
| Public status page | `ops/public-publisher/site/public/index.html`, `assets/ops-20261020.*`, uploader allowlist, relay collector | Ported as published, with the two new assets added to the fixed upload allowlist. |
| Documentation | README, architecture, topology, runtime contract, scorecard, case studies, new route, decision, and page records | Rewritten for the current route; the 2026-10-13 design remains as dated history. |

## Excluded

Host installers that embed private addresses and host layout, the retired
route-introduction and qualification code from 2026-10-14 to 2026-10-19,
one-time recovery steps, the transition copy of the previous page, and live
host state are not published. Their outcome is summarized in the
[decision record](oci-route-simplification-decision.md).
[`publication/selfheal-fanout-port-20261020.json`](../../publication/selfheal-fanout-port-20261020.json)
lists the lineage, adaptations, and omissions, and
[`publication/source-map.json`](../../publication/source-map.json) records the
digest of each ported file.

## Validation

- Stream and publisher suite: 1240 passed, 9 skipped.
- Monitoring suite, integrated and standalone: 457 passed, 6 skipped, 6 failed.
  The six failures are in the release-tree test, which rejects the top-level
  `publication` directory. They reproduce on the standalone commit before this
  port and are not caused by it.
- Public language, port digests, k3s manifests, and the standalone public
  snapshot check pass.

The README diagram test now expects the current route (RTMPS over a private
tunnel, fallback only) instead of the earlier remux-sender wording.

## Not established by this port

The first keeper-driven Twitch renewal, the fallback upload rate during a real
fallback, delivery of a real Twitch alert, relay failure modes other than a
stopped service, and long-term soak.
