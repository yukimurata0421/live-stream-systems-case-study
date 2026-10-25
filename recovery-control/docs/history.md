# Historical CRA Design

The current tree contains the local reflex CRA. The earlier three-host design,
large chaos harness, soak machinery, deployment tools, schemas and migrations
are retained exactly in Git at the last revision before this reorganization:

`c9f632a38d1ca05e1b04dc502af7d7e2a0e7c6df`

- [Full previous recovery-control tree](https://github.com/yukimurata0421/live-stream-systems-case-study/tree/c9f632a38d1ca05e1b04dc502af7d7e2a0e7c6df/recovery-control)
- [Three-host architecture](https://github.com/yukimurata0421/live-stream-systems-case-study/blob/c9f632a38d1ca05e1b04dc502af7d7e2a0e7c6df/recovery-control/docs/architecture.md)
- [Original rationale and observed duplicate attempts](https://github.com/yukimurata0421/live-stream-systems-case-study/blob/c9f632a38d1ca05e1b04dc502af7d7e2a0e7c6df/recovery-control/docs/why-cra.md)
- [Harness trust and evidence limits](https://github.com/yukimurata0421/live-stream-systems-case-study/blob/c9f632a38d1ca05e1b04dc502af7d7e2a0e7c6df/recovery-control/docs/harness-trust.md)
- [Previous source and tests](https://github.com/yukimurata0421/live-stream-systems-case-study/tree/c9f632a38d1ca05e1b04dc502af7d7e2a0e7c6df/recovery-control/src)

That design placed each authorization on the central host and used local
fallback, leases, epochs and journal handback during a central-network outage.
Its central authority, Dell fallback agent, Monitoring projection, soak system
and general test harness are no longer installed as current CRA packages.

The unchanged Stream Executor, target observer, and their required shared
modules moved to the top-level `src/`, with boundary schemas and tests beside
the Stream code. They remain required by current delivery code. The current CRA
continues to use that Executor through its existing protocol.

Dated publication records and [local reflex evidence](local-reflex-records.md)
remain historical records. Their old test counts, source paths, candidate passes,
failures and unobserved outcomes are not rewritten as current claims. The
[reorganization record](../../publication/cra-layout-20261026.json) binds the
old revision and moved-file hashes. No Git history, operational ledger or live
service was reset, migrated or removed by this source cleanup.
