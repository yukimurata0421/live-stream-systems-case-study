# Normal Monitoring And Verified History Copies

Schema v7 separates normal monitoring health from explicit migration
qualification. This is a public source contract; copying these examples does
not migrate a running database or authorize deployment.

## Normal Health

Each completed cycle updates minute slots, bounded rolling aggregates and one
current checkpoint in its existing transaction. `commands.normal_report` reads
only `normal_cycle_current`, `metric_series` and `metric_windows`; it does not
decode a long window of detailed observations. Definitions and stable targets
identify series independently of build revisions. Missing minutes remain
missing, and bad, unknown and not-applicable outcomes stay distinct. Current
facts retain their original expiry, which the reporter and sentinel recheck.

The reporter example runs at second 30 each minute, retains the existing report
filename for consumers, and uses a 120-second readiness limit. Its read-only
role has explicit table grants. PostgreSQL long-history readers use a server
cursor inside a read-only transaction and close it before releasing the pool.

## Migration Qualification

Ordinary cycles skip legacy comparison. `commands.shadow_once --migration-campaign NAME`
explicitly collects a campaign. `commands.shadow_report`
requires either `--campaign NAME` or `--historical`; the latter selects retained
pre-separation rows. Campaign comparison errors remain failed campaign evidence
without stopping normal monitoring. Seven-day coverage, fourteen-day parity and
the existing phase gates are unchanged. A healthy normal report is not migration
qualification or recovery authority.

## History Copy

`commands.history_archive --received-day-jst YYYY-MM-DD --output-dir DIRECTORY`
copies a completed JST day older than seven days. It takes a consistent
read-only snapshot and streams into a bounded staging SQLite file. Before
publication it rereads integrity, counts, source identifiers and content
digests; publication is atomic without replacement, followed by read-only file
permissions. Repeating identical input reuses the verified copy. Later arrivals
produce another copy while preserving earlier evidence. Source rows are never
deleted, and no automatic archive timer is added.

SQLite and PostgreSQL migration schemas agree through v7. Restore verification
accepts exact v6 or v7 migration histories and rereads the new tables for v7.
The legacy SQLite-to-PostgreSQL merger rejects populated normal history instead
of silently losing aggregate checkpoints; use a full restore for that case.
Schema v6 binaries reject v7. A production rollback needs a separately verified
v7-compatible runtime or a deliberate database restore, not a downgrade by
changing an image tag.

## Local Checks And Limits

From the repository root, with the test environment active and the
[subproject test dependencies](../README.md#run-the-tests) installed:

```bash
cd monitoring-v4
python3 -m pytest -q tests/test_history_tiers.py tests/test_history_restore.py \
  tests/test_postgres_k3s.py tests/test_sentinel.py
```

Tests use temporary SQLite state and mocked PostgreSQL cursors. Disposable live
PostgreSQL qualification and production migration, capacity, lock duration,
backup restoration and rollback are separate checks. No private database,
operational evidence, deployment image or credentials are included here.
